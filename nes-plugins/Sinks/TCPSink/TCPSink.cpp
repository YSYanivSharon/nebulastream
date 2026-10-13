/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <TCPSink.hpp>

#include <cerrno>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <netdb.h>
#include <poll.h>
#include <unistd.h>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <sys/socket.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

TCPSink::Connection::Connection(const int fd) : fd(fd)
{
}

TCPSink::Connection::~Connection()
{
    close();
}

void TCPSink::Connection::close()
{
    if (closed)
    {
        return;
    }
    if (pending.size() > pendingOffset)
    {
        NES_WARNING("TCPSink dropping {} unsent bytes of a closed connection.", pending.size() - pendingOffset);
    }
    ::close(fd);
    closed = true;
    pending.clear();
    pendingOffset = 0;
}

bool TCPSink::Connection::flush()
{
    while (!closed && pendingOffset < pending.size())
    {
        const auto written = ::send(fd, pending.data() + pendingOffset, pending.size() - pendingOffset, MSG_NOSIGNAL);
        if (written >= 0)
        {
            pendingOffset += static_cast<size_t>(written);
            continue;
        }
        if (errno == EINTR)
        {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return false;
        }
        NES_WARNING("TCPSink closing client connection after send failed: {}", std::strerror(errno));
        close();
    }
    pending.clear();
    pendingOffset = 0;
    return !closed;
}

void TCPSink::Connection::write(std::span<const char> payload)
{
    while (!payload.empty())
    {
        const auto written = ::send(fd, payload.data(), payload.size(), MSG_NOSIGNAL);
        if (written >= 0)
        {
            payload = payload.subspan(static_cast<size_t>(written));
            continue;
        }
        if (errno == EINTR)
        {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            pending.assign(payload.begin(), payload.end());
            return;
        }
        NES_WARNING("TCPSink closing client connection after send failed: {}", std::strerror(errno));
        pending.assign(payload.begin(), payload.end());
        close();
        return;
    }
}

TCPSink::TCPSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , bindAddress(sinkDescriptor.getFromConfig(ConfigParametersTCPSink::BIND_ADDRESS))
    , port(sinkDescriptor.getFromConfig(ConfigParametersTCPSink::PORT))
    , minConnections(sinkDescriptor.getFromConfig(ConfigParametersTCPSink::MIN_CONNECTIONS))
    , backpressureHandler(
          sinkDescriptor.getFromConfig(SinkDescriptor::BACKPRESSURE_UPPER_THRESHOLD),
          sinkDescriptor.getFromConfig(SinkDescriptor::BACKPRESSURE_LOWER_THRESHOLD))
{
}

std::ostream& TCPSink::toString(std::ostream& os) const
{
    os << "\nTCPSink(";
    os << "\n  bindAddress: " << bindAddress;
    os << "\n  port: " << port;
    os << "\n  minConnections: " << minConnections;
    os << ")\n";
    return os;
}

void TCPSink::start(PipelineExecutionContext&)
{
    NES_INFO("Opening TCPSink on {}:{}.", bindAddress, port);

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    addrinfo* rawResult = nullptr;
    if (const auto error = getaddrinfo(bindAddress.c_str(), std::to_string(port).c_str(), &hints, &rawResult); error != 0)
    {
        throw CannotOpenSink("TCPSink cannot resolve {}: {}", bindAddress, gai_strerror(error));
    }
    const std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> result(rawResult, freeaddrinfo);

    std::string lastError = "no address";
    for (const auto* address = result.get(); address != nullptr; address = address->ai_next)
    {
        const auto fd = socket(address->ai_family, address->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC, address->ai_protocol);
        if (fd < 0)
        {
            lastError = std::strerror(errno);
            continue;
        }
        constexpr int enable = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));
        if (bind(fd, address->ai_addr, address->ai_addrlen) == 0 && listen(fd, SOMAXCONN) == 0)
        {
            listenFd = fd;
            break;
        }
        lastError = std::strerror(errno);
        ::close(fd);
    }
    if (listenFd < 0)
    {
        throw CannotOpenSink("TCPSink cannot listen on {}:{}: {}", bindAddress, port, lastError);
    }

    acceptThread = std::jthread([this](const std::stop_token& stopToken) { acceptLoop(stopToken); });
}

void TCPSink::acceptLoop(const std::stop_token& stopToken)
{
    pollfd listener{.fd = listenFd, .events = POLLIN, .revents = 0};
    while (!stopToken.stop_requested())
    {
        if (poll(&listener, 1, static_cast<int>(ACCEPT_POLL_INTERVAL.count())) <= 0)
        {
            continue;
        }
        while (true)
        {
            const auto fd = accept4(listenFd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd < 0)
            {
                if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
                {
                    NES_WARNING("TCPSink accept failed: {}", std::strerror(errno));
                }
                break;
            }
            const auto count = connections.withWLock(
                [fd](auto& locked)
                {
                    locked.push_back(std::make_shared<Connection>(fd));
                    return locked.size();
                });
            NES_INFO("TCPSink on port {} accepted client {}.", port, count);
        }
    }
}

void TCPSink::removeClosedConnections()
{
    connections.withWLock(
        [](auto& locked)
        {
            std::erase_if(
                locked,
                [](const auto& connection)
                {
                    const std::scoped_lock lock(connection->mutex);
                    return connection->closed;
                });
        });
}

TCPSink::TrySendResult TCPSink::trySend(const TupleBuffer& buffer)
{
    std::string payload;
    BufferIterator iterator{buffer};
    for (auto element = iterator.getNextElement(); element.has_value(); element = iterator.getNextElement())
    {
        const auto data = element->buffer.getAvailableMemoryArea<char>().first(element->contentLength);
        payload.append(data.begin(), data.end());
    }
    if (payload.empty())
    {
        return TrySendResult::Ok;
    }

    bool sawClosed = false;
    const auto sent = connections.withRLock(
        [&](const auto& locked)
        {
            if (locked.size() < minConnections)
            {
                return false;
            }
            const auto start = nextConnection.fetch_add(1, std::memory_order_relaxed);
            for (size_t i = 0; i < locked.size(); ++i)
            {
                auto& connection = *locked[(start + i) % locked.size()];
                const std::unique_lock lock(connection.mutex, std::try_to_lock);
                if (!lock.owns_lock())
                {
                    continue;
                }
                if (!connection.flush())
                {
                    sawClosed |= connection.closed;
                    continue;
                }
                connection.write(payload);
                sawClosed |= connection.closed;
                return true;
            }
            return false;
        });

    if (sawClosed)
    {
        removeClosedConnections();
    }
    return sent ? TrySendResult::Ok : TrySendResult::Full;
}

void TCPSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pec)
{
    PRECONDITION(listenFd >= 0, "TCPSink is not listening");
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in TCPSink.");

    auto currentBuffer = std::optional{inputTupleBuffer};
    while (currentBuffer)
    {
        switch (trySend(*currentBuffer))
        {
            case TrySendResult::Ok: {
                currentBuffer = backpressureHandler.onSuccess(backpressureController);
                continue;
            }
            case TrySendResult::Full: {
                if (const auto emit = backpressureHandler.onFull(*currentBuffer, backpressureController))
                {
                    pec.repeatTask(*emit, BACKPRESSURE_RETRY_INTERVAL);
                }
                return;
            }
        }
    }
}

void TCPSink::stop(PipelineExecutionContext& pec)
{
    if (listenFd < 0)
    {
        return;
    }
    INVARIANT(backpressureHandler.empty(), "BackpressureHandler is not empty");

    const auto drained = connections.withRLock(
        [](const auto& locked)
        {
            bool allFlushed = true;
            for (const auto& connection : locked)
            {
                const std::scoped_lock lock(connection->mutex);
                allFlushed &= connection->flush() || connection->closed;
            }
            return allFlushed;
        });
    if (!drained)
    {
        pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
        return;
    }

    acceptThread.request_stop();
    acceptThread.join();
    ::close(listenFd);
    listenFd = -1;
    connections.wlock()->clear();
    NES_INFO("TCP Sink completed.");
}

DescriptorConfig::Config TCPSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTCPSink>(std::move(config), NAME);
}

}
