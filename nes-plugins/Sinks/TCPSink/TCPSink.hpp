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

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// A sink that listens on a TCP port and writes the bytes of every incoming TupleBuffer to one of the
/// connected clients, picking clients in round-robin order.
class TCPSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "TCP";
    static constexpr auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(10);
    static constexpr auto ACCEPT_POLL_INTERVAL = std::chrono::milliseconds(100);

    explicit TCPSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    enum class TrySendResult : uint8_t
    {
        Ok,
        Full,
    };

    struct Connection
    {
        explicit Connection(int fd);
        ~Connection();
        Connection(const Connection&) = delete;
        Connection& operator=(const Connection&) = delete;
        Connection(Connection&&) = delete;
        Connection& operator=(Connection&&) = delete;

        /// Writes as much of the pending bytes as the socket accepts. Returns true once nothing is pending.
        bool flush();
        /// Writes the payload; whatever the socket does not accept right away is kept as pending bytes.
        void write(std::span<const char> payload);
        void close();

        std::mutex mutex;
        int fd;
        bool closed = false;
        std::string pending;
        size_t pendingOffset = 0;
    };

    TrySendResult trySend(const TupleBuffer& buffer);
    void acceptLoop(const std::stop_token& stopToken);
    void removeClosedConnections();

    std::string bindAddress;
    uint32_t port;
    uint32_t minConnections;

    int listenFd = -1;
    std::jthread acceptThread;
    folly::Synchronized<std::vector<std::shared_ptr<Connection>>> connections;
    std::atomic<size_t> nextConnection{0};
    BackpressureHandler backpressureHandler;
};

struct ConfigParametersTCPSink
{
    ///NOLINTBEGIN(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> BIND_ADDRESS{
        "BIND_ADDRESS",
        "0.0.0.0",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(BIND_ADDRESS, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> PORT{
        "PORT",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            constexpr uint32_t PORT_NUMBER_MAX = 65535;
            auto value = DescriptorConfig::tryGet(PORT, config);
            if (!value || value.value() == 0 || value.value() > PORT_NUMBER_MAX)
            {
                NES_ERROR("TCPSink PORT must be between 1 and {}.", PORT_NUMBER_MAX);
                return std::nullopt;
            }
            return value;
        }};

    /// Buffers are held back (with backpressure) until this many clients are connected, so that early
    /// connections do not receive all the data before the rest have joined.
    static inline const DescriptorConfig::ConfigParameter<uint32_t> MIN_CONNECTIONS{
        "MIN_CONNECTIONS",
        1,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            auto value = DescriptorConfig::tryGet(MIN_CONNECTIONS, config);
            if (!value || value.value() == 0)
            {
                NES_ERROR("TCPSink MIN_CONNECTIONS must be at least 1.");
                return std::nullopt;
            }
            return value;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, BIND_ADDRESS, PORT, MIN_CONNECTIONS);
    ///NOLINTEND(cert-err58-cpp)
};

}

FMT_OSTREAM(NES::TCPSink);
