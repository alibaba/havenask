/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>
#include <stdint.h>
#include <string>

#include "autil/LoopThread.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace network {
class SwiftRpcChannelManager;
} // namespace network

namespace replication {
class MessageConsumer;
class MessageProducer;
class PartitionReplicator;
} // namespace replication

namespace service {

class BrokerPartition;

class MirrorPartition {
public:
    explicit MirrorPartition(const protocol::PartitionId &pid);
    virtual ~MirrorPartition();

public:
    bool start(const std::string &mirrorZkRoot,
               const std::shared_ptr<network::SwiftRpcChannelManager> &channelManager,
               BrokerPartition *brokerPartition,
               int64_t loopInterval = DEFAULT_LOOP_INTERVAL);
    void stop();

private:
    void workLoop();

public:
    uint64_t selfVersion() const;
    bool canServe() const;
    bool canWrite() const;

private:
    // virtual for test
    virtual std::unique_ptr<replication::MessageProducer>
    createProducer(const std::string &mirrorZkRoot,
                   const std::shared_ptr<network::SwiftRpcChannelManager> &channelManager) const;
    std::unique_ptr<replication::MessageConsumer> createConsumer(BrokerPartition *brokerPartition) const;

private:
    protocol::PartitionId _pid;
    BrokerPartition *_owner = nullptr;
    std::unique_ptr<replication::PartitionReplicator> _replicator;

    autil::LoopThreadPtr _loopThread;

private:
    static constexpr int64_t DEFAULT_LOOP_INTERVAL = 50 * 1000; // 50ms
};

} // namespace service
} // namespace swift
