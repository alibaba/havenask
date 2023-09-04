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
#include "swift/broker/service/MirrorPartition.h"

#include <utility>

#include "autil/Log.h"
#include "swift/broker/replication/MessageConsumer.h"
#include "swift/broker/replication/MessageProducer.h"
#include "swift/broker/replication/MessageProducerImpl.h"
#include "swift/broker/replication/PartitionReplicator.h"
#include "swift/broker/service/BrokerPartition.h"
#include "swift/broker/service/LocalMessageConsumer.h"
#include "swift/client/SwiftTransportAdapter.h"
#include "swift/client/TransportClosure.h"
#include "swift/network/SwiftAdminAdapter.h"

namespace swift {
namespace service {
AUTIL_DECLARE_AND_SETUP_LOGGER(swift, MirrorPartition);

MirrorPartition::MirrorPartition(const protocol::PartitionId &pid) : _pid(pid) {}

MirrorPartition::~MirrorPartition() { stop(); }

bool MirrorPartition::start(const std::string &mirrorZkRoot,
                            const std::shared_ptr<network::SwiftRpcChannelManager> &channelManager,
                            BrokerPartition *brokerPartition,
                            int64_t loopInterval) {
    _owner = brokerPartition;
    auto producer = createProducer(mirrorZkRoot, channelManager);
    auto consumer = createConsumer(brokerPartition);
    int64_t committedId = brokerPartition->getCommittedMsgId();
    _replicator = std::make_unique<replication::PartitionReplicator>(
        &_pid, std::move(producer), std::move(consumer), committedId);
    auto fn = [this]() { workLoop(); };
    _loopThread = autil::LoopThread::createLoopThread(fn, loopInterval, "mirror");
    return true;
}

void MirrorPartition::stop() {
    if (_loopThread) {
        _loopThread->stop();
        _loopThread.reset();
    }
}

void MirrorPartition::workLoop() {
    _replicator->work();
    if (_replicator->canServe()) {
        auto status = _owner->getPartitionStatus();
        if (status != protocol::PARTITION_STATUS_RUNNING) {
            AUTIL_LOG(INFO,
                      "%s: mirror partition recovered from %s to running",
                      _pid.ShortDebugString().c_str(),
                      protocol::PartitionStatus_Name(status).c_str());
            _owner->setPartitionStatus(protocol::PARTITION_STATUS_RUNNING);
        }
    }
}

std::unique_ptr<replication::MessageProducer>
MirrorPartition::createProducer(const std::string &mirrorZkRoot,
                                const std::shared_ptr<network::SwiftRpcChannelManager> &channelManager) const {
    auto adminAdapter = std::make_shared<network::SwiftAdminAdapter>(mirrorZkRoot, channelManager);
    auto client = std::make_unique<client::SwiftTransportAdapter<client::TRT_GETMESSAGE>>(
        adminAdapter, channelManager, _pid.topicname(), _pid.id());
    return std::make_unique<replication::MessageProducerImpl>(
        _pid.inlineversion().masterversion(), true, std::move(client));
}

std::unique_ptr<replication::MessageConsumer> MirrorPartition::createConsumer(BrokerPartition *brokerPartition) const {
    return std::make_unique<LocalMessageConsumer>(brokerPartition);
}

uint64_t MirrorPartition::selfVersion() const {
    if (!_replicator) {
        return 0;
    }

    return _replicator->selfVersion();
}

bool MirrorPartition::canServe() const {
    if (!_replicator) {
        return false;
    }
    return _replicator->canServe();
}

bool MirrorPartition::canWrite() const {
    if (!_replicator) {
        return false;
    }
    return _replicator->canWrite();
}

} // namespace service
} // namespace swift
