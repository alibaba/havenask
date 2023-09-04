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
#include <stddef.h>
#include <stdint.h>
#include <utility>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "autil/ThreadPool.h"
#include "swift/broker/service/BrokerPartition.h"
#include "swift/common/Common.h"
#include "swift/log/BrokerLogClosure.h"
namespace swift {

namespace protocol {
class LongPollingTracer;
class MessageResponse;
} // namespace protocol

namespace config {
class BrokerConfig;
} // namespace config

namespace service {
class TopicPartitionSupervisor;
class TransporterImpl;

class LongPollingRequestHandler {
public:
    using ReadRequestQueue = BrokerPartition::ReadRequestQueue;

public:
    LongPollingRequestHandler(TransporterImpl *transporter, TopicPartitionSupervisor *tp);
    virtual ~LongPollingRequestHandler();

public:
    bool init(const swift::config::BrokerConfig *config);
    // virtual for test
    virtual void addLongPollingRequest(BrokerPartitionPtr &brokerPartition,
                                       const util::ConsumptionLogClosurePtr &closure,
                                       ::swift::protocol::LongPollingTracer *tracer = nullptr);
    // virtual for test
    virtual void addHoldRequest(const util::ConsumptionLogClosurePtr &closure);
    size_t getHoldQueueSize() const { return _holdQueueSize; }
    size_t getHoldTimeoutCount() const { return _holdTimeoutCount; }
    size_t getClearPollingQueueSize() const {
        size_t queueSize = 0u;
        if (_clearPollingThreadPool) {
            queueSize = _clearPollingThreadPool->getItemCount();
        }
        return queueSize;
    }
    void stop();
    // virtual for test
    virtual void registeCallBackBeforeSend(const BrokerPartitionPtr &brokerPartition,
                                           util::ProductionLogClosure *closure);

private:
    bool isEnableLongPolling(BrokerPartitionPtr brokerPartition) const {
        return brokerPartition->isEnableLongPolling();
    }
    void doAfterSendMessage(BrokerPartitionPtr brokerPartition, ::swift::protocol::MessageResponse *response);
    int64_t getExpireTime(int64_t startTime);
    int64_t getHoldExpireTime(int64_t startTime);
    std::pair<bool, int64_t> currentTime();
    void checkTimeout();
    void checkHoldQueue();
    bool isStopped() { return _stopped; }

private:
    volatile bool _stopped = false;
    TransporterImpl *_transporter = nullptr;
    TopicPartitionSupervisor *_tpSupervisor = nullptr;
    size_t _holdTimeUs = 0;
    size_t _timeoutUs = 0;
    size_t _notifyIntervalUs = 0;
    size_t _holdQueueSize;
    size_t _holdTimeoutCount;
    int64_t _lastTime = 0;
    autil::ThreadMutex _holdQueueMutex;
    autil::ThreadMutex _timeMutex;
    autil::LoopThreadPtr _timeoutCheckThread;
    autil::ThreadPoolPtr _clearPollingThreadPool;
    ReadRequestQueue _holdRequestQueue;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(LongPollingRequestHandler);

} // end namespace service
} // end namespace swift
