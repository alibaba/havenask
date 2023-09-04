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
#include "swift/broker/service/LongPollingRequestHandler.h"

#include <cstdint>
#include <functional>
#include <iosfwd>
#include <list>
#include <vector>

#include "autil/TimeUtility.h"
#include "swift/broker/service/RequestPollingWorkItem.h"
#include "swift/broker/service/TopicPartitionSupervisor.h"
#include "swift/broker/service/TransporterImpl.h"
#include "swift/broker/storage/MessageGroup.h"
#include "swift/config/BrokerConfig.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/monitor/TimeTrigger.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/Block.h"
#include "swift/util/FilterUtil.h"

using namespace std;
using namespace swift::util;
using namespace swift::config;
using namespace swift::protocol;
using namespace swift::monitor;
using namespace autil;

namespace swift {
namespace service {
AUTIL_LOG_SETUP(swift, LongPollingRequestHandler);

LongPollingRequestHandler::LongPollingRequestHandler(TransporterImpl *transporter, TopicPartitionSupervisor *tp)
    : _transporter(transporter), _tpSupervisor(tp) {}

LongPollingRequestHandler::~LongPollingRequestHandler() {}

bool LongPollingRequestHandler::init(const BrokerConfig *config) {
    _holdTimeUs = config->getHoldNoDataRequestTimeMs() * 1000;
    _timeoutUs = config->getRequestTimeout();
    _notifyIntervalUs = config->getNoDataRequestNotfiyIntervalMs() * 1000;
    _timeoutCheckThread = autil::LoopThread::createLoopThread(
        std::bind(&LongPollingRequestHandler::checkTimeout, this), _notifyIntervalUs, "timeout_checker");
    _clearPollingThreadPool =
        std::make_shared<autil::ThreadPool>(config->clearPollingThreadNum, config->clearPollingQueueSize);
    if (!_clearPollingThreadPool->start("clear_polling")) {
        AUTIL_LOG(ERROR, "start thread pool failed");
        return false;
    }

    return true;
}

int64_t LongPollingRequestHandler::getExpireTime(int64_t startTime) {
    int64_t checkTime = currentTime().second + _notifyIntervalUs;
    int64_t expireTime = startTime + _timeoutUs;
    if (checkTime < expireTime) {
        return expireTime;
    }
    return -1;
}

int64_t LongPollingRequestHandler::getHoldExpireTime(int64_t startTime) {
    int64_t expireTime = startTime + _timeoutUs;
    int64_t endHoldTime = currentTime().second + _holdTimeUs;
    AUTIL_LOG(DEBUG, "expireTime[%ld] endHoldTime[%ld], holdTimeUs[%ld]", expireTime, endHoldTime, _holdTimeUs);
    if (endHoldTime + _notifyIntervalUs < expireTime && _holdTimeUs > 0) {
        return endHoldTime;
    }
    return -1;
}

std::pair<bool, int64_t> LongPollingRequestHandler::currentTime() {
    int64_t currentTime = TimeUtility::currentTime();
    bool backTime = false;
    ScopedLock lock(_timeMutex);
    if (currentTime > _lastTime) {
        _lastTime = currentTime;
    } else { // 时间戳回退
        _lastTime = _lastTime + 1;
        backTime = true;
    }
    return std::make_pair(backTime, _lastTime);
}

void LongPollingRequestHandler::checkHoldQueue() {
    auto timeInfo = currentTime();
    ReadRequestQueue timeoutQueue;
    ScopedLock lock(_holdQueueMutex);
    if (timeInfo.first) {
        timeoutQueue.swap(_holdRequestQueue);
    } else {
        for (auto it = _holdRequestQueue.begin(); it != _holdRequestQueue.end();) {
            if (it->first <= timeInfo.second) {
                timeoutQueue.push_back(*it);
                it = _holdRequestQueue.erase(it);
            } else {
                ++it;
            }
        }
    }
    _holdTimeoutCount = timeoutQueue.size();
    _holdQueueSize = _holdRequestQueue.size();
}

void LongPollingRequestHandler::checkTimeout() {
    checkHoldQueue();
    if (!_tpSupervisor) {
        return;
    }
    std::vector<protocol::PartitionId> partitionIds;
    _tpSupervisor->getAllPartitions(partitionIds);
    for (const auto &partitionId : partitionIds) {
        BrokerPartitionPtr brokerPartition = _tpSupervisor->getBrokerPartition(partitionId);
        if (!brokerPartition) {
            continue;
        }
        if (brokerPartition->getPartitionStatus() != protocol::PARTITION_STATUS_RUNNING) {
            continue;
        }
        BrokerPartition::ReadRequestQueue timeoutReqs;
        auto timeInfo = currentTime();
        auto remainsCount =
            brokerPartition->stealTimeoutPolling(timeInfo.first, timeInfo.second, _timeoutUs, timeoutReqs);
        auto timeoutCount = timeoutReqs.size();
        AUTIL_LOG(DEBUG, "hold request remain count [%lu] timeout count [%ld]", remainsCount, timeoutCount);
        LongPollingMetricsCollector collector;
        collector.longPollingHoldCount = remainsCount + timeoutCount;
        collector.longPollingTimeoutCount = timeoutCount;
        brokerPartition->reportLongPollingMetrics(collector);
    }
}

void LongPollingRequestHandler::addLongPollingRequest(BrokerPartitionPtr &brokerPartition,
                                                      const util::ConsumptionLogClosurePtr &closure,
                                                      ::swift::protocol::LongPollingTracer *tracer) {
    if (isStopped()) {
        AUTIL_LOG(INFO, "long polling has been stopped");
        return;
    }
    auto expireTime = getExpireTime(closure->_queueTime);
    if (expireTime < 0) {
        AUTIL_LOG(DEBUG, "expired request, skip hold");
        return;
    }
    const auto *response = closure->_response;
    auto *request = const_cast<protocol::ConsumptionRequest *>(closure->_request);
    auto newStartId = response->nextmsgid();
    request->set_startid(newStartId);
    const auto &partitionId = brokerPartition->getPartitionId();
    request->set_compressfilterregion(FilterUtil::compressFilterRegion(partitionId, request->filter()));
    closure->inQueueTimeTrigger.beginTrigger();
    closure->_longPollingEnqueueTimes++;
    auto maxMsgIdBefore = response->maxmsgid();
    auto maxMsgIdAfter = brokerPartition->addLongPolling(expireTime, closure, newStartId);
    if (maxMsgIdBefore < maxMsgIdAfter) { // maxMsgIdBefore > maxMsgIdAfter sendMessage will invoke requestPoll
        if (tracer) {
            tracer->set_maxmsgidbefore(maxMsgIdBefore);
            tracer->set_maxmsgidafter(maxMsgIdAfter);
            tracer->set_requestpolltime(TimeUtility::currentTime());
        }
        _transporter->requestPoll(brokerPartition);
    }
}

void LongPollingRequestHandler::addHoldRequest(const util::ConsumptionLogClosurePtr &closure) {
    if (isStopped()) {
        AUTIL_LOG(INFO, "long polling has been stopped");
        return;
    }
    auto expireTime = getHoldExpireTime(closure->_queueTime);
    if (expireTime < 0) {
        AUTIL_LOG(DEBUG, "expired request, skip hold");
        return;
    }

    ScopedLock lock(_holdQueueMutex);
    _holdRequestQueue.emplace_back(expireTime, closure);
}

void LongPollingRequestHandler::registeCallBackBeforeSend(const BrokerPartitionPtr &brokerPartition,
                                                          ProductionLogClosure *closure) {
    if (isStopped()) {
        AUTIL_LOG(INFO, "long polling has been stopped");
        return;
    }

    const auto *response = closure->_response;
    auto enableLongPolling = isEnableLongPolling(brokerPartition);
    if (!enableLongPolling || !_clearPollingThreadPool) {
        AUTIL_LOG(DEBUG,
                  "skip active polling, enable [%d], thread [%p] acceptedmsg [%d] ",
                  enableLongPolling,
                  _clearPollingThreadPool.get(),
                  response->acceptedmsgcount());
        return;
    }
    closure->setBeforeLogCallBack([this, brokerPartition](protocol::MessageResponse *resp) {
        const protocol::ErrorInfo &ecInfo = resp->errorinfo();
        auto ec = ecInfo.errcode();
        AUTIL_LOG(DEBUG,
                  "send message, ec [%d] acceptedmsgcount [%u] maxMsgId [%ld]",
                  ec,
                  resp->acceptedmsgcount(),
                  resp->maxmsgid());
        if (ec == ERROR_NONE && resp->acceptedmsgcount() > 0) {
            doAfterSendMessage(brokerPartition, resp);
        }
    });
}

void LongPollingRequestHandler::doAfterSendMessage(BrokerPartitionPtr brokerPartition,
                                                   ::swift::protocol::MessageResponse *response) {
    auto enableLongPolling = isEnableLongPolling(brokerPartition);
    if (isStopped() || !enableLongPolling || !_clearPollingThreadPool) {
        return;
    }
    auto *item = new RequestPollingWorkItem(
        brokerPartition, _transporter, response->compresspayload(), response->maxmsgid(), response->donetimestamp());
    auto error = _clearPollingThreadPool->pushWorkItem(item, false);
    if (ThreadPool::ERROR_NONE != error) {
        AUTIL_LOG(WARN, "add work item failed, error [%d]", error);
        delete item;
    }
}

void LongPollingRequestHandler::stop() {
    _stopped = true;
    if (_timeoutCheckThread) {
        _timeoutCheckThread->stop();
    }
    if (_clearPollingThreadPool) {
        _clearPollingThreadPool->stop();
    }
}

} // end namespace service
} // end namespace swift
