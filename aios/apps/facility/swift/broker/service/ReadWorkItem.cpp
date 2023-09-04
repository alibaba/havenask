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
#include "swift/broker/service/ReadWorkItem.h"

#include <memory>
#include <string>

#include "autil/TimeUtility.h"
#include "swift/broker/service/BrokerPartition.h"
#include "swift/broker/service/LongPollingRequestHandler.h"
#include "swift/broker/storage/BrokerResponseError.h"
#include "swift/broker/storage/MessageGroup.h"
#include "swift/config/ConfigDefine.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/IpMapper.h"

namespace swift {
namespace service {

using namespace autil;
using namespace swift::protocol;
using namespace swift::util;
using namespace swift::storage;

AUTIL_LOG_SETUP(swift, ReadWorkItem);

ReadWorkItem::ReadWorkItem(BrokerPartitionPtr &brokerPartition,
                           ConsumptionLogClosurePtr &closure,
                           LongPollingRequestHandler *noDataRequestHandler,
                           TransporterImpl *transporter)
    : _brokerPartition(brokerPartition)
    , _closure(closure)
    , _longPollingRequestHandler(noDataRequestHandler)
    , _transporter(transporter)
    , _activeMsgId(-1) {}

ReadWorkItem::~ReadWorkItem() {}

void ReadWorkItem::process() {
    const std::string &ipPort = _closure->getClientAddress();
    ErrorCode ec = _brokerPartition->getMessage(_closure->_request, _closure->_response, &ipPort);
    // must using  response->mutable_errorinfo() address here
    // if response swap in getMessage, old errorInfo address invalid
    setBrokerResponseError(_closure->_response->mutable_errorinfo(), ec);
    if (!_longPollingRequestHandler) {
        AUTIL_LOG(WARN, "request timeout checker is nullptr");
        return;
    }
    if (!_brokerPartition->isEnableLongPolling()) {
        if (ERROR_BROKER_BUSY == ec || ERROR_BROKER_NO_DATA == ec) {
            _longPollingRequestHandler->addHoldRequest(_closure);
        }
    } else {
        const auto *request = _closure->_request;
        auto *response = _closure->_response;
        bool msgEmpty =
            (ec == ERROR_NONE) && response && response->totalmsgcount() == 0 && (!request->has_committedcheckpoint());
        bool noData = ERROR_BROKER_NO_DATA == ec || msgEmpty;
        bool needLongPolling =
            noData && _closure->_longPollingEnqueueTimes < config::DEFAULT_MAX_LONG_POLLING_RETRY_TIMES;
        if (needLongPolling) {
            auto *tracer = response->mutable_longpollinginfo()->add_longpollingtracer();
            tracer->set_startid(request->startid());
            tracer->set_nextmsgid(response->nextmsgid());
            tracer->set_totalmsgcount(response->totalmsgcount());
            tracer->set_readempty(msgEmpty);
            tracer->set_activemsgid(_activeMsgId);
            _longPollingRequestHandler->addLongPollingRequest(_brokerPartition, _closure, tracer);
        } else {
            monitor::LongPollingMetricsCollector collector;
            monitor::PartitionMetricsCollector partMetricCollector;
            _brokerPartition->collectMetrics(partMetricCollector);
            auto curTime = autil::TimeUtility::currentTime();
            collector.readMsgToCurTimeInterval = curTime - partMetricCollector.lastReadMsgTimeStamp;
            if (_closure->_longPollingEnqueueTimes > 0) {
                collector.longPollingLatencyAcc = _closure->_longPollingLatencyAcc;
                collector.longPollingEnqueueTimes = _closure->_longPollingEnqueueTimes;
                collector.readRequestToCurTimeInterval = curTime - _closure->_beginTime;
                collector.lastAcceptedMsgToCurTimeInterval = curTime - partMetricCollector.lastAcceptedMsgTimeStamp;
            }
            _brokerPartition->reportLongPollingMetrics(collector);
        }
    }
}

} // namespace service
} // namespace swift
