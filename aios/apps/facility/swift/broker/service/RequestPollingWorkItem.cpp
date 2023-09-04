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
#include "swift/broker/service/RequestPollingWorkItem.h"

#include "autil/TimeUtility.h"
#include "swift/broker/service/BrokerPartition.h"
#include "swift/broker/service/TransporterImpl.h"
#include "swift/broker/storage/MessageGroup.h"
#include "swift/protocol/Common.pb.h"
#include "swift/util/IpMapper.h"

namespace swift {
namespace service {

using namespace autil;
using namespace swift::protocol;
using namespace swift::util;
using namespace swift::storage;

AUTIL_LOG_SETUP(swift, RequestPollingWorkItem);

RequestPollingWorkItem::RequestPollingWorkItem(BrokerPartitionPtr &brokerPartition,
                                               TransporterImpl *transporter,
                                               uint64_t compressPayload,
                                               int64_t maxMsgId,
                                               int64_t doneTime)
    : _brokerPartition(brokerPartition)
    , _transporter(transporter)
    , _compressPayload(compressPayload)
    , _maxMsgId(maxMsgId)
    , _doneTime(doneTime) {}

RequestPollingWorkItem::~RequestPollingWorkItem() {}

void RequestPollingWorkItem::process() {
    _transporter->requestPoll(_brokerPartition, _compressPayload, _maxMsgId, _doneTime);
}

} // namespace service
} // namespace swift
