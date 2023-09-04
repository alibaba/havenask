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

#include <stdint.h>

#include "autil/Log.h"
#include "autil/WorkItem.h"
#include "swift/common/Common.h"
#include "swift/log/BrokerLogClosure.h"

namespace swift {
namespace service {
class TransporterImpl;

class RequestPollingWorkItem : public autil::WorkItem {
public:
    RequestPollingWorkItem(BrokerPartitionPtr &brokerPartition,
                           TransporterImpl *transporter,
                           uint64_t compressPayload,
                           int64_t maxMsgId,
                           int64_t doneTime);
    ~RequestPollingWorkItem();

private:
    RequestPollingWorkItem(const RequestPollingWorkItem &);
    RequestPollingWorkItem &operator=(const RequestPollingWorkItem &);

public:
    void process() override;

private:
    BrokerPartitionPtr _brokerPartition;
    TransporterImpl *_transporter;
    uint64_t _compressPayload;
    int64_t _maxMsgId;
    int64_t _doneTime;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(RequestPollingWorkItem);

} // namespace service
} // namespace swift
