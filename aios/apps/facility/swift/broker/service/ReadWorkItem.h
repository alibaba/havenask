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
class LongPollingRequestHandler;
class TransporterImpl;

class ReadWorkItem : public autil::WorkItem {
public:
    ReadWorkItem(BrokerPartitionPtr &brokerPartition,
                 swift::util::ConsumptionLogClosurePtr &closure,
                 LongPollingRequestHandler *requestTimeoutChecker,
                 TransporterImpl *transporter);
    ~ReadWorkItem();

private:
    ReadWorkItem(const ReadWorkItem &);
    ReadWorkItem &operator=(const ReadWorkItem &);

public:
    void process() override;
    void setActiveMsgId(int64_t activeMsgId) { _activeMsgId = activeMsgId; }

private:
    BrokerPartitionPtr _brokerPartition;
    swift::util::ConsumptionLogClosurePtr _closure;
    LongPollingRequestHandler *_longPollingRequestHandler;
    TransporterImpl *_transporter;
    int64_t _activeMsgId;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ReadWorkItem);

} // namespace service
} // namespace swift
