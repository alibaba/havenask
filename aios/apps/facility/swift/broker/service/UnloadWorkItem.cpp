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
#include "swift/broker/service/UnloadWorkItem.h"

#include <string>

#include "autil/TimeUtility.h"
#include "swift/broker/service/TopicPartitionSupervisor.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace service {
AUTIL_LOG_SETUP(swift, UnloadWorkItem);
using namespace autil;
using namespace swift::protocol;

UnloadWorkItem::UnloadWorkItem(TopicPartitionSupervisor *tpSupervisor, const PartitionId &partId) {
    _tpSupervisor = tpSupervisor;
    _partId = partId;
}

UnloadWorkItem::~UnloadWorkItem() {}

void UnloadWorkItem::process() {
    AUTIL_LOG(INFO, "begin process unload partition [%s]", _partId.ShortDebugString().c_str());
    protocol::ErrorCode ec = _tpSupervisor->unLoadBrokerPartition(_partId);
    if (ec != protocol::ERROR_NONE) {
        AUTIL_LOG(ERROR, "failed to unload partition [%s].", _partId.ShortDebugString().c_str());
    } else {
        AUTIL_LOG(INFO, "unload partition [%s] success!", _partId.ShortDebugString().c_str());
    }
}

} // namespace service
} // namespace swift
