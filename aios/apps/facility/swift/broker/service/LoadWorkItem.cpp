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
#include "swift/broker/service/LoadWorkItem.h"

#include <string>

#include "autil/TimeUtility.h"
#include "swift/broker/service/TopicPartitionSupervisor.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace service {
AUTIL_LOG_SETUP(swift, LoadWorkItem);
using namespace autil;
using namespace swift::protocol;

LoadWorkItem::LoadWorkItem(TopicPartitionSupervisor *tpSupervisor, const TaskInfo &taskInfo) {
    _tpSupervisor = tpSupervisor;
    _taskInfo = taskInfo;
}

LoadWorkItem::~LoadWorkItem() {}

void LoadWorkItem::process() {
    AUTIL_LOG(INFO, "begin process load partition [%s]", _taskInfo.ShortDebugString().c_str());
    protocol::ErrorCode ec = _tpSupervisor->loadBrokerPartition(_taskInfo);
    if (ec != protocol::ERROR_NONE) {
        AUTIL_LOG(ERROR, "failed to load partition [%s].", _taskInfo.ShortDebugString().c_str());
    } else {
        AUTIL_LOG(INFO, "load partition [%s] success!", _taskInfo.ShortDebugString().c_str());
    }
}

} // namespace service
} // namespace swift
