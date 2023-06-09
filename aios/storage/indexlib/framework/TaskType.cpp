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
#include "indexlib/framework/TaskType.h"

namespace indexlibv2::framework {

const char* TaskTypeToString(TaskType type)
{
    switch (type) {
    case TaskType::TT_REPORT_METRICS:
        return "REPORT_METRICS";
    case TaskType::TT_CLEAN_RESOURCE:
        return "CLEAN_RESOURCE";
    case TaskType::TT_CLEAN_ON_DISK_INDEX:
        return "CLEAN_ON_DISK_INDEX";
    case TaskType::TT_INTERVAL_DUMP:
        return "INTERVAL_DUMP";
    case TaskType::TT_RENEW_FENCE_LEASE:
        return "RENEW_FENCE_LEASE";
    case TaskType::TT_ASYNC_DUMP:
        return "ASYNC_DUMP";
    case TaskType::TT_CHECK_SECONDARY_INDEX:
        return "CHECK_SECONDARY_INDEX";
    case TaskType::TT_SUBSCRIBE_SECONDARY_INDEX:
        return "SUBSCRIBE_SECONDARY_INDEX";
    case TaskType::TT_SYNC_REALTIME_INDEX:
        return "SYNC_REALTIME_INDEX";
    case TaskType::TT_CALCULATE_METRICS:
        return "CALCULATE_METRICS";
    case TaskType::TT_MEMORY_RECLAIM:
        return "MEMORY_RECLAIM";
    case TaskType::TT_MERGE_VERSION:
        return "MERGE_VERSION";
    case TaskType::TT_UNKOWN:
        return "UNKOWN";

    default:
        return "UNKNOWN_TASK_TYPE";
    }
}

} // namespace indexlibv2::framework
