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

namespace indexlibv2::framework {

enum class TaskType {
    TT_REPORT_METRICS,
    TT_CLEAN_RESOURCE,
    TT_CLEAN_ON_DISK_INDEX,
    TT_INTERVAL_DUMP,
    TT_RENEW_FENCE_LEASE,
    TT_ASYNC_DUMP,
    TT_CHECK_SECONDARY_INDEX,
    TT_SUBSCRIBE_SECONDARY_INDEX,
    TT_SYNC_REALTIME_INDEX,
    TT_CALCULATE_METRICS,
    TT_MEMORY_RECLAIM,
    TT_MERGE_VERSION,
    TT_SUBSCRIBE_REMOTE_INDEX,
    TT_UNKOWN = 255
};

extern const char* TaskTypeToString(TaskType type);

} // namespace indexlibv2::framework
