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

#include <stddef.h>
#include <string>

#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace isearch {
namespace service {

struct QrsSessionSearchRequest {
    QrsSessionSearchRequest(const std::string &requestStr_ = "",
                            const std::string &clientIp_ = "",
                            autil::mem_pool::Pool *pool_ = NULL,
                            const monitor::SessionMetricsCollectorPtr &metricsCollector =
                            monitor::SessionMetricsCollectorPtr())
        : requestStr(requestStr_)
        , clientIp(clientIp_)
        , pool(pool_)
        , metricsCollectorPtr(metricsCollector)
    {
    }
    std::string requestStr;
    std::string clientIp;
    autil::mem_pool::Pool *pool;
    monitor::SessionMetricsCollectorPtr metricsCollectorPtr;
    SessionSrcType sessionSrcType = SESSION_SRC_UNKNOWN;
};

} // namespace service
} // namespace isearch

