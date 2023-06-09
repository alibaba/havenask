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
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/config/ServiceDegradationConfig.h"
#include "aios/network/gig/multi_call/agent/QueryInfo.h"

namespace isearch {
namespace common {
class Request;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace service {

class ServiceDegrade
{
public:
    ServiceDegrade(const config::ServiceDegradationConfig &config);
    ~ServiceDegrade();
private:
    ServiceDegrade(const ServiceDegrade &);
    ServiceDegrade& operator=(const ServiceDegrade &);
public:
    bool needDegade(uint32_t workerQueueSize);
    bool updateRequest(common::Request *request,
                       const multi_call::QueryInfoPtr &queryInfo);
    config::ServiceDegradationConfig getServiceDegradationConfig() const {
        return _config;
    };
private:
    config::ServiceDegradationConfig _config;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ServiceDegrade> ServiceDegradePtr;

} // namespace service
} // namespace isearch

