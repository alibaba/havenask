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
#include "ha3/service/ServiceDegrade.h"

#include <memory>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Request.h"
#include "ha3/config/ServiceDegradationConfig.h"
#include "aios/network/gig/multi_call/agent/QueryInfo.h"
#include "aios/network/gig/multi_call/common/ControllerParam.h"

using namespace autil;

namespace isearch {
namespace service {
AUTIL_LOG_SETUP(ha3, ServiceDegrade);

ServiceDegrade::ServiceDegrade(const config::ServiceDegradationConfig &config) {
    _config = config;
}

ServiceDegrade::~ServiceDegrade() {
}

bool ServiceDegrade::updateRequest(common::Request *request,
                                   const multi_call::QueryInfoPtr &queryInfo)
{
    if (!queryInfo) {
        return false;
    }
    auto *configClause = request->getConfigClause();
    auto level = queryInfo->degradeLevel(multi_call::MAX_PERCENT);
    auto ret = level > multi_call::MIN_PERCENT;
    if (ret && _config.request.rankSize > 0) {
        configClause->setTotalRankSize(0);
    }
    if (ret && _config.request.rerankSize > 0) {
        configClause->setTotalRerankSize(0);
    }
    request->setDegradeLevel(level, _config.request.rankSize,
                             _config.request.rerankSize);
    return ret;
}

} // namespace service
} // namespace isearch
