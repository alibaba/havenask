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
#include "aios/network/gig/multi_call/subscribe/VIPSubscribeService.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, VIPSubscribeService);

void VIPSubscribeService::destroy() {}

VIPSubscribeService::VIPSubscribeService(const VipConfig &config) {}

VIPSubscribeService::~VIPSubscribeService() {}

bool VIPSubscribeService::init() {
    return false;
}

bool VIPSubscribeService::clusterInfoNeedUpdate() {
    return false;
}

bool VIPSubscribeService::getClusterInfoMap(TopoNodeVec &topoNodeVec,
        HeartbeatSpecVec &heartbeatSpecs)
{
    return false;
}

bool VIPSubscribeService::addSubscribe(const std::vector<std::string> &names) {
    return false;
}

bool VIPSubscribeService::addSubscribeDomainConfig(
        const std::vector<VIPDomainConfig> &vipDomains)
{
    return false;
}

bool VIPSubscribeService::deleteSubscribe(const std::vector<std::string> &names) {
    return false;
}

bool VIPSubscribeService::deleteSubscribeDomainConfig(
        const std::vector<VIPDomainConfig> &vipDomains)
{
    return false;
}

bool VIPSubscribeService::queryAllIp(const string &clusterName,
                                     std::any hosts) const
{
    return false;
}

} // namespace multi_call
