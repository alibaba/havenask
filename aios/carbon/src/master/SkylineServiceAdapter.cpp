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
#include "master/SkylineServiceAdapter.h"
#include "autil/StringUtil.h"
#include "autil/EnvUtil.h"

BEGIN_CARBON_NAMESPACE(master);

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace common;

CARBON_LOG_SETUP(master, SkylineServiceAdapter);

#define LOG_PREFIX _id.c_str()

#define DEFAULT_SKYLINE_USER "carbon"
#define DEFAULT_SKYLINE_USER_KEY "t5nkjhOU7qe2AYW9"
#define DEFAULT_APP_USE_TYPE APP_USE_TYPE_PUB

SkylineServiceAdapter::SkylineServiceAdapter(
        const ServiceNodeManagerPtr &serviceNodeManagerPtr,
        const string &id)
    : ServiceAdapter(id)
{
    _serviceNodeManagerPtr = serviceNodeManagerPtr;
}

SkylineServiceAdapter::~SkylineServiceAdapter() {
}

bool SkylineServiceAdapter::doInit(const ServiceConfig &config) {
    try {
        FromJsonString(_skylineServiceConfig, config.configStr);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_PREFIX_LOG(ERROR, "skyline service config from json fail, exception[%s]",
               e.what());
        return  false;
    }
    if (_skylineServiceConfig.host.empty() || _skylineServiceConfig.group.empty() ||
        _skylineServiceConfig.buffGroup.empty()) {
        CARBON_PREFIX_LOG(ERROR, "invalid skyline config [%s]", ToJsonString(_skylineServiceConfig).c_str());
        return false;
    }
    SkylineConf conf = {
        _skylineServiceConfig.host,
        _skylineServiceConfig.key.empty() ? DEFAULT_SKYLINE_USER : _skylineServiceConfig.key,
        _skylineServiceConfig.secret.empty() ? DEFAULT_SKYLINE_USER_KEY : _skylineServiceConfig.secret,
    };
    _skylineAPI.reset(new SkylineAPI(HttpClientPtr(new HttpClient()), conf));
    CARBON_PREFIX_LOG(INFO, "parse skyline service config success.");
    return true;
}

bool SkylineServiceAdapter::doAddNodes(const ServiceNodeMap &nodes) {
    if (nodes.empty()) return true;
    std::vector<std::string> ips;
    std::transform(nodes.begin(), nodes.end(), std::back_inserter(ips),
            [](const std::pair<nodespec_t, ServiceNode>& it) { return it.second.ip; });
    // NOTE: moveServerGroup doesn't check if server appUseType updated success
    if (!_skylineAPI->moveServerGroup(ips, _skylineServiceConfig.group, false, _skylineServiceConfig.appUseType)) {
        CARBON_PREFIX_LOG(ERROR, "move host to group failed: %s, %s", _skylineServiceConfig.group.c_str(), ToJsonString(ips, true).c_str());
        return false;
    }
    if (!_serviceNodeManagerPtr->addNodes(nodes)) {
        CARBON_PREFIX_LOG(ERROR, "add nodes to ServiceNodeManager failed.");
        return false;
    }
    CARBON_PREFIX_LOG(DEBUG, "move nodes size %lu to group [%s] success", nodes.size(), _skylineServiceConfig.group.c_str());
    return true;
}

bool SkylineServiceAdapter::doDelNodes(const ServiceNodeMap &nodes) {
    if (nodes.empty()) return true;
    std::vector<std::string> ips;
    std::transform(nodes.begin(), nodes.end(), std::back_inserter(ips),
            [](const std::pair<nodespec_t, ServiceNode>& it) { return it.second.ip; });
    if (!_skylineAPI->moveServerGroup(ips, _skylineServiceConfig.buffGroup, true, getReturnAppUseType())) {
        CARBON_PREFIX_LOG(ERROR, "move host to group failed: %s, %s", _skylineServiceConfig.buffGroup.c_str(), ToJsonString(ips, true).c_str());
        return false;
    }
    if (!_serviceNodeManagerPtr->delNodes(nodes)) {
        CARBON_PREFIX_LOG(ERROR, "del nodes from serviceNodeManager failed");
        return false;
    }
    CARBON_PREFIX_LOG(DEBUG, "move nodes size %lu to buffer group [%s] success", nodes.size(), _skylineServiceConfig.buffGroup.c_str());
    return true;
}

std::string SkylineServiceAdapter::getReturnAppUseType() const {
    if (!_skylineServiceConfig.returnAppUseType.empty()) {
        return _skylineServiceConfig.returnAppUseType;
    }
    return autil::EnvUtil::getEnv("SKYLINE_RETURN_APPUSETYPE");
}

bool SkylineServiceAdapter::doGetNodes(ServiceNodeMap *nodes) {
    std::vector<Server> servers;
    if (!_skylineAPI->queryGroupServers(_skylineServiceConfig.group, &servers)) {
        CARBON_PREFIX_LOG(ERROR, "get ip list from skyline group [%s] failed", _skylineServiceConfig.group.c_str());
        return false;
    }
    for (const auto& i : servers) {
        ServiceNode node;
        node.ip = i.ip;
        node.status = SN_AVAILABLE; // don't care of appUseType and working status
        (*nodes)[i.ip] = node;
    }
    filterNodes(nodes);
    return true;
}

void SkylineServiceAdapter::filterNodes(ServiceNodeMap *nodes) {
    ServiceNodeMap remote = *nodes;
    const ServiceNodeMap &local = _serviceNodeManagerPtr->getNodes();
    nodes->clear();
    intersection(local, remote, nodes);
}

bool SkylineServiceAdapter::recover() {
    if (_serviceNodeManagerPtr == NULL) {
        CARBON_LOG(ERROR, "recover service adapter failed, "
                   "cause ServiceNodeManager is NULL, adapterId: %s",
                   _id.c_str());
        return false;
    }

    return _serviceNodeManagerPtr->recover();
}

#undef LOG_PREFIX

END_CARBON_NAMESPACE(master);
