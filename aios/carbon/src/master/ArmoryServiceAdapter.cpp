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
#include "master/ArmoryServiceAdapter.h"
#include "autil/StringUtil.h"
#include "autil/EnvUtil.h"

BEGIN_CARBON_NAMESPACE(master);

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace common;

CARBON_LOG_SETUP(master, ArmoryServiceAdapter);

#define LOG_PREFIX _id.c_str()

#define DEFAULT_ARMORY_USER "hippo"
#define DEFAULT_ARMORY_USER_KEY "0nVumlpnWZ8xuh2CsA2miA=="
#define DEFAULT_APP_USE_TYPE "PUBLISH"

ArmoryServiceAdapter::ArmoryServiceAdapter(
        const ServiceNodeManagerPtr &serviceNodeManagerPtr,
        const string &id)
    : ServiceAdapter(id)
{
    _serviceNodeManagerPtr = serviceNodeManagerPtr;
}

ArmoryServiceAdapter::~ArmoryServiceAdapter() {
}

bool ArmoryServiceAdapter::doInit(const ServiceConfig &config) {
    try {
        FromJsonString(_armoryServiceConfig, config.configStr);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_PREFIX_LOG(ERROR, "armory service config from json fail, exception[%s]",
               e.what());
        return  false;
    }
    if (_armoryServiceConfig.host.empty() || _armoryServiceConfig.group.empty() ||
        _armoryServiceConfig.buffGroup.empty()) {
        CARBON_PREFIX_LOG(ERROR, "invalid armory config [%s]", ToJsonString(_armoryServiceConfig).c_str());
        return false;
    }
    ArmoryConf conf = {
        _armoryServiceConfig.host,
        DEFAULT_ARMORY_USER,
        DEFAULT_ARMORY_USER_KEY,
        _armoryServiceConfig.owner
    };
    _armoryAPI.reset(new ArmoryAPI(HttpClientPtr(new HttpClient()), conf));
    CARBON_PREFIX_LOG(INFO, "parse armory service config success.");
    return true;
}

bool ArmoryServiceAdapter::doAddNodes(const ServiceNodeMap &nodes) {
    if (nodes.empty()) return true;
    std::vector<std::string> ips;
    std::transform(nodes.begin(), nodes.end(), std::back_inserter(ips),
            [](const std::pair<nodespec_t, ServiceNode>& it) { return it.second.ip; });
    ArmoryResult<IPItem> result;
    if (!_armoryAPI->getIPIDs(ips, result) || result.result.empty()) {
        CARBON_PREFIX_LOG(ERROR, "get device ids from armory failed [%s]", ToJsonString(ips).c_str());
        return false;
    }
    std::vector<uint64_t> ids;
    std::transform(result.result.begin(), result.result.end(), std::back_inserter(ids),
            [](const IPItem& item) { return item.id; });
    if (!_armoryAPI->moveHostGroup(ids, _armoryServiceConfig.group, false)) {
        CARBON_PREFIX_LOG(ERROR, "move ips to armory group [%s] failed, size [%lu]",
                _armoryServiceConfig.group.c_str(), ids.size());
        return false;
    }
    CARBON_PREFIX_LOG(DEBUG, "success to move ips to armory group [%s:%lu]",
            _armoryServiceConfig.group.c_str(), ids.size());
    std::string appUseType = _armoryServiceConfig.appUseType.empty() ? DEFAULT_APP_USE_TYPE : 
        _armoryServiceConfig.appUseType;
    // better to remove these failed nodes ? 
    for (const auto& id : ids) {
        if (!_armoryAPI->updateHostInfo(id, appUseType)) { 
            CARBON_PREFIX_LOG(WARN, "update armory host [%lu] appUseType [%s] failed", id, appUseType.c_str());
        }
    }
    // TODO: if persist all nodes including failed, these failed nodes will remain the failed status
    if (!_serviceNodeManagerPtr->addNodes(nodes)) {
        CARBON_PREFIX_LOG(ERROR, "add nodes to ServiceNodeManager failed.");
        return false;
    }
    return true;
}

bool ArmoryServiceAdapter::doDelNodes(const ServiceNodeMap &nodes) {
    if (nodes.empty()) return true;
    std::vector<std::string> ips;
    std::transform(nodes.begin(), nodes.end(), std::back_inserter(ips),
            [](const std::pair<nodespec_t, ServiceNode>& it) { return it.second.ip; });
    ArmoryResult<IPItem> result;
    if (!_armoryAPI->getIPIDs(ips, result) || result.result.empty()) {
        CARBON_PREFIX_LOG(ERROR, "get device ids from armory failed [%s]", ToJsonString(ips).c_str());
        return false;
    }
    std::vector<uint64_t> ids;
    std::transform(result.result.begin(), result.result.end(), std::back_inserter(ids),
            [](const IPItem& item) { return item.id; });

    if (!_armoryAPI->moveHostGroup(ids, _armoryServiceConfig.buffGroup, true)) {
        CARBON_PREFIX_LOG(ERROR, "move nodes to buffer group [%s] failed", _armoryServiceConfig.buffGroup.c_str());
        return false;
    }
    std::string appUseType = getReturnAppUseType();
    if (!appUseType.empty()) {
        for (const auto& id : ids) {
            if (!_armoryAPI->updateHostInfo(id, appUseType)) { 
                CARBON_PREFIX_LOG(WARN, "update armory host [%lu] appUseType [%s] failed", id, appUseType.c_str());
            }
        }
    }
    if (!_serviceNodeManagerPtr->delNodes(nodes)) {
        CARBON_PREFIX_LOG(ERROR, "del nodes from serviceNodeManager failed");
        return false;
    }
    CARBON_PREFIX_LOG(DEBUG, "move nodes size %lu to buffer group [%s] success", nodes.size(), 
            _armoryServiceConfig.buffGroup.c_str());
    return true;
}

std::string ArmoryServiceAdapter::getReturnAppUseType() const {
    if (!_armoryServiceConfig.returnAppUseType.empty()) {
        return _armoryServiceConfig.returnAppUseType;
    }
    return  autil::EnvUtil::getEnv("ARMORY_RETURN_APPUSETYPE");
}

bool ArmoryServiceAdapter::doGetNodes(ServiceNodeMap *nodes) {
    ArmoryResult<IPItem> result;
    if (!_armoryAPI->getGroupIPList(_armoryServiceConfig.group, result)) {
        CARBON_PREFIX_LOG(ERROR, "get ip list from armory group [%s] failed", _armoryServiceConfig.group.c_str());
        return false;
    }
    for (const auto& i : result.result) {
        ServiceNode node;
        node.ip = i.dns_ip;
        node.status = SN_AVAILABLE;
        (*nodes)[i.dns_ip] = node;
    }
    filterNodes(nodes);
    CARBON_PREFIX_LOG(DEBUG, "get armory nodes:[%s].", ToJsonString(*nodes).c_str());
    return true;
}

void ArmoryServiceAdapter::filterNodes(ServiceNodeMap *nodes) {
    ServiceNodeMap remote = *nodes;
    const ServiceNodeMap &local = _serviceNodeManagerPtr->getNodes();
    nodes->clear();
    intersection(local, remote, nodes);
}

bool ArmoryServiceAdapter::recover() {
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
