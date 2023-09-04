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
#include "master/ServiceAdapter.h"
#include "carbon/Log.h"
#include "master/ServiceNodeManager.h"
#include "master/Flag.h"
#include "monitor/MonitorUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

namespace carbon {

CARBON_LOG_SETUP(master, ServiceAdapter);

#define LOG_PREFIX _id.c_str()

ServiceAdapter::ServiceAdapter(const std::string &id)
    : _id(id)
{
    _publishSwitch = true;
    _hasGetNode = false;
    _status = SS_PUBLISHED;
}

ServiceAdapter::~ServiceAdapter() {
}

bool ServiceAdapter::sync(const set<ServiceNode> &localNodes,
                          set<ServiceNode> *remoteNodes)
{
    REPORT_USED_TIME(_id, METRIC_SERVICE_SYNC_TIME);
    if (_publishSwitch) {
        _status = SS_PUBLISHED;
    } else {
        if (_status == SS_PUBLISHED) {
            _status = SS_UNPUBLISHING;
        }
        if (_status == SS_UNPUBLISHING && !_hasGetNode) {
            _status = SS_UNPUBLISHED;
            return true;
        }
    }
    
    assert(remoteNodes);
    remoteNodes->clear();

    ServiceNodeMap remoteNodesMap;
    bool succ = getNodes(&remoteNodesMap);
    if (!succ) {
        CARBON_PREFIX_LOG(ERROR, "get nodes failed.");
        return false;
    }
    _hasGetNode = true;

    CARBON_PREFIX_LOG(DEBUG, "remote service nodes size:[%zd].",
               remoteNodesMap.size());
    clearDelayDeletedNodes(remoteNodesMap);
    extractNodes(remoteNodesMap, remoteNodes);

    ServiceNodeMap localNodesMap;
    ServiceNodeMap needAddNodesMap;
    ServiceNodeMap needDelNodesMap;
    ServiceNodeMap needUpdateNodesMap;

    arrangeNodes(localNodes, &localNodesMap);

    if (_status == SS_UNPUBLISHING) {
        if (remoteNodesMap.size() == 0 || master::Flag::isSilentUnpublish(getType())) {
            _status = SS_UNPUBLISHED;
            return true;
        }
        needDelNodesMap = remoteNodesMap;
    } else {
        diffNodes(localNodesMap, remoteNodesMap, &needAddNodesMap,
                  &needDelNodesMap, &needUpdateNodesMap);
    }

    CARBON_PREFIX_LOG(DEBUG, "localNodes:[%s], remoteNodes:[%s], needAddNodes:[%s], "
               "needDelNodes:[%s], needUpdateNodes:[%s].",
               ToJsonString(localNodesMap, true).c_str(),
               ToJsonString(remoteNodesMap, true).c_str(),
               ToJsonString(needAddNodesMap, true).c_str(),
               ToJsonString(needDelNodesMap, true).c_str(),
               ToJsonString(needUpdateNodesMap, true).c_str());
    
    addNodes(needAddNodesMap);
    delNodes(needDelNodesMap);
    updateNodes(needUpdateNodesMap);
    return true;
}

void ServiceAdapter::unPublish() {
    _publishSwitch = false;
}

bool ServiceAdapter::isUnPublished() {
    return _status == SS_UNPUBLISHED;
}

void ServiceAdapter::diffNodes(const ServiceNodeMap &localNodes,
                               const ServiceNodeMap &remoteNodes,
                               ServiceNodeMap *needAddNodes,
                               ServiceNodeMap *needDelNodes,
                               ServiceNodeMap *needUpdateNodes)
{
    assert(needAddNodes && needDelNodes && needUpdateNodes);
    
    needUpdateNodes->clear();
    needAddNodes->clear();
    needDelNodes->clear();
    
    ServiceNodeMap::const_iterator localIt = localNodes.begin();
    ServiceNodeMap::const_iterator remoteIt = remoteNodes.begin();
    for (;localIt != localNodes.end() && remoteIt != remoteNodes.end();) {
        const nodespec_t &localSpec = localIt->first;
        const nodespec_t &remoteSpec = remoteIt->first;
        if (localSpec < remoteSpec) {
            (*needAddNodes)[localSpec] = localIt->second;
            localIt++;
        } else if (remoteSpec < localSpec) {
            (*needDelNodes)[remoteSpec] = remoteIt->second;
            remoteIt++;
        } else {
            (*needUpdateNodes)[localSpec] = localIt->second;
            localIt++;
            remoteIt++;
        }
    }

    for (; localIt != localNodes.end(); localIt++) {
        (*needAddNodes)[localIt->first] = localIt->second;
    }

    for (; remoteIt != remoteNodes.end(); remoteIt++) {
        (*needDelNodes)[remoteIt->first] = remoteIt->second;
    }
}

bool ServiceAdapter::getNodes(ServiceNodeMap *nodes) {
    bool ret = doGetNodes(nodes);
    if (!ret) {
        REPORT_METRIC(_id, METRIC_OPERATE_SERVICENODE_FAILED_TIMES, 1);
    }
    return ret;
}

void ServiceAdapter::updateNodes(const ServiceNodeMap &nodes) {
    if (nodes.size() == 0) {
        return;
    }
    for (const auto &kv : nodes) {
        _firstDeleteTime.erase(kv.first);
    }
    if (!doUpdateNodes(nodes)) {
        CARBON_PREFIX_LOG(ERROR, "update nodes failed");
        REPORT_METRIC(_id, METRIC_OPERATE_SERVICENODE_FAILED_TIMES, 1);
    }
}

void ServiceAdapter::addNodes(const ServiceNodeMap &nodes) {
    if (nodes.size() == 0) {
        return;
    }
    for (const auto &kv : nodes) {
        _firstDeleteTime.erase(kv.first);
    }
    CARBON_PREFIX_LOG(INFO, "add service nodes, nodes count:[%zd].",
               nodes.size());

    if (!doAddNodes(nodes)) {
        CARBON_PREFIX_LOG(ERROR, "add nodes failed");
        REPORT_METRIC(_id, METRIC_OPERATE_SERVICENODE_FAILED_TIMES, 1);
    }
}

void ServiceAdapter::delNodes(const ServiceNodeMap &nodes) {
    ServiceNodeMap toDelNodes;
    auto curTime = TimeUtility::currentTime();
    for (const auto &kv : nodes) {
        if (_firstDeleteTime.find(kv.first) == _firstDeleteTime.end()) {
            _firstDeleteTime[kv.first] = curTime;
        }

        if (curTime - _firstDeleteTime[kv.first] >= (_config.deleteDelay * 1000 * 1000)) {
            toDelNodes.insert(kv);
        }
    }
    
    if (toDelNodes.size() == 0) {
        return;
    }
    CARBON_PREFIX_LOG(INFO, "delete service nodes, nodes count:[%zd], actual delete count:[%zd],.",
                      nodes.size(), toDelNodes.size());
    if (!doDelNodes(toDelNodes)) {
        CARBON_PREFIX_LOG(ERROR, "del nodes failed.");
        REPORT_METRIC(_id, METRIC_OPERATE_SERVICENODE_FAILED_TIMES, 1);
    }

}

void ServiceAdapter::intersection(const ServiceNodeMap &local,
                                  const ServiceNodeMap &remote,
                                  ServiceNodeMap *result)
{
    ServiceNodeMap::const_iterator localIt = local.begin();
    ServiceNodeMap::const_iterator remoteIt = remote.begin();
    for (; localIt != local.end() && remoteIt != remote.end();) {
        if (localIt->first < remoteIt->first) {
            localIt++;
        } else if (remoteIt->first < localIt->first) {
            remoteIt++;
        } else {
            ServiceNode node = remoteIt->second;
            node.nodeId = localIt->second.nodeId;
            result->insert(make_pair(localIt->first, node));
            localIt++;
            remoteIt++;
        }
    }
}

void ServiceAdapter::extractNodes(const ServiceNodeMap &remoteNodesMap,
                                  ServiceNodeSet *remoteNodes)
{
    remoteNodes->clear();
    for (ServiceNodeMap::const_iterator it = remoteNodesMap.begin();
         it != remoteNodesMap.end(); it++)
    {
        remoteNodes->insert(it->second);
    }
}

void ServiceAdapter::arrangeNodes(const ServiceNodeSet &localNodes,
                                  ServiceNodeMap *localNodesMap)
{
    for (ServiceNodeSet::const_iterator it = localNodes.begin();
         it != localNodes.end(); it++)
    {
        nodespec_t nodeSpec = getServiceNodeSpec(*it);
        (*localNodesMap)[nodeSpec] = *it;
    }
}

nodespec_t ServiceAdapter::getServiceNodeSpec(const ServiceNode &node) const {
    return node.ip;
}

void ServiceAdapter::clearDelayDeletedNodes(const ServiceNodeMap &remoteServiceNodes) {
    for (auto it = _firstDeleteTime.begin(); it != _firstDeleteTime.end();) {
        if (remoteServiceNodes.find(it->first) == remoteServiceNodes.end())  {
            _firstDeleteTime.erase(it++);
        } else {
            it++;
        }
    }
}

#undef LOG_PREFIX

}

