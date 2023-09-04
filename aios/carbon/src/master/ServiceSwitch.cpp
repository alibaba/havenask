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
#include "master/ServiceSwitch.h"
#include "master/WorkerNode.h"
#include "master/SlotUtil.h"
#include "master/SignatureGenerator.h"
#include "monitor/MonitorUtil.h"
#include "carbon/Log.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, ServiceSwitch);

ServiceSwitch::ServiceSwitch(const std::string &id,
                             const SerializerCreatorPtr &serializerCreatorPtr, ServiceAdapterExtCreatorPtr extCreatorPtr)
{
    _id = id;
    _serviceAdapterCreatorPtr.reset(
            new ServiceAdapterCreator(serializerCreatorPtr, extCreatorPtr));
    _updated = false;
    _isWorking = false;
    _succeed = true;
}

ServiceSwitch::~ServiceSwitch() {
}

void ServiceSwitch::update(const vector<WorkerNodePtr> &workerNodes) {
    doUpdate(workerNodes);
    _updated = true;
}

void ServiceSwitch::doUpdate(const vector<WorkerNodePtr> &workerNodes) {
    map<nodeid_t, ServiceNode> serviceNodes;
    map<nodeid_t, ServiceNode> serviceNodesAll;
    for (size_t i = 0; i < workerNodes.size(); i++) {
        const WorkerNodePtr &workerNodePtr = workerNodes[i];
        if (workerNodePtr->isUnAssignedSlot()) {
            CARBON_LOG(INFO, "worker node has not assigned slots. do not publish. worker node:[%s].", workerNodePtr->getId().c_str());
            continue;
        }
        if (workerNodePtr->hasEmptySlotInfo()) {
            CARBON_LOG(WARN, "worker node has empty slot info, maybe be released, do not publish. worker node:[%s].", workerNodePtr->getId().c_str());
            continue;
        }
        string ip = workerNodePtr->getIp();
        nodeid_t nodeId = workerNodePtr->getId();
        ServiceNode serviceNode;
        serviceNode.nodeId = nodeId;
        serviceNode.ip = ip;
        serviceNode.metas = workerNodePtr->getHealthInfoMetas();
        serviceNodesAll.insert(make_pair(nodeId, serviceNode));
        if (!workerNodePtr->isOffline()) {
            serviceNodes.insert(make_pair(nodeId, serviceNode));
        }
    }

    ScopedLock lock(_lock);
    _serviceNodes.swap(serviceNodes);
    _serviceNodesAll.swap(serviceNodesAll);

    CARBON_LOG(DEBUG, "updated service nodes, count:[%zd], nodes:[%s].",
               _serviceNodes.size(), ToJsonString(_serviceNodes).c_str());
}

map<nodeid_t, ServiceInfo> ServiceSwitch::getServiceInfos() const {
    ScopedLock lock(_lock);
    return _serviceInfos;
}

void ServiceSwitch::updateConfigs(const vector<ServiceConfig> &serviceConfigs) {
    map<string, ServiceConfig> serviceConfigMap;
    for (size_t i = 0; i < serviceConfigs.size(); i++) {
        const ServiceConfig &serviceConfig = serviceConfigs[i];
        if (serviceConfig.masked) {
            CARBON_LOG(DEBUG, "skip no.%zd service config", i);
            continue;
        }
        string serviceAdapterId = genServiceAdapterId(serviceConfig);
        serviceConfigMap[serviceAdapterId] = serviceConfig;
    }

    ScopedLock lock(_lock);
    map<string, ServiceConfig>::iterator configIt;
    for (configIt = serviceConfigMap.begin();
         configIt != serviceConfigMap.end(); configIt ++)
    {
        const string &serviceAdapterId = configIt->first;
        if (_serviceAdapters.find(serviceAdapterId) != _serviceAdapters.end()) {
            _serviceAdapters[serviceAdapterId]->updateConfig(configIt->second);
            continue;
        }
        ServiceAdapterPtr serviceAdapterPtr =
            _serviceAdapterCreatorPtr->create(serviceAdapterId,
                    configIt->second);

        if (serviceAdapterPtr) {
            _serviceAdapters[serviceAdapterId] = serviceAdapterPtr;
            CARBON_LOG(INFO, "add service adapter:[%s], config:[%s].",
                       configIt->first.c_str(),
                       ToJsonString(configIt->second).c_str());
        }
    }

    map<string, ServiceAdapterPtr>::iterator adapterIt;
    for (adapterIt = _serviceAdapters.begin();
         adapterIt != _serviceAdapters.end(); adapterIt ++)
    {
        const string &serviceAdapterId = adapterIt->first;
        if (serviceConfigMap.find(serviceAdapterId) != serviceConfigMap.end()) {
            continue;
        }
        adapterIt->second->unPublish();
        CARBON_LOG(INFO, "unpublish service adapter:[%s].",
                   serviceAdapterId.c_str());
    }
}

void ServiceSwitch::sync() {
    REPORT_USED_TIME(_id, METRIC_SERVICE_SYNC_TIME);
    removeUnPublishedAdapters();
    map<nodeid_t, ServiceNode> serviceNodes;
    map<nodeid_t, ServiceNode> serviceNodesAll;
    map<string, ServiceAdapterPtr> serviceAdapterMap;
    {
        ScopedLock lock(_lock);
        if (!_updated) {
            CARBON_LOG(INFO, "[%s] serviceSwitch hasn't been updated, skip sync.",
                       _id.c_str());
            return;
        }
        serviceNodes = _serviceNodes;
        serviceNodesAll = _serviceNodesAll;
        serviceAdapterMap = _serviceAdapters;
    }

    CARBON_LOG(DEBUG, "sync node size:%zd", serviceNodes.size());

    map<string, ServiceNodeSet> remoteServiceNodesMap;
    _succeed = true;
    if (serviceAdapterMap.size() != 0) {
        ServiceNodeSet serviceNodeSet;
        for (auto node : serviceNodes) {
            serviceNodeSet.insert(node.second);
        }

        for (map<string, ServiceAdapterPtr>::iterator it = serviceAdapterMap.begin();
             it != serviceAdapterMap.end(); it++)
        {

            if (!it->second->canSync()) {
                // it used to control the sync rate for service adapter,
                // especially for lvs service adapter
                continue;
            }

            ServiceNodeSet *remoteServiceNodes = &remoteServiceNodesMap[it->first];
            if (!it->second->sync(serviceNodeSet, remoteServiceNodes)) {
                CARBON_LOG(ERROR, "sync service %s nodes failed.", it->first.c_str());
                _succeed = false;
                continue;
            }

            CARBON_LOG(DEBUG, "remote service nodes count [%zd]",
                       remoteServiceNodes->size());
        }
    }

    map<nodeid_t, ServiceInfo> serviceInfoMap;
    // hack: when serviceAdapterMap.empty(), the service status for a node is SVT_AVAILABLE,
    // which cause an offline node can't be offlined to release.
    if (serviceAdapterMap.empty()) {
        getServiceInfos(serviceNodes, remoteServiceNodesMap, &serviceInfoMap);
    } else {
        getServiceInfos(serviceNodesAll, remoteServiceNodesMap, &serviceInfoMap);
    }

    ScopedLock lock(_lock);
    _serviceInfos.swap(serviceInfoMap);
    _remoteServiceNodesMap.swap(remoteServiceNodesMap);
    _isWorking = _succeed;
    CARBON_LOG(DEBUG, "after sync service infos:[%s].",
               ToJsonString(_serviceInfos).c_str());
}

bool ServiceSwitch::isCleared() {
    if (!_succeed){
        return false;
    }
    ScopedLock lock(_lock);
    if (_remoteServiceNodesMap.empty()) {
        return true;
    }
    for (auto kv : _remoteServiceNodesMap) {
        if (!kv.second.empty()){
            return false;
        }
    }
    return true;
}

void ServiceSwitch::getServiceInfos(
        const map<nodeid_t, ServiceNode> &serviceNodes,
        const map<string, ServiceNodeSet> &remoteServiceNodesMap,
        map<nodeid_t, ServiceInfo> *serviceInfoMap)
{
    for (map<nodeid_t, ServiceNode>::const_iterator it = serviceNodes.begin();
         it != serviceNodes.end(); it++)
    {
        const ServiceNode &localNode = it->second;
        ServiceType status = getServiceStatus(localNode, remoteServiceNodesMap);
        ServiceInfo serviceInfo;
        serviceInfo.status = status;
        for (const auto &kv : remoteServiceNodesMap) {
            auto setIter = kv.second.find(localNode);
            if (setIter == kv.second.end()) {
                continue;
            }
            serviceInfo.metas.insert(setIter->metas.begin(), setIter->metas.end());
            serviceInfo.score += setIter->score;
        }
        (*serviceInfoMap)[localNode.nodeId] = serviceInfo;
    }
}

ServiceType ServiceSwitch::getServiceStatus(const ServiceNode &localNode,
        const map<string, ServiceNodeSet> &remoteServiceNodesMap)
{
    // when the remoteServiceNodesMap.size() == 0, it means there is
    // no ServiceAdapter, the node is always SVT_AVAILABLE.
    if (remoteServiceNodesMap.size() == 0) {
        return SVT_AVAILABLE;
    }

    size_t availableCount = 0;
    size_t unAvailableCount = 0;

    for (const auto &node : remoteServiceNodesMap) {
        const ServiceNodeSet &serviceNodes = node.second;
        ServiceNodeSet::const_iterator it = serviceNodes.find(localNode);
        if (it == serviceNodes.end()) {
            unAvailableCount += 1;
        } else {
            if (it->status == SN_AVAILABLE) {
                availableCount += 1;
            } else if (it->status == SN_UNAVAILABLE ||
                       it->status == SN_DISABLED)
            {
                unAvailableCount += 1;
            }
        }
    }

    if (availableCount == remoteServiceNodesMap.size()) {
        return SVT_AVAILABLE;
    }

    if (availableCount > 0 && availableCount < remoteServiceNodesMap.size()) {
        return SVT_PART_AVAILABLE;
    }

    if (unAvailableCount == remoteServiceNodesMap.size()) {
        return SVT_UNAVAILABLE;
    }

    CARBON_LOG(ERROR, "ServiceNode [%s:%s] turn to SVT_UNKNOWN in runtime",
               localNode.ip.c_str(), localNode.nodeId.c_str());
    return SVT_UNKNOWN;
}

void ServiceSwitch::removeUnPublishedAdapters() {
    ScopedLock lock(_lock);
    for (map<string, ServiceAdapterPtr>::iterator it = _serviceAdapters.begin();
         it != _serviceAdapters.end();)
    {
        if (it->second->isUnPublished()) {
            CARBON_LOG(INFO, "remove service adapter:[%s].",
                       it->first.c_str());
            _serviceAdapters.erase(it++);
        } else {
            it++;
        }
    }
}

string ServiceSwitch::genServiceAdapterId(const ServiceConfig &config) {
    string adapterId = SignatureGenerator::genServiceAdapterId(config);
    adapterId = _id + "." + adapterId;
    return adapterId;
}

bool ServiceSwitch::recoverServiceAdapters(const vector<ServiceConfig> &serviceConfigs) {
    for (size_t i = 0; i < serviceConfigs.size(); i++) {
        const ServiceConfig &serviceConfig = serviceConfigs[i];
        string serviceAdapterId = genServiceAdapterId(serviceConfig);
        ServiceAdapterPtr serviceAdapterPtr =
            _serviceAdapterCreatorPtr->create(serviceAdapterId, serviceConfig);
        if (serviceAdapterPtr == NULL) {
            CARBON_LOG(ERROR, "create service adapter failed, adapterId:[%s].",
                       serviceAdapterId.c_str());
            continue;
        }

        if (!serviceAdapterPtr->recover()) {
            CARBON_LOG(ERROR, "recover service adapter failed, adapterId:[%s].",
                       serviceAdapterId.c_str());
            return false;
        }

        _serviceAdapters[serviceAdapterId] = serviceAdapterPtr;
        CARBON_LOG(INFO, "recover service adapter:[%s], config:[%s].",
                   serviceAdapterId.c_str(),
                   ToJsonString(serviceConfig).c_str());
    }

    return true;
}

END_CARBON_NAMESPACE(master);
