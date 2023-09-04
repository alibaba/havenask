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
#include "aios/apps/facility/cm2/cm_sub/topo_cluster_table_impl.h"

#include "aios/apps/facility/cm2/cm_sub/topo_cluster.h"
#include "autil/StringUtil.h"

using namespace cm_basic;

namespace cm_sub {

AUTIL_LOG_SETUP(cm_sub, TopoClusterTableImpl);

int32_t TopoClusterTableImpl::buildTopo(const cm_basic::CMClusterWrapperPtr& cmCluster)
{
    if (cmCluster == nullptr) {
        return -1;
    }

    // 确认一个cluster的markDeleted是否可以被新的topoCluster继承, 不需要, 下次build的时候会删除这个cluster
    if (cmCluster->getClusterTopoType() == CMCluster::TT_ONE_MAP_ONE) {
        AUTIL_LOG(INFO, "build topo cluster[%s], topo_type = TT_ONE_MAP_ONE", cmCluster->getClusterName().c_str());
        return updateOneMapOneCMCluster(cmCluster);
    } else if (cmCluster->getClusterTopoType() == CMCluster::TT_CLUSTER_MAP_TABLE) {
        AUTIL_LOG(INFO, "build topo cluster[%s], topo_type = TT_CLUSTER_MAP_TABLE",
                  cmCluster->getClusterName().c_str());
        return updateClusterMapCMCluster(cmCluster);
    }
    return 0;
}

int32_t TopoClusterTableImpl::updateOneMapOneCMCluster(const cm_basic::CMClusterWrapperPtr& cmCluster)
{
    auto topoCluster = convertOneMapOneCMClusterToTopoCluster(cmCluster);
    if (!topoCluster) {
        return -1;
    }

    autil::ScopedWriteLock lock(_rwlock);
    auto topoClusterName = cmCluster->getClusterName();
    updateTopoCluster(cmCluster->getClusterName(), topoClusterName, topoCluster);
    return 0;
}

std::shared_ptr<TopoCluster>
TopoClusterTableImpl::convertOneMapOneCMClusterToTopoCluster(const cm_basic::CMClusterWrapperPtr& cmCluster)
{
    auto groupNodeList = cmCluster->getGroupNodeList();
    auto topoCluster = std::make_shared<TopoCluster>(cmCluster, groupNodeList.size());
    if (!topoCluster) {
        return nullptr;
    }
    for (int i = 0; i < groupNodeList.size(); i++) {
        auto topoPartition = topoCluster->getPartitionByIndex(i);
        for (auto& node : groupNodeList[i]) {
            auto topoNode = std::make_shared<TopoNode>(node, _conhashWeightRatio, _virtualNodeRatio);
            topoPartition->addNode(topoNode);
        }
    }
    return topoCluster;
}

int32_t TopoClusterTableImpl::updateClusterMapCMCluster(const cm_basic::CMClusterWrapperPtr& cmCluster)
{
    auto topoClusters = convertTopoCMClusterToTopoClusters(cmCluster);

    autil::ScopedWriteLock lock(_rwlock);
    auto lastTopoClusterNames = _clusterNameToTopoClusterNames[cmCluster->getClusterName()];
    for (auto lastTopoClusterName : lastTopoClusterNames) {
        if (topoClusters.find(lastTopoClusterName) == topoClusters.end()) {
            deleteTopoCluster(cmCluster->getClusterName(), lastTopoClusterName);
        }
    }

    for (auto iter : topoClusters) {
        updateTopoCluster(cmCluster->getClusterName(), iter.first, iter.second);
    }
    return 0;
}

std::map<std::string, std::shared_ptr<TopoCluster>>
TopoClusterTableImpl::convertTopoCMClusterToTopoClusters(const cm_basic::CMClusterWrapperPtr& cmCluster)
{
    std::map<std::string, std::shared_ptr<TopoCluster>> topoClusters;
    auto nodeList = cmCluster->getNodeList();

    for (auto& node : nodeList) {
        auto topoInfos = autil::StringUtil::split(node->getTopoInfo(), '|');
        for (auto& topoInfo : topoInfos) {
            auto tableInfos = autil::StringUtil::split(topoInfo, ':');
            if (tableInfos.size() < 3) {
                continue;
            }
            auto& topoClusterName = tableInfos[0];
            int32_t partitionCount = strtol(tableInfos[1].c_str(), nullptr, 10);
            int32_t partitionId = strtol(tableInfos[2].c_str(), nullptr, 10);
            long ver = tableInfos.size() > 3 ? strtol(tableInfos[3].c_str(), nullptr, 10) : 0;
            int weight = tableInfos.size() > 4 ? strtol(tableInfos[4].c_str(), nullptr, 10) : -1;

            auto& topoCluster = topoClusters[topoClusterName];
            if (topoCluster == nullptr) {
                topoCluster.reset(new TopoCluster(cmCluster, partitionCount));
            }
            auto topoNode = std::make_shared<TopoNode>(node, _conhashWeightRatio, _virtualNodeRatio, ver, weight);
            topoCluster->addNode(partitionId, ver, topoNode);
        }
    }
    return topoClusters;
}

int32_t TopoClusterTableImpl::deleteTopoCluster(const std::string& cmClusterName, const std::string& topoClusterName)
{
    // 清理topoClusterMap中的cluster
    auto topoClusterIter = _topoClusterMap.find(topoClusterName);
    if (topoClusterIter == _topoClusterMap.end()) {
        return 0;
    }
    if (!topoClusterIter->second->hasOwner(cmClusterName)) {
        // 当前的topoCluster中没有对应的cmCluster的节点
        return 0;
    }
    if (topoClusterIter->second->ownerCount() > 1) {
        // 如果一个TopoCluster里有多个CMCluster的节点. 多个CMCluster有同名biz.
        // 那么要将要被删掉的这个从里面拿出来
        auto splitTopoCluster = topoClusterIter->second->split(cmClusterName);
        if (splitTopoCluster) {
            splitTopoCluster->initLBPolicyInfo();
            _topoClusterMap[topoClusterName] = splitTopoCluster;
        }
    } else {
        // 直接清理这个cluster
        _topoClusterMap.erase(topoClusterIter);
    }

    // 清理topoClusterNameMap的cluster
    auto topoClusterNamesIter = _clusterNameToTopoClusterNames.find(cmClusterName);
    if (topoClusterNamesIter != _clusterNameToTopoClusterNames.end()) {
        topoClusterNamesIter->second.erase(topoClusterName);
    }
    return 0;
}

int32_t TopoClusterTableImpl::updateTopoCluster(const std::string& cmClusterName, const std::string& topoClusterName,
                                                const std::shared_ptr<TopoCluster>& topoCluster)
{
    bool initLBInfo = false;
    auto curTopoClusterIter = _topoClusterMap.find(topoClusterName);
    if (curTopoClusterIter != _topoClusterMap.end()) {
        // 需要更新的TopoCluster, 需要检查之前的topoCluster的状态
        // 如果之前的TopoCluster里存在非当前的CMCluster映射的节点,
        // 那么需要通过merge把这部分节点保存到新的TopoCluster
        if (curTopoClusterIter->second->ownerCount() > 1 ||
            (curTopoClusterIter->second->ownerCount() == 1 && !curTopoClusterIter->second->hasOwner(cmClusterName))) {
            topoCluster->merge(curTopoClusterIter->second);
        } else {
            initLBInfo = true;
            topoCluster->initLBPolicyInfo(curTopoClusterIter->second);
        }
    }

    if (!initLBInfo) {
        topoCluster->initLBPolicyInfo();
    }

    _topoClusterMap[topoClusterName] = topoCluster;

    auto& topoClusterNames = _clusterNameToTopoClusterNames[cmClusterName];
    topoClusterNames.insert(topoClusterName);
    return 0;
}

int32_t TopoClusterTableImpl::deleteTopo(const std::string& cmClusterName)
{
    autil::ScopedWriteLock lock(_rwlock);

    auto iter = _clusterNameToTopoClusterNames.find(cmClusterName);
    if (iter == _clusterNameToTopoClusterNames.end()) {
        return 0;
    }

    auto topoClusterNames = iter->second;
    for (auto topoClusterName : topoClusterNames) {
        deleteTopoCluster(cmClusterName, topoClusterName);
    }
    _clusterNameToTopoClusterNames.erase(iter);
    return 0;
}

void TopoClusterTableImpl::updateTopoClusterStatus(const std::string& cmClusterName, cm_basic::OnlineStatus curStatus)
{
    autil::ScopedReadLock lock(_rwlock);
    auto iter = _clusterNameToTopoClusterNames.find(cmClusterName);
    if (iter == _clusterNameToTopoClusterNames.end()) {
        return;
    }

    for (auto& topoClusterName : iter->second) {
        auto topoClusterIter = _topoClusterMap.find(topoClusterName);
        if (topoClusterIter == _topoClusterMap.end()) {
            continue;
        }
        topoClusterIter->second->setClusterStatus(curStatus);
    }
}

void TopoClusterTableImpl::markClusterDeleted(const std::string& cmClusterName)
{
    autil::ScopedReadLock lock(_rwlock);
    auto iter = _clusterNameToTopoClusterNames.find(cmClusterName);
    if (iter == _clusterNameToTopoClusterNames.end()) {
        return;
    }

    for (auto& topoClusterName : iter->second) {
        auto topoClusterIter = _topoClusterMap.find(topoClusterName);
        if (topoClusterIter == _topoClusterMap.end()) {
            continue;
        }
        topoClusterIter->second->setDeleted();
    }
}

void TopoClusterTableImpl::syncTopoClusters(const std::string& cmClusterName,
                                            const std::shared_ptr<TopoClusterTableImpl>& from)
{
    // optimize: only sync specified cm cluster
    autil::ScopedWriteLock lock(_rwlock);
    auto topoClusters = from->getTopoClusters(cmClusterName);

    auto lastTopoClusterNamesIter = _clusterNameToTopoClusterNames.find(cmClusterName);
    if (lastTopoClusterNamesIter != _clusterNameToTopoClusterNames.end()) {
        for (auto& lastTopoClusterName : lastTopoClusterNamesIter->second) {
            if (topoClusters.find(lastTopoClusterName) == topoClusters.end()) {
                deleteTopoCluster(cmClusterName, lastTopoClusterName);
            }
        }
    }

    for (auto iter : topoClusters) {
        _topoClusterMap[iter.first] = iter.second;
        _clusterNameToTopoClusterNames[cmClusterName].insert(iter.first);
    }
}

void TopoClusterTableImpl::getTopoClusterNames(const std::string& cmClusterName,
                                               std::vector<std::string>& topoClusterNames)
{
    autil::ScopedReadLock lock(_rwlock);
    auto iter = _clusterNameToTopoClusterNames.find(cmClusterName);
    if (iter != _clusterNameToTopoClusterNames.end()) {
        topoClusterNames.reserve(iter->second.size());
        for (auto topoClusterName : iter->second) {
            topoClusterNames.emplace_back(topoClusterName);
        }
    }
}

int32_t TopoClusterTableImpl::getTopoClusterCount()
{
    autil::ScopedReadLock lock(_rwlock);
    return _topoClusterMap.size();
}

std::map<std::string, std::shared_ptr<TopoCluster>> TopoClusterTableImpl::getTopoClusters()
{
    autil::ScopedReadLock lock(_rwlock);
    return _topoClusterMap;
}

int32_t TopoClusterTableImpl::getConhashWeightRatio() { return _conhashWeightRatio; }

std::shared_ptr<TopoCluster> TopoClusterTableImpl::getTopoCluster(const std::string& topoClusterName)
{
    autil::ScopedReadLock lock(_rwlock);
    auto iter = _topoClusterMap.find(topoClusterName);
    if (iter != _topoClusterMap.end() && iter->second->getClusterStatus() == OS_ONLINE) {
        if (iter->second->isDeleted()) {
            AUTIL_LOG(ERROR, "cluster already deleted on server: [%s]", topoClusterName.c_str());
        }
        return iter->second;
    }
    return nullptr;
}

std::map<std::string, std::shared_ptr<TopoCluster>>
TopoClusterTableImpl::getTopoClusters(const std::string& cmClusterName)
{
    std::map<std::string, std::shared_ptr<TopoCluster>> topoClusters;
    autil::ScopedReadLock lock(_rwlock);
    auto iter = _clusterNameToTopoClusterNames.find(cmClusterName);
    if (iter != _clusterNameToTopoClusterNames.end()) {
        for (auto topoClusterName : iter->second) {
            auto topoClusterIter = _topoClusterMap.find(topoClusterName);
            if (topoClusterIter != _topoClusterMap.end()) {
                topoClusters[topoClusterIter->first] = topoClusterIter->second;
            }
        }
    }
    return topoClusters;
}

void TopoClusterTableImpl::printTopo(const std::string& cmClusterName)
{
    autil::ScopedReadLock lock(_rwlock);
    auto iter = _clusterNameToTopoClusterNames.find(cmClusterName);
    if (iter != _clusterNameToTopoClusterNames.end()) {
        AUTIL_LOG(INFO, "========= cluster:%s, topocluster cnt:%zu =========", cmClusterName.c_str(),
                  iter->second.size());
        for (auto topoClusterName : iter->second) {
            auto topoClusterIter = _topoClusterMap.find(topoClusterName);
            if (topoClusterIter != _topoClusterMap.end()) {
                topoClusterIter->second->printTopo();
            }
        }
    }
}

} // namespace cm_sub