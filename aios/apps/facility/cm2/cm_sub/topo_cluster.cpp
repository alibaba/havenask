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
#include "aios/apps/facility/cm2/cm_sub/topo_cluster.h"

#include <limits>
#include <sys/time.h>

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central.h"
#include "aios/apps/facility/cm2/cm_sub/lb_policy/lb_policy.h"

namespace cm_sub {

using namespace cm_basic;

AUTIL_LOG_SETUP(cm_sub, TopoCluster);

TopoCluster::TopoCluster(CMClusterWrapperPtr cmCluster, int partitionCount)
    : _idcPreferRatio(0)
    , _clusterStatus(OS_ONLINE)
    , _deleted(false)
{
    if (!cmCluster) {
        return;
    }
    ClusterInfo info;
    info.ref = cmCluster;
    info.partitionCount = partitionCount;
    _owners[cmCluster->getClusterName()] = info;

    setClusterStatus(cmCluster->getClusterStatus());

    if (cmCluster->hasIDCPreferRatio()) {
        _idcPreferRatio = cmCluster->getIDCPreferRatio();
    }

    for (int i = 0; i < partitionCount; i++) {
        auto partition = std::make_shared<TopoPartition>();
        if (cmCluster->hasIDCPreferRatioNew()) {
            partition->setIDCPreferRatioNew(cmCluster->getIDCPreferRatioNew());
        }
        if (cmCluster->hasCloseIDCPreferNew()) {
            partition->setCloseIDCPreferNew(cmCluster->getCloseIDCPreferNew());
        }
        addPartition(partition);
    }
}

TopoCluster::TopoCluster() : _idcPreferRatio(0), _clusterStatus(OS_ONLINE), _deleted(false) {}

TopoCluster::~TopoCluster()
{
    _lbPartitionVec.clear();
    _owners.clear();
}

bool TopoCluster::initLBPolicyInfo(std::shared_ptr<TopoCluster> originCluster)
{
    if (originCluster && originCluster->getPartitionCount() != getPartitionCount()) {
        originCluster.reset();
    }
    autil::ScopedWriteLock lock(_partitionRWLock);
    if (originCluster == nullptr) {
        for (auto& lbPartition : _lbPartitionVec) {
            lbPartition.partition->initLBPolicyInfo();
            lbPartition.lbPolicy = LBPolicyTable::create(lbPartition.partition);
        }
    } else {
        for (int i = 0; i < _lbPartitionVec.size(); i++) {
            _lbPartitionVec[i].partition->initLBPolicyInfo();

            auto& originLBPartition = originCluster->_lbPartitionVec[i];
            _lbPartitionVec[i].lbPolicy = LBPolicyTable::create(originLBPartition.lbPolicy, originLBPartition.partition,
                                                                _lbPartitionVec[i].partition);
        }
    }
    return true;
}

// merge the existed topo cluster to a new created topo cluster
bool TopoCluster::merge(const std::shared_ptr<TopoCluster>& from)
{
    autil::ScopedWriteLock lock(_partitionRWLock);
    assert(_owners.size() == 1);
    std::string selfName = _owners.begin()->first;
    size_t maxPartitionCount = 0;
    // copy owner
    for (auto it = from->_owners.begin(); it != from->_owners.end(); ++it) {
        if (it->first != selfName) {
            _owners.insert(*it);
            if (maxPartitionCount < it->second.partitionCount) {
                maxPartitionCount = it->second.partitionCount;
            }
        }
    }
    if (_owners.size() == 1) {
        return false; // no new owners merged
    }

    for (size_t i = 0; i < maxPartitionCount; ++i) {
        auto fromPartition = from->_lbPartitionVec[i].partition;
        if (_lbPartitionVec.size() > i) { // partition exists
            auto thisPartition = _lbPartitionVec[i].partition;
            thisPartition->merge(selfName, fromPartition);
        } else { // append partition
            auto newPartition = std::make_shared<TopoPartition>(fromPartition);
            _lbPartitionVec.emplace_back(newPartition);
        }
    }
    return true;
}

std::shared_ptr<TopoCluster> TopoCluster::split(const std::string& splitCluster)
{
    autil::ScopedWriteLock lock(_partitionRWLock);
    assert(_owners.find(splitCluster) != _owners.end());
    auto newCluster = std::make_shared<TopoCluster>();
    newCluster->_clusterStatus = _clusterStatus;
    newCluster->_idcPreferRatio = _idcPreferRatio;
    newCluster->_deleted = _deleted;
    size_t maxPartitionCount = 0;
    // copy owner and find maxPartitionCount
    for (auto it = _owners.begin(); it != _owners.end(); ++it) {
        if (splitCluster != it->first) {
            newCluster->_owners.insert(*it);
            if (maxPartitionCount < it->second.partitionCount) {
                maxPartitionCount = it->second.partitionCount;
            }
        }
    }
    // copy nodes
    for (size_t i = 0; i < _lbPartitionVec.size() && i < maxPartitionCount; ++i) {
        auto thisPartition = _lbPartitionVec[i].partition;
        auto newPartition = thisPartition->split(splitCluster);
        newCluster->_lbPartitionVec.emplace_back(newPartition);
    }
    return newCluster;
}

std::vector<long> TopoCluster::getFullRowVersions()
{
    std::vector<long> vers;
    size_t partitionCount = _lbPartitionVec.size();
    for (auto it = _versionStatus.begin(); it != _versionStatus.end(); ++it) {
        if (it->second.size() == partitionCount) {
            int rowCount = std::numeric_limits<int>::max();
            for (auto pit : it->second) {
                if (pit.second < rowCount)
                    rowCount = pit.second;
            }
            vers.insert(vers.end(), rowCount, it->first);
        }
    }
    return vers;
}

std::shared_ptr<TopoPartition> TopoCluster::getPartitionByIndex(size_t i)
{
    autil::ScopedReadLock lock(_partitionRWLock);
    if (i >= _lbPartitionVec.size()) {
        return nullptr;
    }
    return _lbPartitionVec[i].partition;
}

size_t TopoCluster::getPartitionCount()
{
    autil::ScopedReadLock lock(_partitionRWLock);
    return _lbPartitionVec.size();
}

int32_t TopoCluster::getNodeCntOfPartition(int32_t partitionId)
{
    auto partition = getPartitionByIndex(partitionId);
    if (!partition) {
        return -1;
    }
    return partition->getNodeSize();
}

int32_t TopoCluster::getValidNodeCntOfPartition(int32_t partitionId)
{
    auto partition = getPartitionByIndex(partitionId);
    if (!partition) {
        return -1;
    }
    return partition->getValidNodeSize();
}

int32_t TopoCluster::allocRow(std::vector<ServiceNode*>& nodeVec, ProtocolType protocol, IDCType idc,
                              uint32_t locatingSignal, LBPolicyType policy)
{
    idc = getIDCType(policy, idc, getValidNodeRatio());

    autil::ScopedReadLock lock(_partitionRWLock);
    for (auto& lbPartition : _lbPartitionVec) {
        if (lbPartition.partition->getNodeSize() == 0) {
            continue;
        }
        auto lbPolicy = lbPartition.lbPolicy->getPolicy(policy);
        if (lbPolicy) {
            ServiceNode* node = lbPolicy->getServiceNode(lbPartition.partition, protocol, idc, locatingSignal);
            if (node) {
                nodeVec.push_back(node);
            }
        }
    }
    return 0;
}

int32_t TopoCluster::allocValidRow(std::vector<ServiceNode*>& nodeVec, ProtocolType protocol, IDCType idc,
                                   uint32_t locatingSignal, LBPolicyType policy)
{
    idc = getIDCType(policy, idc, getValidNodeRatio());

    autil::ScopedReadLock lock(_partitionRWLock);
    for (auto& lbPartition : _lbPartitionVec) {
        if (lbPartition.partition->getNodeSize() == 0) {
            continue;
        }
        auto lbPolicy = lbPartition.lbPolicy->getPolicy(policy);
        if (lbPolicy) {
            ServiceNode* node = lbPolicy->getValidServiceNode(lbPartition.partition, protocol, idc, locatingSignal);
            if (node) {
                nodeVec.push_back(node);
            }
        }
    }
    return 0;
}

long TopoCluster::allocValidVersionRow(std::map<long, std::vector<ServiceNode*>>& versionNodeVec, ProtocolType protocol,
                                       IDCType idc, uint32_t locatingSignal, LBPolicyType policy)
{
    idc = IDC_ANY;

    autil::ScopedReadLock lock(_partitionRWLock);
    std::vector<long> fullVers = getFullRowVersions();
    size_t idx = fullVers.size() <= 1 ? 0 : rand() % fullVers.size();
    long selfVersion = -1;
    for (size_t i = 0; i < fullVers.size(); ++i) {
        long ver = fullVers[idx];
        idx = (idx + 1) % fullVers.size();

        // get version service node from each partition
        std::vector<ServiceNode*> row;
        for (auto& lbPartition : _lbPartitionVec) {
            auto lbPolicy = lbPartition.lbPolicy->getPolicy(policy);
            if (!lbPolicy) {
                continue;
            }
            // get version service node from this partition
            for (int i = 0; i < lbPartition.partition->getNodeSize(); i++) {
                ServiceNode* node =
                    lbPolicy->getValidServiceNode(lbPartition.partition, protocol, idc, (uint32_t)locatingSignal);
                if (node && node->_version == ver) {
                    row.push_back(node);
                    break;
                }
                if (node) {
                    delete node;
                }
            }
        }
        // find fully version
        versionNodeVec.insert(std::make_pair(ver, row));
        if (row.size() == _lbPartitionVec.size()) {
            selfVersion = ver;
            break;
        }
    }
    return selfVersion;
}

int32_t TopoCluster::allocNodeOfPartition(std::vector<ServiceNode*>& nodeVec, int32_t partitionId,
                                          ProtocolType protocol, IDCType idc)
{
    auto partition = getPartitionByIndex(partitionId);
    if (partition) {
        idc = getIDCType(LB_ROUNDROBIN, idc, partition->getValidNodeRatio());
        auto topoNodes = partition->allocNode(protocol, idc);
        for (auto& topoNode : topoNodes) {
            ServiceNode* node = new (std::nothrow) ServiceNode(topoNode, protocol);
            if (node == NULL) {
                continue;
            }
            nodeVec.push_back(node);
        }
        return 0;
    }
    return -1;
}

int32_t TopoCluster::allocValidNodeOfPartition(std::vector<ServiceNode*>& nodeVec, int32_t partitionId,
                                               ProtocolType protocol, IDCType idc)
{
    auto partition = getPartitionByIndex(partitionId);
    if (partition) {
        idc = getIDCType(LB_ROUNDROBIN, idc, partition->getValidNodeRatio());
        auto topoNodes = partition->allocValidNode(protocol, idc);
        for (auto& topoNode : topoNodes) {
            ServiceNode* node = new (std::nothrow) ServiceNode(topoNode, protocol);
            if (node == NULL) {
                continue;
            }
            nodeVec.push_back(node);
        }
        return 0;
    }
    return -1;
}

int32_t TopoCluster::allocNodeByPartitionId(ServiceNode*& node, int32_t partitionId, ProtocolType protocol,
                                            LBPolicyType policy, IDCType idc, uint32_t locatingSignal)
{
    autil::ScopedReadLock lock(_partitionRWLock);
    if (partitionId >= _lbPartitionVec.size()) {
        return -1;
    }

    auto partition = _lbPartitionVec[partitionId].partition;
    auto lbPolicy = _lbPartitionVec[partitionId].lbPolicy->getPolicy(policy);
    if (!partition || partition->getNodeSize() == 0 || !lbPolicy) {
        return -1;
    }

    idc = getIDCType(policy, idc, partition->getValidNodeRatio());
    node = lbPolicy->getServiceNode(partition, protocol, idc, locatingSignal);
    return 0;
}

int32_t TopoCluster::allocValidNodeByPartitionId(ServiceNode*& node, int32_t partitionId, ProtocolType protocol,
                                                 LBPolicyType policy, IDCType idc, uint32_t locatingSignal)
{
    autil::ScopedReadLock lock(_partitionRWLock);
    if (partitionId >= _lbPartitionVec.size()) {
        return -1;
    }

    auto partition = _lbPartitionVec[partitionId].partition;
    auto lbPolicy = _lbPartitionVec[partitionId].lbPolicy->getPolicy(policy);
    if (!partition || partition->getNodeSize() == 0 || !lbPolicy) {
        return -1;
    }

    idc = getIDCType(policy, idc, partition->getValidNodeRatio());
    node = lbPolicy->getValidServiceNode(partition, protocol, idc, locatingSignal);
    return 0;
}

int32_t TopoCluster::allocValidNodeByPartitionIdUint64(ServiceNode*& node, int32_t partitionId, ProtocolType protocol,
                                                       LBPolicyType policy, IDCType idc, uint64_t locatingSignal)
{
    autil::ScopedReadLock lock(_partitionRWLock);
    if (partitionId >= _lbPartitionVec.size()) {
        return -1;
    }

    auto partition = _lbPartitionVec[partitionId].partition;
    auto lbPolicy = _lbPartitionVec[partitionId].lbPolicy->getPolicy(policy);
    if (!partition || partition->getNodeSize() == 0 || !lbPolicy) {
        return -1;
    }

    idc = getIDCType(policy, idc, partition->getValidNodeRatio());
    node = lbPolicy->getValidServiceNode(partition, protocol, idc, locatingSignal);
    return 0;
}

int32_t TopoCluster::allocAllNode(std::vector<ServiceNode*>& nodeVec, bool valid)
{
    autil::ScopedReadLock lock(_partitionRWLock);
    for (auto& lbPartition : _lbPartitionVec) {
        auto topoNodes = lbPartition.partition->allocAllNode(valid);
        for (auto topoNode : topoNodes) {
            ServiceNode* node = new (std::nothrow) ServiceNode(topoNode, cm_basic::PT_ANY);
            if (node == NULL) {
                continue;
            }
            nodeVec.push_back(node);
        }
    }
    return 0;
}

bool TopoCluster::addNode(int32_t partitionId, long version, const std::shared_ptr<TopoNode>& node)
{
    autil::ScopedReadLock lock(_partitionRWLock);
    if (partitionId >= _lbPartitionVec.size()) {
        return false;
    }
    _versionStatus[version][partitionId] += 1;
    _lbPartitionVec[partitionId].partition->addNode(node);
    return true;
}

void TopoCluster::addPartition(const std::shared_ptr<TopoPartition>& partition)
{
    autil::ScopedWriteLock lock(_partitionRWLock);
    _lbPartitionVec.emplace_back(partition);
}

void TopoCluster::printTopo()
{
    autil::ScopedReadLock lock(_partitionRWLock);
    AUTIL_LOG(INFO, "cluster_status:%d", _clusterStatus);
    AUTIL_LOG(INFO, "partition cnt :%zu", _lbPartitionVec.size());
    for (uint32_t i = 0; i < _lbPartitionVec.size(); ++i) {
        auto partition = (_lbPartitionVec)[i].partition;
        AUTIL_LOG(INFO, "partition[%u]:nodecnt:%zu", i, partition->getNodeSize());
        partition->printTopo();
    }
}

IDCType TopoCluster::getIDCType(LBPolicyType policy, IDCType idc, double validNodeRatio)
{
    if (idc == cm_basic::IDC_ANY) {
        // 直接不考虑本机房优先策略
        return idc;
    }
    if (policy == LB_LOCATING || policy == LB_CONHASH) {
        // 这两种策略是取指定行的节点，不需要考虑本机房优先策略
        return cm_basic::IDC_ANY;
    }

    double minValidNodeRatio = (double)(_idcPreferRatio) / 100;
    if (unlikely(validNodeRatio < minValidNodeRatio)) {
        // 有效节点数量小于配置的最小值，不考虑本机房优先策略
        return cm_basic::IDC_ANY;
    }
    return idc;
}

double TopoCluster::getValidNodeRatio()
{
    int32_t nodeCnt = 0;
    int32_t valideNodeCnt = 0;

    autil::ScopedReadLock lock(_partitionRWLock);
    for (auto& lbPartition : _lbPartitionVec) {
        nodeCnt += lbPartition.partition->getNodeSize();
        valideNodeCnt += lbPartition.partition->getValidNodeSize();
    }
    return nodeCnt > 0 ? double(valideNodeCnt) / double(nodeCnt) : 0;
}

int32_t TopoCluster::getValidNodeCount()
{
    int32_t nodeCount = 0;

    autil::ScopedReadLock lock(_partitionRWLock);
    for (auto& lbPartition : _lbPartitionVec) {
        nodeCount += lbPartition.partition->getValidNodeSize();
    }
    return nodeCount;
}

int32_t TopoCluster::getNodeCount()
{
    int32_t nodeCount = 0;

    autil::ScopedReadLock lock(_partitionRWLock);
    for (auto& lbPartition : _lbPartitionVec) {
        nodeCount += lbPartition.partition->getNodeSize();
    }
    return nodeCount;
}

std::shared_ptr<ILBPolicy> TopoCluster::getPartitionPolicy(int32_t partitionId, LBPolicyType policyType)
{
    autil::ScopedReadLock lock(_partitionRWLock);
    if (partitionId >= _lbPartitionVec.size()) {
        return nullptr;
    }
    if (_lbPartitionVec[partitionId].lbPolicy == nullptr) {
        return nullptr;
    }
    return _lbPartitionVec[partitionId].lbPolicy->getPolicy(policyType);
}

} // namespace cm_sub
