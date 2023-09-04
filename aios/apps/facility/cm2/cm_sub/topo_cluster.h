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
#ifndef __TOPO_CLUSTER_H_
#define __TOPO_CLUSTER_H_

#include <set>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_cluster_wrapper.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_common.pb.h"
#include "aios/apps/facility/cm2/cm_sub/lb_policy/lb_policy_table.h"
#include "aios/apps/facility/cm2/cm_sub/topo_partition.h"

namespace cm_sub {

// 集群所有结点的数据信息, 该信息可能引起集群拓扑结构变化, 例如新增结点, 该数组和集群中结点一一对应
class TopoCluster
{
public:
    TopoCluster();
    TopoCluster(cm_basic::CMClusterWrapperPtr initRef, int partitionCount);
    ~TopoCluster();

private:
    TopoCluster(const TopoCluster&) = delete;
    TopoCluster(TopoCluster&&) = delete;

public:
    void addPartition(const std::shared_ptr<TopoPartition>& partition);
    size_t getPartitionCount();
    std::shared_ptr<TopoPartition> getPartitionByIndex(size_t i);

    bool addNode(int32_t partitionId, long version, const std::shared_ptr<TopoNode>& node);

public:
    int32_t getNodeCntOfPartition(int32_t partitionId);
    int32_t getValidNodeCntOfPartition(int32_t partitionId);

    int32_t allocRow(std::vector<ServiceNode*>& nodeVec, cm_basic::ProtocolType protocol, cm_basic::IDCType idc,
                     uint32_t locatingSignal, LBPolicyType policy);
    int32_t allocValidRow(std::vector<ServiceNode*>& nodeVec, cm_basic::ProtocolType protocol, cm_basic::IDCType idc,
                          uint32_t locatingSignal, LBPolicyType policy);

    long allocValidVersionRow(std::map<long, std::vector<ServiceNode*>>& versionNodeVec,
                              cm_basic::ProtocolType protocol, cm_basic::IDCType idc, uint32_t locatingSignal,
                              LBPolicyType policy);

    int32_t allocNodeOfPartition(std::vector<ServiceNode*>& nodeVec, int32_t partitionId,
                                 cm_basic::ProtocolType protocol, cm_basic::IDCType idc);
    int32_t allocValidNodeOfPartition(std::vector<ServiceNode*>& nodeVec, int32_t partitionId,
                                      cm_basic::ProtocolType protocol, cm_basic::IDCType idc);

    int32_t allocNodeByPartitionId(ServiceNode*& node, int32_t partitionId, cm_basic::ProtocolType protocol,
                                   LBPolicyType policy, cm_basic::IDCType idc, uint32_t locatingSignal);
    int32_t allocValidNodeByPartitionId(ServiceNode*& node, int32_t partitionId, cm_basic::ProtocolType protocol,
                                        LBPolicyType policy, cm_basic::IDCType idc, uint32_t locatingSignal);
    int32_t allocValidNodeByPartitionIdUint64(ServiceNode*& node, int32_t partitionId, cm_basic::ProtocolType protocol,
                                              LBPolicyType policy, cm_basic::IDCType idc, uint64_t locatingSignal);

    int32_t allocAllNode(std::vector<ServiceNode*>& nodeVec, bool valid);

public:
    bool initLBPolicyInfo(std::shared_ptr<TopoCluster> origin = nullptr);
    bool merge(const std::shared_ptr<TopoCluster>& from);
    std::shared_ptr<TopoCluster> split(const std::string& splitCluster);
    void printTopo();

public:
    size_t ownerCount() const { return _owners.size(); }
    bool hasOwner(const std::string& cname) const { return _owners.find(cname) != _owners.end(); }

    void setDeleted() { _deleted = true; }
    bool isDeleted() { return _deleted; }

    void setClusterStatus(cm_basic::OnlineStatus status) { _clusterStatus = status; }
    cm_basic::OnlineStatus getClusterStatus() { return _clusterStatus; }

private:
    cm_basic::IDCType getIDCType(LBPolicyType policy, cm_basic::IDCType idc, double validNodeRatio);
    std::vector<long> getFullRowVersions();
    double getValidNodeRatio();

public: // for test only
    int32_t getNodeCount();
    int32_t getValidNodeCount();
    std::shared_ptr<ILBPolicy> getPartitionPolicy(int32_t partitionId, LBPolicyType policyType);

public:
    enum { DefaultLBWeight = 100 };
    int32_t _idcPreferRatio; // 最小有效节点数量，如果大于该值，使用本地优先策略

private:
    cm_basic::OnlineStatus _clusterStatus;             // default = OS_ONLINE
    bool _deleted;                                     // deleted on server
    std::map<long, std::map<int, int>> _versionStatus; // <version, <partition_id, replica_count>>

private:
    struct ClusterInfo {
        size_t partitionCount;
        cm_basic::CMClusterWrapperPtr ref;
    };
    std::map<std::string, ClusterInfo> _owners;

private:
    autil::ReadWriteLock _partitionRWLock;
    struct LBTopoPartition {
        std::shared_ptr<TopoPartition> partition;
        std::shared_ptr<LBPolicyTable> lbPolicy;
        LBTopoPartition() = default;
        LBTopoPartition(const std::shared_ptr<TopoPartition>& p) : partition(p) {}
    };
    std::vector<LBTopoPartition> _lbPartitionVec;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_sub
#endif
