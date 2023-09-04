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
#ifndef __TOPO_CLUSTER_MANAGER_H_
#define __TOPO_CLUSTER_MANAGER_H_

#include <list>
#include <map>
#include <set>

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_cluster_wrapper.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_common.pb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_subscriber.pb.h"
#include "aios/apps/facility/cm2/cm_sub/query_interface.h"
#include "aios/apps/facility/cm2/cm_sub/topo_cluster_table.h"
#include "autil/Lock.h"

namespace cm_sub {

class ServiceNode;
class ILBPolicy;
class TopoCluster;
class TopoPartition;

class TopoClusterManager : public TopoClusterTable
{
public:
    TopoClusterManager(int hashWeightRatio = 0, uint32_t virtualNodeRatio = 20);
    ~TopoClusterManager();
    /**
     * @param: ServiceNode节点指针释放函数
     */
    void freeNode(ServiceNode*& node) { deletePtr(node); }
    /**
     * @param: ServiceNode节点数组指针释放函数
     */
    void freeNodes(std::vector<ServiceNode*>& nodes)
    {
        for (uint32_t i = 0; i < nodes.size(); ++i) {
            deletePtr(nodes[i]);
        }
        nodes.clear();
    }
    int32_t init();

    int32_t getPartitionCnt(const char* topo_cluster_name);
    int32_t getNodeCntOfPartition(const char* topo_cluster_name, int32_t partition_id = 0);
    int32_t getValidNodeCntOfPartition(const char* topo_cluster_name, int32_t partition_id = 0);
    /**
     * @brief: 获取一个 TopoCluster 的一行节点
     * @param: topo_cluster_name 表示拓扑结构集群的集群名
     * @param: node ServiceNode节点数组指针，内部申请外部释放
     * @param: protocol 要求节点的协议类型
     * @param: policy 节点的分配策略
     * @param: locating_signal 选择行的依据（例如：按q哈希）
     * @return: 0 is success.
     */
    int32_t allocRow(const char* topo_cluster_name, std::vector<ServiceNode*>& node_vec,
                     cm_basic::ProtocolType protocol = cm_basic::PT_ANY, LBPolicyType policy = LB_ROUNDROBIN,
                     cm_basic::IDCType idc = cm_basic::IDC_ANY, uint32_t locating_signal = 0);
    /**
     * @brief: 获取一个 TopoCluster 的一行节点
     * @param: topo_cluster_name 表示拓扑结构集群的集群名
     * @param: node ServiceNode节点数组指针，内部申请外部释放
     * @param: protocol 要求节点的协议类型
     * @param: policy 节点的分配策略
     * @param: locating_signal 选择行的依据（例如：按q哈希）
     * @return: 0 is success.
     */
    int32_t allocValidRow(const char* topo_cluster_name, std::vector<ServiceNode*>& node_vec,
                          cm_basic::ProtocolType protocol = cm_basic::PT_ANY, LBPolicyType policy = LB_ROUNDROBIN,
                          cm_basic::IDCType idc = cm_basic::IDC_ANY, uint32_t locating_signal = 0);

    int32_t allocValidVersionRow(std::vector<ServiceNode*>& node_vec, long& version, const char* topo_cluster_name,
                                 cm_basic::ProtocolType protocol = cm_basic::PT_ANY,
                                 LBPolicyType policy = LB_ROUNDROBIN, uint32_t locating_signal = 0);
    /**
     * @brief: 获取一个 TopoCluster 的一个分区的全部节点
     * @param: topo_cluster_name 表示拓扑结构集群的集群名
     * @param: partition_id    分区号，0 ~ getNodeCntOfPartition()-1
     * @param: node ServiceNode节点数组指针，内部申请外部释放
     * @param: protocol 要求节点的协议类型
     * @return: 0 is success.
     */
    int32_t allocNodeOfPartition(const char* topo_cluster_name, int32_t partition_id,
                                 std::vector<ServiceNode*>& node_vec,
                                 cm_basic::ProtocolType protocol = cm_basic::PT_ANY,
                                 cm_basic::IDCType idc = cm_basic::IDC_ANY);
    /**
     * @brief: 获取一个 TopoCluster 的一个分区的全部节点
     * @param: topo_cluster_name 表示拓扑结构集群的集群名
     * @param: partition_id    分区号，0 ~ getNodeCntOfPartition()-1
     * @param: node ServiceNode节点数组指针，内部申请外部释放
     * @param: protocol 要求节点的协议类型
     * @return: 0 is success.
     */
    int32_t allocValidNodeOfPartition(const char* topo_cluster_name, int32_t partition_id,
                                      std::vector<ServiceNode*>& node_vec,
                                      cm_basic::ProtocolType protocol = cm_basic::PT_ANY,
                                      cm_basic::IDCType idc = cm_basic::IDC_ANY);

    /**
     * @brief: 获取一个 TopoCluster 的某个分区中分配一个的节点
     * @param: topo_cluster_name 表示拓扑结构集群的集群名
     * @param: partition_id 分区号，0 ~ getNodeCntOfPartition()-1
     * @param: node ServiceNode节点指针，内部申请外部释放
     * @param: protocol 要求节点的协议类型
     * @param: policy 节点的分配策略
     * @param: locating_signal 选择行的依据（例如：按q哈希）
     * @return: 0 is success.
     */
    int32_t allocNodeByPartitionId(const char* topo_cluster_name, int32_t partition_id, ServiceNode*& node,
                                   cm_basic::ProtocolType protocol = cm_basic::PT_ANY,
                                   LBPolicyType policy = LB_ROUNDROBIN, cm_basic::IDCType idc = cm_basic::IDC_ANY,
                                   uint32_t locating_signal = 0);

    /**
     * @brief: 获取一个 TopoCluster 的某个分区中分配一个的节点
     * @param: topo_cluster_name 表示拓扑结构集群的集群名
     * @param: partition_id 分区号，0 ~ getNodeCntOfPartition()-1
     * @param: node ServiceNode节点指针，内部申请外部释放
     * @param: protocol 要求节点的协议类型
     * @param: policy 节点的分配策略
     * @param: locating_signal 选择行的依据（例如：按q哈希）
     * @return: 0 is success.
     */
    int32_t allocValidNodeByPartitionId(const char* topo_cluster_name, int32_t partition_id, ServiceNode*& node,
                                        cm_basic::ProtocolType protocol = cm_basic::PT_ANY,
                                        LBPolicyType policy = LB_ROUNDROBIN, cm_basic::IDCType idc = cm_basic::IDC_ANY,
                                        uint32_t locating_signal = 0);

    int32_t allocValidNodeByPartitionIdUint64(const char* topo_cluster_name, int32_t partition_id, ServiceNode*& node,
                                              cm_basic::ProtocolType protocol = cm_basic::PT_ANY,
                                              LBPolicyType policy = LB_ROUNDROBIN,
                                              cm_basic::IDCType idc = cm_basic::IDC_ANY, uint64_t locating_signal = 0);
    /**
     * @brief: 获取集群所有有效节点
     * @param: topo_cluster_name 表示拓扑结构集群的集群名
     * @param: node ServiceNode节点数组指针，内部申请外部释放
     * @param: valid 是否申请有效节点，默认不指定申请的节点状态
     * @return: 0 is success.
     */
    int32_t allocAllNode(const char* topo_cluster_name, std::vector<ServiceNode*>& node_vec, bool valid = false);

    size_t getQueriedClusters(std::vector<std::string>& names);

private:
    // 根据集群名和列号 获得一个Partition
    void onClusterQuery(const std::string& cluster_name);

private:
    // <cluster name, query timestamp>
    typedef std::map<std::string, int64_t> QueriedClusters;
    QueriedClusters _queriedClusters;
    autil::ThreadMutex _queriedMutex;

    friend class ClusterManagerTest;
};

} // namespace cm_sub
#endif
