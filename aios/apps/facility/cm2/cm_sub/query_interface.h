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
/**
 * =====================================================================================
 *
 *       Filename:  query_interface.h
 *
 *    Description:  客户端基础库，订阅者查询接口
 *
 *        Version:  0.1
 *        Created:  2012-08-07 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 *
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 *
 * =====================================================================================
 */

#ifndef __QUERY_INTERFACE_H_
#define __QUERY_INTERFACE_H_

#include <stdint.h>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_common.pb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_subscriber.pb.h"
#include "aios/apps/facility/cm2/cm_sub/lb_policy/lb_policy.h"
#include "aios/apps/facility/cm2/cm_sub/service_node.h"

namespace cm_basic {
class CMCentralSub;
}

namespace cm_sub {

class TopoClusterManager;
/**
 * @class QueryInterface
 * @brief 订阅者查询接口类
 */
class QueryInterface
{
protected:
    TopoClusterManager* _topoClusterManager; ///< 集群管理
    cm_basic::CMCentralSub* _cmCentral;      ///< cm_cetral

public:
    QueryInterface() : _topoClusterManager(NULL), _cmCentral(NULL) {}
    virtual ~QueryInterface() {}
    /**
     * @brief   ServiceNode节点指针释放函数
     * @param   [in]    node 节点指针
     */
    void freeNode(ServiceNode*& node);
    /**
     * @brief   ServiceNode节点数组指针释放函数
     * @param   [in]    nodes 节点指针数组
     */
    void freeNodes(std::vector<ServiceNode*>& nodes);
    /**
     * @brief   获取一个TopoCluster中的分区数
     * @param   [in]    topo_cluster_name 表示拓扑结构集群的集群名
     * @return  分区数
     */
    int32_t getPartitionCnt(const char* topo_cluster_name);
    /**
     * @brief   获取一个TopoCluster中的某分区的节点数
     * @param   [in]    topo_cluster_name 表示拓扑结构集群的集群名
     * @param   [in]    partition_id 表示分区号
     * @return  分区内的节点数
     */
    int32_t getNodeCntOfPartition(const char* topo_cluster_name, int32_t partition_id = 0);
    /**
     * @brief   获取一个 TopoCluster 的一行可用节点
     * @param   [out]   node_vec ServiceNode节点数组指针，内部申请外部释放, 如果没有有效节点返回数组大小为0
     * @param   [in]    topo_cluster_name 表示拓扑结构集群的集群名
     * @param   [in]    protocol 要求节点的协议类型
     * @param   [in]    policy 节点的分配策略
     * @param   [in]    idc 节所在的机房
     * @param   [in]    locating_signal 选择行的依据（例如：按q哈希）
     * @return  0 is success.
     */
    int32_t allocValidRow(std::vector<ServiceNode*>& node_vec, const char* topo_cluster_name,
                          cm_basic::ProtocolType protocol = cm_basic::PT_ANY, LBPolicyType policy = LB_ROUNDROBIN,
                          cm_basic::IDCType idc = cm_basic::IDC_ANY, uint32_t locating_signal = 0);

    /*
     * 分配完整的一行，每个节点版本相同，并返回版本。没有完整的一行时返回失败
     */
    int32_t allocValidVersionRow(std::vector<ServiceNode*>& node_vec, long& version, const char* topo_cluster_name,
                                 cm_basic::ProtocolType protocol = cm_basic::PT_ANY,
                                 LBPolicyType policy = LB_ROUNDROBIN, uint32_t locating_signal = 0);
    /**
     * @brief   获取一个 TopoCluster 的一行节点
     * @param   [out]   node_vec ServiceNode节点数组指针，内部申请外部释放, 如果没有节点返回数组大小为0
     * @param   [in]    topo_cluster_name 表示拓扑结构集群的集群名
     * @param   [in]    protocol 要求节点的协议类型
     * @param   [in]    policy 节点的分配策略
     * @param   [in]    idc 节所在的机房
     * @param   [in]    locating_signal 选择行的依据（例如：按q哈希）
     * @return  0 is success.
     */
    int32_t allocRow(std::vector<ServiceNode*>& node_vec, const char* topo_cluster_name,
                     cm_basic::ProtocolType protocol = cm_basic::PT_ANY, LBPolicyType policy = LB_ROUNDROBIN,
                     cm_basic::IDCType idc = cm_basic::IDC_ANY, uint32_t locating_signal = 0);
    /**
     * @brief   获取一个 TopoCluster 的一个分区的全部节点
     * @param   [out]   node_vec ServiceNode节点数组指针，内部申请外部释放, 如果没有节点返回数组大小为0
     * @param   [in]    topo_cluster_name 表示拓扑结构集群的集群名
     * @param   [in]    partition_id 分区号，0 ~ getNodeCntOfPartition()-1
     * @param   [in]    protocol 要求节点的协议类型
     * @param   [in]    idc 节所在的机房
     * @return  0 is success.
     */
    int32_t allocNodeOfPartition(std::vector<ServiceNode*>& node_vec, const char* topo_cluster_name,
                                 int32_t partition_id = 0, cm_basic::ProtocolType protocol = cm_basic::PT_ANY,
                                 cm_basic::IDCType idc = cm_basic::IDC_ANY);
    /**
     * @brief   获取一个 TopoCluster 的一个分区的全部可用节点
     * @param   [out]   node_vec ServiceNode节点数组指针，内部申请外部释放, 如果没有有效节点返回数组大小为0
     * @param   [in]    topo_cluster_name 表示拓扑结构集群的集群名
     * @param   [in]    partition_id 分区号，0 ~ getNodeCntOfPartition()-1
     * @param   [in]    protocol 要求节点的协议类型
     * @param   [in]    idc 节所在的机房
     * @return 0 is success.
     */
    int32_t allocValidNodeOfPartition(std::vector<ServiceNode*>& node_vec, const char* topo_cluster_name,
                                      int32_t partition_id = 0, cm_basic::ProtocolType protocol = cm_basic::PT_ANY,
                                      cm_basic::IDCType idc = cm_basic::IDC_ANY);
    /**
     * @brief   获取一个 TopoCluster 的某个分区中分配一个可用的节点
     * @param   [out]   node ServiceNode节点指针，内部申请外部释放, 如果没有有效节点返回NULL
     * @param   [in]    topo_cluster_name 表示拓扑结构集群的集群名
     * @param   [in]    partition_id 分区号，0 ~ getNodeCntOfPartition()-1
     * @param   [in]    protocol 要求节点的协议类型
     * @param   [in]    policy 节点的分配策略
     * @param   [in]    idc 节所在的机房
     * @param   [in]    locating_signal 选择行的依据（例如：按q哈希）
     * @return  0 is success.
     */
    int32_t allocValidNodeByPartitionId(ServiceNode*& node, const char* topo_cluster_name, int32_t partition_id = 0,
                                        cm_basic::ProtocolType protocol = cm_basic::PT_ANY,
                                        LBPolicyType policy = LB_ROUNDROBIN, cm_basic::IDCType idc = cm_basic::IDC_ANY,
                                        uint32_t locating_signal = 0);

    int32_t allocValidNodeByPartitionIdUint64(ServiceNode*& node, const char* topo_cluster_name,
                                              int32_t partition_id = 0,
                                              cm_basic::ProtocolType protocol = cm_basic::PT_ANY,
                                              LBPolicyType policy = LB_ROUNDROBIN,
                                              cm_basic::IDCType idc = cm_basic::IDC_ANY, uint64_t locating_signal = 0);
    /**
     * @brief   获取一个 TopoCluster 的某个分区中分配一个的节点
     * @param   [out]   node ServiceNode节点指针，内部申请外部释放, 如果没有节点返回NULL
     * @param   [in]    topo_cluster_name 表示拓扑结构集群的集群名
     * @param   [in]    partition_id 分区号，0 ~ getNodeCntOfPartition()-1
     * @param   [in]    protocol 要求节点的协议类型
     * @param   [in]    policy 节点的分配策略
     * @param   [in]    idc 节所在的机房
     * @param   [in]    locating_signal 选择行的依据（例如：按q哈希）
     * @return  0 is success.
     */
    int32_t allocNodeByPartitionId(ServiceNode*& node, const char* topo_cluster_name, int32_t partition_id = 0,
                                   cm_basic::ProtocolType protocol = cm_basic::PT_ANY,
                                   LBPolicyType policy = LB_ROUNDROBIN, cm_basic::IDCType idc = cm_basic::IDC_ANY,
                                   uint32_t locating_signal = 0);
    /**
     * @brief   获取集群所有有效节点
     * @param   [out]   node_vec ServiceNode节点数组指针，内部申请外部释放，如果没有有效节点返回数组大小为0
     * @param   [in]    topo_cluster_name 表示拓扑结构集群的集群名
     * @return  0 is success.
     */
    int32_t allocAllValidNode(std::vector<ServiceNode*>& node_vec, const char* topo_cluster_name);
    /**
     * @brief   获取集群所有节点
     * @param   [out]   node_vec ServiceNode节点数组指针，内部申请外部释放, 如果没有节点返回数组大小为0
     * @param   [in]    topo_cluster_name 表示拓扑结构集群的集群名
     * @return  0 is success.
     */
    int32_t allocAllNode(std::vector<ServiceNode*>& node_vec, const char* topo_cluster_name);
    /**
     * @brief   根据节点spec获取当前节点的状态
     * @param   [out]   node_status 节点的状态信息引用
     * @param   [in]    node_spec 节点spec: 10.232.42.11|tcp:7788|http:2088
     * @param   [in]    cluster_name 物理集群名
     * @return  0 is success.
     */
    int32_t getNodeStatus(cm_basic::NodeStatus& node_status, const char* node_spec, const char* cluster_name);
    /**
     * @brief   根据节点spec获取当前节点的MetaInfo信息
     * @param   [out]   meta_info 节点的MetaInfo信息的指针
     * @param   [in]    node_spec 节点spec: 10.232.42.11|tcp:7788|http:2088
     * @param   [in]    cluster_name 物理集群名
     * @return  0 is success.
     */
    int32_t getNodeMetaInfo(cm_basic::NodeMetaInfo* meta_info, const char* node_spec, const char* cluster_name);
    /**
     * @brief   根据一个CMCluster名字(物理集群)获取对应的TopoCluster名字列表
     * @param   [out]   t_names TopoCluster名字列表
     * @param   [in]    c_name CMCluster名字
     * @return  0 is success.
     */
    int32_t getTopoClusterNames(const std::string& c_name, std::vector<std::string>& t_names);
};

} // namespace cm_sub
#endif
