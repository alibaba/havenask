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
/*
 * =====================================================================================
 *
 *       Filename:  cm_central.h
 *
 *    Description:  集群 与 集群内部结点 操作基础库
 *
 *        Version:  0.1
 *        Created:  2012-07-19 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 *
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 *
 * =====================================================================================
 */

#ifndef __CM_CENTRAL_H_
#define __CM_CENTRAL_H_

#include <functional>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_cluster_wrapper.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_node.pb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_subscriber.pb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/serialized_cluster.h"
#include "autil/Lock.h"

namespace cm_basic {

enum EPrintClusterType {
    // 只打印集群名字列表
    egClusterName = 0,
    // 只打印集群信息，不需要node具体信息
    egClusterGroup,
    // 打印所有node信息
    egClusterNode,
    // 未知操作
    eGUnknown
};

class CMCentral
{
    friend class CMCentralTest;

public:
    explicit CMCentral() {};
    CMCentral(const CMCentral&) {};
    CMCentral& operator=(const CMCentral&);
    virtual ~CMCentral();

public:
    // 清空所有集群
    void clear();
    // 删除一个 CMCluster
    RespStatus delCluster(const std::string& cluster_name);
    // 通过集群名 复制一个集群，内部申请，外部释放
    RespStatus cloneCluster(const std::string& cluster_name, CMCluster*& cm_cluster);
    // 检查cm_cluster的数据合法性
    RespStatus checkCluster(const std::string& cluster_name);
    // 获取所有集群
    std::vector<CMClusterWrapperPtr> getAllCluster();
    // 获得所有集群名
    int32_t getAllClusterName(std::vector<std::string>& cluster_name_vec);
    // 查询cluster
    CMClusterWrapperPtr getClusterWrapper(const std::string& cluster_name);
    std::set<std::string> getClusterNamesByBelongs(const std::set<std::string>& belongs);
    int64_t getClusterVersion(const std::string& cluster_name);

public:
    // 根据节点 spec, 得到相应的CMNode，线程安全，内部申请外部释放
    CMNode* cloneNode(const std::string& node_spec, const std::string& rstr_cluster_name);
    // check node exist
    RespStatus checkNode(const std::string& node_spec, const std::string& cluster_name);
    // check node group id valid
    RespStatus checkNode(const CMNode* node);
    // 获得集群内部所有结点，内部申请，外部释放
    int32_t getClusterNodeList(const std::string& cluster_name, std::vector<CMNode*>& node_vec);

public: // for test
    // 增加一个 CMCluster;  0:成功； -1:失败(需要释放cm_cluster)
    int32_t addCluster(CMCluster* cm_cluster);
    // 添加节点，添加前需要保证集群和组存在
    RespStatus addNode(CMNode* node);
    // 删除节点，如果节点不存在也返回成功;
    int32_t delNode(const std::string& node_spec, const std::string& rstr_cluster_name);

public:
    /**
     * @brief 打印cluster的信息，在cm_ctrl的命令里会用到
     * @param cm_cluster[in] 要打印的cluster
     */
    static void printCluster(const CMCluster& cm_cluster, EPrintClusterType e_print_type = egClusterNode);
    /**
     * @brief 打印group的信息，在cm_ctrl的命令里会用到
     * @param cm_group[in] 要打印的group
     */
    static void printGroup(const CMGroup& cm_group);
    /**
     * @brief 打印node中会用到，在cm_ctrl的命令里会用到
     * @param cm_node[in] 要打印的 node
     */
    static void printNode(const CMNode& cm_node);

protected:
    mutable autil::ReadWriteLock _rwlock;
    std::map<std::string, CMClusterWrapperPtr> _cluster_map;
};

} // namespace cm_basic
#endif
