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
#ifndef __CM_CLUSTER_WRAPPER_H_
#define __CM_CLUSTER_WRAPPER_H_

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_node_wrapper.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_node.pb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_subscriber.pb.h"
#include "aios/apps/facility/cm2/cm_basic/common/common.h"
#include "autil/Lock.h"

namespace cm_basic {

DEFINE_SHARED_CONST_PTR(CMCluster);

/*
 * CMClusterWrapper 对CMCluster的操作包装, 提供多线程操作接口
 * CMClusterWrapper构建之后, 内部的CMCluster/CMGroup/CMNode不会有增加和减少, 只会更新属性
 * 新增node/减少node操作需要cloneCluster后在新cluster上操作
 */
class CMClusterWrapper
{
public:
    CMClusterWrapper(CMCluster* cm_cluster);
    virtual ~CMClusterWrapper();

private:
    CMClusterWrapper(const CMClusterWrapper&);
    CMClusterWrapper& operator=(const CMClusterWrapper&);

public:
    RespStatus cloneCluster(CMCluster*& cm_cluster);
    CMNode* cloneNode(const std::string& node_spec);
    int32_t getClusterNodeList(std::vector<CMNode*>& node_vec);
    int mergeCluster(CMCluster* cluster, const std::string& cname);
    bool hasBelongClusters(const std::set<std::string>& belongs);
    void CopyTo(CMCluster* cluster);
    bool CopyToGroup(int id, CMGroup* group);
    cm_basic::RespStatus checkNode(const CMNode& node);

public:
    std::map<std::string, CMNodeWrapperPtr> getNodeMap();
    std::vector<CMNodeWrapperPtr> getNodeList();
    const std::vector<cm_basic::CMNodeWrapperPtr>& getNodeListRef();
    std::vector<std::vector<CMNodeWrapperPtr>> getGroupNodeList();
    CMNodeWrapperPtr findNode(const std::string& node_spec);

    size_t getNodeCount();
    size_t getNormalNodeCount();

public:
    void setClusterStatus(OnlineStatus cluster_status);
    void setClusterVersion(int64_t version);

public:
    int64_t getClusterVersion();
    int32_t getClusterGroupVecSize();
    CMCluster::NodeCheckType getClusterCheckType();
    CMCluster::TopoType getClusterTopoType();
    bool getClusterAutoOffline();
    OnlineStatus getClusterStatus();
    NodeLimit getClusterNodeLimits();
    int64_t getClusterCreateTime();
    std::string getClusterName();
    int32_t getClusterNodeNum();
    std::string getClusterCheckFilename();
    int32_t getBelongVecSize();
    std::string getBelong(int32_t i);
    bool hasProtectRatio();
    int32_t getProtectRatio();
    bool hasIDCPreferRatio();
    int32_t getIDCPreferRatio();
    bool hasIDCPreferRatioNew();
    int32_t getIDCPreferRatioNew();
    bool hasCloseIDCPreferNew();
    bool getCloseIDCPreferNew();

public:
    // for some compatible code
    CMCluster* getCMCluster();

private:
    // 添加集群的所有节点到 _node_map
    void initNodeMap();
    // 从cmCluster中删除一个节点
    void eraseNode(CMNodeWrapperPtr& node, const std::string& node_spec);

protected:
    // 直接操作CMCluster / CMNode时, 需要用这个锁
    mutable autil::ReadWriteLock _rwlock;
    CMCluster* _cm_cluster;
    std::map<std::string, CMNodeWrapperPtr> _node_map;
    std::vector<CMNodeWrapperPtr> _node_list;
    std::vector<std::vector<CMNodeWrapperPtr>> _group_node_list;
};

DEFINE_SHARED_PTR(CMClusterWrapper);

} // namespace cm_basic

#endif // __CM_CLUSTER_WRAPPER_H_