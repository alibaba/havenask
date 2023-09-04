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
#ifndef __CM_SUB_TOPO_PARTITION_H_
#define __CM_SUB_TOPO_PARTITION_H_

#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_common.pb.h"
#include "aios/apps/facility/cm2/cm_sub/conhash/weight_conhash.h"
#include "aios/apps/facility/cm2/cm_sub/topo_node.h"
#include "aios/apps/facility/cm2/cm_sub/wrr.h"
#include "autil/Lock.h"

namespace cm_sub {
class TopoPartition
{
public:
    TopoPartition();
    TopoPartition(const std::shared_ptr<TopoPartition>& t);
    ~TopoPartition();

public:
    void addNode(const std::shared_ptr<TopoNode>& node);
    size_t getNodeSize();
    size_t getValidNodeSize();
    double getValidNodeRatio();
    std::shared_ptr<TopoNode> getNodeByIndex(size_t i);
    std::shared_ptr<TopoNode> getNodeBySpec(const std::string& spec);

    cm_basic::NodeStatus getNodeStatusByIndex(size_t i);
    void setNodeStatusByIndex(size_t i, cm_basic::NodeStatus status);

public:
    std::vector<std::shared_ptr<TopoNode>> allocAllNode(bool valid);
    std::vector<std::shared_ptr<TopoNode>> allocNode(cm_basic::ProtocolType protocol, cm_basic::IDCType idc);
    std::vector<std::shared_ptr<TopoNode>> allocValidNode(cm_basic::ProtocolType protocol, cm_basic::IDCType idc);

public:
    void merge(const std::string& clusterName, const std::shared_ptr<TopoPartition>& from);
    std::shared_ptr<TopoPartition> split(const std::string& clusterName);

    void initLBPolicyInfo();

    std::vector<int> getNodeWeights();
    void getIDCNodeWeight(int32_t& validWeight, int32_t& totalWeight, cm_basic::ProtocolType protocol,
                          cm_basic::IDCType idc);

    int32_t getNextNodeId(cm_basic::ProtocolType protocol, cm_basic::IDCType idc, int32_t nid);
    int32_t getNextValidNodeId(cm_basic::ProtocolType protocol, cm_basic::IDCType idc, int32_t nid);

    void printTopo();

    void setIDCPreferRatioNew(int32_t idcPreferRatioNew) { _idcPreferRatioNew = idcPreferRatioNew; }
    int32_t getIDCPreferRatioNew() { return _idcPreferRatioNew; }

    void setCloseIDCPreferNew(bool closeIDCPreferNew) { _closeIdcPreferNew = closeIDCPreferNew; }
    bool isCloseIDCPreferNew() { return _closeIdcPreferNew; }

public:
    enum { DefaultLBWeight = 100 };

private:
    int32_t _idcPreferRatioNew; // 本机房最小有效节点数量[0,100]
    bool _closeIdcPreferNew;    // 强制关闭本机房优先策略

    autil::ReadWriteLock _rwLock;
    std::vector<std::shared_ptr<TopoNode>> _nodeVec;  // 节点指针数组
    std::vector<cm_basic::NodeStatus> _nodeStatusVec; // 节点状态数组
    std::map<std::string, std::shared_ptr<TopoNode>> _spec2Nodes;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_sub

#endif // __CM_SUB_TOPO_PARTITION_H_