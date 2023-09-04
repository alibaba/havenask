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
#ifndef __SUB_RESP_MSG_PROCESS_H_
#define __SUB_RESP_MSG_PROCESS_H_

#include <map>
#include <stdint.h>
#include <string>

#include "autil/Log.h"

namespace cm_basic {
class CMCentralSub;
class SubRespMsg;
class ClusterUpdateInfo;
class CMCluster;
} // namespace cm_basic

namespace cm_sub {

class TopoClusterManager;

class SubRespMsgProcess
{
protected:
    typedef std::map<std::string, int64_t> MapClusterName2Version; // 集群名(表名) 到 集群版本
public:
    SubRespMsgProcess(bool disableTopo = false) : _topoClusterManager(NULL), _cmCentral(NULL), _disableTopo(disableTopo)
    {
    }

    SubRespMsgProcess(TopoClusterManager* topo_cluster_manager, cm_basic::CMCentralSub* cm_central)
        : _topoClusterManager(topo_cluster_manager)
        , _cmCentral(cm_central)
    {
    }
    virtual ~SubRespMsgProcess() {}
    //
    void setTopoClusterManager(TopoClusterManager* topo_cluster_manager);
    TopoClusterManager* getTopoClusterManager() { return _topoClusterManager; }
    void setCMCentral(cm_basic::CMCentralSub* cm_central);
    cm_basic::CMCentralSub* getCMCentral() { return _cmCentral; }

    MapClusterName2Version& getMapClusterName2Version() { return _mapClusterName2Version; }
    // 获取集群版本号
    int64_t getClusterVersion(const char* cluster_name);
    // 更新所有集群的信息
    virtual int32_t updateAllClusterInfo(cm_basic::SubRespMsg* resp_msg);
    //
    void printTopo(const char* topo_cluster_name);

protected:
    // 更新集群的状态信息
    int32_t updateClusterStatus(cm_basic::ClusterUpdateInfo* msg);
    // 更新集群的结构信息
    int32_t updateTopoCluster(cm_basic::ClusterUpdateInfo* msg);

private:
    // 删除集群
    int32_t delCluster(cm_basic::ClusterUpdateInfo* msg);
    // 重建集群前需要更新 集群内部节点的状态
    int32_t reinitNodeStatus(cm_basic::CMCluster* cm_cluster);
    // 重建集群 并且更新集群版本
    int32_t reinitCluster(cm_basic::ClusterUpdateInfo* msg);
    bool isClusterEmpty(cm_basic::CMCluster* cm_cluster);

protected:
    MapClusterName2Version _mapClusterName2Version;

private:
    TopoClusterManager* _topoClusterManager;
    cm_basic::CMCentralSub* _cmCentral;
    bool _disableTopo;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_sub
#endif
