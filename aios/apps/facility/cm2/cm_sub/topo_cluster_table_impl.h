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
#ifndef __CM_SUB_TOPO_CLUSTER_TABLE_IMPL_H_
#define __CM_SUB_TOPO_CLUSTER_TABLE_IMPL_H_

#include <map>
#include <set>
#include <string>

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_cluster_wrapper.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_common.pb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_subscriber.pb.h"
#include "autil/Lock.h"
#include "autil/Log.h"

namespace cm_sub {

class ServiceNode;
class ILBPolicy;
class TopoNode;
class TopoCluster;
class TopoPartition;

/**
 * TopoClusterTableImpl used for build and operate a group of topo clusters.
 * Every time we build topo cluster from cm cluster should always create new topo clusters and do not change old ones.
 */
class TopoClusterTableImpl
{
public:
    TopoClusterTableImpl(int hashWeightRatio, uint32_t virtualNodeRatio)
        : _conhashWeightRatio(hashWeightRatio)
        , _virtualNodeRatio(virtualNodeRatio)
    {
    }
    ~TopoClusterTableImpl() = default;

public:
    int32_t buildTopo(const cm_basic::CMClusterWrapperPtr& cmCluster);
    int32_t deleteTopo(const std::string& cmClusterName);
    void updateTopoClusterStatus(const std::string& cmClusterName, cm_basic::OnlineStatus curStatus);
    void markClusterDeleted(const std::string& cmClusterName);

    // 在两个TopoClusterTableImpl之间同步TopoCluster而不需要重建节点
    void syncTopoClusters(const std::string& cmClusterName, const std::shared_ptr<TopoClusterTableImpl>& from);

    void getTopoClusterNames(const std::string& cmClusterName, std::vector<std::string>& topoClusterNames);
    std::shared_ptr<TopoCluster> getTopoCluster(const std::string& topoClusterName);
    int32_t getTopoClusterCount();
    std::map<std::string, std::shared_ptr<TopoCluster>> getTopoClusters();
    int32_t getConhashWeightRatio();

    void printTopo(const std::string& cmClusterName);

private:
    int32_t updateOneMapOneCMCluster(const cm_basic::CMClusterWrapperPtr& cmCluster);
    std::shared_ptr<TopoCluster> convertOneMapOneCMClusterToTopoCluster(const cm_basic::CMClusterWrapperPtr& cmCluster);

    int32_t updateClusterMapCMCluster(const cm_basic::CMClusterWrapperPtr& cmCluster);
    std::map<std::string, std::shared_ptr<TopoCluster>>
    convertTopoCMClusterToTopoClusters(const cm_basic::CMClusterWrapperPtr& cmCluster);

    int32_t deleteTopoCluster(const std::string& cmClusterName, const std::string& topoClusterName);
    int32_t updateTopoCluster(const std::string& cmClusterName, const std::string& topoClusterName,
                              const std::shared_ptr<TopoCluster>& topoCluster);
    std::map<std::string, std::shared_ptr<TopoCluster>> getTopoClusters(const std::string& cmClusterName);

private:
    int _conhashWeightRatio;
    uint32_t _virtualNodeRatio;

    // _rwlock: used to make sure _clusterNameToTopoClusterNames and _topoClusterMap thread safe
    // not use on TopoCluster inner change
    autil::ReadWriteLock _rwlock;
    std::map<std::string, std::set<std::string>> _clusterNameToTopoClusterNames;
    std::map<std::string, std::shared_ptr<TopoCluster>> _topoClusterMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_sub

#endif //