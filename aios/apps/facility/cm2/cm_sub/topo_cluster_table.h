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
#ifndef __TOPO_CLUSTER_TABLE_H_
#define __TOPO_CLUSTER_TABLE_H_

#include <list>
#include <map>
#include <set>

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
class TopoClusterTableImpl;

// manage topo clusters
class TopoClusterTable
{
public:
    TopoClusterTable(int hashWeightRatio = 0, uint32_t virtualNodeRatio = 20);
    ~TopoClusterTable();

public:
    // should not concurrent call build topo in diffrent thread
    int32_t buildTopo(cm_basic::CMClusterWrapperPtr ref);
    void updateTopoClusterStatus(cm_basic::ClusterUpdateInfo* msg, cm_basic::OnlineStatus curStatus);
    void markTopoDeleted(const char* cmClusterName);

    void setUpdateTime() { _updateTime = autil::TimeUtility::currentTime(); }

    int32_t getTopoClusterNames(const std::string& cmClusterName, std::vector<std::string>& topoClusterNames);
    std::shared_ptr<TopoCluster> getTopoCluster(const char* topoClusterName);

    void printTopo(const char* cmClusterName);

public:
    // for test
    int64_t getUpdateTime() const { return _updateTime; }
    int32_t delTopo(const char* cmClusterName);
    int32_t getTopoClusterCount();
    std::map<std::string, std::shared_ptr<TopoCluster>> getTopoClusters();
    int32_t getConhashWeightRatio();

private:
    // currentImpl和backupImpl使用同一套TopoCluster
    // 每次重新buildTopo都会在backup新建TopoCluster
    // 然后将更新后的TopoCluster同步给current
    mutable autil::ReadWriteLock _rwlock;
    std::shared_ptr<TopoClusterTableImpl> _currentImpl;
    std::shared_ptr<TopoClusterTableImpl> _backupImpl;
    volatile int64_t _updateTime;

private:
    friend class TopoClusterTableTest;
    friend class ClusterManagerTest;
    AUTIL_LOG_DECLARE();
};

} // namespace cm_sub

#endif
