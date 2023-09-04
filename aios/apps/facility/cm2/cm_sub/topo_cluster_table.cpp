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
#include "aios/apps/facility/cm2/cm_sub/topo_cluster_table.h"

#include <assert.h>

#include "aios/apps/facility/cm2/cm_basic/util/str_util.h"
#include "aios/apps/facility/cm2/cm_sub/topo_cluster.h"
#include "aios/apps/facility/cm2/cm_sub/topo_cluster_table_impl.h"
#include "autil/TimeUtility.h"

namespace cm_sub {

using namespace cm_basic;

AUTIL_LOG_SETUP(cm_sub, TopoClusterTable);

TopoClusterTable::TopoClusterTable(int hashWeightRatio, uint32_t virtualNodeRatio)
{
    _currentImpl = std::make_shared<TopoClusterTableImpl>(hashWeightRatio, virtualNodeRatio);
    _backupImpl = std::make_shared<TopoClusterTableImpl>(hashWeightRatio, virtualNodeRatio);
    _updateTime = 0;
}

TopoClusterTable::~TopoClusterTable() {}

int32_t TopoClusterTable::buildTopo(CMClusterWrapperPtr cmCluster)
{
    if (cmCluster == nullptr) {
        return -1;
    }

    if (_backupImpl->buildTopo(cmCluster) != 0) {
        return -1;
    }

    {
        autil::ScopedWriteLock lock(_rwlock);
        _currentImpl.swap(_backupImpl);
    }
    _backupImpl->syncTopoClusters(cmCluster->getClusterName(), _currentImpl);
    _updateTime = autil::TimeUtility::currentTime();
    return 0;
}

int32_t TopoClusterTable::delTopo(const char* cmClusterName)
{
    if (cmClusterName == nullptr) {
        return -1;
    }

    if (_backupImpl->deleteTopo(cmClusterName) != 0) {
        return -1;
    }
    {
        autil::ScopedWriteLock lock(_rwlock);
        _currentImpl.swap(_backupImpl);
    }
    _backupImpl->syncTopoClusters(cmClusterName, _currentImpl);
    _updateTime = autil::TimeUtility::currentTime();
    return 0;
}

void TopoClusterTable::markTopoDeleted(const char* cmClusterName)
{
    if (cmClusterName == nullptr) {
        return;
    }
    // 这里的实现无法保证backup一定被正确更新
    // 但是因为只有一个更新线程,
    // 所以不会出现同时更新backup和clusterdeleted的情况
    autil::ScopedReadLock lock(_rwlock);
    _currentImpl->markClusterDeleted(cmClusterName);
}

std::shared_ptr<TopoCluster> TopoClusterTable::getTopoCluster(const char* topoClusterName)
{
    autil::ScopedReadLock lock(_rwlock);
    return _currentImpl->getTopoCluster(topoClusterName);
}

// topo cluster status use the latest cluster status
void TopoClusterTable::updateTopoClusterStatus(cm_basic::ClusterUpdateInfo* msg, cm_basic::OnlineStatus cur_status)
{
    // 这里的实现无法保证backup一定被正确更新
    // 但是因为只有一个更新线程
    // 所以不会出现同时更新backup和cluster status的情况
    autil::ScopedReadLock lock(_rwlock);
    if (msg->has_node_status_vec() && msg->node_status_vec().cluster_status() != cur_status) {
        AUTIL_LOG(INFO, "cluster[%s] status changed, %d to %d", msg->cluster_name().c_str(), cur_status,
                  msg->node_status_vec().cluster_status());
        _currentImpl->updateTopoClusterStatus(msg->cluster_name(), msg->node_status_vec().cluster_status());
    }
}

int32_t TopoClusterTable::getTopoClusterNames(const std::string& cmClusterName,
                                              std::vector<std::string>& topoClusterNames)
{
    autil::ScopedReadLock lock(_rwlock);
    _currentImpl->getTopoClusterNames(cmClusterName, topoClusterNames);
    return 0;
}

int32_t TopoClusterTable::getTopoClusterCount()
{
    autil::ScopedReadLock lock(_rwlock);
    return _currentImpl->getTopoClusterCount();
}

std::map<std::string, std::shared_ptr<TopoCluster>> TopoClusterTable::getTopoClusters()
{
    autil::ScopedReadLock lock(_rwlock);
    return _currentImpl->getTopoClusters();
}

int32_t TopoClusterTable::getConhashWeightRatio()
{
    autil::ScopedReadLock lock(_rwlock);
    return _currentImpl->getConhashWeightRatio();
}

void TopoClusterTable::printTopo(const char* cluster_name)
{
    if (cluster_name == nullptr) {
        return;
    }

    autil::ScopedReadLock lock(_rwlock);
    _currentImpl->printTopo(cluster_name);
}

} // namespace cm_sub
