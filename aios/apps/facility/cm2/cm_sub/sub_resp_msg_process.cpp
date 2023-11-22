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
// #include "autil/ZlibCompressor.h"
#include "aios/apps/facility/cm2/cm_sub/sub_resp_msg_process.h"

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central_sub.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_subscriber.pb.h"
#include "aios/apps/facility/cm2/cm_sub/topo_cluster_manager.h"
#include "snappy.h"

namespace cm_sub {

using namespace cm_basic;

char enum_msg_type[][32] = {"unknown",        "node_status",      "node_lb_info", "node_status_lb_info",
                            "cluster_reinit", "cluster_building", "cluster_del",  "cluster_not_exsit"};

AUTIL_LOG_SETUP(cm_sub, SubRespMsgProcess);

// 更新所有集群的信息
int32_t SubRespMsgProcess::updateAllClusterInfo(SubRespMsg* resp_msg)
{
    int32_t size = resp_msg->update_info_vec_size();
    for (int32_t i = 0; i < size; ++i) {
        ClusterUpdateInfo update_info;

        if (resp_msg->has_compress_type() && resp_msg->compress_type() == CT_SNAPPY) {
            std::string compressed = resp_msg->update_info_vec(i);
            std::string output;
            snappy::Uncompress(compressed.c_str(), compressed.length(), &output);
            update_info.ParseFromString(output);
            AUTIL_LOG(DEBUG, "name:%s, snappy.in_len:%zu, snappy.out_len:%zu", update_info.cluster_name().c_str(),
                      compressed.length(), output.length());
        } else {
            update_info.ParseFromString(resp_msg->update_info_vec(i));
        }

        int64_t version = getClusterVersion(update_info.cluster_name().c_str());
        // Todo 是否需要考虑版本小于当前版本号的情况
        if (version < update_info.cluster_version()) {
            if (updateTopoCluster(&update_info) != 0)
                continue;
        } else {
            updateClusterStatus(&update_info);
        }
    }
    return 0;
}

void SubRespMsgProcess::setTopoClusterManager(TopoClusterManager* topo_cluster_manager)
{
    _topoClusterManager = topo_cluster_manager;
}

void SubRespMsgProcess::setCMCentral(CMCentralSub* cm_central) { _cmCentral = cm_central; }

int64_t SubRespMsgProcess::getClusterVersion(const std::string& cluster_name)
{
    autil::ScopedReadLock lock(_rwlock);
    MapClusterName2Version::iterator it = _mapClusterName2Version.find(cluster_name);
    if (it == _mapClusterName2Version.end()) {
        return -1;
    }
    return it->second;
}

void SubRespMsgProcess::setClusterVersion(const std::string& cluster_name, int64_t version)
{
    autil::ScopedWriteLock lock(_rwlock);
    _mapClusterName2Version[cluster_name] = version;
}

int32_t SubRespMsgProcess::delCluster(ClusterUpdateInfo* msg)
{
    // issue #22
    _topoClusterManager->markTopoDeleted(msg->cluster_name().c_str());
    // 删除集群，只是把集群的版本号设置为-1
    setClusterVersion(msg->cluster_name(), -1);
    return 0;
}

int32_t SubRespMsgProcess::reinitNodeStatus(CMCluster* cm_cluster)
{
    if (cm_cluster == NULL)
        return -1;

    CMGroup* cm_group = NULL;
    CMNode* cm_node = NULL;
    uint32_t g_cnt = cm_cluster->group_vec_size();
    for (uint32_t i = 0; i < g_cnt; ++i) {
        cm_group = cm_cluster->mutable_group_vec(i);
        uint32_t n_cnt = cm_group->node_vec_size();
        for (uint32_t j = 0; j < n_cnt; ++j) {
            cm_node = cm_group->mutable_node_vec(j);
            if (cm_node->online_status() == OS_OFFLINE) {
                cm_node->set_cur_status(NS_UNVALID);
            }
        }
    }
    return 0;
}

bool SubRespMsgProcess::isClusterEmpty(cm_basic::CMCluster* cm_cluster)
{
    for (int i = 0; i < cm_cluster->group_vec_size(); ++i) {
        CMGroup* p_cm_group = cm_cluster->mutable_group_vec(i);
        if (p_cm_group->node_vec_size() > 0)
            return false;
    }
    return true;
}

int32_t SubRespMsgProcess::reinitCluster(ClusterUpdateInfo* msg)
{
    CMClusterWrapperPtr old_ref = _cmCentral->getClusterWrapper(msg->cluster_name());
    // NOTE: only apply to TT_ONE_MAP_ONE cluster, see issue #22
    if (old_ref != NULL && msg->mutable_cm_cluster()->topo_type() == CMCluster::TT_ONE_MAP_ONE &&
        isClusterEmpty(msg->mutable_cm_cluster())) {
        AUTIL_LOG(ERROR, "receive empty cluster message for %s", msg->cluster_name().c_str());
        setClusterVersion(msg->cluster_name(), msg->cluster_version());
        return 0;
    }

    CMCluster* c = new (std::nothrow) CMCluster(msg->cm_cluster());
    if (reinitNodeStatus(c) != 0 || _cmCentral->updateCluster(c) != cm_basic::RSP_SUCCESS) {
        AUTIL_LOG(ERROR, "_cmCentral->updateCluster(%s) != 0", msg->cluster_name().c_str());
        return -1;
    }

    int32_t ret = 0;
    if (!_disableTopo) {
        // 构建内部topo集群
        CMClusterWrapperPtr ref = _cmCentral->getClusterWrapper(msg->cluster_name());
        if (ref == NULL) {
            return -1;
        }
        if (_topoClusterManager->buildTopo(ref) != 0) {
            AUTIL_LOG(ERROR, "_topoClusterManager->buildTopo(%s) failed !!", msg->cluster_name().c_str());
            ret = -1;
        } else {
            // 更新集群版本号
            AUTIL_LOG(INFO, "_topoClusterManager->buildTopo(%s) success ..", msg->cluster_name().c_str());
            setClusterVersion(msg->cluster_name(), msg->cluster_version());
        }
    } else {
        setClusterVersion(msg->cluster_name(), msg->cluster_version());
    }
    return ret;
}

int32_t SubRespMsgProcess::updateTopoCluster(ClusterUpdateInfo* msg)
{
    // 集群的版本更新了
    int32_t ret = 0;
    if (msg->msg_type() == ClusterUpdateInfo::MT_CLUSTER_REINIT) {
        ret = reinitCluster(msg);
        if (ret != 0) {
            AUTIL_LOG(ERROR, "cm_sub : reinit cluster[%s] failed !!", msg->cluster_name().c_str());
            return ret;
        }
        AUTIL_LOG(INFO, "cluster[%s] updated, new_version = %ld", msg->cluster_name().c_str(),
                  msg->cm_cluster().version());
    } else {
        ret = -1;
    }
    return ret;
}

int32_t SubRespMsgProcess::updateClusterStatus(ClusterUpdateInfo* msg)
{
    int32_t ret = 0;
    // 集群版本没有更新，只是集群的状态更新
    switch (msg->msg_type()) {
    case ClusterUpdateInfo::MT_CLUSTER_DEL:
        AUTIL_LOG(INFO, "msg->msg_type(%s), cluster_name = %s", enum_msg_type[msg->msg_type()],
                  msg->cluster_name().c_str());
        ret = delCluster(msg);
        break;

    case ClusterUpdateInfo::MT_NODE_STATUS:
        AUTIL_LOG(DEBUG, "msg->msg_type(%s), cluster_name = %s", enum_msg_type[msg->msg_type()],
                  msg->cluster_name().c_str());
        // 只有一个线程更新 是安全的
        _topoClusterManager->updateTopoClusterStatus(msg, _cmCentral->getClusterStatus(msg->cluster_name()));
        _cmCentral->updateClusterNodeStatus(msg->cluster_name().c_str(), msg->mutable_node_status_vec());
        _topoClusterManager->setUpdateTime();
        break;

    case ClusterUpdateInfo::MT_CLUSTER_FAILOVER:
        AUTIL_LOG(INFO, "cluster %s failover, reset local version", msg->cluster_name().c_str());
        setClusterVersion(msg->cluster_name(), 0);
        break;
    case ClusterUpdateInfo::MT_CLUSTER_NOT_EXSIT:
    case ClusterUpdateInfo::MT_CLUSTER_BUILDING:
    default:
        AUTIL_LOG(INFO, "msg->msg_type(%s), cluster_name = %s, version = %ld", enum_msg_type[msg->msg_type()],
                  msg->cluster_name().c_str(), msg->cluster_version());
        break;
    }
    return ret;
}

void SubRespMsgProcess::printTopo(const char* topo_cluster_name) { _topoClusterManager->printTopo(topo_cluster_name); }

} // namespace cm_sub
