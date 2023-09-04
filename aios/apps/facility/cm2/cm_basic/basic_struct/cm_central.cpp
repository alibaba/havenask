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
#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central.h"

#include "aios/apps/facility/cm2/cm_basic/common/common.h"
#include "aios/apps/facility/cm2/cm_basic/util/str_util.h"
#include "autil/TimeUtility.h"

namespace cm_basic {

CMCentral::~CMCentral() { clear(); }

void CMCentral::clear()
{
    autil::ScopedWriteLock lock(_rwlock);
    _cluster_map.clear();
}

int32_t CMCentral::addCluster(CMCluster* cm_cluster)
{
    if (unlikely(NULL == cm_cluster)) {
        return -1;
    }
    autil::ScopedWriteLock lock(_rwlock);
    auto iter = _cluster_map.find(cm_cluster->cluster_name());
    if (iter != _cluster_map.end()) {
        return -1;
    }
    CMClusterWrapperPtr cluster_wrapper(new CMClusterWrapper(cm_cluster));
    _cluster_map[cm_cluster->cluster_name()] = cluster_wrapper;
    return 0;
}

RespStatus CMCentral::delCluster(const std::string& cluster_name)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cluster_map.erase(cluster_name);
    return RSP_SUCCESS;
}

RespStatus CMCentral::cloneCluster(const std::string& cluster_name, CMCluster*& cm_cluster)
{
    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (cluster_wrapper == nullptr) {
        return RSP_CLUSTER_NOT_EXIST;
    }
    return cluster_wrapper->cloneCluster(cm_cluster);
}

RespStatus CMCentral::checkCluster(const std::string& cluster_name)
{
    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (unlikely(nullptr == cluster_wrapper)) {
        return RSP_CLUSTER_NOT_EXIST;
    }
    return RSP_SUCCESS;
}

CMClusterWrapperPtr CMCentral::getClusterWrapper(const std::string& cluster_name)
{
    autil::ScopedReadLock lock(_rwlock);
    auto iter = _cluster_map.find(cluster_name);
    if (iter == _cluster_map.end()) {
        return CMClusterWrapperPtr();
    }
    return iter->second;
}

std::vector<CMClusterWrapperPtr> CMCentral::getAllCluster()
{
    std::vector<CMClusterWrapperPtr> clusterVec;
    {
        autil::ScopedReadLock lock(_rwlock);
        clusterVec.reserve(_cluster_map.size());
        for (auto& iter : _cluster_map) {
            clusterVec.emplace_back(iter.second);
        }
    }
    return clusterVec;
}

int32_t CMCentral::getAllClusterName(std::vector<std::string>& cluster_name_vec)
{
    autil::ScopedReadLock lock(_rwlock);
    for (auto& iter : _cluster_map) {
        cluster_name_vec.emplace_back(iter.first);
    }
    return 0;
}

std::set<std::string> CMCentral::getClusterNamesByBelongs(const std::set<std::string>& belongs)
{
    autil::ScopedReadLock lock(_rwlock);
    std::set<std::string> names;
    for (auto& iter : _cluster_map) {
        if (iter.second->hasBelongClusters(belongs)) {
            names.insert(iter.first);
        }
    }
    return names;
}

int64_t CMCentral::getClusterVersion(const std::string& cluster_name)
{
    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (cluster_wrapper == nullptr) {
        return 0;
    }
    return cluster_wrapper->getClusterVersion();
}

RespStatus CMCentral::addNode(CMNode* node)
{
    RespStatus ret = checkNode(node);
    if (ret != RSP_SUCCESS) {
        return ret;
    }
    auto cluster_wrapper = getClusterWrapper(node->cluster_name());
    if (cluster_wrapper == nullptr) {
        return RSP_CLUSTER_NOT_EXIST;
    }

    // clone cluster and add node
    int64_t cur_time = autil::TimeUtility::currentTime();
    CMCluster* cluster = nullptr;
    cluster_wrapper->cloneCluster(cluster);
    std::string node_spec;
    CMNodeWrapper::makeSpec(node, node_spec);

    autil::ScopedWriteLock lock(_rwlock);
    for (size_t group_index = 0; group_index < cluster->group_vec_size(); group_index++) {
        CMGroup* group = cluster->mutable_group_vec(group_index);
        for (size_t node_index = 0; node_index < group->node_vec_size(); node_index++) {
            CMNode* cm_node = group->mutable_node_vec(node_index);
            std::string tmp_spec;
            CMNodeWrapper::makeSpec(cm_node, tmp_spec);
            if (node_spec.compare(tmp_spec) != 0) {
                continue;
            }
            // 删除同名节点
            group->mutable_node_vec()->SwapElements(node_index, group->node_vec_size() - 1);
            group->mutable_node_vec()->RemoveLast();
            cluster->set_node_num(cluster->node_num() - 1);
            break;
        }
    }
    // 增加节点
    cluster->set_version(cur_time);
    cluster->set_node_num(cluster->node_num() + 1);
    CMNode* cm_node = cluster->mutable_group_vec(node->group_id())->add_node_vec();
    cm_node->CopyFrom(*node);

    // 更新cluster
    CMClusterWrapperPtr cw(new CMClusterWrapper(cluster));
    _cluster_map[node->cluster_name()] = cw;
    return RSP_SUCCESS;
}

int32_t CMCentral::delNode(const std::string& node_spec, const std::string& rstr_cluster_name)
{
    auto cluster_wrapper = getClusterWrapper(rstr_cluster_name);
    if (cluster_wrapper == nullptr) {
        return 0;
    }
    // clone cluster and add node
    int64_t cur_time = autil::TimeUtility::currentTime();
    CMCluster* cluster = nullptr;
    cluster_wrapper->cloneCluster(cluster);

    autil::ScopedWriteLock lock(_rwlock);
    for (size_t group_index = 0; group_index < cluster->group_vec_size(); group_index++) {
        CMGroup* group = cluster->mutable_group_vec(group_index);
        for (size_t node_index = 0; node_index < group->node_vec_size(); node_index++) {
            CMNode* cm_node = group->mutable_node_vec(node_index);
            std::string tmp_spec;
            CMNodeWrapper::makeSpec(cm_node, tmp_spec);
            if (node_spec.compare(tmp_spec) != 0) {
                continue;
            }
            // 删除同名节点
            group->mutable_node_vec()->SwapElements(node_index, group->node_vec_size() - 1);
            group->mutable_node_vec()->RemoveLast();
            cluster->set_node_num(cluster->node_num() - 1);
            break;
        }
    }
    cluster->set_version(cur_time);
    CMClusterWrapperPtr cw(new CMClusterWrapper(cluster));
    _cluster_map[rstr_cluster_name] = cw;
    return RSP_SUCCESS;
}

CMNode* CMCentral::cloneNode(const std::string& node_spec, const std::string& rstr_cluster_name)
{
    auto cluster_wrapper = getClusterWrapper(rstr_cluster_name);
    if (cluster_wrapper == nullptr) {
        return nullptr;
    }
    return cluster_wrapper->cloneNode(node_spec);
}

int32_t CMCentral::getClusterNodeList(const std::string& cluster_name, std::vector<CMNode*>& node_vec)
{
    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (cluster_wrapper == nullptr) {
        return -1;
    }
    return cluster_wrapper->getClusterNodeList(node_vec);
}

RespStatus CMCentral::checkNode(const std::string& node_spec, const std::string& cluster_name)
{
    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (cluster_wrapper == nullptr) {
        return RSP_NODE_NOT_EXIST;
    }
    if (nullptr == cluster_wrapper->findNode(node_spec)) {
        return RSP_NODE_NOT_EXIST;
    }
    return RSP_SUCCESS;
}

RespStatus CMCentral::checkNode(const CMNode* node)
{
    if (unlikely(NULL == node)) {
        return RSP_FAILED;
    }
    auto cluster_wrapper = getClusterWrapper(node->cluster_name());
    if (cluster_wrapper == nullptr) {
        return RSP_CLUSTER_NOT_EXIST;
    }

    // 判断node对应的groupid是在范围内
    if (node->group_id() >= cluster_wrapper->getClusterGroupVecSize()) {
        return RSP_PARTITION_NOT_EXIST;
    }
    return RSP_SUCCESS;
}

void CMCentral::printCluster(const CMCluster& cm_cluster, EPrintClusterType e_print_type)
{
    fprintf(stdout, "================== cluster_name:%s ==================\n", cm_cluster.cluster_name().c_str());
    fprintf(stdout, "\tcreate_time:%lu\n", cm_cluster.create_time());
    fprintf(stdout, "\tversion:%ld\n", cm_cluster.version());
    fprintf(stdout, "\tcluster_status:%s\n", enum_online_status[cm_cluster.cluster_status()]);
    fprintf(stdout, "\tcheck_type:%s\n", enum_check_type[cm_cluster.check_type()]);
    fprintf(stdout, "\ttopo_type:%s\n", enum_topo_type[cm_cluster.topo_type()]);
    fprintf(stdout, "\tgroup_cnt:%d, node_cnt:%d\n", cm_cluster.group_vec_size(), cm_cluster.node_num());
    if (cm_cluster.has_node_limits()) {
        const NodeLimit& cluster_limit = cm_cluster.node_limits();
        if (cluster_limit.has_cpu_busy_limit()) {
            fprintf(stdout, "\tcpu_busy_limit:%d", cluster_limit.cpu_busy_limit());
        }
        if (cluster_limit.has_load_one_limit()) {
            fprintf(stdout, "\tload_one_limit:%d", cluster_limit.load_one_limit());
        }
        if (cluster_limit.has_iowait_limit()) {
            fprintf(stdout, "\tiowait_limit:%d", cluster_limit.iowait_limit());
        }
        if (cluster_limit.has_latency_limit()) {
            fprintf(stdout, "\tlatency_limit:%d", cluster_limit.latency_limit());
        }
        if (cluster_limit.has_qps_limit()) {
            fprintf(stdout, "\tqps_limit:%d", cluster_limit.qps_limit());
        }
        fprintf(stdout, "\n");
    }
    if (cm_cluster.has_check_filename()) {
        fprintf(stdout, "\tcheck_filename:%s\n", cm_cluster.check_filename().c_str());
    }
    if (cm_cluster.has_idc_prefer_ratio()) {
        fprintf(stdout, "\tidc_prefer_ratio:%d\n", cm_cluster.idc_prefer_ratio());
    }
    if (cm_cluster.has_idc_prefer_ratio_new()) {
        fprintf(stdout, "\tidc_prefer_ratio_new:%d\n", cm_cluster.idc_prefer_ratio_new());
    }
    if (cm_cluster.has_close_idc_prefer_new()) {
        fprintf(stdout, "\tclose_idc_prefer_new:%s\n", (cm_cluster.close_idc_prefer_new() ? "true" : "false"));
    }
    if (cm_cluster.belong_vec_size() > 0) {
        fprintf(stdout, "\tbelong:%s\n", joinStr(cm_cluster.belong_vec()).c_str());
    }
    fprintf(stdout, "\n");
    uint32_t g_cnt = cm_cluster.group_vec_size();
    for (uint32_t i = 0; i < g_cnt; ++i) {
        auto& cm_group = cm_cluster.group_vec(i);
        fprintf(stdout, "\tgroup:%d, node_num:%d\n", i, cm_group.node_vec_size());
        if (egClusterNode == e_print_type) {
            printGroup(cm_group);
        }
    }
}

void CMCentral::printGroup(const CMGroup& cm_group)
{
    uint32_t n_cnt = cm_group.node_vec_size();
    for (uint32_t j = 0; j < n_cnt; ++j) {
        auto& cm_node = cm_group.node_vec(j);
        printNode(cm_node);
    }
}

void CMCentral::printNode(const CMNode& cm_node)
{
    fprintf(stdout, "\tnode_spec:%s", cm_node.node_ip().c_str());
    uint32_t port_cnt = cm_node.proto_port_size();
    for (uint32_t k = 0; k < port_cnt; ++k) {
        ProtocolType type = cm_node.proto_port(k).protocol();
        fprintf(stdout, "|%s:%d", enum_protocol_type[type], cm_node.proto_port(k).port());
    }
    fprintf(stdout, "\n");
    fprintf(stdout, "\tcluster_name:%s\n", cm_node.cluster_name().c_str());
    fprintf(stdout, "\tgroup_id:%d\n", cm_node.group_id());
    fprintf(stdout, "\tonline_status:%s\n", enum_online_status[cm_node.online_status()]);
    fprintf(stdout, "\tcur_status:%s\n", enum_node_status[cm_node.cur_status()]);
    fprintf(stdout, "\tprev_status:%s\n", enum_node_status[cm_node.prev_status()]);
    fprintf(stdout, "\tvalid_time:%ld\n", cm_node.valid_time());
    fprintf(stdout, "\theartbeat_time:%ld\n", cm_node.heartbeat_time());
    fprintf(stdout, "\tstart_time:%ld\n", cm_node.start_time());
    if (cm_node.has_idc_type()) {
        fprintf(stdout, "\tidc_type:%d\n", (int)cm_node.idc_type());
    }
    if (cm_node.has_lb_info()) {
        if (cm_node.lb_info().has_cpu_busy()) {
            fprintf(stdout, "\tcpu_busy:%d\n", cm_node.lb_info().cpu_busy());
        }
        if (cm_node.lb_info().has_load_one()) {
            fprintf(stdout, "\tload_one:%d\n", cm_node.lb_info().load_one());
        }
        if (cm_node.lb_info().has_iowait()) {
            fprintf(stdout, "\tio_wait:%d\n", cm_node.lb_info().iowait());
        }
        if (cm_node.lb_info().has_weight()) {
            fprintf(stdout, "\tweight:%d\n", cm_node.lb_info().weight());
        }
        if (cm_node.lb_info().has_latency()) {
            fprintf(stdout, "\tlatency:%d\n", cm_node.lb_info().latency());
        }
        if (cm_node.lb_info().has_qps()) {
            fprintf(stdout, "\tqps:%d\n", cm_node.lb_info().qps());
        }
    }
    if (cm_node.has_topo_info()) {
        fprintf(stdout, "\ttopo_info:%s\n", cm_node.topo_info().c_str());
    }
    if (cm_node.has_meta_info()) {
        const NodeMetaInfo& node_meta_info = cm_node.meta_info();
        std::string mstr;
        for (int32_t i = 0; i < node_meta_info.kv_array_size(); ++i) {
            std::string str = node_meta_info.kv_array(i).meta_key();
            if (node_meta_info.kv_array(i).has_meta_value())
                str += (":" + node_meta_info.kv_array(i).meta_value());
            if (mstr.length() > 0)
                mstr += ("|" + str);
            else
                mstr = str;
        }
        fprintf(stdout, "\tmeta_info:%s", mstr.c_str());
        fprintf(stdout, "\n");
    }
    fprintf(stdout, "\n");
}

} // namespace cm_basic
