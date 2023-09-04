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
#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_node_wrapper.h"

namespace cm_basic {

CMNodeWrapper::CMNodeWrapper(const std::string& node_spec, CMNode* node) : _node_spec(node_spec), _cm_node(node)
{
    assert(_cm_node != nullptr);
}

CMNodeWrapper::~CMNodeWrapper() {}

CMNode* CMNodeWrapper::cloneNode()
{
    autil::ScopedReadLock lock(_rwlock);
    CMNode* cm_node = new (std::nothrow) CMNode(*(_cm_node));
    assert(cm_node != nullptr);
    return cm_node;
}

void CMNodeWrapper::setOnlineStatus(OnlineStatus online_status)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->set_online_status(online_status);
}

void CMNodeWrapper::setCurStatus(NodeStatus node_status)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->set_cur_status(node_status);
}

void CMNodeWrapper::setPrevStatus(NodeStatus node_status)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->set_prev_status(node_status);
}

void CMNodeWrapper::setLBInfo(const NodeLBInfo& lb_info)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->mutable_lb_info()->CopyFrom(lb_info);
}

void CMNodeWrapper::setLBInfoWeight(int32_t weight)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->mutable_lb_info()->set_weight(weight);
}

void CMNodeWrapper::setHeartbeatTime(int64_t hb_time)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->set_heartbeat_time(hb_time);
}

void CMNodeWrapper::setValidTime(int64_t valid_time)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->set_valid_time(valid_time);
}

void CMNodeWrapper::setStartTime(int64_t start_time)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->set_start_time(start_time);
}

void CMNodeWrapper::setMetaInfo(const NodeMetaInfo& meta_info)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->mutable_meta_info()->CopyFrom(meta_info);
}

void CMNodeWrapper::clearTopoInfo()
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->clear_topo_info();
}

void CMNodeWrapper::setTopoInfo(const std::string& topo_info)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->set_topo_info(topo_info);
}

void CMNodeWrapper::setIDCType(IDCType idcType)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->set_idc_type(idcType);
}

void CMNodeWrapper::addProtoPort(ProtocolType protocol, int32_t port)
{
    autil::ScopedWriteLock lock(_rwlock);
    auto proto_port = _cm_node->add_proto_port();
    proto_port->set_protocol(protocol);
    proto_port->set_port(port);
}

void CMNodeWrapper::setNodeIP(const std::string& ip)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->set_node_ip(ip);
}

void CMNodeWrapper::setClusterName(const std::string& cluster)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_node->set_cluster_name(cluster);
}

std::string CMNodeWrapper::getNodeIP()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->node_ip();
}

std::string CMNodeWrapper::getClusterName()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->cluster_name();
}

std::string CMNodeWrapper::getNodeSpec()
{
    autil::ScopedReadLock lock(_rwlock);
    return _node_spec;
}

int32_t CMNodeWrapper::getGroupID()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->group_id();
}

int32_t CMNodeWrapper::getLBInfoWeight()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->lb_info().weight();
}

int64_t CMNodeWrapper::getValidTime()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->valid_time();
}

int64_t CMNodeWrapper::getHeartbeatTime()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->heartbeat_time();
}

int64_t CMNodeWrapper::getStartTime()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->start_time();
}

OnlineStatus CMNodeWrapper::getOnlineStatus()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->online_status();
}

NodeStatus CMNodeWrapper::getCurStatus()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->cur_status();
}

NodeStatus CMNodeWrapper::getPrevStatus()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->prev_status();
}

bool CMNodeWrapper::hasIDCType()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->has_idc_type();
}

IDCType CMNodeWrapper::getIDCType()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->idc_type();
}

bool CMNodeWrapper::hasTopoInfo()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->has_topo_info();
}

std::string CMNodeWrapper::getTopoInfo()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->topo_info();
}

const std::string& CMNodeWrapper::getTopoInfoRef()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->topo_info();
}

bool CMNodeWrapper::hasLBInfo()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->has_lb_info();
}

bool CMNodeWrapper::hasLBInfoWeight()
{
    autil::ScopedReadLock lock(_rwlock);
    if (!_cm_node->has_lb_info()) {
        return false;
    }
    return _cm_node->lb_info().has_weight();
}

NodeLBInfo CMNodeWrapper::getLBInfo()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->lb_info();
}

bool CMNodeWrapper::hasMetaInfo()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->has_meta_info();
}

NodeMetaInfo CMNodeWrapper::getMetaInfo()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->meta_info();
}

int32_t CMNodeWrapper::getProtoPortSize()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->proto_port_size();
}

ProtocolType CMNodeWrapper::getProtoType(int i)
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->proto_port(i).protocol();
}

int32_t CMNodeWrapper::getProtoPort(int i)
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->proto_port(i).port();
}

ProtocolPort CMNodeWrapper::getProto(int i)
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_node->proto_port(i);
}

void CMNodeWrapper::CopyTo(CMNode* node)
{
    if (node) {
        autil::ScopedReadLock lock(_rwlock);
        node->CopyFrom(*_cm_node);
    }
}

void CMNodeWrapper::makeSpec(const CMNode* cm_node, std::string& node_spec)
{
    int32_t len = 0;
    char tmp_make_node_spec[256] = {0};
    len = snprintf(tmp_make_node_spec, 255, "%s", cm_node->node_ip().c_str());
    int32_t port_cnt = cm_node->proto_port_size();
    for (int32_t n_port_cnt_idx = 0; n_port_cnt_idx < port_cnt; ++n_port_cnt_idx) {
        len += snprintf(tmp_make_node_spec + len, 255 - len, "|%s:%d",
                        cm_basic::enum_protocol_type[cm_node->proto_port(n_port_cnt_idx).protocol()],
                        cm_node->proto_port(n_port_cnt_idx).port());
    }
    node_spec.assign(tmp_make_node_spec);
}
} // namespace cm_basic