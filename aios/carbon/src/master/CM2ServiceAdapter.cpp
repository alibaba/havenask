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
#include <math.h>
#include "master/CM2ServiceAdapter.h"
#include "master/ANetTransportSingleton.h"
#include "master/Util.h"
#include "carbon/CommonDefine.h"
#include "carbon/Log.h"
#include "master/GlobalVariable.h"
#include "autil/legacy/jsonizable.h"
#include "autil/StringUtil.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_common.pb.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"

using namespace std;
using namespace arpc;
using namespace cm_basic;
using namespace cm_server;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace google::protobuf;

BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, CM2ServiceAdapter);

#define LOG_PREFIX _id.c_str()

#define CM2_CONFIG_KEY "cm2"
#define CM2_CONFIG_WEIGHT_KEY "weight"
#define CM2_CONFIG_TOPO_INFO_KEY "topo_info"

CM2ServiceAdapter::CM2ServiceAdapter(const string &id)
    : ServiceAdapter(id)
{
    _masterApply = NULL;
    _rpcChannelManager = NULL;
    _rpcChannel = NULL;
    const CarbonInfo &tmpCarbonInfo = GlobalVariable::getCarbonInfo();
    _serviceSignature = tmpCarbonInfo.carbonZk + "." + id;
    CARBON_PREFIX_LOG(INFO, "Service Signature:%s", _serviceSignature.c_str());
}

CM2ServiceAdapter::~CM2ServiceAdapter() {
    delete _rpcChannel;
    delete _rpcChannelManager;
    delete _masterApply;
}

#define CM2_SERVER_RPC_CALL(request, response, methodName)              \
    do {                                                                \
        CMCmdProcService *stub = getServiceStub();                      \
        if (stub == NULL) {                                             \
            return false;                                               \
        }                                                               \
        unique_ptr<CMCmdProcService> stubPtr(stub);                       \
        arpc::ANetRPCController controller;                             \
        stubPtr->methodName(&controller, &request, &response, NULL);    \
        if (controller.Failed()) {                                      \
            CARBON_PREFIX_LOG(WARN, "rpc call failed, errorcode[%d], reason[%s]", \
                    controller.GetErrorCode(), controller.ErrorText().c_str()); \
            return false;                                               \
        }                                                               \
        if (response.status() == cm_basic::RSP_NOT_CM_LEADER) {         \
            CARBON_PREFIX_LOG(WARN, "response with error, not cm leader");        \
            DELETE_AND_SET_NULL(_rpcChannel);                           \
            return false;                                               \
        }                                                               \
    } while (0);


bool CM2ServiceAdapter::doInit(const ServiceConfig &serviceConfig) {
    string configStr = serviceConfig.configStr;
    _metaStr = serviceConfig.metaStr;
    try {
        FromJsonString(_cm2ServiceConfig, configStr);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_PREFIX_LOG(ERROR, "cm2 service config from json fail, exception[%s], configStr:%s",
                   e.what(), configStr.c_str());
        return false;
    }
    CARBON_PREFIX_LOG(INFO, "parse cm2 service config success.");

    _zkLeaderPath = _cm2ServiceConfig.leaderPath;
    if (_zkLeaderPath.size() && *(_zkLeaderPath.rbegin()) == '/') {
        _zkLeaderPath = _zkLeaderPath + "leader_elector";
    } else {
        _zkLeaderPath = _zkLeaderPath + "/leader_elector";
    }

    _masterApply = new cm_basic::MasterApply(_cm2ServiceConfig.zkHost, 10000);
    _rpcChannelManager = new ANetRPCChannelManager(ANetTransportSingleton::getInstance());
    return true;
}

void CM2ServiceAdapter::doUpdateConfig(const ServiceConfig &serviceConfig) {
    ScopedLock lock(_lock);
    _metaStr = serviceConfig.metaStr;
}

nodespec_t CM2ServiceAdapter::getServiceNodeSpec(
        const ServiceNode &node) const
{
    return getSpec(node.ip,
                   _cm2ServiceConfig.tcpPort,
                   _cm2ServiceConfig.httpPort);
}

bool CM2ServiceAdapter::doAddNodes(const ServiceNodeMap &nodes) {
    return operateNodes(nodes, CMD_UPDATE_NODES);
}

bool CM2ServiceAdapter::doDelNodes(const ServiceNodeMap &nodes) {
    return operateNodes(nodes, CMD_DEL_NODES);
}


bool CM2ServiceAdapter::doUpdateNodes(const ServiceNodeMap &nodes) {
    vector<cm_basic::CMNode> cmNodes;
    ServiceNodeMap::const_iterator it = nodes.begin();
    for (; it != nodes.end(); it++ ) {
        const nodespec_t &nodeSpec = it->first;
        const ServiceNode &node = it->second;
        if (_lastCMNodeMap.find(nodeSpec) == _lastCMNodeMap.end()) {
            CARBON_PREFIX_LOG(WARN, "No corresponding CM node to update:%s",
                       nodeSpec.c_str());
            continue;
        }

        cm_basic::CMNode cmNode = _lastCMNodeMap[nodeSpec];
        fillByMetaInfo(node.metas, &cmNode);
        if (needUpdate(_lastCMNodeMap[nodeSpec], cmNode)){
            cmNodes.push_back(cmNode);
        }
    }

    return doOperateNodes(cmNodes, CMD_UPDATE_NODES);
}

bool CM2ServiceAdapter::doGetNodes(ServiceNodeMap *nodes){
    nodes->clear();
    GetClusterNodeReqMsg request;
    request.set_cmd_type(CMD_GET_SOME_CLUSTER_NODE);
    request.add_cluster_name_vec(_cm2ServiceConfig.clusterName);

    GetClusterNodeRespMsg response;
    CM2_SERVER_RPC_CALL(request, response, GetClusterNode);
    CARBON_PREFIX_LOG(DEBUG, "get cm2 nodes response:[%s]",
               response.ShortDebugString().c_str());
    if (response.status() == cm_basic::RSP_CLUSTER_NOT_EXIST) {
        CARBON_PREFIX_LOG(WARN, "cm2 cluster not exist, cluster:[%s].",
                   _cm2ServiceConfig.clusterName.c_str());
        return true;
    }
    if (response.status() != cm_basic::RSP_SUCCESS) {
        CARBON_PREFIX_LOG(WARN, "response with error, errorcode[%d], reason[%s]",
               response.status(), response.error_msg().c_str());
        return false;
    }
    CMNodeMap tmpCMNodeMap;
    for (int i = 0; i < response.cluster_vec_size(); ++i) {
        const cm_basic::CMCluster &cluster = response.cluster_vec(i);
        for (int j = 0; j < cluster.group_vec_size(); ++j) {
            const cm_basic::CMGroup &group = cluster.group_vec(j);
            for (int k = 0; k < group.node_vec_size(); ++k) {
                const cm_basic::CMNode &node = group.node_vec(k);
                if (!checkUniqueSignature(node)) {
                    continue;
                }
                nodespec_t nodeSpec = getCMNodeSpec(node);
                tmpCMNodeMap[nodeSpec] = node;
                (*nodes)[nodeSpec] = toServiceNode(node);
            }
        }
    }
    CARBON_PREFIX_LOG(DEBUG, "get cm2 nodes:[%s].", ToJsonString(*nodes).c_str());
    _lastCMNodeMap.swap(tmpCMNodeMap);
    return true;
}

ServiceNode CM2ServiceAdapter::toServiceNode(const cm_basic::CMNode &node) {
    ServiceNode serviceNode;
    serviceNode.ip = node.node_ip();
    if (node.online_status() == OS_ONLINE &&
        node.cur_status() == NS_NORMAL)
    {
        serviceNode.status = SN_AVAILABLE;
    } else {
        serviceNode.status = SN_UNAVAILABLE;
    }
    serviceNode.metas[SERVICEINFO_PAYLOAD_CM2_TOPOINFO] = node.topo_info();
    return serviceNode;
}

bool CM2ServiceAdapter::needUpdate(const cm_basic::CMNode &preNode,
                                   const cm_basic::CMNode &curNode)
{
    return (preNode.topo_info() != curNode.topo_info() ||
            preNode.lb_info().weight() != curNode.lb_info().weight() ||
            preNode.meta_info().DebugString() != curNode.meta_info().DebugString());
}

cm_basic::CMNode CM2ServiceAdapter::toCMNode(const ServiceNode &node) {
    cm_basic::CMNode cmNode;
    cmNode.set_node_ip(node.ip);
    cmNode.set_cluster_name(_cm2ServiceConfig.clusterName);
    cmNode.set_group_id(0);

    if (_cm2ServiceConfig.tcpPort > 0) {
        ProtocolPort *tcp = cmNode.add_proto_port();
        tcp->set_protocol(PT_TCP);
        tcp->set_port(_cm2ServiceConfig.tcpPort);
    }
    if (_cm2ServiceConfig.httpPort > 0) {
        ProtocolPort *http = cmNode.add_proto_port();
        http->set_protocol(PT_HTTP);
        http->set_port(_cm2ServiceConfig.httpPort);
    }

    cmNode.set_online_status(OS_ONLINE);
    cmNode.set_cur_status(NS_ABNORMAL);
    cmNode.set_prev_status(NS_UNINIT);
    cmNode.set_valid_time(0);
    cmNode.set_heartbeat_time(0);
    cmNode.set_start_time(0);
    if (_cm2ServiceConfig.idcType >= (int32_t)IDC_ANY
        && _cm2ServiceConfig.idcType < (int32_t)IDC_END)
    {
        cmNode.set_idc_type((IDCType)_cm2ServiceConfig.idcType);
    }
    NodeLBInfo *lbInfo = cmNode.mutable_lb_info();
    lbInfo->set_weight(_cm2ServiceConfig.weight);
    fillByMetaInfo(node.metas, &cmNode);
    return cmNode;
}

void CM2ServiceAdapter::fillByMetaInfo(const KVMap &metas, cm_basic::CMNode *cmNode) {
    CARBON_PREFIX_LOG(DEBUG, "service node metas:[%s].",
               autil::legacy::ToJsonString(metas, true).c_str());
    cmNode->clear_meta_info();
    cm_basic::MetaKV *kv = cmNode->mutable_meta_info()->add_kv_array();
    kv->set_meta_key(SERVICE_SIGNAURE_KEY);
    kv->set_meta_value(_serviceSignature);
    kv = cmNode->mutable_meta_info()->add_kv_array();
    kv->set_meta_key(SERVICE_META_KEY);
    {
        ScopedLock lock(_lock);
        kv->set_meta_value(_metaStr);
    }

    KVMap::const_iterator it = metas.find(HEALTHCHECK_PAYLOAD_SERVICEINFO);
    if (it == metas.end()) {
        return;
    }

    const string &serviceInfoStr = it->second;
    if (serviceInfoStr == "") {
        return;
    }

    JsonMap serviceMeta;
    try {
        FromJsonString(serviceMeta, serviceInfoStr);
    } catch (const ExceptionBase &e) {
        CARBON_PREFIX_LOG(ERROR, "service info string from json failed. "
                   "error:[%s], service info str:[%s].",
                   e.what(), serviceInfoStr.c_str());
        return;
    }

    JsonMap cm2Metas;
    CARBON_PREFIX_LOG(DEBUG, "begin extract value");
    if (Util::getValue(serviceMeta, CM2_CONFIG_KEY, &cm2Metas)) {
        CARBON_PREFIX_LOG(DEBUG, "get cm2 service info");
        int32_t weight;
        if (Util::getValue(cm2Metas, CM2_CONFIG_WEIGHT_KEY, &weight)) {
            CARBON_PREFIX_LOG(DEBUG, "weight:%d", weight);
            NodeLBInfo *lbInfo = cmNode->mutable_lb_info();
            lbInfo->set_weight(weight);
        }
        string topoInfo;
        if (Util::getValue(cm2Metas, CM2_CONFIG_TOPO_INFO_KEY, &topoInfo)) {
            CARBON_PREFIX_LOG(DEBUG, "topoInfo:%s", topoInfo.c_str());
            cmNode->set_topo_info(topoInfo);
        }
    }
}

bool CM2ServiceAdapter::operateNodes(const ServiceNodeMap &nodes,
                                     cm_server::CMD_TYPE type) {
    vector<cm_basic::CMNode> cmNodes;
    ServiceNodeMap::const_iterator it = nodes.begin();
    for (; it != nodes.end(); it++ ) {
        cmNodes.push_back(toCMNode(it->second));
    }

    return doOperateNodes(cmNodes, type);
}

bool CM2ServiceAdapter::doOperateNodes(const vector<cm_basic::CMNode> &cmNodes, CMD_TYPE type) {
    if (cmNodes.size() == 0) {
        return true;
    }

    SetClusterCfgReqMsg request;
    request.set_cmd_type(type);
    SetClusterCfgReqMsg::MutlNode multiNodes;
    for (size_t i = 0; i < cmNodes.size(); ++i) {
        (*multiNodes.add_nodes()) = cmNodes[i];
    }

    CARBON_PREFIX_LOG(INFO, "%s : %s.", CMD_TYPE_Name(type).c_str(),
               multiNodes.ShortDebugString().c_str());

    if (!multiNodes.SerializeToString(request.mutable_cfg_content())) {
        CARBON_PREFIX_LOG(WARN, "invalid CMNodes, %s", multiNodes.ShortDebugString().c_str());
        return false;
    }
    CommonRespMsg response;
    CM2_SERVER_RPC_CALL(request, response, SetClusterCfg);
    if (response.status() != cm_basic::RSP_SUCCESS) {
        CARBON_PREFIX_LOG(WARN, "response with error, errorcode[%d], reason[%s]",
               response.status(), response.error_msg().c_str());
        return false;
    }

    return true;
}

CMCmdProcService* CM2ServiceAdapter::getServiceStub() {
    arpc::ANetRPCChannel *rpcChannel = getRpcChannel();
    if (!rpcChannel) {
        CARBON_PREFIX_LOG(WARN, "get cm server rpc channel failed");
        return NULL;
    }
    return new CMCmdProcService_Stub(rpcChannel,
            Service::STUB_DOESNT_OWN_CHANNEL);
}

ANetRPCChannel* CM2ServiceAdapter::getRpcChannel() {
    if (_rpcChannel && !_rpcChannel->ChannelBroken()) {
        return _rpcChannel;
    }

    DELETE_AND_SET_NULL(_rpcChannel);

    string leaderSpec;
    if (!getLeader(&leaderSpec)) {
        return NULL;
    }

    bool block = false;
    size_t queueSize = 50ul;
    int timeout = 5000;
    bool autoReconnect = false;
    _rpcChannel = (arpc::ANetRPCChannel *)_rpcChannelManager->OpenChannel(
            leaderSpec.c_str(), block, queueSize, timeout, autoReconnect);
    return _rpcChannel;
}

bool CM2ServiceAdapter::getLeader(string *leaderSpec) {
    string leaderAddress;
    if (_masterApply->getMaster(_zkLeaderPath.c_str(), leaderAddress) < 0) {
        CARBON_PREFIX_LOG(WARN, "get cm server leader failed, leader path: %s",
                _zkLeaderPath.c_str());
        return false;
    }
    CARBON_PREFIX_LOG(INFO, "cm server leader address: %s", leaderAddress.c_str());
    size_t pos = leaderAddress.rfind(":");
    if (pos == string::npos) {
        CARBON_PREFIX_LOG(WARN, "wrong leader address: %s", leaderAddress.c_str());
        return false;
    }
    (*leaderSpec) = "tcp:" + leaderAddress.substr(0, pos);
    return true;
}

string CM2ServiceAdapter::getSpec(const string &ip,
                                  int32_t tcpPort, int32_t httpPort) const
{
    string spec = ip + ":" + StringUtil::toString(tcpPort) +
                  ":" + StringUtil::toString(httpPort);
    return spec;
}

nodespec_t CM2ServiceAdapter::getCMNodeSpec(const cm_basic::CMNode &node) {
    int32_t tcpPort = 0;
    int32_t httpPort = 0;
    for (int32_t i = 0; i < node.proto_port_size(); i++) {
        const cm_basic::ProtocolPort &port = node.proto_port(i);
        if (port.protocol() == PT_TCP) {
            tcpPort = port.port();
        } else if (port.protocol() == PT_HTTP) {
            httpPort = port.port();
        }
    }

    return getSpec(node.node_ip(), tcpPort, httpPort);
}

bool CM2ServiceAdapter::checkUniqueSignature(const cm_basic::CMNode &node) const {
    for (int32_t i = 0; i < node.meta_info().kv_array_size(); ++i) {
        const cm_basic::MetaKV &kv = node.meta_info().kv_array(i);
        if (kv.meta_key() == SERVICE_SIGNAURE_KEY) {
            return kv.meta_value() == _serviceSignature;
        }
    }
    return false;
}

#undef LOG_PREFIX
#undef CM2_SERVER_RPC_CALL
END_CARBON_NAMESPACE(master);
