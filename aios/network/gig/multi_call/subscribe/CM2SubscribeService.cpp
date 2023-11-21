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
#include "aios/network/gig/multi_call/subscribe/CM2SubscribeService.h"

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central_sub.h"
#include "aios/apps/facility/cm2/cm_sub/config/subscriber_config.h"
#include "aios/network/gig/multi_call/util/MiscUtil.h"
#include "autil/ConstString.h"
#include "autil/EnvUtil.h"
#include "autil/StringConvertor.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace cm_basic;
using namespace cm_sub;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, CM2SubscribeService);

int CM2SubscribeService::_rdmaPortDiff = 0;

CM2SubscribeService::CM2SubscribeService(const Cm2Config &config) : _config(config) {
    _rdmaPortDiff = autil::EnvUtil::getEnv(GIG_RDMA_PORT_DIFF_KEY, _rdmaPortDiff);
}

CM2SubscribeService::~CM2SubscribeService() {
}

bool CM2SubscribeService::init() {
    return initGroupSub() && initPartSub();
}

bool CM2SubscribeService::initGroupSub() {
    if (_config.groups.empty()) {
        return true;
    }
    auto *subConfig = newSubConfig(_config);
    if (!subConfig) {
        return false;
    }
    subConfig->_subType = SubReqMsg::ST_BELONG;
    for (const auto &node : _config.groups) {
        subConfig->_subBelongSet.insert(node);
    }
    auto sub = createSubscriber(subConfig);
    if (!sub) {
        return false;
    }
    _groupSubscriber = sub;
    return true;
}

bool CM2SubscribeService::initPartSub() {
    // if (_config.clusters.empty() && _config.noTopoClusters.empty()) {
    // return true;
    // }
    auto *subConfig = newSubConfig(_config);
    if (!subConfig) {
        return false;
    }
    subConfig->_subType = SubReqMsg::ST_PART;
    for (const auto &node : _config.clusters) {
        subConfig->_subClusterSet.insert(node);
        _topoClusterSet.insert(node);
    }
    for (const auto &node : _config.noTopoClusters) {
        subConfig->_subClusterSet.insert(node);
    }
    auto sub = createSubscriber(subConfig);
    if (!sub) {
        AUTIL_LOG(ERROR, "create cm2 subscriber failed, config[%s]",
                  ToJsonString(_config, true).c_str());
        return false;
    }
    _partSubscriber = sub;
    return true;
}

cm_sub::SubscriberConfig *CM2SubscribeService::newSubConfig(const Cm2Config &config) {
    auto *subConfig = new cm_sub::SubscriberConfig();
    subConfig->_disableTopo = true;
    subConfig->_subMaster = config.subMasterOnly;
    subConfig->_zkServer = config.zkHost;
    subConfig->_zkPath = config.zkPath;
    subConfig->_serverType = cm_sub::FromZK;
    subConfig->_compressType = cm_basic::CT_SNAPPY;
    return subConfig;
}

CMSubscriberPtr CM2SubscribeService::createSubscriber(SubscriberConfig *subConfig) {
    CMSubscriberPtr sub(new CMSubscriber());
    if (0 != sub->init(subConfig, NULL)) {
        return CMSubscriberPtr();
    }
    if (0 != sub->subscriber()) {
        return CMSubscriberPtr();
    }
    return sub;
}

bool CM2SubscribeService::checkClusters(cm_basic::CMCentralSub *central) {
    if (!central) {
        return false;
    }
    vector<string> clusterNameVec;
    if (0 != central->getAllClusterName(clusterNameVec)) {
        AUTIL_LOG(ERROR, "get all cm2 cluster name failed.");
        return false;
    }
    int64_t curVersion;
    for (const auto &clusterName : clusterNameVec) {
        curVersion = central->getClusterVersion(clusterName);
        if (0 == _clusterVersions.count(clusterName) || curVersion != _clusterVersions[clusterName])
        // version should only be compared with !=, not >
        {
            return true; // cluster version has changed
        }
    }
    return false;
}

bool CM2SubscribeService::clusterInfoNeedUpdate() {
    if (_partSubscriber) {
        if (checkClusters(_partSubscriber->getCMCentral())) {
            return true;
        }
    }
    if (_groupSubscriber) {
        if (checkClusters(_groupSubscriber->getCMCentral())) {
            return true;
        }
    }
    AUTIL_LOG(DEBUG, "all cm2 cluster not changed");
    return false;
}

bool CM2SubscribeService::getClusterInfoMap(TopoNodeVec &topoNodeVec,
                                            HeartbeatSpecVec &heartbeatSpecs) {
    if (_partSubscriber) {
        if (!addClusters(_partSubscriber->getCMCentral(), topoNodeVec, heartbeatSpecs)) {
            return false;
        }
    }
    if (_groupSubscriber) {
        if (!addClusters(_groupSubscriber->getCMCentral(), topoNodeVec, heartbeatSpecs)) {
            return false;
        }
    }
    return true;
}

bool CM2SubscribeService::addClusters(CMCentralSub *central, TopoNodeVec &topoNodeVec,
                                      HeartbeatSpecVec &heartbeatSpecs) {
    if (!central) {
        AUTIL_LOG(ERROR, "cm central is null");
        return false;
    }
    vector<string> clusterNameVec;
    if (0 != central->getAllClusterName(clusterNameVec)) {
        AUTIL_LOG(ERROR, "get all cm2 cluster name failed.");
        return false;
    }
    int64_t curVersion;
    std::unordered_map<std::string, int64_t> clusterNewVersions;
    for (const auto &clusterName : clusterNameVec) {
        vector<CMNode *> cmNodeVec;
        curVersion = central->getClusterVersion(clusterName);
        if (0 != central->getClusterNodeList(clusterName, cmNodeVec)) {
            AUTIL_LOG(ERROR, "get cluster node failed, cm2 clusterName: %s", clusterName.c_str());
            continue;
        }
        clusterNewVersions.emplace(clusterName, curVersion);

        bool ignoreTopo = false;
        {
            autil::ScopedLock lock(_mutex);
            ignoreTopo = !_config.allGroupsTopo &&
                         (_topoClusterSet.find(clusterName) == _topoClusterSet.end());
        }
        addCluster(cmNodeVec, clusterName, ignoreTopo, _config.enableClusterBizSearch,
                   _config.subProtocolVersion, topoNodeVec, heartbeatSpecs);
        freeNodeVec(cmNodeVec);
    }
    _clusterVersions.swap(clusterNewVersions);
    return true;
}

void CM2SubscribeService::addCluster(const vector<CMNode *> &cmNodeVec, const string &clusterName,
                                     bool ignoreTopo, bool enableClusterBizSearch,
                                     VersionTy subProtocolVersion, TopoNodeVec &topoNodeVec,
                                     HeartbeatSpecVec &heartbeatSpecs) {
    for (const auto node : cmNodeVec) {
        const auto &topoInfoStr = node->topo_info();
        if (ignoreTopo || topoInfoStr.empty()) {
            addTopoNode(node, clusterName, topoNodeVec);
        } else {
            addTopoNodeFromTopoStr(node, clusterName, enableClusterBizSearch, subProtocolVersion,
                                   topoNodeVec, heartbeatSpecs);
        }
    }
}

void CM2SubscribeService::addTopoNode(CMNode *node, const string &clusterName,
                                      TopoNodeVec &topoNodeVec) {
    auto weight = getNodeWeight(node);
    TopoNode topoNode;
    fillSpec(node, topoNode.spec);
    topoNode.bizName = clusterName;
    topoNode.partCnt = 1;
    topoNode.partId = 0;
    topoNode.version = DEFAULT_VERSION_ID;
    topoNode.weight = weight;
    topoNode.clusterName = clusterName;
    topoNode.isValid = isValidNode(node);
    topoNode.ssType = ST_CM2;
    fillNodeMeta(node, topoNode);
    if (topoNode.generateNodeId()) {
        topoNodeVec.push_back(topoNode);
    }
}

void CM2SubscribeService::addTopoNodeFromTopoStr(CMNode *node, const string &clusterName,
                                                 bool enableClusterBizSearch,
                                                 VersionTy subProtocolVersion,
                                                 TopoNodeVec &topoNodeVec,
                                                 HeartbeatSpecVec &heartbeatSpecs) {
    const auto &topoInfoStr = node->topo_info();
    auto weight = getNodeWeight(node);
    auto isValid = isValidNode(node);
    const auto &topoStrVec =
        StringTokenizer::constTokenize(StringView(topoInfoStr.data(), topoInfoStr.length()), "|");
    for (const auto &topoStr : topoStrVec) {
        const auto &topoInfoVec =
            StringTokenizer::constTokenize(StringView(topoStr.data(), topoStr.length()), ":");
        if (topoInfoVec.size() > 8) {
            const auto &info = topoInfoVec[8];
            const auto &portVec =
                StringTokenizer::constTokenize(StringView(info.data(), info.length()), "@");
            if (portVec.size() > 0) {
                auto heartbeatPort =
                    StringConvertor::atoi<int32_t>(portVec[0].data(), portVec[0].length());
                if (heartbeatPort > 0) {
                    HeartbeatSpec spec;
                    fillSpec(node, spec.spec);
                    spec.spec.ports[MC_PROTOCOL_GRPC_STREAM] = heartbeatPort;
                    spec.clusterName = clusterName;
                    spec.enableClusterBizSearch = enableClusterBizSearch;
                    heartbeatSpecs.push_back(spec);
                    break;
                }
            }
        }
        if (topoInfoVec.size() < 3) {
            AUTIL_LOG(WARN, "error topo info [%s]", topoStr.to_string().c_str());
            continue;
        }
        if (INVALID_VERSION_ID != subProtocolVersion) {
            if (topoInfoVec.size() > 5) {
                auto protocalVersion = StringConvertor::atoi<VersionTy>(topoInfoVec[5].data(),
                                                                        topoInfoVec[5].length());
                if (protocalVersion != subProtocolVersion) {
                    continue;
                }
            }
        }
        TopoNode topoNode;
        fillSpec(node, topoNode.spec);
        if (enableClusterBizSearch) {
            topoNode.bizName = MiscUtil::createBizName(clusterName, topoInfoVec[0].to_string());
        } else {
            topoNode.bizName = topoInfoVec[0].to_string();
        }
        topoNode.partCnt =
            StringConvertor::atoi<PartIdTy>(topoInfoVec[1].data(), topoInfoVec[1].length());
        topoNode.partId =
            StringConvertor::atoi<PartIdTy>(topoInfoVec[2].data(), topoInfoVec[2].length());
        if (topoInfoVec.size() > 3) {
            topoNode.version =
                StringConvertor::atoi<VersionTy>(topoInfoVec[3].data(), topoInfoVec[3].length());
        } else {
            topoNode.version = DEFAULT_VERSION_ID;
        }
        if (topoInfoVec.size() > 4) {
            topoNode.weight =
                StringConvertor::atoi<WeightTy>(topoInfoVec[4].data(), topoInfoVec[4].length());
        } else {
            topoNode.weight = weight;
        }
        if (topoInfoVec.size() > 5) {
            auto protocalVersion =
                StringConvertor::atoi<VersionTy>(topoInfoVec[5].data(), topoInfoVec[5].length());
            topoNode.protocalVersion = protocalVersion;
        }
        if (topoInfoVec.size() > 6) {
            int32_t grpcPort =
                StringConvertor::atoi<int32_t>(topoInfoVec[6].data(), topoInfoVec[6].length());
            if (grpcPort == 0) {
                grpcPort = INVALID_PORT;
            }
            topoNode.spec.ports[MC_PROTOCOL_GRPC] = grpcPort;
            topoNode.spec.ports[MC_PROTOCOL_GRPC_STREAM] = grpcPort;
        }
        if (topoInfoVec.size() > 7) {
            bool supportHeartbeat = false;
            if (StringUtil::fromString<bool>(topoInfoVec[7].to_string(), supportHeartbeat)) {
                topoNode.supportHeartbeat = supportHeartbeat;
            }
        }
        topoNode.clusterName = clusterName;
        topoNode.isValid = isValid;
        topoNode.ssType = ST_CM2;
        fillNodeMeta(node, topoNode);
        if (topoNode.generateNodeId()) {
            topoNodeVec.push_back(topoNode);
        } else {
            AUTIL_LOG(WARN, "generateNodeId failed: topoStr [%s], node [%s]",
                      topoStr.to_string().c_str(), node->ShortDebugString().c_str());
        }
    }
}

WeightTy CM2SubscribeService::getNodeWeight(CMNode *node) {
    WeightTy weight = MAX_WEIGHT;
    if (node->lb_info().has_weight()) {
        weight = node->lb_info().weight();
    }
    return weight;
}

void CM2SubscribeService::fillSpec(CMNode *node, Spec &spec) {
    spec.ip = node->node_ip();
    for (int32_t i = 0; i < node->proto_port_size(); i++) {
        if (PT_TCP == node->proto_port(i).protocol()) {
            spec.ports[MC_PROTOCOL_TCP] = node->proto_port(i).port();
            spec.ports[MC_PROTOCOL_ARPC] = node->proto_port(i).port();
            if (_rdmaPortDiff != 0) {
                spec.ports[MC_PROTOCOL_RDMA_ARPC] = node->proto_port(i).port() + _rdmaPortDiff;
            }
        } else if (PT_HTTP == node->proto_port(i).protocol()) {
            spec.ports[MC_PROTOCOL_HTTP] = node->proto_port(i).port();
        }
    }
}

void CM2SubscribeService::fillNodeMeta(CMNode *node, TopoNode &topoNode) {
    const auto &metaInfo = node->meta_info();
    topoNode.fillNodeMeta(metaInfo);
}

void CM2SubscribeService::freeNodeVec(const vector<CMNode *> &nodeVec) {
    for (auto node : nodeVec) {
        DELETE_AND_SET_NULL(node);
    }
}

bool CM2SubscribeService::isValidNode(CMNode *node) {
    return node->online_status() == cm_basic::OS_ONLINE &&
           node->cur_status() == cm_basic::NS_NORMAL && node->proto_port_size() > 0;
}

bool CM2SubscribeService::addSubscribe(const std::vector<std::string> &names) {
    bool result = true;
    for (const auto &name : names) {
        if (0 != _partSubscriber->addSubCluster(name)) {
            AUTIL_LOG(INFO, "add subscribe failed, name:%s", name.c_str());
            result = false;
        } else {
            AUTIL_LOG(INFO, "add subscribe succeed, name:%s", name.c_str());
        }
    }
    return result;
}

bool CM2SubscribeService::deleteSubscribe(const std::vector<std::string> &names) {
    bool result = true;
    for (const auto &name : names) {
        if (0 != _partSubscriber->removeSubCluster(name)) {
            result = false;
        }
    }
    return result;
}

void CM2SubscribeService::addTopoCluster(const std::vector<std::string> &names) {
    autil::ScopedLock lock(_mutex);
    _topoClusterSet.insert(names.begin(), names.end());
}

void CM2SubscribeService::deleteTopoCluster(const std::vector<std::string> &names) {
    autil::ScopedLock lock(_mutex);
    for (const auto &name : names) {
        _topoClusterSet.erase(name);
    }
}

} // namespace multi_call
