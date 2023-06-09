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
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/common/WorkerNodeInfo.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, Cm2Config);
AUTIL_LOG_SETUP(multi_call, VipConfig);
AUTIL_LOG_SETUP(multi_call, IstioConfig);
AUTIL_LOG_SETUP(multi_call, LocalConfig);
AUTIL_LOG_SETUP(multi_call, SubscribeConfig);
AUTIL_LOG_SETUP(multi_call, ProtocolConfig);
AUTIL_LOG_SETUP(multi_call, MiscConfig);
AUTIL_LOG_SETUP(multi_call, MultiCallConfig);

void Cm2Config::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("zk_host", zkHost, zkHost);
    json.Jsonize("zk_path", zkPath, zkPath);
    json.Jsonize("enable_cluster_biz_search", enableClusterBizSearch,
                 enableClusterBizSearch);
    json.Jsonize("sub_master_only", subMasterOnly, subMasterOnly);
    json.Jsonize("sub_protocol_version", subProtocolVersion,
                 subProtocolVersion);
    json.Jsonize("cm2_part", clusters, clusters);
    json.Jsonize("cm2_part_no_topo_info", noTopoClusters, noTopoClusters);
    json.Jsonize("cm2_group", groups, groups);
    json.Jsonize("cm2_all_group_topo", allGroupsTopo, allGroupsTopo);
}

bool Cm2Config::operator==(const Cm2Config &rhs) const {
    return zkHost == rhs.zkHost && zkPath == rhs.zkPath &&
           enableClusterBizSearch == rhs.enableClusterBizSearch &&
           subMasterOnly == rhs.subMasterOnly &&
           subProtocolVersion == rhs.subProtocolVersion &&
           clusters == rhs.clusters && noTopoClusters == rhs.noTopoClusters &&
           groups == rhs.groups && allGroupsTopo == rhs.allGroupsTopo;
}

bool Cm2Config::validate(bool &enabled) const {
    enabled = !zkHost.empty() || !zkPath.empty() || !clusters.empty() ||
              !noTopoClusters.empty() || !groups.empty();
    if (!enabled) {
        return true;
    }
    if (zkHost.empty()) {
        AUTIL_LOG(ERROR, "cm2 zkHost is empty");
        return false;
    }
    if (zkPath.empty()) {
        AUTIL_LOG(ERROR, "cm2 zkPath is empty");
        return false;
    }
    for (size_t i = 0; i < clusters.size(); i++) {
        if (clusters[i].empty()) {
            AUTIL_LOG(ERROR, "the %luth cm2 cluster name is empty", i);
            return false;
        }
    }
    for (size_t i = 0; i < noTopoClusters.size(); i++) {
        if (noTopoClusters[i].empty()) {
            AUTIL_LOG(ERROR, "the %luth no topo cm2 cluster name is empty", i);
            return false;
        }
    }
    for (size_t i = 0; i < groups.size(); i++) {
        if (groups[i].empty()) {
            AUTIL_LOG(ERROR, "the %luth cm2 group name is empty", i);
            return false;
        }
    }
    return true;
}

void VipConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("jmenv_dom", jmenvDom, jmenvDom);
    json.Jsonize("services", clusters, clusters);
}

bool VipConfig::operator==(const VipConfig &rhs) const {
    return jmenvDom == rhs.jmenvDom && clusters == rhs.clusters;
}

bool VipConfig::validate(bool &enabled) const {
    enabled = !jmenvDom.empty() || !clusters.empty();
    if (!enabled) {
        return true;
    }
    if (jmenvDom.empty()) {
        AUTIL_LOG(ERROR, "vip jmenvDom is empty");
        return false;
    }
    for (size_t i = 0; i < clusters.size(); i++) {
        if (clusters[i].empty()) {
            AUTIL_LOG(ERROR, "the %luth vip cluster name is empty", i);
            return false;
        }
    }
    return true;
}

void IstioConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("istio_host", istioHost, istioHost);
    json.Jsonize("biz_clusters", clusters, clusters);
    json.Jsonize("xdsCallbackQueueSize", xdsCallbackQueueSize,
                 xdsCallbackQueueSize);
    json.Jsonize("enable_host_transform", enableHostTransform,
                 enableHostTransform);
    json.Jsonize("cacheFile", cacheFile, cacheFile);
    json.Jsonize("readCache", readCache, readCache);
    json.Jsonize("xdsClientWorkerThreadNumber", xdsClientWorkerThreadNumber,
                 xdsClientWorkerThreadNumber);
    json.Jsonize("writeCache", writeCache, writeCache);
    json.Jsonize("writeInterval", writeInterval, writeInterval);
}

bool IstioConfig::operator==(const IstioConfig &rhs) const {
    return istioHost == rhs.istioHost && clusters == rhs.clusters &&
           enableHostTransform == rhs.enableHostTransform &&
           xdsCallbackQueueSize == rhs.xdsCallbackQueueSize &&
           cacheFile == rhs.cacheFile && readCache == rhs.readCache &&
           writeCache == rhs.writeCache &&
           xdsClientWorkerThreadNumber == rhs.xdsClientWorkerThreadNumber &&
           writeInterval == rhs.writeInterval;
}

bool IstioConfig::validate(bool &enabled) const {
    enabled = !istioHost.empty() || !clusters.empty();
    if (!enabled) {
        return true;
    }
    if (istioHost.empty()) {
        AUTIL_LOG(ERROR, "istio istioHost is empty");
        return false;
    }
    for (auto &c : clusters) {
        if (c.empty()) {
            AUTIL_LOG(ERROR, "empty cluster name in istio biz_clusters config");
            return false;
        }
    }
    if (!xdsCallbackQueueSize) {
        AUTIL_LOG(ERROR, "istio xdsCallbackQueueSize is 0");
        return false;
    }
    return true;
}

LocalConfig::LocalConfig() {}

LocalConfig::~LocalConfig() {}

LocalJsonize::LocalJsonize()
    : _partCnt(INVALID_PART_COUNT), _partId(INVALID_PART_ID),
      _version(INVALID_VERSION_ID), _weight(MAX_WEIGHT), _tcpPort(INVALID_PORT),
      _httpPort(INVALID_PORT), _grpcPort(INVALID_PORT),
      _rdmaPort(INVALID_PORT),
      _supportHeartbeat(false) {}

void LocalJsonize::Jsonize(Jsonizable::JsonWrapper &json) {
    json.Jsonize("biz_name", _bizName, _bizName);
    json.Jsonize("part_count", _partCnt, _partCnt);
    json.Jsonize("part_id", _partId, _partId);
    json.Jsonize("version", _version, _version);
    json.Jsonize("weight", _weight, _weight);
    json.Jsonize("ip", _ip, _ip);
    json.Jsonize("tcp_port", _tcpPort, _tcpPort);
    json.Jsonize("http_port", _httpPort, _httpPort);
    json.Jsonize("grpc_port", _grpcPort, _grpcPort);
    json.Jsonize("rdma_port", _rdmaPort, _rdmaPort);
    json.Jsonize("node_meta", _meta, _meta);
    json.Jsonize("support_heartbeat", _supportHeartbeat, _supportHeartbeat);
}

bool LocalJsonize::validate() const {
    const auto &node = getTopoNode();
    return node.validate();
}

bool LocalJsonize::operator==(const LocalJsonize &rhs) const {
    return _bizName == rhs._bizName && _partCnt == rhs._partCnt &&
           _partId == rhs._partId && _version == rhs._version &&
           _weight == rhs._weight && _ip == rhs._ip &&
           _tcpPort == rhs._tcpPort && _httpPort == rhs._httpPort &&
           _grpcPort == rhs._grpcPort && _rdmaPort == rhs._rdmaPort &&
           _meta == rhs._meta && _supportHeartbeat == rhs._supportHeartbeat;
}

TopoNode LocalJsonize::getTopoNode() const {
    TopoNode node;
    node.bizName = _bizName;
    node.partCnt = _partCnt;
    node.partId = _partId;
    node.version = _version;
    node.weight = _weight;
    node.spec.ip = _ip;
    node.spec.ports[MC_PROTOCOL_TCP] = _tcpPort;
    node.spec.ports[MC_PROTOCOL_ARPC] = _tcpPort;
    node.spec.ports[MC_PROTOCOL_HTTP] = _httpPort;
    node.spec.ports[MC_PROTOCOL_GRPC] = _grpcPort;
    node.spec.ports[MC_PROTOCOL_GRPC_STREAM] = _grpcPort;
    node.spec.ports[MC_PROTOCOL_RDMA_ARPC] = _rdmaPort;
    node.meta = _meta;
    node.supportHeartbeat = _supportHeartbeat;
    node.ssType = ST_LOCAL;
    node.generateNodeId();
    return node;
}

bool LocalConfig::operator==(const LocalConfig &rhs) const {
    return nodes == rhs.nodes;
}

bool LocalConfig::validate(bool &enabled) const {
    enabled = !nodes.empty();
    if (!enabled) {
        return true;
    }
    for (size_t i = 0; i < nodes.size(); i++) {
        const auto &node = nodes[i];
        if (!node.validate()) {
            AUTIL_LOG(ERROR, "the %luth local node is invalid", i);
            return false;
        }
    }
    return true;
}

bool LocalConfig::empty() const { return nodes.empty(); }

void SubscribeConfig::Jsonize(Jsonizable::JsonWrapper &json) {
    json.Jsonize("cm2", cm2Config, cm2Config);
    json.Jsonize("vip", vipConfig, vipConfig);
    json.Jsonize("istio", istioConfig, istioConfig);
    json.Jsonize("local", localConfig.nodes, localConfig.nodes);
    json.Jsonize("cm2_configs", cm2Configs, cm2Configs);
    json.Jsonize("istio_configs", istioConfigs, istioConfigs);
    json.Jsonize("allow_empty_sub", allowEmptySub, allowEmptySub);
}

bool SubscribeConfig::operator==(const SubscribeConfig &rhs) const {
    return cm2Config == rhs.cm2Config && vipConfig == rhs.vipConfig &&
           localConfig == rhs.localConfig && istioConfig == rhs.istioConfig &&
           cm2Configs == rhs.cm2Configs && istioConfigs == rhs.istioConfigs;
}

bool SubscribeConfig::getCm2Configs(std::vector<Cm2Config> &configs) const {
    bool enable = false;
    if (!cm2Config.validate(enable)) {
        return false;
    }
    if (enable) {
        configs.push_back(cm2Config);
    }
    for (auto &config : cm2Configs) {
        enable = false;
        if (!config.validate(enable)) {
            return false;
        }
        if (enable) {
            configs.push_back(config);
        }
    }
    return true;
}

bool SubscribeConfig::getIstioConfigs(std::vector<IstioConfig> &configs) const {
    bool enable = false;
    if (!istioConfig.validate(enable)) {
        return false;
    }
    if (enable) {
        configs.push_back(istioConfig);
    }
    for (auto &config : istioConfigs) {
        enable = false;
        if (!config.validate(enable)) {
            return false;
        }
        if (enable) {
            configs.push_back(config);
        }
    }
    return true;
}

bool SubscribeConfig::validate(bool &cm2Enabled, bool &vipEnabled,
                               bool &localEnabled, bool &istioEnabled) const {
    std::vector<Cm2Config> cm2Configs;
    auto cm2Validate = getCm2Configs(cm2Configs);
    cm2Enabled = !cm2Configs.empty();

    vipEnabled = false;
    auto vipValidate = vipConfig.validate(vipEnabled);

    std::vector<IstioConfig> istioConfigs;
    auto istioValidate = getIstioConfigs(istioConfigs);
    istioEnabled = !istioConfigs.empty();

    localEnabled = false;
    auto localValidate = localConfig.validate(localEnabled);

    if (cm2Validate && vipValidate && localValidate && istioValidate) {
        if (!cm2Enabled && !vipEnabled && !localEnabled && !istioEnabled) {
            AUTIL_LOG(WARN, "no cm2, vip, istio or local sub is configured");
        }
        return true;
    }

    return false;
}

void MultiCallConfig::Jsonize(Jsonizable::JsonWrapper &json) {
    json.Jsonize("subscribe_config", subConf, subConf);
    json.Jsonize("connection_config", connectConf, connectConf);
    json.Jsonize("misc_config", miscConf, miscConf);
    json.Jsonize("trace_config", traceConf, traceConf);
}

bool MultiCallConfig::operator==(const MultiCallConfig &rhs) const {
    return subConf == rhs.subConf &&
           connectConf == rhs.connectConf &&
           miscConf == rhs.miscConf &&
           traceConf == rhs.traceConf;
}

void SecureConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("pem_root_certs", pemRootCerts, pemRootCerts);
    json.Jsonize("pem_private_key", pemPrivateKey, pemPrivateKey);
    json.Jsonize("pem_cert_chain", pemCertChain, pemCertChain);
    json.Jsonize("target_name", targetName, targetName);
}

bool SecureConfig::operator==(const SecureConfig &rhs) const {
    return pemRootCerts == rhs.pemRootCerts &&
           pemPrivateKey == rhs.pemPrivateKey &&
           pemCertChain == rhs.pemCertChain && targetName == rhs.targetName;
}

void ProtocolConfig::Jsonize(Jsonizable::JsonWrapper &json) {
    json.Jsonize("thread_num", threadNum, threadNum);
    json.Jsonize("queue_size", queueSize, queueSize);
    json.Jsonize("anet_loop_timeout", anetLoopTimeout, anetLoopTimeout);
    json.Jsonize("keep_alive_interval", keepAliveInterval, keepAliveInterval);
    json.Jsonize("keep_alive_timeout", keepAliveTimeout, keepAliveTimeout);
    json.Jsonize("secure_config", secureConfig, secureConfig);
}

bool ProtocolConfig::operator==(const ProtocolConfig &rhs) const {
    return threadNum == rhs.threadNum && queueSize == rhs.queueSize &&
           anetLoopTimeout == rhs.anetLoopTimeout &&
           keepAliveInterval == rhs.keepAliveInterval &&
           keepAliveTimeout == rhs.keepAliveTimeout &&
           secureConfig == rhs.secureConfig;
}

void MiscConfig::Jsonize(Jsonizable::JsonWrapper &json) {
    json.Jsonize("snapshot_log_count", snapshotLogCount, snapshotLogCount);
    json.Jsonize("snapshot_interval", snapshotInterval, snapshotInterval);
    json.Jsonize("consistent_hash_virtual_point", virtualNodeCount,
                 virtualNodeCount);
    json.Jsonize("heartbeat_timeout", heartbeatTimeout, heartbeatTimeout);
    json.Jsonize("heartbeat_thread_num", heartbeatThreadCount,
                 heartbeatThreadCount);
    json.Jsonize("callback_thread_num", callbackThreadNum, callbackThreadNum);
    json.Jsonize("call_process_interval", callProcessInterval,
                 callProcessInterval);
    json.Jsonize("grpc_channel_poll_interval_ms", grpcChannelPollIntervalMs,
                 grpcChannelPollIntervalMs);
    json.Jsonize("weight_update_step", weightUpdateStep, weightUpdateStep);
    json.Jsonize("snapshot_log_prefix", logPrefix, logPrefix);
    json.Jsonize("enable_heartbeat", enableHeartbeat, enableHeartbeat);
    json.Jsonize("check_need_update_snapshot", checkNeedUpdateSnapshot,
                 checkNeedUpdateSnapshot);
    json.Jsonize("disable_biz_not_exist_log", disableBizNotExistLog,
                 disableBizNotExistLog);
    json.Jsonize("disable_metric_report", disableMetricReport,
                 disableMetricReport);
    json.Jsonize("simplify_metric_report", simplifyMetricReport,
                 simplifyMetricReport);
    json.Jsonize("metric_report_sampling_rate", metricReportSamplingRate,
                 metricReportSamplingRate);
    json.Jsonize("manager_update_time_interval", updateTimeInterval,
                 updateTimeInterval);
    json.Jsonize("hash_policy", hashPolicy, hashPolicy);
    json.Jsonize("call_delegation_queue_size", callDelegationQueueSize, callDelegationQueueSize);
}

bool MiscConfig::operator==(const MiscConfig &rhs) const {
    return snapshotLogCount == rhs.snapshotLogCount &&
           virtualNodeCount == rhs.virtualNodeCount &&
           heartbeatTimeout == rhs.heartbeatTimeout &&
           heartbeatThreadCount == rhs.heartbeatThreadCount &&
           callbackThreadNum == rhs.callbackThreadNum &&
           grpcChannelPollIntervalMs == rhs.grpcChannelPollIntervalMs &&
           weightUpdateStep == rhs.weightUpdateStep &&
           logPrefix == rhs.logPrefix &&
           enableHeartbeat == rhs.enableHeartbeat &&
           checkNeedUpdateSnapshot == rhs.checkNeedUpdateSnapshot &&
           simplifyMetricReport == rhs.simplifyMetricReport &&
           metricReportSamplingRate == rhs.metricReportSamplingRate &&
           hashPolicy == rhs.hashPolicy;
}

bool MiscConfig::enableConsistentHash() const { return virtualNodeCount != 0; }

HashPolicy MiscConfig::getHashPolicy() const {
    if (virtualNodeCount != 0) {
        return HashPolicy::CONSISTENT_HASH;
    }
    if (hashPolicy == "consistent_hash") {
        return HashPolicy::CONSISTENT_HASH;
    } else if (hashPolicy == "random_hash") {
        return HashPolicy::RANDOM_HASH;
    }
    return HashPolicy::RANDOM_WEIGHTED_HASH;
}

void GigRpcServerConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("heartbeat_thread_num", heartbeatThreadNum,
                 heartbeatThreadNum);
    json.Jsonize("heartbeat_queue_size", heartbeatQueueSize,
                 heartbeatQueueSize);
    json.Jsonize("enable_agent_stat", enableAgentStat, enableAgentStat);
}

bool GigRpcServerConfig::operator==(const GigRpcServerConfig &rhs) const {
    return heartbeatThreadNum == rhs.heartbeatThreadNum &&
           heartbeatQueueSize == rhs.heartbeatQueueSize;
}

void EagleeyeConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("e_appid", eAppId, eAppId);
    json.Jsonize("e_dataid", eDataId, eDataId);
    json.Jsonize("e_group", eGroup, eGroup);
    json.Jsonize("e_appgroup", eAppGroup, eAppGroup);
}

bool EagleeyeConfig::operator==(const EagleeyeConfig &rhs) const {
    return eAppId == rhs.eAppId && eDataId == rhs.eDataId &&
           eGroup == rhs.eGroup && eAppGroup == rhs.eAppGroup;
}

} // namespace multi_call
