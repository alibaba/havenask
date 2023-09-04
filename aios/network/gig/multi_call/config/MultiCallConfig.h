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
#ifndef ISEARCH_MULTI_CALL_MULTICALL_CONFIG_H
#define ISEARCH_MULTI_CALL_MULTICALL_CONFIG_H

#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/WorkerNodeInfo.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/opentelemetry/core/TraceConfig.h"
#include "autil/legacy/jsonizable.h"

namespace multi_call {

struct TopoNode;

class Cm2Config : public autil::legacy::Jsonizable
{
public:
    Cm2Config()
        : enableClusterBizSearch(false)
        , subMasterOnly(false)
        , subProtocolVersion(INVALID_VERSION_ID)
        , allGroupsTopo(false) {
    }
    ~Cm2Config() {
    }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool validate(bool &enabled) const;
    bool operator==(const Cm2Config &rhs) const;

public:
    std::string zkHost;
    std::string zkPath;
    bool enableClusterBizSearch;
    bool subMasterOnly;
    VersionTy subProtocolVersion;
    std::vector<std::string> clusters;
    std::vector<std::string> noTopoClusters;
    std::vector<std::string> groups;
    bool allGroupsTopo;

private:
    AUTIL_LOG_DECLARE();
};

class VipConfig : public autil::legacy::Jsonizable
{
public:
    VipConfig() {
    }
    ~VipConfig() {
    }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool validate(bool &enabled) const;
    bool operator==(const VipConfig &rhs) const;

public:
    std::string jmenvDom;
    std::vector<std::string> clusters;

private:
    AUTIL_LOG_DECLARE();
};

class IstioConfig : public autil::legacy::Jsonizable
{
public:
    IstioConfig()
        : xdsCallbackQueueSize(1000)
        , cacheFile("./local_cache/istio_cluster_cache")
        , readCache(true)
        , writeCache(true)
        , writeInterval(5 * 60)
        , xdsClientWorkerThreadNumber(1)
        , asyncSubscribe(true)
        , asyncSubIntervalSec(2)
        , enableHostTransform(true) {
    }
    ~IstioConfig() {
    }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool validate(bool &enabled) const;
    bool operator==(const IstioConfig &rhs) const;

public:
    // istio ip and port
    std::string istioHost;
    // hostnames to be subscribed, such as bizname.clustername
    std::set<std::string> clusters;

    size_t xdsCallbackQueueSize;
    std::string cacheFile;
    bool readCache;
    bool writeCache;
    int32_t writeInterval;
    // thread number to process received xds response
    int32_t xdsClientWorkerThreadNumber;
    // if send subscribe request on calling AddSub
    bool asyncSubscribe;
    int32_t asyncSubIntervalSec;
    // depressed
    bool enableHostTransform; // whether need transform

private:
    AUTIL_LOG_DECLARE();
};

class LocalJsonize : public autil::legacy::Jsonizable
{
public:
    LocalJsonize();
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool operator==(const LocalJsonize &rhs) const;
    bool validate() const;
    TopoNode getTopoNode() const;
    bool supportHeartbeat() const {
        return _supportHeartbeat;
    }

public:
    std::string _bizName;
    PartIdTy _partCnt;
    PartIdTy _partId;
    VersionTy _version;
    WeightTy _weight;
    std::string _ip;
    int32_t _tcpPort;
    int32_t _httpPort;
    int32_t _grpcPort;
    int32_t _rdmaPort;
    TopoNodeMeta _meta;
    bool _supportHeartbeat;
};

class LocalConfig
{
public:
    LocalConfig();
    ~LocalConfig();

public:
    bool validate(bool &enabled) const;
    bool operator==(const LocalConfig &rhs) const;
    bool empty() const;

public:
    std::vector<LocalJsonize> nodes;

private:
    AUTIL_LOG_DECLARE();
};

class SubscribeConfig : public autil::legacy::Jsonizable
{
public:
    SubscribeConfig() : allowEmptySub(true) {
    }
    ~SubscribeConfig() {
    }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    // virtual for ut
    virtual bool validate(bool &cm2Enabled, bool &vipEnabled, bool &localEnabled,
                          bool &istioEnabled) const;
    bool operator==(const SubscribeConfig &rhs) const;
    bool getCm2Configs(std::vector<Cm2Config> &cm2Configs) const;
    bool getIstioConfigs(std::vector<IstioConfig> &istioConfigs) const;

public:
    Cm2Config cm2Config; // deprecated
    VipConfig vipConfig; // deprecated
    LocalConfig localConfig;
    IstioConfig istioConfig;
    std::vector<Cm2Config> cm2Configs;
    std::vector<IstioConfig> istioConfigs;
    bool allowEmptySub;

private:
    AUTIL_LOG_DECLARE();
};

class SecureConfig : public autil::legacy::Jsonizable
{
public:
    SecureConfig() {
    }
    ~SecureConfig() {
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool operator==(const SecureConfig &rhs) const;

public:
    std::string pemRootCerts;
    std::string pemPrivateKey;
    std::string pemCertChain;
    std::string targetName;
};

struct GigRpcServerConfig : public autil::legacy::Jsonizable {
    GigRpcServerConfig()
        : heartbeatThreadNum(5) // default
        , heartbeatQueueSize(1000)
        , enableAgentStat(true) {
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool operator==(const GigRpcServerConfig &rhs) const;

    size_t heartbeatThreadNum;
    size_t heartbeatQueueSize;
    std::string logPrefix;
    bool enableAgentStat;
};

struct GrpcServerDescription {
    bool valid() const {
        // todo
        return ioThreadNum > 0;
    }
    size_t ioThreadNum;
    std::string port;
    SecureConfig secureConfig;
};

class ArpcServerDescription
{
public:
    ArpcServerDescription() : threadNum(1), queueSize(256), ioThreadNum(1) {
    }

    std::string name;
    std::string port;
    uint32_t threadNum;
    uint32_t queueSize;
    uint32_t ioThreadNum;
};

class HttpArpcServerDescription
{
public:
    HttpArpcServerDescription()
        : threadNum(8)
        , queueSize(10000)
        , ioThreadNum(5)
        , decodeUri(false)
        , haCompatible(false) {
    }

    std::string name;
    std::string port;
    uint32_t threadNum;
    uint32_t queueSize;
    uint32_t ioThreadNum;
    bool decodeUri;
    bool haCompatible;
};

class RdmaArpcServerDescription
{
public:
    RdmaArpcServerDescription()
        : arpcWorkerThreadNum(5)
        , arpcWorkerQueueSize(50)
        , rdmaIoThreadNum(1)
        , rdmaWorkerThreadNum(5) {
    }
    std::string name;
    std::string ip;
    std::string port;
    uint32_t arpcWorkerThreadNum;
    uint32_t arpcWorkerQueueSize;
    uint32_t rdmaIoThreadNum;
    uint32_t rdmaWorkerThreadNum;
};

class ProtocolConfig : public autil::legacy::Jsonizable
{
public:
    ProtocolConfig()
        : threadNum(DEFAULT_THREAD_NUM)
        , queueSize(DEFAULT_QUEUE_SIZE)
        , anetLoopTimeout(DEFAULT_TIMEOUT_LOOP_INTERVAL) {
    }
    ~ProtocolConfig() {
    }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool operator==(const ProtocolConfig &rhs) const;

public:
    uint32_t threadNum;
    uint32_t queueSize;
    int64_t anetLoopTimeout;
    int64_t keepAliveInterval = 4000; // ms
    int64_t keepAliveTimeout = 2000;  // ms
    SecureConfig secureConfig;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::map<std::string, ProtocolConfig> ConnectionConfig;

class MiscConfig : public autil::legacy::Jsonizable
{
public:
    MiscConfig()
        : snapshotLogCount(2000)
        , snapshotInterval(0)
        , virtualNodeCount(3000)
        , metricReportSamplingRate(0)
        , heartbeatTimeout(DEFAULT_TIMEOUT)
        , heartbeatThreadCount(DEFAULT_HEARTBEAT_THREAD_NUM)
        , heartbeatUnhealthInterval(DEFAULT_HEARTBEAT_UNHEALTH_INTERVAL)
        , callbackThreadNum(DEFAULT_CALLBACK_THREAD_NUM)
        , callProcessInterval(PROCESS_INTERVAL)
        , grpcChannelPollIntervalMs(DEFAULT_GRPC_CHANNEL_POLL_INTERVAL_MS)
        , weightUpdateStep(MIN_WEIGHT_UPDATE_STEP_FLOAT)
        , enableHeartbeat(false)
        , checkNeedUpdateSnapshot(false)
        , disableBizNotExistLog(false)
        , disableMetricReport(false)
        , simplifyMetricReport(false)
        , updateTimeInterval(UPDATE_TIME_INTERVAL)
        , callDelegationQueueSize(CALL_DELEGATION_QUEUE_SIZE) {
    }
    ~MiscConfig() {
    }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool operator==(const MiscConfig &rhs) const;
    bool enableConsistentHash() const;
    HashPolicy getHashPolicy() const;

public:
    size_t snapshotLogCount;
    size_t snapshotInterval;
    size_t virtualNodeCount;
    int32_t metricReportSamplingRate;
    int32_t heartbeatTimeout;
    int32_t heartbeatThreadCount;
    int32_t heartbeatUnhealthInterval;
    int32_t callbackThreadNum;
    int64_t callProcessInterval;
    int64_t grpcChannelPollIntervalMs;
    float weightUpdateStep;
    std::string logPrefix;
    bool enableHeartbeat;
    bool checkNeedUpdateSnapshot;
    bool disableBizNotExistLog;
    bool disableMetricReport;
    bool simplifyMetricReport;
    int64_t updateTimeInterval;
    std::string hashPolicy;
    size_t callDelegationQueueSize;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(MiscConfig);

class MultiCallConfig : public autil::legacy::Jsonizable
{
public:
    MultiCallConfig() {
    }
    ~MultiCallConfig() {
    }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool operator==(const MultiCallConfig &rhs) const;

public:
    SubscribeConfig subConf;
    ConnectionConfig connectConf;
    MiscConfig miscConf;
    opentelemetry::TraceConfig traceConf;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(MultiCallConfig);

class EagleeyeConfig : public autil::legacy::Jsonizable
{
public:
    EagleeyeConfig() {
    }
    ~EagleeyeConfig() {
    }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool operator==(const EagleeyeConfig &rhs) const;

public:
    std::string eAppId;
    std::string eDataId;
    std::string eGroup;
    std::string eAppGroup;
};

MULTI_CALL_TYPEDEF_PTR(EagleeyeConfig);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_MULTICALL_CONFIG_H
