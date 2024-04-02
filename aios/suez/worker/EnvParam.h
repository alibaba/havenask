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
#pragma once

#include <map>
#include <string>

namespace suez {

struct EnvParam {
public:
    EnvParam();

public:
    bool init();
    bool check();
    const std::string toString() const;

private:
    static std::string tagsToString(const std::map<std::string, std::string> &tagsMap);
    static bool parseKMonitorTags(const std::string &tagsStr, std::map<std::string, std::string> &tagsMap);
    bool tryExtractPosIntValueFromEnv(const std::string &envName, int &value);
    bool initRdma();
    bool initGrpc();

public:
    std::string httpPort;
    int httpThreadNum;
    int httpQueueSize;
    int dpThreadNum;
    int loadThreadNum;
    int httpIoThreadNum;
    int decisionLoopInterval;
    bool waitForDebugger;
    bool decodeUri;
    bool haCompatible;
    bool enableSapSearch;
    bool allowPartialTableReady;
    bool allowReloadByConfig;
    bool needShutdownGracefully;
    bool noDiskQuotaCheck;
    std::string serviceName;
    std::string amonitorPort;
    std::string amonitorPath;
    std::string amonitorPathStyle;
    std::string roleType;
    std::string roleName;
    std::string zoneName;
    std::string partId;
    std::string hippoSlaveIp;
    std::string hippoDp2SlavePort;
    std::string carbonIdentifier;
    std::string procInstanceId;
    std::string packageCheckSum;
    int reportLoopInterval;
    int discardMetricSampleInterval;
    std::string kmonitorPort;
    std::string kmonitorServiceName;
    std::string kmonitorSinkAddress;
    bool kmonitorEnableLogFileSink;
    bool kmonitorEnablePrometheusSink;
    bool kmonitorManuallyMode;
    std::string kmonitorTenant;
    std::string kmonitorMetricsPrefix;
    std::string kmonitorGlobalTableMetricsPrefix;
    std::string kmonitorTableMetricsPrefix;
    std::string kmonitorMetricsReporterCacheLimit;
    std::map<std::string, std::string> kmonitorTags;
    int kmonitorNormalSamplePeriod;
    int asyncInterExecutorThreadNum;
    int asyncIntraExecutorThreadNum;
    std::string asyncInterExecutorType;
    std::string asyncIntraExecutorType;
    bool debugMode;
    bool localMode;
    std::string rdmaPort;
    int rdmaRpcThreadNum;    // arpc worker thread pool
    int rdmaRpcQueueSize;    // arpc worker thread pool
    int rdmaIoThreadNum;     // for rdma hard run
    int rdmaWorkerThreadNum; // for packet serialize
    // for grpc
    size_t gigGrpcThreadNum;
    std::string gigGrpcPort;
    std::string grpcCertsDir;
    std::string grpcTargetName;
    bool gigEnableAgentStat;
};

} // namespace suez
