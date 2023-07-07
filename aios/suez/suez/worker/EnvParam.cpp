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
#include "suez/worker/EnvParam.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "suez/sdk/CmdLineDefine.h"

using namespace std;
using namespace autil;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, EnvParam);

static const string KMONITOR_KEYVALUE_SEP = "^";
static const string KMONITOR_MULTI_SEP = "@";
static const string ASYNC_INTER_EXECUTOR_THREAD_NUM = "asyncInterExecutorThreadNum";
static const string ASYNC_INTER_EXECUTOR_TYPE = "asyncInterExecutorType";
static const string ASYNC_INTRA_EXECUTOR_THREAD_NUM = "asyncIntraExecutorThreadNum";
static const string ASYNC_INTRA_EXECUTOR_TYPE = "asyncIntraExecutorType";
static const string ENABLE_LOCAL_CATALOG = "enableLocalCatalog";
static const string ENABLE_UPDATE_CATALOG = "enableUpdateCatalog";

EnvParam::EnvParam()
    : httpThreadNum(8)
    , httpQueueSize(10000)
    , dpThreadNum(10)
    , loadThreadNum(10)
    , httpIoThreadNum(5)
    , decisionLoopInterval(1 * 1000 * 1000)
    , waitForDebugger(false)
    , decodeUri(false)
    , haCompatible(false)
    , enableSapSearch(false)
    , allowPartialTableReady(false)
    , needShutdownGracefully(false)
    , noDiskQuotaCheck(false)
    , reportLoopInterval(1 * 1000 * 1000)
    , discardMetricSampleInterval(60)
    , kmonitorNormalSamplePeriod(-1)
    , kmonitorEnableLogFileSink(false)
    , asyncInterExecutorThreadNum(10)
    , asyncIntraExecutorThreadNum(10)
    , enableLocalCatalog(false)
    , enableUpdateCatalog(false)
    , debugMode(false)
    , localMode(false)
    , rdmaRpcThreadNum(8)
    , rdmaRpcQueueSize(1000)
    , rdmaIoThreadNum(2)
    , rdmaWorkerThreadNum(5)
    , gigGrpcThreadNum(10)
    , gigEnableAgentStat(true) {}

bool EnvParam::parseKMonitorTags(const string &tagsStr, map<string, string> &tagsMap) {
    auto tagVec = StringUtil::split(tagsStr, KMONITOR_MULTI_SEP);
    for (const auto &tags : tagVec) {
        auto kvVec = StringUtil::split(tags, KMONITOR_KEYVALUE_SEP);
        if (kvVec.size() != 2) {
            AUTIL_LOG(WARN, "parse kmonitor tags [%s] failed.", tags.c_str());
            return false;
        }
        StringUtil::trim(kvVec[0]);
        StringUtil::trim(kvVec[1]);
        tagsMap[kvVec[0]] = kvVec[1];
    }
    return true;
}

string EnvParam::tagsToString(const map<string, string> &tagsMap) {
    vector<string> tagsVec;
    for (auto &pair : tagsMap) {
        tagsVec.emplace_back(pair.first + KMONITOR_KEYVALUE_SEP + pair.second);
    }
    return StringUtil::toString(tagsVec, KMONITOR_MULTI_SEP);
}

bool EnvParam::tryExtractPosIntValueFromEnv(const string &envName, int &value) {
    auto envStr = autil::EnvUtil::getEnv(envName, "");
    if (!envStr.empty()) {
        if (!StringUtil::fromString<int>(envStr, value) || value <= 0) {
            AUTIL_LOG(ERROR, "invalid env %s [%s]", envName.c_str(), envStr.c_str());
            return false;
        }
    }
    return true;
}

bool EnvParam::init() {
    httpPort = autil::EnvUtil::getEnv(HTTP_PORT, "");

    if (!tryExtractPosIntValueFromEnv(HTTP_THREAD_NUM, httpThreadNum)) {
        return false;
    }

    if (!tryExtractPosIntValueFromEnv(HTTP_QUEUE_SIZE, httpQueueSize)) {
        return false;
    }

    if (!tryExtractPosIntValueFromEnv(HTTP_IO_THREAD_NUM, httpIoThreadNum)) {
        return false;
    }

    if (!tryExtractPosIntValueFromEnv(DP_THREAD_NUM, dpThreadNum)) {
        return false;
    }

    if (!tryExtractPosIntValueFromEnv(LOAD_THREAD_NUM, loadThreadNum)) {
        return false;
    }

    if (!tryExtractPosIntValueFromEnv(DECISION_LOOP_INTERVAL, decisionLoopInterval)) {
        return false;
    }

    auto waitForDebuggerStr = autil::EnvUtil::getEnv(WAIT_FOR_DEBUGGER, "");
    if (waitForDebuggerStr == "true") {
        waitForDebugger = true;
    }

    auto decodeUriStr = autil::EnvUtil::getEnv(DECODE_URI, "");
    if (decodeUriStr == "true") {
        decodeUri = true;
    }

    auto haCompatibleStr = autil::EnvUtil::getEnv(HA_COMPATABLE, "");
    if (haCompatibleStr == "true") {
        haCompatible = true;
    }

    auto enableSapSearchStr = autil::EnvUtil::getEnv("enableSap", "");
    if (!enableSapSearchStr.empty()) {
        enableSapSearch = true;
    }

    auto allowPartialTableReadyStr = autil::EnvUtil::getEnv(ALLOW_PARTIAL_TABLE_READY, "");
    if (allowPartialTableReadyStr == "true") {
        allowPartialTableReady = true;
    }

    auto needShutdownGracefullyStr = autil::EnvUtil::getEnv(NEED_SHUTDOWN_GRACEFULLY, "");
    if (needShutdownGracefullyStr == "true") {
        needShutdownGracefully = true;
    }

    auto noDiskQuotaCheckStr = autil::EnvUtil::getEnv(NO_DISK_QUOTA_CHECK, "");
    if (noDiskQuotaCheckStr == "true") {
        noDiskQuotaCheck = true;
    }

    if (!tryExtractPosIntValueFromEnv(REPORT_INDEX_STATUS_LOOP_INTERVAL, reportLoopInterval)) {
        return false;
    }

    if (!tryExtractPosIntValueFromEnv(DISCARD_METRIC_SAMPLE_INTERVAL, discardMetricSampleInterval)) {
        return false;
    }

    serviceName = autil::EnvUtil::getEnv(SERVICE_NAME, "suez_service");
    amonitorPort = autil::EnvUtil::getEnv(AMONITOR_PORT, "10086");
    amonitorPath = autil::EnvUtil::getEnv(AMONITOR_PATH, "");
    amonitorPathStyle = autil::EnvUtil::getEnv(AMONITOR_PATH_STYLE, "");
    roleType = autil::EnvUtil::getEnv(ROLE_TYPE, "");
    roleName = autil::EnvUtil::getEnv(ROLE_NAME, "");
    zoneName = autil::EnvUtil::getEnv(ZONE_NAME, "");
    partId = autil::EnvUtil::getEnv(PART_ID, "");
    hippoSlaveIp = autil::EnvUtil::getEnv(HIPPO_SLAVE_IP, "127.0.0.1");
    hippoDp2SlavePort = autil::EnvUtil::getEnv(HIPPO_DP2_SLAVE_PORT, "");
    carbonIdentifier = autil::EnvUtil::getEnv(WORKER_IDENTIFIER_FOR_CARBON, "");
    procInstanceId = autil::EnvUtil::getEnv(HIPPO_PROC_INSTANCEID, "");
    packageCheckSum = autil::EnvUtil::getEnv(CARBON_PACKAGE_CHECKSUM, "");

    /***
        for kmon
    ***/
    kmonitorPort = autil::EnvUtil::getEnv(KMONITOR_PORT, "4141");
    kmonitorServiceName = autil::EnvUtil::getEnv(KMONITOR_SERVICE_NAME, "");
    kmonitorSinkAddress = autil::EnvUtil::getEnv(KMONITOR_SINK_ADDRESS, hippoSlaveIp);
    auto kmonitorEnableLogFileSinkStr = autil::EnvUtil::getEnv(KMONITOR_ENABLE_LOGFILE_SINK, "");
    if (kmonitorEnableLogFileSinkStr == "true") {
        kmonitorEnableLogFileSink = true;
    }
    kmonitorTenant = autil::EnvUtil::getEnv(KMONITOR_TENANT, "default");
    kmonitorMetricsPrefix = autil::EnvUtil::getEnv(KMONITOR_METRICS_PREFIX, "");
    kmonitorGlobalTableMetricsPrefix = autil::EnvUtil::getEnv(KMONITOR_GLOBAL_TABLE_METRICS_PREFIX, "");
    kmonitorTableMetricsPrefix = autil::EnvUtil::getEnv(KMONITOR_TABLE_METRICS_PREFIX, "");
    kmonitorMetricsReporterCacheLimit = autil::EnvUtil::getEnv(KMONITOR_METRICS_REPORTER_CACHE_LIMIT, "");
    string kmonitorTagsStr = autil::EnvUtil::getEnv(KMONITOR_TAGS, "");
    if (!kmonitorTagsStr.empty() && !parseKMonitorTags(kmonitorTagsStr, kmonitorTags)) {
        return false;
    }
    kmonitorNormalSamplePeriod = autil::EnvUtil::getEnv(KMONITOR_NORMAL_SAMPLE_PERIOD, -1);

    if (!tryExtractPosIntValueFromEnv(ASYNC_INTER_EXECUTOR_THREAD_NUM, asyncInterExecutorThreadNum)) {
        return false;
    }
    if (!tryExtractPosIntValueFromEnv(ASYNC_INTRA_EXECUTOR_THREAD_NUM, asyncIntraExecutorThreadNum)) {
        return false;
    }

    asyncInterExecutorType = autil::EnvUtil::getEnv(ASYNC_INTER_EXECUTOR_TYPE, "hos");
    asyncIntraExecutorType = autil::EnvUtil::getEnv(ASYNC_INTRA_EXECUTOR_TYPE, "hos");

    enableLocalCatalog = autil::EnvUtil::getEnv<bool>(ENABLE_LOCAL_CATALOG, false);
    enableUpdateCatalog = autil::EnvUtil::getEnv<bool>(ENABLE_UPDATE_CATALOG, false);
    debugMode = autil::EnvUtil::getEnv(DEBUG_MODE, false);
    localMode = autil::EnvUtil::getEnv(LOCAL_MODE, false);

    if (!initRdma()) {
        return false;
    }

    if (!initGrpc()) {
        return false;
    }
    AUTIL_LOG(INFO, "params after init: %s", toString().c_str());
    return check();
}

bool EnvParam::initRdma() {
    rdmaPort = autil::EnvUtil::getEnv(RDMA_PORT, "");

    if (!tryExtractPosIntValueFromEnv(RDMA_RPC_THREAD_NUM, rdmaRpcThreadNum)) {
        return false;
    }
    if (!tryExtractPosIntValueFromEnv(RDMA_RPC_QUEUE_SIZE, rdmaRpcQueueSize)) {
        return false;
    }
    if (!tryExtractPosIntValueFromEnv(RDMA_WORKER_THREAD_NUM, rdmaWorkerThreadNum)) {
        return false;
    }
    if (!tryExtractPosIntValueFromEnv(RDMA_IO_THREAD_NUM, rdmaIoThreadNum)) {
        return false;
    }
    return true;
}

bool EnvParam::initGrpc() {
    gigGrpcPort = EnvUtil::getEnv(GIG_GRPC_PORT);
    gigGrpcThreadNum = EnvUtil::getEnv(GIG_GRPC_THREAD_NUM, gigGrpcThreadNum);
    grpcCertsDir = EnvUtil::getEnv(GIG_GRPC_CERTS_DIR);
    grpcTargetName = EnvUtil::getEnv(GIG_GRPC_TARGET_NAME);
    gigEnableAgentStat = EnvUtil::getEnv(GIG_ENABLE_AGENT_STAT, true);
    return true;
}

const string EnvParam::toString() const {
    stringstream ss;
    ss << "httpPort:" << httpPort << "; "
       << "httpThreadNum:" << httpThreadNum << "; "
       << "httpQueueSize:" << httpQueueSize << "; "
       << "dpThreadNum:" << dpThreadNum << "; "
       << "loadThreadNum:" << loadThreadNum << "; "
       << "decisionLoopInterval:" << decisionLoopInterval << "; "
       << "decodeUri:" << decodeUri << "; "
       << "haCompatible:" << haCompatible << "; "
       << "httpIoThreadNum:" << httpIoThreadNum << "; "
       << "enableSapSearch:" << enableSapSearch << "; "
       << "allowPartialTableReady:" << allowPartialTableReady << "; "
       << "needShutdownGracefully:" << needShutdownGracefully << "; "
       << "noDiskQuotaCheck:" << noDiskQuotaCheck << "; "
       << "serviceName:" << serviceName << "; "
       << "amonitorPort:" << amonitorPort << "; "
       << "amonitorPath:" << amonitorPath << "; "
       << "amonitorPathStyle:" << amonitorPathStyle << "; "
       << "roleType:" << roleType << "; "
       << "roleName:" << roleName << "; "
       << "zoneName:" << zoneName << "; "
       << "partId:" << partId << "; "
       << "hippoSlaveIp:" << hippoSlaveIp << "; "
       << "hippoDp2SlavePort:" << hippoDp2SlavePort << "; "
       << "carbonIdentifier:" << carbonIdentifier << "; "
       << "procInstanceId:" << procInstanceId << "; "
       << "packageCheckSum:" << packageCheckSum << "; "
       << "discardMetricSampleInterval:" << discardMetricSampleInterval << "; "
       << "reportIndexStatusInterval:" << reportLoopInterval << "; "
       << "kmonitorPort:" << kmonitorPort << "; "
       << "kmonitorSinkAddress:" << kmonitorSinkAddress << "; "
       << "kmonitorEnableLogFileSink:" << kmonitorEnableLogFileSink << "; "
       << "kmonitorServiceName:" << kmonitorServiceName << "; "
       << "kmonitorTenant:" << kmonitorTenant << "; "
       << "kmonitorMetricsPrefix:" << kmonitorMetricsPrefix << "; "
       << "kmonitorGlobalTableMetricsPrefix:" << kmonitorGlobalTableMetricsPrefix << "; "
       << "kmonitorTableMetricsPrefix:" << kmonitorTableMetricsPrefix << "; "
       << "kmonitorMetricsReporterCacheLimit:" << kmonitorMetricsReporterCacheLimit << "; "
       << "kmonitorTags:" << tagsToString(kmonitorTags) << "; "
       << "asyncInterExecutorThreadNum:" << asyncInterExecutorThreadNum << "; "
       << "asyncIntraExecutorThreadNum:" << asyncIntraExecutorThreadNum << "; "
       << "asyncInterExecutorType:" << asyncInterExecutorType << "; "
       << "asyncIntraExecutorType:" << asyncIntraExecutorType << "; "
       << "enableLocalCatalog:" << enableLocalCatalog << "; "
       << "enableUpdateCatalog:" << enableUpdateCatalog << "; "
       << "debugMode:" << debugMode << "; "
       << "localMode:" << localMode << "; "
       << "rdmaPort:" << rdmaPort << "; "
       << "rdmaRpcThreadNum:" << rdmaRpcThreadNum << "; "
       << "rdmaRpcQueueSize:" << rdmaRpcQueueSize << "; "
       << "rdmaWorkerThreadNum:" << rdmaWorkerThreadNum << "; "
       << "rdmaIoThreadNum:" << rdmaIoThreadNum << "; ";
    return ss.str();
}

bool EnvParam::check() {
    if (localMode) {
        AUTIL_LOG(INFO, "deploy in local mode, do not need dp2 resource");
        return true;
    }
    uint32_t port;
    if (hippoDp2SlavePort.empty() || !StringUtil::strToUInt32(hippoDp2SlavePort.c_str(), port)) {
        AUTIL_LOG(ERROR, "invalid dp2 port [%s]", hippoDp2SlavePort.c_str());
        return false;
    }
    return true;
}

} // namespace suez
