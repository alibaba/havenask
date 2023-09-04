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
#include "sql/config/NaviConfigAdapter.h"

#include <assert.h>
#include <cstdint>
#include <iosfwd>
#include <set>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "multi_call/config/FlowControlConfig.h"
#include "multi_call/config/MultiCallConfig.h"
#include "navi/common.h"
#include "navi/config/LogConfig.h"
#include "navi/config/NaviConfig.h"
#include "navi/config/NaviRegistryConfigMap.h"

using namespace std;
using namespace navi;
using namespace autil;
using namespace autil::legacy;

namespace sql {
AUTIL_LOG_SETUP(sql, NaviConfigAdapter);

static const string THREAD_NUM = "threadNum";
static const string NAVI_DISABLE_PERF = "naviDisablePerf";
static const string NAVI_THREAD_NUM = "naviThreadNum";
static const string NAVI_QUEUE_SIZE = "naviQueueSize";
static const string NAVI_PROCESSING_SIZE = "naviProcessingSize";
static const string THREAD_NUM_SCALE_FACTOR = "threadNumScaleFactor";
static const string THREAD_NUM_SCALE_FACTOR_INNER = THREAD_NUM_SCALE_FACTOR + "Inner";
static const string NAVI_LOG_LEVEL = "naviLogLevel";
static const string NAVI_LOG_KEEP_COUNT = "naviLogKeepCount";
static const string GRPC_PROTOCOL_THREAD_NUM = "grpcProtocolThreadNum";
static const string GRPC_PROTOCOL_KEEP_ALIVE_INTERVAL = "grpcProtocolKeepAliveInterval";
static const string GRPC_PROTOCOL_KEEP_ALIVE_TIMEOUT = "grpcProtocolKeepAliveTimeout";
static const string NAVI_EXTRA_TASK_QUEUE = "naviExtraTaskQueue";
static const string QUEUE_SEPARATOR = ";";
static const string QUEUE_PARAM_SEPARATOR = "|";
static int32_t DEFAULT_MAX_THREAD_NUM = 400;

NaviConfigAdapter::NaviConfigAdapter()
    : _disablePerf(false)
    , _threadNum(0)
    , _queueSize(0)
    , _processingSize(0) {}

NaviConfigAdapter::~NaviConfigAdapter() {}

bool NaviConfigAdapter::init() {
    if (!parseConfig()) {
        SQL_LOG(ERROR, "parse config failed");
        return false;
    }
    return true;
}

void NaviConfigAdapter::setGigClientConfig(
    const multi_call::MultiCallConfig &mcConfig,
    const multi_call::FlowControlConfigMap &mcFlowConfigMap) {
    assert(!_hasMcConfig);

    SQL_LOG(INFO,
            "set gig client config mcConfig[%s] mcFlowConfigMap[%s]",
            FastToJsonString(mcConfig, true).c_str(),
            FastToJsonString(mcFlowConfigMap, true).c_str());
    _hasMcConfig = true;
    _mcConfig = mcConfig;
    _mcFlowConfigMap = mcFlowConfigMap;

    multi_call::ProtocolConfig protocolConf;
    protocolConf.threadNum = _grpcProtocolThreadNum;
    protocolConf.keepAliveInterval = _grpcProtocolKeepAliveInterval;
    protocolConf.keepAliveTimeout = _grpcProtocolKeepAliveTimeout;
    _mcConfig.connectConf["grpc_stream"] = protocolConf;
    _mcConfig.miscConf.logPrefix = "NaviStream";
}

void NaviConfigAdapter::setPartInfo(int32_t partCount, vector<int32_t> partIds) {
    SQL_LOG(INFO, "set part info %d %s", partCount, StringUtil::toString(partIds).c_str());
    _partCount = partCount;
    _partIds = partIds;
}

bool NaviConfigAdapter::createNaviConfig(const std::string &bizName, navi::NaviConfig &naviConfig) {
    auto &bizConfig = naviConfig.bizMap[bizName];
    bizConfig.partCount = _partCount;
    bizConfig.partIds.insert(_partIds.begin(), _partIds.end());
    if (_addResource) {
        bizConfig.resources.insert("sql.tablet_manager_r");
        bizConfig.resources.insert("SqlBizResource");
        bizConfig.resources.insert("ObjectPoolResource");
        bizConfig.resources.insert("SqlConfigResource");
        bizConfig.resources.insert("HashFunctionCacheR");
    }
    bizConfig.kernels = {
        "sql\\..*",
        "suez_navi\\..*",
    };
    bizConfig.kernelBlacklist = {"sql.AsyncScanKernel",
                                 "sql.KhronosTableScanKernel",
                                 "sql.LeftMultiJoinKernel",
                                 "sql.LookupJoinKernel",
                                 "sql.ScanKernel",
                                 "sql.TabletOrcScanKernel"};
    addLogConfig(naviConfig);
    if (!addEngineConfig(naviConfig)) {
        SQL_LOG(ERROR, "add engine config failed");
        return false;
    }
    if (!addGigClientResourceConfig(naviConfig)) {
        SQL_LOG(ERROR, "add gig client resource config failed");
        return false;
    }
    return true;
}

bool NaviConfigAdapter::parseConfig() {
    parseMiscConfig();
    parseThreadPoolConfigThreadNum();
    parseThreadPoolConfigQueueConfig();
    if (!parseExtraTaskQueueConfig()) {
        return false;
    }
    SQL_LOG(INFO,
            "parse navi thread pool config: "
            "threadNum [%d], queueSize [%lu], processingSize [%lu]",
            _threadNum,
            _queueSize,
            _processingSize);

    parseLogConfig();
    parseGrpcConfig();
    SQL_LOG(INFO,
            "parse log config: "
            "logLevel [%s] logKeepCount [%lu]",
            _logLevel.c_str(),
            _logKeepCount);
    return true;
}

void NaviConfigAdapter::parseThreadPoolConfigThreadNum() {
    int threadNum = EnvUtil::getEnv(NAVI_THREAD_NUM, INVALID_THREAD_NUM);
    double factor = EnvUtil::getEnv(THREAD_NUM_SCALE_FACTOR, -1.0);
    if (factor < 0) {
        factor = EnvUtil::getEnv(THREAD_NUM_SCALE_FACTOR_INNER, -1.0);
    }

    if (threadNum != INVALID_THREAD_NUM) {
        if (factor > 0) {
            SQL_LOG(WARN,
                    "env [%s] will be ignored because [%s] is used, threadNum [%d]",
                    THREAD_NUM_SCALE_FACTOR.c_str(),
                    NAVI_THREAD_NUM.c_str(),
                    threadNum);
        }
    } else {
        if (factor > 0) {
            threadNum = sysconf(_SC_NPROCESSORS_ONLN) * factor + 0.1;
            if (threadNum >= DEFAULT_MAX_THREAD_NUM) {
                threadNum = DEFAULT_MAX_THREAD_NUM;
            }
        } else {
            threadNum = 0;
        }
    }

    _threadNum = threadNum;
}

void NaviConfigAdapter::parseMiscConfig() {
    _disablePerf = EnvUtil::getEnv(NAVI_DISABLE_PERF, false);
}

void NaviConfigAdapter::parseThreadPoolConfigQueueConfig() {
    _queueSize = EnvUtil::getEnv(NAVI_QUEUE_SIZE, navi::DEFAULT_QUEUE_SIZE);
    _processingSize = EnvUtil::getEnv(NAVI_PROCESSING_SIZE, navi::DEFAULT_PROCESSING_SIZE);
}

bool NaviConfigAdapter::parseExtraTaskQueueConfig() {
    std::string configStr = EnvUtil::getEnv(NAVI_EXTRA_TASK_QUEUE, "");
    if (configStr.empty()) {
        return true;
    }
    if (!initExtraTaskQueueFromStr(configStr)) {
        SQL_LOG(ERROR, "parse extra task queue config failed [%s]", configStr.c_str());
        return false;
    }
    return true;
}

void NaviConfigAdapter::parseLogConfig() {
    _logLevel = EnvUtil::getEnv<std::string>(NAVI_LOG_LEVEL, DEFAULT_LOG_LEVEL);
    _logKeepCount = EnvUtil::getEnv<size_t>(NAVI_LOG_KEEP_COUNT, DEFAULT_LOG_KEEP_COUNT);
}

void NaviConfigAdapter::parseGrpcConfig() {
    _grpcProtocolThreadNum
        = EnvUtil::getEnv<size_t>(GRPC_PROTOCOL_THREAD_NUM, DEFAULT_GRPC_PROTOCOL_THREAD_NUM);
    _grpcProtocolKeepAliveInterval = EnvUtil::getEnv<int64_t>(
        GRPC_PROTOCOL_KEEP_ALIVE_INTERVAL, DEFAULT_GRPC_PROTOCOL_KEEP_ALIVE_INTERVAL);
    _grpcProtocolKeepAliveTimeout = EnvUtil::getEnv<int64_t>(
        GRPC_PROTOCOL_KEEP_ALIVE_TIMEOUT, DEFAULT_GRPC_PROTOCOL_KEEP_ALIVE_TIMEOUT);
}

void NaviConfigAdapter::addLogConfig(navi::NaviConfig &naviConfig) {
    navi::FileLogConfig fileConfig;
    fileConfig.fileName = "./logs/navi.log";
    fileConfig.levelStr = _logLevel;
    fileConfig.asyncFlush = true;
    fileConfig.cacheLimit = 128;
    fileConfig.compress = true;
    fileConfig.maxFileSize = 1024;
    fileConfig.logKeepCount = _logKeepCount;
    fileConfig.flushThreshold = 1024;
    fileConfig.flushInterval = 1000;
    naviConfig.logConfig.fileAppenders.emplace_back(std::move(fileConfig));
    SQL_LOG(INFO, "navi log config [%s]", FastToJsonString(fileConfig, true).c_str());
}

bool NaviConfigAdapter::initExtraTaskQueueFromStr(const std::string &configStr) {
    auto vecStr = StringUtil::split(configStr, QUEUE_SEPARATOR);
    for (const auto &str : vecStr) {
        auto paramVec = StringUtil::split(str, QUEUE_PARAM_SEPARATOR);
        if (paramVec.size() != 4) {
            SQL_LOG(ERROR,
                    "size error in task extra queue config, paramVec[%s]",
                    autil::StringUtil::toString(paramVec).c_str());
            return false;
        }
        std::string queueName = paramVec[0];
        if (_extraTaskQueueMap.end() != _extraTaskQueueMap.find(queueName)) {
            SQL_LOG(
                ERROR, "error in task extra queue config, duplicated name[%s]", queueName.c_str());
            return false;
        }
        int threadNum;
        size_t queueSize;
        size_t processingSize;
        if (!(StringUtil::fromString<int>(paramVec[1], threadNum)
              && StringUtil::toString(threadNum) == paramVec[1])) {
            SQL_LOG(ERROR, "error in task extra queue config, threadNum error[%s]", str.c_str());
            return false;
        }
        if (!(StringUtil::fromString<size_t>(paramVec[2], queueSize)
              && StringUtil::toString(queueSize) == paramVec[2])) {
            SQL_LOG(ERROR, "error in task extra queue config, queueSize error[%s]", str.c_str());
            return false;
        }
        if (!(StringUtil::fromString<size_t>(paramVec[3], processingSize)
              && StringUtil::toString(processingSize) == paramVec[3])) {
            SQL_LOG(
                ERROR, "error in task extra queue config, processingSize error[%s]", str.c_str());
            return false;
        }
        ConcurrencyConfig config(threadNum, queueSize, processingSize);
        _extraTaskQueueMap.emplace(std::move(queueName), std::move(config));
    }
    return true;
}

bool NaviConfigAdapter::addEngineConfig(navi::NaviConfig &naviConfig) {
    EngineConfig config;
    config.builtinTaskQueue.threadNum = _threadNum;
    config.builtinTaskQueue.queueSize = _queueSize;
    config.builtinTaskQueue.processingSize = _processingSize;
    config.extraTaskQueueMap = _extraTaskQueueMap;
    config.disablePerf = _disablePerf;
    config.disableSymbolTable = _disablePerf;
    naviConfig.engineConfig = config;
    SQL_LOG(
        INFO, "navi engine config[%s]", FastToJsonString(naviConfig.engineConfig, true).c_str());
    return true;
}

bool NaviConfigAdapter::addGigClientResourceConfig(navi::NaviConfig &naviConfig) {
    string configStr = R"json({
    "init_config" : $$MCCONFIG,
    "flow_config" : $$FLOW_CONFIG
})json";
    StringUtil::replaceLast(configStr, "$$MCCONFIG", FastToJsonString(_mcConfig, true));
    StringUtil::replaceLast(configStr, "$$FLOW_CONFIG", FastToJsonString(_mcFlowConfigMap, true));

    NaviRegistryConfig registryConfig;
    if (!registryConfig.loadConfig(navi::GIG_CLIENT_RESOURCE_ID, configStr)) {
        SQL_LOG(ERROR,
                "create navi resource [%s] config registry failed, content [%s]",
                navi::GIG_CLIENT_RESOURCE_ID.c_str(),
                configStr.c_str());
        return false;
    }
    SQL_LOG(INFO,
            "add gig client resource config, name[%s], level[snapshot]",
            registryConfig.name.c_str());
    naviConfig.snapshotResourceConfigVec.emplace_back(std::move(registryConfig));
    return true;
}

} // namespace sql
