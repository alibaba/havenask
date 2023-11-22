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
#include "suez/worker/KMonitorManager.h"

#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "kmonitor/client/KMonitorFactory.h"
#include "kmonitor/client/KMonitorWorker.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsConfig.h"
#include "kmonitor/client/core/MetricsSystem.h"
#include "suez/sdk/KMonitorMetaInfo.h"
#include "suez/sdk/RpcServer.h"
#include "suez/worker/EnvParam.h"
#include "suez/worker/KMonitorDebugService.h"
#include "suez/worker/KMonitorMetaParser.h"
#include "suez/worker/ManuallySink.h"

using namespace kmonitor;
using namespace std;
using namespace autil;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, KMonitorManager);

KMonitorManager::KMonitorManager() {}

KMonitorManager::~KMonitorManager() { KMonitorFactory::Shutdown(); }

bool KMonitorManager::init(const EnvParam &param) {
    if (!initMetricsFactory(param)) {
        AUTIL_LOG(ERROR, "init metrics factory failed");
        return false;
    }
    return true;
}

bool KMonitorManager::postInit(const EnvParam &param, RpcServer *server) {
    if (param.kmonitorManuallyMode && !registerRpcService(server)) {
        AUTIL_LOG(ERROR, "register rpc service failed");
        return false;
    }
    return true;
}

bool KMonitorManager::initMetricsFactory(const EnvParam &param) {
    KMonitorMetaInfo kmonMetaInfo;
    if (param.kmonitorServiceName.empty()) { // compatible with ha3 qrs && searcher
        KMonitorMetaParam metaParam;
        metaParam.partId = param.partId;
        metaParam.serviceName = param.serviceName;
        metaParam.amonitorPath = param.amonitorPath;
        metaParam.roleType = param.roleType;
        if (!KMonitorMetaParser::parse(metaParam, kmonMetaInfo)) {
            AUTIL_LOG(ERROR, "parse kmonitor meta info failed.");
            return false;
        }
    } else {
        kmonMetaInfo.serviceName = param.kmonitorServiceName;
        kmonMetaInfo.metricsPrefix = param.kmonitorMetricsPrefix;
        kmonMetaInfo.globalTableMetricsPrefix = param.kmonitorGlobalTableMetricsPrefix;
        kmonMetaInfo.tableMetricsPrefix = param.kmonitorTableMetricsPrefix;
        kmonMetaInfo.tagsMap = param.kmonitorTags;
    }
    if (!param.kmonitorMetricsReporterCacheLimit.empty()) {
        size_t limit = 0;
        if (StringUtil::fromString<size_t>(param.kmonitorMetricsReporterCacheLimit, limit) || limit > 0) {
            MetricsReporter::setMetricsReporterCacheLimit(limit);
            AUTIL_LOG(INFO, "set metrics reporter cache limit [%lu].", limit);
        }
    }

    if (param.kmonitorNormalSamplePeriod > 0) {
        AUTIL_LOG(INFO, "set kmonitor normal sample period [%d] seconds.", param.kmonitorNormalSamplePeriod);
        MetricLevelConfig config;
        config.period[NORMAL] = (unsigned int)param.kmonitorNormalSamplePeriod;
        MetricLevelManager::SetGlobalLevelConfig(config);
    }

    MetricsConfig metricsConfig;
    metricsConfig.set_tenant_name(param.kmonitorTenant);
    metricsConfig.set_service_name(kmonMetaInfo.serviceName);
    metricsConfig.set_sink_address((param.kmonitorSinkAddress + ":" + param.kmonitorPort).c_str());
    metricsConfig.set_enable_log_file_sink((param.kmonitorEnableLogFileSink));
    metricsConfig.set_manually_mode(param.kmonitorManuallyMode);
    metricsConfig.set_inited(true);
    metricsConfig.AddGlobalTag("hippo_slave_ip", param.hippoSlaveIp);
    for (auto &pair : kmonMetaInfo.tagsMap) {
        metricsConfig.AddGlobalTag(pair.first, pair.second);
    }
    if (!KMonitorFactory::Init(metricsConfig)) {
        AUTIL_LOG(ERROR, "init kmonitor factory failed with[%s]", FastToJsonString(metricsConfig, true).c_str());
        return false;
    }
    AUTIL_LOG(INFO, "before KMonitorFactory::Start(), config[%s]", FastToJsonString(metricsConfig).c_str());
    KMonitorFactory::Start();
    AUTIL_LOG(INFO, "KMonitorFactory::Start() finished");
    KMonitorFactory::registerBuildInMetrics(nullptr, kmonMetaInfo.metricsPrefix);
    AUTIL_LOG(INFO, "KMonitorFactory::registerBuildInMetrics() finished");
    _metaInfo = std::move(kmonMetaInfo);
    return true;
}

bool KMonitorManager::registerRpcService(RpcServer *server) {
    _rpcService.reset(new KMonitorDebugService(*this));
    arpc::ThreadPoolDescriptor threadPoolDesc("KMonDebug", 1, 1);
    if (!server->RegisterService(_rpcService.get(), threadPoolDesc)) {
        AUTIL_LOG(ERROR, "register rpc service failed");
        return false;
    }
    if (auto *httpServer = server->httpRpcServer; httpServer) {
        if (!httpServer->setProtoJsonizer(_rpcService.get(), "snapshot", std::make_shared<SnapshotProtoJsonizer>())) {
            AUTIL_LOG(ERROR, "register http proto jsonizer failed");
            return false;
        }
        AUTIL_LOG(INFO, "register http proto jsonizer success");
    }

    auto *metricsSystem = getMetricsSystem();
    if (!metricsSystem) {
        AUTIL_LOG(ERROR, "get metrics system failed");
        return false;
    }
    _sink.reset(new ManuallySink());
    if (!metricsSystem->AddSink(_sink)) {
        assert(false && "add sink failed");
    }
    AUTIL_LOG(WARN, "kmonitor manually mode enabled, metrics will not upload automatically");
    return true;
}

const KMonitorMetaInfo &KMonitorManager::getMetaInfo() const { return _metaInfo; }

std::string KMonitorManager::manuallySnapshot() {
    autil::ScopedLock _(_manuallySnapshotLock);
    auto *metricsSystem = getMetricsSystem();
    if (!metricsSystem) {
        return "invalid metrics system";
    }
    metricsSystem->ManuallySnapshot();
    return _sink->getLastResult();
}

kmonitor::MetricsSystem *KMonitorManager::getMetricsSystem() const {
    auto *worker = KMonitorFactory::GetWorker();
    if (!worker) {
        return nullptr;
    }
    return worker->getMetricsSystem();
}

} // namespace suez
