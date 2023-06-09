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
#include <stdint.h>
#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CompressionUtil.h"
#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/AnomalyProcessConfig.h"
#include "ha3/config/ConfigAdapter.h"
#include "ha3/config/QrsConfig.h"
#include "ha3/config/VersionConfig.h"
#include "ha3/isearch.h"
#include "ha3/monitor/Ha3BizMetrics.h"
#include "ha3/qrs/QrsChainConfig.h"
#include "ha3/qrs/QrsChainManager.h"
#include "ha3/service/QrsChainManagerCreator.h"
#include "ha3/service/QrsSearchConfig.h"
#include "ha3/turing/ops/QrsQueryResource.h"
#include "ha3/turing/ops/QrsSessionResource.h"
#include "ha3/turing/qrs/QrsBiz.h"
#include "ha3/turing/qrs/QrsRunGraphContext.h"
#include "ha3/util/TypeDefine.h"
#include "ha3/util/XMLFormatUtil.h"
#include "kmonitor/client/MetricsReporter.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "suez/sdk/BizMeta.h"
#include "suez/turing/common/BizInfo.h"
#include "suez/turing/common/CommonDefine.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/protobuf/config.pb.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace std;
using namespace suez;
using namespace suez::turing;
using namespace tensorflow;
using namespace autil::mem_pool;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace kmonitor;

using namespace isearch::config;
using namespace isearch::qrs;
using namespace isearch::common;
using namespace isearch::service;
using namespace isearch::util;
using namespace isearch::monitor;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, QrsBiz);

QrsBiz::QrsBiz() {
    _qrsSearchConfig.reset(new QrsSearchConfig());
}

QrsBiz::~QrsBiz() {
}

QrsSearchConfigPtr QrsBiz::getQrsSearchConfig() {
    return _qrsSearchConfig;
}

ConfigAdapterPtr QrsBiz::getConfigAdapter() {
    return _configAdapter;
}

MetricsReporter *QrsBiz::getPluginMetricsReporter() {
    return _pluginMetricsReporter.get();
}

SessionResourcePtr QrsBiz::createSessionResource(uint32_t count) {
    if (!createQrsSearchConfig()) {
        return SessionResourcePtr();
    }
    QrsSessionResource *qrsSessionResource = new QrsSessionResource(count);
    qrsSessionResource->_clusterSorterManagers =
        _qrsSearchConfig->_qrsChainMgrPtr->getClusterSorterManagers();
    return SessionResourcePtr(qrsSessionResource);
}

suez::turing::QueryResourcePtr QrsBiz::createQueryResource() {
    QrsQueryResource *qrsQueryResource = new QrsQueryResource();
    qrsQueryResource->qrsSearchConfig = _qrsSearchConfig.get();
    QueryResourcePtr queryResourcePtr(qrsQueryResource);
    return queryResourcePtr;
}

QueryResourcePtr QrsBiz::prepareQueryResource(autil::mem_pool::Pool *pool) {
    auto queryResource = createQueryResource();
    queryResource->setPool(pool);
    queryResource->setIndexSnapshot(_sessionResource->createDefaultIndexSnapshot());
    size_t allocSizeLimit = _bizInfo._cavaConfig._allocSizeLimit * 1024 * 1024;
    queryResource->setCavaJitWrapper(_cavaJitWrapper.get());
    queryResource->setCavaAllocator(SuezCavaAllocatorPtr(new SuezCavaAllocator(pool, allocSizeLimit)));
    return queryResource;
}


QrsRunGraphContextArgs QrsBiz::getQrsRunGraphContextArgs() {
    QrsRunGraphContextArgs args;
    args.session = getDefaultSession();
    const vector<string> &inputs = getDefaultPlaceholders();
    args.inputs = &inputs;
    args.sessionResource = _sessionResource.get();
    args.runOptions = getRunOptions();
    return args;
}

string QrsBiz::getBizInfoFile() {
    return BIZ_JSON_FILE;
}

tensorflow::Status QrsBiz::preloadBiz() {
    _configAdapter.reset(new ConfigAdapter(_bizMeta.getLocalConfigPath()));
    if (_bizMetricsReporter) {
        static const string pluginsName = "plugins";
        _pluginMetricsReporter = _bizMetricsReporter->getSubReporter(pluginsName, {});
        AUTIL_LOG(INFO, "Qrs pluginMetricsReporter created");
    }
    return Status::OK();
}

bool QrsBiz::getDefaultBizJson(std::string &defaultBizJson) {
    QrsConfig qrsConfig;
    _configAdapter->getQrsConfig(qrsConfig);
    auto &bizCavaInfo = qrsConfig._cavaConfig;

    string qrsDefPath = getBinaryPath() + "/usr/local/etc/ha3/qrs_default.def";

    JsonMap jsonMap;
    jsonMap["biz_name"] = Any(string("qrs"));
    jsonMap["graph_config_path"] = Any(qrsDefPath);
    jsonMap["need_log_query"] = Any(false);

    JsonMap poolConfig;
    poolConfig["pool_trunk_size"] = Any(qrsConfig._poolConfig._poolTrunkSize);
    poolConfig["pool_recycle_size_limit"] = Any(qrsConfig._poolConfig._poolRecycleSizeLimit);
    poolConfig["pool_max_count"] = Any(qrsConfig._poolConfig._poolMaxCount);
    jsonMap["pool_config"] = poolConfig;

    JsonMap cavaConfig;
    cavaConfig["enable"] = Any(bizCavaInfo._enable);
    cavaConfig["enable_query_code"] = Any(bizCavaInfo._enableQueryCode);
    cavaConfig["cava_conf"] = Any(bizCavaInfo._cavaConf);
    cavaConfig["lib_path"] = Any(bizCavaInfo._libPath);
    cavaConfig["source_path"] = Any(bizCavaInfo._sourcePath);
    cavaConfig["code_cache_path"] = Any(bizCavaInfo._codeCachePath);
    cavaConfig["alloc_size_limit"] = Any(bizCavaInfo._allocSizeLimit);
    cavaConfig["module_cache_size"] = Any(bizCavaInfo._moduleCacheSize);
    jsonMap["cava_config"] = cavaConfig;

    VersionConfig ha3VersionConf = _configAdapter->getVersionConfig();

    JsonMap versionConf;
    versionConf["protocol_version"] = ha3VersionConf.getProtocolVersion();
    versionConf["data_version"] = ha3VersionConf.getDataVersion();;
    jsonMap["version_config"] = versionConf;

    // ugly but works
    _bizInfo._dependServices = qrsConfig._dependServices;
    defaultBizJson = ToJsonString(jsonMap);
    return true;
}

bool QrsBiz::createQrsSearchConfig() {
    QrsConfig qrsConfig;
    if (!_configAdapter->getQrsConfig(qrsConfig)) {
        AUTIL_LOG(ERROR, "getQrsConfig failed!");
        return false;
    }

    autil::CompressType resultCompressType = getCompressType(qrsConfig);
    if (resultCompressType == autil::CompressType::INVALID_COMPRESS_TYPE) {
        AUTIL_LOG(ERROR, "qrsConfig resultCompressType invalid!");
        return false;
    }
    QrsChainManagerPtr qrsChainMgrPtr = QrsChainManagerCreator::createQrsChainMgr(this);
    if (!qrsChainMgrPtr) {
        AUTIL_LOG(ERROR, "failed to create qrs chain manager");
        return false;
    }
    QrsSearchConfigPtr qrsSearchConfig(new QrsSearchConfig());
    qrsSearchConfig->_configDir = _configAdapter->getConfigPath();
    qrsSearchConfig->_qrsChainMgrPtr = qrsChainMgrPtr;
    qrsSearchConfig->_resultCompressType = resultCompressType;
    qrsSearchConfig->_metricsSrcWhiteList.insert(qrsConfig._metricsSrcWhiteList.begin(),
            qrsConfig._metricsSrcWhiteList.end());
    _qrsSearchConfig = qrsSearchConfig;
    return true;
}

autil::CompressType QrsBiz::getCompressType(const QrsConfig &qrsConfig) {
    const string &compressType = qrsConfig._resultCompress._compressType;
    if (compressType.empty()) {
        return autil::CompressType::NO_COMPRESS;
    }
    auto convertedType = autil::convertCompressType(compressType);
    if (convertedType == autil::CompressType::INVALID_COMPRESS_TYPE) {
        AUTIL_LOG(ERROR, "Parse config failed: invalid result compress type[%s]",
                compressType.c_str());
    }
    return convertedType;
}

bool QrsBiz::updateFlowConfig(const std::string &zoneBizName) {
    if (!_searchService || !_configAdapter) {
        return false;
    }
    if (!updateHa3ClusterFlowConfig()) {
        return false;
    }
    if (!updateTuringClusterFlowConfig()) {
        return false;
    }
    return true;
}

bool QrsBiz::updateHa3ClusterFlowConfig() {
    vector<string> searcherBizNames;
    _configAdapter->getClusterNamesWithExtendBizs(searcherBizNames);
    for (size_t i = 0; i < searcherBizNames.size(); ++i) {
        AnomalyProcessConfig flowConf;
        {
            if (!_configAdapter->getAnomalyProcessConfig(searcherBizNames[i], flowConf))
            {
                AUTIL_LOG(ERROR, "getAnomalyProcessConfig failed, zoneBizName[%s]",
                        searcherBizNames[i].c_str());
                return false;
            }
            fillCompatibleFieldInfo(flowConf);
            if (!_searchService->updateFlowConfig(searcherBizNames[i], &flowConf)) {
                AUTIL_LOG(ERROR, "updateFlowConfig failed, zoneBizName [%s], flowConf [%s]",
                        searcherBizNames[i].c_str(), ToJsonString(flowConf).c_str());
                return false;
            }
        }
        {
            AnomalyProcessConfig flowConfT;
            if (!_configAdapter->getAnomalyProcessConfigT(searcherBizNames[i], flowConfT))
            {
                AUTIL_LOG(INFO, "no benchmark flow control config, zoneBizName[%s], use normal config",
                        searcherBizNames[i].c_str());
                flowConfT = flowConf;
            }
            fillCompatibleFieldInfo(flowConfT);
            auto strategyName = getBenchmarkBizName(searcherBizNames[i]);
            if (!_searchService->updateFlowConfig(strategyName, &flowConfT)) {
                AUTIL_LOG(ERROR, "Benchmark updateFlowConfig failed, zoneBizName [%s], flowConf [%s]",
                        strategyName.c_str(), ToJsonString(flowConfT).c_str());
                return false;
            }
        }
    }
    return true;
}

bool QrsBiz::updateTuringClusterFlowConfig() {
    vector<string> turingBizNames;
    _configAdapter->getTuringClusterNames(turingBizNames);
    for (size_t i = 0; i < turingBizNames.size(); ++i) {
        map<string, autil::legacy::json::JsonMap> flowConfigs;
        if (!_configAdapter->getTuringFlowConfigs(turingBizNames[i], flowConfigs)) {
            AUTIL_LOG(WARN, "get turing biz [%s] flow config failed.", turingBizNames[i].c_str());
            return false;
        }
        for (auto flowConfig : flowConfigs) {
            if (kDefaultMultiConfig == flowConfig.first) {
                if (!_searchService->updateFlowConfig(turingBizNames[i], &(flowConfig.second))) {
                    AUTIL_LOG(WARN, "update turing biz [%s] flow config failed.", turingBizNames[i].c_str());
                    return false;
                }
            }
            if (!_searchService->updateFlowConfig(flowConfig.first, &(flowConfig.second))) {
                AUTIL_LOG(WARN, "update [%s] flow config failed.", flowConfig.first.c_str());
                return false;
            }
        }
    }
    return true;
}

void QrsBiz::fillCompatibleFieldInfo(config::AnomalyProcessConfig &flowConf) {
    flowConf["request_info_field"] = Any(std::string("gigRequestInfo"));
    flowConf["ec_field"] = Any(std::string("multicall_ec"));
    flowConf["response_info_field"] = Any(std::string("gigResponseInfo"));
}

} // namespace turing
} // namespace isearch
