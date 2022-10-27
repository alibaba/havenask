#include <autil/legacy/json.h>
#include <ha3/turing/qrs/QrsBiz.h>
#include <ha3/service/QrsChainManagerCreator.h>
#include <ha3/qrs/QrsChainManager.h>
#include <ha3/common/CompressTypeConvertor.h>
#include <ha3/turing/ops/QrsSessionResource.h>
#include <ha3/turing/ops/QrsQueryResource.h>
#include <autil/StringUtil.h>
#include <libgen.h>
#include <suez/turing/common/CavaConfig.h>

using namespace std;
using namespace suez;
using namespace tensorflow;
using namespace autil::mem_pool;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace kmonitor;

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(monitor);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, QrsBiz);

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

QueryResourcePtr QrsBiz::createQueryResource() {
    QrsQueryResource *qrsQueryResource = new QrsQueryResource();
    qrsQueryResource->qrsSearchConfig = _qrsSearchConfig.get();
    QueryResourcePtr queryResourcePtr(qrsQueryResource);
    return queryResourcePtr;
}

QrsRunGraphContextArgs QrsBiz::getQrsRunGraphContextArgs() {
    QrsRunGraphContextArgs args;
    args.session = getSession();
    const vector<string> &inputs = getPlaceholders();
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
        HA3_LOG(INFO, "Qrs pluginMetricsReporter created");
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
        HA3_LOG(ERROR, "getQrsConfig failed!");
        return false;
    }

    HaCompressType resultCompressType = getCompressType(qrsConfig);
    if (resultCompressType == INVALID_COMPRESS_TYPE) {
        HA3_LOG(ERROR, "qrsConfig resultCompressType invalid!");
        return false;
    }
    QrsChainManagerPtr qrsChainMgrPtr = QrsChainManagerCreator::createQrsChainMgr(this);
    if (!qrsChainMgrPtr) {
        HA3_LOG(ERROR, "failed to create qrs chain manager");
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

HaCompressType QrsBiz::getCompressType(const QrsConfig &qrsConfig) {
    const string &compressType = qrsConfig._resultCompress._compressType;
    if (compressType.empty()) {
        return NO_COMPRESS;
    }
    HaCompressType convertedType = CompressTypeConvertor::convertCompressType(compressType);
    if (convertedType == INVALID_COMPRESS_TYPE) {
        HA3_LOG(ERROR, "Parse config failed: invalid result compress type[%s]",
                compressType.c_str());
    }
    return convertedType;
}

std::string QrsBiz::getBenchmarkBizName(const std::string &bizName) {
    return bizName + ":t";
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
                HA3_LOG(ERROR, "getAnomalyProcessConfig failed, zoneBizName[%s]",
                        searcherBizNames[i].c_str());
                return false;
            }
            fillCompatibleFieldInfo(flowConf);
            if (!_searchService->updateFlowConfig(searcherBizNames[i], &flowConf)) {
                HA3_LOG(ERROR, "updateFlowConfig failed, zoneBizName [%s], flowConf [%s]",
                        searcherBizNames[i].c_str(), ToJsonString(flowConf).c_str());
                return false;
            }
        }
        {
            AnomalyProcessConfig flowConfT;
            if (!_configAdapter->getAnomalyProcessConfigT(searcherBizNames[i], flowConfT))
            {
                HA3_LOG(INFO, "no benchmark flow control config, zoneBizName[%s], use normal config",
                        searcherBizNames[i].c_str());
                flowConfT = flowConf;
            }
            fillCompatibleFieldInfo(flowConfT);
            auto strategyName = getBenchmarkBizName(searcherBizNames[i]);
            if (!_searchService->updateFlowConfig(strategyName, &flowConfT)) {
                HA3_LOG(ERROR, "Benchmark updateFlowConfig failed, zoneBizName [%s], flowConf [%s]",
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
            HA3_LOG(WARN, "get turing biz [%s] flow config failed.", turingBizNames[i].c_str());
            return false;
        }
        for (auto flowConfig : flowConfigs) {
            if (kDefaultMultiConfig == flowConfig.first) {
                if (!_searchService->updateFlowConfig(turingBizNames[i], &(flowConfig.second))) {
                    HA3_LOG(WARN, "update turing biz [%s] flow config failed.", turingBizNames[i].c_str());
                    return false;
                }
            }
            if (!_searchService->updateFlowConfig(flowConfig.first, &(flowConfig.second))) {
                HA3_LOG(WARN, "update [%s] flow config failed.", flowConfig.first.c_str());
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

END_HA3_NAMESPACE(turing);
