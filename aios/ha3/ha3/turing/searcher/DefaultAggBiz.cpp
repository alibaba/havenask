#include <autil/legacy/json.h>
#include <ha3/turing/searcher/DefaultAggBiz.h>
#include <suez/common/PathDefine.h>
#include <ha3/config/TypeDefine.h>
#include <ha3/common/CommonDef.h>
#include <suez/turing/common/CavaConfig.h>

using namespace std;
using namespace suez;
using namespace tensorflow;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, DefaultAggBiz);

DefaultAggBiz::DefaultAggBiz(const string &aggGraphPrefix) {
    _aggGraphPrefix = aggGraphPrefix;
}

DefaultAggBiz::~DefaultAggBiz() {
}

tensorflow::Status DefaultAggBiz::preloadBiz() {
    _configAdapter.reset(new config::ConfigAdapter(_bizMeta.getLocalConfigPath()));
    return Status::OK();
}

bool DefaultAggBiz::isSupportAgg(const string &defaultAgg) {
    if (defaultAgg.find(HA3_DEFAULT_AGG_PREFIX) != string::npos) {
        return true;
    } else {
        return false;
    }
}
string DefaultAggBiz::getBizInfoFile() {
    const string &zoneName = _serviceInfo.getZoneName();
    return string(CLUSTER_CONFIG_DIR_NAME)  + "/" + zoneName + "/agg/" + _aggGraphPrefix +"_biz.json";
}

bool DefaultAggBiz::getDefaultBizJson(string &defaultBizJson) {
    const string &zoneName = _serviceInfo.getZoneName();
    string zoneBizName = zoneName + ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;

    string binaryPath = sap::EnvironUtil::getEnv("binaryPath", "");
    if (binaryPath.empty()) {
        string workDir = suez::PathDefine::getCurrentPath();
        binaryPath = string(workDir) + "/../binary";
    }
    string aggDefPath = binaryPath + "/usr/local/etc/ha3/agg/" + _aggGraphPrefix + ".def";
    JsonMap jsonMap;
    jsonMap["biz_name"] = Any(zoneName);
    jsonMap["graph_config_path"] = Any(aggDefPath);
    jsonMap["ops_config_path"] = Any(string(""));
    jsonMap["arpc_timeout"] = Any(1000);
    jsonMap["session_count"] = Any(1);
    jsonMap["need_log_query"] = Any(false);

    JsonArray dependencyTables;
    const string dependencyTableKey = "dependency_table";
    dependencyTables.push_back(Any(zoneName));
    jsonMap[dependencyTableKey] = dependencyTables;

    std::string mainTable = zoneName;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema;
    if (_configAdapter->getIndexPartitionSchema(zoneBizName, schema)) {
        mainTable = schema->GetSchemaName();
    }
    jsonMap["item_table_name"] = Any(mainTable);

    JsonMap runOptionsConfig;
    string useInterPool = suez::turing::WorkerParam::getEnv("interOpThreadPool");
    if (useInterPool.empty()) {
        runOptionsConfig["interOpThreadPool"] = Any(0); //global pool to parallel
    } else {
        runOptionsConfig["interOpThreadPool"] = Any(StringUtil::fromString<int>(useInterPool));
    }
    jsonMap["run_options"] = runOptionsConfig;
    string interPoolSize = suez::turing::WorkerParam::getEnv("iterOpThreadPoolSize");
    if (!interPoolSize.empty()) {
        JsonMap sessionOptionsConfig;
        sessionOptionsConfig["interOpParallelismThreads"] =  Any(StringUtil::fromString<int>(interPoolSize));
        jsonMap["session_config"] = sessionOptionsConfig;
    }

    suez::turing::CavaConfig bizCavaInfo;
    _configAdapter->getCavaConfig(zoneBizName, bizCavaInfo);
    jsonMap["cava_config"] = bizCavaInfo.getJsonMap();

    ClusterConfigInfo bizClusterInfo;
    _configAdapter->getClusterConfigInfo(zoneBizName, bizClusterInfo);
    JsonMap poolConfig;
    poolConfig["pool_trunk_size"] = Any(bizClusterInfo._poolTrunkSize);
    poolConfig["pool_recycle_size_limit"] = Any(bizClusterInfo._poolRecycleSizeLimit);
    poolConfig["pool_max_count"] = Any(bizClusterInfo._poolMaxCount);
    jsonMap["pool_config"] = poolConfig;

    JsonMap pluginConf;
    const string &confName = convertFunctionConf();
    if (confName.empty()) {
        HA3_LOG(ERROR, "convertFunctionConf failed");
        return false;
    }
    pluginConf["function_conf"] = confName;
    jsonMap["plugin_config"] = pluginConf;

    VersionConfig ha3VersionConf = _configAdapter->getVersionConfig();
    JsonMap versionConf;
    versionConf["protocol_version"] = ha3VersionConf.getProtocolVersion();
    versionConf["data_version"] = ha3VersionConf.getDataVersion();;
    jsonMap["version_config"] = versionConf;

    HA3_NS(config)::TuringOptionsInfo turingOptionsInfo;
    if (_configAdapter->getTuringOptionsInfo(zoneBizName, turingOptionsInfo)) {
        if (turingOptionsInfo._turingOptionsInfo.count(dependencyTableKey) > 0) {
            jsonMap[dependencyTableKey] =
                turingOptionsInfo._turingOptionsInfo[dependencyTableKey];
        }
    }

    defaultBizJson = ToJsonString(jsonMap);
    return true;
}

std::string DefaultAggBiz::convertFunctionConf() {
    const string &zoneName = _serviceInfo.getZoneName();
    string zoneBizName = zoneName + ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;
    suez::turing::FuncConfig funcConfig;
    _configAdapter->getFuncConfig(zoneBizName, funcConfig);
    auto content = ToJsonString(funcConfig);
    string configPath = _configAdapter->getConfigPath();
    string funcConfPath = fslib::fs::FileSystem::joinFilePath(configPath, "agg/_func_conf_.json");
    auto ret = fslib::fs::FileSystem::writeFile(funcConfPath, content);
    if (ret != fslib::EC_OK) {
        HA3_LOG(ERROR, "write funcConf [%s] failed, ErrorCode [%d]", funcConfPath.c_str(), ret);
        return "";
    }
    return "agg/_func_conf_.json";
}

END_HA3_NAMESPACE(turing);
