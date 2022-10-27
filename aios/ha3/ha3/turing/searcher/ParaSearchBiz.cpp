#include <ha3/turing/searcher/ParaSearchBiz.h>
#include <suez/common/PathDefine.h>
#include <fslib/fs/FileSystem.h>
#include <suez/turing/common/CavaConfig.h>
using namespace std;
using namespace suez;
using namespace tensorflow;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, ParaSearchBiz);

ParaSearchBiz::ParaSearchBiz() {
}

ParaSearchBiz::~ParaSearchBiz() {
}

string ParaSearchBiz::getDefaultGraphDefPath() {
    const string &workDir = suez::PathDefine::getCurrentPath();
    string binaryPath = sap::EnvironUtil::getEnv("binaryPath", "");
    if (binaryPath.empty()) {
        binaryPath = workDir + "/../binary";
    }

    return binaryPath + "/usr/local/etc/ha3/searcher_" + _bizName + ".def";
}

string ParaSearchBiz::getConfigZoneBizName(const string &zoneName) {
    return zoneName + ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;
}

string ParaSearchBiz::getOutConfName(const string &confName) {
    string resName = "para/"+ _bizName + confName;
    return resName;
}

bool ParaSearchBiz::getDefaultBizJson(string &defaultBizJson) {
    const string &zoneName = _serviceInfo.getZoneName();
    const string &zoneBizName = getConfigZoneBizName(zoneName);

    ClusterConfigInfo bizClusterInfo;
    _configAdapter->getClusterConfigInfo(zoneBizName, bizClusterInfo);

    const string &searcherDefPath = getDefaultGraphDefPath();
    JsonMap jsonMap;
    jsonMap["biz_name"] = Any(zoneName);
    jsonMap["graph_config_path"] = Any(searcherDefPath);
    jsonMap["ops_config_path"] = Any(string(""));
    jsonMap["arpc_timeout"] = Any(500);
    jsonMap["session_count"] = Any(1);
    jsonMap["need_log_query"] = Any(false);

    JsonArray dependencyTables;
    const string dependencyTableKey = "dependency_table";
    dependencyTables.push_back(Any(zoneName));
    for (auto tableName : _configAdapter->getJoinClusters(zoneBizName)) {
        dependencyTables.push_back(Any(tableName));
    }
    jsonMap[dependencyTableKey] = dependencyTables;

    std::string mainTable = zoneName;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema;
    if (_configAdapter->getIndexPartitionSchema(zoneBizName, schema)) {
        mainTable = schema->GetSchemaName();
    }
    jsonMap["item_table_name"] = Any(mainTable);

    JsonArray join_infos;
    for (auto joinConfigInfo : bizClusterInfo._joinConfig.getJoinInfos()) {
        JsonMap joinConfig;
        FromJsonString(joinConfig, ToJsonString(joinConfigInfo));
        join_infos.push_back(joinConfig);
    }
    jsonMap["join_infos"] = join_infos;

    JsonMap poolConfig;
    poolConfig["pool_trunk_size"] = Any(bizClusterInfo._poolTrunkSize);
    poolConfig["pool_recycle_size_limit"] = Any(bizClusterInfo._poolRecycleSizeLimit);
    poolConfig["pool_max_count"] = Any(bizClusterInfo._poolMaxCount);
    jsonMap["pool_config"] = poolConfig;

    suez::turing::CavaConfig bizCavaInfo;
    _configAdapter->getCavaConfig(zoneBizName, bizCavaInfo);
    jsonMap["cava_config"] = bizCavaInfo.getJsonMap();

    JsonMap runOptionsConfig;
    string useInterPool = suez::turing::WorkerParam::getEnv("interOpThreadPool");
    if (useInterPool.empty()) {
        runOptionsConfig["interOpThreadPool"] = Any(0); // global thread pool
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

    JsonMap pluginConf;
    pluginConf["sorter_conf"] = convertSorterConf();
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

END_HA3_NAMESPACE(turing);
