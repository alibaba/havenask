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
#include <cstddef>
#include <map>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/ConfigAdapter.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/config/TuringOptionsInfo.h"
#include "ha3/config/VersionConfig.h"
#include "ha3/turing/searcher/DefaultAggBiz.h"
#include "ha3/util/TypeDefine.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/misc/common.h"
#include "suez/sdk/PathDefine.h"
#include "suez/sdk/BizMeta.h"
#include "suez/sdk/ServiceInfo.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/expression/function/FuncConfig.h"
#include "tensorflow/core/lib/core/error_codes.pb.h"
#include "tensorflow/core/lib/core/status.h"

using namespace std;
using namespace suez;
using namespace tensorflow;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace isearch::config;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, DefaultAggBiz);

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

    string binaryPath = autil::EnvUtil::getEnv("binaryPath", "");
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
    string useMultiPartStr = autil::EnvUtil::getEnv("enableMultiPartition");
    if (!useMultiPartStr.empty() && useMultiPartStr != "false") {
        jsonMap["enable_multi_partition"] = Any(true);
    }

    JsonArray dependencyTables;
    const string dependencyTableKey = "dependency_table";
    dependencyTables.push_back(Any(zoneName));
    jsonMap[dependencyTableKey] = dependencyTables;

    std::string mainTable = zoneName;
    auto schema = _configAdapter->getIndexSchema(zoneBizName);
    if (schema) {
        mainTable = schema->GetTableName();
    }
    jsonMap["item_table_name"] = Any(mainTable);

    JsonMap runOptionsConfig;
    string useInterPool = autil::EnvUtil::getEnv("interOpThreadPool");
    if (useInterPool.empty()) {
        runOptionsConfig["interOpThreadPool"] = Any(0); //global pool to parallel
    } else {
        runOptionsConfig["interOpThreadPool"] = Any(StringUtil::fromString<int>(useInterPool));
    }
    jsonMap["run_options"] = runOptionsConfig;
    string interPoolSize = autil::EnvUtil::getEnv("iterOpThreadPoolSize");
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
        AUTIL_LOG(ERROR, "convertFunctionConf failed");
        return false;
    }
    pluginConf["function_conf"] = confName;
    jsonMap["plugin_config"] = pluginConf;

    VersionConfig ha3VersionConf = _configAdapter->getVersionConfig();
    JsonMap versionConf;
    versionConf["protocol_version"] = ha3VersionConf.getProtocolVersion();
    versionConf["data_version"] = ha3VersionConf.getDataVersion();;
    jsonMap["version_config"] = versionConf;

    isearch::config::TuringOptionsInfo turingOptionsInfo;
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
        AUTIL_LOG(ERROR, "write funcConf [%s] failed, ErrorCode [%d]", funcConfPath.c_str(), ret);
        return "";
    }
    return "agg/_func_conf_.json";
}

} // namespace turing
} // namespace isearch
