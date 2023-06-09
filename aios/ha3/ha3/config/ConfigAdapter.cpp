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
#include "ha3/config/ConfigAdapter.h"

#include <assert.h>
#include <string.h>
#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

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
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "ha3/config/AnomalyProcessConfig.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/ClusterConfigParser.h"
#include "ha3/config/ClusterTableInfoManager.h"
#include "ha3/config/ClusterTableInfoValidator.h"
#include "ha3/config/FinalSorterConfig.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/config/QrsConfig.h"
#include "ha3/config/SqlConfig.h"
#include "ha3/config/VersionConfig.h"
#include "ha3/util/EnvParser.h"
#include "ha3/util/TypeDefine.h"
#include "indexlib/config/module_info.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/partition_define.h"
#include "indexlib/index_base/schema_adapter.h"
#include "suez/turing/common/BizInfo.h"
#include "fslib/util/FileUtil.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/util/TableInfoConfigurator.h"
#include "worker_framework/DataItem.h"


namespace isearch {
namespace config {
class AggSamplerConfigInfo;
class RankProfileConfig;
class SearchOptimizerConfig;
class SearcherCacheConfig;
class ServiceDegradationConfig;
class SummaryProfileConfig;
class TuringOptionsInfo;
}  // namespace config
}  // namespace isearch
namespace suez {
namespace turing {
class FuncConfig;
}  // namespace turing
}  // namespace suez

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace fslib::fs;
using namespace suez::turing;
using namespace isearch::util;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace worker_framework;
using namespace fslib::util;
namespace isearch {
namespace config {
AUTIL_LOG_SETUP(ha3, ConfigAdapter);

ConfigAdapter::ConfigAdapter(const string &configPath)
    : _configPath(configPath)
{
    initCanBeEmptyTable();
}

ConfigAdapter::~ConfigAdapter() {
}

void ConfigAdapter::initCanBeEmptyTable() {
    if (!_canBeEmptyTable.empty()) {
        return;
    }
    _canBeEmptyTable[AGGREGATE_SAMPLER_CONFIG_SECTION] = true;
    _canBeEmptyTable[BUILD_OPTION_CONFIG_SECTION] = true;
    _canBeEmptyTable[CLUSTER_CONFIG_INFO_SECTION] = false;
    _canBeEmptyTable[DOC_PROCESS_CHAIN_CONFIG_SECTION] = true;
    _canBeEmptyTable[FUNCTION_CONFIG_SECTION] = true;
    _canBeEmptyTable[FINAL_SORTER_CONFIG_SECTION] = true;
    _canBeEmptyTable[RANK_PROFILE_SECTION] = true;
    _canBeEmptyTable[SEARCH_OPTIMIZER_CONFIG_SECTION] = true;
    _canBeEmptyTable[SUMMARY_CONFIG_SECTION] = true;
    _canBeEmptyTable[SEARCHER_CACHE_CONFIG_SECTION] = true;
    _canBeEmptyTable[INDEX_OPTION_CONFIG_SECTION] = true;
    _canBeEmptyTable[SERVICE_DEGRADATION_CONFIG_SECTION] = true;
    _canBeEmptyTable[ANOMALY_PROCESS_CONFIG_SECTION] = true;
    _canBeEmptyTable[CAVA_CONFIG_SECTION] = true;
    _canBeEmptyTable[TURING_OPTIONS_SECTION] = true;
}

bool ConfigAdapter::convertParseResult(
        ParseResult ret, const string &sectionName) const
{
    if (ret == ClusterConfigParser::PR_OK) {
        return true;
    } else if (ret == ClusterConfigParser::PR_FAIL) {
        return false;
    } else {
        auto iter = _canBeEmptyTable.find(sectionName);
        if (iter == _canBeEmptyTable.end()) {
            return false;
        } else {
            return iter->second;
        }
    }
}

#define DECLARE_GET_CONFIG_FUNCTION(configType, sectionName)            \
    bool ConfigAdapter::get##configType(                                \
            const string &clusterName, configType &config) const        \
    {                                                                   \
        string configFileName = getClusterConfigFileName(clusterName);  \
        ClusterConfigParser parser(_configPath, configFileName);        \
        ParseResult ret = parser.get##configType(config);               \
        return convertParseResult(ret, sectionName);                    \
    }

DECLARE_GET_CONFIG_FUNCTION(FuncConfig, FUNCTION_CONFIG_SECTION);
DECLARE_GET_CONFIG_FUNCTION(AggSamplerConfigInfo, AGGREGATE_SAMPLER_CONFIG_SECTION);
DECLARE_GET_CONFIG_FUNCTION(RankProfileConfig, RANK_PROFILE_SECTION);
DECLARE_GET_CONFIG_FUNCTION(SummaryProfileConfig, SUMMARY_CONFIG_SECTION);
DECLARE_GET_CONFIG_FUNCTION(FinalSorterConfig, FINAL_SORTER_CONFIG_SECTION);
DECLARE_GET_CONFIG_FUNCTION(SearchOptimizerConfig, SEARCH_OPTIMIZER_CONFIG_SECTION);
DECLARE_GET_CONFIG_FUNCTION(SearcherCacheConfig, SEARCHER_CACHE_CONFIG_SECTION);
DECLARE_GET_CONFIG_FUNCTION(ClusterConfigInfo, CLUSTER_CONFIG_INFO_SECTION);
DECLARE_GET_CONFIG_FUNCTION(ServiceDegradationConfig, SERVICE_DEGRADATION_CONFIG_SECTION);
DECLARE_GET_CONFIG_FUNCTION(AnomalyProcessConfig, ANOMALY_PROCESS_CONFIG_SECTION);
DECLARE_GET_CONFIG_FUNCTION(AnomalyProcessConfigT, ANOMALY_PROCESS_CONFIG_SECTION_T);
DECLARE_GET_CONFIG_FUNCTION(SqlAnomalyProcessConfig, ANOMALY_PROCESS_CONFIG_SECTION_SQL);
DECLARE_GET_CONFIG_FUNCTION(TuringOptionsInfo, TURING_OPTIONS_SECTION);

#undef DECLARE_GET_CONFIG_FUNCTION

bool ConfigAdapter::getCavaConfig(const string &clusterName, CavaConfig &config) const
{
    config._cavaConf = HA3_CAVA_CONF;// important!
    string configFileName = getClusterConfigFileName(clusterName);
    ClusterConfigParser parser(_configPath, configFileName);
    ParseResult ret = parser.getCavaConfig(config);
    return convertParseResult(ret, CAVA_CONFIG_SECTION);
}

vector<string> ConfigAdapter::splitName(const string &name) const {
    vector<string> names = autil::StringUtil::split(name, ZONE_BIZ_NAME_SPLITTER);
    assert(names.size() > 0);
    if (names.size() == 1) {
        names.push_back(DEFAULT_BIZ_NAME);
    }
    return names;
}

bool ConfigAdapter::getTuringFlowConfigs(const std::string &clusterName,
        std::map<std::string, autil::legacy::json::JsonMap> &flowConfigs) const {
    vector<string> names = splitName(clusterName);
    if (names.empty()) {
        return false;
    }
    string relativePath = string(CLUSTER_CONFIG_DIR_NAME)  + "/" +
                          names[0] + "/" + BIZ_JSON_FILE ;
    string bizFile =  FileUtil::joinFilePath(_configPath, relativePath);
    string content;
    if (!FileUtil::readFile(bizFile, content)) {
        AUTIL_LOG(ERROR, "read file %s failed", bizFile.c_str());
        return false;
    }
    suez::turing::BizInfo bizInfo;
    try {
        FromJsonString(bizInfo, content);
    } catch (const std::exception &e) {
        AUTIL_LOG(WARN, "parse json string failed [%s], exception [%s]",
                  content.c_str(), e.what());
        return false;
    }
    flowConfigs = bizInfo._flowConfigs;
    return true;
}

string ConfigAdapter::getClusterConfigFileName(const string &clusterName) const {
    vector<string> names = splitName(clusterName);
    if(StringUtil::startsWith(names[1], HA3_PARA_SEARCH_PREFIX)) {
        names[1] = DEFAULT_BIZ_NAME;
    }
    string relativePath = string(CLUSTER_CONFIG_DIR_NAME)  + "/" +
                          names[0] + "/" + names[1] + CLUSTER_JSON_FILE_SUFFIX;
    return FileUtil::joinFilePath(_configPath, relativePath);
}

bool ConfigAdapter::listDir(const string &path, fslib::FileList &fileList) const {
    fslib::ErrorCode errorCode = FileSystem::listDir(path, fileList);
    if (errorCode != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "List dir[%s] failed: ErrorCode[%d].", path.c_str(), errorCode);
        return false;
    }
    return true;
}

bool ConfigAdapter::getClusterNames(vector<string> &clusterNames) const {
    string groupConfigDir = FileUtil::joinFilePath(_configPath, CLUSTER_CONFIG_DIR_NAME);
    fslib::FileList upperList;
    if (!listDir(groupConfigDir, upperList)) {
        return false;
    }
    for(size_t i = 0; i < upperList.size(); i++) {
        const string &zoneName = upperList[i];
        string zoneConfigDir = FileUtil::joinFilePath(groupConfigDir, zoneName);
        fslib::ErrorCode errorCode = FileSystem::isDirectory(zoneConfigDir);
        if (errorCode == fslib::EC_TRUE) {
            fslib::FileList lowerList;
            if (!listDir(zoneConfigDir, lowerList)) {
                return false;
            }
            for (size_t j = 0; j < lowerList.size(); ++j) {
                const string &bizFileName = lowerList[j];
                size_t suffixLen = strlen(CLUSTER_JSON_FILE_SUFFIX);
                if (StringUtil::endsWith(bizFileName, CLUSTER_JSON_FILE_SUFFIX)
                    && bizFileName.length() > suffixLen)
                {
                    string bizName = bizFileName.substr(0, bizFileName.length() - suffixLen);
                    clusterNames.push_back(zoneName + ZONE_BIZ_NAME_SPLITTER + bizName);
                }
                // ignore other file
            }
        } else if (errorCode != fslib::EC_FALSE) {
            AUTIL_LOG(ERROR, "Failed to check [%s] isDirectory. errorcode: [%d]",
                    zoneConfigDir.c_str(), errorCode);
            return false;
        }
    }

    if (clusterNames.size() == 0) {
        return false;
    }
    return true;
}

bool ConfigAdapter::getTuringClusterNames(vector<string> &clusterNames) const {
    string groupConfigDir = FileUtil::joinFilePath(_configPath, CLUSTER_CONFIG_DIR_NAME);
    fslib::FileList upperList;
    if (!listDir(groupConfigDir, upperList)) {
        return false;
    }
    for(size_t i = 0; i < upperList.size(); i++) {
        const string &zoneName = upperList[i];
        string zoneConfigDir = FileUtil::joinFilePath(groupConfigDir, zoneName);
        fslib::ErrorCode errorCode = FileSystem::isDirectory(zoneConfigDir);
        if (errorCode == fslib::EC_TRUE) {
            fslib::FileList lowerList;
            if (!listDir(zoneConfigDir, lowerList)) {
                return false;
            }
            for (size_t j = 0; j < lowerList.size(); ++j) {
                const string &bizFileName = lowerList[j];
                if (bizFileName == BIZ_JSON_FILE) {
                    string content;
                    string bizFile = FileUtil::joinFilePath(zoneConfigDir, bizFileName);
                    if (!FileUtil::readFile(bizFile, content)) {
                        AUTIL_LOG(ERROR, "read file %s failed", bizFile.c_str());
                        return false;
                    }
                    suez::turing::BizInfo bizInfo;
                    try {
                        FromJsonString(bizInfo, content);
                    } catch (const std::exception &e) {
                        AUTIL_LOG(WARN, "parse json string failed [%s], exception [%s]",
                                content.c_str(), e.what());
                        return false;
                    }
                    clusterNames.push_back(zoneName + ZONE_BIZ_NAME_SPLITTER + bizInfo._bizName);
                }
            }
        } else if (errorCode != fslib::EC_FALSE) {
            AUTIL_LOG(ERROR, "Failed to check [%s] isDirectory. errorcode: [%d]",
                    zoneConfigDir.c_str(), errorCode);
            return false;
        }
    }
    return true;
}

bool ConfigAdapter::getClusterNamesWithExtendBizs(vector<string> &clusterNames) {
    vector<string> innerClusterNames;
    if (!getClusterNames(innerClusterNames)) {
        return false;
    }
    const string &paraWaysStr = getParaWays();
    vector<string> paraWaysVec;
    EnvParser::parseParaWays(paraWaysStr, paraWaysVec);

    for (auto &innerClusterName : innerClusterNames) {
        clusterNames.push_back(innerClusterName);
        const vector<string> &names = splitName(innerClusterName);
        if (names[1] == DEFAULT_BIZ_NAME) {
            for (auto &paraWay : paraWaysVec) {
                string paraBizStr = HA3_PARA_SEARCH_PREFIX + paraWay;
                clusterNames.push_back(names[0] +
                        ZONE_BIZ_NAME_SPLITTER + paraBizStr);
            }
        }
    }

    return true;
}

ClusterConfigMapPtr ConfigAdapter::getClusterConfigMap() {
    if (_clusterConfigMapPtr) {
        return _clusterConfigMapPtr;
    }
    ClusterConfigMapPtr tmpClusterConfigMapPtr(new ClusterConfigMap());
    vector<string> clusterNames;
    if (!getClusterNames(clusterNames)) {
        AUTIL_LOG(ERROR, "getClusterNames error");
        return _clusterConfigMapPtr;
    }
    const string &defaultAggStr = getDefaultAgg();
    const string &paraWaysStr = getParaWays();
    AUTIL_LOG(DEBUG, "clusterNames size:%zd", clusterNames.size());
    for (vector<string>::const_iterator it = clusterNames.begin();
         it != clusterNames.end(); ++it)
    {
        const string &clusterName = *it;
        ClusterConfigInfo clusterConfig;
        if (!getClusterConfigInfo(clusterName, clusterConfig)) {
            AUTIL_LOG(ERROR, "getClusterConfigInfo error, clusterName:%s",
                    clusterName.c_str());
            return _clusterConfigMapPtr;
        }
        //create hash func
        if (!clusterConfig.initHashFunc()) {
            AUTIL_LOG(ERROR, "create hash func error, clusterName:%s",
                    clusterName.c_str());
            return _clusterConfigMapPtr;
        }
        (*tmpClusterConfigMapPtr)[clusterName] = clusterConfig;
        const vector<string> &names = splitName(clusterName);
        if (names[1] == DEFAULT_BIZ_NAME) {
            if (!defaultAggStr.empty()) {
                string defaultAggBizName = names[0] +
                                           ZONE_BIZ_NAME_SPLITTER + defaultAggStr;
                (*tmpClusterConfigMapPtr)[defaultAggBizName] = clusterConfig;
            }
            vector<string> paraWaysVec;
            if(EnvParser::parseParaWays(paraWaysStr, paraWaysVec)) {
                for (const auto &paraWay: paraWaysVec) {
                    string paraBizStr = HA3_PARA_SEARCH_PREFIX + paraWay;
                    string paraSearchBizName = names[0] +
                                               ZONE_BIZ_NAME_SPLITTER +
                                               paraBizStr;
                    (*tmpClusterConfigMapPtr)[paraSearchBizName] = clusterConfig;
                }
            }
        }
    }
    _clusterConfigMapPtr = tmpClusterConfigMapPtr;
    return _clusterConfigMapPtr;
}

VersionConfig ConfigAdapter::getVersionConfig() const {
    VersionConfig config;
    string versionConfigStr;
    string fileName = HA3_VERSION_CONFIG_FILE_NAME;
    if (!readConfigFile(versionConfigStr, fileName)) {
        return VersionConfig();
    }
    try {
        FromJsonString(config, versionConfigStr);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(WARN, "parse version config fail, config str [%s], exception[%s]",
                versionConfigStr.c_str(), e.what());
        return VersionConfig();
    }
    return config;
}

bool ConfigAdapter::getQrsConfig(QrsConfig &qrsConfig) const {
    string qrsConfigStr;
    string fileName = QRS_CONFIG_JSON_FILE;
    if (!readConfigFile(qrsConfigStr, fileName)) {
        AUTIL_LOG(ERROR, "Get qrs config file fail: [%s]", QRS_CONFIG_JSON_FILE);
        return false;
    }
    try {
        FromJsonString(qrsConfig, qrsConfigStr);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR, "QrsConfig from json fail, exception[%s]", e.what());
        return false;
    }

    return true;
}

bool ConfigAdapter::getSqlConfig(SqlConfig &sqlConfig) const {
    string sqlConfigStr;
    string fileName = SQL_CONFIG_JSON_FILE;
    if (!readConfigFile(sqlConfigStr, fileName)) {
        AUTIL_LOG(WARN, "Get sql config file fail: [%s %s]", _configPath.c_str(),
                SQL_CONFIG_JSON_FILE);
    } else {
        try {
            FastFromJsonString(sqlConfig, sqlConfigStr);
        } catch (const ExceptionBase &e) {
            AUTIL_LOG(ERROR, "SqlConfig from json fail, exception[%s]", e.what());
            return false;
        }
    }
    return true;
}

const TableMapPtr& ConfigAdapter::getTableMap() {
    if (_tableMapPtr) {
        return _tableMapPtr;
    }
    ClusterConfigMapPtr clusterConfigMapPtr = getClusterConfigMap();
    if (!clusterConfigMapPtr) {
        return _tableMapPtr;
    }
    TableMapPtr tmpTableMapPtr(new TableMap());
    for (ClusterConfigMap::const_iterator it = (*clusterConfigMapPtr).begin();
         it != (*clusterConfigMapPtr).end(); ++it)
    {
        TableInfoPtr tableInfoPtr;
        if (!getTableInfo(it->second._tableName, tableInfoPtr)) {
            return _tableMapPtr;
        }
        (*tmpTableMapPtr)[it->second._tableName] = tableInfoPtr;
    }
    _tableMapPtr = tmpTableMapPtr;
    return _tableMapPtr;
}

const ClusterTableInfoMapPtr& ConfigAdapter::getClusterTableInfoMap() {
    if (_clusterTableInfoMapPtr) {
        return _clusterTableInfoMapPtr;
    }
    const string &defaultAggStr = getDefaultAgg();
    const string &paraWaysStr = getParaWays();
    const ClusterTableInfoManagerMapPtr &clusterTableInfoManagerMapPtr =
        getClusterTableInfoManagerMap();
    if (!clusterTableInfoManagerMapPtr) {
        AUTIL_LOG(ERROR, "get cluster table info manager failed!");
        return _clusterTableInfoMapPtr; // return NULL
    }
    _clusterTableInfoMapPtr.reset(new ClusterTableInfoMap);
    ClusterTableInfoManagerMap::const_iterator iter = clusterTableInfoManagerMapPtr->begin();
    for (; iter != clusterTableInfoManagerMapPtr->end(); iter++) {
        TableInfoPtr tableInfoPtr = iter->second->getClusterTableInfo();
        assert(tableInfoPtr);
        string clusterName = iter->first;
        (*_clusterTableInfoMapPtr)[clusterName] = tableInfoPtr;
        const vector<string> &names = splitName(clusterName);
        if (names[1] == DEFAULT_BIZ_NAME) {
            if (!defaultAggStr.empty()) {
                string defaultAggBizName = names[0] +
                                           ZONE_BIZ_NAME_SPLITTER + defaultAggStr;
                (*_clusterTableInfoMapPtr)[defaultAggBizName] = tableInfoPtr;
            }
            vector<string> paraWaysVec;
            if(EnvParser::parseParaWays(paraWaysStr, paraWaysVec)) {
                for (const auto &paraWay: paraWaysVec) {
                    string paraBizStr = HA3_PARA_SEARCH_PREFIX + paraWay;
                    string paraSearchBizName = names[0] +
                                               ZONE_BIZ_NAME_SPLITTER +
                                               paraBizStr;
                    (*_clusterTableInfoMapPtr)[paraSearchBizName] = tableInfoPtr;
                }
            } else {
                AUTIL_LOG(ERROR, "parse env paraSearchWays[%s] failed, "
                        "para search disabled.", paraWaysStr.c_str());
            }
        }
    }
    return _clusterTableInfoMapPtr;
}

ClusterTableInfoManagerMapPtr ConfigAdapter::getClusterTableInfoManagerMap() {
    if (_clusterTableInfoManagerMapPtr) {
        return _clusterTableInfoManagerMapPtr;
    }
    vector<string> clusterNames;
    if (!getClusterNames(clusterNames)) {
        AUTIL_LOG(ERROR, "get cluster names failed.");
        return ClusterTableInfoManagerMapPtr();
    }

    ClusterTableInfoManagerMapPtr tmpClusterTableInfoManagerMapPtr(
            new ClusterTableInfoManagerMap());
    for(vector<string>::const_iterator it = clusterNames.begin();
        it != clusterNames.end(); ++it)
    {
        ClusterTableInfoManagerPtr clusterTableInfoManagerPtr =
            getClusterTableInfoManager(*it);
        if (!clusterTableInfoManagerPtr) {
            return ClusterTableInfoManagerMapPtr();
        }
        (*tmpClusterTableInfoManagerMapPtr)[*it] = clusterTableInfoManagerPtr;
    }

    if (!ClusterTableInfoValidator::validate(tmpClusterTableInfoManagerMapPtr)) {
        AUTIL_LOG(ERROR, "validate cluster table infos failed!");
        return ClusterTableInfoManagerMapPtr();
    }
    _clusterTableInfoManagerMapPtr = tmpClusterTableInfoManagerMapPtr;
    return _clusterTableInfoManagerMapPtr;
}

vector<string> ConfigAdapter::getJoinClusters(const string &clusterName) const {
    vector<string> joinClusters;
    ClusterConfigInfo clusterConfigInfo;
    if (!getClusterConfigInfo(clusterName, clusterConfigInfo)) {
           AUTIL_LOG(ERROR, "get cluster config info failed for cluster [%s]!", clusterName.c_str());
           return joinClusters;
    }
    const vector<JoinConfigInfo> joinInfos =
        clusterConfigInfo._joinConfig.getJoinInfos();;
    for(size_t i = 0; i < joinInfos.size(); i++) {
        joinClusters.push_back(joinInfos[i].getJoinCluster());
    }
    return joinClusters;
}

ClusterTableInfoManagerPtr ConfigAdapter::getClusterTableInfoManager(
        const string &clusterName)
{
    TableInfoPtr mainTableInfoPtr = getTableInfoWithClusterName(clusterName, true);
    if (!mainTableInfoPtr) {
        AUTIL_LOG(ERROR, "main table not found for cluster [%s]", clusterName.c_str());
        return ClusterTableInfoManagerPtr();
    }
    ClusterTableInfoManagerPtr clusterTableInfoManagerPtr(new ClusterTableInfoManager);
    clusterTableInfoManagerPtr->setMainTableInfo(mainTableInfoPtr);
    ClusterConfigInfo clusterConfigInfo;
    if (!getClusterConfigInfo(clusterName, clusterConfigInfo)) {
           AUTIL_LOG(ERROR, "get cluster config info failed for cluster [%s]!", clusterName.c_str());
           return ClusterTableInfoManagerPtr();
    }
    const vector<JoinConfigInfo> joinInfos =
        clusterConfigInfo._joinConfig.getJoinInfos();;
    for(size_t i = 0; i < joinInfos.size(); i++) {
        const string &joinField = joinInfos[i].getJoinField();
        if (joinField.empty()) {
            continue;
        }
        const string &auxCluster = joinInfos[i].getJoinCluster();
        bool checkClusterConfig = joinInfos[i].getCheckClusterConfig();
        TableInfoPtr joinTableInfoPtr = getTableInfoWithClusterName(auxCluster, checkClusterConfig);
        if (!joinTableInfoPtr) {
            AUTIL_LOG(ERROR, "get aux cluster[%s] table failed", auxCluster.c_str());
            return ClusterTableInfoManagerPtr();
        }
        clusterTableInfoManagerPtr->addJoinTableInfo(joinTableInfoPtr, joinInfos[i]);
    }
    clusterTableInfoManagerPtr->setScanJoinClusterName(
        clusterConfigInfo._joinConfig.getScanJoinCluster());
    return clusterTableInfoManagerPtr;
}

TableInfoPtr ConfigAdapter::getTableInfoWithClusterName(const string &clusterName, bool checkClusterConfig) {
    ClusterConfigInfo clusterConfigInfo;
    string tableName;
    if (!checkClusterConfig) {
        tableName = clusterName;
    }
    else {
        if (!getClusterConfigInfo(clusterName, clusterConfigInfo)) {
            AUTIL_LOG(ERROR, "cluster[%s] not found", clusterName.c_str());
            return TableInfoPtr();
        }
        tableName = clusterConfigInfo._tableName;
    }
    TableInfoPtr tableInfoPtr;
    if (!getTableInfo(tableName, tableInfoPtr)) {
        AUTIL_LOG(ERROR, "get [%s]cluster table[%s] info failed.",
                clusterName.c_str(), tableName.c_str());
        return TableInfoPtr();
    }
    return tableInfoPtr;
}

bool ConfigAdapter::getSchemaString(const string& clusterName,
                                    string& schemaStr) const
{
    ClusterConfigInfo clusterConfigInfo;
    bool ret = getClusterConfigInfo(clusterName, clusterConfigInfo);
    if (!ret) {
        AUTIL_LOG(ERROR, "Failed to get clusterConfigInfo clusterName:[%s]",
                clusterName.c_str());
        return false;
    }
    return getTableSchemaConfigString(clusterConfigInfo.getTableName(), schemaStr);
}

bool ConfigAdapter::getTableSchemaConfigString(const string &tableName,
        string &configStr) const
{
    string configFileName = getSchemaConfigFileName(tableName);
    if (!readConfigFile(configStr, configFileName)) {
        AUTIL_LOG(ERROR, "getTableSchemaConfigString Failed! FileName[%s]",
                configFileName.c_str());
        return false;
    }
    return true;
}

string ConfigAdapter::getTableClusterConfigFileName(const string &tableName) const {
    return string(TABLE_CLUSTER_CONFIG_PATH) + "/" + tableName
        + string(TABLE_CLUSTER_JSON_FILE_SUFFIX);
}

bool ConfigAdapter::getTableClusterConfigString(const string &tableName,
        string &configStr) const
{
    string configFileName = getTableClusterConfigFileName(tableName);
    if (!readConfigFile(configStr, configFileName)) {
        AUTIL_LOG(ERROR, "getTableClusterConfigString Failed! FileName[%s]",
                configFileName.c_str());
        return false;
    }
    return true;
}

bool ConfigAdapter::getTableInfo(const string &tableName,
                                 TableInfoPtr &tableInfoPtr) const
{
    string configStr;
    if (!getTableSchemaConfigString(tableName, configStr)) {
        AUTIL_LOG(ERROR, "getTableInfo Failed! tableName[%s]",
                tableName.c_str());
        return false;
    }
    TableInfoConfigurator tableInfoConfigurator;
    tableInfoPtr = tableInfoConfigurator.createFromString(configStr);
    return (tableInfoPtr != NULL);
}

string ConfigAdapter::getSchemaConfigFileName(const string &tableName) const {
    return string(SCHEMA_CONFIG_PATH) + "/" + tableName
        + string(SCHEMA_JSON_FILE_SUFFIX);
}

bool ConfigAdapter::getAnalyzerConfigString(string &analyzerConfigStr) const {
    string fileName = ANALYZER_CONFIG_FILE;
    if (!readConfigFile(analyzerConfigStr, fileName)) {
        AUTIL_LOG(ERROR, "Get Config File Fail: [%s]", ANALYZER_CONFIG_FILE);
        return false;
    }
    return true;
}

bool ConfigAdapter::readConfigFile(string &result,
                                   const string &relativePath) const
{
    string filePath = FileUtil::joinFilePath(_configPath, relativePath);
    return FileUtil::readFile(filePath, result);
}

string ConfigAdapter::getPluginPath() const {
    return FileUtil::joinFilePath(_configPath, PLUGIN_PATH) + '/';
}

string ConfigAdapter::getAliTokenizerConfFilePath() const {
    return FileUtil::joinFilePath(_configPath, ALITOKENIZER_CONF_FILE);
}

bool ConfigAdapter::check() {
    ClusterConfigMapPtr clusterConfigMapPtr = getClusterConfigMap();
    if (!clusterConfigMapPtr) {
        AUTIL_LOG(ERROR, "failed to getClusterConfigMap");
        return false;
    }
    for (ClusterConfigMap::const_iterator it = clusterConfigMapPtr->begin();
         it != clusterConfigMapPtr->end(); it++)
    {
        const string &clusterName = it->first;
        if (StringUtil::endsWith(clusterName, HA3_DEFAULT_AGG)
            || StringUtil::endsWith(clusterName, DEFAULT_SQL_BIZ_NAME))
        {
            continue;
        }
        const ClusterConfigInfo &clusterConfigInfo = it->second;
        if (!clusterConfigInfo.check()) {
            AUTIL_LOG(ERROR, "check ClusterConfigInfo for cluster[%s] failed",
                    clusterName.c_str());
            return false;
        }

        auto schema = getIndexSchema(clusterName);
        if (!schema) {
            return false;
        }

        const auto &joinInfos = clusterConfigInfo._joinConfig.getJoinInfos();
        if (joinInfos.size() == 0) {
            continue;
        }

        TableInfoPtr mainTableInfo;
        vector<TableInfoPtr> joinTableInfos;
        if (!getTableInfo(clusterConfigInfo._tableName, mainTableInfo)) {
            AUTIL_LOG(ERROR, "getTableInfo for cluster[%s] failed",
                    clusterName.c_str());
            return false;
        }
        // TODO remove
        for (size_t i = 0; i < joinInfos.size(); i++) {
            string joinClusterName = joinInfos[i].getJoinCluster();
            const vector<string>& names = splitName(joinClusterName);
            if (names[1] == DEFAULT_BIZ_NAME) {
                joinClusterName = joinClusterName + ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;
            }
            ClusterConfigMap::const_iterator it2 =
                clusterConfigMapPtr->find(joinClusterName);
            if (it2 == clusterConfigMapPtr->end()) {
                AUTIL_LOG(ERROR, "join cluster[%s] does not exist",
                        joinClusterName.c_str());
                return false;
            }
            const ClusterConfigInfo &joinClusterConfigInfo = it2->second;
            if (!joinClusterConfigInfo.check())
            {
                AUTIL_LOG(ERROR, "join cluster[%s]'s config is invalid",
                        joinClusterName.c_str());
                return false;
            }

            TableInfoPtr joinTableInfo;
            if (!getTableInfo(joinClusterConfigInfo._tableName, joinTableInfo)) {
                AUTIL_LOG(ERROR, "getTableInfo for join cluster[%s] failed",
                        joinClusterName.c_str());
                return false;
            }
            if (!ClusterTableInfoValidator::validateMainTableAgainstJoinTable(
		    mainTableInfo, joinTableInfo, joinInfos[i].getJoinField(), joinInfos[i].useJoinPK))
            {
                AUTIL_LOG(ERROR, "validate mainTable[%s] against joinTable[%s] failed",
                        clusterConfigInfo._tableName.c_str(),
                        joinClusterConfigInfo._tableName.c_str());
                return false;
            }
            joinTableInfos.push_back(joinTableInfo);
        }
        if (!ClusterTableInfoValidator::validateJoinTables(joinTableInfos)) {
            AUTIL_LOG(ERROR, "validateJoinTables for cluster[%s] failed",
                    clusterName.c_str());
            return false;
        }
    }
    return true;
}

std::shared_ptr<indexlibv2::config::ITabletSchema>
ConfigAdapter::getIndexSchemaByTable(string table) const {
    string schemaStr;
    if (!getTableSchemaConfigString(table, schemaStr)) {
        return nullptr;
    }
    return indexlib::index_base::SchemaAdapter::LoadSchema(schemaStr);
}

std::shared_ptr<indexlibv2::config::ITabletSchema>
ConfigAdapter::getIndexSchemaByFileName(const std::string &fileName) const {
    std::string schemaStr;
    if (!readConfigFile(schemaStr, fileName)) {
        AUTIL_LOG(ERROR, "getTableSchemaConfigString Failed! FileName[%s]", fileName.c_str());
        return nullptr;
    }
    return indexlib::index_base::SchemaAdapter::LoadSchema(schemaStr);
}

std::shared_ptr<indexlibv2::config::ITabletSchema>
ConfigAdapter::getIndexSchema(string clusterName) const {
    string schemaStr;
    if (!getSchemaString(clusterName, schemaStr)) {
        AUTIL_LOG(ERROR, "read schema error, clusterName: %s", clusterName.c_str());
        return nullptr;
    }
    return indexlib::index_base::SchemaAdapter::LoadSchema(schemaStr);
}

string ConfigAdapter::getDefaultAgg() {
    string defaultAggStr = autil::EnvUtil::getEnv("defaultAgg", "");
    if (defaultAggStr.empty()) {
        defaultAggStr = string(HA3_DEFAULT_AGG_PREFIX) + "4";
    }
    if (defaultAggStr.find(HA3_DEFAULT_AGG_PREFIX) == string::npos) {
        AUTIL_LOG(INFO, "%s is not support agg by default.", defaultAggStr.c_str());
        return "";
    }
    return HA3_DEFAULT_AGG;
}


string ConfigAdapter::getParaWays() {
    const string &paraWaysStr = autil::EnvUtil::getEnv("paraSearchWays", "");
    if (paraWaysStr.empty()) {
        return string("2,4");
    }
    return paraWaysStr;
}

string ConfigAdapter::getSqlFunctionConfigFileName(const string &clusterName) const {
    return string(SQL_CONFIG_PATH) + "/" + clusterName
        + string(SQL_FUNCTION_JSON_FILE_SUFFIX);
}

string ConfigAdapter::getSqlLogicTableConfigFileName(const string &clusterName) const {
    return string(SQL_CONFIG_PATH) + "/" + clusterName
        + string(SQL_LOGICTABLE_JSON_FILE_SUFFIX);
}

string ConfigAdapter::getSqlLayerTableConfigFileName(const string &clusterName) const {
    return string(SQL_CONFIG_PATH) + "/" + clusterName
        + string(SQL_LAYER_TABLE_JSON_FILE_SUFFIX);
}

string ConfigAdapter::getSqlRemoteTableConfigFileName(const string &clusterName) const {
    return string(SQL_CONFIG_PATH) + "/" + clusterName
        + string(SQL_REMOTE_TABLE_JSON_FILE_SUFFIX);
}

bool ConfigAdapter::getSqlFunctionConfigString(const string &clusterName, string &configStr) const
{
    string configFileName = getSqlFunctionConfigFileName(clusterName);
    if (!readConfigFile(configStr, configFileName)) {
        AUTIL_LOG(INFO, "try to sql funcition config [%s] failed. "
                  "use default function config file.", configFileName.c_str());
        return false;
    }
    return true;
}

bool ConfigAdapter::getSqlLogicTableConfigString(const string &clusterName,
        string &configStr) const
{
    string configFileName = getSqlLogicTableConfigFileName(clusterName);
    if (!readConfigFile(configStr, configFileName)) {
        AUTIL_LOG(INFO, "try to get sql logic table config [%s] failed.", configFileName.c_str());
    }
    return true;
}

bool ConfigAdapter::getSqlLayerTableConfigString(const string &clusterName,
        string &configStr) const
{
    string configFileName = getSqlLayerTableConfigFileName(clusterName);
    if (!readConfigFile(configStr, configFileName)) {
        AUTIL_LOG(INFO, "try to get sql layer table config [%s] failed.", configFileName.c_str());
    }
    return true;
}

bool ConfigAdapter::getSqlRemoteTableConfigString(const string &clusterName,
        string &configStr) const
{
    string configFileName = getSqlRemoteTableConfigFileName(clusterName);
    if (!readConfigFile(configStr, configFileName)) {
        AUTIL_LOG(INFO, "try to get sql remote table config [%s] failed.", configFileName.c_str());
    }
    return true;
}

} // namespace config
} // namespace isearch
