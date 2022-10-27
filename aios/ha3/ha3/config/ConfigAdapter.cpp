#include <string>
#include <ha3/config/ConfigAdapter.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/config/ClusterConfigParser.h>
#include <ha3/common/CommonDef.h>
#include <ha3/config/ClusterTableInfoValidator.h>
#include <suez/turing/common/FileUtil.h>
#include <indexlib/config/schema_configurator.h>
#include <autil/legacy/json.h>
#include <autil/legacy/exception.h>
#include <autil/StringUtil.h>
#include <fslib/fslib.h>
#include <signal.h>
#include <sys/wait.h>
#include <suez/turing/common/CavaConfig.h>
#include <sap/util/environ_util.h>
#include <ha3/util/EnvParser.h>
#include <suez/common/PathDefine.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace fslib::fs;
using namespace suez::turing;
USE_HA3_NAMESPACE(util);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

USE_WORKER_FRAMEWORK_NAMESPACE(data_client);
BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, ConfigAdapter);

map<string, bool> ConfigAdapter::_canBeEmptyTable;

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

ErrorCode ConfigAdapter::WFToHa3ErrorCode(DataClientWrapper::WFErrorCode wfCode) {
    switch (wfCode) {
    case DataClientWrapper::ERROR_NONE:
        return ERROR_NONE;
    case DataClientWrapper::ERROR_DOWNLOAD:
        return ERROR_CONFIG_INIT;
    case DataClientWrapper::ERROR_FILEIO:
    case DataClientWrapper::ERROR_DEST:
        return ERROR_CONFIG_INIT_FILEIO_ERROR;
    default:
        assert(false);
    }
    HA3_LOG(ERROR, "unknown error code[%d]", int(wfCode));
    return ERROR_CONFIG_INIT;
}

bool ConfigAdapter::convertParseResult(
        ParseResult ret, const string &sectionName) const
{
    if (ret == ClusterConfigParser::PR_OK) {
        return true;
    } else if (ret == ClusterConfigParser::PR_FAIL) {
        return false;
    } else {
        return _canBeEmptyTable[sectionName];
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
    if (!FileUtil::readFileContent(bizFile, content)) {
        HA3_LOG(ERROR, "read file %s failed", bizFile.c_str());
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
        HA3_LOG(ERROR, "List dir[%s] failed: ErrorCode[%d].", path.c_str(), errorCode);
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
                size_t suffixLen = CLUSTER_JSON_FILE_SUFFIX.size();
                if (StringUtil::endsWith(bizFileName, CLUSTER_JSON_FILE_SUFFIX)
                    && bizFileName.length() > suffixLen)
                {
                    string bizName = bizFileName.substr(0, bizFileName.length() - suffixLen);
                    clusterNames.push_back(zoneName + ZONE_BIZ_NAME_SPLITTER + bizName);
                }
                // ignore other file
            }
        } else if (errorCode != fslib::EC_FALSE) {
            HA3_LOG(ERROR, "Failed to check [%s] isDirectory. errorcode: [%d]",
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
                    if (!FileUtil::readFileContent(bizFile, content)) {
                        HA3_LOG(ERROR, "read file %s failed", bizFile.c_str());
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
            HA3_LOG(ERROR, "Failed to check [%s] isDirectory. errorcode: [%d]",
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
        HA3_LOG(ERROR, "getClusterNames error");
        return _clusterConfigMapPtr;
    }
    const string &defaultAggStr = getDefaultAgg();
    const string &paraWaysStr = getParaWays();
    HA3_LOG(DEBUG, "clusterNames size:%zd", clusterNames.size());
    for (vector<string>::const_iterator it = clusterNames.begin();
         it != clusterNames.end(); ++it)
    {
        const string &clusterName = *it;
        ClusterConfigInfo clusterConfig;
        if (!getClusterConfigInfo(clusterName, clusterConfig)) {
            HA3_LOG(ERROR, "getClusterConfigInfo error, clusterName:%s",
                    clusterName.c_str());
            return _clusterConfigMapPtr;
        }
        //create hash func
        if (!clusterConfig.initHashFunc()) {
            HA3_LOG(ERROR, "create hash func error, clusterName:%s",
                    clusterName.c_str());
            return _clusterConfigMapPtr;
        }
        (*tmpClusterConfigMapPtr)[clusterName] = clusterConfig;
        size_t pos  = clusterName.find(ZONE_BIZ_NAME_SPLITTER);
        if (pos != string::npos) {
            if (!defaultAggStr.empty()) {
                string defaultAggBizName = clusterName.substr(0, pos) +
                                           ZONE_BIZ_NAME_SPLITTER + defaultAggStr;
                (*tmpClusterConfigMapPtr)[defaultAggBizName] = clusterConfig;
            }
            vector<string> paraWaysVec;
            if(EnvParser::parseParaWays(paraWaysStr, paraWaysVec)) {
                for (const auto &paraWay: paraWaysVec) {
                    string paraBizStr = HA3_PARA_SEARCH_PREFIX + paraWay;
                    string paraSearchBizName = clusterName.substr(0, pos) +
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
        HA3_LOG(WARN, "parse version config fail, config str [%s], exception[%s]",
                versionConfigStr.c_str(), e.what());
        return VersionConfig();
    }
    return config;
}

bool ConfigAdapter::getQrsConfig(QrsConfig &qrsConfig) const {
    string qrsConfigStr;
    string fileName = QRS_CONFIG_JSON_FILE;
    if (!readConfigFile(qrsConfigStr, fileName)) {
        HA3_LOG(ERROR, "Get qrs config file fail: [%s]", QRS_CONFIG_JSON_FILE.c_str());
        return false;
    }
    try {
        FromJsonString(qrsConfig, qrsConfigStr);
    } catch (const ExceptionBase &e) {
        HA3_LOG(ERROR, "QrsConfig from json fail, exception[%s]", e.what());
        return false;
    }

    return true;
}

bool ConfigAdapter::getSqlConfig(SqlConfig &sqlConfig) const {
    string sqlConfigStr;
    string fileName = SQL_CONFIG_JSON_FILE;
    if (!readConfigFile(sqlConfigStr, fileName)) {
        HA3_LOG(WARN, "Get sql config file fail: [%s %s]", _configPath.c_str(),
                SQL_CONFIG_JSON_FILE.c_str());
    } else {
        try {
            FromJsonString(sqlConfig, sqlConfigStr);
        } catch (const ExceptionBase &e) {
            HA3_LOG(ERROR, "SqlConfig from json fail, exception[%s]", e.what());
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
        HA3_LOG(ERROR, "get cluster table info manager failed!");
        return _clusterTableInfoMapPtr; // return NULL
    }
    _clusterTableInfoMapPtr.reset(new ClusterTableInfoMap);
    ClusterTableInfoManagerMap::const_iterator iter = clusterTableInfoManagerMapPtr->begin();
    for (; iter != clusterTableInfoManagerMapPtr->end(); iter++) {
        TableInfoPtr tableInfoPtr = iter->second->getClusterTableInfo();
        assert(tableInfoPtr);
        string clusterName = iter->first;
        (*_clusterTableInfoMapPtr)[clusterName] = tableInfoPtr;
        size_t pos  = clusterName.find(ZONE_BIZ_NAME_SPLITTER);
        if (pos != string::npos) {
            if (!defaultAggStr.empty()) {
                string defaultAggBizName = clusterName.substr(0, pos) +
                                           ZONE_BIZ_NAME_SPLITTER + defaultAggStr;
                (*_clusterTableInfoMapPtr)[defaultAggBizName] = tableInfoPtr;
            }
            vector<string> paraWaysVec;
            if(EnvParser::parseParaWays(paraWaysStr, paraWaysVec)) {
                for (const auto &paraWay: paraWaysVec) {
                    string paraBizStr = HA3_PARA_SEARCH_PREFIX + paraWay;
                    string paraSearchBizName = clusterName.substr(0, pos) +
                                               ZONE_BIZ_NAME_SPLITTER +
                                               paraBizStr;
                    (*_clusterTableInfoMapPtr)[paraSearchBizName] = tableInfoPtr;
                }
            } else {
                HA3_LOG(ERROR, "parse env paraSearchWays[%s] failed, "
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
        HA3_LOG(ERROR, "get cluster names failed.");
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
        HA3_LOG(ERROR, "validate cluster table infos failed!");
        return ClusterTableInfoManagerMapPtr();
    }
    _clusterTableInfoManagerMapPtr = tmpClusterTableInfoManagerMapPtr;
    return _clusterTableInfoManagerMapPtr;
}

vector<string> ConfigAdapter::getJoinClusters(const string &clusterName) const {
    vector<string> joinClusters;
    ClusterConfigInfo clusterConfigInfo;
    if (!getClusterConfigInfo(clusterName, clusterConfigInfo)) {
           HA3_LOG(ERROR, "get cluster config info failed for cluster [%s]!", clusterName.c_str());
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
        HA3_LOG(ERROR, "main table not found for cluster [%s]", clusterName.c_str());
        return ClusterTableInfoManagerPtr();
    }
    ClusterTableInfoManagerPtr clusterTableInfoManagerPtr(new ClusterTableInfoManager);
    clusterTableInfoManagerPtr->setMainTableInfo(mainTableInfoPtr);
    ClusterConfigInfo clusterConfigInfo;
    if (!getClusterConfigInfo(clusterName, clusterConfigInfo)) {
           HA3_LOG(ERROR, "get cluster config info failed for cluster [%s]!", clusterName.c_str());
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
            HA3_LOG(ERROR, "get aux cluster[%s] table failed", auxCluster.c_str());
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
            HA3_LOG(ERROR, "cluster[%s] not found", clusterName.c_str());
            return TableInfoPtr();
        }
        tableName = clusterConfigInfo._tableName;
    }
    TableInfoPtr tableInfoPtr;
    if (!getTableInfo(tableName, tableInfoPtr)) {
        HA3_LOG(ERROR, "get [%s]cluster table[%s] info failed.",
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
        HA3_LOG(ERROR, "Failed to get clusterConfigInfo clusterName:[%s]",
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
        HA3_LOG(ERROR, "getTableSchemaConfigString Failed! FileName[%s]",
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
        HA3_LOG(ERROR, "getTableClusterConfigString Failed! FileName[%s]",
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
        HA3_LOG(ERROR, "getTableInfo Failed! tableName[%s]",
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
        HA3_LOG(ERROR, "Get Config File Fail: [%s]", ANALYZER_CONFIG_FILE.c_str());
        return false;
    }
    return true;
}

bool ConfigAdapter::readConfigFile(string &result,
                                   const string &relativePath) const
{
    string filePath = FileUtil::joinFilePath(_configPath, relativePath);
    return FileUtil::readFileContent(filePath, result);
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
        HA3_LOG(ERROR, "failed to getClusterConfigMap");
        return false;
    }
    for (ClusterConfigMap::const_iterator it = clusterConfigMapPtr->begin();
         it != clusterConfigMapPtr->end(); it++)
    {
        const string &clusterName = it->first;
        if (StringUtil::endsWith(clusterName, HA3_DEFAULT_AGG)
            || StringUtil::endsWith(clusterName, HA3_DEFAULT_SQL))
        {
            continue;
        }
        const ClusterConfigInfo &clusterConfigInfo = it->second;
        if (!clusterConfigInfo.check()) {
            HA3_LOG(ERROR, "check ClusterConfigInfo for cluster[%s] failed",
                    clusterName.c_str());
            return false;
        }

        IndexPartitionSchemaPtr schema;
        if (!getIndexPartitionSchema(clusterName, schema)) {
            return false;
        }

        const auto &joinInfos = clusterConfigInfo._joinConfig.getJoinInfos();
        if (joinInfos.size() == 0) {
            continue;
        }

        TableInfoPtr mainTableInfo;
        vector<TableInfoPtr> joinTableInfos;
        if (!getTableInfo(clusterConfigInfo._tableName, mainTableInfo)) {
            HA3_LOG(ERROR, "getTableInfo for cluster[%s] failed",
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
                HA3_LOG(ERROR, "join cluster[%s] does not exist",
                        joinClusterName.c_str());
                return false;
            }
            const ClusterConfigInfo &joinClusterConfigInfo = it2->second;
            if (!joinClusterConfigInfo.check())
            {
                HA3_LOG(ERROR, "join cluster[%s]'s config is invalid",
                        joinClusterName.c_str());
                return false;
            }

            TableInfoPtr joinTableInfo;
            if (!getTableInfo(joinClusterConfigInfo._tableName, joinTableInfo)) {
                HA3_LOG(ERROR, "getTableInfo for join cluster[%s] failed",
                        joinClusterName.c_str());
                return false;
            }
            if (!ClusterTableInfoValidator::validateMainTableAgainstJoinTable(
		    mainTableInfo, joinTableInfo, joinInfos[i].getJoinField(), joinInfos[i].useJoinPK))
            {
                HA3_LOG(ERROR, "validate mainTable[%s] against joinTable[%s] failed",
                        clusterConfigInfo._tableName.c_str(),
                        joinClusterConfigInfo._tableName.c_str());
                return false;
            }
            joinTableInfos.push_back(joinTableInfo);
        }
        if (!ClusterTableInfoValidator::validateJoinTables(joinTableInfos)) {
            HA3_LOG(ERROR, "validateJoinTables for cluster[%s] failed",
                    clusterName.c_str());
            return false;
        }
    }
    return true;
}

bool ConfigAdapter::getIndexPartitionSchemaByTable(string table,
        IndexPartitionSchemaPtr& schema) const
{
    string schemaStr;
    if (!getTableSchemaConfigString(table, schemaStr)) {
        return false;
    }
    try {
        FromJsonString(schema, schemaStr);
    } catch(autil::legacy::ExceptionBase &e) {
        HA3_LOG(ERROR, "schema's json format error, e.what(): %s, schemaStr: %s",
                e.what(), schemaStr.c_str());
        return false;
    }
    return true;
}

bool ConfigAdapter::getIndexPartitionSchema(string clusterName,
        IndexPartitionSchemaPtr& schema) const
{
    string schemaStr;
    if (!getSchemaString(clusterName, schemaStr)) {
        HA3_LOG(ERROR, "read schema error, clusterName: %s",
                clusterName.c_str());
        return false;
    }
    try {
        FromJsonString(schema, schemaStr);
    } catch(autil::legacy::ExceptionBase &e) {
        HA3_LOG(ERROR, "schema's json format error, e.what(): %s, schemaStr: %s",
                e.what(), schemaStr.c_str());
        return false;
    }
    return true;
}

string ConfigAdapter::getDefaultAgg() {
    string defaultAggStr = sap::EnvironUtil::getEnv("defaultAgg", "");
    if (defaultAggStr.empty()) {
        defaultAggStr = HA3_DEFAULT_AGG_PREFIX + "4";
    }
    if (defaultAggStr.find(HA3_DEFAULT_AGG_PREFIX) == string::npos) {
        HA3_LOG(INFO, "%s is not support agg by default.", defaultAggStr.c_str());
        return "";
    }
    return HA3_DEFAULT_AGG;
}


string ConfigAdapter::getParaWays() {
    const string &paraWaysStr = sap::EnvironUtil::getEnv("paraSearchWays", "");
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

bool ConfigAdapter::getSqlFunctionConfigString(const string &binaryPath,
        const string &clusterName, string &configStr) const
{
    string configFileName = getSqlFunctionConfigFileName(clusterName);
    if (!readConfigFile(configStr, configFileName)) {
        HA3_LOG(INFO, "try to sql funcition config [%s] failed. read default function config file.", configFileName.c_str());
        if (!FileUtil::readFileContent(binaryPath + SQL_FUNCTION_CONF, configStr)) {
            HA3_LOG(ERROR, "get default sql funcition config Failed! FileName[%s]",
                    SQL_FUNCTION_CONF.c_str());
            return false;
        }
    }
    return true;
}

bool ConfigAdapter::getSqlLogicTableConfigString(const string &clusterName,
        string &configStr) const
{
    string configFileName = getSqlLogicTableConfigFileName(clusterName);
    if (!readConfigFile(configStr, configFileName)) {
        HA3_LOG(INFO, "try to get sql logic table config [%s] failed.", configFileName.c_str());
    }
    return true;
}

END_HA3_NAMESPACE(config);
