#include <ha3/turing/qrs/QrsSqlBiz.h>
#include <ha3/qrs/QrsChainManager.h>
#include <ha3/common/CompressTypeConvertor.h>
#include <ha3/turing/ops/QrsSessionResource.h>
#include <ha3/turing/ops/QrsQueryResource.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/tvf/TvfLocalSearchResource.h>
#include <ha3/schema/IndexPartitionSchemaConverter.h>
#include <autil/legacy/json.h>
#include <autil/StringUtil.h>
#include <autil/HashAlgorithm.h>
#include <suez/common/PathDefine.h>
#include <libgen.h>
#include <iquan_jni/api/IquanEnv.h>
#include <iquan_common/catalog/json/Ha3TableDef.h>

using namespace std;
using namespace suez;
using namespace suez::turing;
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
USE_HA3_NAMESPACE(sql);
IE_NAMESPACE_USE(config);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, QrsSqlBiz);

static const std::string SPECIAL_CATALOG_LIST = "specialCatalogList";

QrsSqlBiz::QrsSqlBiz()
    : _tableVersion(0)
    , _functionVersion(0)
{
    _defaultCatalogName = SQL_DEFAULT_CATALOG_NAME;

}

QrsSqlBiz::~QrsSqlBiz() {
}

Status QrsSqlBiz::init(const std::string &bizName,
                       const suez::BizMeta &bizMeta,
                       const suez::turing::ProcessResource &processResource)
{
    _naviPtr.reset(new navi::Navi());
    if (!_naviPtr->init()) {
        return errors::Internal("navi init failed");
    }
    TF_RETURN_IF_ERROR(Biz::init(bizName, bizMeta, processResource));
    TF_RETURN_IF_ERROR(initSqlClient());
    return Status::OK();
}

Ha3BizMetricsPtr QrsSqlBiz::getBizMetrics() const {
    return _bizMetrics;
}

SessionResourcePtr QrsSqlBiz::createSessionResource(uint32_t count) {
    SessionResourcePtr sessionResource(new QrsSessionResource(count));
    _sqlBizResource.reset(new SqlBizResource(sessionResource));
    _naviPtr->setResource(_sqlBizResource->getResourceName(), _sqlBizResource.get());
    const map<string, navi::Resource*> &naviRes = _naviPtr->getResourceMap();
    auto iter = naviRes.find(navi::RESOURCE_MEMORY_POOL_URI);
    if (iter != naviRes.end()) {
        navi::MemoryPoolResource *poolRes = dynamic_cast<navi::MemoryPoolResource *>(iter->second);
        if (poolRes) {
            HA3_LOG(INFO, "navi pool info: chunk size [%ld], trim size [%ld], max count[%ld]",
                    poolRes->getPoolChunkSize(), poolRes->getPoolReleaseThreshold(),
                    poolRes->getPoolMaxCount());
        }
    }
    SqlSessionResource *sqlResource = new SqlSessionResource();
    sqlResource->aggFuncManager = _aggFuncManager;
    sqlResource->tvfFuncManager = _tvfFuncManager;
    sessionResource->sharedObjectMap.setWithDelete(SqlSessionResource::name(),
            sqlResource, suez::turing::DeleteLevel::Delete);
    return sessionResource;
}

QueryResourcePtr QrsSqlBiz::createQueryResource() {
    QueryResourcePtr queryResourcePtr(new QrsQueryResource);
    return queryResourcePtr;
}

SqlBizResourcePtr QrsSqlBiz::getSqlBizResource() {
    return _sqlBizResource;
}

SqlQueryResourcePtr QrsSqlBiz::createSqlQueryResource(const kmonitor::MetricsTags &tags,
        const string &prefix, autil::mem_pool::Pool *pool)
{
    QueryResourcePtr queryResource = prepareQueryResource(pool);
    queryResource->setQueryMetricsReporter(getQueryMetricsReporter(tags, prefix));
    SqlQueryResourcePtr sqlQueryResource(new SqlQueryResource(queryResource)); // hold query resource
    return sqlQueryResource;
}

string QrsSqlBiz::getBizInfoFile() {
    return BIZ_JSON_FILE;
}

Status QrsSqlBiz::preloadBiz() {
    _configAdapter.reset(new ConfigAdapter(_bizMeta.getLocalConfigPath()));
    _sqlConfigPtr.reset(new SqlConfig());
    if (!_configAdapter->getSqlConfig(*_sqlConfigPtr)) {
        return errors::Internal("parse sql config failed.");
    }
    HA3_LOG(INFO, "sql config : %s", autil::legacy::ToJsonString(_sqlConfigPtr).c_str());
    return Status::OK();
}

bool QrsSqlBiz::getDefaultBizJson(std::string &defaultBizJson) {
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

    defaultBizJson = ToJsonString(jsonMap);
    return true;
}

bool QrsSqlBiz::updateFlowConfig(const std::string &zoneBizName) {
    if (!_searchService || !_configAdapter) {
        return false;
    }
    vector<string> searcherBizNames;
    _configAdapter->getClusterNames(searcherBizNames);

    for (size_t i = 0; i < searcherBizNames.size(); ++i) {
        SqlAnomalyProcessConfig flowConf;
        if (!_configAdapter->getSqlAnomalyProcessConfig(searcherBizNames[i], flowConf))
        {
            HA3_LOG(WARN, "getSqlAnomalyProcessConfig failed, zoneBizName[%s]",
                    searcherBizNames[i].c_str());
            continue;
        }
        string searcherSqlBizName;
        size_t pos  = searcherBizNames[i].find(ZONE_BIZ_NAME_SPLITTER);
        if (pos != string::npos) {
            searcherSqlBizName = searcherBizNames[i].substr(0, pos) +
                                 ZONE_BIZ_NAME_SPLITTER + HA3_DEFAULT_SQL;
        } else {
            HA3_LOG(ERROR, "get default sql biz from [%s] failed",
                    searcherBizNames[i].c_str());
            return false;
        }
        if (!_searchService->updateFlowConfig(searcherSqlBizName, &flowConf)) {
            HA3_LOG(ERROR, "updateFlowConfig failed, zoneBizName [%s], flowConf [%s]",
                    searcherSqlBizName.c_str(), ToJsonString(flowConf).c_str());
            return false;
        }
    }
    return true;
}

Status QrsSqlBiz::initSqlClient() {
    _sqlClientPtr.reset(new iquan::Iquan());

    iquan::Status status = _sqlClientPtr->init(_sqlConfigPtr->jniConfig, _sqlConfigPtr->clientConfig);
    if (!status.ok()) {
        HA3_LOG(ERROR, "init sql client failed, error msg: %s",
                status.errorMessage().c_str());
        return errors::Internal("init sql client failed, error msg: ",
                                status.errorMessage());
    }

    std::vector<iquan::Ha3TableModel> ha3TableModels;
    iquan::TableModels tableModels;
    vector<string> searcherBizNames;
    if (!_configAdapter->getClusterNames(searcherBizNames)) {
        HA3_LOG(ERROR, "get biz names failed");
        return errors::Internal("get biz names failed");
    }

    // 1. update table
    TF_RETURN_IF_ERROR(fillHa3TableModels(searcherBizNames, ha3TableModels));
    status = _sqlClientPtr->updateHa3Tables(ha3TableModels);
    if (!status.ok()) {
        HA3_LOG(ERROR, "update table info failed, error msg: %s",
                status.errorMessage().c_str());
        return errors::Internal("update table info failed, error msg: ",
                                status.errorMessage());
    }

    TF_RETURN_IF_ERROR(fillTableModels(searcherBizNames, tableModels));
    status = _sqlClientPtr->updateTables(tableModels);
    if (!status.ok()) {
        HA3_LOG(ERROR, "update logic table info failed, error msg: %s",
                status.errorMessage().c_str());
        return errors::Internal("update logic table info failed, error msg: ",
                                status.errorMessage());
    }

    // 2. update function
    iquan::FunctionModels functionModels;
    iquan::TvfModels tvfModels;
    TF_RETURN_IF_ERROR(fillFunctionModels(searcherBizNames, functionModels, tvfModels));
    HA3_LOG(DEBUG, "function models json [%s]", ToJsonString(functionModels).c_str());
    HA3_LOG(DEBUG, "tvf models json [%s]", ToJsonString(tvfModels).c_str());

    status = _sqlClientPtr->updateFunctions(functionModels);
    if (!status.ok()) {
        std::string errorMsg = "update function info failed, error msg: " +
                               status.errorMessage();
        HA3_LOG(ERROR, "%s", errorMsg.c_str());
        return errors::Internal(errorMsg);
    }
    status = _sqlClientPtr->updateFunctions(tvfModels);
    if (!status.ok()) {
        std::string errorMsg = "update tvf info failed, error msg: " +
                               status.errorMessage();
        HA3_LOG(ERROR, "%s", errorMsg.c_str());
        return errors::Internal(errorMsg);
    }

    // 3. dump catalog
    std::string result;
    status = dumpCatalog(result);
    if (!status.ok()) {
        std::string errorMsg = "register sql client catalog failed: " +
                               status.errorMessage();
        HA3_LOG(ERROR, "%s", errorMsg.c_str());
        return errors::Internal(errorMsg);
    }
    HA3_LOG(INFO, "sql client catalog: [%s]", result.c_str());

    // 4. warmup sql client
    const string &disableSqlWarmup = WorkerParam::getEnv("disableSqlWarmup", "");
    if (disableSqlWarmup.empty() || !autil::StringUtil::fromString<bool>(disableSqlWarmup)) {
        HA3_LOG(INFO, "begin warmup iquan.");
        if (_sqlConfigPtr->warmupConfig.warmUpFilePath.empty()) {
            status = iquan::IquanEnv::warmup(_sqlConfigPtr->warmupConfig);
        } else {
            string path = _bizMeta.getLocalConfigPath() + _sqlConfigPtr->warmupConfig.warmUpFilePath;
            _sqlConfigPtr->warmupConfig.warmUpFilePath = path;
            HA3_LOG(INFO, "warmup with sql file [%s].", path.c_str());
            status = _sqlClientPtr->warmup(_sqlConfigPtr->warmupConfig);
        }
        if (!status.ok()) {
            HA3_LOG(ERROR, "failed to warmup iquan, %s", status.errorMessage().c_str());
        }
        HA3_LOG(INFO, "end warmup iquan.");
    }

    // 5. try to start kmon
    HA3_LOG(INFO, "begin iquan kmon.");
    status = iquan::IquanEnv::startKmon(_sqlConfigPtr->kmonConfig);
    if (!status.ok()) {
        HA3_LOG(WARN, "failed to start kmon iquan, %s", status.errorMessage().c_str());
    }
    HA3_LOG(INFO, "end iquan kmon.");

    return Status::OK();
}

Status QrsSqlBiz::fillHa3TableModels(const vector<string> &searcherBizNames,
                                     vector<iquan::Ha3TableModel> &ha3TableModels)
{
    std::string blackListStr = sap::EnvironUtil::getEnv("sqlTableBlacklist", "");
    vector<std::string> blackList = autil::StringUtil::split(blackListStr, ",");
    for (auto &table : blackList) {
        autil::StringUtil::trim(table);
    }
    for (const auto &bizName: searcherBizNames) {
        vector<string> dependTables;
        string dbName;
        auto status = getDependTables(bizName, dependTables, dbName);
        if (_defaultDatabaseName.empty()) {
            _defaultDatabaseName = dbName;
        }
        if (!status.ok()) {
            return status;
        }
        for (const auto &table : dependTables) {
            if (std::find(blackList.begin(), blackList.end(), table)
                != blackList.end())
            {
                HA3_LOG(INFO, "sql biz [%s] table [%s] filter by blackList",
                        dbName.c_str(), table.c_str());
                continue;
            }

            IndexPartitionSchemaPtr schemaPtr;
            if (!_configAdapter->getIndexPartitionSchemaByTable(table, schemaPtr)) {
                string errorMsg = "get biz[" + bizName + "] table[" + table + "] indexPartitionSchema failed";
                HA3_LOG(ERROR, "%s", errorMsg.c_str());
                return errors::Internal(errorMsg);
            }

            iquan::Ha3TableModel ha3TableModel;
            TF_RETURN_IF_ERROR(readClusterFile(table, ha3TableModel));

            // iquanHa3Table
            iquan::Ha3TableDef ha3TableDef;
            schema::IndexPartitionSchemaConverter::convertToIquanHa3Table(schemaPtr, ha3TableDef);
            ha3TableModel.ha3TableDefContent = ToJsonString(ha3TableDef);

            ha3TableModel.catalogName = SQL_DEFAULT_CATALOG_NAME;
            ha3TableModel.databaseName = dbName;
            ha3TableModel.tableType = "default_type";
            ha3TableModel.tableContentVersion = "json_default_0.1";
            ha3TableModel.tableVersion = ++_tableVersion;
            ha3TableModels.emplace_back(ha3TableModel);
        }
    }
    return Status::OK();
}

Status QrsSqlBiz::fillTableModels(const vector<string> &searcherBizNames,
                                     iquan::TableModels &tableModels)
{
    for (const auto &bizName: searcherBizNames) {
        string dbName;
        if (isDefaultBiz(bizName)) {
            const auto &names = autil::StringUtil::split(bizName, ZONE_BIZ_NAME_SPLITTER);
            dbName = names[0];
        } else {
            dbName = bizName;
        }

        std::string schemaConfig;
        if (!_configAdapter->getSqlLogicTableConfigString(dbName, schemaConfig))
        {
            string errorMsg = "get [" + dbName + "] logic table config failed";
            HA3_LOG(ERROR, "%s", errorMsg.c_str());
            return errors::Internal(errorMsg);
        }

        if (schemaConfig.empty()) {
            continue;
        }

        try {
            iquan::TableModels config;
            FromJsonString(config, schemaConfig);
            for (const auto& tableModel : config.tables) {
                if (tableModel.databaseName != dbName || tableModel.tableContent.tableType != SQL_LOGICTABLE_TYPE) {
                    string errorMsg = "logic table content error[cluster:" + dbName +
                            "][dbName:" + tableModel.databaseName + "][table_type:"
                            + tableModel.tableContent.tableType + "]";
                    HA3_LOG(ERROR, "%s", errorMsg.c_str());
                    return errors::Internal(errorMsg);
                }
                tableModels.tables.emplace_back(tableModel);
            }
        } catch (const std::exception &e) {
            HA3_LOG(ERROR, "parse json string failed, exception [%s], content:\n%s",
                    e.what(), schemaConfig.c_str());
            return errors::Internal("parse json string failed, content\n" + schemaConfig);
        }
    }
    return Status::OK();
}

string QrsSqlBiz::getSearchBizInfoFile(const string &bizName) {
    return string(CLUSTER_CONFIG_DIR_NAME) + "/" + bizName + "/" + BIZ_JSON_FILE;
}

bool QrsSqlBiz::isDefaultBiz(const string &bizName) {
    string bizInfoFile = getSearchBizInfoFile(bizName);
    string absPath = fslib::fs::FileSystem::joinFilePath(
            _resourceReader->getConfigPath(), bizInfoFile);
    return fslib::fs::FileSystem::isExist(absPath) != fslib::EC_TRUE;
}

string QrsSqlBiz::getSearchDefaultBizJson(const string &bizName, const string &dbName) {
    JsonMap jsonMap;
    JsonArray dependencyTables;
    dependencyTables.push_back(Any(dbName));
    for (auto tableName : _configAdapter->getJoinClusters(bizName)) {
        dependencyTables.push_back(Any(tableName));
    }
    jsonMap["dependency_table"] = dependencyTables;

    HA3_NS(config)::TuringOptionsInfo turingOptionsInfo;
    if (_configAdapter->getTuringOptionsInfo(bizName, turingOptionsInfo)) {
        for (auto it : turingOptionsInfo._turingOptionsInfo) {
            if (it.first == "dependency_table") {
                jsonMap["dependency_table"] = it.second;
            }
        }
    }
    return ToJsonString(jsonMap, true);
}

Status QrsSqlBiz::getDependTables(const string &bizName,
                                  vector<string> &dependTables, string &dbName)
{
    string bizConfigStr;
    if (isDefaultBiz(bizName)) {
        const auto &names = autil::StringUtil::split(bizName, ZONE_BIZ_NAME_SPLITTER);
        dbName = names[0];
        bizConfigStr = getSearchDefaultBizJson(bizName, dbName);
    } else {
        dbName = bizName;
        string bizInfoFile = getSearchBizInfoFile(bizName);
        if (!_resourceReader->getConfigContent(bizInfoFile, bizConfigStr)) {
            HA3_LOG(ERROR, "read biz.json failed");
            return errors::Internal("read biz.json failed.");
        }
    }
    if (bizConfigStr.empty()) {
        HA3_LOG(ERROR, "biz.json is empty");
        return errors::Internal("biz.json is empty.");
    }
    BizInfo bizInfo;
    try {
        FromJsonString(bizInfo, bizConfigStr);
        HA3_LOG(INFO, "load searcher biz [%s],  info [%s]", bizName.c_str(),
                ToJsonString(bizInfo).c_str());
    } catch (const std::exception &e) {
        HA3_LOG(ERROR, "parse json string [%s] failed, exception [%s]", bizConfigStr.c_str(),
                  e.what());
        return errors::Internal("parse json string [", bizConfigStr, "] failed.");
    }
    dependTables = bizInfo.getDependTables(dbName);
    return Status::OK();
}

Status QrsSqlBiz::readClusterFile(const std::string &tableName,
        iquan::Ha3TableModel &ha3TableModel)
{
    std::string cluster;
    if (!_configAdapter->getTableClusterConfigString(tableName, cluster)) {
        string errorMsg = "get [" + tableName + "] cluster config failed";
        HA3_LOG(ERROR, "%s", errorMsg.c_str());
        return errors::Internal(errorMsg);
    }
    ha3TableModel.ha3ClusterDefContent = std::move(cluster);
    return Status::OK();
}

Status QrsSqlBiz::fillFunctionModels(const vector<string> &searcherBizNames,
                                     iquan::FunctionModels &functionModels,
                                     iquan::TvfModels &tvfModels)
{
    for (const auto &bizName: searcherBizNames) {
        string dbName;
        if (isDefaultBiz(bizName)) {
            const auto &names = autil::StringUtil::split(bizName, ZONE_BIZ_NAME_SPLITTER);
            dbName = names[0];
        } else {
            dbName = bizName;
        }

        std::string udfConfig;
        if (!_configAdapter->getSqlFunctionConfigString(getBinaryPath(), dbName, udfConfig))
        {
            string errorMsg = "get [" + dbName + "] udf sql function config failed";
            HA3_LOG(ERROR, "%s", errorMsg.c_str());
            return errors::Internal(errorMsg);
        }

        iquan::FunctionModels udfFunctionModels;
        try {
            FromJsonString(udfFunctionModels, udfConfig);
        } catch (const std::exception &e) {
            HA3_LOG(ERROR, "parse json string failed, exception [%s], content:\n%s",
                    e.what(), udfConfig.c_str());
            return errors::Internal("parse json string failed, content\n" + udfConfig);
        }

        _functionVersion++;
        string specialCatalogsStr = sap::EnvironUtil::getEnv(SPECIAL_CATALOG_LIST, "");
        const vector<string> &specialCatalogs =
            autil::StringUtil::split(specialCatalogsStr, ",");
        addUserFunctionModels<iquan::FunctionModels>(
                udfFunctionModels, specialCatalogs, dbName, functionModels);
        if (_aggFuncManager) {
            addUserFunctionModels<iquan::FunctionModels>(
                    _aggFuncManager->getFunctionModels(), specialCatalogs,
                    dbName, functionModels);
        }
        if (_tvfFuncManager) {
            iquan::TvfModels inputTvfModels;
            if (!_tvfFuncManager->regTvfModels(inputTvfModels)) {
                return errors::Internal("reg tvf models failed");
            }
            addUserFunctionModels<iquan::TvfModels>(
                    inputTvfModels, specialCatalogs, dbName, tvfModels);
        }
    }
    return Status::OK();
}


tensorflow::Status QrsSqlBiz::loadUserPlugins() {
    if (!initSqlAggFuncManager(_sqlConfigPtr->sqlAggPluginConfig)) {
        return errors::Internal("init sql agg plugin Manager failed");
    }
    if (!initSqlTvfFuncManager(_sqlConfigPtr->sqlTvfPluginConfig)) {
        return errors::Internal("init sql tvf plugin Manager failed");
    }

    return Status::OK();
}

bool QrsSqlBiz::initSqlAggFuncManager(const SqlAggPluginConfig &sqlAggPluginConfig) {
    _aggFuncManager.reset(new AggFuncManager());
    auto configPath = _resourceReader->getConfigPath();
    if (!_aggFuncManager->init(configPath, sqlAggPluginConfig)) {
        HA3_LOG(ERROR, "init sql agg plugin Manager failed");
        return false;
    }
    if (!_aggFuncManager->registerFunctionInfos()) {
        HA3_LOG(ERROR, "register agg function infos from plugin failed");
        return false;
    }
    return true;
}

bool QrsSqlBiz::initSqlTvfFuncManager(const SqlTvfPluginConfig &sqlTvfPluginConfig) {
    _tvfFuncManager.reset(new TvfFuncManager());
    auto configPath = _resourceReader->getConfigPath();
    if (!_tvfFuncManager->init(configPath, sqlTvfPluginConfig)) {
        HA3_LOG(ERROR, "init sql tvf plugin Manager failed");
        return false;
    }
    auto resourceContainer = _tvfFuncManager->getTvfResourceContainer();
    resourceContainer->put(new TvfLocalSearchResource(
                    _localSearchService));
    return true;
}


END_HA3_NAMESPACE(turing);
