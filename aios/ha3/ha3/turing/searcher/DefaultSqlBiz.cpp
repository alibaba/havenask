#include <ha3/turing/searcher/DefaultSqlBiz.h>
#include <ha3/config/TypeDefine.h>
#include <ha3/common/CommonDef.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <ha3/turing/searcher/SearcherBiz.h>
#include <ha3/util/RangeUtil.h>
#include <autil/legacy/json.h>
#include <suez/common/PathDefine.h>
#include <suez/turing/common/CavaConfig.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <build_service/config/ResourceReader.h>
#include <ha3/sql/ops/tvf/TvfLocalSearchResource.h>
#include <ha3/sql/ops/tvf/TvfSummaryResource.h>

using namespace std;
using namespace suez;
using namespace tensorflow;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(sql);
USE_HA3_NAMESPACE(summary);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, DefaultSqlBiz);

static const std::string DEFAULT_SQL_BIZ_JSON = "default_sql_biz.json";
DefaultSqlBiz::DefaultSqlBiz() {
}

DefaultSqlBiz::~DefaultSqlBiz() {
    _naviPtr.reset();
}

tensorflow::Status DefaultSqlBiz::init(const std::string &bizName,
                                       const suez::BizMeta &bizMeta,
                                       const suez::turing::ProcessResource &processResource)
{
    _naviPtr.reset(new navi::Navi());
    if (!_naviPtr->init()) {
        return errors::Internal("navi init failed");
    }
    return Biz::init(bizName, bizMeta, processResource);
}

tensorflow::Status DefaultSqlBiz::preloadBiz() {
    _configAdapter.reset(new config::ConfigAdapter(_bizMeta.getLocalConfigPath()));
    _sqlConfigPtr.reset(new SqlConfig());
    if (!_configAdapter->getSqlConfig(*_sqlConfigPtr)) {
        return errors::Internal("parse sql config failed.");
    }
    HA3_LOG(INFO, "sql config : %s", autil::legacy::ToJsonString(_sqlConfigPtr).c_str());
    return Status::OK();
}

SessionResourcePtr DefaultSqlBiz::createSessionResource(uint32_t count) {
    SessionResourcePtr sessionResource = Biz::createSessionResource(count);
    _sqlBizResourcePtr.reset(
            new HA3_NS(sql)::SqlBizResource(sessionResource));
    _naviPtr->setResource(_sqlBizResourcePtr->getResourceName(), _sqlBizResourcePtr.get());
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
    proto::Range range;

    if (!getRange(_serviceInfo.getPartCount(), _serviceInfo.getPartId(), range)) {
        HA3_LOG(ERROR, "get range failed, total part count [%d], part id [%d]",
                _serviceInfo.getPartCount(), _serviceInfo.getPartId());
        return SessionResourcePtr();
    }

    sqlResource->range = range;
    sqlResource->naviPtr = _naviPtr;
    string configPath = _bizMeta.getLocalConfigPath();
    build_service::config::ResourceReaderPtr resourceReader(
            new build_service::config::ResourceReader(configPath));
    sqlResource->analyzerFactory = build_service::analyzer::AnalyzerFactory::create(resourceReader);
    const string &zoneName = _serviceInfo.getZoneName();
    string zoneBizName = zoneName + ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;
    ClusterConfigInfo bizClusterInfo;
    _configAdapter->getClusterConfigInfo(zoneBizName, bizClusterInfo);
    sqlResource->queryInfo = bizClusterInfo.getQueryInfo();
    sqlResource->aggFuncManager = _aggFuncManager;
    sqlResource->tvfFuncManager = _tvfFuncManager;
    sessionResource->sharedObjectMap.setWithDelete(SqlSessionResource::name(),
            sqlResource, suez::turing::DeleteLevel::Delete);

    auto resourceContainer = _tvfFuncManager->getTvfResourceContainer();
    TvfSummaryResource *tvfSummaryResource = perpareTvfSummaryResource();
    if (tvfSummaryResource == nullptr) {
        SQL_LOG(ERROR, "perpare tvf summary resource failed");
        return SessionResourcePtr();
    }
    resourceContainer->put(tvfSummaryResource);
    return sessionResource;
}

TvfSummaryResource *DefaultSqlBiz::perpareTvfSummaryResource() {
    suez::turing::TableInfoPtr tableInfoPtr = getTableInfo();
    if (!tableInfoPtr) {
        SQL_LOG(ERROR, "get table info failed");
        return nullptr;
    }
    HitSummarySchemaPtr hitSummarySchemaPtr(new HitSummarySchema(tableInfoPtr.get()));
    assert(hitSummarySchemaPtr);
    SummaryProfileManagerPtr summaryProfileMgrPtr;
    const string &zoneName = _serviceInfo.getZoneName();
    std::string clusterName = getConfigZoneBizName(zoneName);
    if (!createSummaryConfigMgr(summaryProfileMgrPtr, hitSummarySchemaPtr,
                                clusterName, tableInfoPtr))
    {
        SQL_LOG(ERROR, "create summary config manager failed");
        return nullptr;
    }
    return new TvfSummaryResource(summaryProfileMgrPtr);
}

bool DefaultSqlBiz::createSummaryConfigMgr(
        SummaryProfileManagerPtr &summaryProfileMgrPtr,
        HitSummarySchemaPtr &hitSummarySchemaPtr,
        const string& clusterName,
        const suez::turing::TableInfoPtr &tableInfoPtr)
{
    SummaryProfileConfig summaryProfileConfig;
    if (!_configAdapter->getSummaryProfileConfig(clusterName, summaryProfileConfig)) {
        SQL_LOG(WARN, "get summary profile config failed. cluster_name[%s]",
                clusterName.c_str());
        return false;
    }
    _resourceReaderPtr.reset(new ResourceReader(
                    _configAdapter->getConfigPath()));
    SummaryProfileManagerCreator summaryCreator(_resourceReaderPtr,
            hitSummarySchemaPtr, _cavaPluginManager, getBizMetricsReporter());
    summaryProfileMgrPtr = summaryCreator.create(summaryProfileConfig);
    return (summaryProfileMgrPtr != NULL);
}

string DefaultSqlBiz::getBizInfoFile() {
    const string &zoneName = _serviceInfo.getZoneName();
    return string(CLUSTER_CONFIG_DIR_NAME)  + "/" + zoneName + "/sql/" + DEFAULT_SQL_BIZ_JSON;
}

bool DefaultSqlBiz::getDefaultBizJson(string &defaultBizJson) {
    const string &zoneName = _serviceInfo.getZoneName();
    string zoneBizName = zoneName + ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;

    string binaryPath = sap::EnvironUtil::getEnv("binaryPath", "");
    if (binaryPath.empty()) {
        string workDir = suez::PathDefine::getCurrentPath();
        binaryPath = string(workDir) + "/../binary";
    }
    JsonMap jsonMap;
    string sqlDefPath = binaryPath + "/usr/local/etc/ha3/sql/"+ "default_sql.def";
    jsonMap["graph_config_path"] = Any(sqlDefPath);
    jsonMap["biz_name"] = Any(zoneName);
    jsonMap["ops_config_path"] = Any(string(""));
    jsonMap["arpc_timeout"] = Any(1000);
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

    JsonMap runOptionsConfig;
    string useInterPool = suez::turing::WorkerParam::getEnv("interOpThreadPool");
    if (useInterPool.empty()) {
        runOptionsConfig["interOpThreadPool"] = Any(-1); // run single thread
    } else {
        runOptionsConfig["interOpThreadPool"] = Any(StringUtil::fromString<int>(useInterPool));
    }
    jsonMap["run_options"] = runOptionsConfig;

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

std::string DefaultSqlBiz::convertFunctionConf() {
    const string &zoneName = _serviceInfo.getZoneName();
    string zoneBizName = getConfigZoneBizName(zoneName);
    suez::turing::FuncConfig funcConfig;
    _configAdapter->getFuncConfig(zoneBizName, funcConfig);
    auto content = ToJsonString(funcConfig);
    string configPath = _configAdapter->getConfigPath();
    string funcConfPath = fslib::fs::FileSystem::joinFilePath(configPath, "sql/_func_conf_.json");
    auto ret = fslib::fs::FileSystem::writeFile(funcConfPath, content);
    if (ret != fslib::EC_OK) {
        HA3_LOG(ERROR, "write funcConf [%s] failed, ErrorCode [%d]", funcConfPath.c_str(), ret);
        return "";
    }
    return "sql/_func_conf_.json";
}

tensorflow::Status DefaultSqlBiz::loadUserPlugins() {
    if (!initSqlAggFuncManager(_sqlConfigPtr->sqlAggPluginConfig)) {
        return errors::Internal("init sql agg plugin Manager failed");
    }
    if (!initSqlTvfFuncManager(_sqlConfigPtr->sqlTvfPluginConfig)) {
        return errors::Internal("init sql tvf plugin Manager failed");
    }
    return Status::OK();
}

bool DefaultSqlBiz::initSqlAggFuncManager(const SqlAggPluginConfig &sqlAggPluginConfig) {
    _aggFuncManager.reset(new HA3_NS(sql)::AggFuncManager());
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

bool DefaultSqlBiz::initSqlTvfFuncManager(const SqlTvfPluginConfig &sqlTvfPluginConfig) {
    _tvfFuncManager.reset(new sql::TvfFuncManager());
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

bool DefaultSqlBiz::getRange(uint32_t partCount, uint32_t partId,
                             proto::Range &range)
{
    util::PartitionRange partRange;
    if (!RangeUtil::getRange(partCount, partId, partRange)) {
        return false;
    }
    range.set_from(partRange.first);
    range.set_to(partRange.second);
    return true;
}

std::string DefaultSqlBiz::getConfigZoneBizName(const std::string &zoneName) {
    return zoneName + ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;
}

END_HA3_NAMESPACE(turing);
