#include <autil/legacy/json.h>
#include <ha3/turing/searcher/SearcherBiz.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/config/ConfigAdapter.h>
#include <ha3/service/SearcherResourceCreator.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/common/ReturnInfo.h>
#include <suez/turing/common/WorkerParam.h>
#include <fslib/fs/FileSystem.h>
#include <libgen.h>
#include <suez/turing/common/CavaConfig.h>
#include <suez/common/PathDefine.h>
using namespace std;
using namespace suez;
using namespace tensorflow;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(search);
BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, SearcherBiz);

static int64_t HA3_REFRESH_SNAPSHOT_INTERVAL = 500 * 1000; //500 ms

SearcherBiz::SearcherBiz() : _lastCreateSnapshotTime(0) {
}

SearcherBiz::~SearcherBiz() {
    _clearSnapshotThread.reset();
}

tensorflow::Status SearcherBiz::init(const string &bizName, const suez::BizMeta &bizMeta,
                                     const suez::turing::ProcessResource &pr) {
    Status status = Biz::init(bizName, bizMeta, pr);
    if (status != Status::OK()) {
        return status;
    }
    _clearSnapshotThread = LoopThread::createLoopThread(
            std::tr1::bind(&SearcherBiz::clearSnapshot, this), HA3_REFRESH_SNAPSHOT_INTERVAL);
    if (_clearSnapshotThread == NULL) {
        return errors::Internal("create clear snapshot reader thread failed.");
    }
    return status;
}

SessionResourcePtr SearcherBiz::createSessionResource(uint32_t count) {
    SessionResourcePtr sessionResourcePtr;
    SearcherSessionResource *searcherSessionResource = new SearcherSessionResource(count);
    searcherSessionResource->ip = suez::turing::WorkerParam::getEnv(suez::turing::WorkerParam::IP);
    sessionResourcePtr.reset(searcherSessionResource);
    searcherSessionResource->configAdapter = _configAdapter;
    searcherSessionResource->searcherResource = createSearcherResource(_configAdapter);

    if (!searcherSessionResource->searcherResource) {
        sessionResourcePtr.reset();
    }
    return sessionResourcePtr;
}



QueryResourcePtr SearcherBiz::createQueryResource() {
    QueryResourcePtr queryResourcePtr(new SearcherQueryResource());
    return queryResourcePtr;
}

string SearcherBiz::getBizInfoFile() {
    const string &zoneName = _serviceInfo.getZoneName();
    return string(CLUSTER_CONFIG_DIR_NAME) + "/" + zoneName + "/" + BIZ_JSON_FILE;
}

tensorflow::Status SearcherBiz::preloadBiz() {
    _configAdapter.reset(new ConfigAdapter(_bizMeta.getLocalConfigPath()));
    return Status::OK();
}

std::string SearcherBiz::getDefaultGraphDefPath() {
    return getBinaryPath() + "/usr/local/etc/ha3/searcher_default.def";
}

std::string SearcherBiz::getConfigZoneBizName(const std::string &zoneName) {
    return zoneName + ZONE_BIZ_NAME_SPLITTER + _bizName;
}

std::string SearcherBiz::getOutConfName(const std::string &confName) {
    return confName;
}

std::string SearcherBiz::convertSorterConf() {
    const string &zoneName = _serviceInfo.getZoneName();
    const string &zoneBizName = getConfigZoneBizName(zoneName);
    FinalSorterConfig finalSorterConfig;
    _configAdapter->getFinalSorterConfig(zoneBizName, finalSorterConfig);
    // add buildin sorters
    build_service::plugin::ModuleInfo moduleInfo;
    finalSorterConfig.modules.push_back(moduleInfo);
    suez::turing::SorterInfo sorterInfo;
    sorterInfo.sorterName = "DEFAULT";
    sorterInfo.className = "DefaultSorter";
    finalSorterConfig.sorterInfos.push_back(sorterInfo);
    sorterInfo.sorterName = "NULL";
    sorterInfo.className = "NullSorter";
    finalSorterConfig.sorterInfos.push_back(sorterInfo);
    // dump
    auto content = ToJsonString(finalSorterConfig);
    const string &configPath = _configAdapter->getConfigPath();
    string fileName("_sorter_conf_.json");
    const string &confName = getOutConfName(fileName);
    const string &sorterConfPath = fslib::fs::FileSystem::joinFilePath(configPath,
            confName);
    if (fslib::fs::FileSystem::writeFile(sorterConfPath, content) != fslib::EC_OK) {
        return "";
    }
    return confName;
}

std::string SearcherBiz::convertFunctionConf() {
    const string &zoneName = _serviceInfo.getZoneName();
    const string &zoneBizName = getConfigZoneBizName(zoneName);
    suez::turing::FuncConfig funcConfig;
    _configAdapter->getFuncConfig(zoneBizName, funcConfig);
    auto content = ToJsonString(funcConfig);
    const string &configPath = _configAdapter->getConfigPath();
    string fileName("_func_conf_.json");
    const string &confName = getOutConfName(fileName);
    const string &funcConfPath = fslib::fs::FileSystem::joinFilePath(configPath,
            confName);
    auto ret = fslib::fs::FileSystem::writeFile(funcConfPath, content);
    if (ret != fslib::EC_OK) {
        HA3_LOG(ERROR, "write funcConf [%s] failed, ErrorCode [%d]", funcConfPath.c_str(), ret);
        return "";
    }
    return confName;
}

bool SearcherBiz::getDefaultBizJson(string &defaultBizJson) {
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
    dependencyTables.push_back(Any(zoneName));
    for (auto tableName : _configAdapter->getJoinClusters(zoneBizName)) {
        dependencyTables.push_back(Any(tableName));
    }

    std::string mainTable = zoneName;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema;
    if (_configAdapter->getIndexPartitionSchema(zoneBizName, schema)) {
        mainTable = schema->GetSchemaName();
    }
    jsonMap["item_table_name"] = Any(mainTable);
    jsonMap["dependency_table"] = dependencyTables;

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
        runOptionsConfig["interOpThreadPool"] = Any(-1); // close parallel
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
        for (auto it : turingOptionsInfo._turingOptionsInfo) {
            jsonMap[it.first] = it.second;
        }
    }

    defaultBizJson = ToJsonString(jsonMap);
    return true;
}

IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr SearcherBiz::createIndexSnapshot() {
    int64_t curTime = TimeUtility::currentTime();
    ScopedLock lock(_snapshotLock);
    if (_lastCreateSnapshotTime + HA3_REFRESH_SNAPSHOT_INTERVAL > curTime && _snapshotReader) {
        return _snapshotReader;
    } else if (_sessionResource->indexApplication) {
        _snapshotReader = _sessionResource->createIndexSnapshot();
        _lastCreateSnapshotTime = curTime;
        return _snapshotReader;
    } else {
        return IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr();
    }
}

SearcherResourcePtr SearcherBiz::createSearcherResource(const ConfigAdapterPtr &configAdapter) {
    SearcherResourcePtr searcherResource;
    if (!configAdapter) {
        HA3_LOG(ERROR, "configAdapter is null");
        return searcherResource;
    }
    const TableReader &tableReader = _indexProvider->tableReader;
    const string &zoneName = _serviceInfo.getZoneName();
    const string &zoneBizName = getConfigZoneBizName(zoneName);
    ClusterConfigInfo bizConfigInfo;
    if (!configAdapter->getClusterConfigInfo(zoneBizName, bizConfigInfo)) {
        HA3_LOG(ERROR, "Get section [cluster_config] failed, zone [%s], biz [%s]",
                zoneName.c_str(), _bizName.c_str());
        return searcherResource;
    }
    util::PartitionRange utilRange;
    if (!getRange(_serviceInfo.getPartCount(), _serviceInfo.getPartId(), utilRange)) {
        HA3_LOG(ERROR, "getRange failed");
        return searcherResource;
    }
    Range range;
    range.set_from(utilRange.first);
    range.set_to(utilRange.second);
    pair<PartitionID, IE_NAMESPACE(partition)::IndexPartitionPtr> mainTable;
    if (!getIndexPartition(bizConfigInfo._tableName, range, tableReader, mainTable)) {
        HA3_LOG(ERROR, "can't find any partition for table [%s], zone [%s], biz [%s]",
                bizConfigInfo._tableName.c_str(), zoneName.c_str(), _bizName.c_str());
        return searcherResource;
    }
    Cluster2IndexPartitionMapPtr cluster2IndexPartitionMap =
        createCluster2IndexPatitionMap(configAdapter, zoneBizName, tableReader);
    HA3_NS(search)::IndexPartitionWrapperPtr indexPartWrapper =
        IndexPartitionWrapper::createIndexPartitionWrapper(
                configAdapter, mainTable.second, zoneBizName);
    if (!indexPartWrapper) {
        HA3_LOG(ERROR, "createIndexPartitionWrapper failed");
        return searcherResource;
    }
    SearcherResourceCreator searcherResourceCreator(configAdapter, getBizMetricsReporter(), this);
    ReturnInfo ret = searcherResourceCreator.createSearcherResource(zoneBizName,
            mainTable.first.range(), mainTable.first.fullversion(),
            _serviceInfo.getPartCount(), indexPartWrapper, searcherResource);
    if (!ret) {
        HA3_LOG(ERROR, "create SearcherResource Failed For Biz Name[%s], ErrorCode [%d]",
                _bizName.c_str(), ret.code);
    }
    return searcherResource;
}

bool SearcherBiz::getRange(uint32_t partCount, uint32_t partId, util::PartitionRange &range) {
    RangeVec vec = RangeUtil::splitRange(0, 65535, partCount);
    if (partId >= vec.size()) {
        return false;
    }
    range = vec[partId];
    return true;
}

bool SearcherBiz::getIndexPartition(const string &tableName, const Range &range,
                                    const TableReader &tableReader,
                                    pair<PartitionID, IE_NAMESPACE(partition)::IndexPartitionPtr> &table)
{
    // newer partition closer to map end
    for (TableReader::const_reverse_iterator it = tableReader.rbegin();
         it != tableReader.rend(); ++it)
    {
        if (it->first.tableId.tableName == tableName
            && (uint32_t)it->first.from == range.from()
            && (uint32_t)it->first.to == range.to()
            && it->second)
        {
            table = make_pair(suezPid2HaPid(it->first), it->second);
            return true;
        }
    }
    return false;
}

PartitionID SearcherBiz::suezPid2HaPid(const PartitionId& sPid) {
    PartitionID pPid;
    pPid.set_clustername(sPid.tableId.tableName);
    pPid.set_fullversion(sPid.tableId.fullVersion);
    pPid.set_role(ROLE_SEARCHER);
    Range range;
    range.set_from(sPid.from);
    range.set_to(sPid.to);
    *(pPid.mutable_range()) = range;
    return pPid;
}

Cluster2IndexPartitionMapPtr SearcherBiz::createCluster2IndexPatitionMap(
        const ConfigAdapterPtr &configAdapter,
        const string &zoneBizName,
        const TableReader &tableReader)
{
    Cluster2IndexPartitionMapPtr cluster2IndexPartitionMap(new Cluster2IndexPartitionMap);
    const vector<string> &joinTableNames = configAdapter->getJoinClusters(zoneBizName);
    vector<string>::const_iterator it = joinTableNames.begin();
    proto::Range range;
    range.set_from(0);
    range.set_to(65535);
    for (; it != joinTableNames.end() ; ++it) {
        pair<PartitionID, IE_NAMESPACE(partition)::IndexPartitionPtr> joinTable;
        if (!getIndexPartition(*it, range, tableReader, joinTable)) {
            HA3_LOG(ERROR, "get join table failed, zoneBizName [%s], join table [%s]",
                    zoneBizName.c_str(), (*it).c_str());
            return cluster2IndexPartitionMap;
        }
        (*cluster2IndexPartitionMap)[*it] = joinTable.second;
    }
    return cluster2IndexPartitionMap;
}

void SearcherBiz::clearSnapshot() {
    ScopedLock lock(_snapshotLock);
    _snapshotReader.reset();
}

END_HA3_NAMESPACE(turing);
