#include "build_service/task_base/TaskBase.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/task_base/ConfigDownloader.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/IndexPartitionOptionsWrapper.h"
#include "build_service/config/DocReclaimSource.h"
#include "build_service/common/PathDefine.h"
#include "build_service/common/CheckpointList.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/util/FileUtil.h"
#include "build_service/util/EnvUtil.h"
#include "build_service/util/KmonitorUtil.h"
#include "build_service/util/IndexPathConstructor.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::common;
using namespace build_service::util;

namespace build_service {
namespace task_base {
BS_LOG_SETUP(task_base, TaskBase);

TaskBase::TaskBase()
    : _monitor(NULL)
{
}

TaskBase::TaskBase(IE_NAMESPACE(misc)::MetricProviderPtr metricProvider)
    : _metricProvider(metricProvider)
    , _monitor(NULL)
{
}

TaskBase::~TaskBase() {
}

bool TaskBase::init(const std::string &jobParam)
{
    BS_LOG(INFO, "job parameter string: [%s]", jobParam.c_str());

    if (!initKeyValueMap(jobParam, _kvMap)) {
        string errorMsg = "parse jobParam failed! job will exit!";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }

    string configPath = getValueFromKeyValueMap(_kvMap, CONFIG_PATH);
    string localConfPath;
    ConfigDownloader::DownloadErrorCode errorCode =
        ConfigDownloader::downloadConfig(configPath, localConfPath);
    if (errorCode != ConfigDownloader::DEC_NONE) {
        string errorMsg = "download config["
                          + configPath + "] to local failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _resourceReader.reset(new ResourceReader(localConfPath));
    _resourceReader->init();
    if (!_jobConfig.init(_kvMap)) {
        string errorMsg = "Invalid job config.";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }

    if (!_resourceReader->getDataTableFromClusterName(_jobConfig.getClusterName(), _dataTable)) {
        return false;
    }

    if (!getIndexPartitionOptions(_mergeIndexPartitionOptions)) {
        return false;
    }
    BS_LOG(INFO, "use merge config[%s]", autil::legacy::ToJsonString(_mergeIndexPartitionOptions).c_str());
    if (!addDataDescription(_dataTable, _kvMap)) {
        return false;
    }

    return true;
}

bool TaskBase::initKeyValueMap(const string &jobParam,
                               KeyValueMap &kvMap)
{
    try {
        FromJsonString(kvMap, jobParam);
    } catch (const ExceptionBase &e) {
        string errorMsg = "Invalid job param["+ jobParam
                          + "], from json failed[" + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

proto::PartitionId TaskBase::createPartitionId(
        uint32_t instanceId, Mode mode, const string &role)
{
    PartitionId partitionId;
    if (role == "merger") {
        partitionId.set_role(ROLE_MERGER);
    }
    *partitionId.mutable_range() = createRange(instanceId, mode);
    BuildId buildId;
    buildId.set_generationid(_jobConfig.getGenerationId());
    buildId.set_datatable(_dataTable);
    *partitionId.mutable_buildid() = buildId;
    partitionId.add_clusternames(_jobConfig.getClusterName());
    return partitionId;
}

proto::Range TaskBase::createRange(uint32_t instanceId, Mode mode) {
    switch (mode) {
    case BUILD_MAP:
        return _jobConfig.calculateBuildMapRange(instanceId);
    case BUILD_REDUCE:
        return _jobConfig.calculateBuildReduceRange(instanceId);
    case MERGE_MAP:
        return _jobConfig.calculateMergeMapRange(instanceId);
    case MERGE_REDUCE:
        return _jobConfig.calculateMergeReduceRange(instanceId);
    case END_MERGE_MAP:
        // same as merge map.
        return _jobConfig.calculateMergeMapRange(instanceId);
    default:
        assert(false);
        return Range();
    }
}

std::string TaskBase::getModeStr(Mode mode) {
    switch (mode) {
    case BUILD_MAP:
        return "build_map";
    case BUILD_REDUCE:
        return "build_reduce";
    case MERGE_MAP:
        return "merge_map";
    case MERGE_REDUCE:
        return "merge_reduce";
    case END_MERGE_MAP:
        return "end_merge_map";
    case END_MERGE_REDUCE:
        return "end_merge_reduce";
    default:
        assert(false);
        return "";
    }
}

bool TaskBase::startMonitor(const Range &range, Mode mode) {
    string kmonServiceName;
    if (EnvUtil::getValueFromEnv(BS_ENV_KMON_SERVICE_NAME.c_str(), kmonServiceName)) {
        kmonServiceName = kmonServiceName;
    } else {
        kmonServiceName = BS_ENV_DEFAULT_KMON_SERVICE_NAME;
    }
    BS_LOG(INFO, "set kmonservice name[%s]", kmonServiceName.c_str());
    _monitor = kmonitor_adapter::MonitorFactory::getInstance()->
               createMonitor(kmonServiceName);

    kmonitor::MetricsTags tags;
    KmonitorUtil::getTags(range, _jobConfig.getClusterName(),
                          _dataTable,
                          _jobConfig.getGenerationId(), tags);
    BS_LOG(INFO, "set metrics tag[%s]", tags.ToString().c_str());

    _reporter.reset(new kmonitor::MetricsReporter(kmonServiceName, "", tags));
    _metricProvider.reset(new IE_NAMESPACE(misc)::MetricProvider(_reporter));
    return true;
}

bool TaskBase::getIndexPartitionOptions(
        IE_NAMESPACE(config)::IndexPartitionOptions &options)
{
    string clusterName = _jobConfig.getClusterName();
    string mergeConfigName = getValueFromKeyValueMap(_kvMap, MERGE_CONFIG_NAME);
    bool ret =  IndexPartitionOptionsWrapper::getIndexPartitionOptions(
        *_resourceReader, clusterName, mergeConfigName, options);
    if (!ret) {
        return false;
    }
    options.SetIsOnline(false);

    if (!IndexPartitionOptionsWrapper::initReservedVersions(_kvMap, options)) {
        return false;
    }
    IndexPartitionOptionsWrapper::initOperationIds(_kvMap, options);

    return initDocReclaimConfig(options, mergeConfigName, clusterName);
}

bool TaskBase::initDocReclaimConfig(IE_NAMESPACE(config)::IndexPartitionOptions &options,
                                    const string& mergeConfigName,
                                    const string& clusterName) {
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_kvMap[CONFIG_PATH]);
    DocReclaimSource reclaimSource;
    bool isExist = false;
    string jsonPath = "offline_index_config.customized_merge_config." +
                      mergeConfigName + "." + DocReclaimSource::DOC_RECLAIM_SOURCE;
    if (!resourceReader->getClusterConfigWithJsonPath(
            clusterName, jsonPath, reclaimSource, isExist) || !isExist)
    {
        BS_LOG(WARN, "no doc_reclaim_source is configurated");
        return true;
    }

    generationid_t generationId;
    if (!StringUtil::fromString(_kvMap[GENERATION_ID], generationId)) {
        BS_LOG(ERROR, "conver [%s] generation id failed ", _kvMap[GENERATION_ID].c_str());
        return false;
    }

    string docReclaimConfigPath = FileUtil::joinFilePath(IndexPathConstructor::constructGenerationPath(
                    _kvMap[INDEX_ROOT_PATH], _kvMap[CLUSTER_NAMES], generationId),
                    IndexPathConstructor::DOC_RECLAIM_DATA_DIR);
    string docReclaimFile = FileUtil::joinFilePath(docReclaimConfigPath, BS_DOC_RECLAIM_CONF_FILE);
    bool isFileExist = false;
    if (!FileUtil::isExist(docReclaimFile, isFileExist)) {
        BS_LOG(ERROR, "get reclaim file [%s] failed", docReclaimFile.c_str());
        return false;
    }
    if (isFileExist) {
        options.GetMergeConfig().docReclaimConfigPath = docReclaimFile;
    }
    return true;
}

bool TaskBase::addDataDescription(const string &dataTable, KeyValueMap &kvMap) {
    // if src not specified, use user defined DATA_PATH
    string src = getValueFromKeyValueMap(kvMap, READ_SRC);
    if (src.empty()) {
        kvMap[READ_SRC] = FILE_READ_SRC;
        kvMap[READ_SRC_TYPE] = FILE_READ_SRC;
        return true;
    }

    DataDescriptions dataDescriptions;
    if (!_resourceReader->getDataTableConfigWithJsonPath(dataTable,
                    "data_descriptions", dataDescriptions.toVector()))
    {
        string errorMsg = "Invalid data_descriptions in data table.json.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    // use dataDescription
    for (size_t i = 0; i < dataDescriptions.size(); i++) {
        const DataDescription &dataDescription = dataDescriptions[i];
        if (getValueFromKeyValueMap(dataDescription.kvMap, READ_SRC) == src) {
            BS_LOG(INFO, "use data description[%s]",
                   autil::legacy::ToJsonString(dataDescription).c_str());
            for (auto it = dataDescription.begin();
                 it != dataDescription.end(); ++it)
            {
                if (kvMap.find(it->first) == kvMap.end()) {
                    kvMap[it->first] = it->second;
                }
            }
            BS_LOG(INFO, "kv map after merge[%s]",
                   autil::legacy::ToJsonString(kvMap).c_str());
            return true;
        }

    }

    string errorMsg = "data description [" + src + "] not found";
    BS_LOG(ERROR, "%s", errorMsg.c_str());
    return false;
}

bool TaskBase::getWorkerPathVersion(int32_t& workerPathVersion) const
{
    string workerPathVersionStr = getValueFromKeyValueMap(_kvMap, WORKER_PATH_VERSION, "");
    if (workerPathVersionStr.empty())
    {
        // legacy heartbeat from old version admin
        workerPathVersion = -1;
        return true;
    }

    if (!StringUtil::fromString(workerPathVersionStr, workerPathVersion))
    {
        string errorMsg = "invalid workerPathVersion [" + workerPathVersionStr + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool TaskBase::getBuildStep(BuildStep &buildStep) const
{
    string buildModeStr = getValueFromKeyValueMap(_kvMap, BUILD_MODE, "");
    if (BUILD_MODE_FULL == buildModeStr)
    {
        buildStep = BUILD_STEP_FULL;
        return true;
    }
    if (BUILD_MODE_INCREMENTAL == buildModeStr)
    {
        buildStep = BUILD_STEP_INC;
        return true;
    }
    string errorMsg = "invalid build mode [" + buildModeStr + "]";
    BS_LOG(ERROR, "%s", errorMsg.c_str());
    return false;
}

bool TaskBase::getIncBuildParallelNum(uint32_t& parallelNum) const {
    const string& clusterName = _jobConfig.getClusterName();
    BuildRuleConfig buildRuleConfig;
    if (!_resourceReader->getClusterConfigWithJsonPath(clusterName,
                    "cluster_config.builder_rule_config", buildRuleConfig))
    {
        string errorMsg = "parse cluster_config.builder_rule_config for ["
                          + clusterName +"] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    parallelNum = buildRuleConfig.incBuildParallelNum;
    return true;
}


}
}
