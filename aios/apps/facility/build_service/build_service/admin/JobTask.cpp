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
#include "build_service/admin/JobTask.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/ZlibCompressor.h"
#include "build_service/admin/taskcontroller/BuildServiceTask.h"
#include "build_service/admin/taskcontroller/SingleJobBuilderTask.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/proto/WorkerNodeCreator.h"
#include "build_service/util/DataSourceHelper.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/table/BuiltinDefine.h"

using namespace indexlib::config;

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::proto;
using namespace build_service::util;
using namespace build_service::config;
using namespace indexlib;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, JobTask);

JobTask::JobTask(const BuildId& buildId, cm_basic::ZkWrapper* zkWrapper)
    : GenerationTask(buildId, zkWrapper)
    , _mergeConfigName("full")
    , _needMerge(true)
{
    this->_taskType = TT_JOB;
}

JobTask::~JobTask() {}

void JobTask::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    GenerationTask::Jsonize(json);
    json.Jsonize("cluster_name", _clusterName);
    json.Jsonize("data_descriptions", _dataDescriptions.toVector());
    json.Jsonize("need_merge", _needMerge, _needMerge);
    json.Jsonize("merge_config_name", _mergeConfigName);
}

bool JobTask::checkDescriptionsValid()
{
    // check dataDescriptions, make sure all data source will terminate automatically
    for (size_t i = 0; i < _dataDescriptions.size(); i++) {
        DataDescription& ds = _dataDescriptions[i];
        if (DataSourceHelper::isRealtime(ds)) {
            string stopTimeStr = getValueFromKeyValueMap(ds.kvMap, SWIFT_STOP_TIMESTAMP);
            if (stopTimeStr.empty()) {
                BS_LOG(ERROR, "swift-like source must specify swift_stop_timestamp for src[%d]", (int)i);
                return false;
            }
            int64_t t;
            if (!StringUtil::fromString<int64_t>(stopTimeStr, t)) {
                BS_LOG(ERROR, "invalid stop time [%s] for swift-like source", stopTimeStr.c_str());
                return false;
            }
            BS_LOG(INFO, "stop time for swift-like source is %ld", t);
        }
    }
    return true;
}

bool JobTask::handleDataDescriptions(const DataDescriptions& dataDescriptions)
{
    assert(!dataDescriptions.empty());
    _dataDescriptions = dataDescriptions;
    DataDescription& ds = _dataDescriptions[dataDescriptions.size() - 1];
    if (DataSourceHelper::isRealtime(ds)) {
        if (ds.find(SWIFT_STOP_TIMESTAMP) == ds.end()) {
            _realTimeDataDesc = ds;
            _dataDescriptions.pop_back();
            int64_t srcSignature = (int64_t)_dataDescriptions.size();
            _realTimeDataDesc[SRC_SIGNATURE] = StringUtil::toString(srcSignature);
            BS_LOG(INFO, "find realtime swift info for job task.");
        }
    }
    if (_dataDescriptions.empty()) {
        BS_LOG(ERROR, "has no full dataDescriptions.");
        return false;
    }
    if (!checkDescriptionsValid()) {
        return false;
    }
    return true;
}

bool JobTask::doLoadFromConfig(const string& configPath, const string& generationDir,
                               const DataDescriptions& dataDescriptions, BuildStep buildStep)
{
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));
    ConfigReaderAccessorPtr configResource;
    _resourceManager->getResource(configResource);
    configResource->addConfig(resourceReader, true);

    vector<string> allClusters;
    if (!resourceReader->getAllClusters(_buildId.datatable(), allClusters)) {
        BS_LOG(ERROR, "getAllClusters from %s failed", _buildId.datatable().c_str());
        return false;
    }
    if (allClusters.size() != 1) {
        BS_LOG(ERROR, "job task only support one cluster");
        return false;
    }
    _clusterName = allClusters[0];

    BuildRuleConfig buildRuleConfig;
    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.builder_rule_config",
                                                      buildRuleConfig)) {
        BS_LOG(ERROR, "parse builder_rule_config for %s failed", _clusterName.c_str());
        return false;
    }

    if (!handleDataDescriptions(dataDescriptions)) {
        return false;
    }
    if (!setupRawFileOrLineData(resourceReader)) {
        return false;
    }
    KeyValueMap kvMap;
    kvMap["dataDescriptions"] = ToJsonString(_dataDescriptions.toVector(), true);
    kvMap["realtimeDataDescription"] = ToJsonString(_realTimeDataDesc, true);
    kvMap["buildId"] = ProtoUtil::buildIdToStr(_buildId);
    kvMap["buildStep"] = "full";
    kvMap["clusterName"] = _clusterName;
    kvMap["needMerge"] = _needMerge ? "true" : "false";
    kvMap["mergeConfigName"] = _mergeConfigName;
    if (!_taskFlowManager->init("BuildJob.graph", kvMap)) {
        REPORT(SERVICE_ERROR_CONFIG, "init [%s] graph failed", _buildId.ShortDebugString().c_str());
        return false;
    }
    _taskFlowManager->stepRun();
    return true;
}

bool JobTask::setupRawFileOrLineData(const ResourceReaderPtr& resourceReader)
{
    std::string tableType;
    if (!resourceReader->getTableType(_clusterName, tableType)) {
        return false;
    }
    if (indexlib::table::TABLE_TYPE_RAWFILE == tableType || indexlib::table::TABLE_TYPE_LINEDATA == tableType) {
        _needMerge = false;
    }
    return true;
}

bool JobTask::loadFromString(const string& statusString, const std::string& generationDir)
{
    try {
        FromJsonString(*this, statusString);
    } catch (const autil::legacy::ExceptionBase& e) {
        ZlibCompressor compressor;
        string decompressedStatus;
        if (!compressor.decompress(statusString, decompressedStatus)) {
            BS_LOG(ERROR, "decompress status string failed [%s]", statusString.c_str());
            return false;
        }
        try {
            FromJsonString(*this, decompressedStatus);
        } catch (const ExceptionBase& e) {
            BS_LOG(ERROR, "recover from %s faild, error: %s", statusString.c_str(), e.what());
            return false;
        }
    }
    return true;
}

bool JobTask::writeRealtimeInfoToIndex()
{
    vector<string> allClusters;
    if (!getAllClusterNames(allClusters)) {
        return false;
    }

    for (auto& clusterName : allClusters) {
        if (!doWriteRealtimeInfoForRealtimeJobMode(clusterName, _realTimeDataDesc)) {
            return false;
        }
    }
    return true;
}

void JobTask::prepareGeneration()
{
    if (!writeRealtimeInfoToIndex()) {
        string errorMsg = "write swift topic to index failed...";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return;
    }
    _step = GENERATION_STARTED;
    _startTimeStamp = TimeUtility::currentTimeInSeconds();
}

void JobTask::makeDecision(WorkerTable& workerTable)
{
    _taskFlowManager->stepRun();
    _flowCleaner->cleanFlows(_taskFlowManager, _generationDir);
    switch (_step) {
    case GENERATION_STARTING:
        prepareGeneration();
        break;
    case GENERATION_STARTED:
        doMakeDecision(workerTable);
        break;
    case GENERATION_STOPPING:
        _step = GENERATION_STOPPED;
        break;
    case GENERATION_IDLE:
        cleanup(workerTable);
        break;
    case GENERATION_STOPPED:
        cleanup(workerTable);
        break;
    default:
        break;
    }
}

void JobTask::doMakeDecision(WorkerTable& workerTable)
{
    _configCleaner->cleanObsoleteConfig();
    int32_t workerProtocolVersion = getWorkerProtocolVersion();
    if (workerProtocolVersion == UNKNOWN_WORKER_PROTOCOL_VERSION) {
        updateWorkerProtocolVersion(workerTable.getActiveNodes());
    }

    _isFullBuildFinish = checkFullBuildFinish();
    runTasks(workerTable);
    checkFatalError(workerTable);
    if (!_isFullBuildFinish) {
        return;
    }

    BS_LOG(INFO, "the whole job finished");
    string param = autil::EnvUtil::getEnv("bs_job_auto_stop");
    if (param == "false") {
        _step = GENERATION_IDLE;
    } else {
        _step = GENERATION_STOPPED;
    }
    _stopTimeStamp = TimeUtility::currentTimeInSeconds();
}

bool JobTask::checkFullBuildFinish() const
{
    if (_isFullBuildFinish) {
        return true;
    }
    vector<TaskFlowPtr> flows = _taskFlowManager->getAllFlow();
    for (auto& flow : flows) {
        assert(flow);
        if (!flow->isFlowFinish()) {
            return false;
        }
    }
    return true;
}

void JobTask::cleanup(WorkerTable& workerTable)
{
    if (!_isFullBuildFinish) {
        // force stop, kill all nodes
        workerTable.clearAllNodes();
        BS_LOG(INFO, "job is force stopped");
    }
}

// JobTask *JobTask::clone() const {
//     return new JobTask(*this);
// }

bool JobTask::restart(const string& statusString, const string& generationDir)
{
    BS_LOG(ERROR, "job not support restart");
    return false;
}

void JobTask::fillGenerationInfo(GenerationInfo* generationInfo, const WorkerTable* workerTable, bool fillTaskHistory,
                                 bool fillFlowInfo, bool fillEffectiveIndexInfo,
                                 const CounterCollector::RoleCounterMap& roleCounterMap)
{
    generationInfo->set_configpath(_configPath);
    generationInfo->set_buildstep(getBuildStepString(_step));
    generationInfo->set_datadescriptions(ToJsonString(_dataDescriptions.toVector()));
    JobInfo* jobInfo = generationInfo->mutable_jobinfo();
    TaskFlowPtr jobBuilderFlow = getFlowByTag("JobBuilder");
    if (workerTable) {
        fillJobInfo(jobInfo, jobBuilderFlow, "jobBuilder", *workerTable, roleCounterMap);
    }
    jobInfo->set_mergeconfigname(_mergeConfigName);

    ::google::protobuf::RepeatedPtrField<IndexInfo>* indexInfos = generationInfo->mutable_indexinfos();
    fillIndexInfosForCluster(_clusterName, true, indexInfos);
    for (auto& indexInfo : *indexInfos) {
        indexInfo.set_starttimestamp(_startTimeStamp);
        indexInfo.set_finishtimestamp(_stopTimeStamp);
    }
    if (fillFlowInfo && workerTable) {
        _taskFlowManager->fillTaskFlowInfos(generationInfo, *workerTable, roleCounterMap);
    }
    if (_step == GENERATION_STARTED) {
        _fatalErrorChecker.checkFatalErrorInRoles(generationInfo);
    }
    _fatalErrorChecker.fillGenerationFatalErrors(generationInfo);
}

bool JobTask::doUpdateConfig(const string& configPath)
{
    BS_LOG(ERROR, "job not support upc");
    return false;
}

bool JobTask::doSuspendBuild(const string& flowId, string& errorMsg)
{
    errorMsg = "job mode does not support suspend task";
    BS_LOG(ERROR, "job mode does not support suspend task");
    return false;
}

bool JobTask::doResumeBuild(const string& flowId, string& errorMsg)
{
    errorMsg = "job mode does not support resume task";
    BS_LOG(ERROR, "job mode does not support resume task");
    return false;
}

bool JobTask::doRollBack(const string& clusterName, const string& generationZkRoot, versionid_t versionId,
                         int64_t startTimestamp, string& errorMsg)
{
    errorMsg = "job mode does not support rollback";
    BS_LOG(ERROR, "%s", errorMsg.c_str());
    return false;
}

bool JobTask::doCreateVersion(const string& clusterName, const string& mergeConfigName)
{
    BS_LOG(ERROR, "job not support create version");
    return false;
}

bool JobTask::doStartTask(int64_t taskId, const string& taskName, const string& taskConfigPath,
                          const string& clusterName, const string& taskParam,
                          GenerationTaskBase::StartTaskResponse* response)
{
    response->message = "job not support start task";
    return false;
}

bool JobTask::doStopTask(int64_t taskId, std::string& errorMsg)
{
    errorMsg = "job not support stop task";
    return false;
}

bool JobTask::doSwitchBuild()
{
    BS_LOG(ERROR, "job not support switch");
    return false;
}

bool JobTask::cleanVersions(const string& clusterName, versionid_t version)
{
    string generationPath =
        IndexPathConstructor::getGenerationIndexPath(_indexRoot, clusterName, _buildId.generationid());

    try {
        vector<string> fileList;
        if (!fslib::util::FileUtil::listDir(generationPath, fileList)) {
            BS_LOG(WARN, "list generation dir [%s] failed.", generationPath.c_str());
            return false;
        }
        BS_LOG(INFO, "clean version [%d], generation dir [%s] cluster[%s]", version, generationPath.c_str(),
               clusterName.c_str());
        for (auto filePath : fileList) {
            if (!StringUtil::startsWith(filePath, IndexPathConstructor::PARTITION_PREFIX)) {
                continue;
            }
            bool dirExist = false;
            const string& partitionDir = fslib::util::FileUtil::joinFilePath(generationPath, filePath);
            if (!fslib::util::FileUtil::isDir(partitionDir, dirExist)) {
                continue;
            }
            auto directory = indexlib::file_system::Directory::GetPhysicalDirectory(partitionDir);
            indexlib::index_base::VersionCommitter::CleanVersionAndBefore(directory, version);
        }
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "clean versions before [%d] in cluster[%s] failed", version, clusterName.c_str());
        return false;
    }
    return true;
}

string JobTask::getBuildStepString(GenerationStep gs) const
{
    switch (gs) {
    case GENERATION_STARTING:
        return "starting";
    case GENERATION_STARTED: {
        TaskFlowPtr mergerFlow = getFlowByTag("JobMerger");
        if (!mergerFlow) {
            return "build";
        }
        TaskFlowPtr builderFlow = getFlowByTag("JobBuilder");
        if (builderFlow && builderFlow->isFlowFinish()) {
            return "merge";
        }
        return "build";
    }
    case GENERATION_STOPPING:
        return "stopping";
    case GENERATION_IDLE:
        return "finished";
    case GENERATION_STOPPED:
        return _isFullBuildFinish ? "finished" : "killed";
    default:
        return "unknown";
    }
}

vector<string> JobTask::getClusterNames() const
{
    vector<string> ret = {_clusterName};
    return ret;
}

TaskFlowPtr JobTask::getFlowByTag(const string& flowTag) const
{
    vector<string> flowIds;
    _taskFlowManager->getFlowIdByTag(flowTag, flowIds);

    vector<TaskFlowPtr> flows;
    fillFlows(flowIds, false, flows);
    return flows.empty() ? TaskFlowPtr() : *flows.rbegin();
}

void JobTask::fillJobInfo(JobInfo* jobInfo, const TaskFlowPtr& flow, const string& taskId,
                          const WorkerTable& workerTable, const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    TaskBasePtr task;
    if (flow) {
        task = flow->getTask(taskId);
    }

    if (!task) {
        jobInfo->set_clustername(_clusterName);
        jobInfo->set_partitioncount(1);
        jobInfo->set_buildparallelnum(1);
        jobInfo->set_datadescriptionid(_dataDescriptions.size());
        return;
    }

    BuildServiceTaskPtr bsTask = DYNAMIC_POINTER_CAST(BuildServiceTask, task);
    if (!bsTask) {
        return;
    }
    SingleJobBuilderTaskPtr jobTask = DYNAMIC_POINTER_CAST(SingleJobBuilderTask, bsTask->getTaskImpl());
    assert(jobTask);
    const WorkerNodes& workerNodes = workerTable.getWorkerNodes(task->getIdentifier());
    JobNodes jobNodes;
    for (auto& workerNode : workerNodes) {
        JobNodePtr node = DYNAMIC_POINTER_CAST(JobNode, workerNode);
        if (node) {
            jobNodes.push_back(node);
        }
    }
    jobTask->fillJobInfo(jobInfo, jobNodes, roleCounterMap);
}

}} // namespace build_service::admin
