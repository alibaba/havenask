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
#include "build_service/worker/MergerServiceImpl.h"

#include <algorithm>
#include <assert.h>
#include <map>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "beeper/beeper.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/CpuSpeedEstimater.h"
#include "build_service/common/NetworkTrafficEstimater.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/OfflineIndexConfigMap.h"
#include "build_service/config/OfflineMergeConfig.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/proto/ProtoJsonizer.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/task_base/NewFieldMergeTask.h"
#include "build_service/util/BsMetricTagsHandler.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/RangeUtil.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "fslib/util/MetricTagsHandler.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/index_summary.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace fslib;
using namespace fslib::fs;
using namespace fslib::util;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::common;
using namespace build_service::task_base;
using namespace build_service::util;
using namespace build_service::common;
using namespace build_service::config;

using namespace indexlib::index_base;
using namespace indexlib::common;

namespace build_service { namespace worker {
BS_LOG_SETUP(worker, MergerServiceImpl);

MergerServiceImpl::MergerServiceImpl(const PartitionId& pid, indexlib::util::MetricProviderPtr metricProvider,
                                     const LongAddress& address, const string& zkRoot, const string& appZkRoot,
                                     const string& adminServiceName, const std::string& epochId)
    : WorkerStateHandler(pid, metricProvider, appZkRoot, adminServiceName, epochId)
    , _zkRoot(zkRoot)
    , _hasCachedCheckpoint(false)
    , _task(NULL)
    , _startTimestamp(-1)
{
    *_current.mutable_longaddress() = address;
}

bool MergerServiceImpl::init()
{
    // for admin clear full worker znode, remove after bs support lease server
    bool ret = true;
    string fileName = fslib::util::FileUtil::joinFilePath(fslib::util::FileUtil::getParentDir(_zkRoot),
                                                          NO_MERGER_CHECKPOINT_FILE_NAME);
    fslib::util::FileUtil::writeFile(fileName, "");
    ret = _cpuEstimater.Start(/*sampleCountLimit=*/5, /*checkInterval=*/60);
    if (!ret) {
        return false;
    }
    ret = _networkEstimater.Start();
    return ret;
}

MergerServiceImpl::~MergerServiceImpl()
{
    if (_task) {
        // clean unused resource
        DELETE_AND_SET_NULL(_task);
    }
}

bool MergerServiceImpl::getServiceConfig(ResourceReader& resourceReader, BuildServiceConfig& serviceConfig)
{
    if (!resourceReader.getConfigWithJsonPath(BUILD_APP_FILE_NAME, "", serviceConfig)) {
        string errorMsg = "failed to parse build_app.json, configPath is [" + resourceReader.getConfigPath() + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        _hasFatalError = true;
        return false;
    }
    return true;
}

void MergerServiceImpl::doHandleTargetState(const string& state, bool hasResourceUpdated)
{
    MergerTarget target;
    if (!target.ParseFromString(state)) {
        FILL_ERRORINFO(MERGER_ERROR_FAILED, "failed to deserialize merger target state[" + state + "]", BS_RETRY);
        return;
    }
    if (!target.has_configpath()) {
        BS_LOG(INFO, "target is none, do nothing");
        return;
    }
    {
        ScopedLock lock(_lock);
        if (target.has_configpath()) {
            _configPath = target.configpath();
        }
    }
    if (_checkpointFilePath.empty()) {
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(target.configpath());

        BuildServiceConfig serviceConfig;
        if (!getServiceConfig(*(resourceReader.get()), serviceConfig)) {
            return;
        }
        _checkpointFilePath = IndexPathConstructor::getMergerCheckpointPath(serviceConfig.getIndexRoot(), _pid);
    }

    MergerCurrent checkpoint;
    if (!readCheckpoint(checkpoint)) {
        FILL_ERRORINFO(MERGER_ERROR_FAILED, "read checkpoint from [" + _checkpointFilePath + "] failed", BS_RETRY);
        _hasFatalError = true;
        return;
    }
    if (checkpoint.targetstatus() != target) {
        doMergeTarget(target);
    } else {
        ScopedLock lock(_lock);
        // //clean unused resource
        cleanUselessResource(target);
        _current = checkpoint;
        *_current.mutable_targetstatus() = target;
        _startTimestamp = _current.mutable_progressstatus()->starttimestamp();
        BS_INTERVAL_LOG(300, INFO, "merge task has been done, current[%s]", _current.ShortDebugString().c_str());
        string msg = "merge task has been done, current[" + _current.ShortDebugString() + "]";
        BEEPER_INTERVAL_REPORT(300, WORKER_STATUS_COLLECTOR_NAME, msg);
    }
}

bool MergerServiceImpl::merge(const proto::MergerTarget& target)
{
    string localConfigPath = downloadConfig(target.configpath());
    if (localConfigPath.empty()) {
        return false;
    }
    return mergeIndex(target, localConfigPath);
}

bool MergerServiceImpl::needSuspendTask(const std::string& state)
{
    MergerTarget target;
    if (!target.ParseFromString(state)) {
        string errorMsg = "failed to deserialize builder target state[" + state + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (target.has_suspendtask() && target.suspendtask() == true) {
        BS_LOG(WARN, "suspend task cmd received!");
        return true;
    }
    return false;
}

bool MergerServiceImpl::needRestartProcess(const string& state)
{
    MergerTarget target;
    if (!target.ParseFromString(state)) {
        string errorMsg = "failed to deserialize merger target state[" + state + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (checkUpdateConfig(target)) {
        return true;
    }
    return false;
}

bool MergerServiceImpl::checkUpdateConfig(const MergerTarget& target)
{
    if (!target.has_configpath() || _configPath.empty()) {
        return false;
    }
    string targetConfigPath = target.configpath();
    if (targetConfigPath != _configPath) {
        BS_LOG(INFO, "worker need restart for update config, old configPath[%s], new configPath[%s]",
               _configPath.c_str(), targetConfigPath.c_str());
        return true;
    }
    return false;
}

void MergerServiceImpl::updateMetricTagsHandler(const MergerTarget& target)
{
    MetricTagsHandlerPtr handler = FileSystem::getMetricTagsHandler();
    if (!handler) {
        return;
    }
    BsMetricTagsHandler* bsTagsHandler = dynamic_cast<BsMetricTagsHandler*>(handler.get());
    if (bsTagsHandler) {
        bsTagsHandler->setMergePhase(target.mergestep());
        if (target.optimizemerge()) {
            bsTagsHandler->setBuildStep(BuildStep::BUILD_STEP_FULL);
        } else {
            bsTagsHandler->setBuildStep(BuildStep::BUILD_STEP_INC);
        }
    }
}

void MergerServiceImpl::doMergeTarget(const MergerTarget& target)
{
    string localConfigPath = downloadConfig(target.configpath());
    if (localConfigPath.empty()) {
        FILL_ERRORINFO(MERGER_ERROR_FAILED, "config [" + target.configpath() + "] down load failed", BS_RETRY);
        setFatalError();
        return;
    }
    updateMetricTagsHandler(target);
    if (target.mergetask().mergeconfigname() == BS_ALTER_FIELD_MERGE_CONFIG) {
        if (!mergeNewField(target, localConfigPath)) {
            FILL_ERRORINFO(MERGER_ERROR_FAILED, "alter field merge failed", BS_RETRY);
            setFatalError();
            return;
        }
    } else {
        if (!mergeIndex(target, localConfigPath)) {
            FILL_ERRORINFO(MERGER_ERROR_FAILED, "merge failed", BS_RETRY);
            setFatalError();
            return;
        }
    }

    ScopedLock lock(_lock);
    *_current.mutable_targetstatus() = target;
    while (!commitCheckpoint(_current)) {
        FILL_ERRORINFO(MERGER_ERROR_FAILED,
                       "commit checkpoint[" + target.ShortDebugString() + "] to [" + _checkpointFilePath +
                           "] failed, will retry after 1 second",
                       BS_RETRY);
        usleep(COMMIT_CHECKPOINT_RETRY_INTERVAL);
    }

    if (_task) {
        _task->cleanUselessResource();
        DELETE_AND_SET_NULL(_task);
    }

    string msg = "merge task [" + target.ShortDebugString() + "] done";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);

    BS_LOG(INFO, "merge task[%s] done", target.ShortDebugString().c_str());
}

void MergerServiceImpl::fillProgressStatus(proto::MergerCurrent& current)
{
    current.mutable_progressstatus()->set_starttimestamp(_startTimestamp);
    current.mutable_progressstatus()->set_reporttimestamp(TimeUtility::currentTime());
    current.mutable_progressstatus()->set_startpoint(-1);
    current.mutable_progressstatus()->set_progress(-1);
}

void MergerServiceImpl::getCurrentState(string& content)
{
    ScopedLock lock(_lock);
    if (isSuspended()) {
        _current.set_issuspended(true);
    }
    fillProgressStatus(_current);
    fillProtocolVersion(_current);
    fillCpuSpeed(_current);
    fillNetworkTraffic(_current);
    saveCurrent(_current, content);
}

bool MergerServiceImpl::hasFatalError() { return _hasFatalError; }

bool MergerServiceImpl::mergeNewField(const MergerTarget& target, const std::string& localConfigPath)
{
    string msg = "alter field merge [" + target.ShortDebugString() + "] begin";
    BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, msg);

    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(localConfigPath);
    BuildServiceConfig serviceConfig;
    if (!getServiceConfig(*(resourceReader.get()), serviceConfig)) {
        return false;
    }

    const string& clusterName = _pid.clusternames(0);
    KeyValueMap kvMap;
    if (!prepareKVMap(target, serviceConfig, localConfigPath, kvMap)) {
        BS_LOG(ERROR, "merge new field failed");
        return false;
    }
    kvMap[MERGE_CONFIG_NAME] = BS_ALTER_FIELD_MERGE_CONFIG; // not really use merge config

    string jobParam = ToJsonString(kvMap);
    MergeStep mergeStep = target.mergestep();
    uint32_t instanceId = 0;
    if (!calculateMergerInstanceId(target, instanceId)) {
        return false;
    }
    _startTimestamp = TimeUtility::currentTime();
    uint32_t backupId = _pid.has_backupid() ? _pid.backupid() : 0;
    NewFieldMergeTask* mergeTask = new NewFieldMergeTask(_metricProvider, backupId, _epochId);
    if (!mergeTask->init(jobParam)) {
        BS_LOG(ERROR, "alter field merge task init failed");
    }
    _task = mergeTask;
    bool ret = false;
    if (mergeStep == MS_BEGIN_MERGE) {
        ret = mergeTask->run(instanceId, TaskBase::MERGE_MAP);
        if (ret) {
            ScopedLock lock(_lock);
            _current.set_targetversionid(mergeTask->getTargetVersionId());
        }
        return ret;
    } else if (mergeStep == MS_DO_MERGE) {
        ret = mergeTask->run(instanceId, TaskBase::MERGE_REDUCE);
    } else if (mergeStep == MS_END_MERGE) {
        ret = mergeTask->run(instanceId, TaskBase::END_MERGE_MAP);
        ScopedLock lock(_lock);
        ret &= fillIndexInfo(clusterName, _pid.range(), serviceConfig.getIndexRoot(), _current.mutable_indexinfo());
    } else {
        assert(false);
    }

    if (!ret) {
        stringstream ss;
        ss << "merge failed, jobParam is: [" << jobParam << "], "
           << "instanceId: [" << instanceId << "], "
           << "partitionId: [" << _pid.ShortDebugString().c_str() << "]";
        string errorMsg = ss.str();
        FILL_ERRORINFO(MERGER_ERROR_FAILED, errorMsg, BS_RETRY);
        setFatalError();
    }
    ScopedLock lock(_lock);
    BS_LOG(INFO, "merge worker stopped, current status : [%s]", _current.ShortDebugString().c_str());
    return ret;
}

bool MergerServiceImpl::parseTargetDescription(const MergerTarget& target, KeyValueMap& kvMap)
{
    KeyValueMap tdKVMap;
    try {
        FromJsonString(tdKVMap, target.targetdescription());
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "parse targetDescription [%s] fail", target.targetdescription().c_str());
        return false;
    }

    for (auto it = tdKVMap.begin(); it != tdKVMap.end(); ++it) {
        auto oldValue = kvMap.find(it->first);
        if (oldValue != kvMap.end()) {
            BS_LOG(INFO, "[%s:%s] is already exits, override it with [%s]", oldValue->first.c_str(),
                   oldValue->second.c_str(), it->second.c_str());
        }
        kvMap[it->first] = it->second;
    }
    return true;
}

bool MergerServiceImpl::prepareKVMap(const MergerTarget& target, const BuildServiceConfig& serviceConfig,
                                     const std::string& localConfigPath, KeyValueMap& kvMap)
{
    const string& clusterName = _pid.clusternames(0);
    const string& mergeConfigName = target.mergetask().mergeconfigname();
    kvMap[CONFIG_PATH] = localConfigPath;
    kvMap[INDEX_ROOT_PATH] = serviceConfig.getIndexRoot();
    kvMap[CLUSTER_NAMES] = clusterName;
    kvMap[MERGE_CONFIG_NAME] = mergeConfigName;
    kvMap[MERGE_TIMESTAMP] = StringUtil::toString(target.mergetask().timestamp());
    kvMap[PARTITION_COUNT] = StringUtil::toString(target.partitioncount());
    kvMap[BUILD_PARTITION_FROM] = "0";
    kvMap[BUILD_PARTITION_COUNT] = StringUtil::toString(target.partitioncount());
    kvMap[MERGE_PARALLEL_NUM] = StringUtil::toString(target.mergeparallelnum());
    kvMap[BUILD_PARALLEL_NUM] = StringUtil::toString(target.buildparallelnum());
    kvMap[GENERATION_ID] = StringUtil::toString(_pid.buildid().generationid());
    kvMap[BUILDER_INDEX_ROOT_PATH] = serviceConfig.getBuilderIndexRoot(target.optimizemerge());
    if (target.has_targetdescription()) {
        if (!parseTargetDescription(target, kvMap)) {
            return false;
        }
    }

    if (target.optimizemerge()) {
        kvMap[BUILD_MODE] = BUILD_MODE_FULL;
    } else {
        kvMap[BUILD_MODE] = BUILD_MODE_INCREMENTAL;
    }
    if (target.has_workerpathversion()) {
        kvMap[WORKER_PATH_VERSION] = StringUtil::toString(target.workerpathversion());
    }
    if (target.has_alignedversionid()) {
        kvMap[ALIGNED_VERSION_ID] = StringUtil::toString(target.alignedversionid());
    } else {
        kvMap[ALIGNED_VERSION_ID] = "-1";
    }
    return true;
}

bool MergerServiceImpl::calculateMergerInstanceId(const MergerTarget& target, uint32_t& instanceId)
{
    MergeStep mergeStep = target.mergestep();
    instanceId = mergeStep == MS_DO_MERGE
                     ? calculateInstanceId(_pid, target.partitioncount(), target.mergeparallelnum())
                     : calculateInstanceId(_pid, target.partitioncount(), 1);

    if (instanceId == (uint32_t)-1) {
        stringstream ss;
        ss << "instanceId error, pid[" << _pid.ShortDebugString() << "], mergeparallelnum[" << target.mergeparallelnum()
           << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool MergerServiceImpl::parseReservedVersions(const MergerTarget& target, KeyValueMap& kvMap)
{
    // read reserved_cluster_checkpoints
    string reservedCCP = "";
    if (target.has_reservedversions()) {
        kvMap[RESERVED_VERSION_LIST] = target.reservedversions();
        BS_LOG(INFO, "MergerTarget.reservedversions: [%s]", target.reservedversions().c_str());
    } else if (readReservedClusterCheckpoints(_pid, reservedCCP)) {
        kvMap[RESERVED_CLUSTER_CHECKPOINT_LIST] = reservedCCP;
        BS_LOG(INFO, "MergerTarget has no reservedversions. read it from zk. reserved_version_list: [%s]",
               reservedCCP.c_str());
    } else {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "read reserved_checkpoints failed", BS_RETRY);
        return false;
    }
    return true;
}

bool MergerServiceImpl::mergeIndex(const MergerTarget& target, const std::string& localConfigPath)
{
    string msg = "mergeIndex [" + target.ShortDebugString() + "] begin";
    BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, msg);

    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(localConfigPath);
    BuildServiceConfig serviceConfig;
    if (!getServiceConfig(*(resourceReader.get()), serviceConfig)) {
        return false;
    }

    KeyValueMap kvMap;
    if (!parseReservedVersions(target, kvMap)) {
        return false;
    }

    // overwrite CounterConfig
    CounterConfig& counterConfig = serviceConfig.counterConfig;
    if (!overWriteCounterConfig(_pid, counterConfig)) {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "rewrite CounterConfig failed", BS_STOP);
        return false;
    }

    _checkpointFilePath = IndexPathConstructor::getMergerCheckpointPath(serviceConfig.getIndexRoot(), _pid);

    const string& clusterName = _pid.clusternames(0);
    const string& mergeConfigName = target.mergetask().mergeconfigname();
    // TODO: only for test
    trySleep(*(resourceReader.get()), clusterName, mergeConfigName);
    kvMap[COUNTER_CONFIG_JSON_STR] = ToJsonString(counterConfig);
    if (!prepareKVMap(target, serviceConfig, localConfigPath, kvMap)) {
        BS_LOG(ERROR, "merge index failed");
        return false;
    }

    string jobParam = ToJsonString(kvMap);
    MergeStep mergeStep = target.mergestep();
    uint32_t instanceId = 0;
    if (!calculateMergerInstanceId(target, instanceId)) {
        return false;
    }
    _startTimestamp = TimeUtility::currentTime();
    task_base::MergeTask* mergeTask = createMergeTask();
    _task = mergeTask;

    bool optimize = target.optimizemerge();
    bool ret = false;
    if (mergeStep == MS_BEGIN_MERGE) {
        ret = mergeTask->run(jobParam, instanceId, TaskBase::MERGE_MAP, optimize);
        if (ret) {
            ScopedLock lock(_lock);
            task_base::MergeTask::MergeStatus status = mergeTask->getMergeStatus();
            _current.set_targetversionid(status.targetVersionId);
        }
    } else if (mergeStep == MS_DO_MERGE) {
        ret = mergeTask->run(jobParam, instanceId, TaskBase::MERGE_REDUCE, optimize);
    } else if (mergeStep == MS_END_MERGE) {
        ret = mergeTask->run(jobParam, instanceId, TaskBase::END_MERGE_MAP, optimize);
        ScopedLock lock(_lock);
        ret &= fillIndexInfo(clusterName, _pid.range(), serviceConfig.getIndexRoot(), _current.mutable_indexinfo());
    } else {
        assert(false);
    }

    if (!ret) {
        stringstream ss;
        ss << "merge failed, jobParam is: [" << jobParam << "], "
           << "instanceId: [" << instanceId << "], "
           << "partitionId: [" << _pid.ShortDebugString().c_str() << "]";
        string errorMsg = ss.str();
        FILL_ERRORINFO(MERGER_ERROR_FAILED, errorMsg, BS_RETRY);
        setFatalError();
    }
    ScopedLock lock(_lock);
    BS_LOG(INFO, "merge worker stopped, current status : [%s]", _current.ShortDebugString().c_str());
    return ret;
}

bool MergerServiceImpl::fillIndexInfo(const string& clusterName, const Range& range, const string& indexRoot,
                                      IndexInfo* indexInfo)
{
    indexInfo->set_clustername(clusterName);
    *(indexInfo->mutable_range()) = range;
    Version version;
    string targetIndexPath = IndexPathConstructor::constructIndexPath(indexRoot, _pid);
    BS_LOG(INFO, "get index version from[%s]", targetIndexPath.c_str());
    try {
        VersionLoader::GetVersionS(targetIndexPath, version, -1);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "get latest version failed: %s", e.what());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "get latest version failed: unknown exception!");
        return false;
    }
    indexInfo->set_indexversion(version.GetVersionId());
    indexInfo->set_versiontimestamp(version.GetTimestamp());
    indexInfo->set_schemaversion(version.GetSchemaVersionId());

    int64_t totalLength = DeployIndexWrapper::GetIndexSize(targetIndexPath, version.GetVersionId());
    if (totalLength < 0) {
        BS_LOG(ERROR, "get index size failed");
        return false;
    }

    indexInfo->set_indexsize(totalLength);
    IndexSummary sum = IndexSummary::Load(indexlib::file_system::Directory::GetPhysicalDirectory(targetIndexPath),
                                          version.GetVersionId());
    indexInfo->set_totalremainindexsize(sum.GetTotalSegmentSize());
    return true;
}

uint32_t MergerServiceImpl::calculateInstanceId(const PartitionId& pid, uint32_t partitionCount,
                                                uint32_t mergeParallelNum)
{
    vector<Range> ranges = RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, partitionCount, mergeParallelNum);
    for (size_t i = 0; i < ranges.size(); i++) {
        if (ranges[i].from() == pid.range().from() && ranges[i].to() == pid.range().to()) {
            return (uint32_t)i;
        }
    }
    return (uint32_t)-1;
}

task_base::MergeTask* MergerServiceImpl::createMergeTask()
{
    return new task_base::MergeTask(_metricProvider, _pid.has_backupid() ? _pid.backupid() : 0, _epochId);
}

bool MergerServiceImpl::readCheckpoint(MergerCurrent& checkpoint)
{
    if (_hasCachedCheckpoint.load(std::memory_order_acquire)) {
        checkpoint = _checkpoint;
        return true;
    }
    if (_checkpointFilePath.empty()) {
        BS_LOG(ERROR, "checkpoint file path is empty");
        return false;
    }
    string content;
    if (!fslib::util::FileUtil::readWithBak(_checkpointFilePath, content)) {
        BS_LOG(ERROR, "read checkpoint file[%s] failed", _checkpointFilePath.c_str());
        return false;
    }

    MergerCurrent remoteCheckpoint;
    if (content.empty()) {
        BS_LOG(INFO, "checkpoint file[%s] does not exist", _checkpointFilePath.c_str());
    } else {
        if (!ProtoJsonizer::fromJsonString(content, &remoteCheckpoint)) {
            string errorMsg = "translate json string[" + content + "]to proto failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }

    checkpoint = remoteCheckpoint;
    _checkpoint = remoteCheckpoint;
    _hasCachedCheckpoint.store(true, std::memory_order_release);
    return true;
}

bool MergerServiceImpl::commitCheckpoint(const MergerCurrent& checkpoint)
{
    MergerCurrent writeCheckpoint = checkpoint;
    writeCheckpoint.clear_longaddress();
    writeCheckpoint.clear_errorinfos();
    string jsonStr = ProtoJsonizer::toJsonString(writeCheckpoint);
    bool ret = fslib::util::FileUtil::writeWithBak(_checkpointFilePath, jsonStr);
    if (ret) {
        _checkpoint = checkpoint;
        _hasCachedCheckpoint.store(true, std::memory_order_release);
    }
    return ret;
}

void MergerServiceImpl::trySleep(const ResourceReader& resourceReader, const string& clusterName,
                                 const string& mergeConfigName)
{
    string clusterConfig = resourceReader.getClusterConfRelativePath(clusterName);
    OfflineIndexConfigMap configMap;
    if (!resourceReader.getConfigWithJsonPath(clusterConfig, "offline_index_config", configMap)) {
        return;
    }
    OfflineIndexConfigMap::ConstIterator iter = configMap.find(mergeConfigName);
    if (iter == configMap.end()) {
        return;
    }

    uint32_t sleepTime = iter->second.offlineMergeConfig.mergeSleepTime;
    if (sleepTime > 0) {
        BS_LOG(INFO, "merger will sleep [%u] seconds", sleepTime);
        sleep(sleepTime);
    }
}

bool MergerServiceImpl::isMultiPartMerge(const KeyValueMap& kvMap, const JobConfig& jobConfig)
{
    string indexRoot = getValueFromKeyValueMap(kvMap, INDEX_ROOT_PATH);
    string builderIndexRoot = getValueFromKeyValueMap(kvMap, BUILDER_INDEX_ROOT_PATH, "");
    if (!builderIndexRoot.empty() && indexRoot != builderIndexRoot) {
        return true;
    }
    const BuildRuleConfig& buildRuleConf = jobConfig.getBuildRuleConf();
    uint32_t originBuildParallelNum = buildRuleConf.buildParallelNum / buildRuleConf.partitionSplitNum;
    assert(originBuildParallelNum * buildRuleConf.partitionSplitNum == buildRuleConf.buildParallelNum);
    return originBuildParallelNum != 1;
}

void MergerServiceImpl::cleanUselessResource(const MergerTarget& target)
{
    string localConfigPath = downloadConfig(target.configpath());
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(localConfigPath);
    BuildServiceConfig serviceConfig;
    if (!getServiceConfig(*(resourceReader.get()), serviceConfig)) {
        BS_LOG(ERROR, "cleanUselessResource failed");
        _hasFatalError = true;
        return;
    }
    KeyValueMap kvMap;
    if (!prepareKVMap(target, serviceConfig, localConfigPath, kvMap)) {
        BS_LOG(ERROR, "cleanUselessResource failed");
        _hasFatalError = true;
        return;
    }
    if (target.mergestep() != MS_END_MERGE) {
        return;
    }
    JobConfig tmpJobConfig;
    tmpJobConfig.init(kvMap);
    if (!isMultiPartMerge(kvMap, tmpJobConfig)) {
        return;
    }
    string indexRoot = getValueFromKeyValueMap(kvMap, INDEX_ROOT_PATH);
    string builderIndexRoot = getValueFromKeyValueMap(kvMap, BUILDER_INDEX_ROOT_PATH, indexRoot);
    vector<Range> mergePartRanges = tmpJobConfig.getAllNeedMergePart(_pid.range());
    vector<string> mergePartPaths;
    mergePartPaths.reserve(mergePartRanges.size());
    for (size_t i = 0; i < mergePartRanges.size(); ++i) {
        PartitionId toMergePartId = _pid;
        *toMergePartId.mutable_range() = mergePartRanges[i];
        mergePartPaths.push_back(IndexPathConstructor::constructIndexPath(builderIndexRoot, toMergePartId));
    }
    for (size_t i = 0; i < mergePartPaths.size(); ++i) {
        if (fslib::util::FileUtil::remove(mergePartPaths[i])) {
            BS_LOG(INFO, "successfully remove dir[%s]", mergePartPaths[i].c_str());
        } else {
            string errorMsg = "failed to remove dir[" + mergePartPaths[i] + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
        }
    }
}

}} // namespace build_service::worker
