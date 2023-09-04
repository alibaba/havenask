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
#include "build_service/admin/taskcontroller/SingleJobBuilderTask.h"

#include "autil/TimeUtility.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/admin/CheckpointCreator.h"
#include "build_service/common/BuilderCheckpointAccessor.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/IndexCheckpointFormatter.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/WorkerNodeCreator.h"
#include "build_service/util/DataSourceHelper.h"
#include "indexlib/table/BuiltinDefine.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::util;
using namespace indexlib;

using build_service::common::BuilderCheckpointAccessorPtr;
using build_service::common::CheckpointAccessorPtr;
using build_service::common::IndexCheckpointFormatter;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, SingleJobBuilderTask);

SingleJobBuilderTask::SingleJobBuilderTask(const proto::BuildId& buildId, const string& clusterName,
                                           const TaskResourceManagerPtr& resMgr)
    : AdminTaskBase(buildId, resMgr)
    , _clusterName(clusterName)
    , _currentDataDescriptionId(0)
    , _partitionCount(1)
    , _buildParallelNum(1)
    , _schemaVersion(-1)
    , _swiftStopTs(-1)
    , _builderStartTimestamp(0)
    , _builderStopTimestamp(0)
{
}

SingleJobBuilderTask::~SingleJobBuilderTask() {}

bool SingleJobBuilderTask::init(const proto::DataDescriptions& dataDescriptions)
{
    _dataDescriptions = dataDescriptions;
    if (_dataDescriptions.empty()) {
        BS_LOG(ERROR, "has no full dataDescriptions.");
        return false;
    }

    if (!getSwiftStopTimestamp(dataDescriptions, _swiftStopTs)) {
        return false;
    }
    ConfigReaderAccessorPtr configReaderAccessor;
    _resourceManager->getResource(configReaderAccessor);
    config::ResourceReaderPtr resourceReader = configReaderAccessor->getLatestConfig();
    return loadFromConfig(resourceReader);
}

bool SingleJobBuilderTask::start(const KeyValueMap& kvMap)
{
    ConfigReaderAccessorPtr configReaderAccessor;
    _resourceManager->getResource(configReaderAccessor);
    config::ResourceReaderPtr resourceReader = configReaderAccessor->getLatestConfig();
    if (!loadFromConfig(resourceReader)) {
        BS_LOG(ERROR, "start job builder [%s] failed.", _clusterName.c_str());
        return false;
    }
    _taskStatus = TASK_NORMAL;
    _builderStartTimestamp = TimeUtility::currentTimeInSeconds();
    return true;
}

bool SingleJobBuilderTask::loadFromConfig(const config::ResourceReaderPtr& resourceReader)
{
    BuildRuleConfig buildRuleConfig;
    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.builder_rule_config",
                                                      buildRuleConfig)) {
        BS_LOG(ERROR, "parse builder_rule_config for %s failed", _clusterName.c_str());
        return false;
    }

    _configPath = resourceReader->getOriginalConfigPath();
    _partitionCount = buildRuleConfig.partitionCount;
    if (_partitionCount != 1) {
        BS_LOG(ERROR, "only support build one partition for job");
        return false;
    }

    _buildParallelNum = buildRuleConfig.buildParallelNum;
    std::string tableType;
    if (!resourceReader->getTableType(_clusterName, tableType)) {
        return false;
    }

    if (indexlib::table::TABLE_TYPE_RAWFILE == tableType || indexlib::table::TABLE_TYPE_LINEDATA == tableType) {
        _buildParallelNum = 1;
    }

    auto schema = resourceReader->getTabletSchema(_clusterName);
    if (!schema) {
        string errorMsg = "fail to get Schema for cluster[" + _clusterName + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _schemaVersion = schema->GetSchemaId();
    return true;
}

void SingleJobBuilderTask::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("current_data_description_id", _currentDataDescriptionId);
    json.Jsonize("data_descriptions", _dataDescriptions.toVector());
    json.Jsonize("partition_count", _partitionCount);
    json.Jsonize("build_parallel_num", _buildParallelNum);
    json.Jsonize("nodes_start_timestamp", _nodesStartTimestamp, -1L);
    json.Jsonize("schema_version", _schemaVersion, _schemaVersion);
    json.Jsonize("swift_stop_timestamp", _swiftStopTs, _swiftStopTs);
    json.Jsonize("builder_start_timestamp", _builderStartTimestamp, _builderStartTimestamp);
    json.Jsonize("builder_stop_timestamp", _builderStopTimestamp, _builderStopTimestamp);
}

string SingleJobBuilderTask::getTaskPhaseIdentifier() const
{
    return string("jobBuilder_phase_") + autil::StringUtil::toString(_currentDataDescriptionId);
}

void SingleJobBuilderTask::doSupplementLableInfo(KeyValueMap& info) const
{
    info["current_data_description_id"] = StringUtil::toString(_currentDataDescriptionId);
    info["total_data_description_size"] = StringUtil::toString(_dataDescriptions.size());

    if (_fatalErrorDiscoverer) {
        _fatalErrorDiscoverer->supplementFatalErrorInfo(info);
    }
}

bool SingleJobBuilderTask::finish(const KeyValueMap& kvMap) { return true; }

void SingleJobBuilderTask::waitSuspended(WorkerNodes& workerNodes)
{
    if (_taskStatus == TASK_SUSPENDED) {
        return;
    }
    bool allSuspended = true;
    JobNodes jobNodes;
    jobNodes.reserve(workerNodes.size());
    for (auto& workerNode : workerNodes) {
        jobNodes.push_back(DYNAMIC_POINTER_CAST(JobNode, workerNode));
    }

    for (const auto& jobNode : jobNodes) {
        if (jobNode->isSuspended() || jobNode->isFinished()) {
            continue;
        }
        const JobCurrent& current = jobNode->getCurrentStatus();
        if (current.has_issuspended() && current.issuspended()) {
            jobNode->setSuspended(true);
        } else {
            allSuspended = false;
            JobTarget suspendTarget;
            suspendTarget.set_suspendtask(true);
            jobNode->setTargetStatus(suspendTarget);
        }
    }
    if (allSuspended) {
        _taskStatus = TASK_SUSPENDED;
    }
    return;
}

bool SingleJobBuilderTask::run(WorkerNodes& workerNodes)
{
    JobNodes jobNodes;
    jobNodes.reserve(workerNodes.size());
    for (auto& workerNode : workerNodes) {
        jobNodes.push_back(DYNAMIC_POINTER_CAST(JobNode, workerNode));
    }
    bool ret = run(jobNodes);
    workerNodes.clear();
    workerNodes.reserve(jobNodes.size());
    for (auto& jobNode : jobNodes) {
        workerNodes.push_back(jobNode);
    }
    if (ret) {
        _taskStatus = TASK_FINISHED;
    }
    return ret;
}

bool SingleJobBuilderTask::run(JobNodes& nodes)
{
    if (_currentDataDescriptionId >= _dataDescriptions.size()) {
        BS_LOG(INFO, "job builder finished");
        endBuild();
        return true;
    }

    if (nodes.empty()) {
        nodes = WorkerNodeCreator<ROLE_JOB>::createNodes(_partitionCount, _buildParallelNum, BUILD_STEP_FULL, _buildId,
                                                         _clusterName, "",
                                                         autil::StringUtil::toString(_currentDataDescriptionId));
        initJobNodes(nodes);
        return false;
    }

    if (!_fatalErrorDiscoverer) {
        _fatalErrorDiscoverer.reset(new FatalErrorDiscoverer());
    }
    _fatalErrorDiscoverer->collectNodeFatalErrors(nodes);
    bool allNodesFinished = markFinishedWorkers(nodes);
    // detectSlowNodes(nodes, getConfigReader(), _nodesStartTimestamp, false, _clusterName);
    if (!allNodesFinished) {
        generateAndSetTarget(nodes);
        return false;
    }

    nodes.clear();
    _fatalErrorDiscoverer.reset();
    _nodesStartTimestamp = -1;
    _slowNodeDetector.reset();
    _currentDataDescriptionId++;
    if (_currentDataDescriptionId < _dataDescriptions.size()) {
        BS_LOG(INFO, "stepping to next data source: %s",
               ToJsonString(_dataDescriptions[_currentDataDescriptionId]).c_str());
        return false;
    }
    BS_LOG(INFO, "job builder finished");
    endBuild();
    return true;
}

void SingleJobBuilderTask::initJobNodes(JobNodes& activeNodes)
{
    if (_nodesStartTimestamp == -1) {
        _nodesStartTimestamp = autil::TimeUtility::currentTimeInMicroSeconds();
    }
    _slowNodeDetector.reset();
    generateAndSetTarget(activeNodes);
    BS_LOG(INFO, "cluster(%s) data source (%d) begin", _clusterName.c_str(), _currentDataDescriptionId);
}

bool SingleJobBuilderTask::markFinishedWorkers(const JobNodes& nodes)
{
    bool finished = true;
    for (size_t i = 0; i < nodes.size(); i++) {
        const JobNodePtr& node = nodes[i];
        if (node->isFinished() || nodeFinished(node)) {
            node->setFinished(true);
        } else {
            finished = false;
        }
    }
    return finished;
}

bool SingleJobBuilderTask::nodeFinished(const JobNodePtr& node) const
{
    const JobTarget& target = node->getTargetStatus();
    const JobCurrent& current = node->getCurrentStatus();
    return current.has_targetstatus() && current.targetstatus() == target && current.status() == WS_STOPPED;
}

void SingleJobBuilderTask::generateAndSetTarget(const JobNodes& nodes)
{
    JobTarget target;
    target.set_configpath(_configPath);
    target.set_datasourceid(_currentDataDescriptionId);
    target.set_datadescription(ToJsonString(_dataDescriptions[_currentDataDescriptionId]));

    for (size_t i = 0; i < nodes.size(); i++) {
        if (!nodes[i]->isFinished()) {
            nodes[i]->setTargetStatus(target);
        }
    }
}

bool SingleJobBuilderTask::updateConfig()
{
    BS_LOG(WARN, "job mode not support suspend");
    return false;
}

void SingleJobBuilderTask::clearFullWorkerZkNode(const std::string& generationDir) const
{
    doClearFullWorkerZkNode(generationDir, ROLE_JOB, _clusterName);
}

void SingleJobBuilderTask::fillJobInfo(JobInfo* jobInfo, const JobNodes& nodes,
                                       const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    jobInfo->set_clustername(_clusterName);
    jobInfo->set_partitioncount(_partitionCount);
    jobInfo->set_buildparallelnum(_buildParallelNum);
    jobInfo->set_datadescriptionid(_currentDataDescriptionId);
    if (_currentDataDescriptionId < _dataDescriptions.size()) {
        jobInfo->set_datadescription(ToJsonString(_dataDescriptions[_currentDataDescriptionId]));
    }
    if (_fatalErrorDiscoverer) {
        _fatalErrorDiscoverer->fillFatalErrors(jobInfo->mutable_fatalerrors());
    }
    fillPartitionInfos(jobInfo, nodes, roleCounterMap);
}

void SingleJobBuilderTask::endBuild()
{
    if (_builderStopTimestamp <= _builderStartTimestamp) {
        _builderStopTimestamp = TimeUtility::currentTimeInSeconds();
    }

    proto::BuilderCheckpoint buildCheckpoint;
    buildCheckpoint.set_clustername(_clusterName);
    buildCheckpoint.set_versiontimestamp(_swiftStopTs);
    buildCheckpoint.set_processorcheckpoint(-1);
    buildCheckpoint.set_builderstarttime(_builderStartTimestamp);
    buildCheckpoint.set_builderstoptime(_builderStopTimestamp);
    buildCheckpoint.set_schemaversion(_schemaVersion);
    buildCheckpoint.set_workerpathversion(-1);

    BuilderCheckpointAccessorPtr checkpointAccessor =
        CheckpointCreator::createBuilderCheckpointAccessor(_resourceManager);
    checkpointAccessor->clearCheckpoint(_clusterName);
    checkpointAccessor->addCheckpoint(_clusterName, buildCheckpoint);
}

bool SingleJobBuilderTask::getSwiftStopTimestamp(const DataDescriptions& dataDescriptions, int64_t& ts)
{
    for (size_t i = 0; i < dataDescriptions.size(); i++) {
        const DataDescription& ds = dataDescriptions[i];
        auto iter = ds.find(READ_SRC_TYPE);
        if (iter == ds.end()) {
            BS_LOG(ERROR, "invalid data source without type");
            return false;
        }
        if (!DataSourceHelper::isRealtime(ds)) {
            continue;
        }
        iter = ds.find(SWIFT_STOP_TIMESTAMP);
        if (iter == ds.end()) {
            BS_LOG(ERROR, "swift type data source should has stopTimestamp for job builder");
            return false;
        }
        if (!StringUtil::fromString(iter->second, ts)) {
            BS_LOG(ERROR, "invalid stop timestamp [%s] in swift data source", iter->second.c_str());
            return false;
        }
    }
    return true;
}

}} // namespace build_service::admin
