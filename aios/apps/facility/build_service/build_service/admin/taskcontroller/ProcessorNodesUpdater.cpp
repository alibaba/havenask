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
#include "build_service/admin/taskcontroller/ProcessorNodesUpdater.h"

#include "build_service/admin/HippoProtoHelper.h"
#include "build_service/admin/taskcontroller/ProcessorTargetInfos.h"
#include "build_service/admin/taskcontroller/ProcessorTask.h"
#include "build_service/admin/taskcontroller/ProcessorWriterVersion.h"
#include "build_service/config/CLIOptionNames.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::proto;
using namespace build_service::config;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ProcessorNodesUpdater);

const int64_t ProcessorNodesUpdater::UNLIMIT_STOP_TIME = std::numeric_limits<int64_t>::max();

ProcessorNodesUpdater::ProcessorNodesUpdater(const config::ResourceReaderPtr configReader,
                                             taskcontroller::NodeStatusManagerPtr nodeStatusManager,
                                             std::vector<std::string> clusterNames, const ProcessorIO& io,
                                             int64_t stopTimestamp, const std::string& configName, bool isTablet,
                                             bool needSafeWrite)
    : _configReader(configReader)
    , _nodeStatusManager(nodeStatusManager)
    , _clusterNames(clusterNames)
    , _input(io.input)
    , _output(io.output)
    , _stopTimestamp(stopTimestamp)
    , _lastSyncTime(0)
    , _syncCheckpointInterval(5)
    , _configName(configName)
    , _isTablet(isTablet)
    , _needSafeWrite(needSafeWrite)
{
    std::string param = autil::EnvUtil::getEnv("processor_sync_checkpoint_interval");
    if (param.empty() || !autil::StringUtil::fromString(param, _syncCheckpointInterval)) {
        _syncCheckpointInterval = 5;
    }
    BS_LOG(INFO, "safe write enabled [%d]", (int32_t)_needSafeWrite);
}

int64_t ProcessorNodesUpdater::getMinCheckpoint(const ProcessorNodes& processorNodes)
{
    int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
    if (currentTime < _lastSyncTime + _syncCheckpointInterval) {
        return _input.offset;
    }
    _lastSyncTime = currentTime;
    int64_t minCheckpoint = numeric_limits<int64_t>::max();
    for (size_t i = 0; i < processorNodes.size(); i++) {
        if (processorNodes[i]->isFinished()) {
            continue;
        }
        const ProcessorCurrent& current = processorNodes[i]->getCurrentStatus();
        if (!current.has_currentlocator()) {
            return -1;
        }
        minCheckpoint = min(minCheckpoint, current.currentlocator().checkpoint());
    }
    if (minCheckpoint != numeric_limits<int64_t>::max()) {
        return minCheckpoint;
    } else {
        return -1;
    }
}

void ProcessorNodesUpdater::update(proto::ProcessorNodes& processorNodes, const ProcessorWriterVersion& writerVersion,
                                   const ProcessorBasicInfo& basicInfo, ProcessorTargetInfos& lastTargetInfo)
{
    assert(processorNodes.size() == basicInfo.parallelNum * basicInfo.partitionCount);
    assert(writerVersion.size() == basicInfo.parallelNum * basicInfo.partitionCount);
    if (_input.supportUpdateCheckpoint()) {
        _input.offset = max(getMinCheckpoint(processorNodes), _input.offset);
    }

    _finished = AdminTaskBase::checkAndSetFinished(_nodeStatusManager, processorNodes);
    if (!_finished) {
        // can not switch when not all workers have been finished
        generateAndSetTarget(processorNodes, writerVersion, basicInfo, lastTargetInfo);
    }
}

ProcessorNodesUpdater* ProcessorNodesUpdater::clone() const { return new ProcessorNodesUpdater(*this); }

void ProcessorNodesUpdater::generateAndSetTarget(ProcessorNodes& nodes, const ProcessorWriterVersion& writerVersion,
                                                 const ProcessorBasicInfo& basicInfo,
                                                 ProcessorTargetInfos& lastTargetInfos) const
{
    assert(std::is_sorted(nodes.begin(), nodes.end(), [](const auto& left, const auto& right) {
        return left->getPartitionId().range().from() < right->getPartitionId().range().from();
    }));
    std::vector<ProcessorTargetInfo> targets;
    for (size_t i = 0; i < nodes.size(); ++i) {
        const auto& current = nodes[i]->getCurrentStatus();
        ProcessorTargetInfo targetInfo;
        if (current.has_currentlocator()) {
            const auto& locator = current.currentlocator();
            targetInfo = ProcessorTargetInfo(locator.checkpoint(), locator.userdata());
        }
        targets.push_back(targetInfo);
    }
    lastTargetInfos.update(basicInfo, targets);
    for (size_t i = 0; i < nodes.size(); ++i) {
        std::string targetIdentifier;
        if (_needSafeWrite) {
            targetIdentifier = HippoProtoHelper::getNodeIdentifier(*nodes[i]);
            nodes[i]->setIsReady(!targetIdentifier.empty());
        }
        nodes[i]->setTargetStatus(generateTargetStatus(lastTargetInfos.get(i), writerVersion.getMajorVersion(),
                                                       writerVersion.getMinorVersion(i)),
                                  targetIdentifier);
    }
}

ProcessorTarget ProcessorNodesUpdater::generateTargetStatus(const ProcessorTargetInfo& targetInfo,
                                                            uint32_t majorVersion, uint32_t minorVersion) const
{
    ProcessorTarget target;
    target.set_configpath(_configReader->getOriginalConfigPath());
    string dataDescription = ToJsonString(_input.dataDescriptions[_input.src]);
    target.set_datadescription(dataDescription);
    target.mutable_startlocator()->set_sourcesignature(_input.src);
    target.mutable_startlocator()->set_checkpoint(targetInfo.offset);
    target.mutable_startlocator()->set_userdata(targetInfo.userData);

    KeyValueMap kvMap;
    if (!_configName.empty()) {
        kvMap["configName"] = _configName;
    }
    kvMap["clusterNames"] = StringUtil::toString(_clusterNames, ";");
    if (_input.batchMask != -1) {
        kvMap[BATCH_MASK] = StringUtil::toString(_input.batchMask);
    }
    if (!_output.isEmpty()) {
        kvMap["output"] = ToJsonString(_output);
    }
    if (!_input.rawDocQueryString.empty()) {
        kvMap["rawDocQuery"] = _input.rawDocQueryString;
    }
    if (_isTablet) {
        kvMap["isTablet"] = "true";
    }
    if (_needSafeWrite) {
        kvMap[SYNC_LOCATOR_FROM_COUNTER] = KV_FALSE;
        kvMap[SWIFT_MAJOR_VERSION] = StringUtil::toString(majorVersion);
        kvMap[SWIFT_MINOR_VERSION] = StringUtil::toString(minorVersion);
    }
    target.set_targetdescription(ToJsonString(kvMap));
    if (_stopTimestamp != ProcessorTask::DEFAULT_SWITCH_TIMESTAMP) {
        target.set_stoptimestamp(_stopTimestamp);
    }

    return target;
}

}} // namespace build_service::admin
