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
#include "build_service/admin/taskcontroller/MergeCrontabTask.h"

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/proto/ProtoUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::proto;
using namespace build_service::config;
using namespace autil::legacy::json;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, MergeCrontabTask);

const std::string MergeCrontabTask::MERGE_CRONTAB = "mergeCrontab";

MergeCrontabTask::MergeCrontabTask() : _buildStep(proto::BuildStep::BUILD_STEP_FULL) {}

MergeCrontabTask::~MergeCrontabTask() {}

MergeCrontabTask::MergeCrontabTask(const MergeCrontabTask& other) : TaskBase(other), _impl(other._impl) {}

bool MergeCrontabTask::doInit(const KeyValueMap& kvMap)
{
    string buildIdStr;
    if (!getValueFromKVMap(kvMap, "buildId", buildIdStr)) {
        BS_LOG(ERROR, "create mergeCrontab fail, lack of buildId");
        return false;
    }
    proto::BuildId buildId;
    if (!ProtoUtil::strToBuildId(buildIdStr, buildId)) {
        BS_LOG(ERROR, "deserialize failed, create task [%s] fail", _taskType.c_str());
        return false;
    }

    if (!getValueFromKVMap(kvMap, "clusterName", _clusterName) || _clusterName.empty()) {
        BS_LOG(ERROR, "create MergeCrontabTask for buildId[%s] failed.", buildIdStr.c_str());
        return false;
    }

    string buildStepStr;
    if (!getValueFromKVMap(kvMap, "buildStep", buildStepStr)) {
        BS_LOG(ERROR, "parmeter no buildStep");
        return false;
    }
    if (buildStepStr == "full") {
        _buildStep = BUILD_STEP_FULL;
    } else {
        _buildStep = BUILD_STEP_INC;
    }

    _impl.reset(new SingleMergeTaskManager(_resourceManager));

    ConfigReaderAccessorPtr configAccessor;
    _resourceManager->getResource(configAccessor);
    if (!configAccessor) {
        BS_LOG(ERROR, "create MergeCrontabTask for buildId[%s] failed.", buildIdStr.c_str());
        return false;
    }
    const string& configPath = configAccessor->getLatestConfigPath();
    if (!_impl->loadFromConfig(buildId.datatable(), _clusterName, configPath, false)) {
        return false;
    }
    return true;
}

bool MergeCrontabTask::executeCmd(const string& cmdName, const KeyValueMap& params)
{
    if (cmdName == "syncVersion") {
        auto iter = params.find("mergeConfig");
        if (iter == params.end()) {
            BS_LOG(ERROR, "lack mergeConfig for %s", _clusterName.c_str());
            return false;
        }
        _impl->syncVersion(iter->second);
        return true;
    }
    return false;
}

bool MergeCrontabTask::doStart(const KeyValueMap& kvMap)
{
    assert(_impl);
    // start from inc build
    if (_buildStep == BUILD_STEP_INC) {
        if (!_impl->startMergeCrontab(true)) {
            return false;
        }
        return true;
    }

    // start from full build
    if (!_impl->startMergeCrontab(false)) {
        return false;
    }
    int32_t fullMergeTime = 1, flowValue = 0;
    if (StringUtil::fromString(getValueFromKeyValueMap(kvMap, "fullMergeStrategyCount", "-1"), flowValue) &&
        flowValue >= 0) {
        fullMergeTime = flowValue;
    }
    BS_LOG(INFO, "will trigger full merge stragety [%u] times", fullMergeTime);
    for (int32_t i = 0; i < fullMergeTime; ++i) {
        _impl->triggerFullMerge();
    }
    return true;
}

bool MergeCrontabTask::doFinish(const KeyValueMap& kvMap)
{
    assert(false);
    _impl->stopMergeCrontab();
    return true;
}

void MergeCrontabTask::doSyncTaskProperty(WorkerNodes& workerNodes)
{
    string mergeTask;
    if (_impl->generateMergeTask(mergeTask)) {
        setProperty("merging_task_name", mergeTask);
        setProperty("has_merging_task", "true");
        setProperty("current_merging_task_finished", "false");
        return;
    }

    const string& isFinishedStr = getProperty("current_merging_task_finished");
    bool isFinished = false;
    if (!StringUtil::fromString(isFinishedStr, isFinished)) {
        return;
    }
    if (isFinished) {
        _impl->mergeTaskDone();
        static std::string emptyStr;
        setProperty("merging_task_name", emptyStr);
        setProperty("has_merging_task", "false");
    }
}

TaskBase* MergeCrontabTask::clone()
{
    TaskBase* taskCopy = new MergeCrontabTask(*this);
    return taskCopy;
}

void MergeCrontabTask::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("start_build_step", _buildStep, _buildStep);
    json.Jsonize("cluster_name", _clusterName, _clusterName);
    if (json.GetMode() == TO_JSON) {
        _impl->Jsonize(json);
    } else {
        _impl.reset(new SingleMergeTaskManager(_resourceManager));
        _impl->Jsonize(json);
    }
}

bool MergeCrontabTask::updateConfig()
{
    ConfigReaderAccessorPtr configAccessor;
    _resourceManager->getResource(configAccessor);
    const string& configPath = configAccessor->getLatestConfigPath();
    if (!_impl->loadFromConfig(_impl->getDataTable(), _clusterName, configPath, _impl->hasCrontab())) {
        return false;
    }
    return true;
}

bool MergeCrontabTask::createVersion(const string& mergeConfigName)
{
    assert(_impl);
    return _impl->createVersion(mergeConfigName);
}

bool MergeCrontabTask::CheckKeyAuthority(const string& key) { return key == "current_merging_task_finished"; }

vector<string> MergeCrontabTask::getPendingMergeTasks() const { return _impl->getPendingMergeTasks(); }

}} // namespace build_service::admin
