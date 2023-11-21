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
#include "indexlib/framework/IndexTaskQueue.h"

#include "indexlib/framework/index_task/Constant.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, IndexTaskQueue);
AUTIL_LOG_SETUP(indexlib.framework, IndexTaskMeta);

const std::string IndexTaskMeta::PENDING = "PENDING";
const std::string IndexTaskMeta::ABORTED = "ABORTED";
const std::string IndexTaskMeta::SUSPENDED = "SUSPENDED";
const std::string IndexTaskMeta::DONE = "DONE";
const std::string IndexTaskMeta::UNKNOWN = "UNKNOWN";

void IndexTaskMeta::Jsonize(JsonWrapper& json)
{
    json.Jsonize("task_type", _taskType, _taskType);
    json.Jsonize("task_trace_id", _taskTraceId, _taskTraceId);
    json.Jsonize("task_name", _taskName, _taskName);
    json.Jsonize("state", _state, _state);
    json.Jsonize("event_time_in_secs", _eventTimeInSecs, _eventTimeInSecs);
    json.Jsonize("begin_time_in_secs", _beginTimeInSecs, _beginTimeInSecs);
    json.Jsonize("end_time_in_secs", _endTimeInSecs, _endTimeInSecs);
    json.Jsonize("committed_version_id", _committedVersionId, _committedVersionId);
    json.Jsonize("comment", _comment, _comment);
    if (json.GetMode() == TO_JSON) {
        if (!_params.empty()) {
            json.Jsonize("params", _params, _params);
        }
    } else {
        json.Jsonize("params", _params, _params);
    }
}

bool IndexTaskMeta::UpdateState(const std::string& state)
{
    if (!ValidateStateTransfer(_state, state)) {
        return false;
    }
    _state = state;
    if (_state == DONE || _state == ABORTED) {
        _params.clear();
        _endTimeInSecs = autil::TimeUtility::currentTimeInSeconds();
    }
    return true;
}

bool IndexTaskMeta::ValidateStateTransfer(const std::string& srcState, const std::string& dstState)
{
    bool isValid = true;
    if (srcState == dstState) {
        return isValid;
    }
    if (srcState == DONE) {
        isValid = false;
    } else if (srcState == ABORTED) {
        isValid = false;
    } else if (srcState == SUSPENDED) {
        if (dstState == DONE) {
            isValid = false;
        }
    }
    if (!isValid) {
        AUTIL_LOG(ERROR, "state transition: '%s' to '%s' is forbidden", srcState.c_str(), dstState.c_str());
    }
    return isValid;
}

void IndexTaskQueue::Join(const IndexTaskQueue& other, int64_t currentTimeInSec)
{
    std::map<std::pair<std::string, std::string>, std::shared_ptr<IndexTaskMeta>> stoppedTasks;
    for (const auto& task : _indexTasks) {
        if (task->GetState() == IndexTaskMeta::DONE || task->GetState() == IndexTaskMeta::ABORTED) {
            stoppedTasks[std::make_pair(task->GetTaskType(), task->GetTaskTraceId())] = task;
        }
    }
    auto otherIndexTasks = other.GetAllTasks();
    std::vector<std::shared_ptr<IndexTaskMeta>> result;
    for (const auto& task : otherIndexTasks) {
        auto iter = stoppedTasks.find(std::make_pair(task->GetTaskType(), task->GetTaskTraceId()));
        if (iter != stoppedTasks.end()) {
            const auto& stoppedTask = iter->second;
            if (currentTimeInSec - stoppedTask->GetEndTimeInSecs() < indexlib::DEFAULT_DONE_TASK_TTL_IN_SECONDS) {
                result.emplace_back(iter->second);
            } else {
                AUTIL_LOG(INFO, "task type[%s], task trace id[%s] exceeds stopped task ttl [%ld]",
                          task->GetTaskType().c_str(), task->GetTaskTraceId().c_str(),
                          indexlib::DEFAULT_DONE_TASK_TTL_IN_SECONDS);
            }
        } else {
            result.emplace_back(task);
        }
    }
    _indexTasks.swap(result);
}

void IndexTaskQueue::Handle(IndexTaskMeta meta, Action action)
{
    switch (action) {
    case Action::ABORT:
        Abort(meta.GetTaskType(), meta.GetTaskTraceId());
        break;
    case Action::OVERWRITE:
        Overwrite(meta);
        break;
    case Action::SUSPEND:
        if (Get(meta.GetTaskType(), meta.GetTaskTraceId()))
            meta.UpdateState(IndexTaskMeta::SUSPENDED);
        Add(meta);
        Suspend(meta.GetTaskType(), meta.GetTaskTraceId());
        break;
    case Action::ADD:
        Add(meta);
        break;
    default:
        AUTIL_LOG(WARN, "handle: invalid action: usage action=%s|%s|%s|%s",
                  ActionConvertUtil::ActionToStr(Action::ADD).c_str(),
                  ActionConvertUtil::ActionToStr(Action::ABORT).c_str(),
                  ActionConvertUtil::ActionToStr(Action::OVERWRITE).c_str(),
                  ActionConvertUtil::ActionToStr(Action::SUSPEND).c_str());
        return;
    }
}

void IndexTaskQueue::Add(IndexTaskMeta meta)
{
    // ignore duplicate index task
    size_t idx = 0;
    if (Find(meta.GetTaskType(), meta.GetTaskTraceId(), &idx)) {
        return;
    }
    meta.SetBeginTimeInSecs(autil::TimeUtility::currentTimeInSeconds());
    _indexTasks.emplace_back(std::make_shared<IndexTaskMeta>(meta));
}

bool IndexTaskQueue::Find(const std::string& taskType, const std::string& taskTraceId, size_t* idx) const
{
    for (size_t i = 0; i < _indexTasks.size(); i++) {
        if (_indexTasks[i]->GetTaskType() == taskType && _indexTasks[i]->GetTaskTraceId() == taskTraceId) {
            *idx = i;
            return true;
        }
    }
    return false;
}

std::shared_ptr<IndexTaskMeta> IndexTaskQueue::Get(const std::string& taskType, const std::string& taskTraceId) const
{
    size_t idx = 0;
    if (Find(taskType, taskTraceId, &idx)) {
        return _indexTasks[idx];
    }
    return nullptr;
}

bool IndexTaskQueue::Abort(const std::string& taskType, const std::string& taskTraceId)
{
    auto taskMeta = Get(taskType, taskTraceId);
    if (taskMeta == nullptr) {
        AUTIL_LOG(ERROR, "abort: '%s %s': No such index task meta", taskType.c_str(), taskTraceId.c_str());
        return false;
    }
    return taskMeta->UpdateState(IndexTaskMeta::ABORTED);
}

bool IndexTaskQueue::Done(const std::string& taskType, const std::string& taskTraceId)
{
    auto taskMeta = Get(taskType, taskTraceId);
    if (taskMeta == nullptr) {
        AUTIL_LOG(ERROR, "abort: '%s %s': No such index task meta", taskType.c_str(), taskTraceId.c_str());
        return false;
    }
    return taskMeta->UpdateState(IndexTaskMeta::DONE);
}

bool IndexTaskQueue::Suspend(const std::string& taskType, const std::string& taskTraceId)
{
    auto taskMeta = Get(taskType, taskTraceId);
    if (taskMeta == nullptr) {
        AUTIL_LOG(ERROR, "suspend: '%s %s': No such index task meta", taskType.c_str(), taskTraceId.c_str());
        return false;
    }
    return taskMeta->UpdateState(IndexTaskMeta::SUSPENDED);
}

bool IndexTaskQueue::Overwrite(IndexTaskMeta newMeta)
{
    size_t idx = 0;
    if (!Find(newMeta.GetTaskType(), newMeta.GetTaskTraceId(), &idx)) {
        AUTIL_LOG(ERROR, "cannot overwrite %s %s: No such index task meta", newMeta.GetTaskType().c_str(),
                  newMeta.GetTaskTraceId().c_str());
        return false;
    }
    auto oldMeta = _indexTasks[idx];
    if (!IndexTaskMeta::ValidateStateTransfer(oldMeta->GetState(), newMeta.GetState())) {
        return false;
    }
    if (newMeta.GetParams().find(PARAM_LAST_SEQUENCE_NUMBER) == newMeta.GetParams().end()) {
        // reuse previous last sequence number to ensure data sequence is right
        auto iter = oldMeta->GetParams().find(PARAM_LAST_SEQUENCE_NUMBER);
        if (iter == oldMeta->GetParams().end()) {
            AUTIL_LOG(WARN, "cannot overwrite %s %s: params '%s' not found", newMeta.GetTaskType().c_str(),
                      newMeta.GetTaskTraceId().c_str(), PARAM_LAST_SEQUENCE_NUMBER);
        } else {
            newMeta.UpdateParam(PARAM_LAST_SEQUENCE_NUMBER, iter->second);
        }
    }
    if (newMeta.GetEventTimeInSecs() == indexlib::INVALID_TIMESTAMP) {
        newMeta.SetEventTimeInSecs(oldMeta->GetEventTimeInSecs());
    }
    newMeta.SetBeginTimeInSecs(autil::TimeUtility::currentTimeInSeconds());
    _indexTasks[idx] = std::make_shared<IndexTaskMeta>(newMeta);
    return true;
}

} // namespace indexlibv2::framework
