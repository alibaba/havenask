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
#include "indexlib/framework/index_task/IndexTaskHistory.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, IndexTaskHistory);

namespace {
const std::string TASK_CONFIG_META_KEY = "_task_config_meta_";
} // namespace

IndexTaskHistory::IndexTaskHistory() {}

IndexTaskHistory::~IndexTaskHistory() {}

void IndexTaskHistory::Jsonize(JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        for (auto& item : _taskLogMap) {
            json.Jsonize(item.first, item.second);
        }
        if (!_taskConfigMetas.empty()) {
            json.Jsonize(TASK_CONFIG_META_KEY, _taskConfigMetas);
        }
    } else {
        autil::legacy::json::JsonMap jsonMap = json.GetMap();
        for (auto& item : jsonMap) {
            if (item.first == TASK_CONFIG_META_KEY) {
                json.Jsonize(TASK_CONFIG_META_KEY, _taskConfigMetas);
                continue;
            }
            std::vector<IndexTaskLogPtr> taskItemVec;
            json.Jsonize(item.first, taskItemVec);
            for (size_t i = 0; i < taskItemVec.size(); i++) {
                InnerAddLog(item.first, taskItemVec[i]);
            }
        }
    }
}

void IndexTaskHistory::AddLog(const std::string& taskType, const std::shared_ptr<IndexTaskLog>& item)
{
    InnerAddLog(taskType, item);
    auto& itemVec = _taskLogMap[taskType];
    std::sort(itemVec.begin(), itemVec.end(),
              [](const std::shared_ptr<IndexTaskLog>& lft, const std::shared_ptr<IndexTaskLog>& rht) {
                  return lft->GetTriggerTimestampInSecond() > rht->GetTriggerTimestampInSecond();
              });
    if (itemVec.size() > _maxReserveLogSize) {
        itemVec.resize(_maxReserveLogSize);
    }
}

void IndexTaskHistory::InnerAddLog(const std::string& taskType, const std::shared_ptr<IndexTaskLog>& item)
{
    auto iter = _taskLogMap.find(taskType);
    if (iter == _taskLogMap.end()) {
        _taskLogMap[taskType] = {item};
        return;
    }

    auto& itemVec = iter->second;
    for (size_t i = 0; i < itemVec.size(); i++) {
        if (itemVec[i]->GetTaskId() == item->GetTaskId()) {
            itemVec[i] = item;
            return;
        }
    }
    itemVec.emplace_back(item);
}

const std::vector<IndexTaskHistory::IndexTaskLogPtr>& IndexTaskHistory::GetTaskLogs(const std::string& taskType) const
{
    auto iter = _taskLogMap.find(taskType);
    if (iter == _taskLogMap.end()) {
        static std::vector<IndexTaskLogPtr> emptyItemVec;
        return emptyItemVec;
    }
    return iter->second;
}

std::shared_ptr<IndexTaskLog> IndexTaskHistory::TEST_GetTaskLog(const std::string& taskType,
                                                                const std::string& taskId) const
{
    auto logs = GetTaskLogs(taskType);
    for (auto& log : logs) {
        if (log->GetTaskId() == taskId) {
            return log;
        }
    }
    return nullptr;
}

IndexTaskHistory IndexTaskHistory::Clone() const
{
    auto str = autil::legacy::ToJsonString(*this);
    IndexTaskHistory newHistory;
    newHistory.FromString(str);
    return newHistory;
}

std::string IndexTaskHistory::ToString() const { return autil::legacy::ToJsonString(*this); }

bool IndexTaskHistory::FromString(const std::string& str)
{
    try {
        autil::legacy::FromJsonString(*this, str);
    } catch (const std::exception& e) {
        assert(false);
        AUTIL_LOG(ERROR, "invalid json str [%s], exception [%s]", str.c_str(), e.what());
        return false;
    }
    return true;
}

int64_t IndexTaskHistory::GetOldestTaskLogTimestamp() const
{
    int64_t oldTimestamp = std::numeric_limits<int64_t>::max();
    for (auto& item : _taskLogMap) {
        auto& logVec = item.second;
        if (!logVec.empty()) {
            oldTimestamp = std::min(oldTimestamp, (*logVec.rbegin())->GetTriggerTimestampInSecond());
        }
    }
    return oldTimestamp != std::numeric_limits<int64_t>::max() ? oldTimestamp : 0;
}

int64_t IndexTaskHistory::GetLatestTaskLogTimestamp() const
{
    int64_t latestTimestamp = 0;
    for (auto& item : _taskLogMap) {
        auto& logVec = item.second;
        if (!logVec.empty()) {
            latestTimestamp = std::max(latestTimestamp, (*logVec.begin())->GetTriggerTimestampInSecond());
        }
    }
    return latestTimestamp;
}

std::vector<std::string> IndexTaskHistory::GetAllTaskTypes() const
{
    std::vector<std::string> ret;
    ret.reserve(_taskLogMap.size());
    for (auto& item : _taskLogMap) {
        ret.push_back(item.first);
    }
    return ret;
}

bool IndexTaskHistory::DeclareTaskConfig(const std::string& taskName, const std::string& taskType)
{
    for (auto& meta : _taskConfigMetas) {
        if (meta.GetTaskName() == taskName && meta.GetTaskType() == taskType) {
            // already exist
            return false;
        }
    }
    TaskConfigMeta meta(taskName, taskType, autil::TimeUtility::currentTimeInSeconds());
    _taskConfigMetas.push_back(meta);
    return true;
}

bool IndexTaskHistory::FindTaskConfigEffectiveTimestamp(const std::string& taskName, const std::string& taskType,
                                                        int64_t& timestamp) const
{
    for (auto& meta : _taskConfigMetas) {
        if (meta.GetTaskName() == taskName && meta.GetTaskType() == taskType) {
            timestamp = meta.GetEffectiveTimeInSecond();
            return true;
        }
    }
    return false;
}

} // namespace indexlibv2::framework
