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
#pragma once

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::framework {

class TaskConfigMeta : public autil::legacy::Jsonizable
{
public:
    TaskConfigMeta() : _effectiveTimeInSec(-1) {}

    TaskConfigMeta(const std::string& taskName, const std::string& taskType, int64_t effectiveTimestampInSecond)
        : _taskName(taskName)
        , _taskType(taskType)
        , _effectiveTimeInSec(effectiveTimestampInSecond)
    {
    }

    const std::string& GetTaskName() const { return _taskName; }
    const std::string& GetTaskType() const { return _taskType; }
    int64_t GetEffectiveTimeInSecond() const { return _effectiveTimeInSec; }

    void Jsonize(JsonWrapper& json) override
    {
        json.Jsonize("task_name", _taskName);
        json.Jsonize("task_type", _taskType);
        json.Jsonize("effective_timestamp", _effectiveTimeInSec);
    }

private:
    std::string _taskName;
    std::string _taskType;
    int64_t _effectiveTimeInSec;
};

class IndexTaskLog : public autil::legacy::Jsonizable
{
public:
    IndexTaskLog() = default;
    IndexTaskLog(const std::string& taskId, versionid_t baseVersion, versionid_t targetVersion,
                 int64_t triggerTsInSecond, const std::map<std::string, std::string>& taskDesc)
        : _taskId(taskId)
        , _baseVersion(baseVersion)
        , _targetVersion(targetVersion)
        , _triggerTimestampInSec(triggerTsInSecond)
        , _taskDescription(taskDesc)
    {
    }

    void Jsonize(JsonWrapper& json) override
    {
        json.Jsonize("task_id", _taskId);
        json.Jsonize("base_version", _baseVersion);
        json.Jsonize("target_version", _targetVersion);
        json.Jsonize("trigger_timestamp", _triggerTimestampInSec);
        json.Jsonize("task_description", _taskDescription);
    }

    const std::string& GetTaskId() const { return _taskId; }
    versionid_t GetBaseVersion() const { return _baseVersion; }
    int64_t GetTriggerTimestampInSecond() const { return _triggerTimestampInSec; }
    const std::map<std::string, std::string>& GetTaskDescription() const { return _taskDescription; }
    void AddTaskDescItem(const std::string& key, const std::string& value) { _taskDescription[key] = value; }

private:
    std::string _taskId;
    versionid_t _baseVersion = -1;
    versionid_t _targetVersion = -1;
    int64_t _triggerTimestampInSec = -1;
    std::map<std::string, std::string> _taskDescription;
};

////////////////////////////////////////////////////////////////////////////////////////////////
class IndexTaskHistory : public autil::legacy::Jsonizable
{
public:
    IndexTaskHistory();
    ~IndexTaskHistory();

public:
    // taskType example : alter_table / merge@_default_ / merge_@_inc_optimize / reclaim_index ...
    // replace item when same task_id exist, otherwise add new log item
    void AddLog(const std::string& taskType, const std::shared_ptr<IndexTaskLog>& item);

    // return task log vector, from latest to old task order by trigger timestamp
    const std::vector<std::shared_ptr<IndexTaskLog>>& GetTaskLogs(const std::string& taskType) const;

    IndexTaskHistory Clone() const;

    std::string ToString() const;
    bool FromString(const std::string& str);
    void Jsonize(JsonWrapper& json) override;

    // hint: it should be the full merge task
    // because only one log item exist, that make it never be erased
    int64_t GetOldestTaskLogTimestamp() const;

    std::vector<std::string> GetAllTaskTypes() const;

    bool DeclareTaskConfig(const std::string& taskName, const std::string& taskType);
    bool FindTaskConfigEffectiveTimestamp(const std::string& taskName, const std::string& taskType,
                                          int64_t& timestamp) const;

public:
    std::shared_ptr<IndexTaskLog> TEST_GetTaskLog(const std::string& taskType, const std::string& taskId) const;

private:
    void InnerAddLog(const std::string& taskType, const std::shared_ptr<IndexTaskLog>& item);

private:
    typedef std::shared_ptr<IndexTaskLog> IndexTaskLogPtr;
    std::map<std::string, std::vector<IndexTaskLogPtr>> _taskLogMap; // key: taskType
    size_t _maxReserveLogSize = 10;
    std::vector<TaskConfigMeta> _taskConfigMetas;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
