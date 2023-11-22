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
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::framework {

enum class Action { ADD = 0, SUSPEND = 1, ABORT = 2, OVERWRITE = 3, UNKNOWN = 128 };

class ActionConvertUtil
{
public:
    static std::string ActionToStr(Action action)
    {
        switch (action) {
        case Action::ADD:
            return "add";
        case Action::SUSPEND:
            return "suspend";
        case Action::ABORT:
            return "abort";
        case Action::OVERWRITE:
            return "overwrite";
        case Action::UNKNOWN:
            return "unknown";
        default:
            return "unknown";
        }
    }
    static Action StrToAction(const std::string& actionStr)
    {
        if (actionStr == "add") {
            return Action::ADD;
        } else if (actionStr == "suspend") {
            return Action::SUSPEND;
        } else if (actionStr == "abort") {
            return Action::ABORT;
        } else if (actionStr == "overwrite") {
            return Action::OVERWRITE;
        } else {
            return Action::UNKNOWN;
        }
    }
};

class IndexTaskMeta : public autil::legacy::Jsonizable
{
public:
    static const std::string PENDING;
    static const std::string SUSPENDED;
    static const std::string ABORTED;
    static const std::string DONE;
    static const std::string UNKNOWN;

    IndexTaskMeta() = default;
    IndexTaskMeta(const std::string& taskType, const std::string& taskTraceId, const std::string& taskName,
                  const std::map<std::string, std::string>& params)
        : _taskType(taskType)
        , _taskTraceId(taskTraceId)
        , _taskName(taskName)
        , _params(params)
    {
    }
    bool operator==(const IndexTaskMeta& other) const
    {
        return _taskType == other._taskType && _taskTraceId == other._taskTraceId && _taskName == other._taskName &&
               _state == other._state && _eventTimeInSecs == other._eventTimeInSecs &&
               _beginTimeInSecs == other._beginTimeInSecs && _endTimeInSecs == other._endTimeInSecs &&
               _committedVersionId == other._committedVersionId && _params == other._params &&
               _comment == other._comment;
    }
    bool operator!=(const IndexTaskMeta& other) const { return !(*this == other); }

    void Jsonize(JsonWrapper& json) override;
    bool UpdateState(const std::string& state);
    void UpdateParam(const std::string& key, const std::string& value) { _params[key] = value; }
    void ClearParams() { _params.clear(); }

    const std::string& GetTaskType() const { return _taskType; }
    const std::string& GetTaskTraceId() const { return _taskTraceId; }
    const std::string& GetTaskName() const { return _taskName; }
    const std::string& GetState() const { return _state; }
    const std::map<std::string, std::string>& GetParams() const { return _params; }
    const std::string& GetComment() const { return _comment; }
    int64_t GetEventTimeInSecs() const { return _eventTimeInSecs; }
    int64_t GetBeginTimeInSecs() const { return _beginTimeInSecs; }
    int64_t GetEndTimeInSecs() const { return _endTimeInSecs; }
    versionid_t GetCommittedVersionId() const { return _committedVersionId; }

    void SetBeginTimeInSecs(int64_t timestamp) { _beginTimeInSecs = timestamp; }
    void SetEndTimeInSecs(int64_t timestamp) { _endTimeInSecs = timestamp; }
    void SetEventTimeInSecs(int64_t timestamp) { _eventTimeInSecs = timestamp; }
    void SetCommittedVersionId(versionid_t versionId) { _committedVersionId = versionId; }

    static bool ValidateStateTransfer(const std::string& srcState, const std::string& dstState);

private:
    std::string _taskType;
    std::string _taskTraceId;
    std::string _taskName;
    std::string _state = PENDING;
    std::map<std::string, std::string> _params;
    int64_t _eventTimeInSecs = indexlib::INVALID_TIMESTAMP;
    int64_t _beginTimeInSecs = INVALID_TIMESTAMP;
    int64_t _endTimeInSecs = INVALID_TIMESTAMP;
    versionid_t _committedVersionId = INVALID_VERSIONID;
    std::string _comment;

    friend class IndexTaskMetaCreator;

    AUTIL_LOG_DECLARE();
};

class IndexTaskMetaCreator
{
public:
    IndexTaskMeta Create()
    {
        auto meta = _meta;
        _meta = IndexTaskMeta();
        return meta;
    }
    IndexTaskMetaCreator& TaskType(const std::string& taskType)
    {
        _meta._taskType = taskType;
        return *this;
    }
    IndexTaskMetaCreator& TaskTraceId(const std::string& taskTraceId)
    {
        _meta._taskTraceId = taskTraceId;
        return *this;
    }
    IndexTaskMetaCreator& TaskName(const std::string& taskName)
    {
        _meta._taskName = taskName;
        return *this;
    }
    IndexTaskMetaCreator& Params(const std::map<std::string, std::string>& params)
    {
        _meta._params = params;
        return *this;
    }
    IndexTaskMetaCreator& EventTimeInSecs(int64_t eventTimeInSecs)
    {
        _meta._eventTimeInSecs = eventTimeInSecs;
        return *this;
    }
    IndexTaskMetaCreator& BeginTimeInSecs(int64_t beginTimeInSecs)
    {
        _meta._beginTimeInSecs = beginTimeInSecs;
        return *this;
    }
    IndexTaskMetaCreator& EndTimeInSecs(int64_t endTimeInSecs)
    {
        _meta._endTimeInSecs = endTimeInSecs;
        return *this;
    }
    IndexTaskMetaCreator& CommittedVersionId(versionid_t committedVersionId)
    {
        _meta._committedVersionId = committedVersionId;
        return *this;
    }
    IndexTaskMetaCreator& Comment(const std::string& comment)
    {
        _meta._comment = comment;
        return *this;
    }
    IndexTaskMetaCreator& State(const std::string& state)
    {
        _meta._state = state;
        return *this;
    }

private:
    IndexTaskMeta _meta;
};

class IndexTaskQueue : public autil::legacy::Jsonizable
{
public:
    void Handle(IndexTaskMeta meta, Action action);

    void Add(IndexTaskMeta meta);
    bool Overwrite(IndexTaskMeta meta);
    bool Suspend(const std::string& taskType, const std::string& taskName);
    bool Abort(const std::string& taskType, const std::string& taskName);
    bool Done(const std::string& taskType, const std::string& taskName);
    std::shared_ptr<IndexTaskMeta> Get(const std::string& taskType, const std::string& taskName) const;
    bool Empty() { return _indexTasks.empty(); }
    size_t Size() { return _indexTasks.size(); }

    void Join(const IndexTaskQueue& other, int64_t currentTimeInSec = autil::TimeUtility::currentTimeInSeconds());

    std::vector<std::shared_ptr<IndexTaskMeta>> GetAllTasks() const { return _indexTasks; }

    void Jsonize(JsonWrapper& json) override { json.Jsonize("index_tasks", _indexTasks, _indexTasks); }

private:
    bool Find(const std::string& taskType, const std::string& taskName, size_t* idx) const;

    std::vector<std::shared_ptr<IndexTaskMeta>> _indexTasks;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
