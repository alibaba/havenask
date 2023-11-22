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

#include <map>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

extern const std::string BS_TASK_CONFIG_FILE_PATH;

class TaskTarget : public autil::legacy::Jsonizable
{
public:
    TaskTarget();
    TaskTarget(const std::string& targetName);
    TaskTarget(const TaskTarget&);
    ~TaskTarget();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    const std::string& getTargetName() const { return _targetName; }
    void setTargetName(const std::string& targetName) { _targetName = targetName; }
    const KeyValueMap& getTargetDescription() const { return _description; }
    bool operator==(const TaskTarget& other) const;
    bool operator!=(const TaskTarget& other) const { return !(*this == other); }
    int32_t getPartitionCount() const { return _partitionCount; }
    int32_t getParallelNum() const { return _parallelNum; }
    void setPartitionCount(int32_t partitionCount) { _partitionCount = partitionCount; }
    void setParallelNum(int32_t parallelNum) { _parallelNum = parallelNum; }
    bool getTaskConfigPath(std::string& taskConfigPath) const
    {
        return getTargetDescription(BS_TASK_CONFIG_FILE_PATH, taskConfigPath);
    }
    void setTargetTimestamp(int64_t targetTimestamp) { _targetTimestamp = targetTimestamp; }
    int64_t getTargetTimestamp() const { return _targetTimestamp; }

    void addTaskConfigPath(const std::string& taskConfigPath)
    {
        _description.insert(make_pair(BS_TASK_CONFIG_FILE_PATH, taskConfigPath));
    }
    void addTargetDescription(const std::string& key, const std::string& value)
    {
        _description.insert(make_pair(key, value));
    }

    template <typename T>
    bool getTargetDescription(const std::string& key, T& value) const
    {
        std::string valueStr;
        auto iter = _description.find(key);
        if (iter == _description.end()) {
            return false;
        }
        valueStr = iter->second;
        if (!autil::StringUtil::fromString(valueStr, value)) {
            return false;
        }
        return true;
    }
    void updateTargetDescription(const std::string& key, const std::string& value) { _description[key] = value; }

    void removeTargetDescription(const std::string& key)
    {
        auto iter = _description.find(key);
        if (iter != _description.end()) {
            _description.erase(iter);
        }
    }
    bool hasTargetDescription(const std::string& key) const { return _description.find(key) != _description.end(); }

private:
    std::string _targetName;
    int32_t _partitionCount;
    int32_t _parallelNum;
    KeyValueMap _description;
    int64_t _targetTimestamp;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskTarget);

}} // namespace build_service::config
