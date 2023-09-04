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
#include "build_service/config/TaskTarget.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, TaskTarget);

const std::string BS_TASK_CONFIG_FILE_PATH = "task_config_file_path";

TaskTarget::TaskTarget() : _partitionCount(0), _parallelNum(0), _targetTimestamp(-1) {}

TaskTarget::TaskTarget(const TaskTarget& other)
    : _targetName(other._targetName)
    , _partitionCount(other._partitionCount)
    , _parallelNum(other._parallelNum)
    , _description(other._description)
    , _targetTimestamp(other._targetTimestamp)
{
}

TaskTarget::TaskTarget(const string& targetName)
    : _targetName(targetName)
    , _partitionCount(0)
    , _parallelNum(0)
    , _targetTimestamp(-1)
{
}

TaskTarget::~TaskTarget() {}

void TaskTarget::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("target_name", _targetName);
    json.Jsonize("partition_count", _partitionCount);
    json.Jsonize("parallel_num", _parallelNum);
    json.Jsonize("target_description", _description, KeyValueMap());
    json.Jsonize("target_timestamp", _targetTimestamp, _targetTimestamp);
}

bool TaskTarget::operator==(const TaskTarget& other) const
{
    return _targetName == other._targetName && _description == other._description &&
           _partitionCount == other._partitionCount && _parallelNum == other._parallelNum &&
           _targetTimestamp == other._targetTimestamp;
}

}} // namespace build_service::config
