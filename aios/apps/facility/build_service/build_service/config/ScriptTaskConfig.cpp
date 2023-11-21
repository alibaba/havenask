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
#include "build_service/config/ScriptTaskConfig.h"

#include <iosfwd>

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, ScriptTaskConfig);

string ScriptTaskConfig::PYTHON_SCRIPT_TYPE = "python";
string ScriptTaskConfig::SHELL_SCRIPT_TYPE = "shell";

string ScriptTaskConfig::BINARY_RESOURCE_TYPE = "binary";
string ScriptTaskConfig::LIBRARY_RESOURCE_TYPE = "library";

string ScriptTaskConfig::HANG_ACTION_WHEN_FAILED = "hang";
string ScriptTaskConfig::EXIT_ACTION_WHEN_FAILED = "exit";
string ScriptTaskConfig::FINISH_ACTION_WHEN_FAILED = "finish";

ScriptTaskConfig::ScriptTaskConfig()
    : actionWhenFailed(HANG_ACTION_WHEN_FAILED)
    , maxRetryTimes(0)
    , minRetryInterval(0)
    , forceRetry(false)
    , partitionCount(1)
    , parallelNum(1)
{
}

ScriptTaskConfig::~ScriptTaskConfig() {}

void ScriptTaskConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("script_type", scriptType);
    json.Jsonize("script_file", scriptFile);
    json.Jsonize("resources", resources, resources);
    json.Jsonize("arguments", arguments, arguments);
    json.Jsonize("action_when_failed", actionWhenFailed, actionWhenFailed);
    json.Jsonize("max_retry_times", maxRetryTimes, maxRetryTimes);
    json.Jsonize("min_retry_interval", minRetryInterval, minRetryInterval);
    json.Jsonize("force_retry", forceRetry, forceRetry);
    json.Jsonize("partition_count", partitionCount, partitionCount);
    json.Jsonize("parallel_num", parallelNum, parallelNum);
}

}} // namespace build_service::config
