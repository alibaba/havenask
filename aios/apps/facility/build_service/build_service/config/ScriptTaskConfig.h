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

#include <stdint.h>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class ScriptTaskConfig : public autil::legacy::Jsonizable
{
public:
    struct Resource : public autil::legacy::Jsonizable {
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("type", type);
            json.Jsonize("path", path);
        }
        std::string type;
        std::string path;
    };

public:
    ScriptTaskConfig();
    ~ScriptTaskConfig();

private:
    ScriptTaskConfig(const ScriptTaskConfig&);
    ScriptTaskConfig& operator=(const ScriptTaskConfig&);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    std::string scriptType;
    std::string scriptFile;
    std::vector<Resource> resources;
    std::string arguments;        // the arguments to the user script
    std::string actionWhenFailed; // action when reach max retry times and failed, only if @maxRetryTimes > 0

    int32_t maxRetryTimes;    // max retry times
    int32_t minRetryInterval; // seconds
    bool forceRetry;          // no matter failed or success, just retry
    int32_t partitionCount;
    int32_t parallelNum;

public:
    static std::string PYTHON_SCRIPT_TYPE;
    static std::string SHELL_SCRIPT_TYPE;

    static std::string BINARY_RESOURCE_TYPE;
    static std::string LIBRARY_RESOURCE_TYPE;

    static std::string HANG_ACTION_WHEN_FAILED;   // worker not finished, just hang
    static std::string EXIT_ACTION_WHEN_FAILED;   // worker exit and do task again
    static std::string FINISH_ACTION_WHEN_FAILED; // worker marked finished

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ScriptTaskConfig);

}} // namespace build_service::config
