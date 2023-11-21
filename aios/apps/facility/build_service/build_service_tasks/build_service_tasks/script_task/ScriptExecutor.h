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

#include <atomic>
#include <stdint.h>
#include <string>

#include "build_service/common_define.h"
#include "build_service/config/ScriptTaskConfig.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Log.h"

namespace build_service { namespace task_base {

class ScriptExecutor
{
public:
    ScriptExecutor();
    virtual ~ScriptExecutor();

private:
    ScriptExecutor(const ScriptExecutor&);
    ScriptExecutor& operator=(const ScriptExecutor&);

public:
    bool init(const std::string& workDir = "");

    bool runScript(const std::string& scriptRoot, const std::string& runScriptStr, std::string* errorMsg,
                   proto::ErrorAdvice* advice);

    int64_t getBeginRunCmdTimeInSecond() const { return _beginTs; }

    void setEnv(const std::string& key, const std::string& value);

    int64_t getRetryTimes() const { return _retryTimes; }

    bool isHanged() const { return _isHanged; }

private:
    bool prepareEnvironment(const std::string& scriptRoot, const config::ScriptTaskConfig& conf,
                            KeyValueMap& envParams);

    void addEnvFromResourcePath(KeyValueMap& map, const std::string& key, const std::string& resPath);

    void addEnv(KeyValueMap& map, const std::string& key, const std::string& path);

    std::string makeCmdString(const std::string& scriptRoot, const KeyValueMap& envParam,
                              const config::ScriptTaskConfig& conf);

    bool checkShellEnvironment();
    bool makeScriptLogDir(const std::string& workDir);

    virtual std::string getBinaryPath();

    void removeObsoleteRetryLog();

private:
    std::string _logPath;
    std::string _runScriptFile;
    KeyValueMap _envParam;
    std::atomic_bool _isHanged;
    std::atomic_int64_t _retryTimes;
    volatile int64_t _lastTs;
    volatile int64_t _beginTs;

    static const int MAX_RESERVE_RETRY_LOG_NUM = 50;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ScriptExecutor);

}} // namespace build_service::task_base
