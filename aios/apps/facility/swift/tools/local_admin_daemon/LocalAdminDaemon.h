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
#include <sys/types.h>
#include <vector>

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace tools {

struct ProcessLauncherParam {
    std::string binPath;
    std::vector<std::string> args;
    std::map<std::string, std::string> envs;
    std::string workDir;
};

class LocalAdminDaemon {
public:
    LocalAdminDaemon();
    ~LocalAdminDaemon();

private:
    LocalAdminDaemon(const LocalAdminDaemon &);
    LocalAdminDaemon &operator=(const LocalAdminDaemon &);

public:
    bool init(const std::string &configPath, const std::string &workDir, const std::string &binaryPath);
    bool daemonRun();

private:
    std::vector<ProcessLauncherParam> _adminProcessParam;
    std::vector<pid_t> _pidVec;
    int32_t _maxStartCount = -1;
    int32_t _startCount = 0;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(LocalAdminDaemon);

} // end namespace tools
} // end namespace swift
