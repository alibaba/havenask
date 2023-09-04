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
#include <memory>
#include <stdint.h>
#include <string>
#include <sys/types.h>
#include <vector>

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace util {

struct ProcStat {
    bool isExited;
    bool normalExited;
    int32_t exitCode;
    int32_t exitSig;

    ProcStat() {
        isExited = true;
        normalExited = true;
        exitCode = 0;
        exitSig = 0;
    }

    void reset() {
        isExited = true;
        normalExited = true;
        exitCode = 0;
        exitSig = 0;
    }
};

class LocalProcessLauncher {
public:
    enum ExitStatus {
        NOT_EXITED,
        NORMAL_EXITED,
        SIG_EXITED,
        SIG_STOPPED
    };
    typedef std::map<std::string, std::string> KeyValueMap;

public:
    LocalProcessLauncher();
    ~LocalProcessLauncher();

private:
    LocalProcessLauncher(const LocalProcessLauncher &);
    LocalProcessLauncher &operator=(const LocalProcessLauncher &);

public:
    pid_t start(const std::string &path,
                const std::vector<std::string> &args,
                const KeyValueMap &envs,
                const std::string &workDir);

    bool stop(pid_t pid, int32_t sig);

    ProcStat getProcStat(pid_t pid, const std::string &workDir);

    int32_t killProcess(pid_t pid, int32_t signal);

    bool isMyProcess(pid_t pid, const std::string &workDir);

    bool hasPorcessPidFile(pid_t pid);

private:
    bool createWorkDir(const std::string &dir);
    std::shared_ptr<char *> createCmd(const std::string &path, const std::vector<std::string> &args);
    bool setEnv(const KeyValueMap &envs);

    LocalProcessLauncher::ExitStatus parseExitStatus(pid_t pid, int status, int32_t *exitCode, int32_t *exitSig);

    void printArgsAndEnvs(const std::string &path, const std::vector<std::string> &args, const KeyValueMap &envs);

    bool getAllFds(std::vector<int32_t> *fds);
    bool closeFds();
    bool generateProcSignature(pid_t pid, std::string *signature);
    bool getStartTime(pid_t pid, std::string *startTime);
    bool processDirExist(pid_t pid);
    std::string getCwd(pid_t pid);

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(LocalProcessLauncher);

} // end namespace util
} // end namespace swift
