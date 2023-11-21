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
#ifndef HIPPO_CMDEXECUTOR_H
#define HIPPO_CMDEXECUTOR_H

#include "util/Log.h"
#include "common/common.h"

BEGIN_HIPPO_NAMESPACE(sdk);

class CmdExecutor
{
public:
    CmdExecutor();
    ~CmdExecutor();
private:
    CmdExecutor(const CmdExecutor &);
    CmdExecutor& operator=(const CmdExecutor &);
public:
    bool startContainer(const std::string &address,
                        const std::string &containerName,
                        const std::string &params,
                        const std::string &image,
                        std::string &errorMsg);
    void stopContainer(const std::string &address,
                       const std::string &containerName);
    bool startProcess(const std::string &address,
                      const std::string &containerName,
                      const std::string &processCmd,
                      const std::string &workDir,
                      const std::string &processName,
                      std::string &errorMsg);
    bool stopProcess(const std::string &address,
                     const std::string &containerName,
                     const int32_t pid);
    bool checkProcessExist(const std::string &address,
                           const std::string &containerName,
                           const std::string &processName,
                           const std::string &workDir,
                           int32_t &pid);
private:
    bool checkContainerExist(const std::string &address,
                             const std::string &containerName);
    bool addUser(const std::string &address,
                 const std::string &containerName,
                 std::string &msg);
    bool execute(const std::string &cmd, std::string &msg);
    void generateCmd(const std::string &address,
                     const std::string &containerCmd,
                     std::string &cmd);
private:
    const static std::string SSH_PORT;
    const static std::string DOCKER_EXE_NAME;
    const static std::string CMD_EXEC_TIMEOUT;
    const static std::string CMD_LOCAL_MODE;
    const static std::string CMD_ONE_CONTAINER;
private:
    std::string _sshPort;
    std::string _dockerExeName;
    int32_t _cmdExecTimeout;
    std::string _userName;
    bool _cmdLocalMode;
    bool _cmdOneContainer;
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(CmdExecutor);

END_HIPPO_NAMESPACE(sdk);

#endif  // HIPPO_CMDEXECUTOR_H
