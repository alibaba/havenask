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
#include "sdk/default/CmdExecutor.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/SystemUtil.h"
#include <unistd.h>

using namespace std;
BEGIN_HIPPO_NAMESPACE(sdk);

const string CmdExecutor::SSH_PORT = "SSH_PORT";
const string CmdExecutor::DOCKER_EXE_NAME = "DOCKER_EXE_NAME";
const string CmdExecutor::CMD_EXEC_TIMEOUT = "CMD_EXEC_TIMEOUT";
const string CmdExecutor::CMD_LOCAL_MODE = "CMD_LOCAL_MODE";
const string CmdExecutor::CMD_ONE_CONTAINER = "CMD_ONE_CONTAINER";

HIPPO_LOG_SETUP(sdk, CmdExecutor);

CmdExecutor::CmdExecutor()
    : _dockerExeName("docker")
    , _cmdExecTimeout(30)
{
    _sshPort = autil::EnvUtil::getEnv(SSH_PORT);
    _cmdLocalMode = autil::EnvUtil::getEnv<bool>(CMD_LOCAL_MODE, false);
    _cmdOneContainer = autil::EnvUtil::getEnv<bool>(CMD_ONE_CONTAINER, false);
    string dockerExeName = autil::EnvUtil::getEnv(DOCKER_EXE_NAME);
    if (!dockerExeName.empty()) {
        _dockerExeName = dockerExeName;
    }
    string timeoutStr = autil::EnvUtil::getEnv(CMD_EXEC_TIMEOUT);
    int32_t timeout = 30;
    if (autil::StringUtil::fromString(timeoutStr, timeout)) {
        _cmdExecTimeout = timeout;
    }

    char * pUser = std::getenv("USER");
    if (pUser) {
        _userName = pUser;
    }
    else {
        char *pUser = getlogin();
        if (pUser) {
            _userName = string(pUser);
        }
    }
    HIPPO_LOG(INFO, "run hippp cmd executor as user [%s]", _userName.c_str());
}

CmdExecutor::~CmdExecutor() {
}

bool CmdExecutor::startContainer(const string &address,
                                 const string &containerName,
                                 const string &params,
                                 const string &image,
                                 string &msg)
{
    if (_cmdOneContainer) {
        return true;
    }
    if (checkContainerExist(address, containerName)) {
        return true;
    }
    string startContainerCmd = _dockerExeName + " run " + params + " --name "
                               + containerName + " " + image + " /sbin/init";
    string cmd;
    generateCmd(address, startContainerCmd, cmd);
    if (!execute(cmd, msg)) {
        return false;
    }
    if (!checkContainerExist(address, containerName)) {
        return false;
    }
    return addUser(address, containerName, msg);
}

bool CmdExecutor::addUser(const string &address,
                          const string &containerName,
                          string &msg)
{
    string addUserCmd = _dockerExeName + " exec -t " + containerName +
                        " /bin/bash -c \"cp /home/.passwd /etc/passwd;cp /home/.group /etc/group\"";
    string cmd;
    generateCmd(address, addUserCmd, cmd);
    if (!execute(cmd, msg)) {
        return false;
    }
    return true;
}

bool CmdExecutor::checkContainerExist(const string &address,
                                      const string &containerName)
{
    string checkCmd = _dockerExeName + " ps --format {{.Names}} | grep ^" + containerName + "$";
    string cmd;
    generateCmd(address, checkCmd, cmd);
    string msg;
    if (!execute(cmd, msg)) {
        return false;
    }
    autil::StringUtil::trim(msg);
    return !msg.empty();
}

void CmdExecutor::stopContainer(const string &address,
                                const string &containerName)
{
    if (_cmdOneContainer) {
        return;
    }
    string stopContainerCmd = _dockerExeName + " rm -f " + containerName;
    string cmd;
    generateCmd(address, stopContainerCmd, cmd);
    string msg;
    execute(cmd, msg);
}

bool CmdExecutor::startProcess(const string &address,
                               const string &containerName,
                               const string &processCmd,
                               const string &workDir,
                               const string &processName,
                               string &errorMsg)
{
    string cmd;
    string mkdirCmd = "mkdir -p " + workDir;
    generateCmd(address, mkdirCmd, cmd);
    if (!execute(cmd, errorMsg)) {
        return false;
    }
    const string FILE_PROCESS_STARTER = "process_starter.sh";
    string generateStartFileCmd = "cd " + workDir + "&& echo \"" + processCmd
                                  + "\">" + FILE_PROCESS_STARTER;
    if (!containerName.empty()) {
        string sshme = "docker exec --user " + _userName +  " -it " + containerName + " bash";
        generateStartFileCmd += " && echo \"" + sshme + "\" > sshme";
    }
    generateCmd(address, generateStartFileCmd, cmd);
    if (!execute(cmd, errorMsg)) {
        return false;
    }

    string startProcessCmd = _dockerExeName + " exec";
    if (!_userName.empty())  {
        startProcessCmd += " -u " + _userName;
    }
    if (_cmdOneContainer) {
        startProcessCmd = "cd " + workDir + "&&sh " + FILE_PROCESS_STARTER;
    } else {
        startProcessCmd += " -t " + containerName
                           + " /bin/bash -c \"cd " + workDir + "&&sh " + FILE_PROCESS_STARTER
                           + "\"";
    }
    generateCmd(address, startProcessCmd, cmd);
    if (!execute(cmd, errorMsg)) {
        return false;
    }
    HIPPO_LOG(INFO, "start process, cmd[%s] msg[%s]", startProcessCmd.c_str(), errorMsg.c_str());
    return errorMsg.empty();
}

bool CmdExecutor::stopProcess(const string &address,
                              const string &containerName,
                              const int32_t pid)
{
    if (pid < 1) {
        return false;
    }
    string pidStr = to_string(pid);
    string killProcesCmd = _dockerExeName + " exec";
    if (!_userName.empty()) {
        killProcesCmd += " -u " + _userName;
    }
    if (_cmdOneContainer) {
        killProcesCmd ="kill -10 " + pidStr;
    } else {
        killProcesCmd += " -t " + containerName + " /bin/bash -c \"kill -10 " + pidStr + "\"";
    }
    string cmd;
    generateCmd(address, killProcesCmd, cmd);
    string msg;
    if (!execute(cmd, msg)) {
        return false;
    }
    autil::StringUtil::trim(msg);
    HIPPO_LOG(INFO, "stop process, cmd[%s] msg[%s]", killProcesCmd.c_str(), msg.c_str());
    return !msg.empty();
}

bool CmdExecutor::checkProcessExist(const string &address,
                                    const string &containerName,
                                    const string &processName,
                                    const string &workDir,
                                    int32_t &pid)
{
    pid = -1;
    string cmd;
    string checkProcesCmd = _dockerExeName + " exec";
    if (!_userName.empty()) {
        checkProcesCmd += " -u " + _userName;
    }
    if (_cmdOneContainer) {
        checkProcesCmd = "pgrep " + processName.substr(0, 15) + "|xargs pwdx|grep " + workDir  + "|cut -d: -f1";
    } else {
        checkProcesCmd += " -t " + containerName + " /bin/bash -c \"" + "pgrep " + processName.substr(0, 15) + "|xargs pwdx|grep " + workDir  + "|cut -d: -f1|tr -d \\\"\r\n\\\"" + "\"";
    }
    generateCmd(address, checkProcesCmd, cmd);
    string msg;
    if (!execute(cmd, msg)) {
        return false;
    }
    autil::StringUtil::trim(msg);
    if (!autil::StringUtil::fromString(msg, pid)) {
        HIPPO_LOG(ERROR, "get pid from msg[%s] failed", msg.c_str());
        return false;
    }
    HIPPO_LOG(INFO, "check process, cmd[%s] msg[%s]", checkProcesCmd.c_str(), msg.c_str());
    return !msg.empty();
}

bool CmdExecutor::execute(const string &cmd, string &msg) {
    HIPPO_LOG(INFO, "begin execute cmd:%s", cmd.c_str());
    int ret = autil::SystemUtil::call(cmd, &msg, _cmdExecTimeout, true);
    if (ret < 0) {
        HIPPO_LOG(ERROR, "exec command:[%s] failed, out[%s], code[%d]",
                  cmd.c_str(), msg.c_str(), WEXITSTATUS(ret));
        return false;
    }
    HIPPO_LOG(INFO, "exec command:[%s] success, out[%s], code[%d]",
              cmd.c_str(), msg.c_str(), WEXITSTATUS(ret));
    return true;
}

void CmdExecutor::generateCmd(const string &address,
                              const string &containerCmd,
                              string &cmd)
{
    if (_cmdLocalMode) {
        cmd = "sh -c '" + containerCmd + " 2>&1'";
        return;
    }
    cmd.clear();
    cmd = "ssh -o PasswordAuthentication=no -o StrictHostKeyChecking=no" ;
    if (!_sshPort.empty()) {
        cmd += " -p " + _sshPort;
    }
    cmd += " " + address + " '" + containerCmd + " 2>&1'";
}

END_HIPPO_NAMESPACE(sdk);
