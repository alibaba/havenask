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
#include "sdk/default/ProcessStartWorkItem.h"
#include "util/PathUtil.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/base64.h"

using namespace std;
using namespace autil;
USE_HIPPO_NAMESPACE(util);

BEGIN_HIPPO_NAMESPACE(sdk);

const string ProcessStartWorkItem::CUSTOM_CONTAINER_PARAMS = "CUSTOM_CONTAINER_PARAMS";
const string ProcessStartWorkItem::CUSTOM_PROCESSOR_START_CMD = "custom_processor_start_cmd";
const string ProcessStartWorkItem::CMD_LOCAL_MODE = "CMD_LOCAL_MODE";
HIPPO_LOG_SETUP(sdk, ProcessStartWorkItem);

ProcessStartWorkItem::ProcessStartWorkItem()
    : cmdExecutor(nullptr)
    , launchSignature(-1)
{
    _cmdLocalMode = autil::EnvUtil::getEnv<bool>(CMD_LOCAL_MODE, false);
}

ProcessStartWorkItem::~ProcessStartWorkItem() {
}

bool ProcessStartWorkItem::checkProcessExist(bool &exist) const {
    vector<int32_t> pids;
    return checkProcessExist(exist, pids);
}

bool ProcessStartWorkItem::checkProcessExist(bool &exist, vector<int32_t> &pids) const {
    exist = false;
    if (!cmdExecutor) {
        HIPPO_LOG(ERROR, "cmdExecutor is null", applicationId.c_str());
        return false;
    }
    string container = getContainerName(slotId, applicationId);
    if (processContext.processes_size() == 0) {
        return true;
    }
    bool allExist = true;
    for (int i = 0; i < processContext.processes_size(); ++i) {
        const auto& processInfo = processContext.processes(i);
        string workDir = getProcessWorkDir(processInfo);
        int32_t pid = -1;
        if (cmdExecutor->checkProcessExist(slotId.slaveAddress, container,
                        processInfo.cmd(), workDir, pid))
        {
            pids.push_back(pid);
        } else {
            allExist = false;
        }
    }
    exist = allExist;
    return true;
}

void ProcessStartWorkItem::process() {
    if (!cmdExecutor) {
        HIPPO_LOG(ERROR, "cmdExecutor is null", applicationId.c_str());
        callback(slotId, launchSignature, ERROR_PREPARE_RESOURCE, "cmd executor is null");
        return;
    }
    string image = getImage();
    if (image.empty()) {
        HIPPO_LOG(ERROR, "%s image is empty", applicationId.c_str());
        callback(slotId, launchSignature, ERROR_PREPARE_RESOURCE, "image is not configed.");
        return;
    }
    string container = getContainerName(slotId, applicationId);
    string parameter = getContainerParameter();
    string msg;

    // stop process and container if exist before start
    bool allExist = false;
    vector<int32_t> pids;
    if (!checkProcessExist(allExist, pids)) {
        HIPPO_LOG(ERROR, "check process exist failed.");
    }
    if (pids.size() > 0) {
        bool restart = true;
        if (restart) {
            for (auto pid : pids) {
                cmdExecutor->stopProcess(slotId.slaveAddress, container, pid);
            }
            int32_t count = 20;
            while(count--) {
                if (!checkProcessExist(allExist, pids)) {
                    HIPPO_LOG(ERROR, "check process exist for kill failed.");
                }
                if (pids.size() > 0) {
                    usleep(1000000);
                } else {
                    break;
                }
            }
            cmdExecutor->stopContainer(slotId.slaveAddress, container);
        } else {
            HIPPO_LOG(WARN, "process exist, skip start process.");
            callback(slotId, launchSignature, ERROR_NONE, "");
            return;
        }
    }
    // stop start container and process
    if (!cmdExecutor->startContainer(slotId.slaveAddress, container,
                                 parameter, image, msg))
    {
        HIPPO_LOG(ERROR, "start container for role %s failed, slave:%s",
                  role.c_str(), slotId.slaveAddress.c_str());
        callback(slotId, launchSignature, ERROR_START_CONTAINER, "start container failed, errorMsg:" + msg);
        return;
    }
    for (int i = 0; i < processContext.processes_size(); ++i) {
        const auto& processInfo = processContext.processes(i);
        string workDir = getProcessWorkDir(processInfo);
        string cmd = getProcessCmd(processInfo, workDir);
        msg.clear();
        if (!cmdExecutor->startProcess(slotId.slaveAddress, container,
                        cmd, workDir, processInfo.cmd(), msg))
        {
            HIPPO_LOG(ERROR, "start process for role %s failed, "
                      "process_name:%s slave:%s", role.c_str(),
                      processInfo.processname().c_str(),
                      slotId.slaveAddress.c_str());
            callback(slotId, launchSignature, ERROR_START_PROCESS, "start process failed, errorMsg:" + msg);
            return;
        }
    }
    callback(slotId, launchSignature, ERROR_NONE, "");
}

string ProcessStartWorkItem::getContainerParameter() const {
    string parameter;
    string customParams = getUserDefineParameter();
    if (!customParams.empty()) {
        parameter = customParams;
    }
    string resourceParams = getResourceParameter();
    if (!resourceParams.empty()) {
        parameter += " " + resourceParams;
    }
    string mountParams = getMountParameter();
    if (!mountParams.empty()) {
        parameter += " " + mountParams;
    }
    return parameter;
}

string ProcessStartWorkItem::getUserDefineParameter() const {
    string parameter = EnvUtil::getEnv(CUSTOM_CONTAINER_PARAMS, "");
    return autil::legacy::Base64DecodeFast(parameter);
}

string ProcessStartWorkItem::getResourceParameter() const {
    string parameter;
    for (auto &resource : slotResource.resources) {
        if (resource.name == "cpu") {
            if (!parameter.empty()) {
                parameter += " ";
            }
            parameter += "--cpu-quota=" +
                         StringUtil::toString(resource.amount * 1000) +
                         " --cpu-period=100000";
        } else if (resource.name == "mem") {
            if (!parameter.empty()) {
                parameter += " ";
            }
            parameter += "--memory=" +
                         StringUtil::toString(resource.amount) + "m";
        }
    }
    return parameter;
}

string ProcessStartWorkItem::getMountParameter() const {
    return "-v " + homeDir + ":" + homeDir +
        " -v /etc/passwd:/home/.passwd -v /etc/group:/home/.group";
}

string ProcessStartWorkItem::getImage() const {
    for (int i = 0; i < processContext.requiredpackages_size(); ++i) {
        const auto &packageInfo = processContext.requiredpackages(i);
        if (packageInfo.type() == proto::PackageInfo::IMAGE) {
            return packageInfo.packageuri();
        }
    }
    return "";
}

string ProcessStartWorkItem::getProcessCmd(
        const proto::ProcessInfo &processInfo,
        const string& workDir) const
{
    for (int i = 0; i < processInfo.otherinfos_size(); ++i) {
        auto &otherInfo = processInfo.otherinfos(i);
        if (otherInfo.key() == CUSTOM_PROCESSOR_START_CMD) {
            return otherInfo.value();
        }
    }
    map<string, string> envMap;
    string env;
    for (int i = 0; i < processInfo.envs_size(); ++i) {
        auto &parameter = processInfo.envs(i);
        auto &key = parameter.key();
        auto &value = parameter.value();
        joinProcessParameter(key, value, "=", env);
        envMap[parameter.key()] = parameter.value();
    }
    addSystemEnv(workDir, envMap, env);

    string args;
    for (int i = 0; i < processInfo.args_size(); ++i) {
        auto &parameter = processInfo.args(i);
        auto &key = parameter.key();
        auto &value = parameter.value();
        string newValue;
        if (replaceByEnv(value, envMap, newValue)) {
            joinProcessParameter(key, newValue, " ", args);
        } else {
            joinProcessParameter(key, value, " ", args);
        }
    }

    string cmd;
    if (!env.empty()) {
        if (_cmdLocalMode) {
            cmd += env;
        } else {
            cmd += "/bin/env " + env;
        }
    }

    if (!processInfo.cmd().empty()) {
        cmd += " " + processInfo.cmd();
    }

    if (!args.empty()) {
        cmd += " " + args;
    }
    cmd += " 1>>stdout 2>>stderr";
    if (_cmdLocalMode) {
      cmd += " &";
    }
    return cmd;
}

void ProcessStartWorkItem::joinProcessParameter(const string& key,
        const string& value, const string &sep, string &str) const
{
    if (!str.empty()) {
        str += " ";
    }

    str += key + sep + value;
}

void ProcessStartWorkItem::addSystemEnv(const string &workDir,
                                        map<string, string> &envMap,
                                        string &env) const
{
    if (!env.empty()) {
        env += " ";
    }
    envMap["HIPPO_PROC_WORKDIR"] = workDir;
    env += "HIPPO_PROC_WORKDIR=" + workDir;
}

bool ProcessStartWorkItem::replaceByEnv(const string &oldValue,
                                        const map<string, string> &envMap,
                                        string &newValue) const
{
    if (StringUtil::startsWith(oldValue, "${") &&
        StringUtil::endsWith(oldValue, "}"))
    {
        string key = oldValue.substr(2, oldValue.size() - 3);
        HIPPO_LOG(DEBUG, "replace %s", key.c_str());
        auto it = envMap.find(key);
        if (it != envMap.end()) {
            newValue = it->second;
            return true;
        }
    }
    return false;
}

string ProcessStartWorkItem::getProcessWorkDir(
        const proto::ProcessInfo &processInfo) const
{
    string roleDir = PathUtil::joinPath(homeDir, applicationId + "_" + role);
    string workDir = PathUtil::joinPath(roleDir, processInfo.processname());
    return workDir;
}

string ProcessStartWorkItem::getContainerName(const hippo::SlotId& slotId,
        const string& applicationId)
{
    const static string DOCKER_PREFIX = "havenask_container_" + applicationId + "_";
    return DOCKER_PREFIX + StringUtil::toString(slotId.id);
}

END_HIPPO_NAMESPACE(sdk);
