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
#include "build_service_tasks/script_task/ScriptExecutor.h"

#include <stdlib.h>
#include <unistd.h>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/util/FileUtil.h"

extern char** environ;

using namespace std;
using namespace autil;
using namespace build_service::util;
using namespace build_service::config;

namespace build_service { namespace task_base {

BS_LOG_SETUP(task_base, ScriptExecutor);

ScriptExecutor::ScriptExecutor() : _isHanged(false), _retryTimes(-1), _lastTs(-1), _beginTs(-1) {}

ScriptExecutor::~ScriptExecutor() {}

bool ScriptExecutor::init(const string& workDir)
{
    if (!checkShellEnvironment()) {
        return false;
    }

    if (!makeScriptLogDir(workDir)) {
        return false;
    }

    string binaryPath = getBinaryPath();
    // _runScriptFile = fslib::util::FileUtil::joinFilePath(binaryPath, "run_task_script");

    // bool exist = false;
    // if (!fslib::util::FileUtil::isExist(_runScriptFile, exist) || !exist) {
    //     BS_LOG(ERROR, "check [%s] existance fail!", _runScriptFile.c_str());
    //     return false;
    // }
    _runScriptFile = "run_task_script";
    // binaryPath has '=', /bin/env will recongnize the bianryPath is a param, exec error

    // discard some env parameters
    char** p = environ;
    while (*p != NULL) {
        string envStr = *p;
        auto cur = envStr.find("=");
        if (cur == string::npos || cur == 0 || cur == (envStr.size() - 1)) {
            p++;
            continue;
        }
        string key = envStr.substr(0, cur);
        string value = envStr.substr(cur + 1);
        if (value.find("\n") != string::npos || value.find(" ") != string::npos || value.find("*") != string::npos ||
            value.find(";") != string::npos || value.find("|") != string::npos) {
            BS_LOG(INFO, "ignore env [%s=%s]", key.c_str(), value.c_str());
            p++;
            continue;
        }
        _envParam[key] = value;
        p++;
    }

    addEnv(_envParam, "PATH", binaryPath);
    _isHanged = false;
    _retryTimes = -1;
    _lastTs = -1;
    return true;
}

bool ScriptExecutor::runScript(const string& scriptRoot, const string& runScriptStr, string* errorMsg,
                               proto::ErrorAdvice* advice)
{
    assert(errorMsg);
    if (_isHanged) {
        // do nothing, no exit, not finished
        return false;
    }
    // will use run_task_script (file) with prepared env parameters to run script

    ScriptTaskConfig scriptConf;
    try {
        FromJsonString(scriptConf, runScriptStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        *errorMsg = "parse ScriptTaskConfig from [" + runScriptStr + "] failed, exception[" + e.what() + "]";
        *advice = proto::BS_STOP;
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }

    if (scriptConf.scriptType != ScriptTaskConfig::PYTHON_SCRIPT_TYPE &&
        scriptConf.scriptType != ScriptTaskConfig::SHELL_SCRIPT_TYPE) {
        *errorMsg = "invalid script type [" + scriptConf.scriptType + "]";
        *advice = proto::BS_STOP;
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }

    if (_retryTimes >= scriptConf.maxRetryTimes) {
        BS_LOG(INFO, "reach maxRetryTimes [%d], no need to run script", scriptConf.maxRetryTimes);
        return true;
    }

    bool exist = false;
    if (!fslib::util::FileUtil::isDir(scriptRoot, exist) || !exist) {
        *errorMsg = "check script root [" + scriptRoot + "] existance fail!";
        *advice = proto::BS_STOP;
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }

    string scriptPath = fslib::util::FileUtil::joinFilePath(scriptRoot, scriptConf.scriptFile);
    if (!fslib::util::FileUtil::isFile(scriptPath, exist) || !exist) {
        *errorMsg = "check target script file [" + scriptPath + "] existance fail!";
        *advice = proto::BS_STOP;
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }

    KeyValueMap envParams;
    if (!prepareEnvironment(scriptRoot, scriptConf, envParams)) {
        *errorMsg = "prepare environment fail!";
        *advice = proto::BS_STOP;
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }

    if (_lastTs > 0 && TimeUtility::currentTimeInSeconds() < _lastTs + scriptConf.minRetryInterval) {
        BS_LOG(INFO, "not reach min retry interval [%d seconds]", scriptConf.minRetryInterval);
        return false;
    }

    ++_retryTimes;
    string cmdString = makeCmdString(scriptRoot, envParams, scriptConf);
    cout << "##################################################################" << endl;
    cout << "retry [" << _retryTimes << "], run script cmd [" << cmdString << "]" << endl;
    BS_LOG(INFO, "retry [%ld], run script cmd [%s]", (int64_t)_retryTimes, cmdString.c_str());

    _beginTs = TimeUtility::currentTimeInSeconds();
    int ret = system(cmdString.c_str());
    bool success = ret == 0 ? true : false;
    _beginTs = -1;

    cout << "------------------------------------------------------------------" << endl;
    cout << "run script cmd finish with return status [" << ret << "], retryTimes [" << _retryTimes << "]" << endl;
    BS_LOG(INFO, "run script cmd finish with return status [%d], retryTimes [%ld]", ret, (int64_t)_retryTimes);
    _lastTs = TimeUtility::currentTimeInSeconds();

    removeObsoleteRetryLog();
    if (success) {
        return !scriptConf.forceRetry /* finished */;
    }

    assert(!success);
    // built-in script call user script
    // if built-in script failed, error code in [0, 255]
    // otherwise, user script failed, "error code" = "user error code" * 256
    if (ret >= 256) {
        *errorMsg = "script run failed, user script called but failed, "
                    "user error code [" +
                    to_string(ret / 256) + "], " + "retryTimes [" + to_string(_retryTimes) + "], " + "maxRetryTimes [" +
                    to_string(scriptConf.maxRetryTimes) + "]";
    } else {
        *errorMsg = "script run failed, built-in error code, [" + to_string(ret) + "], " + "retryTimes [" +
                    to_string(_retryTimes) + "], " + "maxRetryTimes [" + to_string(scriptConf.maxRetryTimes) + "]";
    }
    *advice = proto::BS_RETRY;
    BS_LOG(ERROR, "%s", errorMsg->c_str());

    if (_retryTimes >= scriptConf.maxRetryTimes) {
        if (scriptConf.actionWhenFailed == ScriptTaskConfig::EXIT_ACTION_WHEN_FAILED) {
            BS_LOG(INFO, "exec script error, exit!");
            exit(0);
        }

        if (scriptConf.actionWhenFailed == ScriptTaskConfig::HANG_ACTION_WHEN_FAILED) {
            BS_LOG(ERROR, "exec script error, hang!");
            *advice = proto::BS_FATAL_ERROR;
            _isHanged = true;
            return false;
        }

        if (scriptConf.actionWhenFailed != ScriptTaskConfig::FINISH_ACTION_WHEN_FAILED) {
            BS_LOG(ERROR, "exec script error with invaild actionWhenFailed [%s], maybe admin/worker is not compatible",
                   scriptConf.actionWhenFailed.c_str());
            *advice = proto::BS_FATAL_ERROR;
            _isHanged = true;
            return false;
        }

        assert(scriptConf.actionWhenFailed == ScriptTaskConfig::FINISH_ACTION_WHEN_FAILED);
        BS_LOG(INFO, "reach maxRetryTimes [%d], no need to run script", scriptConf.maxRetryTimes);
        return true;
    }
    return false;
}

bool ScriptExecutor::prepareEnvironment(const string& scriptRoot, const ScriptTaskConfig& conf, KeyValueMap& envParams)
{
    envParams = _envParam;
    addEnv(envParams, "PATH", scriptRoot);
    addEnv(envParams, "LD_LIBRARY_PATH", scriptRoot);
    if (conf.scriptType == ScriptTaskConfig::PYTHON_SCRIPT_TYPE) {
        addEnv(envParams, "PYTHONPATH", scriptRoot);
    }

    for (const auto& res : conf.resources) {
        string absPath = fslib::util::FileUtil::joinFilePath(scriptRoot, res.path);
        if (res.type == ScriptTaskConfig::BINARY_RESOURCE_TYPE) {
            addEnvFromResourcePath(envParams, "PATH", absPath);
            continue;
        }
        if (res.type == ScriptTaskConfig::LIBRARY_RESOURCE_TYPE) {
            addEnvFromResourcePath(envParams, "LD_LIBRARY_PATH", absPath);
            continue;
        }
        BS_LOG(ERROR, "invalid resource type [%s]", res.type.c_str());
        return false;
    }
    return true;
}

void ScriptExecutor::addEnvFromResourcePath(KeyValueMap& map, const string& key, const string& resPath)
{
    string targetPath = resPath;
    bool exist = false;
    if (fslib::util::FileUtil::isFile(resPath, exist) && exist) {
        targetPath = fslib::util::FileUtil::getParentDir(resPath);
    }
    addEnv(map, key, targetPath);
}

void ScriptExecutor::addEnv(KeyValueMap& map, const string& key, const string& path)
{
    string envValue = map[key];
    vector<string> paths;
    StringUtil::fromString(envValue, paths, ":");
    if (find(paths.begin(), paths.end(), path) == paths.end()) {
        BS_LOG(INFO, "add path [%s] to env [%s]", path.c_str(), key.c_str());
        paths.insert(paths.begin(), path);
    } else {
        BS_LOG(INFO, "path [%s] already exist in env [%s]", path.c_str(), key.c_str());
    }
    map[key] = StringUtil::toString(paths, ":");
}

string ScriptExecutor::makeCmdString(const string& scriptRoot, const KeyValueMap& envParam,
                                     const ScriptTaskConfig& conf)
{
    vector<string> tmp = {"/bin/env"};
    for (const auto& item : envParam) {
        tmp.push_back(string(item.first) + "=" + string(item.second));
    }
    tmp.push_back(_runScriptFile);
    tmp.push_back(StringUtil::toString(getpid()));
    tmp.push_back(conf.scriptType);

    string scriptPath = fslib::util::FileUtil::joinFilePath(scriptRoot, conf.scriptFile);
    tmp.push_back(scriptPath);
    tmp.push_back(conf.arguments);
    if (!_logPath.empty()) {
        string logFile = fslib::util::FileUtil::joinFilePath(_logPath, string("run_script.log.") +
                                                                           StringUtil::toString(_retryTimes));
        string addLogCmd = string(" >> ") + logFile;
        tmp.push_back(addLogCmd);
    }
    return StringUtil::toString(tmp, " ");
}

string ScriptExecutor::getBinaryPath()
{
    char buffer[1024];
    int ret = readlink("/proc/self/exe", buffer, sizeof(buffer) - 1);
    return fslib::util::FileUtil::getParentDir(string(buffer, ret));
}

bool ScriptExecutor::checkShellEnvironment()
{
    bool exist = false;
    if (!fslib::util::FileUtil::isExist("/bin/env", exist) || !exist) {
        BS_LOG(ERROR, "check /bin/env existance fail in current host!");
        return false;
    }
    if (!fslib::util::FileUtil::isExist("/bin/sh", exist) || !exist) {
        BS_LOG(ERROR, "check /bin/sh existance fail in current host!");
        return false;
    }
    return true;
}

bool ScriptExecutor::makeScriptLogDir(const string& workDir)
{
    if (workDir.empty()) {
        BS_LOG(INFO, "no need make script log dir.");
        _logPath = "";
        return true;
    }

    _logPath = fslib::util::FileUtil::joinFilePath(workDir, "script_logs");
    bool exist = false;
    if (!fslib::util::FileUtil::isDir(_logPath, exist)) {
        return false;
    }
    if (!exist && !fslib::util::FileUtil::mkDir(_logPath, true)) {
        BS_LOG(ERROR, "mkdir for log path [%s] fail!", _logPath.c_str());
        return false;
    }
    return true;
}

void ScriptExecutor::setEnv(const string& key, const string& value) { _envParam[key] = value; }

void ScriptExecutor::removeObsoleteRetryLog()
{
    if (_logPath.empty()) {
        return;
    }
    if (_retryTimes < MAX_RESERVE_RETRY_LOG_NUM) {
        return;
    }

    int64_t targetRemoveId = _retryTimes - MAX_RESERVE_RETRY_LOG_NUM;
    string logFile =
        fslib::util::FileUtil::joinFilePath(_logPath, string("run_script.log.") + StringUtil::toString(targetRemoveId));
    BS_LOG(INFO, "remove file [%s]", logFile.c_str());
    if (!fslib::util::FileUtil::removeIfExist(logFile)) {
        BS_LOG(WARN, "remove file [%s] fail!", logFile.c_str());
    }
}

}} // namespace build_service::task_base
