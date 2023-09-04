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
#include "swift/tools/local_admin_daemon/LocalAdminDaemon.h"

#include <algorithm>
#include <ctime>
#include <iosfwd>
#include <memory>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/util/FileUtil.h"
#include "swift/config/AdminConfig.h"
#include "swift/config/AuthorizerInfo.h"
#include "swift/util/LocalProcessLauncher.h"

using namespace autil;
using namespace std;
using namespace swift::util;
using namespace swift::config;

namespace swift {
namespace tools {

AUTIL_LOG_SETUP(swift, LocalAdminDaemon);

LocalAdminDaemon::LocalAdminDaemon() {}

LocalAdminDaemon::~LocalAdminDaemon() {}

bool LocalAdminDaemon::init(const string &configPath, const string &workDir, const string &binaryPath) {
    std::unique_ptr<AdminConfig> config(AdminConfig::loadConfig(configPath));
    if (NULL == config) {
        AUTIL_LOG(WARN, "init admin config failed, config path [%s]", configPath.c_str());
        return false;
    }
    _maxStartCount = config->getMaxRestartCountInLocal();
    auto adminCount = config->getAdminCount() > 0 ? config->getAdminCount() : 1;
    string adminBinPath = fslib::util::FileUtil::joinPath(binaryPath, "/usr/local/bin/swift_admin");
    swift::util::LocalProcessLauncher procLauncher;
    for (uint32_t i = 0; i < adminCount; i++) {
        ProcessLauncherParam param;
        string adminWorkDir = fslib::util::FileUtil::joinPath(workDir, "admin_" + StringUtil::toString(i));
        vector<string> args = {"-l",
                               StringUtil::formatString("%s/usr/local/etc/swift/swift_alog.conf", binaryPath.c_str()),
                               "-c",
                               config->getConfigPath(),
                               "-w",
                               adminWorkDir,
                               "-t",
                               "20",
                               "-q",
                               "50",
                               "-i",
                               "3",
                               "--recommendPort"};
        param.args = args;
        param.binPath = adminBinPath;
        param.workDir = adminWorkDir;
        _adminProcessParam.push_back(param);
        pid_t pid = procLauncher.start(adminBinPath, args, {}, adminWorkDir);
        if (pid < 0) {
            AUTIL_LOG(INFO, "start app[%s], workDir[%s] failed", config->getApplicationId().c_str(), workDir.c_str());
            return false;
        } else {
            AUTIL_LOG(INFO, "start admin[%d %d] success, workDir[%s]", i, pid, workDir.c_str());
            _pidVec.push_back(pid);
        }
    }
    if (_adminProcessParam.size() > 0 && _adminProcessParam.size() == _pidVec.size()) {
        AUTIL_LOG(INFO, "start app [%s], workDir[%s] success.", config->getApplicationId().c_str(), workDir.c_str());
        return true;
    }
    return false;
}

bool LocalAdminDaemon::daemonRun() {
    swift::util::LocalProcessLauncher procLauncher;
    for (size_t i = 0; i < _pidVec.size(); ++i) {
        ProcessLauncherParam &param = _adminProcessParam[i];
        auto status = procLauncher.getProcStat(_pidVec[i], param.workDir);
        if (status.isExited) {
            if (_maxStartCount > 0 && _startCount >= _maxStartCount) {
                AUTIL_LOG(WARN,
                          "start process failed, start count [%d] is large than max start count [%d]",
                          _startCount,
                          _maxStartCount);
                return false;
            }
            AUTIL_LOG(WARN, "admin[%lu %d] disappear, restart workDir[%s]", i, _pidVec[i], param.workDir.c_str());
            pid_t pid = procLauncher.start(param.binPath, param.args, param.envs, param.workDir);
            _startCount++;
            if (pid < 0) {
                AUTIL_LOG(INFO, "start admin[%lu] fail, workDir[%s]", i, param.workDir.c_str());
                continue;
            } else {
                AUTIL_LOG(INFO, "start admin[%lu %d] success, workDir[%s]", i, pid, param.workDir.c_str());
                _pidVec[i] = pid;
            }
        }
    }
    return true;
}

} // namespace tools
} // end namespace swift
