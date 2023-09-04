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
#include "swift/tools/admin_starter/LocalHelper.h"

#include <iosfwd>
#include <stdlib.h>
#include <unistd.h>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "swift/config/AdminConfig.h"
#include "swift/util/LocalProcessLauncher.h"

namespace swift {
namespace tools {

using namespace std;
using namespace autil;
using namespace fslib::fs;

AUTIL_LOG_SETUP(swift, LocalHelper);

LocalHelper::LocalHelper() {}

LocalHelper::LocalHelper(const string &binPath, const string &workDir) : _binPath(binPath), _workDir(workDir) {}

LocalHelper::~LocalHelper() {}

bool LocalHelper::startApp() {
    if (_binPath.empty() || _workDir.empty()) {
        AUTIL_LOG(ERROR, "local swift need set binPath and workDir");
        return false;
    }
    const string &swiftDir = FileSystem::joinFilePath(_workDir, _config->getApplicationId());
    if (appExist()) {
        AUTIL_LOG(ERROR, "start failed, app exist. swift dir[%s]", swiftDir.c_str());
        return false;
    }
    AUTIL_LOG(INFO,
              "start app[%s], config path[%s], binPath[%s], workDir[%s], swift[%s]",
              _config->getApplicationId().c_str(),
              _config->getConfigPath().c_str(),
              _binPath.c_str(),
              _workDir.c_str(),
              swiftDir.c_str());

    if (!writeVersionFile()) {
        AUTIL_LOG(ERROR, "write version file fail, start app fail");
        return false;
    }
    return startLocalAdminDaemon(swiftDir);
}

bool LocalHelper::startLocalAdminDaemon(const string &swiftDir) {
    string binPath = fslib::util::FileUtil::joinPath(_binPath, "/usr/local/bin/local_admin_daemon");
    string daemonDir = fslib::util::FileUtil::joinPath(swiftDir, "admin_daemon_dir");
    vector<string> args = {_config->getConfigPath(), swiftDir, _binPath};
    swift::util::LocalProcessLauncher procLauncher;
    pid_t pid = procLauncher.start(binPath, args, {}, daemonDir);
    if (pid < 0) {
        AUTIL_LOG(INFO, "start app[%s], workDir[%s] failed", _config->getApplicationId().c_str(), _workDir.c_str());
        return false;
    }
    return true;
}

bool LocalHelper::stopApp() {
    AUTIL_LOG(INFO, "stop app[%s], workDir[%s]", _config->getApplicationId().c_str(), _workDir.c_str());
    if (_workDir.empty()) {
        AUTIL_LOG(ERROR, "stop app need workDir");
        return false;
    }
    if (!appExist()) {
        return false;
    }
    string killTemp = "ps uxwww | grep %s | grep -E 'swift_broker|swift_admin|local_admin_daemon' | awk -F' ' '{print "
                      "$2}' | xargs -I{} kill -%d {}";
    string killCmd = StringUtil::formatString(killTemp.c_str(), _config->getApplicationId().c_str(), 10);

    pid_t ret = system(killCmd.c_str());
    AUTIL_LOG(INFO, "run stop[%s] ret[%d]", killCmd.c_str(), ret);

    usleep(2000 * 1000);
    string pstackTemp = "ps uxwww | grep %s | grep -E 'swift_broker|swift_admin' | grep -v grep | awk -F' "
                        "' '{print $2}' | xargs -I "
                        "{} pstack {} >> pstack.all";
    string pstackCmd = StringUtil::formatString(pstackTemp.c_str(), _config->getApplicationId().c_str());
    ret = system(pstackCmd.c_str());
    AUTIL_LOG(INFO, "pstack [%s] ret[%d]", pstackCmd.c_str(), ret);

    killCmd = StringUtil::formatString(killTemp.c_str(), _config->getApplicationId().c_str(), 9);
    ret = system(killCmd.c_str());
    AUTIL_LOG(INFO, "run force stop[%s] ret[%d]", killCmd.c_str(), ret);
    return true;
}

bool LocalHelper::updateAdminConf() {
    if (_workDir.empty()) {
        AUTIL_LOG(ERROR, "restart app need workDir");
        return false;
    }
    string killTemp = "ps uxwww | grep %s | grep -E 'swift_admin|local_admin_daemon'| grep -v 'swift_admin_starter' | "
                      "awk -F' ' '{print $2}' "
                      "| xargs -I{} kill -%d {}";
    string killCmd = StringUtil::formatString(killTemp.c_str(), _config->getApplicationId().c_str(), 10);
    pid_t ret = system(killCmd.c_str());
    AUTIL_LOG(INFO, "run stop admin [%s] ret[%d]", killCmd.c_str(), ret);
    usleep(2000 * 1000);
    killCmd = StringUtil::formatString(killTemp.c_str(), _config->getApplicationId().c_str(), 9);
    ret = system(killCmd.c_str());
    AUTIL_LOG(INFO, "run force stop admin [%s] ret[%d]", killCmd.c_str(), ret);
    const string &swiftDir = FileSystem::joinFilePath(_workDir, _config->getApplicationId());
    if (!startLocalAdminDaemon(swiftDir)) {
        AUTIL_LOG(INFO, "update admin conf failed[%s] config[%s]", swiftDir.c_str(), _config->getConfigPath().c_str());
        return false;
    } else {
        AUTIL_LOG(INFO, "update admin conf success[%s] config[%s]", swiftDir.c_str(), _config->getConfigPath().c_str());
        return updateAdminVersion();
    }
}

bool LocalHelper::getAppStatus() {
    AUTIL_LOG(WARN, "local swift getAppStatus not support");
    return false;
}

bool LocalHelper::appExist() {
    string applicationId = _config->getApplicationId();
    string cmd = StringUtil::formatString(
        "ps xuwww | grep %s | grep swift_admin |grep -v grep |awk -F' ' '{printf \"%%s \",$2}'| xargs kill -0",
        applicationId.c_str());
    AUTIL_LOG(INFO, "detecting exist of app[%s]", cmd.c_str());
    pid_t ret = system(cmd.c_str());
    if (ret == -1) {
        AUTIL_LOG(WARN, "System call error");
        return false;
    }
    auto errorCode = WEXITSTATUS(ret);
    if (errorCode) {
        AUTIL_LOG(INFO, "app not exist[%s]", applicationId.c_str());
        return false;
    }
    return true;
}

} // namespace tools
} // namespace swift
