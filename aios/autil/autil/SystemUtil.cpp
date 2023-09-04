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
#include "autil/SystemUtil.h"

#include <errno.h>
#include <stdlib.h>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/util/FileUtil.h"

using namespace autil;
using namespace std;

namespace autil {

AUTIL_LOG_SETUP(autil, SystemUtil);

SystemUtil::SystemUtil() {}

SystemUtil::~SystemUtil() {}

bool SystemUtil::tar(const std::string &archiveFile, const std::string &sourceDir) {
    if (!fslib::util::FileUtil::removeFile(archiveFile)) {
        return false;
    }
    if (!fslib::util::FileUtil::isExist(sourceDir)) {
        return false;
    }
    std::string cmd = "tar -zcf " + archiveFile + " " + sourceDir;
    AUTIL_LOG(INFO, "exec cmd: %s", cmd.c_str());
    int ret = system(cmd.c_str());
    return (0 == ret);
}

bool SystemUtil::unTar(const std::string &archiveFile, const std::string &targetDir) {
    if (!fslib::util::FileUtil::isFileExist(archiveFile)) {
        return false;
    }
    if (!fslib::util::FileUtil::isExist(targetDir)) {
        fslib::util::FileUtil::createPath(targetDir);
    }
    if (!fslib::util::FileUtil::isPathExist(targetDir)) {
        return false;
    }

    std::string cmd = "tar -C " + targetDir + " -xf " + archiveFile;
    AUTIL_LOG(INFO, "exec cmd: %s", cmd.c_str());
    int ret = system(cmd.c_str());
    return (0 == ret);
}

bool SystemUtil::aInst2(const std::string &rpmFile, const std::string &targetDir, const std::string &varDir) {
    if (!fslib::util::FileUtil::isExist(targetDir)) {
        fslib::util::FileUtil::createPath(targetDir);
    }
    if (!fslib::util::FileUtil::isPathExist(targetDir)) {
        return false;
    }

    // remote repo: pkg://IP:PORT/pkg_name
    // remote repo: pkg://pkg_name
    // local repo: /path/pkg_name

    if (rpmFile.empty()) {
        return false;
    }

    std::string outFile = "/dev/null";
    std::string errFile = "/dev/null";
    std::string confArg;
    if (!varDir.empty()) {
        if (!fslib::util::FileUtil::isExist(varDir)) {
            fslib::util::FileUtil::createPath(varDir);
        }

        outFile = fslib::util::FileUtil::joinPath(varDir, "ainst.out");
        errFile = fslib::util::FileUtil::joinPath(varDir, "ainst.err");
        std::string confFile = fslib::util::FileUtil::joinPath(varDir, "ainst.conf");

        std::string fileContent = "[main]\n";
        fileContent += "ainstroot = " + varDir + "\n";
        fileContent += "expiretime = 500\n";
        fileContent += "logfile = " + fslib::util::FileUtil::joinPath(varDir, "ainst.log") + "\n";
        if (!fslib::util::FileUtil::writeFile(confFile, fileContent)) {
            return false;
        }
        confArg = "-c " + confFile;
    }

    std::string pkgName = fslib::util::FileUtil::baseName(rpmFile);

    std::string repoInfo;
    char PKG_HEADER[] = "pkg://";
    if (autil::StringUtil::startsWith(rpmFile, PKG_HEADER)) {
        // remote repo
        // ainst2 can't change remote repo address now
        // just do nothing
    } else if ('/' == rpmFile[0]) {
        // local repo
        repoInfo = "--localrepo=" + fslib::util::FileUtil::parentPath(rpmFile);
    } else {
        // unknown type
        return false;
    }

    std::string cmd = "ainst2 install " + pkgName + " " + repoInfo + " -v --yes --nostart -i " + targetDir + " " +
                      confArg + " >" + outFile + " 2>" + errFile;
    AUTIL_LOG(INFO, "exec cmd: %s", cmd.c_str());
    int ret = system(cmd.c_str());
    AUTIL_LOG(INFO, "return %d", ret);
    return (0 == ret);
}

int32_t SystemUtil::call(const std::string &command, std::string *out, int32_t timeout, bool useTimeout) {
    string actCmd;
    if (useTimeout && command.find("timeout") != 0) {
        actCmd = "timeout " + autil::StringUtil::toString(timeout) + " " + command;
    } else {
        actCmd = command;
    }
    FILE *stream = popen(actCmd.c_str(), "r");
    if (stream == NULL) {
        AUTIL_LOG(ERROR, "execute cmd[%s] failed, errorno[%d], errormsg:[%s]", actCmd.c_str(), errno, strerror(errno));
        return -1;
    }
    out->clear();
    readStream(stream, out, timeout);
    int32_t ret = pclose(stream);
    if (out->length() > 0 && *out->rbegin() == '\n') {
        *out = out->substr(0, out->length() - 1);
    }
    if (WIFEXITED(ret) != 0) {
        return WEXITSTATUS(ret);
    }
    AUTIL_LOG(ERROR, "execute cmd[%s] failed", actCmd.c_str());
    return -1;
}

void SystemUtil::readStream(FILE *stream, string *pattern, int32_t timeout) {
    int64_t expireTime = TimeUtility::currentTime() + timeout * 1000000;
    char buf[256];
    while (TimeUtility::currentTime() < expireTime) {
        size_t len = fread(buf, sizeof(char), 256, stream);
        if (ferror(stream)) {
            AUTIL_LOG(ERROR, "read cmd stream failed. error:[%s]", strerror(errno));
            *pattern = "";
            return;
        }
        *pattern += string(buf, len);
        if (feof(stream)) {
            break;
        }
    }
}

string SystemUtil::getEnv(const string &key) { return autil::EnvUtil::getEnv(key); }

bool SystemUtil::getPidCgroup(pid_t pid, std::string &cgroupPath) {
    string filePath = "/proc/" + autil::StringUtil::toString(pid) + "/cgroup";
    if (!fslib::util::FileUtil::isExist(filePath)) {
        AUTIL_LOG(ERROR, "%s not exist", filePath.c_str());
        return false;
    }
    string cmd = "cat " + filePath + " | grep \"cpu\" | head -n1 | awk -F: '{print $NF}'";
    string out;
    int32_t ret = autil::SystemUtil::call(cmd, &out, 1);
    if (ret != 0) {
        AUTIL_LOG(ERROR, "execute %s failed", cmd.c_str());
        return false;
    }
    if (out.empty()) {
        AUTIL_LOG(ERROR, "cgroup path of %d empty", pid);
        return false;
    }
    cgroupPath = out;
    return true;
}

bool SystemUtil::asyncCall(const string &command, pid_t &pid) {
    pid_t childPid;
    if ((childPid = fork()) == -1) {
        AUTIL_LOG(ERROR, "fork error.");
        return false;
    }
    if (childPid == 0) {
        // child process
        setpgid(childPid, childPid);
        execl("/bin/sh", "/bin/sh", "-c", command.c_str(), NULL);
        exit(0);
    }
    // parent process
    pid = childPid;
    return true;
}

} // namespace autil
