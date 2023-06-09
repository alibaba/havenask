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
#include "worker_framework/PathUtil.h"

#include <errno.h>
#include <fnmatch.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>
#include "fslib/util/FileUtil.h"
#include "autil/StringTokenizer.h"
using namespace std;
using namespace autil;
namespace worker_framework {


using namespace fslib::util;


AUTIL_LOG_SETUP(worker_framework.common, PathUtil);

#define ZFS_PREFIX "zfs://"
#define ZFS_PREFIX_LEN 6

const std::string PathUtil::LEADER_INFO_DIR = "LeaderElection";
const std::string PathUtil::LEADER_INFO_FILE = "leader_election0000000000";
const std::string PathUtil::LEADER_INFO_FILE_BASE = "leader_election";

PathUtil::PathUtil() {}

PathUtil::~PathUtil() {}

bool PathUtil::getCurrentPath(std::string &path) {
    char cwdPath[PATH_MAX];
    char *ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        AUTIL_LOG(ERROR, "Failed to get current work directory");
        return false;
    }
    path = string(cwdPath);
    if ('/' != *(path.rbegin())) {
        path += "/";
    }
    return true;
}

string PathUtil::getParentDir(const string &currentDir) {
    return FileUtil::getParentDir(currentDir);
}

string PathUtil::joinPath(const string &path, const string &subPath) {
    return FileUtil::joinFilePath(path, subPath);
}

// zkPath: zfs://127.0.0.1:2181/path
string PathUtil::getHostFromZkPath(const std::string &zkPath) {
    return FileUtil::getHostFromZkPath(zkPath);
}

string PathUtil::getPathFromZkPath(const std::string &zkPath) {
    return FileUtil::getPathFromZkPath(zkPath);
}

bool PathUtil::resolveSymbolLink(const string &path, string &realPath) {
    if (path.empty()) {
        AUTIL_LOG(ERROR, "path is empty.");
        return false;
    }

    if (access(path.c_str(), F_OK) != 0) {
        AUTIL_LOG(ERROR, "access path failed. path:[%s], error:[%s]", path.c_str(), strerror(errno));
        return false;
    }

    char buf[PATH_MAX];
    if (realpath(path.c_str(), buf) == NULL) {
        return false;
    }

    realPath = buf;
    return true;
}

bool PathUtil::readLinkRecursivly(string &path) {
    int32_t recLimit = 20;
    while (recLimit-- > 0) {
        if (PathUtil::isLink(path)) {
            string tmpPath = PathUtil::readLink(path);
            if (tmpPath == "") {
                AUTIL_LOG(ERROR, "readlink error. path:[%s].", path.c_str());
                return false;
            }
            path = tmpPath;
        } else {
            break;
        }
    }

    return recLimit > 0;
}

bool PathUtil::isLink(const string &path) {
    struct stat statBuf;
    int ret = lstat(path.c_str(), &statBuf);
    if (ret != 0) {
        AUTIL_LOG(ERROR, "lstat error. path:[%s], error:[%s]", path.c_str(), strerror(errno));
        return false;
    }

    return S_ISLNK(statBuf.st_mode);
}

string PathUtil::readLink(const string &linkPath) {
    char tmpPath[PATH_MAX];
    ssize_t ret = readlink(linkPath.c_str(), tmpPath, PATH_MAX);
    if (ret < 0) {
        return "";
    }
    tmpPath[ret] = '\0';
    return string(tmpPath);
}

bool PathUtil::recursiveMakeLocalDir(const string &localDir) {
    if (localDir.empty() || localDirExist(localDir)) {
        return true;
    }
    string currentDir = localDir;
    string parentDir = getParentDir(currentDir);
    // create parent dir
    if (parentDir != "" && !localDirExist(parentDir)) {
        bool ret = recursiveMakeLocalDir(parentDir);
        if (!ret) {
            AUTIL_LOG(WARN, "create parent dir:%s failed", parentDir.c_str());
            return false;
        }
    }
    // create current dir
    if (mkdir(currentDir.c_str(), S_IRWXU)) {
        AUTIL_LOG(WARN, "mkdir error, dir:[%s], error:%s", currentDir.c_str(), strerror(errno));
        return false;
    }
    return true;
}

bool PathUtil::localDirExist(const string &localDir) {
    struct stat st;
    if (stat(localDir.c_str(), &st) != 0) {
        AUTIL_LOG(WARN, "stat local dir:%s failed, error:%s", localDir.c_str(), strerror(errno));
        return false;
    }
    if (!S_ISDIR(st.st_mode)) {
        AUTIL_LOG(WARN, "%s not a directory", localDir.c_str());
        return false;
    }
    return true;
}

string PathUtil::normalizePath(const string &path) {
    if (path.empty()) {
        return path;
    }

    if (*path.rbegin() != '/') {
        return path + '/';
    }
    return path;
}

int32_t PathUtil::getLinkCount(const std::string &path) {
    struct stat buf;
    int ret = stat(path.c_str(), &buf);
    if (ret != 0) {
        AUTIL_LOG(ERROR, "get link count failed for file:[%s], error:[%s].", path.c_str(), strerror(errno));
        return 0;
    }

    return buf.st_nlink;
}

int32_t PathUtil::getInodeNumber(const std::string &path) {
    struct stat buf;
    int ret = stat(path.c_str(), &buf);
    if (ret != 0) {
        AUTIL_LOG(ERROR, "get link count failed for file:[%s], error:[%s].", path.c_str(), strerror(errno));
        return -1;
    }
    return buf.st_ino;
}

bool PathUtil::matchPattern(const string &path, const string &pattern) {
    int ret = ::fnmatch(pattern.c_str(), path.c_str(), FNM_PATHNAME);
    if (ret == 0) {
        return true;
    } else if (ret == FNM_NOMATCH) {
        return false;
    }

    AUTIL_LOG(ERROR, "match pattern failed. path:[%s], pattern:[%s].", path.c_str(), pattern.c_str());
    return false;
}

string PathUtil::baseName(const string &path) {
    if (path.empty()) {
        return "";
    }
    return basename(&path[0]);
}

std::string PathUtil::getLeaderInfoDir(const std::string &path) {
    return joinPath(path, LEADER_INFO_DIR);
}

std::string PathUtil::getLeaderInfoFile(const std::string &path) {
    return joinPath(path, LEADER_INFO_FILE);
}

}; // namespace worker_framework
