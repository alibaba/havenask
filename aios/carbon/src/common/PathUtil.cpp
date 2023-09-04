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
#include "common/PathUtil.h"
#include "carbon/Log.h"
#include "autil/StringTokenizer.h"
#include <sys/stat.h>
#include <errno.h>
#include <fnmatch.h>
#include <limits.h>
#include "fslib/fslib.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(common);

CARBON_LOG_SETUP(common, PathUtil);

#define ZFS_PREFIX "zfs://"
#define ZFS_PREFIX_LEN 6 

PathUtil::PathUtil() {
}

PathUtil::~PathUtil() {
}

string PathUtil::getParentDir(const string &currentDir) {
    if (currentDir.empty() || currentDir == "/") {
        return "";
    }
    
    size_t delimPos = string::npos;
    if (*(currentDir.rbegin()) == '/') {
        delimPos = currentDir.rfind('/', currentDir.size() - 2);
    } else {
        delimPos = currentDir.rfind('/');
    }

    if (delimPos == string::npos) {
        return "";
    }
    return currentDir.substr(0, delimPos);
}

bool PathUtil::getCurrentPath(std::string &path) {
    char cwdPath[PATH_MAX];
    char *ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        CARBON_LOG(ERROR, "Failed to get current work directory");
        return false;
    }
    path = string(cwdPath);
    if ('/' != *(path.rbegin())) {
        path += "/";
    }
    return true;
}

string PathUtil::baseName(const string &currentDir) {
    size_t delimPos = string::npos;
    delimPos = currentDir.rfind('/');

    if (delimPos == string::npos) {
        return currentDir;
    }
    return currentDir.substr(delimPos + 1);
}


string PathUtil::joinPath(const string &path, const string &subPath) {
    if (path == "") {
        return subPath;
    }
    if (subPath == "") {
        return path;
    }
    string tmp = path;
    if (*tmp.rbegin() != '/') {
        tmp += "/";
    }
    if (*subPath.begin() == '/') {
        tmp += subPath.substr(1, subPath.length() - 1);
    } else {
        tmp += subPath;
    }
    return tmp;
}

//zkPath: zfs://127.0.0.1:2181/path
string PathUtil::getHostFromZkPath(const std::string &zkPath) {
    if (!autil::StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        CARBON_LOG(ERROR, "zkPath[%s] is not invalid.", zkPath.c_str());
        return "";
    }
    std::string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    return tmpStr.substr(0, tmpStr.find("/"));
}

string PathUtil::getPathFromZkPath(const std::string &zkPath) {
    if (!autil::StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        CARBON_LOG(ERROR, "zkPath[%s] is not invalid.", zkPath.c_str());
        return "";
    }
    std::string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    std::string::size_type pos = tmpStr.find("/");
    if (pos == std::string::npos) {
        return "";
    }
    return tmpStr.substr(pos);
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

bool PathUtil::makeSureDirExist(const std::string &dir) {
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(dir);
    if (ec != fslib::EC_TRUE && ec != fslib::EC_FALSE) {
        CARBON_LOG(ERROR, "check dir exist failed. error:[%s].",
                   fslib::fs::FileSystem::getErrorString(ec).c_str());
        return false;
    }

    if (ec == fslib::EC_TRUE) {
        ec = fslib::fs::FileSystem::isDirectory(dir);
        if (ec != fslib::EC_TRUE) {
            CARBON_LOG(ERROR, "check isdir failed. error:[%s], dir:[%s].",
                       fslib::fs::FileSystem::getErrorString(ec).c_str(),
                       dir.c_str());
            return false;
        }
        
        CARBON_LOG(INFO, "check dir exist success, dir already exist. "
                   "dir:[%s].", dir.c_str());
        return true;
    }

    ec = fslib::fs::FileSystem::mkDir(dir, true);
    if (ec != fslib::EC_OK) {
        CARBON_LOG(ERROR, "mkdir failed. error:[%s], dir:[%s].",
                   fslib::fs::FileSystem::getErrorString(ec).c_str(),
                   dir.c_str());
        return false;
    }

    return true;
}

END_CARBON_NAMESPACE(common);

