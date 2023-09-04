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
#include "util/PathUtil.h"
#include "autil/SystemUtil.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include <sys/stat.h>
#include <errno.h>
#include <fnmatch.h>

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
BEGIN_HIPPO_NAMESPACE(util);

HIPPO_LOG_SETUP(util, PathUtil);

#define ZFS_PREFIX "zfs://"
#define ZFS_PREFIX_LEN 6

PathUtil::PathUtil() {
}

PathUtil::~PathUtil() {
}

bool PathUtil::getCurrentPath(std::string &path) {
    char cwdPath[PATH_MAX];
    char *ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        HIPPO_LOG(ERROR, "Failed to get current work directory, err:%s",
                  strerror(errno));
        return false;
    }
    path = string(cwdPath);
    if ('/' != *(path.rbegin())) {
        path += "/";
    }
    return true;
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
        HIPPO_LOG(ERROR, "zkPath[%s] is not invalid.", zkPath.c_str());
        return "";
    }
    std::string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    return tmpStr.substr(0, tmpStr.find("/"));
}

string PathUtil::getPathFromZkPath(const std::string &zkPath) {
    if (!autil::StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        HIPPO_LOG(ERROR, "zkPath[%s] is not invalid.", zkPath.c_str());
        return "";
    }
    std::string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    std::string::size_type pos = tmpStr.find("/");
    if (pos == std::string::npos) {
        return "";
    }
    return tmpStr.substr(pos);
}

bool PathUtil::resolveSymbolLink(const string &path, string &realPath) {
    if (path.empty()) {
        HIPPO_LOG(ERROR, "path is empty.");
        return false;
    }

    if (access(path.c_str(), F_OK) != 0) {
        HIPPO_LOG(ERROR, "access path failed. path:[%s], error:[%s]",
                  path.c_str(), strerror(errno));
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
                HIPPO_LOG(ERROR, "readlink error. path:[%s].", path.c_str());
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
        HIPPO_LOG(ERROR, "lstat error. path:[%s], error:[%s]",
                  path.c_str(), strerror(errno));
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

bool PathUtil::listLinks(const string &linkDir,
                         vector<string> *linkNames)
{
    linkNames->clear();
    vector<string> fileList;
    fslib::EntryInfoMap entryInfoMap;
    fslib::ErrorCode ec = fslib::fs::FileSystem::listDir(linkDir, entryInfoMap);
    if (fslib::EC_OK != ec) {
        HIPPO_LOG(ERROR, "list linkDir:[%s] fail. error:[%s]",
                  linkDir.c_str(),
                  fs::FileSystem::getErrorString(ec).c_str());
        return false;
    }
    fslib::EntryInfoMap::const_iterator it = entryInfoMap.begin();
    for (; it != entryInfoMap.end(); it++) {
        string filePath = PathUtil::joinPath(linkDir, it->second.entryName);
        if (isLink(filePath)) {
            linkNames->push_back(filePath);
        }
    }
    return true;
}

bool PathUtil::listSubDirs(const string &dir, vector<string> *subDirs) {
    fslib::EntryList entryList;
    fslib::ErrorCode ec = fslib::fs::FileSystem::listDir(dir, entryList);
    if (fslib::EC_OK != ec) {
        HIPPO_LOG(ERROR, "list dir:[%s] fail. error:[%s]",
                  dir.c_str(), fs::FileSystem::getErrorString(ec).c_str());
        return false;
    }
    fslib::EntryList::const_iterator it = entryList.begin();
    for (; it != entryList.end(); it++) {
        if (it->isDir) {
            string path = PathUtil::joinPath(dir, it->entryName);
            subDirs->push_back(path);
        }
    }
    return true;
}

bool PathUtil::listSubFiles(const string &dir, KeyValueMap &subFiles) {
    fslib::EntryList entryList;
    fslib::ErrorCode ec = fslib::fs::FileSystem::listDir(dir, entryList);
    if (fslib::EC_OK != ec) {
        HIPPO_LOG(ERROR, "list dir:[%s] fail. error:[%s]",
                  dir.c_str(), fs::FileSystem::getErrorString(ec).c_str());
        return false;
    }
    auto it = entryList.begin();
    for (; it != entryList.end(); it++) {
        if (it->isDir) {
            continue;
        }
        string filePath = PathUtil::joinPath(dir, it->entryName);
        subFiles[it->entryName] = filePath;
    }
    return true;
}

bool PathUtil::makeSureNotExist(const string &path){
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(path);
    if (ec == fslib::EC_TRUE) {
        if (fslib::EC_OK != fslib::fs::FileSystem::remove(path)) {
            return false;
        }
    } else if (ec != fslib::EC_FALSE) {
        return false;
    }
    return true;
}

bool PathUtil::makeSureNotExistLocal(const string &path){
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(path);
    if (ec == fslib::EC_TRUE) {
        if (0 != remove(path.c_str())) {
            HIPPO_LOG(ERROR, "failed to remove [%s], errorinfo is [%s]",
                      path.c_str(), strerror(errno));
            return false;
        }
    } else if (ec != fslib::EC_FALSE) {
        return false;
    }
    return true;
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
            HIPPO_LOG(WARN, "create parent dir:%s failed", parentDir.c_str());
            return false;
        }
    }
    // create current dir
    if (mkdir(currentDir.c_str(), S_IRWXU)) {
        HIPPO_LOG(WARN, "mkdir error, dir:[%s], error:%s",
                  currentDir.c_str(), strerror(errno));
        return false;
    }
    return true;
}

bool PathUtil::localDirExist(const string &localDir)
{
    struct stat st;
    if (stat(localDir.c_str(), &st) != 0) {
        HIPPO_LOG(WARN, "stat local dir:%s failed, error:%s",
                  localDir.c_str(), strerror(errno));
        return false;
    }
    if (!S_ISDIR(st.st_mode)) {
        HIPPO_LOG(WARN, "%s not a directory", localDir.c_str());
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

bool PathUtil::isSubDir(const string &parent, const string &child) {
    if (parent.empty() || child.empty()) {
        return false;
    }
    string nParent = normalizePath(parent);
    string nChild = normalizePath(child);
    if (nChild.find(nParent) == 0) {
        return true;
    }
    return false;
}

int32_t PathUtil::getLinkCount(const std::string &path) {
    struct stat buf;
    int ret = stat(path.c_str(), &buf);
    if (ret != 0) {
        HIPPO_LOG(ERROR, "get link count failed for file:[%s], error:[%s].",
                  path.c_str(), strerror(errno));
        return 0;
    }

    return buf.st_nlink;
}

int32_t PathUtil::getInodeNumber(const std::string &path) {
    struct stat buf;
    int ret = stat(path.c_str(), &buf);
    if (ret != 0) {
        HIPPO_LOG(ERROR, "get link count failed for file:[%s], error:[%s].",
                  path.c_str(), strerror(errno));
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

    HIPPO_LOG(ERROR, "match pattern failed. path:[%s], pattern:[%s].",
              path.c_str(), pattern.c_str());
    return false;
}

string PathUtil::getVolumePath(const string &appWorkDir, const int32_t slotId) {
    //string volumePath = "volumes_" + StringUtil::toString(slotId);
    return joinPath(appWorkDir, "volumes");
}

string PathUtil::getPersistentVolumePath(const string &appWorkDir) {
    return joinPath(appWorkDir, PERSISTENT_VOLUMES_DIR);
}

bool PathUtil::getDirSizeInByte(const string &dirName, int64_t &size) {
    string cmd = SUPER_CMD + " du -B 1 -s " + dirName;
    string out;
    int32_t ret = autil::SystemUtil::call(cmd, &out);
    if (ret != 0) {
        HIPPO_LOG(ERROR, "get dir [%s] size failed! ret[%d]", dirName.c_str(), ret);
        return false;
    }
    vector<string> res = StringUtil::split(out, "\t");
    if (res.empty()) {
        HIPPO_LOG(ERROR, "split output [%s] failed!", out.c_str());
        return false;
    }
    if (!StringUtil::strToInt64(res.front().c_str(), size)) {
        HIPPO_LOG(ERROR, "get size from string [%s] failed!", res.front().c_str());
        return false;
    }
    return true;
}

bool PathUtil::superChmod(const std::string &dirPath, const std::string &pattern) {
    if (!isPathExist(dirPath)) {
        return false;
    }
    string cmd = SUPER_CMD + " chmod -R " + pattern + " " + dirPath + " > /dev/null 2>&1";
    if (0 != system(cmd.c_str())) {
        HIPPO_LOG(ERROR, "chmod path [%s] to [%s] failed",
                  dirPath.c_str(), pattern.c_str());
        return false;
    }
    return true;
}

bool PathUtil::isPathExist(const std::string &filePath) {
    if (fslib::EC_TRUE == fs::FileSystem::isExist(filePath)) {
        return fslib::EC_TRUE == fs::FileSystem::isDirectory(filePath);
    }
    return false;
}

bool PathUtil::chattr(const std::string &dirPath, string args) {
    if (!isPathExist(dirPath)) {
        return false;
    }
    string cmd = SUPER_CMD + " chattr -R " + args + " " + dirPath + " > /dev/null 2>&1";
    if (0 != system(cmd.c_str())) {
        HIPPO_LOG(ERROR, "chattr path [%s] attr [%s] failed",
                  dirPath.c_str(), args.c_str());
        return false;
    }
    return true;
}

END_HIPPO_NAMESPACE(util);
