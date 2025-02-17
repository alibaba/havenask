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
#include "fslib/util/FileUtil.h"

#include <deque>
#include <fnmatch.h>
#include <fstream>
#include <libgen.h>
#include <linux/limits.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <utime.h>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "fslib/fslib.h"
using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

FSLIB_BEGIN_NAMESPACE(util);

AUTIL_LOG_SETUP(util, FileUtil);

const string FileUtil::ZFS_PREFIX = "zfs://";
const size_t FileUtil::ZFS_PREFIX_LEN = 6;
const string FileUtil::BAK_SUFFIX = ".__bak__";
const string FileUtil::TMP_SUFFIX = ".__tmp__";

string FileUtil::readFile(const string &filePath) {
    string content;
    FileSystem::readFile(filePath, content);
    return content;
}

bool FileUtil::readFile(const string &filePath, string &content) {
    return fslib::EC_OK == FileSystem::readFile(filePath, content);
}

bool FileUtil::readFile(const string &filePath, string &content, bool *isNotExist) {
    auto ec = FileSystem::readFile(filePath, content);
    if (isNotExist) {
        (*isNotExist) = (fslib::EC_NOENT == ec);
    }
    return fslib::EC_OK == ec;
}

bool FileUtil::writeFile(const string &filePath, const string &content) {
    return fslib::EC_OK == FileSystem::writeFile(filePath, content);
}

bool FileUtil::listDir(const string &path, vector<string> &fileList) {
    // workaround for zk, fslib not support list dir by entry meta now
    fslib::ErrorCode ec = FileSystem::listDir(path, fileList);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(WARN,
                  "List dir failed with path:[%s] and error:[%s]",
                  path.c_str(),
                  FileSystem::getErrorString(ec).c_str());
        return false;
    }
    return true;
}

bool FileUtil::listDirWithAbsolutePath(const string &path, vector<string> &entryVec, bool fileOnly) {
    if (!listDir(path, entryVec, fileOnly, false)) {
        return false;
    }
    for (size_t i = 0; i < entryVec.size(); ++i) {
        entryVec[i] = FileUtil::joinFilePath(path, entryVec[i]);
    }
    return true;
}

bool FileUtil::listDirRecursive(const string &path, vector<string> &entryVec, bool fileOnly) {

    return listDir(path, entryVec, fileOnly, true);
}

bool FileUtil::listDirRecursiveWithAbsolutePath(const string &path, vector<string> &entryVec, bool fileOnly) {
    if (!listDir(path, entryVec, fileOnly, true)) {
        return false;
    }

    for (size_t i = 0; i < entryVec.size(); ++i) {
        entryVec[i] = FileUtil::joinFilePath(path, entryVec[i]);
    }
    return true;
}

bool FileUtil::listDir(const string &path, vector<string> &entryVec, bool fileOnly, bool recursive) {
    entryVec.clear();

    deque<string> pathList;
    string relativeDir;
    do {
        EntryList tmpList;
        string absoluteDir = FileUtil::joinFilePath(path, relativeDir);
        fslib::ErrorCode ec = FileSystem::listDir(absoluteDir, tmpList);
        if (ec != fslib::EC_OK) {
            AUTIL_LOG(WARN,
                      "List dir recursive failed with path:[%s] and error:[%s]",
                      absoluteDir.c_str(),
                      FileSystem::getErrorString(ec).c_str());
            return false;
        }
        EntryList::iterator it = tmpList.begin();
        for (; it != tmpList.end(); ++it) {
            string tmpName = FileUtil::joinFilePath(relativeDir, (*it).entryName);
            if ((*it).isDir) {
                if (recursive) {
                    pathList.push_back(tmpName);
                }
                if (!fileOnly) {
                    entryVec.push_back(normalizeDir(tmpName));
                }
            } else {
                entryVec.push_back(tmpName);
            }
        }
        if (!recursive) {
            return true;
        }
        if (pathList.empty()) {
            break;
        }
        relativeDir = pathList.front();
        pathList.pop_front();
    } while (true);

    return true;
}
bool FileUtil::createDir(const string &dstDir) { return FileUtil::createPath(dstDir); }

bool FileUtil::mkDir(const string &dirPath, bool recursive) {
    ErrorCode ec = FileSystem::mkDir(dirPath, recursive);
    if (ec == EC_EXIST && recursive) {
        return true;
    }
    if (ec != EC_OK) {
        AUTIL_LOG(DEBUG,
                  "mkdir failed with path:[%s] and error:[%s]",
                  dirPath.c_str(),
                  FileSystem::getErrorString(ec).c_str());
        return false;
    }
    return true;
}

bool FileUtil::createPath(const string &path) { return FileUtil::mkDir(path, true); }

bool FileUtil::mv(const string &srcPath, const string &dstPath) {
    return fslib::EC_OK == fs::FileSystem::move(srcPath, dstPath);
}

bool FileUtil::remove(const string &path, bool force) {
    if (force) {
        return FileUtil::remove(path);
    }
    return FileUtil::removeIfExist(path);
}

bool FileUtil::removeFile(const string &path, bool force) { return FileUtil::remove(path, force); }

bool FileUtil::removePath(const std::string &filePath) { return FileUtil::removeIfExist(filePath); }

bool FileUtil::remove(const string &path) {
    ErrorCode ec = FileSystem::remove(path);
    if (ec != EC_OK) {
        AUTIL_LOG(
            DEBUG, "remove failed with path:[%s] and error:[%s]", path.c_str(), FileSystem::getErrorString(ec).c_str());
        return false;
    }
    return true;
}

bool FileUtil::removeIfExist(const string &path) {
    ErrorCode ec = FileSystem::isExist(path);
    if (ec != EC_TRUE && ec != EC_FALSE) {
        AUTIL_LOG(WARN,
                  "failed to check whether path:[%s] exist with error:[%s]",
                  path.c_str(),
                  FileSystem::getErrorString(ec).c_str());
        return false;
    }
    if (ec == EC_FALSE) {
        return true;
    }
    return FileUtil::remove(path);
}

bool FileUtil::isDir(const string &filePath, bool &dirExist) {
    fslib::ErrorCode ec = FileSystem::isDirectory(filePath);
    if (ec != EC_TRUE && ec != EC_FALSE && ec != EC_NOENT) {
        AUTIL_LOG(WARN,
                  "failed to check whether path:[%s] is directory with error:[%s]",
                  filePath.c_str(),
                  FileSystem::getErrorString(ec).c_str());
        return false;
    }

    dirExist = (ec == EC_TRUE);
    return true;
}

bool FileUtil::isDir(const string &filePath) {
    bool exist = false;
    return FileUtil::isDir(filePath, exist) && exist;
}

bool FileUtil::isFile(const string &filePath, bool &fileExist) {
    fslib::ErrorCode ec = FileSystem::isFile(filePath);
    if (ec != EC_TRUE && ec != EC_FALSE && ec != EC_NOENT) {
        AUTIL_LOG(WARN,
                  "failed to check whether path:[%s] is file with error:[%s]",
                  filePath.c_str(),
                  FileSystem::getErrorString(ec).c_str());
        return false;
    }
    fileExist = (ec == EC_TRUE);
    return true;
}

bool FileUtil::isFile(const string &filePath) {
    bool exist = false;
    return FileUtil::isFile(filePath, exist) && exist;
}

bool FileUtil::isExist(const string &path, bool &exist) {
    ErrorCode ec = FileSystem::isExist(path);
    if (ec != EC_TRUE && ec != EC_FALSE) {
        AUTIL_LOG(WARN,
                  "failed to check whether path:[%s] exist with error:[%s]",
                  path.c_str(),
                  FileSystem::getErrorString(ec).c_str());
        return false;
    }
    exist = (ec == EC_TRUE);
    return true;
}

bool FileUtil::isExist(const string &path) {
    bool exist = false;
    return FileUtil::isExist(path, exist) && exist;
}

bool FileUtil::isFileExist(const std::string &filePath) {
    if (fslib::EC_TRUE == fs::FileSystem::isExist(filePath)) {
        return fslib::EC_TRUE == fs::FileSystem::isFile(filePath);
    }
    return false;
}

bool FileUtil::isPathExist(const std::string &filePath) {
    if (fslib::EC_TRUE == fs::FileSystem::isExist(filePath)) {
        return fslib::EC_TRUE == fs::FileSystem::isDirectory(filePath);
    }
    return false;
}

bool FileUtil::pathJoin(const string &basePath, const string &fileName, string &fullFileName) {
    size_t basePathLen = basePath.size();
    size_t fileNameLen = fileName.size();
    if (basePathLen == 0 || fileNameLen == 0) {
        return false;
    }
    if (fileName[0] == '/') {
        return false;
    }

    if (basePath[basePathLen - 1] != '/') {
        fullFileName = basePath + "/" + fileName;
    } else {
        fullFileName = basePath + fileName;
    }
    return true;
}

string FileUtil::joinFilePath(const string &path, const string &subPath) {
    return FileSystem::joinFilePath(path, subPath);
}

string FileUtil::joinPath(const string &path, const string &subPath) { return FileUtil::joinFilePath(path, subPath); }

bool FileUtil::normalizeFile(const std::string &srcFileName, std::string &dstFileName) {
    if (srcFileName.length() == 0) {
        return false;
    }
    if (srcFileName[0] == '/') {
        return false;
    }
    size_t pos = srcFileName.length() - 1;
    while (pos > 0) {
        if (srcFileName[pos] != '/') {
            break;
        }
        pos--;
    }
    dstFileName = srcFileName.substr(0, pos + 1);
    return true;
}

string FileUtil::normalizeDir(const string &dir) {
    if (dir.empty()) {
        return dir;
    }
    string normalizedStr = dir;
    FileUtil::normalizePath(normalizedStr);
    if (normalizedStr[normalizedStr.length() - 1] != '/') {
        normalizedStr += "/";
    }

    return normalizedStr;
}

std::string FileUtil::normalizePath(const std::string &input) {
    string path = input;
    size_t pos = path.find(":/");
    if (pos == string::npos) {
        pos = 0;
    } else {
        pos += 2;
    }
    size_t len = path.size();
    if (pos >= len) {
        return path;
    }
    size_t newPos = pos;
    ++pos;
    while (pos < len) {
        if (path[newPos] == '/' && path[pos] == '/') {
            ++pos;
        } else {
            path[++newPos] = path[pos++];
        }
    }
    path.resize(newPos + 1);
    return path;
}

std::string FileUtil::normalizePath(std::string &path) {
    const string constPath = path;
    path = FileUtil::normalizePath(constPath);
    return path;
}

// TODO: ut, move to fslib
void FileUtil::splitFileName(const string &input, string &folder, string &fileName) {
    size_t found;
    string inputTmp = input;
    while (!inputTmp.empty() && inputTmp.back() == '/') {
        inputTmp.pop_back();
    }
    found = inputTmp.find_last_of("/\\");
    if (found == string::npos) {
        fileName = inputTmp;
        return;
    }
    folder = inputTmp.substr(0, found);
    fileName = inputTmp.substr(found + 1);
}

string FileUtil::getHostFromZkPath(const string &zkPath) {
    if (!StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        AUTIL_LOG(WARN, "zkPath:[%s] is invalid", zkPath.c_str());
        return "";
    }
    string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    return tmpStr.substr(0, tmpStr.find("/"));
}

string FileUtil::getPathFromZkPath(const string &zkPath) {
    if (!StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        AUTIL_LOG(WARN, "zkPath:[%s] is invalid", zkPath.c_str());
        return zkPath;
    }
    string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    string::size_type pos = tmpStr.find("/");
    if (pos == string::npos) {
        return "";
    }
    return tmpStr.substr(pos);
}
string FileUtil::parentPath(const string &path) { return FileUtil::getParentDir(path); }
string FileUtil::getParentDir(const string &dir) {
    if (dir.empty()) {
        return "";
    }
    if (dir == "/") {
        return dir;
    }
    size_t delimPos = string::npos;

    if ('/' == *(dir.rbegin())) {
        // the last charactor is '/', then rfind from the next char
        delimPos = dir.rfind('/', dir.size() - 2);
    } else {
        delimPos = dir.rfind('/');
    }

    if (string::npos == delimPos) {
        // not found '/', than parent dir is null
        return "";
    }

    string parentDir = dir.substr(0, delimPos);
    return parentDir;
}

bool FileUtil::getFileLength(const string &filePath, int64_t &fileLength, bool needPrintLog) {
    fslib::FileMeta fileMeta;
    fslib::ErrorCode ec = FileSystem::getFileMeta(filePath, fileMeta);
    if (ec != fslib::EC_OK) {
        if (needPrintLog) {
            AUTIL_LOG(WARN,
                      "getFileMeta failed with path:[%s] and error:[%s]",
                      filePath.c_str(),
                      FileSystem::getErrorString(ec).c_str());
        }
        return false;
    }
    fileLength = fileMeta.fileLength;
    return true;
}

bool FileUtil::readWithBak(const std::string filepath, std::string &content) {
    auto ec = FileSystem::isExist(filepath);
    if (EC_TRUE == ec) {
        if (EC_OK == FileSystem::readFile(filepath, content)) {
            return true;
        }
    } else if (EC_FALSE == ec) {
        auto backFile = filepath + BAK_SUFFIX;
        ec = FileSystem::isExist(backFile);
        if (EC_TRUE == ec) {
            if (EC_OK == FileSystem::readFile(backFile, content)) {
                AUTIL_LOG(INFO, "read from bak [%s]", backFile.c_str());
                return true;
            }
        } else if (EC_FALSE == ec) {
            AUTIL_LOG(INFO, "file and bak %s not exist", backFile.c_str());
            return true;
        }
    }
    return false;
}

bool FileUtil::writeWithBak(const std::string filepath, const std::string &content) {
    auto ec = FileSystem::isExist(filepath);
    if (EC_TRUE == ec) {
        auto backFile = filepath + BAK_SUFFIX;
        if (EC_FALSE != FileSystem::isExist(backFile) && EC_OK != FileSystem::remove(backFile)) {
            return false;
        }
        if (EC_OK != FileSystem::move(filepath, backFile)) {
            return false;
        }
    } else if (EC_FALSE != ec) {
        return false;
    }
    auto tmpFile = filepath + TMP_SUFFIX;
    if (EC_OK != FileSystem::writeFile(tmpFile, content)) {
        return false;
    }
    if (EC_OK != FileSystem::move(tmpFile, filepath)) {
        return false;
    }
    return true;
}

bool FileUtil::atomicCopy(const string &src, const string &file) {
    string tempFilePath = file + TMP_SUFFIX;

    ErrorCode ec = FileSystem::isExist(tempFilePath);
    if (EC_TRUE == ec) {
        AUTIL_LOG(WARN, "temp file[%s] already exist, remove it.", tempFilePath.c_str());
        if (EC_OK != FileSystem::remove(tempFilePath)) {
            AUTIL_LOG(WARN, "Remove temp file[%s] FAILED.", tempFilePath.c_str());
            return false;
        }
    } else if (EC_FALSE != ec) {
        AUTIL_LOG(WARN, "Unknown Error with temp file[%s].", tempFilePath.c_str());
        return false;
    }
    if (FileSystem::copy(src, tempFilePath) != EC_OK) {
        AUTIL_LOG(WARN, "Copy file:[%s] to [%s] FAILED.", src.c_str(), tempFilePath.c_str());
        return false;
    }
    if (FileSystem::rename(tempFilePath, file) != EC_OK) {
        AUTIL_LOG(WARN, "Rename file:[%s] to [%s] FAILED.", tempFilePath.c_str(), file.c_str());
        return false;
    }
    return true;
}

string FileUtil::firstLineFromShell(const std::string &cmd) {
    static constexpr std::size_t MAX_LINE_LENGTH = 1024 * 128; // 128K

    auto f = popen(cmd.data(), "r");
    if (!f) {
        AUTIL_LOG(ERROR, "popen [%s] fail", cmd.data());
        return std::string();
    }
    auto buf = new char[MAX_LINE_LENGTH];
    if (fgets(buf, MAX_LINE_LENGTH - 1, f) == nullptr) {
        AUTIL_LOG(WARN, "fgets return nullptr");
    }
    std::string line(buf, strcspn(buf, "\r\n"));
    delete[] buf;

    // after penable the asan/tcmalloc check, pclose may fail
    // try export LSAN_OPTIONS="exitcode=0"
    auto err = pclose(f);
    if (err) {
        AUTIL_LOG(ERROR, "pclose fail, [%s]", strerror(err));
        return std::string();
    }
    return line;
}

bool FileUtil::changeCurrentPath(const std::string &path) {
    int ret = chdir(path.c_str());
    if (0 != ret) {
        AUTIL_LOG(ERROR,
                  "Failed to change current work directory"
                  "(ERROR CODE: %d)",
                  ret);
        return false;
    }
    return true;
}

std::string FileUtil::baseName(const std::string &path) {

    if (path.empty()) {
        return "";
    }
    string p = path;
    return basename(&p[0]);
}

bool FileUtil::getFileNames(const string &path, const string &extension, vector<string> &fileNames) {
    AUTIL_LOG(TRACE1, "getFileNames of path: [%s], extension: [%s]", path.c_str(), extension.c_str());
    fslib::FileList fileList;
    fslib::ErrorCode ec = FileSystem::listDir(path, fileList);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR,
                  "list dir [%s] failed. "
                  "ErrorCode: [%d], ErrorMsg: [%s].",
                  path.c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
        return false;
    }

    fileNames.clear();
    int extensionLen = extension.length();
    for (fslib::FileList::iterator it = fileList.begin(); it != fileList.end(); it++) {
        string fileName = *it;
        int fileNameLen = fileName.length();
        if (fileNameLen < extensionLen) {
            continue;
        }
        int ret = fileName.compare(fileNameLen - extensionLen, extensionLen, extension);
        if (ret == 0) {
            fileNames.push_back(*it);
        }
    }
    sort(fileNames.begin(), fileNames.end());
    return true;
}

bool FileUtil::getDirMeta(const string &pathName, uint32_t &fileCount, uint32_t &dirCount, uint64_t &size) {
    FileList fileList;
    ErrorCode ret = FileSystem::listDir(pathName, fileList);
    if (ret != EC_OK) {
        AUTIL_LOG(ERROR, "list dir [%s] failed: %s", pathName.c_str(), FileSystem::getErrorString(ret).c_str());
        return false;
    }

    for (size_t i = 0; i < fileList.size(); i++) {
        string subPath = FileUtil::joinFilePath(pathName, fileList[i]);
        ret = FileSystem::isFile(subPath);
        if (ret == EC_TRUE) {
            fileCount++;
            FileMeta meta;
            ret = FileSystem::getFileMeta(subPath, meta);
            if (ret != EC_OK) {
                AUTIL_LOG(ERROR,
                          "get metainfo of file [%s] failed: %s",
                          subPath.c_str(),
                          FileSystem::getErrorString(ret).c_str());
                return false;
            }
            size += meta.fileLength;
        } else if (ret == EC_FALSE) {
            ret = FileSystem::isDirectory(subPath);
            if (ret == EC_TRUE) {
                dirCount++;
                bool ret = getDirMeta(subPath, fileCount, dirCount, size);
                if (!ret) {
                    return false;
                }
            } else if (ret == EC_FALSE) {
                AUTIL_LOG(ERROR, "path [%s] is neither a file nor directory.", subPath.c_str());
                return false;
            } else {
                AUTIL_LOG(
                    ERROR, "get path [%s] meta failed: %s", subPath.c_str(), FileSystem::getErrorString(ret).c_str());
                return false;
            }
        } else {
            AUTIL_LOG(ERROR, "get path [%s] meta failed: %s", subPath.c_str(), FileSystem::getErrorString(ret).c_str());
            return false;
        }
    }
    return true;
}

bool FileUtil::getDirNames(const string &path, vector<string> &dirNames) {
    vector<string> fileNames;
    bool ret = FileUtil::getFileNames(path, "", fileNames);
    if (!ret) {
        return false;
    }
    vector<string>::iterator it = fileNames.begin();
    for (; it != fileNames.end(); it++) {
        string fullFileName = FileUtil::joinFilePath(path, *it);
        if (EC_TRUE != FileSystem::isDirectory(fullFileName)) {
            continue;
        }
        dirNames.push_back(*it);
    }
    return true;
}

bool FileUtil::localLinkCopy(const std::string &src, const std::string &dest) {
    std::string cmd = "cp -alf " + FileUtil::joinFilePath(src, "*") + " " + dest;
    AUTIL_LOG(INFO, "exec cmd: %s", cmd.c_str());
    int ret = system(cmd.c_str());
    return (0 == ret);
}
bool FileUtil::getFsUsage(const string &path, uint32_t *usage) {
    struct statfs st;
    if (statfs(path.c_str(), &st) != 0) {
        return false;
    }

    *usage = (uint32_t)(100 * (1 - ((double)st.f_bavail / (double)st.f_blocks)));
    AUTIL_LOG(DEBUG,
              "get fs usage of path:[%s], st.f_bavail:[%ld], "
              "st.f_blocks:[%ld], usage:[%u]",
              path.c_str(),
              st.f_bavail,
              st.f_blocks,
              *usage);

    return true;
}

std::string FileUtil::getFileMd5(const std::string &filePath) {
    string cmd = string("/usr/bin/md5sum ") + filePath + string("| awk '{print $1}'");
    string md5;
    if (call(cmd, &md5) != 0) {
        AUTIL_LOG(ERROR, "get md5 failed, cmd: [%s], output:[%s].", cmd.c_str(), md5.c_str());
        return "";
    }
    return md5;
}

bool FileUtil::isLocalFile(const std::string &basePath) {
    if (StringUtil::startsWith(basePath, "pangu") || StringUtil::startsWith(basePath, "zfs") ||
        StringUtil::startsWith(basePath, "pangu2") || StringUtil::startsWith(basePath, "mpangu") ||
        StringUtil::startsWith(basePath, "http") || StringUtil::startsWith(basePath, "oss") ||
        StringUtil::startsWith(basePath, "dfs") || StringUtil::startsWith(basePath, "dcache") ||
        StringUtil::startsWith(basePath, "hdfs") || StringUtil::startsWith(basePath, "LOCAL") ||
		StringUtil::startsWith(basePath, "jfs")) {
        return false;
    }
    return true;
}

bool FileUtil::isHttpFile(const std::string &basePath) { return StringUtil::startsWith(basePath, "http"); }

bool FileUtil::isPanguFile(const std::string &basePath) {
    return StringUtil::startsWith(basePath, "pangu") || StringUtil::startsWith(basePath, "pangu2") ||
           StringUtil::startsWith(basePath, "mpangu");
}

bool FileUtil::isZfsFile(const std::string &basePath) { return StringUtil::startsWith(basePath, "zfs"); }

bool FileUtil::isHdfsFile(const std::string &basePath) { return StringUtil::startsWith(basePath, "hdfs") || StringUtil::startsWith(basePath, "jfs"); }

bool FileUtil::isOssFile(const std::string &basePath) { return StringUtil::startsWith(basePath, "oss"); }

bool FileUtil::isDfsFile(const std::string &basePath) { return StringUtil::startsWith(basePath, "dfs"); }

bool FileUtil::isDcacheFile(const std::string &basePath) { return StringUtil::startsWith(basePath, "dcache"); }

bool FileUtil::makeSureParentDirExist(const string &fileName) {
    auto parentPath = FileUtil::getParentDir(fileName);
    auto isExist = FileUtil::isExist(parentPath);
    if (!isExist) {
        return FileUtil::mkDir(parentPath, true);
    }
    return true;
}

int32_t FileUtil::call(const std::string &command, std::string *out, int32_t timeout) {
    FILE *stream = popen(command.c_str(), "r");
    if (stream == NULL) {
        AUTIL_LOG(ERROR, "execute cmd[%s] failed, errorno[%d], errormsg:[%s]", command.c_str(), errno, strerror(errno));
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
    AUTIL_LOG(ERROR, "execute cmd[%s] failed", command.c_str());
    return -1;
}

void FileUtil::readStream(FILE *stream, string *pattern, int32_t timeout) {
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

bool FileUtil::getCurrentPath(std::string &path) {
    char cwdPath[PATH_MAX];
    char *ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        // TERR( "Failed to get current work directory");
        return false;
    }
    path = string(cwdPath);
    if ('/' != *(path.rbegin())) {
        path += "/";
    }
    return true;
}

bool FileUtil::getRealPath(const std::string &path, std::string &real_path) {
    char path_buffer[PATH_MAX];
    if (NULL == realpath(path.c_str(), path_buffer)) {
        AUTIL_LOG(ERROR, "get real path of path [%s] failed.", path.c_str());
        return false;
    }
    real_path = string(path_buffer);
    return true;
}

bool FileUtil::writeFileLocalSafe(const string &filePath, const string &content) {
    string tmpFilePath = filePath + ".__tmp__";
    if (writeFile(tmpFilePath, content)) {
        if (rename(tmpFilePath.c_str(), filePath.c_str()) != 0) {
            AUTIL_LOG(ERROR, "Failed to rename %s to %s: %s", tmpFilePath.c_str(), filePath.c_str(), strerror(errno));
            return false;
        }
        return true;
    } else {
        AUTIL_LOG(ERROR, "Failed to write to tmp file %s", tmpFilePath.c_str());
        return false;
    }
}

std::string FileUtil::getTmpFilePath(const std::string &filePath) { return filePath + ".__tmp__"; }

bool FileUtil::renameFile(const string &s_old_file, const string &s_new_file) {
    return (0 == rename(s_old_file.c_str(), s_new_file.c_str()));
}

bool FileUtil::resolveSymbolLink(const string &path, string &realPath) {
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

bool FileUtil::readLinkRecursivly(string &path) {
    int32_t recLimit = 20;
    while (recLimit-- > 0) {
        if (FileUtil::isLink(path)) {
            string tmpPath = FileUtil::readLink(path);
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

bool FileUtil::isLink(const string &path) {
    struct stat statBuf;
    int ret = lstat(path.c_str(), &statBuf);
    if (ret != 0) {
        AUTIL_LOG(ERROR, "lstat error. path:[%s], error:[%s]", path.c_str(), strerror(errno));
        return false;
    }
    return S_ISLNK(statBuf.st_mode);
}

string FileUtil::readLink(const string &linkPath) {
    char tmpPath[PATH_MAX];
    ssize_t ret = readlink(linkPath.c_str(), tmpPath, PATH_MAX);
    if (ret < 0) {
        return "";
    }
    tmpPath[ret] = '\0';
    return string(tmpPath);
}

bool FileUtil::listLinks(const string &linkDir, vector<string> *linkNames) {
    linkNames->clear();
    vector<string> fileList;
    fslib::EntryInfoMap entryInfoMap;
    fslib::ErrorCode ec = fslib::fs::FileSystem::listDir(linkDir, entryInfoMap);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(
            ERROR, "list linkDir:[%s] fail. error:[%s]", linkDir.c_str(), fs::FileSystem::getErrorString(ec).c_str());
        return false;
    }
    fslib::EntryInfoMap::const_iterator it = entryInfoMap.begin();
    for (; it != entryInfoMap.end(); it++) {
        string filePath = FileUtil::joinPath(linkDir, it->second.entryName);
        if (isLink(filePath)) {
            linkNames->push_back(filePath);
        }
    }
    return true;
}

bool FileUtil::matchPattern(const string &path, const string &pattern) {
    int ret = ::fnmatch(pattern.c_str(), path.c_str(), FNM_PATHNAME);
    if (ret == 0) {
        return true;
    } else if (ret == FNM_NOMATCH) {
        return false;
    }

    AUTIL_LOG(ERROR, "match pattern failed. path:[%s], pattern:[%s].", path.c_str(), pattern.c_str());
    return false;
}

bool FileUtil::makeSureNotExist(const string &path) {
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

bool FileUtil::makeSureNotExistLocal(const string &path) {
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(path);
    if (ec == fslib::EC_TRUE) {
        if (0 != remove(path.c_str())) {
            AUTIL_LOG(ERROR, "failed to remove [%s], errorinfo is [%s]", path.c_str(), strerror(errno));
            return false;
        }
    } else if (ec != fslib::EC_FALSE) {
        return false;
    }
    return true;
}
bool FileUtil::listSubDirs(const string &dir, vector<string> *subDirs) {
    fslib::EntryList entryList;
    fslib::ErrorCode ec = fslib::fs::FileSystem::listDir(dir, entryList);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(ERROR, "list dir:[%s] fail. error:[%s]", dir.c_str(), fs::FileSystem::getErrorString(ec).c_str());
        return false;
    }
    fslib::EntryList::const_iterator it = entryList.begin();
    for (; it != entryList.end(); it++) {
        if (it->isDir) {
            string path = FileUtil::joinPath(dir, it->entryName);
            subDirs->push_back(path);
        }
    }
    return true;
}

bool FileUtil::listSubFiles(const string &dir, std::map<std::string, std::string> &subFiles) {
    fslib::EntryList entryList;
    fslib::ErrorCode ec = fslib::fs::FileSystem::listDir(dir, entryList);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(ERROR, "list dir:[%s] fail. error:[%s]", dir.c_str(), fs::FileSystem::getErrorString(ec).c_str());
        return false;
    }
    auto it = entryList.begin();
    for (; it != entryList.end(); it++) {
        if (it->isDir) {
            continue;
        }
        string filePath = FileUtil::joinPath(dir, it->entryName);
        subFiles[it->entryName] = filePath;
    }
    return true;
}

string FileUtil::dirName(const string &path) {
    char tmpPath[PATH_MAX];
    strcpy(tmpPath, path.c_str());
    return string(dirname(tmpPath));
}

bool FileUtil::createLink(const string &target, const string &linkName, bool isSymlink) {
    AUTIL_LOG(DEBUG, "link file from [%s] to [%s].", linkName.c_str(), target.c_str());
    string linkBaseDir = FileUtil::dirName(linkName);
    if (!FileUtil::createDir(linkBaseDir)) {
        return false;
    }
    int32_t ret = 0;
    if (isSymlink) {
        ret = symlink(target.c_str(), linkName.c_str());
        if (ret != 0) {
            AUTIL_LOG(ERROR,
                      "failed to create soft link [%s] to [%s], error:[%s]",
                      linkName.c_str(),
                      target.c_str(),
                      strerror(errno));
        }
    } else {
        ret = link(target.c_str(), linkName.c_str());
        if (ret != 0) {
            AUTIL_LOG(ERROR,
                      "failed to create hard link [%s] to [%s], error:[%s]",
                      linkName.c_str(),
                      target.c_str(),
                      strerror(errno));
        }
    }
    return ret == 0;
}

bool FileUtil::getDiskSize(const string &path, int32_t *diskSize) {
    struct statfs st;
    if (statfs(path.c_str(), &st) != 0) {
        AUTIL_LOG(ERROR, "get disk size for [%s] failed, error:%s.", path.c_str(), strerror(errno));
        return false;
    } else if (utime(path.c_str(), NULL) != 0) {
        AUTIL_LOG(ERROR, "access disk [%s] failed, error:%s.", path.c_str(), strerror(errno));
        return false;
    }
    *diskSize = (int32_t)((double)st.f_blocks * st.f_bsize / (1024 * 1024));
    return true;
}

bool FileUtil::isSubDir(const string &parent, const string &child) {
    if (parent.empty() || child.empty()) {
        return false;
    }
    string nParent = FileUtil::normalizePath(parent);
    string nChild = FileUtil::normalizePath(child);
    if (nChild.find(nParent) == 0) {
        return true;
    }
    return false;
}

int64_t FileUtil::getCgroupValue(const std::string &path) {
    ifstream in(path.c_str());
    if (!in) {
        return -1;
    }
    string content;
    if (!std::getline(in, content)) {
        return -1;
    }
    int64_t value = -1;
    if (!autil::StringUtil::strToInt64(content.c_str(), value)) {
        return -1;
    }
    return value;
}

string FileUtil::getVolumePath(const string &appWorkDir, const int32_t slotId) {
    // string volumePath = "volumes_" + StringUtil::toString(slotId);
    return FileUtil::joinPath(appWorkDir, "volumes");
}

bool FileUtil::copyAnyFileToLocal(const std::string &src, const std::string &dest) {
    // TODO: add HTTP://, FTP://, SFTP:// support
    return (EC_OK == fs::FileSystem::copy(src, dest));
}

uint32_t FileUtil::getQuotaId(const string &quotaFile) {
    string strQuotaId;
    if (!FileUtil::readFile(quotaFile, strQuotaId)) {
        AUTIL_LOG(WARN, "read quotafile [%s] failed.", quotaFile.c_str());
        return 0;
    }
    uint32_t quotaId;
    size_t pos = strQuotaId.find("\n");
    if (pos == string::npos) {
        quotaId = autil::StringUtil::strToUInt32WithDefault(strQuotaId.c_str(), 0);
    } else {
        strQuotaId = strQuotaId.substr(0, pos);
        quotaId = autil::StringUtil::strToUInt32WithDefault(strQuotaId.c_str(), 0);
    }
    if (quotaId == 0) {
        AUTIL_LOG(ERROR, "quotafile [%s] format error, [%s].", quotaFile.c_str(), strQuotaId.c_str());
    }
    return quotaId;
}
bool FileUtil::recoverFromFile(const std::string &filePath, std::string *content) {
    assert(content);
    *content = "";
    fslib::ErrorCode ec = fs::FileSystem::isExist(filePath);
    if (ec != fslib::EC_TRUE && ec != fslib::EC_FALSE) {
        return false;
    }

    if (ec == fslib::EC_TRUE) {
        ec = fs::FileSystem::isFile(filePath);
        if (ec == fslib::EC_TRUE) {
            return readFile(filePath, *content);
        } else if (ec != EC_FALSE) {
            return false;
        }
    }

    AUTIL_LOG(DEBUG, "file not exist. filePath:%s", filePath.c_str());
    string tmpFilePath = FileUtil::getTmpFilePath(filePath);
    if (fslib::EC_TRUE == fs::FileSystem::isExist(tmpFilePath)) {
        return readFile(tmpFilePath, *content);
    }
    AUTIL_LOG(DEBUG, "tmpfile not exist. tmpFilePath:%s", tmpFilePath.c_str());
    return false;
}

bool FileUtil::isFileContentDifference(const std::string &srcFile, const std::string &destFile) {
    std::string srcContent, destContent;
    FileUtil::readFile(srcFile, srcContent);
    FileUtil::readFile(destFile, destContent);
    return srcContent != destContent;
}

FSLIB_END_NAMESPACE(util);
