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
#include "aios/apps/facility/cm2/cm_basic/util/file_util.h"

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fstream>
#include <libgen.h>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

namespace cm_basic {

AUTIL_LOG_SETUP(cm_basic, FileUtil);

FileUtil::FileUtil() {}

FileUtil::~FileUtil() {}

string FileUtil::readFile(const string& filePath)
{
    ifstream in(filePath.c_str(), ifstream::binary);

    if (!in) {
        AUTIL_LOG(INFO, "Wrong FilePath:[%s]", filePath.c_str());
        return string("");
    }

    in.seekg(0, ios::end);
    int32_t fileLength = in.tellg();
    in.seekg(0, ios::beg);
    char* buffer = new char[fileLength];
    in.read(buffer, fileLength);
    in.close();
    string result(buffer, fileLength);
    delete[] buffer;
    return result;
}

bool FileUtil::writeFile(const string& filePath, const string& content)
{
    // AUTIL_LOG(INFO,  "writeFile filePath[%s], content[%s]", filePath.c_str(), content.c_str());
    string parentDir = getParentDir(filePath);
    bool ret = recursiveMakeDir(parentDir);
    if (!ret) {
        AUTIL_LOG(INFO, "create parent dir error, parentDir:[%s]", parentDir.c_str());
        return false;
    }

    std::ofstream file(filePath.c_str());
    if (!file) {
        AUTIL_LOG(INFO, "Create local file failure: %s", filePath.c_str());
        return false;
    }
    file.write(content.c_str(), content.length());
    file.close();
    return true;
}

bool FileUtil::appendFile(const std::string& filePath, const std::string& content)
{
    std::ofstream file(filePath.c_str(), std::ios_base::app);
    if (!file) {
        AUTIL_LOG(INFO, "Create local file failure: %s", filePath.c_str());
        return false;
    }
    file.write(content.c_str(), content.length());
    file.close();
    return true;
}

bool FileUtil::listDir(const string& dirPath, vector<string>& entryVec, bool fileOnly)
{
    entryVec.clear();

    DIR* dp;
    struct dirent* dirp;
    if ((dp = opendir(dirPath.c_str())) == NULL) {
        AUTIL_LOG(INFO, "failed to list localDir[%s]", dirPath.c_str());
        return false;
    }

    while ((dirp = readdir(dp)) != NULL) {
        string name = dirp->d_name;
        if (name == "." || name == "..") {
            continue;
        }
        if (DT_DIR == dirp->d_type) {
            if (!fileOnly) {
                entryVec.push_back(name + "/");
            }
        } else {
            entryVec.push_back(name);
        }
    }
    closedir(dp);
    return true;
}

bool FileUtil::listDirByPattern(const string& dirPath, vector<string>& entryVec, const std::string& pattern,
                                bool fileOnly)
{
    entryVec.clear();

    DIR* dp;
    struct dirent* dirp;
    if ((dp = opendir(dirPath.c_str())) == NULL) {
        AUTIL_LOG(INFO, "failed to list localDir[%s]", dirPath.c_str());
        return false;
    }

    while ((dirp = readdir(dp)) != NULL) {
        string name = dirp->d_name;
        if (name == "." || name == "..") {
            continue;
        }
        if (DT_DIR == dirp->d_type) {
            if (!fileOnly) {
                if (name.find(pattern) != string::npos) {
                    entryVec.push_back(name + "/");
                }
            }
        } else {
            if (name.find(pattern) != string::npos) {
                entryVec.push_back(name);
            }
        }
    }
    closedir(dp);
    return true;
}

bool FileUtil::listDir(const std::string& dirPath, std::vector<std::string>& fileVec, std::string& substr)
{
    fileVec.clear();

    DIR* dp;
    struct dirent* dirp;
    if ((dp = opendir(dirPath.c_str())) == NULL) {
        AUTIL_LOG(INFO, "failed to list localDir[%s]", dirPath.c_str());
        return false;
    }

    while ((dirp = readdir(dp)) != NULL) {
        string name = dirp->d_name;
        if (DT_DIR == dirp->d_type) {
            continue;
        } else {
            if (name.find(substr) != string::npos) {
                fileVec.push_back(name);
            }
        }
    }
    closedir(dp);
    return true;
}

bool FileUtil::fileExist(const std::string& filePath)
{
    if (access(filePath.c_str(), F_OK) == 0) {
        return true;
    }

    return false;
}

bool FileUtil::removeFile(const std::string& filePath) { return (0 == unlink(filePath.c_str())); }

string FileUtil::readFileWithLoyalty(const std::string& filePath)
{
    ifstream in(filePath.c_str());
    stringstream ss;
    string line;
    if (!in) {
        AUTIL_LOG(INFO, "Wrong FilePath:[%s]", filePath.c_str());
        return string("");
    }
    while (in.good()) {
        char c;
        in.get(c);
        ss << c;
    }
    in.close();
    return ss.str();
}

bool FileUtil::copyFile(const string& desFilePath, const string& srcFilePath)
{
    if (!fileExist(srcFilePath)) {
        AUTIL_LOG(INFO, "srcFilePath is not exist, srcFilePath:[%s]", srcFilePath.c_str());
        return false;
    }

    // read content to stringstream
    ifstream in(srcFilePath.c_str());
    stringstream ss;
    string line;
    char c;
    while (in.get(c)) {
        ss << c;
    }
    in.close();

    // write content to desFilePath
    ofstream out(desFilePath.c_str());
    string content = ss.str();
    if (!out.write(content.c_str(), content.size())) {
        AUTIL_LOG(INFO, "write content error, content size:%lu", content.size());
        return false;
    }

    return true;
}

bool FileUtil::renameFile(const string& s_old_file, const string& s_new_file)
{
    return (0 == ::rename(s_old_file.c_str(), s_new_file.c_str()));
}

bool FileUtil::dirExist(const string& dirPath)
{
    struct stat st;
    if (stat(dirPath.c_str(), &st) != 0) {
        return false;
    }

    if (!S_ISDIR(st.st_mode)) {
        return false;
    }

    return true;
}

bool FileUtil::makeDir(const string& dirPath, bool recursive)
{
    if (recursive) {
        return recursiveMakeDir(dirPath);
    }

    if (dirExist(dirPath)) {
        AUTIL_LOG(INFO, "the directory[%s] is already exist", dirPath.c_str());
        return false;
    }

    int mkdirRet = mkdir(dirPath.c_str(), S_IRWXU);
    if (mkdirRet != 0) {
        AUTIL_LOG(INFO, "mkdir error, dir:[%s], errno:%d", dirPath.c_str(), errno);
        return false;
    }
    return true;
}

bool FileUtil::recursiveMakeDir(const string& dirPath)
{
    if (dirPath.empty()) {
        return false;
    }

    if (FileUtil::dirExist(dirPath)) {
        return true;
    }
    string currentDir;
    ;
    if (dirPath[dirPath.length() - 1] == DIR_DELIM)
        currentDir = dirPath.substr(0, dirPath.length() - 1);
    else
        currentDir = dirPath;
    string parentDir = getParentDir(currentDir);

    // create parent dir
    if (parentDir != "" && !FileUtil::dirExist(parentDir)) {
        bool ret = recursiveMakeDir(parentDir);
        if (!ret) {
            AUTIL_LOG(INFO, "recursive make local dir error, dir:[%s]", parentDir.c_str());
            return false;
        }
    }

    // make current dir
    int mkdirRet = mkdir(currentDir.c_str(), S_IRWXU);
    if (mkdirRet != 0) {
        AUTIL_LOG(INFO, "mkdir error, dir:[%s], errno:%d", currentDir.c_str(), errno);
        return false;
    }
    return true;
}

std::string FileUtil::getParentDir(const string& currentDir)
{
    if (currentDir.empty()) {
        return "";
    }

    size_t delimPos = string::npos;
    if (DIR_DELIM == *(currentDir.rbegin())) {
        // the last charactor is '/', then rfind from the next char
        delimPos = currentDir.rfind(DIR_DELIM, currentDir.size() - 1);
    } else {
        delimPos = currentDir.rfind(DIR_DELIM);
    }

    if (string::npos == delimPos) {
        // not found '/', than parent dir is null
        return "";
    }

    string parentDir = currentDir.substr(0, delimPos);

    return parentDir;
}

bool FileUtil::removeDir(const string& dirPath, bool removeNoneEmptyDir)
{
    bool dirExist = FileUtil::dirExist(dirPath);
    if (!dirExist) {
        AUTIL_LOG(INFO, "the dir:[%s] is not exist", dirPath.c_str());
        return false;
    }

    if (removeNoneEmptyDir) {
        return recursiveRemoveDir(dirPath);
    } else if (rmdir(dirPath.c_str()) != 0) {
        AUTIL_LOG(INFO, "dirPath[%s] may not an empty dir", dirPath.c_str());
        return false;
    }
    return true;
}

bool FileUtil::recursiveRemoveDir(const string& dirPath)
{
    DIR* dir = NULL;
    struct dirent* dp = NULL;

    if ((dir = opendir(dirPath.c_str())) == NULL) {
        AUTIL_LOG(INFO, "opendir error, dirPath:[%s]", dirPath.c_str());
        return false;
    }

    bool retFlag = false;
    while ((dp = readdir(dir)) != NULL) {
        if (dp->d_type & DT_DIR) {
            if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0) {
                retFlag = true;
                continue;
            } else {
                string childDirPath = dirPath + DIR_DELIM + string(dp->d_name);
                retFlag = recursiveRemoveDir(childDirPath);
            }
        } else {
            string childFilePath = dirPath + DIR_DELIM + string(dp->d_name);
            retFlag = removeFile(childFilePath);
        }

        if (!retFlag) {
            break;
        }
    }
    closedir(dir);

    // rm itself
    if (retFlag && rmdir(dirPath.c_str()) != 0) {
        AUTIL_LOG(INFO, "rmdir error, dirPath[%s]", dirPath.c_str());
        return false;
    }
    return retFlag;
}

string FileUtil::joinFilePath(const string& path, const string& file)
{
    if (path.empty()) {
        return file;
    }

    if (path[path.length() - 1] == '/') {
        return path + file;
    }

    return path + '/' + file;
}

string FileUtil::normalizeDir(const string& dirName)
{
    string tmpDirName = dirName;
    if (!tmpDirName.empty() && *(tmpDirName.rbegin()) != '/') {
        tmpDirName += "/";
    }
    return tmpDirName;
}

void FileUtil::splitFileName(const std::string& input, std::string& folder, std::string& fileName)
{
    size_t found;
    found = input.find_last_of("/\\");
    if (found == string::npos) {
        fileName = input;
        return;
    }
    folder = input.substr(0, found);
    fileName = input.substr(found + 1);
}

bool FileUtil::readFromFile(const string& srcFileName, string& readStr)
{
    ifstream is(srcFileName.c_str(), ios_base::binary);
    if (!is) {
        return false;
    }

    ostringstream oss;
    oss << is.rdbuf();
    is.close();

    readStr = oss.str();

    return true;
}

bool FileUtil::readFromFile(const string& srcFileName, stringstream& ss)
{
    string readStr;
    if (!readFromFile(srcFileName, readStr)) {
        return false;
    }

    ss << readStr;

    return true;
}

bool FileUtil::getCurrentPath(std::string& path)
{
    char cwdPath[PATH_MAX];
    char* ret = getcwd(cwdPath, PATH_MAX);
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

int FileUtil::splitPath(char* path, char** pathes)
{
    char* ptr;
    int i;
    i = 0;
    while (*path != 0) {
        ptr = strchr(path, '/');
        if (ptr == NULL) {
            pathes[i++] = path;
            break;
        }
        *ptr = 0;
        if (path != ptr) {
            pathes[i++] = path;
        }
        path = ptr + 1;
    }
    return i;
}

char* FileUtil::getRealPath(const char* path, char* rpath)
{
    auto strRealPath = getRealPath(path);
    if (strRealPath.empty()) {
        return nullptr;
    }
    strcpy(rpath, strRealPath.c_str());
    return rpath;
}

std::string FileUtil::getRealPath(const char* path)
{
    char pwd[PATH_MAX + 1];
    char vpath[PATH_MAX + 1];
    int level, level2;
    char* pathes[PATH_MAX / 2 + 1];
    char* pathes2[PATH_MAX / 2 + 1];
    int i;
    if (path == NULL) {
        errno = EINVAL;
        return std::string();
    }
    if (path[0] != '/') {
        if (getcwd(pwd, PATH_MAX + 1) == NULL) {
            return std::string();
        }
        level = FileUtil::splitPath(pwd, pathes);
    } else {
        level = 0;
    }
    strcpy(vpath, path);
    level2 = FileUtil::splitPath(vpath, pathes2);
    for (i = 0; i < level2; i++) {
        if (pathes2[i][0] == 0 || strcmp(pathes2[i], ".") == 0) {
        } else if (strcmp(pathes2[i], "..") == 0) {
            level = level > 0 ? level - 1 : 0; // i.e: /../../home -> /home
        } else {
            pathes[level++] = pathes2[i];
        }
    }
    std::string rpath("/");
    for (i = 0; i < level - 1; i++) {
        rpath += pathes[i];
        rpath += "/";
    }
    if (level > 0) // return '/' only
        rpath += pathes[i];
    return rpath;
}

} // namespace cm_basic
