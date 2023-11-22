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
#include "indexlib/util/PathUtil.h"

#include <algorithm>
#include <cstring>
#include <iosfwd>
#include <stdint.h>
#include <unistd.h>

#include "autil/EnvUtil.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"

using namespace std;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, PathUtil);

const string PathUtil::CONFIG_SEPARATOR("?");

string PathUtil::GetParentDirPath(const string& path) noexcept
{
    string::const_reverse_iterator it;
    for (it = path.rbegin(); it != path.rend() && *it == '/'; it++)
        ;
    for (; it != path.rend() && *it != '/'; it++)
        ;
    for (; it != path.rend() && *it == '/'; it++)
        ;
    return path.substr(0, path.rend() - it);
}

string PathUtil::GetParentDirName(const string& path) noexcept
{
    string parentDirPath = GetParentDirPath(path);
    string::const_reverse_iterator it;
    for (it = parentDirPath.rbegin(); it != parentDirPath.rend() && *it != '/'; it++)
        ;
    return parentDirPath.substr(parentDirPath.rend() - it);
}

string PathUtil::GetFileName(const string& path) noexcept
{
    string::const_reverse_iterator it;
    for (it = path.rbegin(); it != path.rend() && *it != '/'; it++)
        ;
    return path.substr(path.rend() - it);
}

string PathUtil::GetDirName(const string& path) noexcept
{
    string dirPath = path;
    PathUtil::TrimLastDelim(dirPath);
    string::const_reverse_iterator it;
    for (it = dirPath.rbegin(); it != dirPath.rend() && *it != '/'; it++)
        ;
    return dirPath.substr(dirPath.rend() - it);
}

string PathUtil::JoinPath(const string& path, const string& name) noexcept
{
    if (path.empty()) {
        return name;
    }
    if (name.empty()) {
        return path;
    }

    if (*(path.rbegin()) != '/') {
        return path + "/" + name;
    }

    return path + name;
}

string PathUtil::NormalizePath(const string& path) noexcept
{
    string tmpString(path.size(), 0);
    char* pathBuffer = tmpString.data();
    uint32_t cursor = 0;
    size_t start = 0;
    bool hasOneSlash = false;
    size_t idx = path.find("://");
    if (idx != string::npos) {
        size_t end = idx + 3;
        for (size_t i = 0; i < end; i++) {
            pathBuffer[cursor++] = path[i];
        }
        start = end;
        for (; start < path.size(); ++start) {
            if (path[start] != '/') {
                break;
            }
        }
    } else if (!path.empty() && path[0] != '/') {
        hasOneSlash = true;
    }

    char lastChar = 0;
    for (size_t i = start; i < path.size(); ++i) {
        if (lastChar == '/' && path[i] == '/') {
            continue;
        }

        if (lastChar == '/') {
            hasOneSlash = true;
        }
        lastChar = path[i];
        pathBuffer[cursor++] = lastChar;
    }

    if (hasOneSlash && cursor > 0 && pathBuffer[cursor - 1] == '/') {
        pathBuffer[--cursor] = '\0';
    }
    return string(pathBuffer);
}

string PathUtil::NormalizeDir(const string& path) noexcept
{
    string tmpDir = path;
    if (!tmpDir.empty() && *(tmpDir.rbegin()) != '/') {
        tmpDir += '/';
    }
    return tmpDir;
}

void PathUtil::TrimLastDelim(string& dirPath) noexcept
{
    if (dirPath.empty()) {
        return;
    }
    if (*(dirPath.rbegin()) == '/') {
        dirPath.erase(dirPath.size() - 1, 1);
    }
}

bool PathUtil::IsInRootPath(const string& normPath, const string& normRootPath) noexcept
{
    if (normRootPath.empty()) {
        return true;
    }
    size_t prefixLen = normRootPath.size();
    if (!normRootPath.empty() && *normRootPath.rbegin() == '/') {
        prefixLen--;
    }

    if (normPath.size() < normRootPath.size() || normPath.compare(0, normRootPath.size(), normRootPath) != 0 ||
        (normPath.size() > normRootPath.size() && normPath[prefixLen] != '/')) {
        return false;
    }
    return true;
}

bool PathUtil::GetRelativePath(const string& parentPath, const string& path, string& relativePath) noexcept
{
    string normalParentPath = parentPath;
    if (!normalParentPath.empty() && *(normalParentPath.rbegin()) != '/') {
        normalParentPath += '/';
    }

    if (path.find(normalParentPath) != 0) {
        return false;
    }
    relativePath = path.substr(normalParentPath.length());
    return true;
}

string PathUtil::GetRelativePath(const string& absPath) noexcept
{
    size_t idx = absPath.find("://");
    if (idx == std::string::npos) {
        return string(absPath);
    }
    idx = absPath.find('/', idx + 3);
    return absPath.substr(idx, absPath.length() - idx);
}

bool PathUtil::IsInDfs(const string& path) noexcept
{
    fslib::FsType fsType = fslib::fs::FileSystem::getFsType(path);
    return fsType != FSLIB_FS_LOCAL_FILESYSTEM_NAME;
}

bool PathUtil::SupportMmap(const string& path) noexcept
{
    fslib::FsType fsType = fslib::fs::FileSystem::getFsType(path);
    return fsType == FSLIB_FS_LOCAL_FILESYSTEM_NAME || fsType == "dcache";
}

bool PathUtil::SupportPanguInlineFile(const string& path) noexcept
{
    fslib::FsType fsType = fslib::fs::FileSystem::getFsType(path);
    return fsType == "mpangu" || autil::EnvUtil::hasEnv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE");
}

string PathUtil::AddFsConfig(const string& srcPath, const string& configStr) noexcept
{
    size_t found = srcPath.find("://");
    if (found == string::npos) {
        AUTIL_LOG(DEBUG, "path [%s] do not have a scheme", srcPath.c_str());
        return srcPath;
    }

    size_t fsEnd = srcPath.find('/', found + ::strlen("://"));
    if (fsEnd == string::npos) {
        AUTIL_LOG(DEBUG, "path [%s] do not have a valid path", srcPath.c_str());
        return srcPath;
    }
    if (configStr.empty()) {
        return srcPath;
    }
    size_t paramBegin = srcPath.find(CONFIG_SEPARATOR, found);
    bool hasParam = paramBegin != string::npos && paramBegin < fsEnd;
    if (hasParam) {
        return srcPath.substr(0, fsEnd) + '&' + configStr + srcPath.substr(fsEnd, srcPath.size() - fsEnd);
    }
    return srcPath.substr(0, fsEnd) + CONFIG_SEPARATOR + configStr + srcPath.substr(fsEnd, srcPath.size() - fsEnd);
}

string PathUtil::SetFsConfig(const string& srcPath, const string& configStr) noexcept
{
    size_t found = srcPath.find("://");
    if (found == string::npos) {
        AUTIL_LOG(DEBUG, "path [%s] do not have a scheme", srcPath.c_str());
        return srcPath;
    }
    size_t fsEnd = srcPath.find('/', found + ::strlen("://"));
    if (fsEnd == string::npos) {
        AUTIL_LOG(DEBUG, "path [%s] do not have a valid path", srcPath.c_str());
        return srcPath;
    }
    if (configStr.empty()) {
        return srcPath;
    }
    return srcPath.substr(0, fsEnd) + CONFIG_SEPARATOR + configStr + srcPath.substr(fsEnd, srcPath.size() - fsEnd);
}

string PathUtil::GetBinaryPath() noexcept
{
    char buffer[1024];
    int ret = readlink("/proc/self/exe", buffer, sizeof(buffer) - 1);
    return string(buffer, ret);
}

// for an example path of "a/b/c/d", return a list of paths: {"a", "a/b", "a/b/c"}
std::vector<std::string> PathUtil::GetPossibleParentPaths(const std::string& path, bool includeSelf)
{
    std::string normalizedPath = PathUtil::NormalizePath(path);
    if (normalizedPath.empty()) {
        return {};
    }
    if (normalizedPath == "/") {
        return includeSelf ? std::vector<std::string> {"/"} : std::vector<std::string> {};
    }
    std::vector<std::string> parents;
    if (includeSelf) {
        parents.push_back(normalizedPath);
    }
    std::size_t start = 0;
    std::size_t end = normalizedPath.find('/', start);
    while (end != std::string::npos) {
        parents.push_back(end == 0 ? "/" : normalizedPath.substr(0, end));
        start = end + 1;
        end = normalizedPath.find('/', start);
    }
    return parents;
}

// for an example path of "a/b/c/d", given param "b", return "c/d".
// if param is not found, return empty string.
std::string PathUtil::GetFilePathUnderParam(const std::string& path, const std::string& param)
{
    std::string normalizedPath = PathUtil::NormalizePath(path);
    if (param.empty()) {
        return normalizedPath;
    }
    size_t pos = 0;
    // find the position of the param in normalizedPath
    pos = normalizedPath.find(param);
    if (pos == std::string::npos) {
        return "";
    }
    // advance the position to the end of the target directory
    if (pos + param.length() + 1 >= normalizedPath.length()) {
        return "";
    }
    return normalizedPath.substr(pos + param.length() + 1);
}

// @path is expected to be a file path containing both directory and file name
// return the directory path of @path
std::string PathUtil::GetDirectoryPath(const std::string& path)
{
    std::string normalizedPath = PathUtil::NormalizePath(path);
    if (normalizedPath.empty()) {
        return "";
    }
    if (normalizedPath == "/") {
        return "";
    }
    size_t pos = normalizedPath.find_last_of('/');
    if (pos == std::string::npos) {
        return "";
    }

    return pos == 0 ? "/" : normalizedPath.substr(0, pos);
}

}} // namespace indexlib::util
