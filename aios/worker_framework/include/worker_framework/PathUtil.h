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
#pragma once

#include "autil/Log.h"
#include "autil/StringUtil.h"

namespace worker_framework {

class PathUtil {
public:
    PathUtil();
    ~PathUtil();

private:
    PathUtil(const PathUtil &);
    PathUtil &operator=(const PathUtil &);

public:
    static bool getCurrentPath(std::string &path);

    static std::string getParentDir(const std::string &currentDir);

    static std::string joinPath(const std::string &path, const std::string &subPath);

    static std::string getHostFromZkPath(const std::string &zkPath);

    static std::string getPathFromZkPath(const std::string &zkPath);

    static bool resolveSymbolLink(const std::string &path, std::string &realPath);

    static bool readLinkRecursivly(std::string &path);

    static bool isLink(const std::string &path);

    static std::string readLink(const std::string &linkPath);

    static bool recursiveMakeLocalDir(const std::string &localDir);

    static bool localDirExist(const std::string &localDir);

    static std::string normalizePath(const std::string &path);

    static int32_t getLinkCount(const std::string &path);

    static int32_t getInodeNumber(const std::string &path);

    static bool matchPattern(const std::string &path, const std::string &pattern);

    static std::string baseName(const std::string &path);

    static std::string getLeaderInfoDir(const std::string &path);
    static std::string getLeaderInfoFile(const std::string &path);

public:
    static const std::string LEADER_INFO_DIR;
    static const std::string LEADER_INFO_FILE;
    static const std::string LEADER_INFO_FILE_BASE;

private:
    AUTIL_LOG_DECLARE();
};

}; // namespace worker_framework
