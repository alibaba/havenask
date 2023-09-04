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
#ifndef HIPPO_PATHUTIL_H
#define HIPPO_PATHUTIL_H

#include "util/Log.h"
#include "common/common.h"
#include "autil/StringUtil.h"
#include "fslib/fslib.h"
#include <limits.h>

BEGIN_HIPPO_NAMESPACE(util);

class PathUtil
{
public:
    PathUtil();
    ~PathUtil();
private:
    PathUtil(const PathUtil &);
    PathUtil& operator=(const PathUtil &);
public:
    static bool getCurrentPath(std::string &path);

    static std::string getParentDir(const std::string &currentDir);
    
    static std::string joinPath(const std::string &path, 
                                const std::string &subPath);
    
    static std::string getHostFromZkPath(const std::string &zkPath);

    static std::string getPathFromZkPath(const std::string &zkPath);

    static bool resolveSymbolLink(const std::string &path, 
                                  std::string &realPath);

    static bool readLinkRecursivly(std::string &path);

    static bool isLink(const std::string &path);

    static std::string readLink(const std::string &linkPath);

    static bool listLinks(const std::string &linkDir, 
                          std::vector<std::string> *linkNames);

    //append to subDirs
    static bool listSubDirs(const std::string &dir,
                            std::vector<std::string> *subDirs);
    
    static bool listSubFiles(const std::string &dir, KeyValueMap &subFiles);

    static bool recursiveMakeLocalDir(const std::string &localDir);

    static bool localDirExist(const std::string &localDir);

    static std::string normalizePath(const std::string &path);
    
    static bool isSubDir(const std::string &parent, const std::string &child);

    static int32_t getLinkCount(const std::string &path);
    
    static int32_t getInodeNumber(const std::string &path);

    static bool matchPattern(const std::string &path, const std::string &pattern);

    static bool makeSureNotExist(const std::string &path);

    static bool makeSureNotExistLocal(const std::string &path);

    static std::string getVolumePath(const std::string &appWorkDir, const int32_t slotId);

    static std::string getPersistentVolumePath(const std::string &appWorkDir);

    static bool getDirSizeInByte(const std::string &dirName, int64_t &size);

    static bool superChmod(const std::string &file, const std::string &pattern);

    static bool isPathExist(const std::string &filePath);

    static bool chattr(const std::string &path, std::string args);

private:
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(PathUtil);

END_HIPPO_NAMESPACE(util);

#endif //HIPPO_PATHUTIL_H
