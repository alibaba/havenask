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
#ifndef CARBON_PATHUTIL_H
#define CARBON_PATHUTIL_H

#include "common/common.h"
#include "carbon/Log.h"
#include "autil/StringUtil.h"

BEGIN_CARBON_NAMESPACE(common);

class PathUtil
{
public:
    PathUtil();
    ~PathUtil();
private:
    PathUtil(const PathUtil &);
    PathUtil& operator=(const PathUtil &);
public:
    static std::string getParentDir(const std::string &currentDir);

    static bool getCurrentPath(std::string &path);

    static std::string baseName(const std::string &currentDir);

    static std::string joinPath(const std::string &path, 
                                const std::string &subPath);
    
    static std::string getHostFromZkPath(const std::string &zkPath);

    static std::string getPathFromZkPath(const std::string &zkPath);

    static std::string normalizePath(const std::string &path);

    static bool makeSureDirExist(const std::string &dir);
private:
    CARBON_LOG_DECLARE();
};

END_CARBON_NAMESPACE(common);

#endif //CARBON_PATHUTIL_H
