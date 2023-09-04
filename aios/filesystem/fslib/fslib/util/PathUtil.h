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
#ifndef FSLIB_PATHUTIL_H
#define FSLIB_PATHUTIL_H

#include "autil/Log.h"
#include "fslib/common/common_define.h"

FSLIB_BEGIN_NAMESPACE(util);

class PathUtil {
public:
    PathUtil();
    ~PathUtil();

public:
    static std::string normalizePath(const std::string &path);

    static bool isInRootPath(const std::string &normPath, const std::string &normRootPath);

    static std::string joinPath(const std::string &path, const std::string &name);

    static std::string GetParentDirPath(const std::string &path);

    static std::string GetFileName(const std::string &path);

    static bool rewriteToDcachePath(const std::string &source, std::string &target);
};

FSLIB_TYPEDEF_AUTO_PTR(PathUtil);

FSLIB_END_NAMESPACE(util);

#endif // FSLIB_PATHUTIL_H
