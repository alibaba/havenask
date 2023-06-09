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
#ifndef ISEARCH_BS_FILEPATTERNBASE_H
#define ISEARCH_BS_FILEPATTERNBASE_H

#include "autil/StringUtil.h"
#include "build_service/common_define.h"
#include "build_service/reader/CollectResult.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class FilePatternBase;
BS_TYPEDEF_PTR(FilePatternBase);

class FilePatternBase
{
public:
    FilePatternBase(const std::string& dirPath) { setDirPath(dirPath); }
    virtual ~FilePatternBase() {}

public:
    virtual CollectResults calculateHashIds(const CollectResults& results) const = 0;

public:
    void setDirPath(const std::string& dirPath) { _relativePath = normalizeDir(dirPath); }
    const std::string& getDirPath() const { return _relativePath; }

public:
    static bool isPatternConsistent(const FilePatternBasePtr& lft, const FilePatternBasePtr& rhs);

private:
    static std::string normalizeDir(const std::string& path)
    {
        std::vector<std::string> dirs = autil::StringUtil::split(path, "/", true);
        return autil::StringUtil::toString(dirs, "/");
    }

protected:
    std::string _relativePath;
};

typedef std::vector<FilePatternBasePtr> FilePatterns;

}} // namespace build_service::reader

#endif // ISEARCH_BS_FILEPATTERNBASE_H
