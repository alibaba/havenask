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

#include "build_service/common_define.h"
#include "build_service/reader/FilePatternBase.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class FilePattern : public FilePatternBase
{
public:
    FilePattern(const std::string& dirPath, const std::string& prefix, const std::string& suffix,
                uint32_t fileCount = 0);
    ~FilePattern();

public:
    CollectResults calculateHashIds(const CollectResults& results) const override;

public:
    bool operator==(const FilePattern& other) const;

public:
    void setPrefix(const std::string& prefix) { _prefix = prefix; }
    const std::string& getPrefix() const { return _prefix; }

    void setSuffix(const std::string& suffix) { _suffix = suffix; }
    const std::string& getSuffix() const { return _suffix; }

    void setFileCount(uint32_t count) { _fileCount = count; }
    uint32_t getFileCount() const { return _fileCount; }

    void setRootPath(const std::string& rootPath) { _rootPath = rootPath; }
    const std::string& getRootPath() const { return _rootPath; }

private:
    bool extractFileId(const std::string& fileName, uint32_t& fileId) const;

private:
    std::string _prefix;
    std::string _suffix;
    uint32_t _fileCount;
    std::string _rootPath;

private:
    BS_LOG_DECLARE();
};
BS_TYPEDEF_PTR(FilePattern);

}} // namespace build_service::reader
