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

#include <stdint.h>
#include <string>

#include "autil/legacy/jsonizable.h"

namespace indexlib { namespace file_system {

struct FileInfo : public autil::legacy::Jsonizable {
public:
    FileInfo() : fileLength(-1), modifyTime((uint64_t)-1) {};

    FileInfo(const std::string& _filePath, int64_t _fileLength = -1, uint64_t _modifyTime = (uint64_t)-1)
        : filePath(_filePath)
        , fileLength(_fileLength)
        , modifyTime(_modifyTime)
    {
    }

    ~FileInfo() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("path", filePath, filePath);
        json.Jsonize("file_length", fileLength, fileLength);
        json.Jsonize("modify_time", modifyTime, modifyTime);
    }

public:
    bool operator==(const FileInfo& other) const
    {
        return filePath == other.filePath && fileLength == other.fileLength && modifyTime == other.modifyTime;
    }
    bool operator!=(const FileInfo& other) const { return !operator==(other); }

    static bool IsPathEqual(const FileInfo& lhs, const FileInfo& rhs) { return lhs.filePath == rhs.filePath; }

    static bool PathCompare(const FileInfo& lhs, const FileInfo& rhs) { return lhs.filePath < rhs.filePath; }

    bool operator<(const FileInfo& other) const = delete;

    bool isValid() const { return isDirectory() || fileLength != -1; }
    bool isDirectory() const { return *(filePath.rbegin()) == '/'; }
    bool isFile() const { return !isDirectory(); }

public:
    std::string filePath;
    int64_t fileLength;
    uint64_t modifyTime;
};
}} // namespace indexlib::file_system
