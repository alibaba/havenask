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

#include <stddef.h>
#include <string>
#include <sys/types.h>

#include "indexlib/file_system/package/InnerFileMeta.h"

namespace indexlib { namespace file_system {
class FileCarrier;
typedef std::shared_ptr<FileCarrier> FileCarrierPtr;

class PackageOpenMeta
{
public:
    PackageOpenMeta() = default;
    ~PackageOpenMeta() = default;

public:
    size_t GetOffset() const { return innerFileMeta.GetOffset(); }
    size_t GetLength() const { return innerFileMeta.GetLength(); }
    const std::string& GetLogicalFilePath() const { return logicalFilePath; }
    const std::string& GetPhysicalFilePath() const { return innerFileMeta.GetFilePath(); }
    void SetLogicalFilePath(const std::string& filePath) { logicalFilePath = filePath; }
    void SetPhysicalFilePath(const std::string& filePath) { innerFileMeta.SetFilePath(filePath); }
    ssize_t GetPhysicalFileLength() const { return physicalFileLength; }

    // for test
    bool operator==(const PackageOpenMeta& other) const
    {
        return logicalFilePath == other.logicalFilePath && innerFileMeta == other.innerFileMeta;
    }

public:
    InnerFileMeta innerFileMeta;
    std::string logicalFilePath;
    FileCarrierPtr physicalFileCarrier; // for dcache package optimize
    ssize_t physicalFileLength = -1;
};
}} // namespace indexlib::file_system
