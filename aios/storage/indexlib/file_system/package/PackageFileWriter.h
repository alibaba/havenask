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

#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/file_system/file/FileWriter.h"

namespace indexlib { namespace file_system {
struct WriterOption;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class PackageFileWriter
{
public:
    PackageFileWriter(const std::string& filePath) : _filePath(filePath), _isClosed(false) {}

    virtual ~PackageFileWriter() {}

public:
    virtual std::shared_ptr<FileWriter> CreateInnerFileWriter(const std::string& filePath,
                                                              const WriterOption& param = WriterOption()) = 0;

    virtual void MakeInnerDir(const std::string& dirPath) = 0;

    virtual void GetPhysicalFiles(std::vector<std::string>& files) const = 0;

    FSResult<void> Close() noexcept
    {
        autil::ScopedLock scopeLock(_lock);
        if (!_isClosed) {
            RETURN_IF_FS_ERROR(DoClose(), "DoClose failed");
            _isClosed = true;
        }
        return FSEC_OK;
    }

    bool IsClosed() const { return _isClosed; }

    const std::string& GetFilePath() const { return _filePath; }

protected:
    virtual ErrorCode DoClose() = 0;

protected:
    autil::RecursiveThreadMutex _lock;
    std::string _filePath;
    bool _isClosed;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PackageFileWriter> PackageFileWriterPtr;
}} // namespace indexlib::file_system
