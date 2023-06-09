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
#include <stdint.h>

#include "autil/Log.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileWriter.h"

namespace indexlib { namespace file_system {
class PackageDiskStorage;
struct WriterOption;

// TODO
class BufferedPackageFileWriter : public BufferedFileWriter
{
public:
    BufferedPackageFileWriter(PackageDiskStorage* packageStorage, size_t packageUnitId,
                              const WriterOption& writerOption = WriterOption(),
                              UpdateFileSizeFunc&& updateFileSizeFunc = nullptr);
    ~BufferedPackageFileWriter();

public:
    ErrorCode DoOpen() noexcept override;
    ErrorCode DoClose() noexcept override;

private:
    FSResult<void> DumpBuffer() noexcept override;

private:
    PackageDiskStorage* _storage;
    size_t _unitId;
    uint32_t _physicalFileId;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BufferedPackageFileWriter> BufferedPackageFileWriterPtr;
}} // namespace indexlib::file_system
