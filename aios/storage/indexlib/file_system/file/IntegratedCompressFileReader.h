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

#include "autil/Log.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/file/FileReader.h"

namespace autil { namespace mem_pool {
class Pool;
}} // namespace autil::mem_pool
namespace indexlib { namespace file_system {
class CompressFileInfo;
class Directory;
struct ReadOption;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class IntegratedCompressFileReader : public CompressFileReader
{
public:
    IntegratedCompressFileReader(autil::mem_pool::Pool* pool = NULL) noexcept
        : CompressFileReader(pool)
        , _dataAddr(NULL)
    {
    }

    ~IntegratedCompressFileReader() noexcept {}

public:
    bool Init(const std::shared_ptr<FileReader>& fileReader, const std::shared_ptr<FileReader>& metaReader,
              const std::shared_ptr<CompressFileInfo>& compressInfo, IDirectory* directory) noexcept(false) override;

    IntegratedCompressFileReader* CreateSessionReader(autil::mem_pool::Pool* pool) noexcept override;

    std::string GetLoadTypeString() const noexcept override { return std::string("integrated"); }

    FSResult<void> Close() noexcept override
    {
        auto ret = CompressFileReader::Close();
        _dataAddr = NULL;
        return ret;
    };

private:
    void LoadBuffer(size_t offset, ReadOption option) noexcept(false) override;
    void LoadBufferFromMemory(size_t offset, uint8_t* buffer, uint32_t bufLen,
                              bool enableTrace) noexcept(false) override;
    future_lite::coro::Lazy<std::vector<ErrorCode>>
    BatchLoadBuffer(const std::vector<std::pair<size_t, util::BufferCompressor*>>& blockInfo,
                    ReadOption option) noexcept override;
    bool DecompressOneBlock(size_t blockId, util::BufferCompressor* compressor) const noexcept(false);

private:
    char* _dataAddr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IntegratedCompressFileReader> IntegratedCompressFileReaderPtr;
}} // namespace indexlib::file_system
