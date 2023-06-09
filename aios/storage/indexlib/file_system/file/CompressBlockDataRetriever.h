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

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/file/BlockDataRetriever.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/file/ReadOption.h"

namespace indexlib::util {
class BufferCompressor;
}

namespace indexlib { namespace file_system {
class FileReader;
class CompressFileAddressMapper;

class CompressBlockDataRetriever : public BlockDataRetriever
{
public:
    CompressBlockDataRetriever(const ReadOption& option, const std::shared_ptr<CompressFileInfo>& compressInfo,
                               CompressFileAddressMapper* compressAddrMapper, FileReader* dataFileReader,
                               autil::mem_pool::Pool* pool);
    ~CompressBlockDataRetriever();

    CompressBlockDataRetriever(const CompressBlockDataRetriever&) = delete;
    CompressBlockDataRetriever& operator=(const CompressBlockDataRetriever&) = delete;
    CompressBlockDataRetriever(CompressBlockDataRetriever&&) = delete;
    CompressBlockDataRetriever& operator=(CompressBlockDataRetriever&&) = delete;

protected:
    void DecompressBlockData(size_t blockIdx) noexcept(false);

protected:
    CompressFileAddressMapper* _compressAddrMapper;
    FileReader* _dataFileReader;
    util::BufferCompressor* _compressor;
    autil::mem_pool::Pool* _pool;
    ReadOption _readOption;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CompressBlockDataRetriever> CompressBlockDataRetrieverPtr;
}} // namespace indexlib::file_system
