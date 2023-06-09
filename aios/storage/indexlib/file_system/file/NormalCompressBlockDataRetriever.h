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

#include "indexlib/file_system/file/BlockPrefetcher.h"
#include "indexlib/file_system/file/CompressBlockDataRetriever.h"

namespace indexlib { namespace file_system {

class NormalCompressBlockDataRetriever final : public CompressBlockDataRetriever
{
public:
    NormalCompressBlockDataRetriever(const ReadOption& option, const std::shared_ptr<CompressFileInfo>& compressInfo,
                                     CompressFileAddressMapper* compressAddrMapper, FileReader* dataFileReader,
                                     autil::mem_pool::Pool* pool) noexcept;
    ~NormalCompressBlockDataRetriever() noexcept;

    NormalCompressBlockDataRetriever(const NormalCompressBlockDataRetriever&) = delete;
    NormalCompressBlockDataRetriever& operator=(const NormalCompressBlockDataRetriever&) = delete;
    NormalCompressBlockDataRetriever(NormalCompressBlockDataRetriever&&) = delete;
    NormalCompressBlockDataRetriever& operator=(NormalCompressBlockDataRetriever&&) = delete;

public:
    uint8_t* RetrieveBlockData(size_t fileOffset, size_t& blockDataBeginOffset,
                               size_t& blockDataLength) noexcept(false) override;
    void Reset() noexcept override;
    future_lite::coro::Lazy<ErrorCode> Prefetch(size_t fileOffset, size_t length) noexcept override;

private:
    void ReleaseBuffer() noexcept;

private:
    std::vector<autil::StringView> _addrVec;
    BlockPrefetcher _prefetcher;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<NormalCompressBlockDataRetriever> NormalCompressBlockDataRetrieverPtr;
}} // namespace indexlib::file_system
