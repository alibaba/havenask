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

#include "autil/StringView.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/file/CompressBlockDataRetriever.h"

namespace indexlib { namespace file_system {

class IntegratedCompressBlockDataRetriever final : public CompressBlockDataRetriever
{
public:
    IntegratedCompressBlockDataRetriever(const ReadOption& option,
                                         const std::shared_ptr<CompressFileInfo>& compressInfo,
                                         CompressFileAddressMapper* compressAddrMapper, FileReader* dataFileReader,
                                         autil::mem_pool::Pool* pool);
    ~IntegratedCompressBlockDataRetriever();

    IntegratedCompressBlockDataRetriever(const IntegratedCompressBlockDataRetriever&) = delete;
    IntegratedCompressBlockDataRetriever& operator=(const IntegratedCompressBlockDataRetriever&) = delete;
    IntegratedCompressBlockDataRetriever(IntegratedCompressBlockDataRetriever&&) = delete;
    IntegratedCompressBlockDataRetriever& operator=(IntegratedCompressBlockDataRetriever&&) = delete;

public:
    uint8_t* RetrieveBlockData(size_t fileOffset, size_t& blockDataBeginOffset,
                               size_t& blockDataLength) noexcept(false) override;
    void Reset() noexcept override;
    future_lite::coro::Lazy<ErrorCode> Prefetch(size_t fileOffset, size_t length) noexcept override
    {
        co_return ErrorCode::FSEC_OK;
    }

private:
    void ReleaseBuffer() noexcept;

private:
    char* _baseAddress;
    std::vector<autil::StringView> _addrVec;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IntegratedCompressBlockDataRetriever> IntegratedCompressBlockDataRetrieverPtr;
}} // namespace indexlib::file_system
