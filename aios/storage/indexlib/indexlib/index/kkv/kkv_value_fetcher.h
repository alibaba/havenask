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
#ifndef __INDEXLIB_KKV_VALUE_FETCHER_H
#define __INDEXLIB_KKV_VALUE_FETCHER_H

#include <memory>

#include "autil/NoCopyable.h"
#include "indexlib/common/chunk/chunk_file_decoder.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/index/kkv/kkv_cache_item.h"
#include "indexlib/index/kkv/kkv_coro_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlib { namespace index {

class KKVValueFetcher : private autil::NoCopyable
{
private:
    using ChunkFileDecoderPtr = util::PooledUniquePtr<common::ChunkFileDecoder>;
    using CompressFileReaderPtr = util::PooledUniquePtr<file_system::CompressFileReader>;

public:
    KKVValueFetcher() = default;
    KKVValueFetcher(CompressFileReaderPtr compressReader, ChunkFileDecoderPtr valueDecoder,
                    file_system::ReadOption readOption)
        : mCompressReader(std::move(compressReader))
        , mValueDecoder(std::move(valueDecoder))
        , mReadOption(readOption) {};

    KKVValueFetcher(KKVValueFetcher&& other)
        : mCompressReader(std::move(other.mCompressReader))
        , mValueDecoder(std::move(other.mValueDecoder))
        , mReadOption(other.mReadOption)
    {
    }
    KKVValueFetcher& operator=(KKVValueFetcher&& other)
    {
        if (this != &other) {
            mCompressReader = std::move(other.mCompressReader);
            mValueDecoder = std::move(other.mValueDecoder);
            mReadOption = other.mReadOption;
        }
        return *this;
    }

    operator bool() const { return mValueDecoder.get(); }

public:
    FL_LAZY(future_lite::Unit)
    FetchValues(typename KKVDocs::iterator beginIter, typename KKVDocs::iterator endIter)
    {
        if (!mValueDecoder) {
            FL_CORETURN future_lite::Unit {};
        }

        util::PooledUniquePtr<common::ChunkDecoder> chunk;
        uint64_t lastChunkOffset = 0;
        for (auto iter = beginIter; iter != endIter; ++iter) {
            auto& doc = *iter;
            if (doc.skeyDeleted) {
                continue;
            }
            uint64_t chunkOffset = 0;
            uint32_t inChunkPosition = 0;
            ResolveSkeyOffset(doc.valueOffset, chunkOffset, inChunkPosition);
            autil::StringView value;
            // TODO: use batch readRecord?
            if (!chunk || lastChunkOffset != chunkOffset) {
                lastChunkOffset = chunkOffset;
                chunk = FL_COAWAIT mValueDecoder->GetChunk(chunkOffset, mReadOption);
            }
            assert(chunk->GetType() == common::ct_plain_integrated);
            auto typedChunk = static_cast<common::IntegratedPlainChunkDecoder*>(chunk.get());
            typedChunk->Seek(inChunkPosition);
            typedChunk->ReadRecord(value);
            doc.SetValue(value);
        }
        FL_CORETURN future_lite::Unit {};
    }

private:
    CompressFileReaderPtr mCompressReader;
    ChunkFileDecoderPtr mValueDecoder;
    file_system::ReadOption mReadOption;
};
}} // namespace indexlib::index

#endif //__INDEXLIB_KKV_VALUE_FETCHER_H
