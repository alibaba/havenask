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
#ifndef __INDEXLIB_CHUNK_DECODER_CREATOR_H
#define __INDEXLIB_CHUNK_DECODER_CREATOR_H

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "future_lite/CoroInterface.h"
#include "indexlib/common/chunk/chunk_decoder.h"
#include "indexlib/common/chunk/chunk_define.h"
#include "indexlib/common/chunk/integrated_plain_chunk_decoder.h"
#include "indexlib/common/chunk/simple_cached_chunk_decoder.h"
#include "indexlib/common/chunk/slice_plain_chunk_decoder.h"
#include "indexlib/common/fixed_size_byte_slice_list_reader.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib { namespace common {

class ChunkDecoderCreator
{
public:
    ChunkDecoderCreator();
    ~ChunkDecoderCreator();

public:
    // recordLen 0 means record has varied length, for integrated reader
    static ChunkDecoder* Create(const char* baseAddr, uint64_t offset, uint32_t recordLen, autil::mem_pool::Pool* pool);

    // recordLen 0 means record has varied length
    static ChunkDecoder* Create(file_system::FileReader* fileReader, file_system::FSOpenType openType, uint64_t offset,
                                uint32_t recordLen, autil::mem_pool::Pool* pool, file_system::ReadOption option);

    static FL_LAZY(ChunkDecoder*)
        CreateAsync(file_system::FileReader* fileReader, file_system::FSOpenType openType, uint64_t offset,
                    uint32_t recordLen, autil::mem_pool::Pool* pool, file_system::ReadOption option);

    static void Reset() { mSimpleCacheDecoder.Reset(); }

private:
    static SimpleCachedChunkDecoder mSimpleCacheDecoder;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ChunkDecoderCreator);

//////////////////////////////////////////////////////////////////////
inline ChunkDecoder* ChunkDecoderCreator::Create(const char* baseAddr, uint64_t offset, uint32_t recordLen,
                                                 autil::mem_pool::Pool* pool)
{
    const ChunkMeta& meta = *(ChunkMeta*)(baseAddr + offset);
    return IE_POOL_COMPATIBLE_NEW_CLASS(pool, IntegratedPlainChunkDecoder, baseAddr + offset + sizeof(meta),
                                        meta.length, recordLen);
}

inline ChunkDecoder* ChunkDecoderCreator::Create(file_system::FileReader* fileReader, file_system::FSOpenType openType,
                                                 uint64_t offset, uint32_t recordLen, autil::mem_pool::Pool* pool,
                                                 file_system::ReadOption option)
{
    return future_lite::interface::syncAwait(CreateAsync(fileReader, openType, offset, recordLen, pool, option));
}

inline FL_LAZY(ChunkDecoder*) ChunkDecoderCreator::CreateAsync(file_system::FileReader* fileReader,
                                                               file_system::FSOpenType openType, uint64_t offset,
                                                               uint32_t recordLen, autil::mem_pool::Pool* pool,
                                                               file_system::ReadOption option)
{
    assert(fileReader && fileReader->GetOpenType() == openType);
    if (openType == file_system::FSOT_BUFFERED) {
        // for merge
        ChunkMeta meta;
        autil::StringView data;
        mSimpleCacheDecoder.Read(fileReader, offset, pool, meta, data);
        if (meta.isEncoded) {
            assert(false);
            FL_CORETURN nullptr;
        }
        FL_CORETURN IE_POOL_COMPATIBLE_NEW_CLASS(pool, IntegratedPlainChunkDecoder, data.data(), meta.length,
                                                 recordLen);
    }

    ChunkMeta meta;
    (FL_COAWAIT fileReader->ReadAsyncCoro(&meta, sizeof(meta), offset, option)).GetOrThrow();
    if (meta.isEncoded) {
        assert(false);
        FL_CORETURN nullptr;
    }

    assert(fileReader->GetBaseAddress() == NULL);
    char* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, char, meta.length);
    if ((FL_COAWAIT fileReader->ReadAsyncCoro(buffer, meta.length, offset + sizeof(meta), option)).GetOrThrow() !=
        meta.length) {
        INDEXLIB_FATAL_ERROR(FileIO, "read chunk data [offset:%lu, length:%u] failed!", offset + sizeof(meta),
                             meta.length);
    }
    FL_CORETURN IE_POOL_COMPATIBLE_NEW_CLASS(pool, IntegratedPlainChunkDecoder, buffer, meta.length, recordLen);
}
}} // namespace indexlib::common

#endif //__INDEXLIB_CHUNK_DECODER_CREATOR_H
