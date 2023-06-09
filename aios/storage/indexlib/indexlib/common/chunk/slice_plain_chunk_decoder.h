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
#ifndef __INDEXLIB_SLICE_PLAIN_CHUNK_DECODER_H
#define __INDEXLIB_SLICE_PLAIN_CHUNK_DECODER_H

#include <memory>

#include "autil/MultiValueType.h"
#include "indexlib/common/chunk/chunk_decoder.h"
#include "indexlib/common/fixed_size_byte_slice_list_reader.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace common {

class SlicePlainChunkDecoder : public ChunkDecoder
{
public:
    SlicePlainChunkDecoder(const util::ByteSliceList* sliceList, autil::mem_pool::Pool* pool)
        : ChunkDecoder(ct_plain_slice, 0, 0)
    {
    }
    ~SlicePlainChunkDecoder() {}

public:
    void GetValue(autil::StringView& value, uint32_t inChunkPosition)
    {
        assert(false);
        /* char bytes[5]; */
        /* mSliceListReader->Seek(inChunkPosition); */
        /* bytes[0] = mSliceListReader->ReadByte(); */
        /* size_t encodeCount = autil::MultiValueFormatter::getEncodedCountFromFirstByte(bytes[0]); */
        /* mSliceListReader->Read(bytes + 1, encodeCount - 1); */
        /* uint32_t len = autil::MultiValueFormatter::decodeCount(bytes, encodeCount); */

        /* // TODO : reserve buffer */
        /* void *baseAddr = NULL; */
        /* mSliceListReader->Seek(inChunkPosition + encodeCount); */
        /* mSliceListReader->ReadMayCopy(baseAddr, len); */
        /* value = autil::StringView((char*)baseAddr, len); */
    }

private:
    /* util::ByteSliceList* mSliceList; */
    /* autil::mem_pool::Pool* mPool; */
    /* common::FixedSizeByteSliceListReader* mSliceListReader; */
    /* char* mBaseAddress; */

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SlicePlainChunkDecoder);
}} // namespace indexlib::common

#endif //__INDEXLIB_SLICE_PLAIN_CHUNK_DECODER_H
