#ifndef __INDEXLIB_SLICE_PLAIN_CHUNK_DECODER_H
#define __INDEXLIB_SLICE_PLAIN_CHUNK_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/chunk/chunk_decoder.h"
#include "indexlib/common/fixed_size_byte_slice_list_reader.h"
#include <autil/MultiValueType.h>

IE_NAMESPACE_BEGIN(common);

class SlicePlainChunkDecoder : public ChunkDecoder
{
public:
    SlicePlainChunkDecoder(const util::ByteSliceList* sliceList,
                           autil::mem_pool::Pool* pool)
        : ChunkDecoder(ct_plain_slice, 0, 0) {}
    ~SlicePlainChunkDecoder() {}
public:
    void GetValue(autil::ConstString& value, uint32_t inChunkPosition)
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
        /* value = autil::ConstString((char*)baseAddr, len); */
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

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SLICE_PLAIN_CHUNK_DECODER_H
