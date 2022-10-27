#ifndef __INDEXLIB_CHUNK_DECODER_H
#define __INDEXLIB_CHUNK_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/chunk/chunk_define.h"

IE_NAMESPACE_BEGIN(common);

enum ChunkDecoderType
{
    ct_plain_integrated = 0,
    ct_plain_slice = 1,
    ct_encoded = 2,
};

class ChunkDecoder
{
public:
    ChunkDecoder(ChunkDecoderType type, uint32_t chunkLen, uint32_t fixedRecordLen)
        : mType(type)
        , mChunkLength(chunkLen)
        , mFixedRecordLen(fixedRecordLen)
    {}
    virtual ~ChunkDecoder() {}

public:
    ChunkDecoderType GetType() const
    { return mType; }

    uint32_t GetChunkLength() const
    { return mChunkLength; }

protected:
    ChunkDecoderType mType;
    uint32_t mChunkLength;
    uint32_t mFixedRecordLen;
};

DEFINE_SHARED_PTR(ChunkDecoder);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CHUNK_DECODER_H
