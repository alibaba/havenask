#ifndef __INDEXLIB_INTEGRATED_PLAIN_CHUNK_DECODER_H
#define __INDEXLIB_INTEGRATED_PLAIN_CHUNK_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/chunk/chunk_decoder.h"
#include <autil/MultiValueType.h>

IE_NAMESPACE_BEGIN(common);

class IntegratedPlainChunkDecoder : public ChunkDecoder
{
public:
    IntegratedPlainChunkDecoder(const char* baseAddress, uint32_t chunkLen, uint32_t fixedRecordLen)
        : ChunkDecoder(ct_plain_integrated, chunkLen, fixedRecordLen)
        , mCursor(0)
        , mBaseAddress(baseAddress)
    {}
    
    ~IntegratedPlainChunkDecoder() {}
public:
    void Seek(uint32_t position)
    { mCursor = position; }

    int64_t Read(autil::ConstString& value, uint32_t len)
    {
        value = autil::ConstString(mBaseAddress + mCursor, len);
        mCursor += len;
        assert(mCursor <= mChunkLength);
        return len;
    }

    int64_t ReadRecord(autil::ConstString& value)
    {
        int64_t readSize = 0;
        if (mFixedRecordLen == 0)
        {
            autil::MultiChar multiChar;
            multiChar.init(mBaseAddress + mCursor);
            value = autil::ConstString(multiChar.data(), multiChar.size());
            auto length = multiChar.length();
            mCursor += length;
            readSize = length;
        }
        else
        {
            value = autil::ConstString(mBaseAddress + mCursor, mFixedRecordLen);
            mCursor += mFixedRecordLen;
            readSize = mFixedRecordLen;
        }
        return readSize;
    }

    bool IsEnd() const
    { return mCursor == mChunkLength; }
    
private:
    uint32_t mCursor;
    const char* mBaseAddress;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IntegratedPlainChunkDecoder);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_INTEGRATED_PLAIN_CHUNK_DECODER_H
