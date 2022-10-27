#ifndef __INDEXLIB_FLUSH_INFO_H
#define __INDEXLIB_FLUSH_INFO_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_define.h"

IE_NAMESPACE_BEGIN(index);

class FlushInfo
{
public:
    FlushInfo() { Reset(); }
    ~FlushInfo() {}

    FlushInfo(const FlushInfo& flushInfo)
    { mFlushInfo = flushInfo.mFlushInfo; }

private:
#define SET_BIT_VALUE(mask, offset, value)                              \
    mFlushInfo = (mFlushInfo & ~mask) | ((uint64_t)value << offset)

#define GET_BIT_VALUE(mask, offset)             \
    (mFlushInfo & mask) >> offset

public:
    bool IsValidShortBuffer() const
    { return GET_BIT_VALUE(MASK_IS_VALID_SB, OFFSET_IS_VALID_SB); }

    void SetIsValidShortBuffer(bool isValid)
    { 
        uint64_t isValidShortBuffer = isValid ? 1 : 0;
        SET_BIT_VALUE(MASK_IS_VALID_SB, OFFSET_IS_VALID_SB, isValidShortBuffer); 
    }
    
    uint32_t GetFlushLength() const
    { return GET_BIT_VALUE(MASK_FLUSH_LENGTH, OFFSET_FLUSH_LENGTH); }

    void SetFlushLength(uint32_t flushLength)
    { SET_BIT_VALUE(MASK_FLUSH_LENGTH, OFFSET_FLUSH_LENGTH, flushLength); }

    uint8_t GetCompressMode() const
    { return GET_BIT_VALUE(MASK_COMPRESS_MODE, OFFSET_COMPRESS_MODE); }

    void SetCompressMode(uint8_t compressMode)
    { SET_BIT_VALUE(MASK_COMPRESS_MODE, OFFSET_COMPRESS_MODE, compressMode); }

    uint32_t GetFlushCount() const
    { return GET_BIT_VALUE(MASK_FLUSH_COUNT, OFFSET_FLUSH_COUNT); }

    void SetFlushCount(uint32_t flushCount)
    { SET_BIT_VALUE(MASK_FLUSH_COUNT, OFFSET_FLUSH_COUNT, flushCount); }

    void Reset() 
    { 
        mFlushInfo = 0; 
        SetCompressMode(index::SHORT_LIST_COMPRESS_MODE); 
    }

private:
    static const uint64_t OFFSET_IS_VALID_SB = 0;
    static const uint64_t OFFSET_FLUSH_LENGTH = 1;
    static const uint64_t OFFSET_COMPRESS_MODE = 32;
    static const uint64_t OFFSET_FLUSH_COUNT = 34;

    static const uint64_t MASK_IS_VALID_SB = 0x1;
    static const uint64_t MASK_FLUSH_LENGTH = 0xFFFFFFFE;
    static const uint64_t MASK_COMPRESS_MODE = 0x300000000;
    static const uint64_t MASK_FLUSH_COUNT = 0xFFFFFFFC00000000;

private:
    uint64_t volatile mFlushInfo;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FlushInfo);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FLUSH_INFO_H
