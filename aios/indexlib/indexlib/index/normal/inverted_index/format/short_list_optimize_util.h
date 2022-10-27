#ifndef __INDEXLIB_SHORT_LIST_OPTIMIZE_UTIL_H
#define __INDEXLIB_SHORT_LIST_OPTIMIZE_UTIL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"

IE_NAMESPACE_BEGIN(index);

class ShortListOptimizeUtil
{
public:
    ShortListOptimizeUtil();
    ~ShortListOptimizeUtil();
public:
    //////////////// dict value util /////////////////////////////////////
    static bool GetOffset(dictvalue_t dictValue, int64_t& offset)
    {
        if (IsDictInlineCompressMode(dictValue))
        {
            return false;
        }
        offset = dictValue & OFFSET_MARK; 
        return true;
    }

    static int64_t GetDictInlineValue(dictvalue_t dictValue)
    {
        assert(IsDictInlineCompressMode(dictValue));
        return dictValue & OFFSET_MARK; 
    }

    static uint8_t GetCompressMode(dictvalue_t dictValue)
    { return (uint8_t)((uint64_t)dictValue >> 60); }

    static dictvalue_t CreateDictInlineValue(uint64_t compressValue)
    { 
        uint8_t mode = 0;
        mode = DICT_INLINE_COMPRESS_MODE;
        mode <<= 2;
        mode |= GetPosListCompressMode(0);
        return (uint64_t)compressValue | ((uint64_t)mode) << 60; 
    }

    static bool IsDictInlineCompressMode(dictvalue_t dictValue)
    {
        uint8_t mode = GetCompressMode(dictValue);
        uint8_t docCompressMode = GetDocCompressMode(mode);
        return docCompressMode == DICT_INLINE_COMPRESS_MODE;
    }
    
    static dictvalue_t CreateDictValue(uint8_t compressMode, int64_t offset)
    { return (uint64_t)offset | ((uint64_t)compressMode) << 60; }

    //////////////////////////////////////////////////////////////////////
    //////////////// compress mode util //////////////////////////////////

    static uint8_t GetPosCompressMode(uint8_t compressMode)
    { return compressMode & 0x03; }

    static uint8_t GetDocCompressMode(uint8_t compressMode)
    { return compressMode >> 2; }

    static uint8_t GetPosListCompressMode(ttf_t ttf)
    {
        return IsShortPosList(ttf) ? 
            SHORT_LIST_COMPRESS_MODE : PFOR_DELTA_COMPRESS_MODE;
    }

    static uint8_t GetDocListCompressMode(df_t df, CompressMode longListCompressMode)
    {
        return IsShortDocList(df) ? 
            SHORT_LIST_COMPRESS_MODE : longListCompressMode;
    }

    static uint8_t GetCompressMode(df_t df, ttf_t ttf, CompressMode longListCompressMode)
    {
        uint8_t mode = 0;
        mode = GetDocListCompressMode(df, longListCompressMode);
        mode <<= 2;
        mode |= GetPosListCompressMode(ttf);
        return mode;        
    }

    static uint8_t GetSkipListCompressMode(uint32_t size)
    {
        return IsShortSkipList(size) ?
            SHORT_LIST_COMPRESS_MODE : PFOR_DELTA_COMPRESS_MODE;
    }

    //////////////////////////////////////////////////////////////////////

    static bool IsShortDocList(df_t df)
    { return df <= (df_t)MAX_UNCOMPRESSED_DOC_LIST_SIZE; }

    static bool IsShortPosList(ttf_t ttf)
    { return ttf <= (ttf_t)MAX_UNCOMPRESSED_POS_LIST_SIZE; }
    
    static bool IsShortSkipList(uint32_t size)
    { return size <= MAX_UNCOMPRESSED_SKIP_LIST_SIZE; }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ShortListOptimizeUtil);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SHORT_LIST_OPTIMIZE_UTIL_H
