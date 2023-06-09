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

#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"

namespace indexlib::index {

class ShortListOptimizeUtil
{
private:
    enum DictInlineState {
        /*reserved state 0x00*/
        UNKNOWN_STATE = 0x0,
        STRAT_DOCID_FIRST_DOC_LIST = 0x01,
        DF_FIRST_DOC_LIST = 0x02,
        SINGLE_DOC = 0x03,
    };
    static constexpr uint64_t OFFSET_MARK = 0x0fffffffffffffff; // used 4 bit.

public:
    ShortListOptimizeUtil();
    ~ShortListOptimizeUtil();

    //////////////// dict value util /////////////////////////////////////
    static bool GetOffset(dictvalue_t dictValue, int64_t& offset)
    {
        bool isDocList = false;
        bool dfFirst = true;
        if (IsDictInlineCompressMode(dictValue, isDocList, dfFirst)) {
            return false;
        }
        offset = dictValue & OFFSET_MARK;
        return true;
    }

    static int64_t GetDictInlineValue(dictvalue_t dictValue) { return dictValue & OFFSET_MARK; }

    static uint8_t GetCompressMode(dictvalue_t dictValue) { return (uint8_t)((uint64_t)dictValue >> 60); }

    static dictvalue_t CreateDictInlineValue(uint64_t compressValue, bool isDocList, bool dfFirst)
    {
        uint8_t mode = DICT_INLINE_COMPRESS_MODE;
        mode <<= 2;
        if (isDocList) {
            mode |= dfFirst ? DF_FIRST_DOC_LIST : STRAT_DOCID_FIRST_DOC_LIST;
        } else {
            mode |= SINGLE_DOC;
        }
        return (uint64_t)compressValue | ((uint64_t)mode) << 60;
    }

    static bool IsDictInlineCompressMode(dictvalue_t dictValue, bool& isDocList, bool& dfFirst)
    {
        uint8_t mode = GetCompressMode(dictValue);
        uint8_t docCompressMode = GetDocCompressMode(mode);
        if (docCompressMode != DICT_INLINE_COMPRESS_MODE) {
            isDocList = false;
            dfFirst = true;
            return false;
        }
        uint8_t dictLineState = GetPosCompressMode(mode);
        assert(dictLineState != UNKNOWN_STATE);
        if (dictLineState == SINGLE_DOC) {
            isDocList = false;
            dfFirst = true;
            return true;
        }
        isDocList = true;
        dfFirst = dictLineState == DF_FIRST_DOC_LIST ? true : false;
        return true;
    }

    static dictvalue_t CreateDictValue(uint8_t compressMode, int64_t offset)
    {
        return (uint64_t)offset | ((uint64_t)compressMode) << 60;
    }

    //////////////////////////////////////////////////////////////////////
    //////////////// compress mode util //////////////////////////////////

    static uint8_t GetPosCompressMode(uint8_t compressMode) { return compressMode & 0x03; }

    static uint8_t GetDocCompressMode(uint8_t compressMode) { return compressMode >> 2; }

    static uint8_t GetPosListCompressMode(ttf_t ttf)
    {
        return IsShortPosList(ttf) ? SHORT_LIST_COMPRESS_MODE : PFOR_DELTA_COMPRESS_MODE;
    }

    static uint8_t GetDocListCompressMode(df_t df, CompressMode longListCompressMode)
    {
        return IsShortDocList(df) ? SHORT_LIST_COMPRESS_MODE : longListCompressMode;
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
        return IsShortSkipList(size) ? SHORT_LIST_COMPRESS_MODE : PFOR_DELTA_COMPRESS_MODE;
    }

    //////////////////////////////////////////////////////////////////////

    static bool IsShortDocList(df_t df) { return df <= (df_t)MAX_UNCOMPRESSED_DOC_LIST_SIZE; }

    static bool IsShortPosList(ttf_t ttf) { return ttf <= (ttf_t)MAX_UNCOMPRESSED_POS_LIST_SIZE; }

    static bool IsShortSkipList(uint32_t size) { return size <= MAX_UNCOMPRESSED_SKIP_LIST_SIZE; }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
