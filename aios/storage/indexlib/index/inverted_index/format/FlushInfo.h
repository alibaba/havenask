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

#include "indexlib/index/inverted_index/Types.h"

namespace indexlib::index {

class FlushInfo
{
private:
#define SET_BIT_VALUE(mask, offset, value) _flushInfo = (_flushInfo & ~mask) | ((uint64_t)value << offset)

#define GET_BIT_VALUE(mask, offset) (_flushInfo & mask) >> offset

public:
    FlushInfo() { Reset(); }
    ~FlushInfo() = default;

    FlushInfo(const FlushInfo& flushInfo) { _flushInfo = flushInfo._flushInfo; }

    bool IsValidShortBuffer() const { return GET_BIT_VALUE(MASK_IS_VALID_SB, OFFSET_IS_VALID_SB); }

    void SetIsValidShortBuffer(bool isValid)
    {
        uint64_t isValidShortBuffer = isValid ? 1 : 0;
        SET_BIT_VALUE(MASK_IS_VALID_SB, OFFSET_IS_VALID_SB, isValidShortBuffer);
    }

    uint32_t GetFlushLength() const { return GET_BIT_VALUE(MASK_FLUSH_LENGTH, OFFSET_FLUSH_LENGTH); }
    void SetFlushLength(uint32_t flushLength) { SET_BIT_VALUE(MASK_FLUSH_LENGTH, OFFSET_FLUSH_LENGTH, flushLength); }
    uint8_t GetCompressMode() const { return GET_BIT_VALUE(MASK_COMPRESS_MODE, OFFSET_COMPRESS_MODE); }
    void SetCompressMode(uint8_t compressMode)
    {
        SET_BIT_VALUE(MASK_COMPRESS_MODE, OFFSET_COMPRESS_MODE, compressMode);
    }

    uint32_t GetFlushCount() const { return GET_BIT_VALUE(MASK_FLUSH_COUNT, OFFSET_FLUSH_COUNT); }
    void SetFlushCount(uint32_t flushCount) { SET_BIT_VALUE(MASK_FLUSH_COUNT, OFFSET_FLUSH_COUNT, flushCount); }
    void Reset()
    {
        _flushInfo = 0;
        SetCompressMode(SHORT_LIST_COMPRESS_MODE);
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

    uint64_t volatile _flushInfo;
};

} // namespace indexlib::index
