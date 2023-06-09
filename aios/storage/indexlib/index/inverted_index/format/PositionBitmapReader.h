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

#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/util/Bitmap.h"
#include "indexlib/util/NumericUtil.h"

namespace indexlib::index {

struct PosCountInfo {
    uint32_t preDocAggPosCount;
    uint32_t currentDocPosCount;
};

class PositionBitmapReader
{
public:
    PositionBitmapReader();
    ~PositionBitmapReader();

    uint32_t Init(const util::ByteSliceList* sliceList, uint32_t offset);
    uint32_t Init(util::ByteSlice* sliceList, uint32_t offset);

    inline PosCountInfo GetPosCountInfo(uint32_t seekedDocCount);

    uint32_t GetBitmapListSize() const { return _bitmapListSize; }

private:
    uint32_t DoInit();

    inline uint32_t GetKthSignedBitPos(uint32_t k);

    inline uint32_t GetNextSignedBitDistance(uint32_t begin);

    inline int32_t KthHighBitIdx(uint8_t value, uint32_t k);

    inline int32_t KthHighBitIdx(uint32_t value, uint32_t& signedBitCount, uint32_t& k);

    inline uint32_t PopulationCount(uint32_t value)
    {
        return PopulationCount((uint8_t)value) + PopulationCount((uint8_t)(value >> 8)) +
               PopulationCount((uint8_t)(value >> 16)) + PopulationCount((uint8_t)(value >> 24));
    }

    inline uint8_t PopulationCount(uint8_t value)
    {
        static const uint8_t countTable[256] = {
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};
        return countTable[value];
    }

    friend class BitmapPositionReaderTest;

    file_system::ByteSliceReader _sliceReader;
    util::Bitmap _bitmap;
    uint32_t _blockCount;
    uint32_t* _blockOffsets;
    bool _ownBlockOffsets;

    uint32_t* _bitmapSlots;
    bool _ownBitmapSlots;

    uint32_t _bitmapListSize;
    uint32_t _totalBitCount;

    int32_t _lastBlockIndex;
    uint32_t _currentSlotIndex;
    uint32_t _preSlotSignedBitSum;

    AUTIL_LOG_DECLARE();
};

inline PosCountInfo PositionBitmapReader::GetPosCountInfo(uint32_t seekedDocCount)
{
    PosCountInfo info;
    info.preDocAggPosCount = GetKthSignedBitPos(seekedDocCount);
    info.currentDocPosCount = GetNextSignedBitDistance(info.preDocAggPosCount + 1);
    return info;
}

inline int32_t PositionBitmapReader::KthHighBitIdx(uint8_t value, uint32_t k)
{
    for (int32_t i = 7; i >= 0; --i) {
        if (value & (1 << i)) {
            k--;
            if (k == 0) {
                return i;
            }
        }
    }
    return 0;
}

inline int32_t PositionBitmapReader::KthHighBitIdx(uint32_t value, uint32_t& signedBitCount, uint32_t& k)
{
    uint32_t totalCount = 0;
    for (int32_t shift = 24; shift >= 0; shift -= 8) {
        uint8_t byte = value >> shift;
        uint32_t count = PopulationCount(byte);
        totalCount += count;
        if (count >= k) {
            return KthHighBitIdx(byte, k) + shift;
        } else {
            k -= count;
        }
    }
    signedBitCount += totalCount;
    return -1;
}

inline uint32_t PositionBitmapReader::GetKthSignedBitPos(uint32_t k)
{
    uint32_t position = 0;
    uint32_t blockIndex = (k - 1) / MAX_DOC_PER_BITMAP_BLOCK;
    assert(blockIndex <= _blockCount);

    _lastBlockIndex = blockIndex;
    uint32_t blockBeginPos = blockIndex == 0 ? 0 : _blockOffsets[blockIndex - 1] + 1;

    _currentSlotIndex = blockBeginPos >> util::Bitmap::SLOT_SIZE_BIT_NUM;
    uint32_t bitmapSlot = _bitmapSlots[_currentSlotIndex];
    // since we treat bitmap as 32 bit integers, so when a block's begin
    // position not aligned to 32, we will have som extra bit belong to
    // pre block
    uint32_t extraBit = blockBeginPos & util::Bitmap::SLOT_SIZE_BIT_MASK;

    _preSlotSignedBitSum = blockIndex << MAX_DOC_PER_BITMAP_BLOCK_BIT_NUM;
    if (extraBit != 0) {
        _preSlotSignedBitSum -= PopulationCount(bitmapSlot >> (util::Bitmap::SLOT_SIZE - extraBit));
    }
    position = blockBeginPos - extraBit;
    uint32_t remainSignedBit = k - _preSlotSignedBitSum;

    while (true) {
        // attempt to find kth high signed bit in this slot
        // if we can't find, we will skip to next slot, and add current
        // slot's signed bit count to _preSlotSignedBitSum, and subtract
        // it from remain signed bit count
        int32_t bitPos = KthHighBitIdx(bitmapSlot, _preSlotSignedBitSum, remainSignedBit);
        if (bitPos == -1) {
            position += util::Bitmap::SLOT_SIZE;
            bitmapSlot = _bitmapSlots[++_currentSlotIndex];
        } else {
            position += util::Bitmap::SLOT_SIZE - bitPos - 1;
            break;
        }
    }
    return position;
}

inline uint32_t PositionBitmapReader::GetNextSignedBitDistance(uint32_t begin)
{
    uint32_t dist = 1;
    uint32_t curBit = begin;
    uint32_t blockEndPos =
        _lastBlockIndex == (int32_t)_blockCount ? _totalBitCount - 1 : _blockOffsets[_lastBlockIndex];

    while (curBit <= blockEndPos) {
        if (_bitmap.Test(curBit)) {
            break;
        }
        dist++;
        curBit++;
    }
    return dist;
}

} // namespace indexlib::index
