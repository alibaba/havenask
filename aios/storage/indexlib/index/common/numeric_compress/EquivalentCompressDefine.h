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

#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
namespace indexlib::index {

enum SlotItemType {
    SIT_EQUAL = 1,
    SIT_DELTA_UINT8 = 0,
    SIT_DELTA_UINT16 = 2,
    SIT_DELTA_UINT32 = 4,
    SIT_DELTA_BIT1 = 5,
    SIT_DELTA_BIT2 = 6,
    SIT_DELTA_BIT4 = 7,
    SIT_DELTA_UINT64 = 3,
};

struct EquivalentCompressFileContent {
    uint32_t itemCount = 0;
    uint32_t slotBitNum = 0;
    uint32_t slotMask = 0;
    uint32_t slotNum = 0;
    uint64_t slotBaseOffset = 0;
    uint64_t valueBaseOffset = 0;
    uint64_t fileLength = 0;
    EquivalentCompressFileContent(uint32_t _itemCount, uint32_t _slotBitNum, uint32_t _slotMask, uint32_t _slotNum,
                                  uint64_t _slotBaseOffset, uint64_t _valueBaseOffset, uint64_t _fileLength)
        : itemCount(_itemCount)
        , slotBitNum(_slotBitNum)
        , slotMask(_slotMask)
        , slotNum(_slotNum)
        , slotBaseOffset(_slotBaseOffset)
        , valueBaseOffset(_valueBaseOffset)
        , fileLength(_fileLength)
    {
    }
    EquivalentCompressFileContent() {}
};

class EquivalentCompressFileFormat
{
public:
    inline static uint8_t DecodeBitValue(uint32_t itemCountMask, uint32_t itemCountPowerNum, uint32_t bitCountPowerNum,
                                         uint32_t bitMask, size_t valueIdx, uint8_t* deltaArray)
    {
        uint32_t alignSize = 8;
        uint32_t byteIndex = valueIdx >> itemCountPowerNum;
        uint32_t posInByte = valueIdx & itemCountMask;
        uint8_t byteValue = deltaArray[byteIndex];
        uint32_t bitMoveCount = alignSize - ((posInByte + 1) << bitCountPowerNum);
        byteValue = byteValue >> bitMoveCount;
        byteValue &= bitMask;
        return byteValue;
    }

    inline static size_t GetDeltaArraySizeBySlotItemType(SlotItemType slotType, size_t count)
    {
        switch (slotType) {
        case SIT_DELTA_UINT8: // uint8_t
        {
            return sizeof(uint8_t) * count;
        }
        case SIT_DELTA_UINT16: // uint16_t
        {
            return sizeof(uint16_t) * count;
        }
        case SIT_DELTA_UINT32: // uint32_t
        {
            return sizeof(uint32_t) * count;
        }
        case SIT_DELTA_UINT64: // uint64_t
        {
            return sizeof(uint64_t) * count;
        }
        case SIT_DELTA_BIT1: {
            return (count + 7) >> 3;
        }
        case SIT_DELTA_BIT2: {
            return ((count << 1) + 7) >> 3;
        }
        case SIT_DELTA_BIT4: {
            return ((count << 2) + 7) >> 3;
        }
        default:
            assert(false);
        }
        return 0;
    }

    inline static size_t GetDeltaArraySizeByDeltaType(uint64_t deltaType, size_t count)
    {
        switch (deltaType) {
        case 0: // uint8_t
        {
            return sizeof(uint8_t) * count;
        }
        case 1: // uint16_t
        {
            return sizeof(uint16_t) * count;
        }
        case 2: // uint32_t
        {
            return sizeof(uint32_t) * count;
        }
        case 3: // uint64_t
        {
            return sizeof(uint64_t) * count;
        }
        case 4: // 1 bit
        {
            return (count + 7) >> 3;
        }
        case 5: // 2 bits
        {
            return ((count << 1) + 7) >> 3;
        }
        case 6: // 4 bits
        {
            return ((count << 2) + 7) >> 3;
        }
        default:
            assert(false);
        }
        return 0;
    }

    template <typename ReturnType>
    inline static ReturnType GetDeltaValueBySlotItemType(SlotItemType slotType, uint8_t* deltaBuffer, size_t valueIdx)
    {
        switch (slotType) {
        case SIT_DELTA_UINT8: // uint8_t
        {
            return deltaBuffer[valueIdx];
        }
        case SIT_DELTA_UINT16: // uint16_t
        {
            return ((uint16_t*)deltaBuffer)[valueIdx];
        }
        case SIT_DELTA_UINT32: // uint32_t
        {
            return ((uint32_t*)deltaBuffer)[valueIdx];
        }
        case SIT_DELTA_UINT64: // uint64_t
        {
            return ((uint64_t*)deltaBuffer)[valueIdx];
        }
        case SIT_DELTA_BIT1: {
            return DecodeBitValue(7, 3, 0, 1, valueIdx, deltaBuffer);
        }
        case SIT_DELTA_BIT2: {
            return DecodeBitValue(3, 2, 1, 3, valueIdx, deltaBuffer);
        }
        case SIT_DELTA_BIT4: {
            return DecodeBitValue(1, 1, 2, 15, valueIdx, deltaBuffer);
        }
        default:
            assert(false);
        }
        return 0;
    }

    template <typename ReturnType>
    inline static ReturnType GetDeltaValueByDeltaType(uint64_t deltaType, uint8_t* deltaArray, size_t valueIdx)
    {
        switch (deltaType) {
        case 0: // uint8_t
        {
            return ((uint8_t*)deltaArray)[valueIdx];
        }
        case 1: // uint16_t
        {
            return ((uint16_t*)deltaArray)[valueIdx];
        }
        case 2: // uint32_t
        {
            return ((uint32_t*)deltaArray)[valueIdx];
        }
        case 3: // uint64_t
        {
            return ((uint64_t*)deltaArray)[valueIdx];
        }
        case 4: // 1 bit
        {
            return DecodeBitValue(7, 3, 0, 1, valueIdx, deltaArray);
        }
        case 5: // 2 bits
        {
            return DecodeBitValue(3, 2, 1, 3, valueIdx, deltaArray);
        }
        case 6: // 4 bits
        {
            return DecodeBitValue(1, 1, 2, 15, valueIdx, deltaArray);
        }
        default:
            assert(false);
        }
        return 0;
    }
};

struct EquivalentCompressSessionOption {
    size_t slotBufferSize = 8 * 1024;
    size_t valueBufferSize = 32 * 1024;
    size_t ioGapSize = 4 * 1024;
    size_t maxRecursionDepth = 100;
};

template <typename UT>
SlotItemType GetSlotItemType(UT maxDelta)
{
    if (maxDelta <= (UT)1) {
        return SIT_DELTA_BIT1;
    }
    if (maxDelta <= (UT)3) {
        return SIT_DELTA_BIT2;
    }
    if (maxDelta <= (UT)15) {
        return SIT_DELTA_BIT4;
    }
    if (maxDelta <= (UT)std::numeric_limits<uint8_t>::max()) {
        return SIT_DELTA_UINT8;
    }
    if (maxDelta <= (UT)std::numeric_limits<uint16_t>::max()) {
        return SIT_DELTA_UINT16;
    }
    if (maxDelta <= (UT)std::numeric_limits<uint32_t>::max()) {
        return SIT_DELTA_UINT32;
    }
    return SIT_DELTA_UINT64;
}

inline SlotItemType DeltaFlagToSlotItemType(uint64_t flag)
{
    switch (flag) {
    case 0:
        return SIT_DELTA_UINT8;
    case 1:
        return SIT_DELTA_UINT16;
    case 2:
        return SIT_DELTA_UINT32;
    case 3:
        return SIT_DELTA_UINT64;
    case 4:
        return SIT_DELTA_BIT1;
    case 5:
        return SIT_DELTA_BIT2;
    case 6:
        return SIT_DELTA_BIT4;
    default:
        assert(false);
    }
    return SIT_EQUAL;
}

inline uint64_t SlotItemTypeToDeltaFlag(SlotItemType slotType)
{
    switch (slotType) {
    case SIT_DELTA_UINT8: {
        return 0;
    }
    case SIT_DELTA_UINT16: {
        return 1;
    }
    case SIT_DELTA_UINT32: {
        return 2;
    }
    case SIT_DELTA_UINT64: {
        return 3;
    }
    case SIT_DELTA_BIT1: {
        return 4;
    }
    case SIT_DELTA_BIT2: {
        return 5;
    }
    case SIT_DELTA_BIT4: {
        return 6;
    }
    default:
        assert(false);
    }
    return 7;
}

inline size_t GetCompressDeltaBufferSize(SlotItemType slotType, uint32_t bufferItemCount)
{
    uint32_t alignSize = 8; // 8 bit
    switch (slotType) {
    case SIT_DELTA_BIT1: {
        return (bufferItemCount + alignSize - 1) / alignSize;
    }
    case SIT_DELTA_BIT2: {
        return (bufferItemCount * 2 + alignSize - 1) / alignSize;
    }
    case SIT_DELTA_BIT4: {
        return (bufferItemCount * 4 + alignSize - 1) / alignSize;
    }
    case SIT_DELTA_UINT8: {
        return bufferItemCount * sizeof(uint8_t);
    }
    case SIT_DELTA_UINT16: {
        return bufferItemCount * sizeof(uint16_t);
    }
    case SIT_DELTA_UINT32: {
        return bufferItemCount * sizeof(uint32_t);
    }
    case SIT_DELTA_UINT64: {
        return bufferItemCount * sizeof(uint64_t);
    }
    default:
        assert(false);
    }
    return 0;
}

struct SlotItem {
    SlotItemType slotType : 3;
    uint64_t value        : 61;
    bool IsEqual() const { return slotType == SIT_EQUAL; }
};

template <typename UT>
struct DeltaValueArrayTyped {
    UT baseValue;
    uint8_t delta[0];
};

/* uint64_t/int64_t type only*/
struct LongSlotItem {
    uint64_t isValue : 1;
    uint64_t value   : 63;
    bool IsEqual() const { return isValue == 1; }
};

/* uint64_t/int64_t type only*/
struct LongValueArrayHeader {
    uint64_t baseValue;
    uint64_t deltaType; // delta type
};

template <typename T>
class ItemIteratorTyped
{
public:
    ItemIteratorTyped(const T* data, uint32_t count) : _data(data), _count(count), _cursor(0) {}

    bool HasNext() const { return _cursor < _count; }

    std::pair<Status, T> Next()
    {
        assert(_data);
        assert(_cursor < _count);
        return std::make_pair(Status::OK(), _data[_cursor++]);
    }

private:
    const T* _data;
    uint32_t _count;
    uint32_t _cursor;
};

template <typename T>
struct SlotItemTraits {
    typedef SlotItem SlotArrayValueType;
};

#define DECLARE_LONG_SLOT_ITEM_TRAITS(TYPE)                                                                            \
    template <>                                                                                                        \
    struct SlotItemTraits<TYPE> {                                                                                      \
        typedef LongSlotItem SlotArrayValueType;                                                                       \
    }

DECLARE_LONG_SLOT_ITEM_TRAITS(double);
DECLARE_LONG_SLOT_ITEM_TRAITS(int64_t);
DECLARE_LONG_SLOT_ITEM_TRAITS(uint64_t);

} // namespace indexlib::index
