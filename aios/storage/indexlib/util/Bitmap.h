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
#include <assert.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>

#include "autil/mem_pool/PoolBase.h"
#include "indexlib/util/NumericUtil.h"

namespace indexlib { namespace util {

class Bitmap
{
public:
    Bitmap(bool bSet = false, autil::mem_pool::PoolBase* pool = NULL);
    Bitmap(uint32_t nItemCount, bool bSet = false, autil::mem_pool::PoolBase* pool = NULL);
    Bitmap(const Bitmap& rhs);
    Bitmap& operator=(const Bitmap& rhs);
    virtual ~Bitmap();

public:
    bool Alloc(uint32_t nItemCount, bool bSet = false);
    void Clear();

    uint32_t Begin() const;
    uint32_t Next(uint32_t nIndex) const;
    bool Set(uint32_t nIndex);
    bool Reset(uint32_t nIndex);
    void ResetAll();
    void ResetAllAfter(uint32_t nIndex);
    uint32_t GetItemCount() const { return _itemCount; }
    virtual uint32_t GetValidItemCount() const { return _itemCount; }
    uint32_t Size() const;

    uint32_t GetSetCount() const;
    uint32_t GetSetCount(uint32_t begin, uint32_t end) const;

    void SetSetCount(uint32_t nCount);
    uint32_t GetUnsetCount() const;
    uint32_t GetSlotCount() const { return _slotCount; }
    uint32_t* GetData() const { return _data; }
    void Mount(uint32_t nItemCount, uint32_t* pData, bool doNotDelete = true);

    // copy bitmap in *data to *_data from startIndex
    void Copy(uint32_t startIndex, uint32_t* data, uint32_t itemCount);

    // not refreshing setcount in read-only mode
    // can improve performance
    void MountWithoutRefreshSetCount(uint32_t nItemCount, uint32_t* pData, bool doNotDelete = true);

    inline bool Test(uint32_t nIndex) const;
    inline bool IsEmpty() const;
    inline uint32_t CountBits(uint64_t value) const;
    inline uint32_t CountBits(uint32_t value, uint32_t validCount) const;
    bool HasSetData(uint32_t beginIndex, uint32_t endIndex) const;
    void ResetBetween(uint32_t beginIndex, uint32_t endIndex) const;

    Bitmap* Clone() const;

private:
    void Copy(uint32_t* dstSlotAddr, uint32_t startIndex, uint32_t* data, uint32_t itemCount);

public:
    uint32_t& operator[](uint32_t i);
    Bitmap& operator&(Bitmap& bitmap);
    Bitmap& operator&=(Bitmap& bitmap);
    Bitmap& operator|(const Bitmap& bitmap);
    Bitmap& operator|=(const Bitmap& bitmap);
    Bitmap& operator~();
    Bitmap& operator-=(Bitmap& bitmap);
    Bitmap& operator-(Bitmap& bitmap);
    bool operator==(Bitmap& bitmap);
    bool operator!=(Bitmap& bitmap);

protected:
    friend class BitmapTest;
    void RefreshSetCountByScanning() const;

public:
    static constexpr uint32_t BYTE_SLOT_NUM = 8;
    static constexpr uint32_t SLOT_SIZE = BYTE_SLOT_NUM * sizeof(uint32_t);
    static constexpr uint32_t SLOT_SIZE_BIT_NUM = 5;
    static constexpr uint32_t SLOT_SIZE_BIT_MASK = 0x1F;
    static constexpr uint32_t INVALID_INDEX = 0xFFFFFFFF;
    static const uint32_t BITMAPOPMASK[SLOT_SIZE];

    static int32_t GetSlotCount(int32_t docCount) { return (docCount + SLOT_SIZE - 1) / SLOT_SIZE; }

    static uint32_t GetDumpSize(uint32_t bitCount)
    {
        return NumericUtil::UpperPack(bitCount, SLOT_SIZE) / BYTE_SLOT_NUM;
    }

protected:
    uint32_t _itemCount;
    uint32_t _slotCount;
    uint32_t* _data;
    autil::mem_pool::PoolBase* _pool;
    mutable uint32_t _setCount;
    bool _mount;
    bool _initSet;
};

typedef std::shared_ptr<Bitmap> BitmapPtr;

///////////////////////////////////////////////////
// inline functions
inline bool Bitmap::Test(uint32_t nIndex) const
{
    assert(nIndex < _itemCount);

    uint32_t quot = nIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t rem = nIndex & SLOT_SIZE_BIT_MASK;

    return (_data[quot] & BITMAPOPMASK[rem]) != 0;
}

inline bool Bitmap::IsEmpty() const { return _setCount == 0; }

inline uint32_t Bitmap::CountBits(uint64_t value) const
{
    value = value - ((value >> 1) & 0x5555555555555555);
    value = (value & 0x3333333333333333) + ((value >> 2) & 0x3333333333333333);

    return (uint32_t)((((value + (value >> 4)) & 0xF0F0F0F0F0F0F0F) * 0x101010101010101) >> 56);
    return value;
}

inline uint32_t Bitmap::CountBits(uint32_t value, uint32_t validCount) const
{
    uint32_t count = 0;
    for (uint32_t i = 0; value != 0 && i < validCount; i++) {
        if ((value & BITMAPOPMASK[0]) != 0) {
            count++;
        }
        value <<= 1;
    }
    return count;
}

inline void Bitmap::MountWithoutRefreshSetCount(uint32_t nItemCount, uint32_t* pData, bool doNotDelete)
{
    if (!_mount)
        assert(_data == NULL);
    _data = pData;
    _mount = doNotDelete;
    _itemCount = nItemCount;
    _slotCount = (nItemCount + SLOT_SIZE - 1) >> 5;
}
}} // namespace indexlib::util
