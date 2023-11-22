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
#include "indexlib/util/Bitmap.h"

#include <string.h>

#include "indexlib/util/PoolUtil.h"

namespace indexlib { namespace util {

const uint32_t Bitmap::BITMAPOPMASK[SLOT_SIZE] = {
    0x80000000, 0x40000000, 0x20000000, 0x10000000, 0x08000000, 0x04000000, 0x02000000, 0x01000000,
    0x00800000, 0x00400000, 0x00200000, 0x00100000, 0x00080000, 0x00040000, 0x00020000, 0x00010000,
    0x00008000, 0x00004000, 0x00002000, 0x00001000, 0x00000800, 0x00000400, 0x00000200, 0x00000100,
    0x00000080, 0x00000040, 0x00000020, 0x00000010, 0x00000008, 0x00000004, 0x00000002, 0x00000001};

const static uint32_t BITMAP_NEXT_MASK[Bitmap::SLOT_SIZE] = {
    0xffffffff, 0x7fffffff, 0x3fffffff, 0x1fffffff, 0x0fffffff, 0x07ffffff, 0x03ffffff, 0x01ffffff,
    0x00ffffff, 0x007fffff, 0x003fffff, 0x001fffff, 0x000fffff, 0x0007ffff, 0x0003ffff, 0x0001ffff,
    0x0000ffff, 0x00007fff, 0x00003fff, 0x00001fff, 0x00000fff, 0x000007ff, 0x000003ff, 0x000001ff,
    0x000000ff, 0x0000007f, 0x0000003f, 0x0000001f, 0x0000000f, 0x00000007, 0x00000003, 0x00000001,
};

Bitmap::Bitmap(bool bSet, autil::mem_pool::PoolBase* pool)
{
    if (bSet)
        _itemCount = _slotCount = INVALID_INDEX;
    else
        _itemCount = _slotCount = 0;
    _initSet = bSet;
    _data = NULL;
    _setCount = 0;
    _mount = false;
    _pool = pool;
}

Bitmap::Bitmap(uint32_t nItemCount, bool bSet, autil::mem_pool::PoolBase* pool)
{
    _pool = pool;
    _slotCount = (nItemCount + SLOT_SIZE - 1) >> 5;
    _data = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, uint32_t, _slotCount);

    _itemCount = nItemCount;
    memset(_data, bSet ? 0xFF : 0x0, _slotCount * sizeof(uint32_t));
    _setCount = bSet ? nItemCount : 0;
    _mount = false;
    _initSet = bSet;
}

Bitmap::Bitmap(const Bitmap& rhs)
{
    _itemCount = rhs._itemCount;
    _setCount = rhs._setCount;
    _slotCount = rhs._slotCount;
    _initSet = rhs._initSet;
    _pool = NULL;

    if (rhs.GetData() != NULL) {
        _data = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, uint32_t, _slotCount);
        memcpy(_data, rhs._data, _slotCount * sizeof(uint32_t));
    } else {
        _data = NULL;
    }
    _mount = false;
}

Bitmap& Bitmap::operator=(const Bitmap& rhs)
{
    if (this != &rhs) {
        _pool = rhs._pool;
        if (!_mount && _data != NULL) {
            // only release _data when _pool is NULL
            IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _data, _slotCount);
        }

        _itemCount = rhs._itemCount;
        _setCount = rhs._setCount;
        _slotCount = rhs._slotCount;
        _initSet = rhs._initSet;

        if (rhs.GetData() != NULL) {
            _data = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, uint32_t, _slotCount);
            memcpy(_data, rhs._data, _slotCount * sizeof(uint32_t));
        } else {
            _data = NULL;
        }
        _mount = false;
    }
    return *this;
}

Bitmap::~Bitmap(void) { Clear(); }

Bitmap* Bitmap::Clone() const { return new Bitmap(*this); }

bool Bitmap::Alloc(uint32_t nItemCount, bool bSet)
{
    Clear();

    _slotCount = (nItemCount + SLOT_SIZE - 1) >> 5;
    _data = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, uint32_t, _slotCount);

    _itemCount = nItemCount;
    memset(_data, bSet ? 0xFF : 0x0, _slotCount * sizeof(uint32_t));
    _setCount = bSet ? nItemCount : 0;

    return true;
}

void Bitmap::Clear()
{
    if (!_mount && _data != NULL) {
        // only release _data when _pool is NULL
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _data, _slotCount);
    }
    _data = NULL;

    _itemCount = 0;
    _setCount = 0;
}

uint32_t Bitmap::Size() const { return _slotCount * sizeof(uint32_t); }

void Bitmap::RefreshSetCountByScanning() const
{
    if (_data == NULL) {
        _setCount = 0;
        return;
    }

    _setCount = 0;

    uint32_t lastSlot = _slotCount - 1;
    uint32_t testBitInLastSlot = (_itemCount & SLOT_SIZE_BIT_MASK);
    if (testBitInLastSlot) {
        _setCount = CountBits(_data[lastSlot], testBitInLastSlot);
        if (lastSlot == 0) {
            return;
        }
        --lastSlot;
    }
    if (!(lastSlot & 1)) {
        _setCount += CountBits(_data[lastSlot], 32);
        if (lastSlot == 0) {
            return;
        }
        --lastSlot;
    }
    assert(lastSlot & 1);
    uint64_t nData;
    uint64_t* batchAddr = (uint64_t*)_data;
    uint32_t countEnd = lastSlot / 2 + 1;
    for (uint32_t i = 0; i < countEnd; ++i) {
        if ((nData = batchAddr[i]) == 0) {
            continue;
        }
        _setCount += CountBits(nData);
    }
}

uint32_t Bitmap::GetSetCount() const { return _setCount; }

void Bitmap::SetSetCount(uint32_t nCount) { _setCount = nCount; }

uint32_t Bitmap::GetUnsetCount() const { return _itemCount - GetSetCount(); }

uint32_t Bitmap::Begin() const
{
    uint32_t nData;
    for (uint32_t i = 0; i < _slotCount; ++i) {
        if ((nData = _data[i]) != 0) {
            return i * SLOT_SIZE + __builtin_clz(nData);
        }
    }
    return INVALID_INDEX;
}

uint32_t Bitmap::Next(uint32_t nIndex) const
{
    if (++nIndex < _itemCount) {
        uint32_t quot = nIndex >> SLOT_SIZE_BIT_NUM;
        uint32_t rem = nIndex & SLOT_SIZE_BIT_MASK;

        for (uint32_t i = quot; i < _slotCount; ++i) {
            uint32_t nData = _data[i];
            if (i == quot) {
                uint32_t restData = nData & BITMAP_NEXT_MASK[rem];
                if (restData) {
                    return i * SLOT_SIZE + __builtin_clz(restData);
                }
            } else {
                if (nData == 0)
                    continue;
                return i * SLOT_SIZE + __builtin_clz(nData);
            }
        }
    }
    return INVALID_INDEX;
}

bool Bitmap::Set(uint32_t nIndex)
{
    assert(nIndex < _itemCount);

    uint32_t quot = nIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t rem = nIndex & SLOT_SIZE_BIT_MASK;

    if (!(_data[quot] & BITMAPOPMASK[rem])) {
        _data[quot] |= BITMAPOPMASK[rem];
        ++_setCount;
        return true;
    }
    return false;
}

bool Bitmap::Reset(uint32_t nIndex)
{
    if (nIndex >= _itemCount) {
        return false;
    }
    uint32_t quot = nIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t rem = nIndex & SLOT_SIZE_BIT_MASK;

    if (_data[quot] & BITMAPOPMASK[rem]) {
        _data[quot] &= ~BITMAPOPMASK[rem];
        --_setCount;
        return true;
    }
    return false;
}

void Bitmap::ResetAll()
{
    memset(_data, 0x0, _slotCount * sizeof(uint32_t));
    _setCount = 0;
}

void Bitmap::ResetAllAfter(uint32_t nIndex)
{
    assert(nIndex < _itemCount);

    uint32_t quot = nIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t rem = nIndex & SLOT_SIZE_BIT_MASK;

    for (uint32_t i = rem; i < SLOT_SIZE; i++) {
        _data[quot] &= ~BITMAPOPMASK[i];
    }

    if (quot < _slotCount - 1) {
        uint32_t siz = _slotCount - quot - 1;
        memset(((char*)_data) + (quot + 1) * sizeof(uint32_t), 0, siz * sizeof(uint32_t));
    }

    RefreshSetCountByScanning();
}

void Bitmap::Mount(uint32_t nItemCount, uint32_t* pData, bool doNotDelete)
{
    MountWithoutRefreshSetCount(nItemCount, pData, doNotDelete);
    RefreshSetCountByScanning();
}

void Bitmap::Copy(uint32_t startIndex, uint32_t* data, uint32_t itemCount)
{
    assert(_data != NULL);
    if (data == NULL || itemCount == 0) {
        return;
    }
    uint32_t* dstSlotAddr = _data + (startIndex >> 5);
    startIndex &= 31;
    Copy(dstSlotAddr, startIndex, data, itemCount);
}

void Bitmap::Copy(uint32_t* dstSlotAddr, uint32_t startIndex, uint32_t* data, uint32_t itemCount)
{
    assert(startIndex < 32);
    uint32_t offset = startIndex;
    uint32_t remainBitsInDstSlot = 32 - offset;

    uint32_t numSlotsInSrc = (itemCount + 31) >> 5;
    for (uint32_t i = 0; i < numSlotsInSrc - 1; i++) {
        uint32_t value = data[i];
        // copy first #remainBitsInDstSlot# bits from value to dstSlotAddr
        *dstSlotAddr |= (value >> offset);
        dstSlotAddr++;
        // copy next #offset# bits from value to dstSlotAddr
        if (offset != 0) {
            *dstSlotAddr |= (value << remainBitsInDstSlot);
        }
        if ((!(i & 1)) && (i + 1 < numSlotsInSrc - 1)) {
            // batch count data[i] and data[i+1]
            uint64_t* countAddr = (uint64_t*)(&data[i]);
            uint64_t countValue = *countAddr;
            _setCount += CountBits(countValue);
        }
    }
    if ((!(numSlotsInSrc & 1)) && (numSlotsInSrc >= 2)) {
        // count last slot if numSlotsInSrc is odd
        _setCount += CountBits(data[numSlotsInSrc - 2], 32);
    }
    // copy remain bits in last slot of data
    uint32_t value = data[numSlotsInSrc - 1];
    uint32_t remainBitsInSrcSlot = itemCount - 32 * (numSlotsInSrc - 1);
    _setCount += CountBits(value, remainBitsInSrcSlot);
    if (remainBitsInSrcSlot > 0) {
        if (remainBitsInSrcSlot <= remainBitsInDstSlot) {
            uint32_t tmp = (value >> (32 - remainBitsInSrcSlot)) << (remainBitsInDstSlot - remainBitsInSrcSlot);
            *dstSlotAddr |= tmp;
        } else {
            *dstSlotAddr |= (value >> offset);
            dstSlotAddr++;
            uint32_t tmp = (value >> (32 - remainBitsInSrcSlot)) << (32 - remainBitsInSrcSlot + remainBitsInDstSlot);
            *dstSlotAddr |= tmp;
        }
    }
}

uint32_t& Bitmap::operator[](uint32_t i)
{
    assert(i < _slotCount);
    return _data[i];
}

Bitmap& Bitmap::operator&(Bitmap& bitmap)
{
    assert(bitmap.GetItemCount() == GetItemCount());

    uint32_t* b = bitmap.GetData();
    for (uint32_t i = 0; i < _slotCount; ++i) {
        _data[i] &= b[i];
    }
    RefreshSetCountByScanning();

    return *this;
}

Bitmap& Bitmap::operator&=(Bitmap& bitmap) { return *this & bitmap; }

Bitmap& Bitmap::operator|(const Bitmap& bitmap)
{
    assert(bitmap.GetItemCount() == GetItemCount());
    uint32_t* targetAddr = _data;
    uint32_t* sourceAddr = bitmap.GetData();
    uint32_t batchNum = _slotCount / 32;
    uint32_t remainCount = _slotCount % 32;
    for (size_t i = 0; i < batchNum; i++) {
        targetAddr[0] |= sourceAddr[0];
        targetAddr[1] |= sourceAddr[1];
        targetAddr[2] |= sourceAddr[2];
        targetAddr[3] |= sourceAddr[3];
        targetAddr[4] |= sourceAddr[4];
        targetAddr[5] |= sourceAddr[5];
        targetAddr[6] |= sourceAddr[6];
        targetAddr[7] |= sourceAddr[7];
        targetAddr[8] |= sourceAddr[8];
        targetAddr[9] |= sourceAddr[9];
        targetAddr[10] |= sourceAddr[10];
        targetAddr[11] |= sourceAddr[11];
        targetAddr[12] |= sourceAddr[12];
        targetAddr[13] |= sourceAddr[13];
        targetAddr[14] |= sourceAddr[14];
        targetAddr[15] |= sourceAddr[15];
        targetAddr[16] |= sourceAddr[16];
        targetAddr[17] |= sourceAddr[17];
        targetAddr[18] |= sourceAddr[18];
        targetAddr[19] |= sourceAddr[19];
        targetAddr[20] |= sourceAddr[20];
        targetAddr[21] |= sourceAddr[21];
        targetAddr[22] |= sourceAddr[22];
        targetAddr[23] |= sourceAddr[23];
        targetAddr[24] |= sourceAddr[24];
        targetAddr[25] |= sourceAddr[25];
        targetAddr[26] |= sourceAddr[26];
        targetAddr[27] |= sourceAddr[27];
        targetAddr[28] |= sourceAddr[28];
        targetAddr[29] |= sourceAddr[29];
        targetAddr[30] |= sourceAddr[30];
        targetAddr[31] |= sourceAddr[31];
        targetAddr += 32;
        sourceAddr += 32;
    }
    for (size_t i = 0; i < remainCount; i++) {
        targetAddr[i] |= sourceAddr[i];
    }
    RefreshSetCountByScanning();

    return *this;
}

Bitmap& Bitmap::operator|=(const Bitmap& bitmap) { return *this | bitmap; }

Bitmap& Bitmap::operator~()
{
    _setCount = GetUnsetCount();
    uint32_t nLastSlot = _slotCount - 1;
    for (uint32_t i = 0; i < nLastSlot; ++i) {
        _data[i] = ~_data[i];
    }

    uint32_t nCount = _itemCount - nLastSlot * SLOT_SIZE;
    uint32_t nData = _data[nLastSlot];
    for (uint32_t i = 0; i < nCount; i++) {
        if ((nData & BITMAPOPMASK[i]) != 0) {
            _data[nLastSlot] &= ~BITMAPOPMASK[i];
        } else {
            _data[nLastSlot] |= BITMAPOPMASK[i];
        }
    }

    return *this;
}

Bitmap& Bitmap::operator-=(Bitmap& bitmap) { return *this - bitmap; }

Bitmap& Bitmap::operator-(Bitmap& bitmap)
{
    assert(bitmap.GetItemCount() == GetItemCount());
    for (uint32_t i = 0; i < _slotCount; ++i) {
        _data[i] &= ~bitmap[i];
    }
    RefreshSetCountByScanning();

    return *this;
}

bool Bitmap::operator==(Bitmap& bitmap)
{
    if (this == &bitmap) {
        return true;
    }
    if (_itemCount != bitmap._itemCount) {
        return false;
    }
    if (_slotCount != bitmap._slotCount) {
        return false;
    }
    if (_setCount != bitmap._setCount) {
        return false;
    }
    if (_data == bitmap._data) {
        return true;
    }
    return memcmp(_data, bitmap._data, _slotCount * sizeof(uint32_t)) == 0;
}

bool Bitmap::operator!=(Bitmap& bitmap) { return !(*this == bitmap); }

uint32_t Bitmap::GetSetCount(uint32_t begin, uint32_t end) const
{
    uint32_t nData;
    uint32_t setCount = 0;

    if (end >= _itemCount) {
        end = _itemCount - 1;
    }

    if (_data == NULL || end < begin) {
        return 0;
    }

    uint32_t testBeginSlot = begin / SLOT_SIZE;
    uint32_t testBitInBeginSlot = SLOT_SIZE - (begin & SLOT_SIZE_BIT_MASK);

    uint32_t testEndSlot = (end + 1) / SLOT_SIZE;
    uint32_t testBitInEndSlot = ((end + 1) & SLOT_SIZE_BIT_MASK);

    if (testBitInBeginSlot != SLOT_SIZE) {
        nData = _data[testBeginSlot];
        if (testBeginSlot == testEndSlot) {
            nData &= (0xFFFFFFFFU << (SLOT_SIZE - testBitInEndSlot));
        }
        nData <<= (begin & SLOT_SIZE_BIT_MASK);

        testBeginSlot++;
        while (nData != 0) {
            if ((nData & BITMAPOPMASK[0]) != 0) {
                setCount++;
            }
            nData <<= 1;
        }
    }

    if (testBeginSlot <= testEndSlot && testBitInEndSlot != 0) {
        nData = _data[testEndSlot];
        setCount += CountBits(nData, testBitInEndSlot);
    }

    if ((testBeginSlot < testEndSlot) && ((testEndSlot - testBeginSlot) & 1)) {
        nData = _data[testBeginSlot];
        if (nData) {
            setCount += CountBits(nData, 32);
        }
        testBeginSlot++;
    }
    for (uint32_t i = testBeginSlot; i < testEndSlot; i += 2) {
        uint64_t batchData = *((uint64_t*)(&_data[i]));
        if (batchData == 0) {
            continue;
        }
        setCount += CountBits(batchData);
    }

    return setCount;
}

bool Bitmap::HasSetData(uint32_t beginIndex, uint32_t endIndex) const
{
    assert(beginIndex <= endIndex);
    assert(endIndex < _itemCount);

    uint32_t quotBegin = beginIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t remBegin = beginIndex & SLOT_SIZE_BIT_MASK;
    uint32_t quotEnd = endIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t remEnd = endIndex & SLOT_SIZE_BIT_MASK;

    int32_t testStart = (remBegin == 0) ? (int32_t)quotBegin : (int32_t)quotBegin + 1;
    int32_t testEnd = (remEnd == SLOT_SIZE_BIT_MASK) ? (int32_t)quotEnd : (int32_t)quotEnd - 1;
    int32_t testCursor = testStart;
    while (testCursor <= testEnd) {
        if (_data[testCursor]) {
            return true;
        }
        testCursor++;
    }

    if (quotBegin != quotEnd) {
        if (remBegin != 0) {
            uint32_t mask = (BITMAPOPMASK[remBegin] - 1) | BITMAPOPMASK[remBegin];
            if (_data[quotBegin] & mask) {
                return true;
            }
        }

        if (remEnd != SLOT_SIZE_BIT_MASK) {
            uint32_t mask = ~(BITMAPOPMASK[remEnd] - 1);
            if (_data[quotEnd] & mask) {
                return true;
            }
        }
    } else {
        if (remBegin != 0 || remEnd != SLOT_SIZE_BIT_MASK) {
            uint32_t mask = ((BITMAPOPMASK[remBegin] - 1) | BITMAPOPMASK[remBegin]) - (BITMAPOPMASK[remEnd] - 1);
            ;
            if (_data[quotBegin] & mask) {
                return true;
            }
        }
    }

    return false;
}

void Bitmap::ResetBetween(uint32_t beginIndex, uint32_t endIndex) const
{
    assert(beginIndex <= endIndex);
    assert(endIndex < _itemCount);

    uint32_t quotBegin = beginIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t remBegin = beginIndex & SLOT_SIZE_BIT_MASK;
    uint32_t quotEnd = endIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t remEnd = endIndex & SLOT_SIZE_BIT_MASK;

    int32_t testStart = (remBegin == 0) ? (int32_t)quotBegin : (int32_t)quotBegin + 1;
    int32_t testEnd = (remEnd == SLOT_SIZE_BIT_MASK) ? (int32_t)quotEnd : (int32_t)quotEnd - 1;
    int32_t testCursor = testStart;
    while (testCursor <= testEnd) {
        _data[testCursor] = 0;
        testCursor++;
    }

    if (quotBegin != quotEnd) {
        if (remBegin != 0) {
            uint32_t mask = (BITMAPOPMASK[remBegin] - 1) | BITMAPOPMASK[remBegin];
            _data[quotBegin] &= ~mask;
        }

        if (remEnd != SLOT_SIZE_BIT_MASK) {
            uint32_t mask = (BITMAPOPMASK[remEnd] - 1);
            _data[quotEnd] &= mask;
        }
    } else {
        if (remBegin != 0 || remEnd != SLOT_SIZE_BIT_MASK) {
            uint32_t mask = ((BITMAPOPMASK[remBegin] - 1) | BITMAPOPMASK[remBegin]) - (BITMAPOPMASK[remEnd] - 1);
            _data[quotBegin] &= ~mask;
        }
    }
}
}} // namespace indexlib::util
