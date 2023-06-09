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
#include "indexlib/util/ExpandableBitmap.h"

#include "indexlib/util/PoolUtil.h"

using namespace std;
using namespace autil::mem_pool;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, ExpandableBitmap);

ExpandableBitmap::ExpandableBitmap(bool bSet, PoolBase* pool) : Bitmap(bSet, pool) { _validItemCount = 0; }

ExpandableBitmap::ExpandableBitmap(uint32_t nItemCount, bool bSet, PoolBase* pool) : Bitmap(nItemCount, bSet, pool)
{
    _validItemCount = _itemCount;
}

ExpandableBitmap::ExpandableBitmap(const ExpandableBitmap& rhs) : Bitmap(rhs) { _validItemCount = rhs._validItemCount; }

bool ExpandableBitmap::Set(uint32_t nIndex)
{
    if (nIndex >= _itemCount) {
        Expand(nIndex);
        _validItemCount = _itemCount;
    }
    return Bitmap::Set(nIndex);
}

bool ExpandableBitmap::Reset(uint32_t nIndex)
{
    if (nIndex >= _itemCount) {
        Expand(nIndex);
    }
    return Bitmap::Reset(nIndex);
}

void ExpandableBitmap::ReSize(uint32_t nItemCount)
{
    if (nItemCount >= _itemCount) {
        Expand(nItemCount);
        _validItemCount = nItemCount;
    } else {
        _validItemCount = nItemCount;
    }
}

void ExpandableBitmap::Expand(uint32_t nIndex)
{
    assert(nIndex >= _itemCount);

    uint32_t allocSlotCount = _slotCount;

    uint32_t addSlotCount = (_slotCount / 2) > 0 ? (_slotCount / 2) : 1;
    do {
        allocSlotCount += addSlotCount;
    } while (nIndex >= allocSlotCount * SLOT_SIZE);

    uint32_t* data = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, uint32_t, allocSlotCount);

    AUTIL_LOG(DEBUG, "expand from %u to %u", _slotCount, allocSlotCount);

    if (_data != NULL) {
        memcpy(data, _data, _slotCount * sizeof(uint32_t));

        uint8_t initValue = _initSet ? 0xFF : 0;
        memset(data + _slotCount, initValue, (allocSlotCount - _slotCount) * sizeof(uint32_t));

        // only release _data when _pool is NULL
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _data, _slotCount);
        _data = data;

        _slotCount = allocSlotCount;
        _itemCount = _slotCount * SLOT_SIZE;
    } else {
        uint8_t initValue = _initSet ? 0xFF : 0;
        memset(data + _slotCount, initValue, (allocSlotCount - _slotCount) * sizeof(uint32_t));

        _data = data;
        _slotCount = allocSlotCount;
        _itemCount = _slotCount * SLOT_SIZE;
    }
}

ExpandableBitmap* ExpandableBitmap::CloneWithOnlyValidData() const
{
    ExpandableBitmap* newBitmap = new ExpandableBitmap(false);
    newBitmap->_itemCount = _validItemCount;
    newBitmap->_validItemCount = _validItemCount;
    newBitmap->_setCount = _setCount;
    newBitmap->_slotCount = (newBitmap->_itemCount + SLOT_SIZE - 1) >> 5;
    newBitmap->_initSet = _initSet;
    newBitmap->_mount = _mount;

    if (GetData() != NULL) {
        newBitmap->_data = IE_POOL_COMPATIBLE_NEW_VECTOR(newBitmap->_pool, uint32_t, newBitmap->_slotCount);
        memcpy(newBitmap->_data, _data, newBitmap->_slotCount * sizeof(uint32_t));
    } else {
        newBitmap->_data = NULL;
    }

    newBitmap->_mount = false;
    return newBitmap;
}

void ExpandableBitmap::Mount(uint32_t nItemCount, uint32_t* pData, bool doNotDelete)
{
    _validItemCount = nItemCount;
    Bitmap::Mount(nItemCount, pData, doNotDelete);
}

void ExpandableBitmap::MountWithoutRefreshSetCount(uint32_t nItemCount, uint32_t* pData, bool doNotDelete)
{
    _validItemCount = nItemCount;
    Bitmap::MountWithoutRefreshSetCount(nItemCount, pData, doNotDelete);
}

ExpandableBitmap* ExpandableBitmap::Clone() const { return new ExpandableBitmap(*this); }
}} // namespace indexlib::util
