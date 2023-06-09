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

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/common/RadixTreeNode.h"

namespace indexlib::index {

// ATTENTION:
// only support one write and multi read,
// and read is safe only if corresponding item has been written
class RadixTree
{
public:
    // slot number only support 2,4,8,16...,128,256
    RadixTree(uint32_t slotNum, uint32_t itemNumInSlice, autil::mem_pool::Pool* byteSlicePool, uint16_t itemSize = 1);
    ~RadixTree();

public:
    uint8_t* OccupyOneItem();
    uint8_t* Search(uint64_t itemIdx);

    uint32_t GetSliceNum() const { return _sliceNum; }
    uint32_t GetSliceSize(uint32_t sliceId) const;
    uint8_t* GetSlice(uint64_t sliceId) { return (uint8_t*)_root->Search(sliceId); }
    // return current cursor in bytes
    uint64_t GetCurrentOffset() const { return ((uint64_t)_sliceNum - 1) * CalculateSliceSize() + _sliceCursor; }

    // TODO: conflict with itemsize??
    bool Append(const uint8_t* data, size_t length);
    uint8_t* Allocate(size_t appendSize);

private:
    void DoInit(uint32_t slotNum, uint64_t sliceSize);
    RadixTreeNode* CreateNode(uint8_t height, RadixTreeNode* firstSubNode);
    void AppendSlice(RadixTreeNode::Slot slice);
    bool NeedGrowUp(uint8_t height) const { return _sliceNum >= (uint32_t)(1 << (_powerOfSlotNum * (height + 1))); }

    uint64_t CalculateIdxInSlice(uint64_t itemIdx) const { return itemIdx & ((1 << _powerOfItemNum) - 1); }
    uint64_t CalculateSliceId(uint64_t itemIdx) const { return itemIdx >> _powerOfItemNum; }
    uint32_t CalculateSliceSize() const { return _itemSize * (1 << _powerOfItemNum); }

    uint32_t CalculateNeededSliceNum(uint32_t dataSize, uint32_t sliceSize)
    {
        return (dataSize + sliceSize - 1) / sliceSize;
    }

    uint8_t* AllocateConsecutiveSlices(uint32_t sliceNum, uint32_t sliceSize);

    static uint8_t CalculatePowerOf2(uint64_t value);

private:
    autil::mem_pool::Pool* _byteSlicePool;
    RadixTreeNode* volatile _root;
    uint32_t _sliceNum;
    uint32_t _sliceCursor;
    // sliceSize = _itemSize * (1 << _powerOfItemNum)
    // unit size support for slice size is not equal 2^n
    uint16_t _itemSize;
    uint8_t _powerOfSlotNum;
    uint8_t _powerOfItemNum;

private:
    friend class RadixTreeTest;
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////
inline uint8_t* RadixTree::Search(uint64_t itemIdx)
{
    uint64_t sliceId = CalculateSliceId(itemIdx);

    assert(sliceId < _sliceNum);

    uint8_t* slice = GetSlice(sliceId);
    return slice + CalculateIdxInSlice(itemIdx) * _itemSize;
}

inline uint8_t RadixTree::CalculatePowerOf2(uint64_t value)
{
    uint8_t power = 0;
    uint64_t tmpValue = 1;

    while (tmpValue < value) {
        power++;
        tmpValue <<= 1;
    }

    return power;
}

inline uint32_t RadixTree::GetSliceSize(uint32_t sliceId) const
{
    if (sliceId + 1 < _sliceNum) {
        return CalculateSliceSize();
    } else if (sliceId + 1 == _sliceNum) {
        return _sliceCursor;
    }
    return 0;
}
} // namespace indexlib::index
