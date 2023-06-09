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
#include "indexlib/index/common/RadixTree.h"

using namespace std;
using namespace autil::mem_pool;

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, RadixTree);

RadixTree::RadixTree(uint32_t slotNum, uint32_t itemNumInSlice, Pool* byteSlicePool, uint16_t unitSize)
    : _byteSlicePool(byteSlicePool)
    , _root(NULL)
    , _sliceNum(0)
    , _itemSize(unitSize)
{
    assert(slotNum <= 256);
    DoInit(slotNum, itemNumInSlice);
}

RadixTree::~RadixTree() {}

void RadixTree::DoInit(uint32_t slotNum, uint64_t itemNumInSlice)
{
    assert(slotNum > 1);
    assert(itemNumInSlice != 0);

    _powerOfSlotNum = CalculatePowerOf2(slotNum);
    _powerOfItemNum = CalculatePowerOf2(itemNumInSlice);
    _sliceCursor = CalculateSliceSize();
}

bool RadixTree::Append(const uint8_t* data, size_t length)
{
    // append 0 bytes, allocate memory for search
    // assert(_itemSize == 1);
    if (length == 0) {
        Allocate(length);
        return true;
    }

    uint8_t* address = Allocate(length);
    if (address) {
        memcpy(address, data, length);
        return true;
    }
    AUTIL_LOG(WARN, "Append data fail, can not allocate memory [%lu]", (uint64_t)length);
    return false;
}

uint8_t* RadixTree::AllocateConsecutiveSlices(uint32_t sliceNum, uint32_t sliceSize)
{
    if (sliceNum <= 0) {
        return NULL;
    }

    uint8_t* currentSlice = (uint8_t*)_byteSlicePool->allocate(sliceSize * sliceNum);

    if (currentSlice == NULL) {
        AUTIL_LOG(ERROR,
                  "allocate data fail,"
                  "can not allocate sliceNum[%u]",
                  sliceNum);
        return NULL;
    }

    _sliceCursor = 0;

    uint8_t* tmpSlice = currentSlice;
    for (uint32_t i = 0; i < sliceNum; i++) {
        AppendSlice((RadixTreeNode::Slot)tmpSlice);
        tmpSlice += sliceSize;
    }
    return currentSlice;
}

uint8_t* RadixTree::Allocate(size_t appendSize)
{
    uint32_t sliceSize = CalculateSliceSize();
    uint8_t* currentSlice = NULL;

    if (appendSize > sliceSize - _sliceCursor) {
        uint32_t sliceNumToAllocate = CalculateNeededSliceNum(appendSize, sliceSize);
        currentSlice = AllocateConsecutiveSlices(sliceNumToAllocate, sliceSize);

        if (currentSlice == NULL) {
            AUTIL_LOG(ERROR,
                      "allocate data fail,"
                      "can not allocate size [%lu]",
                      appendSize);
            return NULL;
        }

        _sliceCursor = appendSize - (sliceNumToAllocate - 1) * sliceSize;
    } else if (appendSize == 0 && _sliceCursor == sliceSize) {
        // to avoid read core
        currentSlice = AllocateConsecutiveSlices(1, sliceSize);
        _sliceCursor = 0;
    } else {
        currentSlice = (uint8_t*)_root->Search(_sliceNum - 1) + _sliceCursor;
        _sliceCursor += appendSize;
    }

    return currentSlice;
}

uint8_t* RadixTree::OccupyOneItem() { return Allocate(_itemSize); }

void RadixTree::AppendSlice(RadixTreeNode::Slot slice)
{
    if (_root == NULL) {
        _root = CreateNode(0, NULL);
    }

    uint8_t height = _root->GetHeight();
    if (NeedGrowUp(height)) {
        RadixTreeNode* node = CreateNode(height + 1, _root);
        _root = node;
    }

    _root->Insert(slice, _sliceNum, _byteSlicePool);
    _sliceNum++;
}

RadixTreeNode* RadixTree::CreateNode(uint8_t height, RadixTreeNode* firstSubNode)
{
    void* buffer = _byteSlicePool->allocate(sizeof(RadixTreeNode));
    RadixTreeNode* node = (RadixTreeNode*)new (buffer) RadixTreeNode(_powerOfSlotNum, height);
    node->Init(_byteSlicePool, firstSubNode);
    return node;
}
} // namespace indexlib::index
