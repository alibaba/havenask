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
#include "indexlib/index/common/RadixTreeNode.h"

using namespace std;
using namespace autil::mem_pool;

namespace indexlib::index {
RadixTreeNode::RadixTreeNode(uint8_t powerOfSlotNum, uint8_t height)
    : _slotArray(NULL)
    , _powerOfSlotNum(powerOfSlotNum)
    , _height(height)
    , _shift(_powerOfSlotNum * _height)
    , _mask((1 << _powerOfSlotNum) - 1)
{
    assert(_powerOfSlotNum <= 8);
    assert(_powerOfSlotNum * _height <= 64);
}

RadixTreeNode::~RadixTreeNode() {}

void RadixTreeNode::Init(Pool* byteSlicePool, RadixTreeNode* firtNode)
{
    uint32_t slotNum = GetSlotNum();
    _slotArray = (Slot*)byteSlicePool->allocate(slotNum * sizeof(Slot));
    _slotArray[0] = firtNode;
    for (uint32_t i = 1; i < slotNum; i++) {
        _slotArray[i] = NULL;
    }
}

void RadixTreeNode::Insert(Slot slot, uint64_t slotId, Pool* byteSlicePool)
{
    uint32_t slotIndex = ExtractSubSlotId(slotId);

    if (_height == 0) {
        _slotArray[slotIndex] = slot;
        return;
    }

    RadixTreeNode*& currentSlot = (RadixTreeNode*&)_slotArray[slotIndex];

    if (NULL == currentSlot) {
        void* buffer = byteSlicePool->allocate(sizeof(RadixTreeNode));
        RadixTreeNode* tempNode = (RadixTreeNode*)new (buffer) RadixTreeNode(_powerOfSlotNum, _height - 1);
        tempNode->Init(byteSlicePool, NULL);
        currentSlot = tempNode;
    }

    currentSlot->Insert(slot, slotId, byteSlicePool);
    return;
}
} // namespace indexlib::index
