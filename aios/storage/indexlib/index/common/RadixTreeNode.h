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

#include "autil/mem_pool/Pool.h"

namespace indexlib::index {

class RadixTreeNode
{
public:
    typedef void* Slot;

public:
    // All Node in a tree have same powerOfSlotNum
    // 0 <= powerOfSlotNum * height <= 64, 1 <= powerOfSlotNum <= 8
    RadixTreeNode(uint8_t powerOfSlotNum, uint8_t height);
    ~RadixTreeNode();

public:
    void Init(autil::mem_pool::Pool* byteSlicePool, RadixTreeNode* firstNode);

    // outer space must gurantee slotId must in this node
    void Insert(Slot slot, uint64_t slotId, autil::mem_pool::Pool* byteSlicePool);

    Slot Search(uint64_t slotId);

    uint8_t GetHeight() const { return _height; }

private:
    uint32_t GetSlotNum() const { return 1 << _powerOfSlotNum; }
    uint32_t ExtractSubSlotId(uint64_t slotId) const { return (uint8_t)((slotId >> _shift) & _mask); }

private:
    Slot* _slotArray;
    const uint8_t _powerOfSlotNum;
    const uint8_t _height;
    const uint8_t _shift;
    const uint8_t _mask;

private:
    friend class RadixTreeNodeTest;
};

///////////////////////////////////////////////////////
inline RadixTreeNode::Slot RadixTreeNode::Search(uint64_t slotId)
{
    RadixTreeNode* tmpNode = this;
    while (tmpNode) {
        uint32_t subSlotId = tmpNode->ExtractSubSlotId(slotId);
        if (tmpNode->_height == 0) {
            return tmpNode->_slotArray[subSlotId];
        }

        tmpNode = (RadixTreeNode*)tmpNode->_slotArray[subSlotId];
    }
    return NULL;
}
} // namespace indexlib::index
