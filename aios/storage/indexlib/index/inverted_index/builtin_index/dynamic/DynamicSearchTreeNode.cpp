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
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicSearchTreeNode.h"

#include <limits>

#include "autil/StringUtil.h"

namespace indexlib::index {

Node::Node(size_t maxSlotShift, bool isLeafNode) noexcept
    : _keySlotUse(0)
    , _maxSlotShift(maxSlotShift)
    , _isLeafNode(isLeafNode)
{
    assert(maxSlotShift > 0);
}

InternalNode::InternalNode(size_t maxSlotShift) noexcept : Node(maxSlotShift, /*isLeafNode=*/false)
{
    static_assert(sizeof(Node) == 8u, "Node size error");
}

void InternalNode::SetKey(slotid_t idx, KeyType key) noexcept
{
    assert(idx >= 0 && idx < MaxSlot());
    KeyType* addr = reinterpret_cast<KeyType*>(_data);
    addr[idx] = key;
}

void InternalNode::SetChild(slotid_t idx, const Node* child) noexcept
{
    assert(idx >= 0 && idx <= MaxSlot());
    char* base = const_cast<char*>(_data);
    base += sizeof(KeyType) << _maxSlotShift;
    *(reinterpret_cast<const Node**>(base) + idx) = child;
}

// The split happens when InternalNode has 2k keys and 2k+1 children. (2k=MaxSlot()).
// After the split, key will be inserted into newNode1 or newNode2.
// newNode1 and newNode2 will have k keys and k+1 children each.
// One key will be propagated to the upper level.
void InternalNode::Split(KeyType key, InternalNode* newNode1, InternalNode* newNode2, const Node* splitNode1,
                         const Node* splitNode2, KeyType* splitKey) const noexcept
{
    slotid_t i = GetSlot<InternalNode>(key);
    if (i == INVALID_SLOTID) {
        i = _keySlotUse;
    }
    slotid_t mid = MaxSlot() / 2;
    if (i == MaxSlot() / 2) {
        CopyKey(newNode1, 0, 0, mid);
        CopyChild(newNode1, 0, 0, mid);
        newNode1->SetChild(mid, splitNode1);
        CopyKey(newNode2, 0, mid, MaxSlot());
        CopyChild(newNode2, 1, mid + 1, MaxSlot() + 1);
        newNode2->SetChild(0, splitNode2);
        *splitKey = key;
    } else if (i < mid) {
        // Copy k-1 keys and k children from original node to newNode1.
        // Also add the new key, splitNode1, splitNode2 to newNode1.
        CopyKey(newNode1, 0, 0, i);
        CopyChild(newNode1, 0, 0, i);
        newNode1->SetChild(i, splitNode1);
        newNode1->SetKey(i, key);
        newNode1->SetChild(i + 1, splitNode2);
        CopyKey(newNode1, i + 1, i, mid - 1);
        CopyChild(newNode1, i + 2, i + 1, mid);

        // Copy k keys and k+1 children to newNode2
        CopyKey(newNode2, 0, mid, MaxSlot());
        CopyChild(newNode2, 0, mid, MaxSlot() + 1);

        *splitKey = Key(mid - 1);
    } else {
        // Copy k keys and k+1 children to newNode1
        CopyKey(newNode1, 0, 0, mid);
        CopyChild(newNode1, 0, 0, mid + 1);

        // Copy k-1 keys and k children from original node to newNode2.
        // Also add the new key, splitNode1, splitNode2 to newNode2.
        CopyKey(newNode2, 0, mid + 1, i);
        CopyChild(newNode2, 0, mid + 1, i);
        newNode2->SetChild(i - mid - 1, splitNode1);
        newNode2->SetKey(i - mid - 1, key);
        newNode2->SetChild(i - mid, splitNode2);
        CopyKey(newNode2, i - mid, i, MaxSlot());
        CopyChild(newNode2, i + 1 - mid, i + 1, MaxSlot() + 1);

        *splitKey = Key(mid);
    }
    newNode1->SetKeySlotUse(MaxSlot() / 2);
    newNode2->SetKeySlotUse(MaxSlot() / 2);
}

// Copy keys from srcNode's [srcBegin, srcEnd) to dstNode's dstBegin.
void InternalNode::CopyKey(InternalNode* dstNode, slotid_t dstBegin, slotid_t srcBegin, slotid_t srcEnd) const noexcept
{
    assert(srcBegin <= srcEnd);
    assert(srcEnd <= MaxSlot());
    // assert(dstBegin + srcEnd - srcBegin <= dstNode->MaxSlot());
    if (dstBegin + srcEnd - srcBegin > dstNode->MaxSlot()) {
        assert(false);
    }

    memcpy(/*destination=*/dstNode->_data + sizeof(KeyType) * dstBegin,
           /*source=*/_data + sizeof(KeyType) * srcBegin,
           /*numBytes*/ sizeof(KeyType) * (srcEnd - srcBegin));
}

// Copy children from srcNode's [srcBegin, srcEnd) to dstNode's dstBegin.
void InternalNode::CopyChild(InternalNode* dstNode, slotid_t dstBegin, slotid_t srcBegin,
                             slotid_t srcEnd) const noexcept
{
    assert(srcBegin <= srcEnd);
    assert(srcEnd <= MaxSlot() + 1);
    // assert(dstBegin + srcEnd - srcBegin <= dstNode->MaxSlot());
    if (dstBegin + srcEnd - srcBegin > dstNode->MaxSlot() + 1) {
        assert(false);
    }
    memcpy(/*destination=*/dstNode->_data + sizeof(KeyType) * MaxSlot() + sizeof(Node*) * dstBegin,
           /*source=*/_data + sizeof(KeyType) * MaxSlot() + sizeof(Node*) * srcBegin,
           /*numBytes*/ sizeof(Node*) * (srcEnd - srcBegin));
}

void InternalNode::CopyAndInsert(InternalNode* dstNode, KeyType key, Node* child1, Node* child2) const noexcept
{
    assert(_keySlotUse < MaxSlot());
    slotid_t i = 0;
    for (; i < _keySlotUse; ++i) {
        if (Key(i) > key) {
            break;
        } else if (Key(i) == key) {
            // TODO
            CopyKey(dstNode, 0, 0, _keySlotUse);
            CopyChild(dstNode, 0, 0, _keySlotUse + 1);
            dstNode->SetKeySlotUse(_keySlotUse);
            return;
        }
    }
    CopyKey(dstNode, 0, 0, i);
    CopyChild(dstNode, 0, 0, i);
    dstNode->SetChild(i, child1);
    dstNode->SetKey(i, key);
    dstNode->SetChild(i + 1, child2);
    if (i + 1 < MaxSlot()) {
        CopyKey(dstNode, i + 1, i, _keySlotUse);
        CopyChild(dstNode, i + 2, i + 1, _keySlotUse + 1);
    }

    dstNode->SetKeySlotUse(_keySlotUse + 1);
}

size_t InternalNode::Memory(size_t maxSlotShift) noexcept
{
    auto maxSlots = 1u << maxSlotShift;
    return sizeof(InternalNode) + sizeof(KeyType) * maxSlots + sizeof(Node*) * (1 + maxSlots);
}

std::string InternalNode::DebugString() const noexcept
{
    std::string res;
    for (slotid_t i = 0; i < _keySlotUse; ++i) {
        res += autil::StringUtil::toString<docid_t>(Key(i).DocId()) + " ";
    }
    return res;
}

LeafNode::LeafNode(size_t maxSlotShift) noexcept
    : Node(maxSlotShift, /*isLeafNode=*/true)
    , _prevLeaf(nullptr)
    , _nextLeaf(nullptr)
{
    static_assert(sizeof(Node) == 8u, "Node size error");
}

bool LeafNode::InsertKey(KeyType key) noexcept
{
    assert(_keySlotUse < MaxSlot());
    slotid_t i = 0;
    for (; i < _keySlotUse; ++i) {
        if (Key(i) > key) {
            break;
        } else if (Key(i) == key) {
            bool r = (Key(i).IsDelete() != key.IsDelete());
            SetKey(i, key);
            return r;
        }
    }
    char* base = _data;
    if (i < _keySlotUse) {
        KeyType* dst = (KeyType*)base + _keySlotUse;
        KeyType* src = dst - 1;
        for (size_t j = 0; j < _keySlotUse - i; ++j) {
            *dst = *src;
            dst--;
            src--;
        }
        // // 1. overlap
        // // 2. copy by 4bytes
        // memcpy(/*destination=*/base + sizeof(KeyType) * (i + 1),
        //     /*source=*/base + sizeof(KeyType) * i,
        //     /*numBytes*/ sizeof(KeyType) * (_keySlotUse - i));
    }
    SetKey(i, key);
    ++_keySlotUse;
    return true;
}

// After split newNode1 will have k keys, newNode2 will have k+1 keys.
// TODO: optimize split strategy.
void LeafNode::Split(LeafNode* newNode1, LeafNode* newNode2, KeyType key) noexcept
{
    slotid_t i = GetSlot<LeafNode>(key);
    if (i == INVALID_SLOTID) {
        i = _keySlotUse;
    } else {
        if (Key(i) == key) {
            SetKey(i, key);
            return;
        }
    }
    slotid_t mid = MaxSlot() / 2;
    if (i >= mid) {
        Copy(newNode1, 0, 0, mid);
        Copy(newNode2, 0, mid, i);
        newNode2->SetKey(i - mid, key);
        Copy(newNode2, i - mid + 1, i, MaxSlot());
    } else {
        Copy(newNode2, 0, mid - 1, MaxSlot());
        Copy(newNode1, 0, 0, i);
        newNode1->SetKey(i, key);
        Copy(newNode1, i + 1, i, mid - 1);
    }
    newNode1->SetKeySlotUse(mid);
    newNode2->SetKeySlotUse(mid + 1);
}

void LeafNode::Copy(LeafNode* dstNode, slotid_t dstBegin, slotid_t srcBegin, slotid_t srcEnd) const noexcept
{
    memcpy(/*destination=*/dstNode->_data + sizeof(KeyType) * dstBegin,
           /*source=*/_data + sizeof(KeyType) * srcBegin,
           /*numBytes*/ sizeof(KeyType) * (srcEnd - srcBegin));
}

void LeafNode::SetPrevLeaf(LeafNode* addr) noexcept { _prevLeaf = addr; }
void LeafNode::SetNextLeaf(LeafNode* addr) noexcept { _nextLeaf = addr; }

size_t LeafNode::Memory(size_t maxSlotShift) noexcept
{
    size_t maxSlots = 1u << maxSlotShift;
    return sizeof(LeafNode) + sizeof(KeyType) * maxSlots;
}

bool LeafNode::GetKey(KeyType key, KeyType* result) const noexcept
{
    slotid_t i = GetSlot<LeafNode>(key);
    if (i == INVALID_SLOTID) {
        return false;
    }
    *result = Key(i);
    return true;
}

bool LeafNode::SetKey(slotid_t idx, KeyType key) noexcept
{
    assert(idx >= 0 && idx < MaxSlot());
    KeyType* addr = reinterpret_cast<KeyType*>(_data);
    bool r = (addr[idx].IsDelete() != key.IsDelete());
    addr[idx] = key;
    return r;
}

std::string LeafNode::DebugString() const noexcept
{
    std::string res;
    for (slotid_t i = 0; i < _keySlotUse; ++i) {
        if (!Key(i).IsDelete()) {
            res += autil::StringUtil::toString<docid_t>(Key(i).DocId()) + " ";
        }
    }
    return res;
}

} // namespace indexlib::index
