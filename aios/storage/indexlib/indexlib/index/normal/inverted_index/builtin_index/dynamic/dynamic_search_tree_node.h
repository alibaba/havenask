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

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/format/ComplexDocid.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace dynamic_search_tree_v1 {

using KeyType = indexlib::index::ComplexDocId;

typedef uint16_t slotid_t;
static constexpr slotid_t INVALID_SLOTID = std::numeric_limits<slotid_t>::max();

// data: KeyType slots[MAX_SLOTS];
//       Node* children[MAX_SLOTS + 1];
class Node
{
public:
    Node(size_t maxSlotShift, bool isLeafNode) noexcept;

    Node(const Node&) = delete;
    Node(Node&&) = delete;

public:
    bool IsLeafNode() const noexcept { return _isLeafNode; };
    slotid_t KeySlotUse() const noexcept { return _keySlotUse; }
    slotid_t MaxSlot() const noexcept { return 1u << _maxSlotShift; };
    size_t MaxSlotShift() const noexcept { return _maxSlotShift; }

    void SetKeySlotUse(slotid_t i) noexcept { _keySlotUse = i; };

public:
    // Return the smallest slot whose key is equal to or greater than key.
    template <typename NodeType>
    slotid_t GetSlot(KeyType key) const noexcept
    {
        // TODO: binary search
        slotid_t i = 0;
        for (; i < _keySlotUse; ++i) {
            if (static_cast<const NodeType*>(this)->Key(i) >= key) {
                return i;
            }
        }
        return INVALID_SLOTID;
    }

protected:
    uint64_t _keySlotUse   : 16;
    uint64_t _maxSlotShift : 4;
    uint64_t _isLeafNode   : 1;
};

class InternalNode final : public Node
{
public:
    explicit InternalNode(size_t maxSlotShift) noexcept;

    InternalNode(const InternalNode&) = delete;
    InternalNode(InternalNode&&) = delete;

public:
    KeyType Key(slotid_t idx) const noexcept { return reinterpret_cast<const KeyType*>(_data)[idx]; }
    void SetKey(slotid_t idx, KeyType key) noexcept;
    Node* Child(slotid_t idx) noexcept;
    const Node* Child(slotid_t idx) const noexcept;

    void SetChild(slotid_t idx, const Node* child) noexcept;

public:
    void Split(KeyType key, InternalNode* newNode1, InternalNode* newNode2, const Node* splitNode1,
               const Node* splitNode2, KeyType* splitKey) const noexcept;
    void Copy(InternalNode* dstNode, slotid_t dstBegin, slotid_t srcBegin, slotid_t srcEnd) const noexcept;

    void CopyAndInsert(InternalNode* dstNode, KeyType key, Node* child1, Node* child2) const noexcept;

    void CopyKey(InternalNode* dstNode, slotid_t dstBegin, slotid_t srcBegin, slotid_t srcEnd) const noexcept;
    void CopyChild(InternalNode* dstNode, slotid_t dstBegin, slotid_t srcBegin, slotid_t srcEnd) const noexcept;

public:
    static size_t Memory(size_t maxSlotShift) noexcept;

public:
    std::string DebugString() const noexcept;

private:
    char _data[0];
};

class LeafNode final : public Node
{
public:
    explicit LeafNode(size_t maxSlotShift) noexcept;

    LeafNode(const LeafNode&) = delete;
    LeafNode(LeafNode&&) = delete;

public:
    KeyType Key(slotid_t idx) const noexcept { return reinterpret_cast<const KeyType*>(_data)[idx]; }
    // Return the smallest key equal to or greater than key.
    bool GetKey(KeyType key, KeyType* result) const noexcept;
    bool SetKey(slotid_t idx, KeyType key) noexcept;

    // Shift elements(whose keys are greater than key) right by 1, then insert key.
    // Caller is responsible that there is empty slot in the leaf and split is not needed.
    // It's safe here because if some read thread is reading during memcpy, it will read one same
    // element twice. This can be fixed during dedup logic in upstream read iterator.
    bool InsertKey(KeyType key) noexcept;
    void Split(LeafNode* newNode1, LeafNode* newNode2, KeyType key) noexcept;
    void Copy(LeafNode* dstNode, slotid_t dstBegin, slotid_t srcBegin, slotid_t srcEnd) const noexcept;

    LeafNode* PrevLeaf() const noexcept { return _prevLeaf; }
    LeafNode* NextLeaf() const noexcept { return _nextLeaf; }

    void SetPrevLeaf(LeafNode* addr) noexcept;
    void SetNextLeaf(LeafNode* addr) noexcept;

public:
    static size_t Memory(size_t maxSlotShift) noexcept;

public:
    std::string DebugString() const noexcept;

private:
    LeafNode* _prevLeaf;
    LeafNode* _nextLeaf;

    char _data[0];
};

inline size_t NodeMemory(Node* node)
{
    assert(node);
    if (node->IsLeafNode()) {
        return LeafNode::Memory(node->MaxSlotShift());
    }
    return InternalNode::Memory(node->MaxSlotShift());
}

inline KeyType Key(Node* node, slotid_t slotId)
{
    assert(node);
    if (node->IsLeafNode()) {
        return static_cast<LeafNode*>(node)->Key(slotId);
    }
    return static_cast<InternalNode*>(node)->Key(slotId);
}

inline Node* InternalNode::Child(slotid_t idx) noexcept
{
    assert(idx >= 0 && idx <= MaxSlot());
    char* base = const_cast<char*>(_data);
    base += sizeof(KeyType) << _maxSlotShift;
    return *(reinterpret_cast<Node**>(base) + idx);
}
inline const Node* InternalNode::Child(slotid_t idx) const noexcept
{
    assert(idx >= 0 && idx <= MaxSlot());
    const char* base = _data;
    base += sizeof(KeyType) << _maxSlotShift;
    return *(reinterpret_cast<const Node* const*>(base) + idx);
}

}}} // namespace indexlib::index::dynamic_search_tree_v1
