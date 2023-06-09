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

#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicSearchTreeNode.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/NodeManager.h"

namespace indexlib::index {

class DynamicSearchTree
{
public:
    class Iterator
    {
    public:
        Iterator(const DynamicSearchTree* tree);
        ~Iterator();
        Iterator(const Iterator&) = delete;
        Iterator& operator=(const Iterator&) = delete;
        Iterator(Iterator&&);
        Iterator& operator=(Iterator&&);

    public:
        bool Seek(docid_t docId, KeyType* result);

    private:
        const DynamicSearchTree* _tree;
        util::EpochBasedReclaimManager::CriticalGuard _guard;
        const LeafNode* _currentLeaf;
        docid_t _currentDocId;
        slotid_t _currentSlot;
    };

public:
    explicit DynamicSearchTree(size_t maxSlotShift, NodeManager* nodeManager) noexcept;
    ~DynamicSearchTree();

    DynamicSearchTree(const DynamicSearchTree&) = delete;
    DynamicSearchTree(DynamicSearchTree&&) = delete;

public:
    // search the first element greater or eqaual to the key
    bool Search(docid_t docId, KeyType* result) const noexcept;
    bool Insert(docid_t docId) noexcept;
    bool Remove(docid_t docId) noexcept;

    docid_t MaxDocId() const { return _maxDocId; }
    docid_t MinDocId() const { return _minDocId; }
    size_t EstimateDocCount() const { return _docCount; }

    Iterator CreateIterator() const;

public:
    std::string DebugString() const noexcept;

private:
    // @return: insertation happend
    bool InsertDescend(Node* node, KeyType key, Node** splitNode1, Node** splitNode2, KeyType* splitKey) noexcept;
    slotid_t FindChildSlot(const InternalNode* node, KeyType key) const noexcept;
    Node* FindChild(InternalNode* node, KeyType key) noexcept;
    const Node* FindChild(const InternalNode* node, KeyType key) const noexcept;

    bool InsertInternal(KeyType key) noexcept;

    LeafNode* AllocateLeafNode() noexcept;
    InternalNode* AllocateInternalNode() noexcept;

    void RetireDescend(Node* node);

    bool SearchUnsafe(docid_t docId, KeyType* result, const LeafNode** leafNode, slotid_t* slotId) const noexcept;

private:
    size_t _maxSlotShift;
    Node* _rootNode = nullptr;
    NodeManager* _nodeManager = nullptr;
    docid_t _maxDocId;
    docid_t _minDocId;
    size_t _docCount;

private:
    friend class Iterator;
};

inline bool DynamicSearchTree::Iterator::Seek(docid_t docId, KeyType* result)
{
    constexpr auto CONST_INVALID_SLOTID = INVALID_SLOTID;
    docId = std::max(docId, _currentDocId + 1); // _currentDocId starts from -1
    if (!_currentLeaf or docId != _currentDocId + 1) {
        bool r = _tree->SearchUnsafe(docId, result, &_currentLeaf, &_currentSlot);
        _currentDocId = result->DocId();
        return r;
    }
    while (_currentLeaf) {
        if (++_currentSlot < _currentLeaf->KeySlotUse()) {
            auto key = _currentLeaf->Key(_currentSlot);
            if (_currentDocId == key.DocId()) {
                // this leaf is modified by write thread simultaneously, we skip duplicate key
                continue;
            }
            _currentDocId = key.DocId();
            *result = key;
            return true;
        }
        _currentLeaf = _currentLeaf->NextLeaf();
        _currentSlot = CONST_INVALID_SLOTID;
    }
    return false;
}
} // namespace indexlib::index
