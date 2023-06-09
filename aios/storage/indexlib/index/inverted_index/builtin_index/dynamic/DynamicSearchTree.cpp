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
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicSearchTree.h"

#include "autil/StringUtil.h"
#include "indexlib/base/Constant.h"
namespace indexlib::index {

DynamicSearchTree::DynamicSearchTree(size_t maxSlotShift, NodeManager* nodeManager) noexcept
    : _maxSlotShift(maxSlotShift)
    , _rootNode(nullptr)
    , _nodeManager(nodeManager)
    , _maxDocId(INVALID_DOCID)
    , _minDocId(INVALID_DOCID)
    , _docCount(0)
{
}

DynamicSearchTree::~DynamicSearchTree() { RetireDescend(_rootNode); }

void DynamicSearchTree::RetireDescend(Node* node)
{
    if (node == nullptr) {
        return;
    }
    if (!node->IsLeafNode()) {
        auto internalNode = static_cast<InternalNode*>(node);
        for (slotid_t i = 0; i < internalNode->KeySlotUse() + 1; ++i) {
            RetireDescend(internalNode->Child(i));
        }
    }
    _nodeManager->RetireNode(node);
}

bool DynamicSearchTree::InsertInternal(KeyType key) noexcept
{
    if (_maxDocId == INVALID_DOCID || _maxDocId < key.DocId()) {
        _maxDocId = key.DocId();
    }
    if (_minDocId == INVALID_DOCID || _minDocId > key.DocId()) {
        _minDocId = key.DocId();
    }
    auto guard = _nodeManager->CriticalScope();
    if (!_rootNode) {
        _rootNode = AllocateLeafNode();
    }
    KeyType splitKey = KeyType();
    Node* splitNode1 = nullptr;
    Node* splitNode2 = nullptr;
    InsertDescend(_rootNode, key, &splitNode1, &splitNode2, &splitKey);
    if (splitNode1 == nullptr) {
        return true;
    } else if (splitNode2 == nullptr) {
        _rootNode = splitNode1;
    } else {
        InternalNode* newRoot = AllocateInternalNode();
        newRoot->SetKey(0, splitKey);
        newRoot->SetChild(0, splitNode1);
        newRoot->SetChild(1, splitNode2);
        newRoot->SetKeySlotUse(1);
        // TODO: barrier
        _rootNode = newRoot;
        return true;
    }
    return true;
}

bool DynamicSearchTree::Insert(docid_t docId) noexcept
{
    _docCount++;
    return InsertInternal(KeyType(docId, /*isDelete=*/false));
}

bool DynamicSearchTree::InsertDescend(Node* node, KeyType key, Node** splitNode1, Node** splitNode2,
                                      KeyType* splitKey) noexcept
{
    if (node->IsLeafNode()) {
        LeafNode* leafNode = static_cast<LeafNode*>(node);
        // Insert to leaf when split is not needed
        if (leafNode->KeySlotUse() < leafNode->MaxSlot()) {
            return leafNode->InsertKey(key);
        } else {
            slotid_t i = leafNode->GetSlot<LeafNode>(key);
            if (i != std::numeric_limits<slotid_t>::max() && leafNode->Key(i) == key) {
                return leafNode->SetKey(i, key);
            }

            LeafNode* newNode1 = AllocateLeafNode();
            LeafNode* newNode2 = AllocateLeafNode();
            leafNode->Split(newNode1, newNode2, key);
            newNode1->SetPrevLeaf(leafNode->PrevLeaf());
            newNode1->SetNextLeaf(newNode2);
            newNode2->SetPrevLeaf(newNode1);
            newNode2->SetNextLeaf(leafNode->NextLeaf());

            auto nextLeaf = leafNode->NextLeaf();
            if (nextLeaf) {
                nextLeaf->SetPrevLeaf(newNode2);
            }

            // TODO: add barrier to ensure lines above are done before setting node->NextLeaf.
            auto prevLeaf = leafNode->PrevLeaf();
            if (prevLeaf) {
                prevLeaf->SetNextLeaf(newNode1);
            }

            *splitNode1 = newNode1;
            *splitNode2 = newNode2;
            *splitKey = newNode2->Key(0);

            _nodeManager->RetireNode(leafNode);
            _nodeManager->IncreaseEpoch();
            return true;
        }
    }

    InternalNode* internalNode = static_cast<InternalNode*>(node);
    slotid_t idx = FindChildSlot(internalNode, key);
    bool r = InsertDescend(internalNode->Child(idx), key, splitNode1, splitNode2, splitKey);
    if (*splitNode1 == nullptr) {
        return r;
    }
    if (*splitNode2 == nullptr) {
        internalNode->SetChild(idx, *splitNode1);
        *splitNode1 = nullptr;
        return true;
    }
    if (internalNode->KeySlotUse() < internalNode->MaxSlot()) {
        InternalNode* newNode = AllocateInternalNode();
        internalNode->CopyAndInsert(newNode, *splitKey, *splitNode1, *splitNode2);
        *splitNode1 = newNode;
        *splitNode2 = nullptr;
        _nodeManager->RetireNode(internalNode);
        _nodeManager->IncreaseEpoch();
        return true;
    }
    InternalNode* newNode1 = AllocateInternalNode();
    InternalNode* newNode2 = AllocateInternalNode();
    internalNode->Split(*splitKey, newNode1, newNode2, *splitNode1, *splitNode2, splitKey);
    *splitNode1 = newNode1;
    *splitNode2 = newNode2;
    _nodeManager->RetireNode(internalNode);
    _nodeManager->IncreaseEpoch();
    return true;
}

slotid_t DynamicSearchTree::FindChildSlot(const InternalNode* node, KeyType key) const noexcept
{
    // TODO: binary search
    assert(!node->IsLeafNode());
    uint16_t i = 0;
    while (i < node->KeySlotUse()) {
        if (node->Key(i) < key) {
            i++;
        } else {
            break;
        }
    }
    return i;
}

Node* DynamicSearchTree::FindChild(InternalNode* node, KeyType key) noexcept
{
    assert(!node->IsLeafNode());
    return node->Child(FindChildSlot(node, key));
}

const Node* DynamicSearchTree::FindChild(const InternalNode* node, KeyType key) const noexcept
{
    assert(!node->IsLeafNode());
    return node->Child(FindChildSlot(node, key));
}

bool DynamicSearchTree::Search(docid_t docId, KeyType* result) const noexcept
{
    auto guard = _nodeManager->CriticalScope();
    const LeafNode* leafNode = nullptr;
    slotid_t slotId = INVALID_SLOTID;
    return SearchUnsafe(docId, result, &leafNode, &slotId);
}

bool DynamicSearchTree::SearchUnsafe(docid_t docId, KeyType* result, const LeafNode** leafNode,
                                     slotid_t* slotId) const noexcept
{
    if (!_rootNode) {
        return false;
    }
    const Node* curNode = _rootNode;
    KeyType targetKey(docId, false);
    while (curNode) {
        if (curNode->IsLeafNode()) {
            const auto curLeafNode = static_cast<const LeafNode*>(curNode);
            slotid_t curSlotId = curLeafNode->GetSlot<LeafNode>(targetKey);
            if (curSlotId != INVALID_SLOTID) {
                *result = curLeafNode->Key(curSlotId);
                *leafNode = curLeafNode;
                *slotId = curSlotId;
                return true;
            }
            curNode = curLeafNode->NextLeaf();
        } else {
            const InternalNode* internalNode = static_cast<const InternalNode*>(curNode);
            curNode = FindChild(internalNode, KeyType(docId, false));
        }
    }
    return false;
}

bool DynamicSearchTree::Remove(docid_t docId) noexcept { return InsertInternal(KeyType(docId, /*isDelete=*/true)); }

LeafNode* DynamicSearchTree::AllocateLeafNode() noexcept { return _nodeManager->AllocateLeafNode(_maxSlotShift); }

InternalNode* DynamicSearchTree::AllocateInternalNode() noexcept
{
    return _nodeManager->AllocateInternalNode(_maxSlotShift);
}

std::string DynamicSearchTree::DebugString() const noexcept
{
    if (!_rootNode) {
        return "empty";
    }
    std::string res;
    std::vector<Node*> currentLevel;
    std::vector<Node*> nextLevel;
    currentLevel.push_back(_rootNode);
    int level = 0;
    while (true) {
        nextLevel.clear();
        res += "level " + autil::StringUtil::toString<int>(level) +
               " ######################################################\n";
        for (int i = 0; i < currentLevel.size(); ++i) {
            Node* curNode = currentLevel[i];
            res += "node " + autil::StringUtil::toString<int>(i) + ": ";
            if (!curNode->IsLeafNode()) {
                for (slotid_t j = 0; j < curNode->KeySlotUse() + 1; ++j) {
                    if (static_cast<InternalNode*>(currentLevel[i])->Child(j) != nullptr) {
                        nextLevel.push_back(static_cast<InternalNode*>(currentLevel[i])->Child(j));
                    }
                }
                res += static_cast<InternalNode*>(currentLevel[i])->DebugString();
            } else {
                res += static_cast<LeafNode*>(currentLevel[i])->DebugString();
            }
        }
        res += "\n";
        level++;
        if (nextLevel.empty()) {
            break;
        } else {
            currentLevel = nextLevel;
        }
    }
    return res;
}

DynamicSearchTree::Iterator DynamicSearchTree::CreateIterator() const { return DynamicSearchTree::Iterator(this); }

DynamicSearchTree::Iterator::Iterator(const DynamicSearchTree* tree)
    : _tree(tree)
    , _currentLeaf(nullptr)
    , _currentDocId(INVALID_DOCID)
    , _currentSlot(INVALID_SLOTID)
{
    _guard = _tree->_nodeManager->CriticalScope();
}

DynamicSearchTree::Iterator::~Iterator() {}

DynamicSearchTree::Iterator::Iterator(DynamicSearchTree::Iterator&& other)
    : _tree(std::exchange(other._tree, nullptr))
    , _guard(std::move(other._guard))
    , _currentLeaf(std::exchange(other._currentLeaf, nullptr))
    , _currentDocId(std::exchange(other._currentDocId, INVALID_DOCID))
    , _currentSlot(std::exchange(other._currentSlot, INVALID_SLOTID))
{
}

DynamicSearchTree::Iterator& DynamicSearchTree::Iterator::operator=(DynamicSearchTree::Iterator&& other)
{
    if (this != &other) {
        _tree = std::exchange(other._tree, nullptr);
        _guard = std::move(other._guard);
        _currentLeaf = std::exchange(other._currentLeaf, nullptr);
        _currentDocId = std::exchange(other._currentDocId, INVALID_DOCID);
        _currentSlot = std::exchange(other._currentSlot, INVALID_SLOTID);
    }
    return *this;
}

} // namespace indexlib::index
