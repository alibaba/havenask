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

#include <stddef.h>
#include <stdint.h>

#include "ha3/rank/DistinctInfo.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
class SimpleSegregatedAllocator;
}  // namespace autil
namespace isearch {
namespace rank {
class Comparator;
}  // namespace rank
}  // namespace isearch

namespace isearch {
namespace rank {

class TreeNode {
public:
    TreeNode *_left;
    TreeNode *_right;
    TreeNode *_parent;
    DistinctInfo *_sdi;
    int8_t _balance;
    TreeNode() {
        _left = NULL;
        _right = NULL;
        _parent = NULL;
        _sdi = NULL;
        _balance = 0;
    }
    uint32_t getCount() { //for test
        uint32_t count = 0;
        DistinctInfo *info = _sdi;
        while(info) {
            count++;
            info = info->_next;
        }
        return count;
    }
};

class DistinctMap
{
public:
    DistinctMap(const Comparator *keyCmp, uint32_t maxSize);
    ~DistinctMap();
public:
    bool isEmpty() const;
    void insert(DistinctInfo *sdi);
    bool remove(DistinctInfo *sdi);
    bool removeFirstDistinctInfo(DistinctInfo *sdi);
    void clear();
    TreeNode* getRoot() { return _root; }
    uint32_t getNodeCount() { return _nodeCount; }
//////////////////////////////////////////////////////////////
// just for test
//
    void printTreeBalance(TreeNode *curNode);
    void checkTreeBalance(TreeNode *tn);

private:
    uint32_t getTreeHeight(const TreeNode *root);
    void clearTreeNodes(TreeNode *root);
    void checkBalance(TreeNode *node);
    void checkRemoveBalance(TreeNode *node);
    TreeNode* rotateLeftLeft(TreeNode *topNode);
    TreeNode* rotateRightRight(TreeNode *topNode);
    TreeNode* rotateLeftRight(TreeNode *topNode);
    TreeNode* rotateRightLeft(TreeNode *topNode);

    TreeNode* findReplacementNode(TreeNode* curNode);
    void replaceTwoNode(TreeNode *oldNode, TreeNode *replaceNode);
    void newLeftChild(TreeNode *tn, DistinctInfo *sdi);
    void newRightChild(TreeNode *tn, DistinctInfo *sdi);
    TreeNode* adjustLeftBalance(TreeNode *node);
    TreeNode* adjustRightBalance(TreeNode *node);
    void pushFront(TreeNode *tn, DistinctInfo *sdi);
    void attachNewTopNode(TreeNode *parentNode, TreeNode *oldTopNode, 
                          TreeNode *newTopNode);
    void setLeftChild(TreeNode *p, TreeNode *lc) {
        p->_left = lc; 
        if (lc) {
            lc->_parent = p;
        }
    }

    void setRightChild(TreeNode *p, TreeNode *rc) {
        p->_right = rc; 
        if (rc) {
            rc->_parent = p;
        }
    }

    int8_t maxBalance(int8_t a, int8_t b) {
        return a > b ? a : b;
    }

    int8_t minBalance(int8_t a, int8_t b) {
        return a < b ? a : b;
    }

    void findAndRemoveDistinctInfo(TreeNode *curNode, DistinctInfo *sdi) {
        DistinctInfo *p1 = curNode->_sdi;
        DistinctInfo *p2 = p1;
        
        while (p1 != sdi) {
            p2 = p1;
            p1 = p1->_next;
        }

        if (p1 == NULL) {
            return;
        }

        if (p1 != p2) {
            p2->_next = sdi->_next;
        } else {
            curNode->_sdi = sdi->_next;
        }
    }

private:
    TreeNode *_root;
    uint32_t _nodeCount;
    uint32_t _maxSize;
    const Comparator *_keyCmp;
    autil::SimpleSegregatedAllocator *_allocator;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace rank
} // namespace isearch

