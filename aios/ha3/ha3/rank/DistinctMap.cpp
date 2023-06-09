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
#include "ha3/rank/DistinctMap.h"

#include <assert.h>
#include <cstddef>
#include <new>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/LongHashValue.h"
#include "autil/SimpleSegregatedAllocator.h"
#include "ha3/rank/Comparator.h"
#include "ha3/rank/DistinctInfo.h"

using namespace std;
using namespace autil;

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, DistinctMap);

DistinctMap::DistinctMap(const Comparator *keyCmp, uint32_t maxSize) { 
    assert(keyCmp);
    _keyCmp = keyCmp;
    _nodeCount = 0;
    _root = NULL;
    _maxSize = maxSize;
    _allocator = new SimpleSegregatedAllocator();
    _allocator->init(sizeof(TreeNode), _maxSize + 1);
}

DistinctMap::~DistinctMap() { 
    clear();

    delete _allocator;
    _allocator = NULL;
}

bool DistinctMap::isEmpty() const {
    return _nodeCount == 0;
}

void DistinctMap::pushFront(TreeNode *tn, DistinctInfo *sdi) {
    sdi->_next = tn->_sdi;
    tn->_sdi = sdi;
    sdi->setTreeNode(tn);
}

void DistinctMap::newLeftChild(TreeNode *curNode, DistinctInfo *sdi) {
    TreeNode *newNode = new (_allocator->allocate(sizeof(TreeNode))) TreeNode;
    assert(newNode);
    pushFront(newNode, sdi);
    setLeftChild(curNode, newNode);
    _nodeCount ++;
    checkBalance(newNode);
}

void DistinctMap::newRightChild(TreeNode *curNode, DistinctInfo *sdi) {
    TreeNode *newNode = new (_allocator->allocate(sizeof(TreeNode))) TreeNode;
    assert(newNode);
    pushFront(newNode, sdi);
    setRightChild(curNode, newNode);
    _nodeCount ++;
    checkBalance(newNode);
}

void DistinctMap::insert(DistinctInfo *sdi) {
    if (!_root) {
        assert(_allocator);
        _root = new (_allocator->allocate(sizeof(TreeNode))) TreeNode;    
        assert(_root);
        pushFront(_root, sdi);
        _nodeCount++;
        return;
    }

    TreeNode *curNode = _root;
    bool cmpRet = false;
    while (curNode){
        cmpRet = _keyCmp->compare(sdi->getMatchDoc(),
                curNode->_sdi->getMatchDoc());
        if (cmpRet) {
            if (curNode->_left == NULL) {
                newLeftChild(curNode, sdi);
                return;
            }
            curNode = curNode->_left;
        } else {
            cmpRet = _keyCmp->compare(curNode->_sdi->getMatchDoc(), 
                    sdi->getMatchDoc());
            if (cmpRet) {
                if (curNode->_right == NULL) {
                    newRightChild(curNode, sdi);
                    return;
                }
                curNode = curNode->_right;
            } else {
                pushFront(curNode, sdi);
                return;
            }
        }
    }
}

void DistinctMap::replaceTwoNode(TreeNode *oldNode, TreeNode *replaceNode) {
    assert(replaceNode->_parent);

    if (replaceNode == oldNode->_left) {
        setRightChild(replaceNode, oldNode->_right);
    } else if (replaceNode == oldNode->_right) {
        setLeftChild(replaceNode, oldNode->_left);
    } else {
        // the oldNode is not the parent of the replaceNode
        if (replaceNode == replaceNode->_parent->_left) {
            setLeftChild(replaceNode->_parent, replaceNode->_right);
        } else if (replaceNode == replaceNode->_parent->_right) {
            setRightChild(replaceNode->_parent, replaceNode->_left);
        }
        setLeftChild(replaceNode, oldNode->_left);
        setRightChild(replaceNode, oldNode->_right);
    }

    attachNewTopNode(oldNode->_parent, oldNode, replaceNode);
    replaceNode->_balance = oldNode->_balance;
}

bool DistinctMap::remove(DistinctInfo *sdi) {
    TreeNode *curNode = NULL;
    curNode = sdi->getTreeNode();//findNodeb(sdi);
    if (curNode == NULL) {
        return false;
    }

    TreeNode *replaceNode = findReplacementNode(curNode);
    if (NULL == replaceNode) {
        //the node is leaf node
        checkRemoveBalance(curNode);
        attachNewTopNode(curNode->_parent, curNode, NULL);
    } else {
        checkRemoveBalance(replaceNode);
        replaceTwoNode(curNode, replaceNode);
    }

    if (curNode == _root) {
        _root = replaceNode;
    }

    curNode->~TreeNode();
    _allocator->free(curNode);

    _nodeCount--;
    return true;
}

bool DistinctMap::removeFirstDistinctInfo(DistinctInfo *sdi) {
    TreeNode *curNode = sdi->getTreeNode();
    if (curNode == NULL) {
        return false;
    }
    if (sdi == curNode->_sdi && NULL == sdi->_next) {
        remove(sdi);
    } else {
        findAndRemoveDistinctInfo(curNode, sdi);
    }

    return true;    
}

void DistinctMap::clear() {
    if (_root) {
        clearTreeNodes(_root);
        _root = NULL;
    }
    
    //allocate a new 'SimpleSegregatedAllocator'
    _allocator->release();
    delete _allocator;
    _allocator = NULL;
    _allocator = new SimpleSegregatedAllocator();
    _allocator->init(sizeof(TreeNode), _maxSize + 1);
}

void DistinctMap::clearTreeNodes(TreeNode *root) {
    if (root->_left) {
        clearTreeNodes(root->_left);
    }
    if (root->_right) {
        clearTreeNodes(root->_right);
    }
    _nodeCount--;

    root->~TreeNode();
    _allocator->free(root);
}

void DistinctMap::checkBalance(TreeNode *node) {
    TreeNode *parent = NULL;
    do {
        parent = node->_parent;
        if (parent->_left == node) {
            node = adjustLeftBalance(parent);
        } else {
            node = adjustRightBalance(parent);
        }
    } while (node->_parent && node->_balance != 0);
}

void DistinctMap::checkRemoveBalance(TreeNode *node) {
    TreeNode *parent = NULL;
    while (node && node->_parent) {
        parent = node->_parent;
        if (parent->_left == node) {
            node = adjustRightBalance(parent);
        } else {
            node = adjustLeftBalance(parent);
        }
        if (node->_balance != 0) {
            break;
        }
    } 
}

TreeNode* DistinctMap::adjustLeftBalance(TreeNode *node) {
    node->_balance --;
    if (node->_balance != -2) {
        return node;
    }
    if (node->_left->_balance == 1) {
        return rotateLeftRight(node);
    } else {
        return rotateLeftLeft(node);
    }
}

TreeNode* DistinctMap::adjustRightBalance(TreeNode *node) {
    node->_balance ++;
    if (node->_balance != 2) {
        return node;
    }
    if (node->_right->_balance == -1) {
        return rotateRightLeft(node);
    } else {
        return rotateRightRight(node);
    } 
}

TreeNode* DistinctMap::rotateLeftLeft(TreeNode *topNode) {
    TreeNode *parentNode = topNode->_parent;
    TreeNode *midNode = topNode->_left;
    setLeftChild(topNode, midNode->_right);
    setRightChild(midNode, topNode);
    attachNewTopNode(parentNode, topNode, midNode);
    topNode->_balance = -1 - midNode->_balance;
    midNode->_balance ++ ;
    return midNode;
}

TreeNode* DistinctMap::rotateRightRight(TreeNode *topNode) {
    TreeNode *parentNode = topNode->_parent;
    TreeNode *midNode = topNode->_right;

    setRightChild(topNode, midNode->_left);
    setLeftChild(midNode, topNode);
    attachNewTopNode(parentNode, topNode, midNode);

    topNode->_balance = 1 - midNode->_balance;
    midNode->_balance--;
    return midNode;
}


TreeNode* DistinctMap::rotateLeftRight(TreeNode *topNode) {
    TreeNode *parentNode = topNode->_parent;
    TreeNode *midNode = topNode->_left;
    TreeNode *bottomNode = midNode->_right;

    setLeftChild(topNode, bottomNode->_right);
    setRightChild(midNode, bottomNode->_left);
    setLeftChild(bottomNode, midNode);
    setRightChild(bottomNode, topNode);
    attachNewTopNode(parentNode, topNode, bottomNode);

    topNode->_balance = maxBalance(0, -bottomNode->_balance);
    midNode->_balance = -1 * maxBalance(0, bottomNode->_balance);
    bottomNode->_balance = 0;
    return bottomNode;
}

TreeNode* DistinctMap::rotateRightLeft(TreeNode *topNode) {
    TreeNode *parentNode = topNode->_parent;
    TreeNode *midNode = topNode->_right;
    TreeNode *bottomNode = midNode->_left;

    setRightChild(topNode, bottomNode->_left);
    setLeftChild(midNode, bottomNode->_right);
    setRightChild(bottomNode, midNode);
    setLeftChild(bottomNode, topNode);
    attachNewTopNode(parentNode, topNode, bottomNode);

    topNode->_balance = -1 * maxBalance(0, bottomNode->_balance);
    midNode->_balance = maxBalance(0, -bottomNode->_balance);
    bottomNode->_balance = 0;
    return bottomNode;
}

void DistinctMap::attachNewTopNode(TreeNode *parentNode, TreeNode *oldTopNode, 
                                   TreeNode *newTopNode) 
{
    if (newTopNode) {
        newTopNode->_parent = parentNode;
    }
    if (parentNode == NULL) {
        _root = newTopNode;
    } else if (parentNode->_left == oldTopNode) {
        parentNode->_left = newTopNode;
    } else if (parentNode->_right == oldTopNode) {
        parentNode->_right = newTopNode;
    }    
}

TreeNode* DistinctMap::findReplacementNode(TreeNode* curNode) {
    if (curNode->_balance <= 0) {
        curNode = curNode->_left;
        while (curNode) {
            if (curNode->_right == NULL) {
                return curNode;
            }
            curNode = curNode->_right;
        }
    } else {
        curNode = curNode->_right;
        while (curNode) {
            if (curNode->_left == NULL) {
                return curNode;
            }
            curNode = curNode->_left;
        }
    } 
    return NULL;
}

uint32_t DistinctMap::getTreeHeight(const TreeNode *root) {
    uint32_t leftHeight = 0;
    uint32_t rightHeight = 0;
    if (!root) {
        return 0;
    }

    leftHeight = getTreeHeight(root->_left);
    rightHeight = getTreeHeight(root->_right);

    return (leftHeight > rightHeight) ? leftHeight + 1 : rightHeight + 1;
}

//////////////////////////////////////////////////////////
// just for test
void DistinctMap::printTreeBalance(TreeNode *curNode) {
    if (curNode == NULL) {
        return;
    } else {
        AUTIL_LOG(TRACE3, "docid[%d],balance[%d]", 
            curNode->_sdi->getDocId(), curNode->_balance);
        
        printTreeBalance(curNode->_left);
        printTreeBalance(curNode->_right);
    }
}

void DistinctMap::checkTreeBalance(TreeNode *tn) {
    TreeNode *curNode = tn;
    if (!curNode) {
        return;
    }

    int32_t balance 
        = getTreeHeight(curNode->_right) - getTreeHeight(curNode->_left);
    AUTIL_LOG(DEBUG, "treenode_docid:%d", curNode->_sdi->getDocId());
    assert(balance == curNode->_balance);
    (void)balance;
    checkTreeBalance(curNode->_left);
    checkTreeBalance(curNode->_right);
}

} // namespace rank
} // namespace isearch

