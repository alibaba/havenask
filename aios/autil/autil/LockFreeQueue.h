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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "autil/LockFree.h"

namespace autil {

static const size_t kLockFreeQueueFull = 0xFFFFFFFF;

template <typename T>
class LockFreeQueue {
    struct Node {
        uint64_t mPrev;
        uint64_t mNext;
        Node *mNextFreeNode;
        T mValue;
    };

public:
    LockFreeQueue();
    ~LockFreeQueue();

    size_t Push(const T &elem);
    bool Pop(T *elem);
    size_t Size() const;
    bool Empty() const;

private:
    bool AcquireNode(Node **node, bool allocateIfBufferEmpty = true);
    void ReleaseNode(Node *node);
    void FixList(uint64_t tail, uint64_t head);
    inline static Node *ConvertToNode(uint64_t pointer) { return static_cast<Node *>(detail::GetPointer(pointer)); }

private:
    static const uint64_t kNullPointer = 0xFFFF000000000000;
    static const uint64_t kBatchAllocNum = 16;
    size_t mSize;
    __attribute__((aligned(64))) volatile uint64_t mHead;
    __attribute__((aligned(64))) volatile uint64_t mTail;
    __attribute__((aligned(64))) uint64_t mFreeNodeHead;
};

template <typename T>
LockFreeQueue<T>::LockFreeQueue() {
    mSize = 0;
    mFreeNodeHead = kNullPointer;

    Node *node = 0;
    AcquireNode(&node);
    node->mNext = kNullPointer;
    node->mPrev = detail::MakePointer(node, detail::kInvalidTag);
    mHead = detail::MakePointer(node, 0);
    mTail = mHead;
}

template <typename T>
LockFreeQueue<T>::~LockFreeQueue() {
    T elem;

    while (Pop(&elem)) {}

    Node *node = 0;

    while (AcquireNode(&node, false)) {
        delete node;
    }

    node = ConvertToNode(mHead);
    delete node;
}

template <typename T>
size_t LockFreeQueue<T>::Push(const T &elem) {
    uint64_t tail;
    Node *node = 0;
    AcquireNode(&node);
    node->mValue = elem;
    size_t ret = AtomicIncrement(&mSize);
    node->mPrev = detail::MakePointer(NULL, detail::kInvalidTag);
    while (true) {
        tail = mTail;
        uint32_t tag = detail::GetNextTag(tail);
        node->mNext = detail::MakePointer(detail::GetPointer(tail), tag);
        uint64_t newtail = detail::MakePointer(node, tag);
        if (AtomicCompareExchange(&mTail, newtail, tail)) {
            uint64_t prev = detail::MakePointer(node, detail::GetTag(tail));
            Node *ptail = ConvertToNode(tail);
            ptail->mPrev = prev;
            break;
        }
    }

    return ret;
}

template <typename T>
bool LockFreeQueue<T>::Pop(T *elem) {
    uint64_t tail, head, prev_of_first;

    while (true) {
        head = mHead;
        tail = mTail;
        Node *phead = ConvertToNode(head);
        prev_of_first = phead->mPrev;

        if (head == mHead) {
            if (tail != head) {
                uint16_t tag1 = detail::GetTag(prev_of_first);
                if (__builtin_expect(tag1 == detail::kInvalidTag, 0)) {
                    FixList(tail, head);
                    continue;
                }
                __asm__ __volatile__("" ::: "memory");
                if (__builtin_expect(tag1 != detail::GetTag(head), 0)) {
                    FixList(tail, head);
                    continue;
                }

                Node *ppof = ConvertToNode(prev_of_first);
                *elem = ppof->mValue;
                uint64_t newhead = detail::MakePointer(ppof, detail::GetNextTag(head));

                if (AtomicCompareExchange(&mHead, newhead, head)) {
                    ReleaseNode(phead);
                    AtomicDecrement(&mSize);
                    break;
                }
            } else {
                return false;
            }
        }
    }

    return true;
}

template <typename T>
void LockFreeQueue<T>::FixList(uint64_t tail, uint64_t head) {
    uint64_t current, next;
    current = tail;
    while (head == mHead && current != head) {
        Node *pcurrent = ConvertToNode(current);
        next = pcurrent->mNext;
        Node *pnext = ConvertToNode(next);
        pnext->mPrev = detail::MakePointer(pcurrent, detail::GetPrevTag(current));
        current = detail::MakePointer(pnext, detail::GetPrevTag(current));
    }
}

template <typename T>
size_t LockFreeQueue<T>::Size() const {
    return AtomicGet(&mSize);
}

template <typename T>
bool LockFreeQueue<T>::Empty() const {
    return Size() == 0;
}

template <typename T>
bool LockFreeQueue<T>::AcquireNode(Node **node, bool allocateIfBufferEmpty) {
    Node *pnode;
    uint64_t head;
    uint64_t newhead;

    do {
        head = mFreeNodeHead;
        pnode = ConvertToNode(head);

        if (__builtin_expect(pnode == 0, 0)) {
            if (__builtin_expect(allocateIfBufferEmpty, 1)) {
                pnode = new Node;
                pnode->mNextFreeNode = 0;
                break;
            } else {
                return false;
            }
        }

        uint32_t tag = detail::GetNextTag(head);
        newhead = detail::MakePointer(pnode->mNextFreeNode, tag);
    } while (!AtomicCompareExchange(&mFreeNodeHead, newhead, head));

    *node = pnode;
    return true;
}

template <typename T>
void LockFreeQueue<T>::ReleaseNode(Node *node) {
    // put this node into the head atomical
    node->mValue = T();
    uint64_t head;
    uint64_t newhead;

    do {
        head = mFreeNodeHead;
        newhead = detail::MakePointer(node, detail::GetNextTag(head));
        node->mPrev = detail::MakePointer(NULL, detail::kInvalidTag);
        node->mNextFreeNode = ConvertToNode(head);
    } while (!AtomicCompareExchange(&mFreeNodeHead, newhead, head));
}

} // namespace autil
