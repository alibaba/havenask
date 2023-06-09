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
#include <vector>

namespace autil {

template<typename T>
class LockFreeStack
{
    struct Node {
        uint32_t mNext;
        uint32_t mNextFreeNode;
        T mValue;
    };

public:
    explicit LockFreeStack(size_t size = 65536);
    ~LockFreeStack();

    bool Push(const T &elem);
    bool Pop(T *elem);
    size_t Size() const;
    bool Empty() const;

private:
    size_t Normalize(size_t size) const;
    inline int32_t GetIndex(Node *node) const;
    bool AcquireNode(Node **node);
    void ReleaseNode(Node *node);

private:
    static const uint64_t kNullPointer = 0xFFFFFFFF00000000;
    size_t mLimit;
    size_t mSize;
    __attribute__((aligned(64))) Node *mNodeBuffer;
    __attribute__((aligned(64))) uint64_t mHead;
    __attribute__((aligned(64))) uint64_t mFreeNodeHead;
};

template<typename T>
LockFreeStack<T>::LockFreeStack(size_t size)
{
    mLimit = Normalize(size);
    mSize = 0;
    mFreeNodeHead = kNullPointer;
    mNodeBuffer = new Node[mLimit];

    std::vector<size_t> index;
    index.reserve(mLimit);

    for (size_t k = 0; k < mLimit; ++k) {
        index.push_back(k);
    }

    std::random_shuffle(index.begin(), index.end());

    for (size_t k = 0; k < mLimit; ++k) {
        Node &node = mNodeBuffer[index[k]];
        node.mNext = 0;
        node.mNextFreeNode = detail::kLockFreeNullPointer;
        ReleaseNode(&node);
    }

    mHead = kNullPointer;
}

template<typename T>
LockFreeStack<T>::~LockFreeStack()
{
    delete[] mNodeBuffer;
}

template<typename T>
bool LockFreeStack<T>::Push(const T &elem)
{
    Node *node = 0;

    if (!AcquireNode(&node)) {
        return false;
    }

    node->mValue = elem;
    detail::Pointer head;
    uint32_t index = GetIndex(node);
    detail::Pointer newhead;

    do {
        head = mHead;
        node->mNext = detail::Index(head);
        newhead = detail::MakePointer(index, detail::Tag(head) + 1);
    } while(!AtomicCompareExchange(&mHead, newhead, head));

    AtomicIncrement(&mSize);
    return true;
}

template<typename T>
bool LockFreeStack<T>::Pop(T *elem)
{
    detail::Pointer head;
    detail::Pointer newhead;
    Node *node = 0;

    do {
        head = mHead;
        size_t index = detail::Index(head);

        if (index == detail::kLockFreeNullPointer) {
            return false;
        }

        node = &mNodeBuffer[index];
        newhead = detail::MakePointer(node->mNext, detail::Tag(head) + 1);
    } while (!AtomicCompareExchange(&mHead, newhead, head));

    *elem = node->mValue;
    ReleaseNode(node);

    AtomicDecrement(&mSize);
    return true;
}

template<typename T>
size_t LockFreeStack<T>::Size() const
{
    return AtomicGet(&mSize);
}

template<typename T>
bool LockFreeStack<T>::Empty() const
{
    return Size() == 0;
}

template<typename T>
size_t LockFreeStack<T>::Normalize(size_t size) const
{
    if (size == 0 || size >= (1 << 24)) {
        abort();
    }

    return size;
}

template<typename T>
int32_t LockFreeStack<T>::GetIndex(Node *node) const
{
    return node - mNodeBuffer;
}

template<typename T>
bool LockFreeStack<T>::AcquireNode(Node **node)
{
    Node *pnode;
    uint64_t head;
    uint64_t newhead;

    do {
        head = mFreeNodeHead;
        uint32_t index = detail::Index(head);

        if (__builtin_expect(index == detail::kLockFreeNullPointer, 0)) {
            return false;
        }

        pnode = &mNodeBuffer[index];
        uint32_t tag = detail::Tag(head) + 1;
        newhead = detail::MakePointer(pnode->mNextFreeNode, tag);
    } while (!AtomicCompareExchange(&mFreeNodeHead, newhead, head));

    *node = pnode;
    return true;
}

template<typename T>
void LockFreeStack<T>::ReleaseNode(Node *node)
{
    // put this node into the head atomically
    node->mValue = T();
    uint64_t head;
    uint64_t newhead;

    do {
        head = mFreeNodeHead;
        newhead = detail::MakePointer(GetIndex(node),
                                      detail::Tag(head) + 1);
        node->mNextFreeNode = detail::Index(head);
    } while (!AtomicCompareExchange(&mFreeNodeHead, newhead, head));
}

} // namespace autil
