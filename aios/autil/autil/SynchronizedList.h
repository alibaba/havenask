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

#include "autil/Lock.h"

namespace autil {

template <class T>
struct ListNode {
    T _value;
    struct ListNode<T> *_prev;
    struct ListNode<T> *_next;

    ListNode() : _prev(NULL), _next(NULL) {}

    ListNode(const T &value) : _prev(NULL), _next(NULL) { _value = value; }

    ListNode(const T &value, struct ListNode<T> *prev, struct ListNode<T> *next) : _prev(prev), _next(next) {
        _value = value;
    }
};

template <class T>
class SynchronizedList {
public:
    typedef ListNode<T> *ListNodePtr;

public:
    SynchronizedList();
    ~SynchronizedList();

public:
    bool getFront(T &value);
    bool getFront(ListNodePtr &p);

    bool getBack(T &value);
    bool getBack(ListNodePtr &p);

    ListNodePtr pushFront(const T &value);
    ListNodePtr pushFront(ListNode<T> *node);
    void popFront();

    ListNodePtr pushBack(const T &value);
    ListNodePtr pushBack(ListNode<T> *node);
    void popBack();

    void erase(ListNodePtr p);

    uint32_t getSize() const { return _size; }
    bool isEmpty() { return _size == 0; }

public:
    void moveToBack(ListNodePtr p);
    /* after calling, memory of p is not released */
    void unLink(ListNodePtr p);

private:
    void insertBefore(ListNodePtr next, const T &value);
    void insertBefore(ListNodePtr next, ListNodePtr p);

    void insertAfter(ListNodePtr prev, const T &value);
    void insertAfter(ListNodePtr prev, ListNodePtr p);

    void unLinkWithoutLock(ListNodePtr p);

private:
    uint32_t _size;
    ListNodePtr _head;
    ListNodePtr _tail;
    mutable RecursiveThreadMutex _listMutex;
};

template <class T>
SynchronizedList<T>::SynchronizedList() : _size(0), _head(NULL), _tail(NULL) {}

template <class T>
SynchronizedList<T>::~SynchronizedList() {
    while (_head) {
        ListNodePtr next = _head->_next;
        delete _head;
        _head = next;
    }
}

template <class T>
bool SynchronizedList<T>::getFront(T &value) {
    ScopedLock lock(_listMutex);
    if (isEmpty())
        return false;

    value = _head->_value;
    return true;
}

template <class T>
bool SynchronizedList<T>::getFront(ListNodePtr &p) {
    ScopedLock lock(_listMutex);
    if (isEmpty())
        return false;

    p = _head;
    return true;
}

template <class T>
bool SynchronizedList<T>::getBack(T &value) {
    ScopedLock lock(_listMutex);
    if (isEmpty())
        return false;

    value = _tail->_value;

    return true;
}

template <class T>
bool SynchronizedList<T>::getBack(ListNodePtr &p) {
    ScopedLock lock(_listMutex);
    if (isEmpty())
        return false;

    p = _tail;
    return true;
}

template <class T>
ListNode<T> *SynchronizedList<T>::pushFront(const T &value) {
    ScopedLock lock(_listMutex);
    ListNodePtr p = new ListNode<T>(value, NULL, _head);
    if (_head) {
        _head->_prev = p;
    }
    _head = p;
    if (!_tail)
        _tail = p;
    _size++;
    return p;
}

template <class T>
ListNode<T> *SynchronizedList<T>::pushFront(ListNode<T> *node) {
    ScopedLock lock(_listMutex);
    node->_prev = NULL;
    node->_next = _head;

    if (_head) {
        _head->_prev = node;
    }
    _head = node;
    if (!_tail)
        _tail = node;
    _size++;
    return node;
}

template <class T>
void SynchronizedList<T>::popFront() {
    ScopedLock lock(_listMutex);
    if (!isEmpty()) {
        ListNodePtr p = _head;
        _head = _head->_next;
        if (_head)
            _head->_prev = NULL;
        else
            _tail = NULL;
        delete p;
        _size--;
    }
}

template <class T>
ListNode<T> *SynchronizedList<T>::pushBack(const T &value) {
    ScopedLock lock(_listMutex);
    ListNodePtr p = new ListNode<T>(value, _tail, NULL);
    if (_tail) {
        _tail->_next = p;
    }
    _tail = p;
    if (!_head)
        _head = p;
    _size++;

    return p;
}

template <class T>
ListNode<T> *SynchronizedList<T>::pushBack(ListNode<T> *node) {
    ScopedLock lock(_listMutex);
    node->_prev = _tail;
    node->_next = NULL;
    if (_tail) {
        _tail->_next = node;
    }
    _tail = node;
    if (!_head)
        _head = node;
    _size++;

    return node;
}

template <class T>
void SynchronizedList<T>::popBack() {
    ScopedLock lock(_listMutex);
    if (!isEmpty()) {
        ListNodePtr p = _tail;
        _tail = _tail->_prev;
        if (_tail)
            _tail->_next = NULL;
        else
            _head = NULL;
        delete p;
        _size--;
    }
}

template <class T>
void SynchronizedList<T>::insertBefore(ListNodePtr next, const T &value) {
    ListNodePtr prev = next->_prev;
    ListNodePtr p = new ListNode<T>(value, prev, next);
    next->_prev = p;
    if (prev)
        prev->_next = p;
    else
        _head = p;
    _size++;
}

template <class T>
void SynchronizedList<T>::insertBefore(ListNodePtr next, ListNodePtr p) {
    ListNodePtr prev = next->_prev;
    next->_prev = p;
    p->_prev = prev;
    p->_next = next;
    if (prev)
        prev->_next = p;
    else
        _head = p;
    _size++;
}

template <class T>
void SynchronizedList<T>::insertAfter(ListNodePtr prev, const T &value) {
    ListNodePtr next = prev->_next;
    ListNodePtr p = new ListNode<T>(value, prev, next);
    prev->_next = p;
    if (next)
        next->_prev = p;
    else
        _tail = p;
    _size++;
}

template <class T>
void SynchronizedList<T>::insertAfter(ListNodePtr prev, ListNodePtr p) {
    ListNodePtr next = prev->_next;
    prev->_next = p;
    p->_prev = prev;
    p->_next = next;
    if (next)
        next->_prev = p;
    else
        _tail = p;
    _size++;
}

template <class T>
void SynchronizedList<T>::erase(ListNodePtr p) {
    ScopedLock lock(_listMutex);
    unLink(p);
    delete p;
}

template <class T>
void SynchronizedList<T>::unLink(ListNodePtr p) {
    ScopedLock lock(_listMutex);
    unLinkWithoutLock(p);
}

template <class T>
void SynchronizedList<T>::unLinkWithoutLock(ListNodePtr p) {
    ListNodePtr prev = p->_prev;
    ListNodePtr next = p->_next;
    if (!prev) {
        _head = next;
        if (next)
            next->_prev = NULL;
    }
    if (!next) {
        _tail = prev;
        if (prev)
            prev->_next = NULL;
    }
    if (prev && next) {
        prev->_next = next;
        next->_prev = prev;
    }
    _size--;
}

template <class T>
void SynchronizedList<T>::moveToBack(ListNodePtr p) {
    ScopedLock lock(_listMutex);
    if (p == _tail)
        return;

    unLinkWithoutLock(p);
    insertAfter(_tail, p);
}

} // namespace autil
