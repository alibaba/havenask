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

#include <cassert>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <stddef.h>
#include <vector>

#include "autil/Lock.h"

namespace autil {

template <typename T, typename Alloc = std::allocator<T>>
class SnapshotVector {
public:
    typedef T value_type;
    typedef typename std::vector<T>::difference_type difference_type;
    typedef typename std::vector<T>::size_type size_type;
    typedef value_type &reference;
    typedef const value_type &const_reference;
    typedef value_type *pointer;
    typedef const value_type *const_pointer;
    typedef std::vector<T, Alloc> vector_type;

public:
    class Iterator {
    public:
        Iterator(std::shared_ptr<vector_type> snapshot, size_type sz) : _snapshot(snapshot), _cursor(0), _size(sz) {}
        bool has_next() const { return _cursor < _size; }
        const_reference next() { return _snapshot->operator[](_cursor++); }

    private:
        std::shared_ptr<vector_type> _snapshot;
        size_type _cursor;
        size_type _size;
    };

public:
    SnapshotVector()
        : _allocator()
        , _data(nullptr)
        , _currentCapacity(0)
        , _currentSize(0)
        , _vector(std::make_shared<vector_type>(_allocator)) {
        init();
    }
    SnapshotVector(Alloc allocator)
        : _allocator(allocator)
        , _data(nullptr)
        , _currentCapacity(0)
        , _currentSize(0)
        , _vector(std::make_shared<vector_type>(_allocator)) {
        init();
    }

    void init() {
        _vector->resize(4);
        _currentCapacity = 4;
        _data = _vector->data();
    }

    template <typename Value>
    void push_back(Value &&item) {
        if (_currentSize == _currentCapacity) {
            reallocate();
        }
        _data[_currentSize] = std::forward<Value>(item);
        _currentSize++;
    }

    size_type size() const { return _currentSize; }

    const_reference operator[](size_type n) const {
        auto target = this->currrent();
        return target->operator[](n);
    }

    reference operator[](size_type n) {
        auto target = this->current();
        return target->operator[](n);
    }

    Iterator snapshot() { return Iterator(current(), _currentSize); }

    typename vector_type::iterator begin() {
        auto target = this->current();
        return target->begin();
    }

    typename vector_type::iterator end() {
        auto target = this->current();
        return target->begin() + _currentSize;
    }

private:
    std::shared_ptr<vector_type> current() const {
        ScopedLock lk(_mutex);
        return _vector;
    }
    void reallocate() {
        std::shared_ptr<vector_type> target(std::make_shared<vector_type>(_allocator));
        assert(_currentCapacity == _vector->capacity());
        target->resize(_vector->size() * 2);
        for (size_type i = 0; i < _vector->size(); ++i) {
            target->operator[](i) = _vector->operator[](i);
        }
        _currentCapacity = target->capacity();
        ScopedLock lk(_mutex);
        _vector = target;
        _data = _vector->data();
    }

private:
    Alloc _allocator;
    T *_data;
    size_type _currentCapacity;
    size_type _currentSize;
    std::shared_ptr<vector_type> _vector;
    mutable ThreadMutex _mutex;
};

} // namespace autil
