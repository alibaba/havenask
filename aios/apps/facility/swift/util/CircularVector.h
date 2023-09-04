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

#include <stdint.h>
#include <utility>

namespace swift {
namespace util {

template <typename T>
class CircularVector {
public:
    CircularVector(uint32_t capacity = 10) {
        _items = new T[capacity];
        _capacity = capacity;
        _size = 0;
        _back = 0;
    }
    ~CircularVector() {
        if (_items != nullptr) {
            delete[] _items;
        }
    }
    CircularVector(const CircularVector &other) {
        _items = new T[other._capacity]; // not copy elements
        _capacity = other._capacity;
        _size = other._size;
        _back = other._back;
    }

private:
    CircularVector &operator=(const CircularVector &);

public:
    template <typename Value>
    inline void push_back(Value &&t) {
        _back = (_back + 1) % _capacity;
        _items[_back] = std::forward<Value>(t);
        ++_size;
    }
    inline const T &back() const { return _items[_back]; }
    inline const T &get(uint32_t index) const { return _items[index % _capacity]; }
    inline uint32_t capacity() const { return _capacity; }
    inline uint32_t end() const { return (_back + 1) % _capacity; }
    inline uint64_t size() const { return _size; }
    inline bool empty() const { return _size == 0; }
    inline uint32_t forward(uint32_t step) const { return (_back - step + _capacity) % _capacity; }
    inline uint32_t next(uint32_t pos, uint32_t step = 1) const { return (pos + step + _capacity) % _capacity; }

private:
    T *_items = nullptr;
    uint32_t _capacity;
    uint32_t _back;
    uint64_t _size;
};

} // namespace util
} // namespace swift
