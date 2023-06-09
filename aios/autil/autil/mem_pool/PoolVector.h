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
#include <assert.h>
#include <algorithm>
#include <iterator>
#include <memory>
#include <new>
#include <type_traits>

#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/RecyclePool.h"
#include "autil/CommonMacros.h"

namespace autil { namespace mem_pool {

template <typename T>
void construct(T &value, std::__true_type) {
}

template <typename T>
void construct(T &value, std::__false_type) {
    value.T();
}

template <typename T>
void construct(T &value, const T &other, std::__true_type) {
    value = other;
}

template <typename T>
void construct(T &value, const T &other, std::__false_type) {
    new (&value)T(other);
}

template <typename T>
void destroy(T &value, std::__true_type) {
}

template <typename T>
void destroy(T &value, std::__false_type) {
    value.~T();
}

template<typename T>
class PoolVector
{
public:
    typedef PoolVector<T> vector_type;
    typedef T value_type;
    typedef T* pointer;
    typedef const T* const_pointer;
    typedef __gnu_cxx::__normal_iterator<pointer, vector_type> iterator;
    typedef __gnu_cxx::__normal_iterator<const_pointer, vector_type> const_iterator;
    typedef std::reverse_iterator<const_iterator>  const_reverse_iterator;
    typedef std::reverse_iterator<iterator>		 reverse_iterator;
    typedef typename std::__is_scalar<T>::__type _Has_trivial_destructor;
public:
    static const uint32_t INIT_ELEMENT_COUNT = 8;
private:
    template <typename noDtor, typename = void>
    struct Dtor {
        Dtor() = default;
        ~Dtor() {
            // container_of(this, PoolVector, _dtor)
            auto *pv = ((PoolVector *)((char *)(this) - offsetof(PoolVector, _dtor)));
            pv->clear();
        };
    };
    template <typename UnusedT>
    struct Dtor<std::true_type, UnusedT> {};
public:
    PoolVector(PoolBase *pool)
        : _begin(NULL)
        , _end(NULL)
        , _bufferEnd(NULL)
        , _pool(pool)
    {
        assert(dynamic_cast<RecyclePool *>(pool) == nullptr);
    }
    PoolVector(PoolBase* pool, size_t n, const T& element)
        : _begin(NULL)
        , _end(NULL)
        , _bufferEnd(NULL)
        , _pool(pool)
    {
        assert(dynamic_cast<RecyclePool *>(pool) == nullptr);
        insert(begin(), n, element);
    }

    ~PoolVector() = default;
public:
    PoolVector(const PoolVector &other) {
        _begin = NULL;
        _end = NULL;
        _bufferEnd = NULL;
        _pool = other._pool;
        *this = other;
    }
    PoolVector(PoolVector &&other)
        : PoolVector(NULL)
    {
        swap(other);
    }
public:
    void push_back(const T &element) {
        if (unlikely(_end == _bufferEnd)) {
            grow();
        }
        construct(*_end++, element, _Has_trivial_destructor());
    }
    void pop_back() {
        destroy(*(--_end), _Has_trivial_destructor());
    }

    bool empty() const { return _end <= _begin; }

    void resize(size_t newSize, const T &defaultValue = T()) {
        if (newSize < size()) {
            erase(begin() + newSize, end());
        } else {
            insert(end(), newSize - size(), defaultValue);
        }
    }

    void reserve(size_t newSize) {
        if (capacity() < newSize) {
            size_t oldSize = size();
            pointer buffer = allocateAndCopy(newSize, begin(), end());
            destroy_all(begin(), end(), _Has_trivial_destructor());
            _pool->deallocate(_begin, sizeof(T) * oldSize);
            _begin = buffer;
            _end = buffer + oldSize;
            _bufferEnd = buffer + newSize;
        }
    }
    iterator erase(iterator position) {
        if (unlikely(position == end())) {
            return position;
        }
        if (position + 1 != end()) {
            std::copy(position + 1, end(), position);
        }
        destroy(*(--_end), _Has_trivial_destructor());
        return position;
    }

    iterator erase(iterator first, iterator last) {
        iterator it(std::copy(last, end(), first));
        destroy_all(it, end(), _Has_trivial_destructor());
        _end -= last - first;
        return first;
    }

    iterator insert(iterator position, const T &value) {
        size_t offset = position - begin();
        if (unlikely(_end == _bufferEnd)) {
            grow();
            position = begin() + offset;
        }
        if (_end != _bufferEnd && position.base() == _end) {
            construct(*_end++, value, _Has_trivial_destructor());
        } else {
            construct(*_end, *(_end - 1), _Has_trivial_destructor());
            ++_end;
            std::copy_backward(position,
                    iterator(_end - 2),
                    iterator(_end - 1));
            *position = value;
        }
        return begin() + offset;
    }

    void insert(iterator position, size_t n, const T &value) {
        if (unlikely(n == 0)) {
            return;
        }
        if ((size_t)(_bufferEnd - _end) >= n) {
            size_t elemsAfter = end() - position;
            iterator oldEnd = end();
            if (elemsAfter > n) {
                uninitCopy(end() - n, end(), end());
                _end += n;
                std::copy_backward(position, oldEnd - n, oldEnd);
                std::fill(position, position + n, value);
            } else {
                uninitFill(_end, n - elemsAfter, value);
                _end += n - elemsAfter;
                uninitCopy(position, oldEnd, end());
                _end += elemsAfter;
                std::fill(position, oldEnd, value);
            }
        } else {
            size_t oldSize = size();
            size_t len = oldSize + std::max(oldSize, n);
            iterator newBegin((pointer)_pool->allocate(sizeof(T) * len));
            iterator newEnd(newBegin);
            newEnd = uninitCopy(begin(), position, newBegin);
            uninitFill(newEnd, n, value);
            newEnd += n;
            newEnd = uninitCopy(position, end(), newEnd);
            destroy_all(_begin, _end, _Has_trivial_destructor());
            _pool->deallocate(_begin, sizeof(T) * capacity());
            _begin = newBegin.base();
            _end = newEnd.base();
            _bufferEnd = newBegin.base() + len;
        }
    }
    /*
     * range [_begin, _end], insert [begin, end] to position.
     * if need new buffer, copy [_begin, position] [begin, end] [end, _end] to newEnd.
     * if no new buffer, no init copy [_end, _end + (end-begin)], copy other.
     */
    template <typename iterator_type>
    void insert(iterator position, iterator_type first, iterator_type last) {
        if (unlikely(first == last)) {
            return;
        }
        size_t n = std::distance(first, last);
        if (size_t(_bufferEnd - _end) >= n) {
            size_t elemsAfter = size_t(end() - position);
            iterator oldEnd = end();
            if (elemsAfter > n) {
                uninitCopy(end() - n, end(), _end);
                _end += n;
                std::copy_backward(position, oldEnd - n, oldEnd);
                std::copy(first, last, position);
            } else {
                iterator_type mid = first;
                std::advance(mid, elemsAfter);
                uninitCopy(mid, last, end());
                _end += n - elemsAfter;
                uninitCopy(position, oldEnd, end());
                _end += elemsAfter;
                std::copy(first, last, position);
            }
        } else {
            size_t oldSize = size();
            size_t len = oldSize + std::max(oldSize, n);
            iterator newBegin((pointer)_pool->allocate(sizeof(T) * len));
            iterator newEnd(newBegin);
            newEnd = uninitCopy(begin(), position, newEnd);
            newEnd = uninitCopy(first, last, newEnd);
            newEnd = uninitCopy(position, end(), newEnd);
            destroy_all(_begin, _end, _Has_trivial_destructor());
            _pool->deallocate(_begin, sizeof(T) * capacity());
            _begin = newBegin.base();
            _end = newEnd.base();
            _bufferEnd = newBegin.base() + len;
        }
    }

    // void assign(size_t n, const T &defaultValue = T());
    // void assign(iterator begin, iterator end);

    iterator begin() { return iterator(_begin); }
    iterator end() { return iterator(_end); }

    const_iterator begin() const { return const_iterator(_begin); }
    const_iterator end() const { return const_iterator(_end); }

    reverse_iterator rbegin() { return reverse_iterator(end()); }
    reverse_iterator rend() { return reverse_iterator(begin()); }

    const_reverse_iterator rbegin() const { return const_reverse_iterator(end()); }
    const_reverse_iterator rend() const { return const_reverse_iterator(begin()); }

    void swap(PoolVector &other) {
        std::swap(_begin, other._begin);
        std::swap(_end, other._end);
        std::swap(_bufferEnd, other._bufferEnd);
        std::swap(_pool, other._pool);
    }

    void steal(PoolVector &other) {
        other._begin = this->_begin;
        other._end = this->_end;
        other._bufferEnd = this->_bufferEnd;
        other._pool = this->_pool;
        this->_begin = NULL;
        this->_end = NULL;
        this->_bufferEnd = NULL;
    }

    void clear(){
        destroy_all(begin(), end(), _Has_trivial_destructor());
        _end = _begin;
    }

    const T &operator[](size_t idx) const { return _begin[idx]; }
    T &operator[](size_t idx) { return _begin[idx]; }

    const T &back() const { return *(_end - 1); }
    T &back() { return *(_end - 1); }

    const T &front() const { return *_begin; }
    T &front() { return *_begin; }

    PoolVector &operator=(const PoolVector &other) {
        if (&other != this) {
            size_t len = other.size();
            if (len > capacity()) {
                pointer newBuffer = allocateAndCopy(len, other.begin(), other.end());
                destroy_all(begin(), end(), _Has_trivial_destructor());
                _pool->deallocate(_begin, sizeof(T) * capacity());
                _begin = newBuffer;
                _bufferEnd = newBuffer + len;
            } else if (size() >= len) {
                iterator copyEndPoint(std::copy(other.begin(), other.end(), begin()));
                destroy_all(copyEndPoint, end(), _Has_trivial_destructor());
            } else {
                std::copy(other.begin(), other.begin() + size(), begin());
                uninitCopy(other.begin() + size(), other.end(), _begin + size());
            }
            _end = _begin + len;
        }
        return *this;
    }

    PoolVector &operator=(PoolVector &&other) {
        if (&other != this) {
            destroy_all(begin(), end(), _Has_trivial_destructor());
            _pool->deallocate(_begin, sizeof(T) * capacity());
            _begin = _end = _bufferEnd = NULL;
            _pool = NULL;
            swap(other);
        }
        return *this;
    }

    size_t size() const { return _end - _begin; }
    size_t capacity() const { return _bufferEnd - _begin; }
    const  pointer data() const{ return _begin;}
    pointer data() { return _begin;}
private:
    void grow() {
        size_t oldCapacity = capacity();
        size_t newCapacity = oldCapacity != 0 ? oldCapacity * 2 : INIT_ELEMENT_COUNT;
        pointer newElements = allocateAndCopy(newCapacity, begin(), end());
        destroy_all(begin(), end(), _Has_trivial_destructor());
        _pool->deallocate(_begin, sizeof(T) * oldCapacity);
        size_t curSize = size();
        _begin = newElements;
        _end = newElements + curSize;
        _bufferEnd = newElements + newCapacity;
    }

    pointer allocateAndCopy(size_t size, const_iterator begin, const_iterator end) {
        pointer newBuffer = (pointer)_pool->allocate(sizeof(T) * size);
        pointer iterBuffer = newBuffer;
        for (const_iterator it = begin; it != end; ++it) {
            construct(*(iterBuffer++), *it, _Has_trivial_destructor());
        }
        return newBuffer;
    }

    template<typename iter_type, typename result_iterator_type>
    result_iterator_type uninitCopy(iter_type begin,
                         iter_type end,
                         result_iterator_type result)
    {
        iter_type it = begin;
        for (; it != end; ++it) {
            construct(*(result++), *it, _Has_trivial_destructor());
        }
        return result;
    }

    template<typename iter_type>
    void uninitFill(iter_type begin,
                    size_t n,
                    const T &value)
    {
        while(n-- > 0) {
            construct(*begin++, value, _Has_trivial_destructor());
        }
    }

    template<typename iter_type>
    void destroy_all(iter_type begin, iter_type end, std::__true_type) {}

    template<typename iter_type>
    void destroy_all(iter_type begin, iter_type end, std::__false_type) {
        for (iter_type it = begin; it != end; ++it) {
            destroy<T>(*it, std::__false_type());
        }
    }
private:
    pointer _begin;
    pointer _end;
    pointer _bufferEnd;
    PoolBase *_pool;
private:
    friend class PoolVectorTest;
private:
    // attention: make this as last object
    Dtor<typename std::is_trivially_destructible<T>::type> _dtor;
};

template <typename T>
inline bool operator==(const PoolVector<T> &x, const PoolVector<T> &y) {
    return (x.size() == y.size())
        && std::equal(x.begin(), x.end(), y.begin());
}

template <typename T>
inline bool operator!=(const PoolVector<T> &x, const PoolVector<T> &y) {
    return !(x == y);
}

template <typename T>
inline bool operator<(const PoolVector<T> &x, const PoolVector<T> &y) {
    return std::lexicographical_compare(x.begin(), x.end(),
            y.begin(), y.end());
}

template <typename T>
inline bool operator>(const PoolVector<T> &x, const PoolVector<T> &y) {
    return y < x;
}

template <typename T>
inline bool operator<=(const PoolVector<T> &x, const PoolVector<T> &y) {
    return !(y < x);
}

template <typename T>
inline bool operator>=(const PoolVector<T> &x, const PoolVector<T> &y) {
    return !(x < y);
}

template <typename T>
inline void swap(PoolVector<T> &x, PoolVector<T> &y) {
    x.swap(y);
}

}
}

