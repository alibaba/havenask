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

#include <assert.h>
#include <deque>
#include <iterator>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "autil/Log.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/ObjectBlock.h"

namespace swift {
namespace util {
template <typename T>
class MessageQueueIterator;

template <typename T>
class MessageQueue {
public:
    MessageQueue(util::BlockPool *pool) {
        _pool = pool;
        _remainCount = 0;
        _begin = 0;
        _size = 0;
        _countInBlock = _pool->getBlockSize() / sizeof(T);
    }
    ~MessageQueue() { clear(); }

private:
    MessageQueue(const MessageQueue &);
    MessageQueue &operator=(const MessageQueue &);

public:
    bool reserve(int64_t count = 1) {
        if (_remainCount >= count) {
            return true;
        } else {
            int32_t needCount = count - _remainCount;
            while (needCount > 0) {
                util::BlockPtr block = _pool->allocate();
                if (block == NULL) {
                    return false;
                }
                util::ObjectBlock<T> *objectBlock = new util::ObjectBlock<T>(block);
                assert(_countInBlock == objectBlock->size());
                _objectBlocks.push_back(objectBlock);
                needCount -= _countInBlock;
                _remainCount += _countInBlock;
            }
            return true;
        }
    }
    void push_back(const T &memoryMessage) {
        assert(_remainCount > 0);
        int64_t blockOffset = (_begin + _size) / _countInBlock;
        int64_t offset = (_begin + _size) % _countInBlock;
        assert((int64_t)_objectBlocks.size() >= blockOffset);
        (*_objectBlocks[blockOffset])[offset] = memoryMessage;
        ++_size;
        --_remainCount;
    }
    const T &front() const {
        assert(!empty());
        return (*_objectBlocks[0])[_begin];
    }
    void pop_front() {
        assert(!empty());
        _objectBlocks[0]->destoryBefore(_begin);
        ++_begin;
        --_size;
        if (_begin >= _objectBlocks[0]->size()) {
            _begin = 0;
            util::ObjectBlock<T> *objs = _objectBlocks.front();
            _objectBlocks.pop_front();
            delete objs;
        }
    }
    const T &back() const {
        assert(!empty());
        return (*this)[size() - 1];
    }
    bool empty() const { return size() == 0; }
    const T &operator[](int64_t index) const {
        assert(index < _size && !empty());
        int64_t blockOffset = (_begin + index) / _countInBlock;
        int64_t offset = (_begin + index) % _countInBlock;
        assert((int64_t)_objectBlocks.size() >= blockOffset);
        return (*_objectBlocks[blockOffset])[offset];
    }
    int64_t size() const { return _size; }
    MessageQueueIterator<T> begin() const;
    MessageQueueIterator<T> end() const;
    void clear() {
        _remainCount = 0;
        _begin = 0;
        _size = 0;
        for (size_t i = 0; i < _objectBlocks.size(); i++) {
            delete _objectBlocks[i];
        }
        _objectBlocks.clear();
    }
    int64_t usedBlockCount() const { return _objectBlocks.size(); }
    int64_t remainMetaCount() const { return _remainCount; }
    int64_t msgCountInOneBlock() const { return _countInBlock; }
    void
    stealBlock(int64_t count, int64_t &startOffset, int64_t &msgCount, std::vector<util::ObjectBlock<T> *> &objectVec) {
        int64_t blockOffset = (_begin + count) / _countInBlock;
        for (int64_t i = 0; i < blockOffset && _objectBlocks.size() > 0; i++) {
            util::ObjectBlock<T> *objs = _objectBlocks.front();
            _objectBlocks.pop_front();
            objectVec.push_back(objs);
        }
        startOffset = _begin;
        msgCount = 0;
        if (objectVec.size() > 0) {
            msgCount = objectVec.size() * _countInBlock - _begin;
            if (msgCount > _size) {
                msgCount = _size;
            }
            _size = _size - msgCount;
            _begin = 0;
        }
    }

private:
    util::BlockPool *_pool;
    std::deque<util::ObjectBlock<T> *> _objectBlocks;
    int64_t _countInBlock;
    int64_t _remainCount;
    int64_t _begin;
    int64_t _size;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
class MessageQueueIterator : public std::iterator<std::forward_iterator_tag, T> {
public:
    MessageQueueIterator() : _offset(-1), _queue(NULL) {}
    MessageQueueIterator(const MessageQueue<T> *queue, int64_t offset) : _offset(offset), _queue(queue) {}
    MessageQueueIterator(const MessageQueueIterator &other) : _offset(other._offset), _queue(other._queue) {}
    MessageQueueIterator &operator++() {
        ++_offset;
        return *this;
    }
    MessageQueueIterator operator++(int) {
        MessageQueueIterator tmp(*this);
        ++_offset;
        return tmp;
    }

    MessageQueueIterator &operator+(int64_t step) {
        _offset += step;
        return *this;
    }

    bool operator==(const MessageQueueIterator &rhs) { return _offset == rhs._offset; }
    bool operator!=(const MessageQueueIterator &rhs) { return _offset != rhs._offset; }
    const T *operator->() { return &(*_queue)[_offset]; }
    const T &operator*() { return (*_queue)[_offset]; }

private:
    int64_t _offset;
    const MessageQueue<T> *_queue;
};

template <typename T>
MessageQueueIterator<T> MessageQueue<T>::begin() const {
    return MessageQueueIterator<T>(this, 0);
}

template <typename T>
MessageQueueIterator<T> MessageQueue<T>::end() const {
    return MessageQueueIterator<T>(this, size());
}

} // namespace util
} // namespace swift
