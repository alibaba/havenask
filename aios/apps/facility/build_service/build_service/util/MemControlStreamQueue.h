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
#ifndef ISEARCH_BS_MEMCONTROLSTREAMQUEUE_H
#define ISEARCH_BS_MEMCONTROLSTREAMQUEUE_H

#include <deque>

#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace util {

template <typename T>
class MemControlStreamQueue
{
private:
    static const uint32_t DEFAULT_WAIT_TIME = 1000000; // 1 second
    struct Slot {
        T data;
        size_t size = 0;
        Slot(T&& item, size_t sz) : data(std::forward<T>(item)), size(sz) {}
        Slot(T item, size_t sz) : data(std::forward<T>(item)), size(sz) {}
    };

public:
    static const uint32_t DEFAULT_QUEUE_SIZE = 1000;
    static const size_t DEFAULT_MEMORY_LIMIT = std::numeric_limits<size_t>::max();

public:
    MemControlStreamQueue(uint32_t queueSize = DEFAULT_QUEUE_SIZE, size_t memoryLimit = DEFAULT_MEMORY_LIMIT)
        : _capacity(queueSize)
        , _memoryLimit(memoryLimit)
        , _itemCount(0)
        , _usedMemory(0)
        , _finish(false)
    {
    }
    ~MemControlStreamQueue() {}

private:
    MemControlStreamQueue(const MemControlStreamQueue&);
    MemControlStreamQueue& operator=(const MemControlStreamQueue&);

public:
    template <typename C>
    void push(C&& item, size_t itemSize)
    {
        autil::ScopedLock lk(_cond);
        while (_itemCount.load(std::memory_order_relaxed) >= _capacity.load(std::memory_order_relaxed) ||
               _usedMemory.load(std::memory_order_relaxed) + itemSize > _memoryLimit.load(std::memory_order_relaxed)) {
            _cond.wait(DEF_WAIT_TIME);
        }
        _slots.emplace_back(std::forward<C>(item), itemSize);
        _itemCount.fetch_add(1, std::memory_order_relaxed);
        _usedMemory.fetch_add(itemSize, std::memory_order_relaxed);
        _cond.signal();
    }
    bool pop(T& item)
    {
        while (true) {
            if (_finish && empty()) {
                return false;
            }
            if (tryGetAndPopFront(item)) {
                return true;
            }
            waitNotEmpty();
        }
    }

    // Attention not thread safe
    bool top(T& item)
    {
        while (true) {
            if (_finish && empty()) {
                return false;
            }

            if (!empty()) {
                item = getFront();
                return true;
            }
            waitNotEmpty();
        }
    }

    size_t size() { return _itemCount.load(std::memory_order_acquire); }
    size_t capacity() { return _capacity.load(std::memory_order_acquire); }
    size_t memoryUse() { return _usedMemory.load(std::memory_order_acquire); }

    bool empty()
    {
        autil::ScopedLock lk(_cond);
        return _itemCount.load(std::memory_order_relaxed) == 0;
    }

    void setFinish()
    {
        _finish = true;
        autil::ScopedLock lk(_cond);
        _cond.broadcast();
    }

private:
    bool tryGetAndPopFront(T& item)
    {
        autil::ScopedLock lk(_cond);
        if (_itemCount == 0) {
            return false;
        } else {
            auto& slot = _slots.front();
            item = std::move(slot.data);
            _itemCount.fetch_sub(1, std::memory_order_relaxed);
            _usedMemory.fetch_sub(slot.size, std::memory_order_relaxed);
            _slots.pop_front();
            _cond.signal();
            return true;
        }
    }

    void waitNotEmpty()
    {
        autil::ScopedLock lk(_cond);
        _cond.wait(DEFAULT_WAIT_TIME);
    }

    T getFront()
    {
        autil::ScopedLock lk(_cond);
        const T& item = _slots.front().data;
        return item;
    }

private:
    std::atomic<size_t> _capacity;
    std::atomic<size_t> _memoryLimit;
    std::atomic<size_t> _itemCount;
    std::atomic<size_t> _usedMemory;
    std::deque<Slot> _slots;
    autil::ThreadCond _cond;

    bool _finish;

    static const uint32_t DEF_WAIT_TIME = 1000000; // 1 second
private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::util

#endif // ISEARCH_BS_STREAMQUEUE_H
