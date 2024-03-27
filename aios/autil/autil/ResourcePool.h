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

#include <atomic>
#include <functional>
#include <memory>
#include <stack>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"

namespace autil {

template <typename T>
class ResourcePool {
public:
    ResourcePool(
        const size_t &size = 1,
        const std::function<T *()> &creator = []() { return new T(); },
        const std::function<void(T *)> &destroyer = [](T *t) { delete t; })
        : _size(size), _destroyer(destroyer) {
        for (size_t i = 0; i < size; ++i) {
            _pool.push(creator());
        }
    }

    ~ResourcePool() {
        ScopedLock lock(_lock);
        while (_pool.size() < _size) {
            AUTIL_LOG(INFO, "current size: %zu, expected: %zu, wait for return...", _pool.size(), _size);
            _lock.wait();
        }
        while (!_pool.empty()) {
            T *t = _pool.top();
            _pool.pop();
            _destroyer(t);
        }
    };

public:
    size_t count() const {
        ScopedLock lock(_lock);
        return _pool.size();
    }

    std::shared_ptr<T> getShared(bool blocking = false) { return get<std::shared_ptr<T>>(blocking); }

    std::unique_ptr<T, std::function<void(T *)>> getUnique(bool blocking = false) {
        return get<std::unique_ptr<T, std::function<void(T *)>>>(blocking);
    }

private:
    template <typename ptr_type>
    ptr_type get(bool blocking = false) {
        ScopedLock lock(_lock);
        if (_pool.empty()) {
            if (blocking) {
                while (_pool.empty()) {
                    _lock.wait();
                }
            } else {
                return nullptr;
            }
        }
        T *t = _pool.top();
        _pool.pop();
        return ptr_type(t, [this](T *t) {
            ScopedLock lock(_lock);
            _pool.push(t);
            _lock.signal();
        });
    }

private:
    size_t _size;
    std::function<T *()> _creator;
    std::function<void(T *)> _destroyer;
    std::stack<T *> _pool;
    mutable ThreadCond _lock;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
alog::Logger *ResourcePool<T>::_logger = alog::Logger::getLogger("autil.ResourcePool");

} // namespace autil
