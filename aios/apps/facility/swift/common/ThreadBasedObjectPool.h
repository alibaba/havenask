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

#include <functional>
#include <pthread.h>
#include <stddef.h>
#include <unordered_map>

#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/Log.h"

namespace swift {
namespace common {
template <typename T>
class ThreadBasedObjectPool {
public:
    typedef std::unordered_map<pthread_t, T *> ObjectMap;
    typedef std::function<T *()> ObjectCreateFunc;

public:
    ThreadBasedObjectPool(ObjectCreateFunc func = NULL) { _objectCreateFunc = func; }
    ~ThreadBasedObjectPool() {
        autil::ScopedWriteLock lock(_lock);
        typename ObjectMap::iterator it = _objectMap.begin();
        for (; it != _objectMap.end(); ++it) {
            delete it->second;
        }
        _objectMap.clear();
    }

private:
    ThreadBasedObjectPool(const ThreadBasedObjectPool &);
    ThreadBasedObjectPool &operator=(const ThreadBasedObjectPool &);

public:
    T *getObject() {
        pthread_t tid = pthread_self();
        {
            autil::ScopedReadLock lock(_lock);
            typename ObjectMap::iterator it = _objectMap.find(tid);
            if (likely(_objectMap.end() != it)) {
                return it->second;
            }
        }
        T *object = NULL;
        if (_objectCreateFunc) {
            object = _objectCreateFunc();
        } else {
            object = new T();
        }
        autil::ScopedWriteLock lock(_lock);
        _objectMap[tid] = object;
        return object;
    }

public:
    size_t getBuilderCount() { return _objectMap.size(); }

private:
    autil::ReadWriteLock _lock;
    ObjectMap _objectMap;
    ObjectCreateFunc _objectCreateFunc;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace swift
