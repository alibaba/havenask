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
#include <string>
#include <unordered_map>
#include <utility>

#include "autil/Lock.h"

namespace autil {

extern const std::string VIRTUAL_ATTRIBUTES;
extern const std::string FILTER_SYNTAX_EXPR;

class SharedObjectBase {
public:
    virtual ~SharedObjectBase() = default;
};

enum DeleteLevel {
    None = 0,
    Delete,
    PoolDelete
};

class SharedObjectMap {
public:
    SharedObjectMap();
    ~SharedObjectMap();

private:
    SharedObjectMap(const SharedObjectMap &);
    SharedObjectMap &operator=(const SharedObjectMap &);

public:
    bool setWithoutDelete(const std::string &name, void *object);
    bool setWithDelete(const std::string &name, SharedObjectBase *object, DeleteLevel level);
    template <typename T>
    bool get(const std::string &name, T *&object) const {
        ScopedReadLock lock(_lock);
        auto it = _objectMap.find(name);
        if (it == _objectMap.end()) {
            return false;
        }
        object = static_cast<T *>(it->second.first);
        return true;
    }
    void remove(const std::string &name);
    void reset();
    size_t size() const;

private:
    std::unordered_map<std::string, std::pair<void *, DeleteLevel>> _objectMap;
    mutable ReadWriteLock _lock;
};
} // namespace autil
