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
#include <stddef.h>
#include <iosfwd>
#include <string>
#include <unordered_map>
#include <utility>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/SharedObjectMap.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SharedObjectMap);

namespace autil {

const std::string VIRTUAL_ATTRIBUTES = "__virtual_attributes__";
const std::string FILTER_SYNTAX_EXPR = "__filter_syntax_expr__";


SharedObjectMap::SharedObjectMap() {
}

SharedObjectMap::~SharedObjectMap() {
    reset();
}

bool SharedObjectMap::setWithoutDelete(const std::string &name, void *object) {
    ScopedWriteLock lock(_lock);
    auto it = _objectMap.find(name);
    if (it != _objectMap.end()) {
        return false;
    }
    _objectMap[name] = std::make_pair((void *)object, DeleteLevel::None);
    return true;
}

bool SharedObjectMap::setWithDelete(const std::string &name, SharedObjectBase *object, DeleteLevel level) {
    ScopedWriteLock lock(_lock);
    auto it = _objectMap.find(name);
    if (it != _objectMap.end()) {
        return false;
    }
    if (level == DeleteLevel::None) {
        return false;
    }
    _objectMap[name] = std::make_pair((void *)object, level);
    return true;
}

static void deleteObj(DeleteLevel level, void *ptr) {
    if (level == DeleteLevel::None) {
        return;
    }
    SharedObjectBase *obj = reinterpret_cast<SharedObjectBase *>(ptr);
    if (level == DeleteLevel::Delete) {
        delete obj;
    } else if (level == DeleteLevel::PoolDelete) {
        POOL_DELETE_CLASS(obj);
    }
}

void SharedObjectMap::remove(const std::string &name) {
    ScopedWriteLock lock(_lock);
    auto it = _objectMap.find(name);
    if (it == _objectMap.end()) {
        return;
    }
    deleteObj(it->second.second, it->second.first);
    _objectMap.erase(it);
}

void SharedObjectMap::reset() {
    for (auto it : _objectMap) {
        deleteObj(it.second.second, it.second.first);
    }
    _objectMap.clear();
}

size_t SharedObjectMap::size() const {
    ScopedReadLock lock(_lock);
    return _objectMap.size();
}

}
