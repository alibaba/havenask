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
#include "suez/turing/expression/cava/common/CavaJitPool.h"

#include <algorithm>
#include <assert.h>

#include "cava/common/Common.h"

using namespace cava;
namespace suez {
namespace turing {

CavaJitPool::CavaJitPool() {}

CavaJitPool::~CavaJitPool() {
    for (auto *cavaJitPoolItem : _poolItemVec) {
        delete cavaJitPoolItem;
    }
    _poolItemVec.clear();
}

void CavaJitPool::initAdd(CavaJitPoolItem *item) {
    assert(item != nullptr);
    put(item);
}

CavaJitPoolItemPtr CavaJitPool::get() {
    CavaJitPoolItem *item = nullptr;
    {
        autil::ScopedSpinLock scope(_lock);
        if (!_poolItemVec.empty()) {
            item = _poolItemVec.back();
            _poolItemVec.pop_back();
        }
    }
    assert(item);
    return CavaJitPoolItemPtr(item, [this](CavaJitPoolItem *item) { this->put(item); });
}

void CavaJitPool::put(CavaJitPoolItem *item) {
    autil::ScopedSpinLock scope(_lock);
    _poolItemVec.push_back(item);
}
} // namespace turing
} // namespace suez
