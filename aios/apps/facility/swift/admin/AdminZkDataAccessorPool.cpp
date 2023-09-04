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
#include "swift/admin/AdminZkDataAccessorPool.h"

#include <algorithm>
#include <iosfwd>
#include <memory>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, AdminZkDataAccessorPool);

AdminZkDataAccessorPool::AdminZkDataAccessorPool(const std::string &zkPath) : _zkPath(zkPath) {}

AdminZkDataAccessorPool::~AdminZkDataAccessorPool() { _accessorPool.clear(); }

bool AdminZkDataAccessorPool::init(uint32_t count) {
    for (uint32_t i = 0; i < count; i++) {
        auto accessor = std::make_shared<AdminZkDataAccessor>();
        if (!accessor->init(_zkPath)) {
            return false;
        }
        _accessorPool.push_back(accessor);
    }
    return true;
}

AdminZkDataAccessorPtr AdminZkDataAccessorPool::getOrCreate() {
    {
        ScopedLock lock(_mutex);
        if (_accessorPool.size() > 0) {
            auto accessor = _accessorPool.back();
            _accessorPool.pop_back();
            return accessor;
        }
    }
    auto accessor = make_shared<AdminZkDataAccessor>();
    if (!accessor->init(_zkPath)) {
        return nullptr;
    } else {
        return accessor;
    }
}

void AdminZkDataAccessorPool::put(const AdminZkDataAccessorPtr &accessor) {
    ScopedLock lock(_mutex);
    auto zkWrapper = accessor->getZkWrapper();
    if (!zkWrapper->isConnected()) {
        if (!zkWrapper->open()) {
            AUTIL_LOG(WARN, "reopen zk wrapper failed, retry")
        }
    }
    _accessorPool.push_back(accessor);
}

} // namespace admin
} // end namespace swift
