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
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/admin/AdminZkDataAccessor.h"
#include "swift/common/Common.h"

namespace swift {
namespace admin {

class AdminZkDataAccessorPool {
public:
    AdminZkDataAccessorPool(const std::string &zkPath);
    ~AdminZkDataAccessorPool();

private:
    AdminZkDataAccessorPool(const AdminZkDataAccessorPool &);
    AdminZkDataAccessorPool &operator=(const AdminZkDataAccessorPool &);

public:
    bool init(uint32_t count);
    AdminZkDataAccessorPtr getOrCreate();
    void put(const AdminZkDataAccessorPtr &accessor);

private:
    autil::ThreadMutex _mutex;
    std::string _zkPath;
    std::vector<AdminZkDataAccessorPtr> _accessorPool;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(AdminZkDataAccessorPool);

} // namespace admin
} // end namespace swift
