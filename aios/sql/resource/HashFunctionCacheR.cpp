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
#include "sql/resource/HashFunctionCacheR.h"

#include <iosfwd>
#include <stdint.h>
#include <utility>

#include "autil/TimeUtility.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;
using namespace autil;

namespace sql {
AUTIL_LOG_SETUP(sql, HashFunctionCacheR);

const std::string HashFunctionCacheR::RESOURCE_ID = "HashFunctionCacheR";
HashFunctionCacheR::HashFunctionCacheR() {}
HashFunctionCacheR::~HashFunctionCacheR() {}

void HashFunctionCacheR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool HashFunctionCacheR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode HashFunctionCacheR::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

autil::HashFunctionBasePtr HashFunctionCacheR::getHashFunc(uint64_t key) {
    ScopedReadLock lock(_rwLock);
    auto iter = _hashFuncMap.find(key);
    if (iter != _hashFuncMap.end()) {
        return iter->second;
    } else {
        return autil::HashFunctionBasePtr();
    }
}

void HashFunctionCacheR::putHashFunc(uint64_t key, autil::HashFunctionBasePtr hashFunc) {
    ScopedWriteLock lock(_rwLock);
    _hashFuncMap[key] = hashFunc;
}

REGISTER_RESOURCE(HashFunctionCacheR);

} // namespace sql
