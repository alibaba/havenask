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

#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_map>

#include "autil/HashFunctionBase.h"
#include "autil/Lock.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"

namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

class HashFunctionCacheR : public navi::Resource {
public:
    HashFunctionCacheR();
    ~HashFunctionCacheR();
    HashFunctionCacheR(const HashFunctionCacheR &) = delete;
    HashFunctionCacheR &operator=(const HashFunctionCacheR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    autil::HashFunctionBasePtr getHashFunc(uint64_t key);
    void putHashFunc(uint64_t key, autil::HashFunctionBasePtr hashFunc);

public:
    static const std::string RESOURCE_ID;

private:
    std::unordered_map<uint64_t, autil::HashFunctionBasePtr> _hashFuncMap;
    autil::ReadWriteLock _rwLock;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<HashFunctionCacheR> HashFunctionCacheRPtr;

} // namespace sql
