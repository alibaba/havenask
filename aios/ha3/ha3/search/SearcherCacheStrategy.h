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
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {
class MultiErrorResult;
class Request;

} // namespace common
} // namespace isearch

namespace isearch {
namespace search {

class CacheResult;

class SearcherCacheStrategy
{
public:
    SearcherCacheStrategy() {};
    virtual ~SearcherCacheStrategy() {};
private:
    SearcherCacheStrategy(const SearcherCacheStrategy &);
    SearcherCacheStrategy& operator=(const SearcherCacheStrategy &);
public:
    virtual bool init(const common::Request *request, 
                      common::MultiErrorResult *errorResult) = 0;
    virtual bool genKey(const common::Request *request, uint64_t &key) = 0;
    virtual bool beforePut(uint32_t curTime, 
                           CacheResult *cacheResult) = 0;
    virtual uint32_t filterCacheResult(CacheResult *cacheResult) = 0;
    virtual uint32_t getCacheDocNum(const common::Request *request, 
                                    uint32_t docFound) = 0;
};

typedef std::shared_ptr<SearcherCacheStrategy> SearcherCacheStrategyPtr;

} // namespace search
} // namespace isearch

