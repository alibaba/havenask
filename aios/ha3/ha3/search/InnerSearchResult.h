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

#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/AggregateResult.h"
#include "ha3/common/Ha3MatchDocAllocator.h"

namespace matchdoc {
class MatchDoc;
}  // namespace matchdoc

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace isearch {
namespace search {

struct InnerSearchResult{
    InnerSearchResult(autil::mem_pool::Pool *pool) 
        : matchDocVec(pool) 
        , totalMatchDocs(0)
        , actualMatchDocs(0)
        , extraRankCount(0)
    {
    }
    autil::mem_pool::PoolVector<matchdoc::MatchDoc> matchDocVec;
    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr;
    uint32_t totalMatchDocs;
    uint32_t actualMatchDocs;
    uint32_t extraRankCount;
    common::AggregateResultsPtr aggResultsPtr;
};

} // namespace search
} // namespace isearch

