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

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace matchdoc {
class MatchDoc;
class MatchDocAllocator;
} // namespace matchdoc

namespace isearch {
namespace search {
class QueryExecutor;

class MatchDataCollectorBase {
public:
    MatchDataCollectorBase() = default;
    MatchDataCollectorBase(matchdoc::MatchDocAllocator *matchDocAllocator,
                           autil::mem_pool::Pool *pool)
        : _matchDocAllocator(matchDocAllocator)
        , _pool(pool) {}
    virtual ~MatchDataCollectorBase() = default;
    void setMatchDocAllocator(matchdoc::MatchDocAllocator *matchDocAllocator) {
        _matchDocAllocator = matchDocAllocator;
    }
    void setPool(autil::mem_pool::Pool *pool) {
        _pool = pool;
    }

public:
    virtual void collect(search::QueryExecutor *executor, const matchdoc::MatchDoc &matchDoc) = 0;

protected:
    matchdoc::MatchDocAllocator *_matchDocAllocator = nullptr;
    autil::mem_pool::Pool *_pool = nullptr;
};

} // namespace search
} // namespace isearch
