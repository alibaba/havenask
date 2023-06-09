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
#include <vector>

#include "matchdoc/MatchDoc.h"

#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Result.h"
#include "ha3/search/DocCountLimits.h"

namespace isearch {
namespace search {
class HitCollectorManager;
struct InnerSearchResult;
}  // namespace search
}  // namespace isearch

namespace isearch {
namespace search {

class MatchDocSearchStrategy
{
public:
    MatchDocSearchStrategy(const DocCountLimits &docCountLimits)
        : _docCountLimits(docCountLimits) { }
    virtual ~MatchDocSearchStrategy() { }
private:
    MatchDocSearchStrategy(const MatchDocSearchStrategy &);
    MatchDocSearchStrategy& operator=(const MatchDocSearchStrategy &);
public:
    virtual void afterSeek(HitCollectorManager * ) { }
    virtual void afterStealFromCollector(InnerSearchResult&, HitCollectorManager * ) { }
    virtual void afterRerank(InnerSearchResult& ) { }
    virtual void afterExtraRank(InnerSearchResult& ) { }
    virtual common::Result* reconstructResult(common::Result *result) {
        return result;
    }
    virtual uint32_t getExtraRankCount() {
        return _docCountLimits.requiredTopK;
    }
    virtual uint32_t getResultCount() {
        return _docCountLimits.requiredTopK;
    }
public:
    template <typename T>
    static void releaseRedundantMatchDocs(T &matchDocVect,
            uint32_t requiredTopK, const common::Ha3MatchDocAllocatorPtr &matchDocAllocator)
    {
        uint32_t size = (uint32_t)matchDocVect.size();
        while(size > requiredTopK)
        {
            auto matchDoc = matchDocVect[--size];
            matchDocAllocator->deallocate(matchDoc);
        }
        matchDocVect.erase(matchDocVect.begin() + size, matchDocVect.end());
    }
protected:
    static void truncateResult(uint32_t requireTopK,
                               common::Result *result) {
        common::MatchDocs *matchDocs = result->getMatchDocs();
        std::vector<matchdoc::MatchDoc> &matchDocVect =
            matchDocs->getMatchDocsVect();
        const auto &matchDocAllocator = matchDocs->getMatchDocAllocator();
        releaseRedundantMatchDocs(matchDocVect,
                requireTopK, matchDocAllocator);
    }
protected:
    const DocCountLimits& _docCountLimits;
private:
    friend class MatchDocSearchStrategyTest;
};

} // namespace search
} // namespace isearch

