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

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/isearch.h"
#include "ha3/search/SortExpression.h"

namespace autil {
class DataBuffer;

namespace mem_pool {
template <typename T> class PoolVector;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace rank {
class ComboComparator;
class HitCollectorBase;
}  // namespace rank
}  // namespace isearch
namespace matchdoc {
class MatchDoc;
class ReferenceBase;
}  // namespace matchdoc

namespace isearch {
namespace search {

class CacheMinScoreFilter
{
private:
    typedef std::pair<matchdoc::ReferenceBase *, bool> ScoreInfo;
public:
    CacheMinScoreFilter();
    CacheMinScoreFilter(const std::vector<score_t> & minScores);
    ~CacheMinScoreFilter();
public:
    void storeMinScore(rank::HitCollectorBase *rankHitCollector,
                       const SortExpressionVector &firstExpressions);
    void filterByMinScore(rank::HitCollectorBase *rankHitCollector,
                          const SortExpressionVector &firstExpressions,
                          autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs,
                          size_t expectCount = 0) const;
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

public:
    // for test
    score_t getMinScore(uint32_t idx) {
        if (idx >= _scores.size()) {
            return 0;
        }
        return _scores[idx];
    }
    void addMinScore(score_t minScore) {
        _scores.push_back(minScore);
    }
    std::vector<score_t> &getMinScoreVec() {
        return _scores;
    }
private:
    bool isFilter(const std::vector<ScoreInfo> &scoreInfos,
                  matchdoc::MatchDoc matchDoc) const;
    void doFilterByMinScore(
            const std::vector<ScoreInfo> &infos,
            autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs,
            std::vector<matchdoc::MatchDoc> &matchDocsVec) const;
    void selectExtraMatchDocs(
            size_t selectCount, const rank::ComboComparator *rankComp,
            std::vector<matchdoc::MatchDoc> &filteredMatchDocs,
            autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs) const;
    void releaseFilteredMatchDocs(
            rank::HitCollectorBase *rankHitCollector,
            std::vector<matchdoc::MatchDoc> &filteredMatchDocs) const;
    const rank::ComboComparator* getComparator(
            rank::HitCollectorBase *rankHitCollector) const;
private:
    static score_t getMinScore(rank::HitCollectorBase *hitCollector,
                               bool isScored, SortExpression *expr);
    static score_t defaultScoreMin(bool isAsc);
    static score_t getRankScore(matchdoc::ReferenceBase *rankScoreRef,
                                matchdoc::MatchDoc matchDoc) ;
private:
    std::vector<score_t> _scores;
private:
    friend class CacheMinScoreFilterTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CacheMinScoreFilter> CacheMinScoreFilterPtr;

} // namespace search
} // namespace isearch

