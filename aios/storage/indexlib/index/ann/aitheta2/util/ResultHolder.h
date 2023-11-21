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

#include "autil/Log.h"
#include "indexlib/index/ann/Common.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"

namespace indexlibv2::index::ann {

class ResultHolder
{
public:
    struct SearchStats {
        // doc count for docs filtered by deletion map or customized filter
        size_t filteredCount {0u};
        // doc count for docs do distance calculation
        size_t distCalcCount {0u};
    };

public:
    ResultHolder(const std::string& distanceType) { _isSmallerScoreBetter = IsSmallerScoreBetter(distanceType); }
    ~ResultHolder() = default;
    ResultHolder(const ResultHolder&) = delete;
    ResultHolder& operator=(const ResultHolder&) = delete;

public:
    void ResetBaseDocId(docid_t base) { _baseDocId = base; }
    void AppendResult(docid_t localDocId, match_score_t score);
    void AppendResult(docid_t localDocId, match_score_t score, float threshold);
    void MergeResult(const ResultHolder& holder);
    size_t GetResultSize() const { return _matchItems.size(); }
    const ResultHolder::SearchStats& GetResultStats() const { return _stats; }
    void MergeSearchStats(const ResultHolder::SearchStats& stats);
    const std::vector<ANNMatchItem>& GetTopkMatchItems(size_t topk);

protected:
    void OrderByDocId();
    void UniqAndOrderByDocId();
    void OrderByScore();
    void TruncateResult(size_t topk);

private:
    bool IsSmallerScoreBetter(const std::string& distanceType) const;

protected:
    docid_t _baseDocId {0};
    std::vector<ANNMatchItem> _matchItems {};
    SearchStats _stats {};
    bool _isSmallerScoreBetter {false};

private:
    AUTIL_LOG_DECLARE();
};

inline void ResultHolder::AppendResult(docid_t localDocId, match_score_t score)
{
    ANNMatchItem result = {_baseDocId + localDocId, score};
    _matchItems.emplace_back(result);
}

inline void ResultHolder::AppendResult(docid_t localDocId, match_score_t score, match_score_t threshold)
{
    if (_isSmallerScoreBetter && score >= threshold) {
        return;
    } else if (!_isSmallerScoreBetter && score <= threshold) {
        return;
    }
    AppendResult(localDocId, score);
}

inline void ResultHolder::TruncateResult(size_t topk)
{
    if (topk < _matchItems.size()) {
        _matchItems.resize(topk);
    }
}

inline void ResultHolder::OrderByScore()
{
    bool byAscendingOrder = _isSmallerScoreBetter ? true : false;
    if (byAscendingOrder) {
        std::sort(_matchItems.begin(), _matchItems.end(),
                  [](const ANNMatchItem& lhs, const ANNMatchItem& rhs) { return lhs.score < rhs.score; });
    } else {
        std::sort(_matchItems.begin(), _matchItems.end(),
                  [](const ANNMatchItem& lhs, const ANNMatchItem& rhs) { return lhs.score > rhs.score; });
    }
}

inline void ResultHolder::OrderByDocId()
{
    std::sort(_matchItems.begin(), _matchItems.end(),
              [](const ANNMatchItem& lhs, const ANNMatchItem& rhs) { return lhs.docid < rhs.docid; });
}

inline void ResultHolder::MergeResult(const ResultHolder& holder)
{
    _matchItems.insert(_matchItems.end(), holder._matchItems.begin(), holder._matchItems.end());
    MergeSearchStats(holder.GetResultStats());
}

inline void ResultHolder::MergeSearchStats(const ResultHolder::SearchStats& stats)
{
    _stats.filteredCount += stats.filteredCount;
    _stats.distCalcCount += stats.distCalcCount;
}

} // namespace indexlibv2::index::ann
