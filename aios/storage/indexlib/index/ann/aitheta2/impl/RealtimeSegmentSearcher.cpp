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
#include "indexlib/index/ann/aitheta2/impl/RealtimeSegmentSearcher.h"

#include <unordered_map>
using namespace std;
using namespace indexlib::index;
namespace indexlibv2::index::ann {

bool RealtimeSegmentSearcher::Init(const SegmentPtr& segment, docid_t segmentBaseDocId,
                                   const std::shared_ptr<AithetaFilterCreatorBase>& creator)
{
    _segment = dynamic_pointer_cast<RealtimeSegment>(segment);
    ANN_CHECK(_segment != nullptr, "cast to RealtimeSegment failed");
    _segmentBaseDocId = segmentBaseDocId;
    _creator = creator;
    InitSearchMetrics();
    return true;
}

bool RealtimeSegmentSearcher::DoSearch(const AithetaQueries& indexQuery, const std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo,
                                       ResultHolder& resultHolder)
{
    for (const auto& query : indexQuery.aithetaqueries()) {
        auto searcher = GetRealtimeIndexSearcher(query.indexid());
        if (searcher != nullptr) {
            ANN_CHECK(searcher->Search(query, searchInfo, resultHolder), "search failed");
        }
    }
    return true;
}

RealtimeIndexSearcherPtr RealtimeSegmentSearcher::GetRealtimeIndexSearcher(index_id_t indexId)
{
    {
        autil::ScopedReadLock _(_readWriteLock);
        auto iterator = _indexSearcherMap.find(indexId);
        if (iterator != _indexSearcherMap.end()) {
            return iterator->second;
        }
    }

    AiThetaStreamerPtr streamer;
    bool suc = _segment->GetRealtimeIndex(indexId, streamer, false);
    if (!suc) {
        AUTIL_LOG(DEBUG, "find realtime index[%ld] failed", indexId);
        return nullptr;
    }

    auto indexSearcher = std::make_shared<RealtimeIndexSearcher>(_indexConfig, streamer);
    indexSearcher->SetAithetaFilterCreator(_creator);
    indexSearcher->SetSegmentBaseDocId(_segmentBaseDocId);
    if (!indexSearcher->Init()) {
        AUTIL_LOG(ERROR, "searcher init failed");
        return nullptr;
    }

    autil::ScopedWriteLock _(_readWriteLock);
    _indexSearcherMap.emplace(indexId, indexSearcher);
    return indexSearcher;
}

AUTIL_LOG_SETUP(indexlib.index, RealtimeSegmentSearcher);
} // namespace indexlibv2::index::ann