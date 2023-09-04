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
#include "indexlib/index/ann/aitheta2/impl/NormalSegmentSearcher.h"
using namespace std;
using namespace indexlib::index;
namespace indexlibv2::index::ann {

bool NormalSegmentSearcher::Init(const SegmentPtr& segment, docid_t segmentBaseDocId,
                                 const std::shared_ptr<AithetaFilterCreatorBase>& creator)
{
    auto normalSegment = dynamic_pointer_cast<NormalSegment>(segment);
    ANN_CHECK(normalSegment != nullptr, "cast to NormalSegment failed");
    _segmentBaseDocId = segmentBaseDocId;
    _creator = creator;
    InitSearchMetrics();

    auto segDataReader = normalSegment->GetSegmentDataReader();
    auto& indexMetaMap = normalSegment->GetSegmentMeta().GetIndexMetaMap();
    auto indexContextHolder = make_shared<AiThetaContextHolder>();
    for (auto& [indexId, indexMeta] : indexMetaMap) {
        auto indexDataReader = segDataReader->GetIndexDataReader(indexId);
        ANN_CHECK(indexDataReader, "create index data reader[%ld] failed", indexId);
        NormalIndexSearcherPtr searcher =
            make_shared<NormalIndexSearcher>(indexMeta, _indexConfig, indexDataReader, indexContextHolder);
        searcher->SetAithetaFilterCreator(_creator);
        searcher->SetSegmentBaseDocId(_segmentBaseDocId);

        ANN_CHECK(searcher->Init(), "init index searcher[%ld] failed", indexId);
        _indexSearcherMap.emplace(indexId, searcher);
    }
    return true;
}

bool NormalSegmentSearcher::DoSearch(const AithetaQueries& query,
                                     const std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo,
                                     ResultHolder& resultHolder)
{
    for (const auto& aithetaQuery : query.aithetaqueries()) {
        int64_t indexId = aithetaQuery.indexid();
        auto iterator = _indexSearcherMap.find(indexId);
        if (iterator == _indexSearcherMap.end()) {
            AUTIL_LOG(DEBUG, "index[%ld] not exist", indexId);
            continue;
        }
        auto& indexSearcher = iterator->second;
        ANN_CHECK(indexSearcher->Search(aithetaQuery, searchInfo, resultHolder), "index search failed");
    }
    return true;
}

AUTIL_LOG_SETUP(indexlib.index, NormalSegmentSearcher);
} // namespace indexlibv2::index::ann