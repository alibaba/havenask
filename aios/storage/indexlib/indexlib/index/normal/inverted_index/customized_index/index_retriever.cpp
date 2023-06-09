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
#include "indexlib/index/normal/inverted_index/customized_index/index_retriever.h"

#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"

using namespace std;
using namespace autil::mem_pool;

using namespace indexlib::common;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, IndexRetriever);

IndexRetriever::IndexRetriever() {}

IndexRetriever::~IndexRetriever() {}

bool IndexRetriever::Init(const DeletionMapReaderPtr& deletionMapReader,
                          const vector<IndexSegmentRetrieverPtr>& retrievers, const IndexerResourcePtr& resource)
{
    mDeletionMapReader = deletionMapReader;
    copy(retrievers.begin(), retrievers.end(), back_inserter(mSegRetrievers));
    mIndexerResource = resource;
    return true;
}

index::Result<std::vector<SegmentMatchInfo>> IndexRetriever::Search(const Term& term, Pool* pool)
{
    vector<SegmentMatchInfo> segMatchInfos;
    segMatchInfos.reserve(mSegRetrievers.size());
    for (auto segRetriever : mSegRetrievers) {
        MatchInfo matchInfo = segRetriever->Search(term.GetWord(), pool);
        SegmentMatchInfo segMatchInfo;
        segMatchInfo.baseDocId = segRetriever->GetIndexerSegmentData().baseDocId;
        segMatchInfo.matchInfo.reset(new MatchInfo(move(matchInfo)));
        segMatchInfos.push_back(segMatchInfo);
    }
    return segMatchInfos;
}

void IndexRetriever::SearchAsync(const index::Term& term, autil::mem_pool::Pool* pool,
                                 function<void(bool, vector<SegmentMatchInfo>)> done)
{
    vector<SegmentMatchInfo> segMatchInfos = Search(term, pool).Value();
    done(true, segMatchInfos);
}
}} // namespace indexlib::index
