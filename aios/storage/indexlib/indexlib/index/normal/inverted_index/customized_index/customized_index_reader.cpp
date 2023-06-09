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
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_reader.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/normal/framework/multi_field_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_util.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_user_data.h"
#include "indexlib/index/normal/inverted_index/customized_index/match_info_posting_iterator.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::plugin;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, CustomizedIndexReader);

CustomizedIndexReader::CustomizedIndexReader() {}

CustomizedIndexReader::~CustomizedIndexReader() {}

void CustomizedIndexReader::Open(const IndexConfigPtr& indexConfig, const PartitionDataPtr& partitionData,
                                 const InvertedIndexReader* hintReader)
{
    _indexConfig = indexConfig;
    const auto& pluginManager = partitionData->GetPluginManager();
    vector<IndexSegmentRetrieverPtr> segRetrievers;

    PartitionSegmentIteratorPtr iter = partitionData->CreateSegmentIterator();
    assert(iter);
    while (iter->IsValid()) {
        IndexSegmentRetrieverPtr segRetriever;
        SegmentData segmentData = iter->GetSegmentData();
        if (iter->GetType() == SIT_BUILT) {
            segRetriever = LoadBuiltSegmentRetriever(segmentData, indexConfig, pluginManager);
        } else {
            assert(iter->GetType() == SIT_BUILDING);
            segRetriever = LoadBuildingSegmentRetriever(segmentData, iter->GetInMemSegment());
        }

        if (segRetriever) {
            segRetrievers.push_back(segRetriever);
        }
        iter->MoveToNext();
    }
    mIndexRetriever.reset(IndexPluginUtil::CreateIndexRetriever(indexConfig, pluginManager));
    if (!mIndexRetriever) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "create IndexRetriever failed");
    }

    IndexPluginResourcePtr pluginResource =
        DYNAMIC_POINTER_CAST(IndexPluginResource, pluginManager->GetPluginResource());
    if (!pluginResource) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "empty index plugin resource!");
    }
    IndexerResourcePtr resource(new IndexerResource(
        pluginResource->schema, pluginResource->options, pluginResource->counterMap, pluginResource->partitionMeta,
        indexConfig->GetIndexName(), pluginResource->pluginPath, pluginResource->metricProvider, IndexerUserDataPtr()));

    if (!mIndexRetriever->Init(partitionData->GetDeletionMapReader(), segRetrievers, resource)) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "IndexRetriever init failed");
    }
}

index::Result<PostingIterator*> CustomizedIndexReader::Lookup(const Term& term, uint32_t statePoolSize,
                                                              PostingType type, Pool* pool)
{
    index::Result<vector<SegmentMatchInfo>> searchResult = mIndexRetriever->Search(term, pool);
    if (searchResult.Ok()) {
        return CreatePostingIterator(searchResult.Value(), pool);
    } else {
        return searchResult.GetErrorCode();
    }
}

future_lite::coro::Lazy<index::Result<PostingIterator*>>
CustomizedIndexReader::LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type,
                                   autil::mem_pool::Pool* pool, file_system::ReadOption option) noexcept
{
    struct Awaiter {
        Awaiter(IndexRetrieverPtr retriever, const index::Term* term, autil::mem_pool::Pool* pool,
                CustomizedIndexReader* indexReader)
            : mIndexRetriever(retriever)
            , mTerm(term)
            , mPool(pool)
            , mIter(nullptr)
            , mIndexReader(indexReader)
        {
        }
        bool await_ready() { return false; }
        void await_suspend(std::coroutine_handle<> continuation) noexcept
        {
            mIndexRetriever->SearchAsync(
                *mTerm, mPool, [continuation, this](bool success, std::vector<SegmentMatchInfo> segMatchInfos) mutable {
                    if (success) {
                        mIter = mIndexReader->CreatePostingIterator(segMatchInfos, mPool);
                    }
                    continuation.resume();
                });
        }
        PostingIterator* await_resume() noexcept { return mIter; }

    private:
        IndexRetrieverPtr mIndexRetriever;
        const index::Term* mTerm;
        autil::mem_pool::Pool* mPool;
        PostingIterator* mIter;
        CustomizedIndexReader* mIndexReader;
    };

    co_return co_await Awaiter(mIndexRetriever, term, pool, this);
}

PostingIterator* CustomizedIndexReader::CreatePostingIterator(const vector<SegmentMatchInfo>& segMatchInfos,
                                                              Pool* sessionPool)
{
    PostingIterator* iter =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, MatchInfoPostingIterator, segMatchInfos, sessionPool);
    return iter;
}

IndexSegmentRetrieverPtr CustomizedIndexReader::LoadBuiltSegmentRetriever(const SegmentData& segmentData,
                                                                          const IndexConfigPtr& indexConfig,
                                                                          const PluginManagerPtr& pluginManager)
{
    if (segmentData.GetSegmentInfo()->docCount == 0) {
        return IndexSegmentRetrieverPtr();
    }

    const string& indexName = indexConfig->GetIndexName();
    DirectoryPtr indexDir = segmentData.GetIndexDirectory(indexName, false);
    if (!indexDir) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "index dir [%s] not found in segment [%d]", indexName.c_str(),
                             segmentData.GetSegmentId());
    }

    auto segmentInfo = segmentData.GetSegmentInfo();
    if (!segmentInfo) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "segment info not set in segment [%d]", segmentData.GetSegmentId());
    }

    IndexerSegmentData indexerSegData;
    indexerSegData.indexDir = indexDir;
    indexerSegData.segInfo = *segmentInfo;
    indexerSegData.segId = segmentData.GetSegmentId();
    indexerSegData.baseDocId = segmentData.GetBaseDocId();
    indexerSegData.isBuildingSegment = false;

    IndexSegmentRetrieverPtr segRetriever(IndexPluginUtil::CreateIndexSegmentRetriever(indexConfig, pluginManager));
    if (!segRetriever) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "create IndexSegmentRetriever failed");
    }
    if (!segRetriever->Open(indexerSegData)) {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "IndexSegmentRetriever open "
                             "failed in segment [%d]",
                             segmentData.GetSegmentId());
    }
    return segRetriever;
}

IndexSegmentRetrieverPtr CustomizedIndexReader::LoadBuildingSegmentRetriever(const SegmentData& segmentData,
                                                                             const InMemorySegmentPtr& inMemSegment)
{
    if (!inMemSegment) {
        return IndexSegmentRetrieverPtr();
    }
    InMemorySegmentReaderPtr reader = inMemSegment->GetSegmentReader();
    if (!reader) {
        return IndexSegmentRetrieverPtr();
    }

    const auto& indexSegReader = reader->GetMultiFieldIndexSegmentReader()->GetIndexSegmentReader(GetIndexName());
    auto segReader = DYNAMIC_POINTER_CAST(CustomizedIndexSegmentReader, indexSegReader);
    if (!segReader) {
        return IndexSegmentRetrieverPtr();
    }

    auto segRetriever = segReader->GetSegmentRetriever();
    auto inMemSegRetriever = DYNAMIC_POINTER_CAST(InMemSegmentRetriever, segRetriever);
    if (!inMemSegRetriever) {
        return IndexSegmentRetrieverPtr();
    }

    auto segmentInfo = segmentData.GetSegmentInfo();
    if (!segmentInfo) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "segment info not set in realtime segment [%d]",
                             segmentData.GetSegmentId());
    }

    IndexerSegmentData indexerSegData;
    indexerSegData.segInfo = *segmentInfo;
    indexerSegData.segId = segmentData.GetSegmentId();
    indexerSegData.baseDocId = segmentData.GetBaseDocId();
    indexerSegData.isBuildingSegment = true;
    inMemSegRetriever->SetIndexerSegmentData(indexerSegData);
    return segRetriever;
}
}} // namespace indexlib::index
