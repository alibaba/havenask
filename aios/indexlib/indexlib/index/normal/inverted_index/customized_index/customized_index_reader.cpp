#include "indexlib/index/normal/inverted_index/customized_index/customized_index_reader.h"
#include "indexlib/index/normal/inverted_index/customized_index/match_info_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_util.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/misc/exception.h"
#include "indexlib/common/term.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(plugin);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, CustomizedIndexReader);

CustomizedIndexReader::CustomizedIndexReader() 
{
}

CustomizedIndexReader::~CustomizedIndexReader() 
{
}

void CustomizedIndexReader::Open(const IndexConfigPtr& indexConfig,
                                 const PartitionDataPtr& partitionData)
{
    mIndexConfig = indexConfig;
    const auto& pluginManager = partitionData->GetPluginManager();
    vector<IndexSegmentRetrieverPtr> segRetrievers;
    vector<docid_t> baseDocIds;

    PartitionSegmentIteratorPtr iter = partitionData->CreateSegmentIterator();
    assert(iter);
    while (iter->IsValid())
    {
        IndexSegmentRetrieverPtr segRetriever;
        docid_t baseDocId = INVALID_DOCID;
        if (iter->GetType() == SIT_BUILT)
        {
            SegmentData segmentData = iter->GetSegmentData();
            baseDocId = segmentData.GetBaseDocId();
            segRetriever = LoadBuiltSegmentRetriever(segmentData, indexConfig, pluginManager);
        }
        else
        {
            assert(iter->GetType() == SIT_BUILDING);
            baseDocId = iter->GetBaseDocId();
            segRetriever = LoadBuildingSegmentRetriever(iter->GetInMemSegment(), indexConfig);
        }

        if (segRetriever)
        {
            segRetrievers.push_back(segRetriever);
            baseDocIds.push_back(baseDocId);
        }
        iter->MoveToNext();
    }
    mIndexRetriever.reset(IndexPluginUtil::CreateIndexRetriever(indexConfig, pluginManager));
    if (!mIndexRetriever)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "create IndexRetriever failed"); 
    }

    IndexPluginResourcePtr pluginResource =
        DYNAMIC_POINTER_CAST(IndexPluginResource,
                             pluginManager->GetPluginResource());
    if (!pluginResource)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "empty index plugin resource!");
    }
    IndexerResourcePtr resource(new IndexerResource(pluginResource->schema,
                                                    pluginResource->options,
                                                    pluginResource->counterMap,
                                                    pluginResource->partitionMeta,
                                                    indexConfig->GetIndexName(),
                                                    pluginResource->pluginPath,
                                                    pluginResource->metricProvider,
                                                    IndexerUserDataPtr()));

    if (!mIndexRetriever->Init(partitionData->GetDeletionMapReader(),
                               segRetrievers, baseDocIds, resource))
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "IndexRetriever init failed");
    }
}

PostingIterator* CustomizedIndexReader::Lookup(
        const Term& term, uint32_t statePoolSize, 
        PostingType type, Pool *pool)
{
    vector<SegmentMatchInfo> segMatchInfos = mIndexRetriever->Search(term, pool);
    return CreatePostingIterator(segMatchInfos, pool);
}

future_lite::Future<PostingIterator*> CustomizedIndexReader::LookupAsync(
        const common::Term* term, uint32_t statePoolSize,
        PostingType type, autil::mem_pool::Pool* pool)
{
    std::shared_ptr<future_lite::Promise<PostingIterator*> > promisePtr(new future_lite::Promise<PostingIterator*>());
    if (mIndexRetriever->SearchAsync(*term, pool,
                                    [promisePtr, pool, this](std::vector<SegmentMatchInfo> segMatchInfos) {
                                        PostingIterator *itr = this->CreatePostingIterator(segMatchInfos, pool);
                                        promisePtr->setValue(itr);
                                    })
    ) {
        return promisePtr->getFuture();
    } else {
        return future_lite::makeReadyFuture<PostingIterator*>(nullptr);
    }
}


PostingIterator* CustomizedIndexReader::CreatePostingIterator(
        const vector<SegmentMatchInfo>& segMatchInfos,
        Pool* sessionPool)
{
    PostingIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(
            sessionPool, MatchInfoPostingIterator,
            segMatchInfos, sessionPool);
    return iter;
}

IndexSegmentRetrieverPtr CustomizedIndexReader::LoadBuiltSegmentRetriever(
        const SegmentData& segmentData, const IndexConfigPtr& indexConfig,
        const PluginManagerPtr& pluginManager)
{
    if (segmentData.GetSegmentInfo().docCount == 0)
    {
        return IndexSegmentRetrieverPtr();
    }

    const string& indexName = indexConfig->GetIndexName();
    DirectoryPtr indexDir = segmentData.GetIndexDirectory(indexName, false);
    if (!indexDir)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "index dir [%s] not found in segment [%d]",
                             indexName.c_str(), segmentData.GetSegmentId());
    }
    IndexSegmentRetrieverPtr segRetriever(IndexPluginUtil::CreateIndexSegmentRetriever(
                    indexConfig, pluginManager));
    if (!segRetriever)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "create IndexSegmentRetriever failed");
    }
    if (!segRetriever->Open(indexDir)) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "IndexSegmentRetriever open "
                             "failed in segment [%d]", segmentData.GetSegmentId());
    }
    return segRetriever;
}

IndexSegmentRetrieverPtr CustomizedIndexReader::LoadBuildingSegmentRetriever(
        const InMemorySegmentPtr& inMemSegment, const IndexConfigPtr& indexConfig)
{
    if (!inMemSegment)
    {
        return IndexSegmentRetrieverPtr();
    }
    InMemorySegmentReaderPtr reader = inMemSegment->GetSegmentReader();
    if (!reader)
    {
        return IndexSegmentRetrieverPtr();
    }
    
    const string& indexName = indexConfig->GetIndexName();
    CustomizedIndexSegmentReaderPtr segReader = DYNAMIC_POINTER_CAST(
            CustomizedIndexSegmentReader, reader->GetSingleIndexSegmentReader(indexName));
    if (!segReader)
    {
        return IndexSegmentRetrieverPtr();
    }
    return segReader->GetSegmentRetriever();
}

IE_NAMESPACE_END(index);

