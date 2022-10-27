#include "indexlib/index/normal/inverted_index/customized_index/customized_index_writer.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_module_factory.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_util.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_segment_reader.h"
#include "indexlib/document/index_document/normal_document/index_raw_field.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/config/customized_index_config.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"

using namespace std;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, CustomizedIndexWriter);

void CustomizedIndexWriter::Init(const config::IndexConfigPtr& indexConfig,
                                 util::BuildResourceMetrics* buildResourceMetrics,
                                 const index_base::PartitionSegmentIteratorPtr& segIter)
{
    IndexWriter::Init(indexConfig, buildResourceMetrics, segIter);
    mIndexer.reset(IndexPluginUtil::CreateIndexer(indexConfig, mPluginManager));
    if (!mIndexer)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "create indexer failed");
    }

    IndexPluginResourcePtr pluginResource =
        DYNAMIC_POINTER_CAST(IndexPluginResource,
                             mPluginManager->GetPluginResource());
    if (!pluginResource)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "empty index plugin resource!");
    }

    IndexerUserDataPtr userData(IndexPluginUtil::CreateIndexerUserData(
                    indexConfig, mPluginManager));
    if (userData)
    {
        vector<IndexerSegmentData> indexerSegDatas;
        if (!GetIndexerSegmentDatas(indexConfig, segIter, indexerSegDatas))
        {
            IE_LOG(WARN, "GetIndexerSegmentData fail!");
            userData.reset();
        }
        if (userData && !userData->init(indexerSegDatas))
        {
            IE_LOG(ERROR, "init IndexerUserData fail!");
            userData.reset();
        }
    }
    IndexerResourcePtr resource(new IndexerResource(
                    pluginResource->schema,
                    pluginResource->options,
                    pluginResource->counterMap,
                    pluginResource->partitionMeta,
                    indexConfig->GetIndexName(),
                    pluginResource->pluginPath,
                    pluginResource->metricProvider,
                    userData));
    if (!mIndexer->Init(resource))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "init indexer failed"); 
    }
    mFieldVec.reserve(indexConfig->GetFieldCount());
    UpdateBuildResourceMetrics();    
}

size_t CustomizedIndexWriter::EstimateInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionSegmentIteratorPtr& segIter)
{
    size_t userDataInitSize = 0;
    IndexerUserDataPtr userData(IndexPluginUtil::CreateIndexerUserData(
                    indexConfig, mPluginManager));
    if (userData)
    {
        vector<IndexerSegmentData> indexerSegDatas;
        if (GetIndexerSegmentDatas(indexConfig, segIter, indexerSegDatas))
        {
            userDataInitSize = userData->estimateInitMemUse(indexerSegDatas);
        }
    }

    if (mIndexer)
    {
        return userDataInitSize + mIndexer->EstimateInitMemoryUse(mLastSegDistinctTermCount); 
    }
    IndexerPtr tempIndexer(IndexPluginUtil::CreateIndexer(indexConfig, mPluginManager));
    if (tempIndexer)
    {
        return userDataInitSize + tempIndexer->EstimateInitMemoryUse(mLastSegDistinctTermCount);
    }
    return userDataInitSize;
}

void CustomizedIndexWriter::AddField(const document::Field* field)
{
    mFieldVec.push_back(field);
}

void CustomizedIndexWriter::EndDocument(const document::IndexDocument& indexDocument)
{
    mIndexer->Build(mFieldVec, indexDocument.GetDocId());
    mFieldVec.clear();
    UpdateBuildResourceMetrics(); 
}

void CustomizedIndexWriter::EndSegment()
{
    UpdateBuildResourceMetrics();    
}

void CustomizedIndexWriter::Dump(const file_system::DirectoryPtr& indexDir,
                                 autil::mem_pool::PoolBase* dumpPool)
{
    IE_LOG(DEBUG, "Dumping index : [%s]", 
           mIndexConfig->GetIndexName().c_str());
    file_system::DirectoryPtr dumpDir = indexDir->MakeDirectory(
            mIndexConfig->GetIndexName());
    mIndexer->Dump(dumpDir);
    UpdateBuildResourceMetrics(); 
}

uint32_t CustomizedIndexWriter::GetDistinctTermCount() const
{
    if (!mIndexer)
    {
        return 0;
    }
    return mIndexer->GetDistinctTermCount();
}

IndexSegmentReaderPtr CustomizedIndexWriter::CreateInMemReader()
{
    if (!mIndexer)
    {
        return IndexSegmentReaderPtr();
    }

    InMemSegmentRetrieverPtr segRetriever = mIndexer->CreateInMemSegmentRetriever();
    if (!segRetriever)
    {
        return IndexSegmentReaderPtr();
    }
    CustomizedIndexSegmentReaderPtr inMemSegReader(new CustomizedIndexSegmentReader);
    if (!inMemSegReader->Init(mIndexConfig, segRetriever))
    {
        return IndexSegmentReaderPtr();
    }
    return inMemSegReader;
}

void CustomizedIndexWriter::UpdateBuildResourceMetrics()
{
    assert(mIndexer);
    assert(mBuildResourceMetricsNode);
    mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE,
            mIndexer->EstimateCurrentMemoryUse());
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE,
            mIndexer->EstimateTempMemoryUseInDump());
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE,
            mIndexer->EstimateExpandMemoryUseInDump());
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE,
            mIndexer->EstimateDumpFileSize()); 
}

bool CustomizedIndexWriter::GetIndexerSegmentDatas(
        const config::IndexConfigPtr& indexConfig,
        const PartitionSegmentIteratorPtr& partSegIter,
        vector<IndexerSegmentData> &indexerSegDatas)
{
    if (!partSegIter || !indexConfig)
    {
        return false;
    }
    SegmentIteratorPtr builtSegIter = partSegIter->CreateIterator(SIT_BUILT);
    assert(builtSegIter);
    while (builtSegIter->IsValid())
    {
        const SegmentData& segData = builtSegIter->GetSegmentData();
        IndexerSegmentData indexerSegData;
        indexerSegData.segInfo = segData.GetSegmentInfo();
        indexerSegData.isBuildingSegment = false;
        indexerSegData.segId = segData.GetSegmentId();
        indexerSegData.indexDir = segData.GetIndexDirectory(
                indexConfig->GetIndexName(), false);

        indexerSegDatas.push_back(indexerSegData);
        builtSegIter->MoveToNext();
    }
    // TODO: add building indexer segment data
    return true;
}

IE_NAMESPACE_END(index);

