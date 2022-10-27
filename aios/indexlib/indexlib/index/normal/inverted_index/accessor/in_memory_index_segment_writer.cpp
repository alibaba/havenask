#include "indexlib/index/normal/inverted_index/accessor/in_memory_index_segment_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_multi_sharding_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_writer.h"
#include "indexlib/index/normal/primarykey/primary_key_index_writer.h"
#include "indexlib/index/dump_item_typed.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemoryIndexSegmentWriter);

InMemoryIndexSegmentWriter::InMemoryIndexSegmentWriter() 
{
}

InMemoryIndexSegmentWriter::~InMemoryIndexSegmentWriter() 
{
}

void InMemoryIndexSegmentWriter::Init(
        const IndexPartitionSchemaPtr& schema,        
        const IndexPartitionOptions& options,
        const SegmentMetricsPtr& lastSegmentMetrics,
        util::BuildResourceMetrics* buildResourceMetrics,
        const plugin::PluginManagerPtr& pluginManager,
        const PartitionSegmentIteratorPtr& segIter)
{
    mSegmentMetrics = lastSegmentMetrics;
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    IndexWriterVec indexWriters;
    IndexWriterListVec fieldToIndexWriters;
    fieldToIndexWriters.resize(schema->GetFieldSchema()->GetFieldCount());
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto indexIter = indexConfigs->Begin();
    for (; indexIter != indexConfigs->End(); indexIter++)
    {
        const IndexConfigPtr& indexConfig = *indexIter;
        if(IsTruncateIndex(indexConfig))
        {
            continue;
        }
        if (indexConfig->GetShardingType() == IndexConfig::IST_IS_SHARDING)
        {
            continue;
        }
        IndexWriterPtr indexWriter = CreateIndexWriter(
                indexConfig, options, buildResourceMetrics,
                pluginManager, segIter);
        indexWriters.push_back(indexWriter);

        if (indexConfig == indexSchema->GetPrimaryKeyIndexConfig())
        {
            mPrimaryKeyIndexWriter = DYNAMIC_POINTER_CAST(
                    PrimaryKeyIndexWriter, indexWriter);
        }

        IndexConfig::Iterator iter = indexConfig->CreateIterator();
        while (iter.HasNext())
        {
            const FieldConfigPtr& fieldConfig = iter.Next();
            IndexWriterListPtr& indexWriterList = 
                fieldToIndexWriters[fieldConfig->GetFieldId()];
            if (indexWriterList.get() == NULL)
            {
                indexWriterList.reset(new (nothrow) IndexWriterList);
            }
            indexWriterList->push_back(indexWriter);
        }
    }
    mIndexWriters.swap(indexWriters);
    mFieldToIndexWriters.swap(fieldToIndexWriters);
}

double InMemoryIndexSegmentWriter::GetIndexPoolToDumpFileRatio()
{
    if (mPrimaryKeyIndexWriter && 
        mPrimaryKeyIndexWriter->GetIndexConfig()->GetIndexType() == it_trie)
    {
        return TRIE_INDEX_BYTE_SLICE_POOL_DUMP_FILE_RATIO;
    }
    return DEFAULT_INDEX_BYTE_SLICE_POOL_DUMP_FILE_RATIO;
    
}

bool InMemoryIndexSegmentWriter::IsPrimaryKeyStrValid(const std::string& str) const
{
    if (mPrimaryKeyIndexWriter)
    {
        return mPrimaryKeyIndexWriter->CheckPrimaryKeyStr(str);
    }
    return true;
}

size_t InMemoryIndexSegmentWriter::EstimateInitMemUse(
    const SegmentMetricsPtr& metrics,
    const config::IndexPartitionSchemaPtr& schema,
    const config::IndexPartitionOptions& options,
    const plugin::PluginManagerPtr& pluginManager,
    const index_base::PartitionSegmentIteratorPtr& segIter)
{
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    size_t initMemUse = 0;
    for (; iter != indexConfigs->End(); iter++)
    {
        IndexConfigPtr indexConfig = *iter;
        if(IsTruncateIndex(indexConfig))
        {
            continue;
        }
        if (indexConfig->GetShardingType() == IndexConfig::IST_IS_SHARDING)
        {
            continue;
        }
        initMemUse += IndexWriterFactory::EstimateIndexWriterInitMemUse(
                indexConfig, options, pluginManager, metrics, segIter);
    }
    return initMemUse;
}

bool InMemoryIndexSegmentWriter::AddDocument(const NormalDocumentPtr& doc)
{
    assert(doc);
    const IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    assert(indexDoc);
    IndexDocument::FieldVector::const_iterator iter = indexDoc->GetFieldBegin();
    IndexDocument::FieldVector::const_iterator iterEnd = indexDoc->GetFieldEnd();

    for (; iter != iterEnd; ++iter)
    {
        const Field* field = *iter;
        if (!field)
        {
            continue;
        }
        fieldid_t fieldId = field->GetFieldId();

        if (fieldId >= (fieldid_t)mFieldToIndexWriters.size() || fieldId < 0)
            continue;

        IndexWriterListPtr& indexWriterList = mFieldToIndexWriters[fieldId];
        if (indexWriterList.get() == NULL)
            continue;

        IndexWriterList::const_iterator itIndex = indexWriterList->begin();
        for (; itIndex != indexWriterList->end(); ++itIndex)
        {
            const IndexWriterPtr& writer = *itIndex;
            writer->AddField(field);
        }
    }
    EndDocument(indexDoc);
    return true;
}

bool InMemoryIndexSegmentWriter::IsTruncateIndex(const IndexConfigPtr& indexConfig)
{
    return !indexConfig->GetNonTruncateIndexName().empty();
}

IndexWriterPtr InMemoryIndexSegmentWriter::CreateIndexWriter(
        const IndexConfigPtr& indexConfig,
        const IndexPartitionOptions& options,
        util::BuildResourceMetrics* buildResourceMetrics,
        const plugin::PluginManagerPtr& pluginManager,
        const index_base::PartitionSegmentIteratorPtr& segIter) const
{
    IndexWriterPtr indexWriter(IndexWriterFactory::CreateIndexWriter(
                    indexConfig, mSegmentMetrics, options,
                    buildResourceMetrics, pluginManager, segIter));
    assert(indexWriter);
    return indexWriter;
}

void InMemoryIndexSegmentWriter::EndDocument(const IndexDocumentPtr& indexDocument)
{
    for (IndexWriterVec::const_iterator it = mIndexWriters.begin();
         it != mIndexWriters.end(); ++it)
    {
        (*it)->EndDocument(*indexDocument);
    }
}

void InMemoryIndexSegmentWriter::EndSegment()
{
    for (IndexWriterVec::const_iterator it = mIndexWriters.begin();
         it != mIndexWriters.end(); ++it)
    {
        (*it)->EndSegment();
    }
}

void InMemoryIndexSegmentWriter::CollectSegmentMetrics()
{
    for (IndexWriterVec::const_iterator it = mIndexWriters.begin();
         it != mIndexWriters.end(); ++it)
    {
        (*it)->FillDistinctTermCount(mSegmentMetrics);
    }
}

void InMemoryIndexSegmentWriter::CreateDumpItems(const DirectoryPtr& directory, 
        vector<common::DumpItem*>& dumpItems)
{
    for (IndexWriterVec::iterator it = mIndexWriters.begin();
         it != mIndexWriters.end(); ++it)
    {
        if ((*it)->GetIndexConfig()->GetShardingType() == IndexConfig::IST_NEED_SHARDING)
        {
            MultiShardingIndexWriterPtr shardingIndexWriter = 
                DYNAMIC_POINTER_CAST(MultiShardingIndexWriter, *it);
            assert(shardingIndexWriter);
            vector<common::DumpItem*> shardingDumpItems;
            shardingIndexWriter->CreateDumpItems(directory, shardingDumpItems);
            dumpItems.insert(dumpItems.end(), 
                    shardingDumpItems.begin(), shardingDumpItems.end());
        }
        else
        {
            dumpItems.push_back(new IndexDumpItem(directory, *it));
        }
    }
}

MultiFieldIndexSegmentReaderPtr InMemoryIndexSegmentWriter::CreateInMemSegmentReader() const
{
    MultiFieldIndexSegmentReaderPtr indexSegmentReader(new MultiFieldIndexSegmentReader);
    for (uint32_t i = 0; i < mIndexWriters.size(); ++i)
    {
        IndexConfigPtr indexConfig = mIndexWriters[i]->GetIndexConfig();
        IndexSegmentReaderPtr indexSegReader = mIndexWriters[i]->CreateInMemReader();
        if (indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING)
        {
            InMemMultiShardingIndexSegmentReaderPtr multiShardingSegReader = 
                DYNAMIC_POINTER_CAST(InMemMultiShardingIndexSegmentReader, indexSegReader);
            assert(multiShardingSegReader);
            indexSegmentReader->AddMultiShardingSegmentReader(
                    indexConfig->GetIndexName(), multiShardingSegReader);
        }
        else
        {
            indexSegmentReader->AddIndexSegmentReader(
                    indexConfig->GetIndexName(), indexSegReader);
        }
    }
    return indexSegmentReader;
}

void InMemoryIndexSegmentWriter::GetDumpEstimateFactor(
        priority_queue<double>& factors,
        priority_queue<size_t>& minMemUses) const
{
    for (size_t i = 0; i < mIndexWriters.size(); ++i)
    {
        mIndexWriters[i]->GetDumpEstimateFactor(factors, minMemUses);
    }
}

IndexSegmentReaderPtr InMemoryIndexSegmentWriter::GetInMemPrimaryKeyReader() const
{
    if (mPrimaryKeyIndexWriter)
    {
        return mPrimaryKeyIndexWriter->CreateInMemReader();
    }
    return IndexSegmentReaderPtr();
}

IE_NAMESPACE_END(index);

