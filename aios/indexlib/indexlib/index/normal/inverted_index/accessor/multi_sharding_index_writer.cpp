#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer_factory.h"
#include "indexlib/index/normal/inverted_index/format/sharding_index_hasher.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_multi_sharding_index_segment_reader.h"
#include "indexlib/index/dump_item_typed.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiShardingIndexWriter);

MultiShardingIndexWriter::MultiShardingIndexWriter(
        const index_base::SegmentMetricsPtr& segmentMetrics,
        const IndexPartitionOptions& options)
    : mBasePos(0)
    , mSegmentMetrics(segmentMetrics)
    , mOptions(options)
{
}

MultiShardingIndexWriter::~MultiShardingIndexWriter() 
{
}

void MultiShardingIndexWriter::Init(const IndexConfigPtr& indexConfig,
                                    util::BuildResourceMetrics* buildResourceMetrics,
                                    const index_base::PartitionSegmentIteratorPtr& segIter)
{ 
    assert(indexConfig);
    if (indexConfig->GetShardingType() != IndexConfig::IST_NEED_SHARDING)
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "index [%s] should be need_sharding index",
                             indexConfig->GetIndexName().c_str());
    }

    const vector<IndexConfigPtr>& shardingIndexConfigs =
        indexConfig->GetShardingIndexConfigs();
    if (shardingIndexConfigs.empty())
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "index [%s] has none sharding indexConfigs",
                             indexConfig->GetIndexName().c_str());
    }

    IndexWriter::Init(indexConfig, buildResourceMetrics, segIter);
    for (size_t i = 0; i < shardingIndexConfigs.size(); ++i)
    {
        IndexWriterPtr indexWriter(
                IndexWriterFactory::CreateIndexWriter(shardingIndexConfigs[i],
                        mSegmentMetrics, mOptions,
                        buildResourceMetrics, plugin::PluginManagerPtr(), segIter));

        NormalIndexWriterPtr shardingIndexWriter = 
            DYNAMIC_POINTER_CAST(NormalIndexWriter, indexWriter);
        if (!shardingIndexWriter)
        {
            INDEXLIB_FATAL_ERROR(BadParameter,
                    "CreateShardingIndexWriter for sharding index [%s] failed!", 
                    shardingIndexConfigs[i]->GetIndexName().c_str());
        }
        mShardingWriters.push_back(shardingIndexWriter);
    }
    mSectionAttributeWriter = CreateSectionAttributeWriter(
            indexConfig, buildResourceMetrics);
    mShardingIndexHasher.reset(new ShardingIndexHasher);
    mShardingIndexHasher->Init(mIndexConfig);
}

size_t MultiShardingIndexWriter::EstimateInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionSegmentIteratorPtr& segIter)
{
    size_t initMemUse = 0;
    assert(indexConfig);
    if (indexConfig->GetShardingType() != IndexConfig::IST_NEED_SHARDING)
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "index [%s] should be need_sharding index",
                             indexConfig->GetIndexName().c_str());
    }

    const vector<IndexConfigPtr>& shardingIndexConfigs =
        indexConfig->GetShardingIndexConfigs();
    if (shardingIndexConfigs.empty())
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "index [%s] has none sharding indexConfigs",
                             indexConfig->GetIndexName().c_str());
    }

    for (size_t i = 0; i < shardingIndexConfigs.size(); ++i)
    {
        initMemUse += IndexWriterFactory::EstimateIndexWriterInitMemUse(
                shardingIndexConfigs[i], mOptions,
                plugin::PluginManagerPtr(), mSegmentMetrics, segIter);
    }
    return initMemUse;
}

void MultiShardingIndexWriter::AddField(const Field* field)
{
    const auto tokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
    if (!tokenizeField)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "fieldTag [%d] is not IndexTokenizeField",
                             static_cast<int8_t>(field->GetFieldTag()));
    }
    if (tokenizeField->GetSectionCount() <= 0)
    {
        return;
    }

    pos_t tokenBasePos = mBasePos;
    uint32_t fieldLen = 0;
    fieldid_t fieldId = tokenizeField->GetFieldId();

    for (auto iterField = tokenizeField->Begin(); 
         iterField != tokenizeField->End(); ++iterField)
    {
        const Section* section = *iterField;
        fieldLen += section->GetLength();

        for (size_t i = 0; i < section->GetTokenCount(); i++)
        {
            const Token* token = section->GetToken(i);
            tokenBasePos += token->GetPosIncrement();
            AddToken(token, fieldId, tokenBasePos);
        }
    }
    mBasePos += fieldLen;
}

void MultiShardingIndexWriter::EndDocument(const IndexDocument& indexDocument)
{ 
    for (size_t i = 0; i < mShardingWriters.size(); ++i)
    {
        mShardingWriters[i]->EndDocument(indexDocument);
    }

    if (mSectionAttributeWriter)
    {
	mSectionAttributeWriter->EndDocument(indexDocument);
    }
    mBasePos = 0;
}

void MultiShardingIndexWriter::EndSegment()
{ 
    for (size_t i = 0; i < mShardingWriters.size(); ++i)
    {
        mShardingWriters[i]->EndSegment();
    }
}    

void MultiShardingIndexWriter::Dump(
        const DirectoryPtr& dir,
        autil::mem_pool::PoolBase* dumpPool)
{ 
    for (size_t i = 0; i < mShardingWriters.size(); i++)
    {
        mShardingWriters[i]->Dump(dir, dumpPool);
    }

    if (mSectionAttributeWriter)
    {
        mSectionAttributeWriter->Dump(dir, dumpPool);
    }
}

void MultiShardingIndexWriter::CreateDumpItems(
        const DirectoryPtr& directory, vector<common::DumpItem*>& dumpItems)
{
    for (size_t i = 0; i < mShardingWriters.size(); ++i)
    {
        dumpItems.push_back(new IndexDumpItem(directory, mShardingWriters[i]));
    }
    
    if (mSectionAttributeWriter)
    {
        dumpItems.push_back(new SectionAttributeDumpItem(
                        directory, mSectionAttributeWriter));
    }
}

void MultiShardingIndexWriter::FillDistinctTermCount(SegmentMetricsPtr mSegmentMetrics)
{
    for (size_t i = 0; i < mShardingWriters.size(); ++i)
    {
        mShardingWriters[i]->FillDistinctTermCount(mSegmentMetrics);
    }
}

uint32_t MultiShardingIndexWriter::GetDistinctTermCount() const
{
    assert(false);
    return 0;
}

uint64_t MultiShardingIndexWriter::GetNormalTermDfSum() const
{ 
    uint64_t termDfSum = 0;
    for (size_t i = 0; i < mShardingWriters.size(); ++i)
    {
        termDfSum += mShardingWriters[i]->GetNormalTermDfSum();
    }
    return termDfSum;
}

IndexSegmentReaderPtr MultiShardingIndexWriter::CreateInMemReader()
{ 
    typedef InMemMultiShardingIndexSegmentReader::InMemShardingSegReader InMemShardingSegReader;
    vector<InMemShardingSegReader> segReaders;
    for (size_t i = 0; i < mShardingWriters.size(); ++i)
    {
        const string& indexName = 
            mShardingWriters[i]->GetIndexConfig()->GetIndexName();
        segReaders.push_back(make_pair(indexName, 
                        mShardingWriters[i]->CreateInMemReader()));
    }

    AttributeSegmentReaderPtr sectionReader;
    if (mSectionAttributeWriter)
    {
        sectionReader = mSectionAttributeWriter->CreateInMemReader();
    }

    InMemMultiShardingIndexSegmentReaderPtr inMemSegReader(
            new InMemMultiShardingIndexSegmentReader(segReaders, sectionReader));
    return inMemSegReader;
}

void MultiShardingIndexWriter::GetDumpEstimateFactor(
        priority_queue<double>& factors,
        priority_queue<size_t>& minMemUses) const
{
    for (size_t i = 0; i < mShardingWriters.size(); ++i)
    {
        mShardingWriters[i]->GetDumpEstimateFactor(factors, minMemUses);
    }

    if (mSectionAttributeWriter)
    {
        mSectionAttributeWriter->GetDumpEstimateFactor(factors, minMemUses);
    }
}

void MultiShardingIndexWriter::AddToken(
        const Token* token, fieldid_t fieldId, 
        pos_t tokenBasePos)
{
    assert(mShardingIndexHasher);
    size_t shardingIdx = mShardingIndexHasher->GetShardingIdx(token);
    mShardingWriters[shardingIdx]->AddToken(token, fieldId, tokenBasePos);
}

index::SectionAttributeWriterPtr
MultiShardingIndexWriter::CreateSectionAttributeWriter(
        const IndexConfigPtr& indexConfig,
        util::BuildResourceMetrics* buildResourceMetrics)
{
    IndexFormatOptionPtr indexFormatOption(new IndexFormatOption);
    indexFormatOption->Init(indexConfig);
    IndexFormatWriterCreator writerCreator(indexFormatOption, indexConfig);
    return writerCreator.CreateSectionAttributeWriter(buildResourceMetrics);
}

IE_NAMESPACE_END(index);

