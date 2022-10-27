#include "indexlib/partition/online_partition_reader.h"
#include <memory>
#include "indexlib/index/normal/sorted_docid_range_searcher.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/doc_range_partitioner.h"
#include "indexlib/index/normal/summary/summary_reader_impl.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/cache/search_cache_partition_wrapper.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"

using namespace std;
using namespace std::tr1;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnlinePartitionReader);

#define IE_LOG_PREFIX mPartitionName.c_str()

OnlinePartitionReader::OnlinePartitionReader(
        const IndexPartitionOptions& options,
        const IndexPartitionSchemaPtr& schema,
        const SearchCachePartitionWrapperPtr& searchCache,
        OnlinePartitionMetrics* onlinePartMetrics,
        segmentid_t latestValidRtLinkSegId,
        const string& partitionName,
        int64_t latestNeedSkipIncTs)
    : IndexPartitionReader(schema)
    , mOptions(options)
    , mSearchCache(searchCache)
    , mOnlinePartMetrics(onlinePartMetrics)
    , mLatestValidRtLinkSegId(latestValidRtLinkSegId)
    , mPartitionName(partitionName)
    , mLatestNeedSkipIncTs(latestNeedSkipIncTs)
{
    mSortedDocidRangeSearcher.reset(new SortedDocidRangeSearcher);
}

OnlinePartitionReader::~OnlinePartitionReader()
{
    mSummaryReader.reset();
    mAttrReaderContainer.reset();
    mIndexReader.reset();

    mDeletionMapReader.reset();
    mPrimaryKeyIndexReader.reset();

    mIndexAccessoryReader.reset();
    mSortedDocidRangeSearcher.reset();
    mSubPartitionReader.reset();

    // reset partition data after readers
    // ensure inMemorySegment(segmentWriter) destruct after reader destruction
    mPartitionData.reset();
}

void OnlinePartitionReader::Open(const index_base::PartitionDataPtr& partitionData)
{
    IE_PREFIX_LOG(INFO, "OnlinePartitionReader open begin");
    mPartitionData = partitionData;

    SwitchToLinkDirectoryForRtSegments(mPartitionData);
    InitPartitionVersion(mPartitionData, mLatestValidRtLinkSegId);
    InitSwitchRtSegments(mPartitionData, mLatestValidRtLinkSegId);

    try
    {
        InitDeletionMapReader();
        InitAttributeReaders();
        InitIndexReaders(partitionData);
        InitSummaryReader();
        InitSortedDocidRangeSearcher();
        InitSubPartitionReader();
    }
    catch(const FileIOException& ioe)
    {
        // do not catch FileIOException, just throw it out to searcher
        // searcher will decide how to handle this exception
        IE_PREFIX_LOG(ERROR, "caught FileIOException when opening index partition");
        throw;
    }
    catch(const ExceptionBase& e)
    {
        stringstream ss;
        ss << "FAIL to Load latest version: " << e.ToString();
        IE_PREFIX_LOG(WARN, "%s", ss.str().data());
        throw;
    }
    IE_PREFIX_LOG(INFO, "OnlinePartitionReader open end");
}

void OnlinePartitionReader::InitPrimaryKeyIndexReader(
        const index_base::PartitionDataPtr& partitionData)
{
    const IndexSchemaPtr indexSchema = mSchema->GetIndexSchema();
    assert(indexSchema);

    IndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    if (!indexConfig)
    {
        return;
    }

    IndexType type = indexConfig->GetIndexType();
    if (type == it_primarykey64 || type == it_primarykey128 || type == it_trie)
    {
        IE_PREFIX_LOG(INFO, "InitPrimaryKeyIndexReader begin");
        IndexReaderPtr indexReader = CreateIndexReader(indexConfig, mPartitionData);
        IE_PREFIX_LOG(INFO, "InitPrimaryKeyIndexReader end");
        mPrimaryKeyIndexReader =
            DYNAMIC_POINTER_CAST(PrimaryKeyIndexReader, indexReader);

        assert(mPrimaryKeyIndexReader);
        if (mIndexReader)
        {
            mIndexReader->AddIndexReader(indexConfig, indexReader);
        }
    }
}

void OnlinePartitionReader::InitIndexReaders(
        const index_base::PartitionDataPtr& partitionData)
{
    IE_PREFIX_LOG(INFO, "InitIndexReaders begin");
    const IndexSchemaPtr indexSchema = mSchema->GetIndexSchema();
    assert(indexSchema);

    IndexMetrics* indexMetrics = NULL;
    if (mOnlinePartMetrics)
    {
        indexMetrics = mOnlinePartMetrics->GetIndexMetrics();
    }
    MultiFieldIndexReaderPtr multiIndexReader(
            new MultiFieldIndexReader(indexMetrics));
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    for (; iter != indexConfigs->End(); iter++)
    {
        const IndexConfigPtr& indexConfig = *iter;
        if (indexConfig->GetShardingType() == IndexConfig::IST_IS_SHARDING)
        {
            continue;
        }
        IndexType type = indexConfig->GetIndexType();
        if (type == it_kkv || type == it_kv)
        {
            continue;
        }
        if (type == it_primarykey64 || type == it_primarykey128 || type == it_trie)
        {
            InitPrimaryKeyIndexReader(partitionData);
            continue;
        }
        if (type == it_customized && mOptions.GetOnlineConfig().disableLoadCustomizedIndex) {
            IE_LOG(WARN, "disable load customized index[%s]",
                   indexConfig->GetIndexName().c_str());
            continue;
        }

        IndexReaderPtr indexReader = CreateIndexReader(indexConfig, partitionData);
        if (indexConfig->GetIndexType() == it_range)
        {
            AddAttributeReader<RangeIndexReader>(indexReader, indexConfig);
        }
        else if(indexConfig->GetIndexType() == it_spatial)
        {
            AddAttributeReader<SpatialIndexReader>(indexReader, indexConfig);
        }
        else if (indexConfig->GetIndexType() == it_date)
        {
            AddAttributeReader<DateIndexReader>(indexReader, indexConfig);
        }
        multiIndexReader->AddIndexReader(indexConfig, indexReader);
    }

    if (mPrimaryKeyIndexReader)
    {
        multiIndexReader->AddIndexReader(
                indexSchema->GetPrimaryKeyIndexConfig(), mPrimaryKeyIndexReader);
    }

    InitIndexAccessoryReader();
    multiIndexReader->SetAccessoryReader(mIndexAccessoryReader);

    mIndexReader = multiIndexReader;
    IE_PREFIX_LOG(INFO, "InitIndexReaders end");
}

const SummaryReaderPtr& OnlinePartitionReader::GetSummaryReader() const
{
    return mSummaryReader;
}

const AttributeReaderPtr& OnlinePartitionReader::GetAttributeReader(
        const string& field) const
{
    if (!mAttrReaderContainer)
    {
        return AttributeReaderContainer::RET_EMPTY_ATTR_READER;
    }
    return mAttrReaderContainer->GetAttributeReader(field);
}

const PackAttributeReaderPtr& OnlinePartitionReader::GetPackAttributeReader(
    const string& packAttrName) const
{
    if (!mAttrReaderContainer)
    {
        return AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER;
    }
    return mAttrReaderContainer->GetPackAttributeReader(packAttrName);
}

const PackAttributeReaderPtr& OnlinePartitionReader::GetPackAttributeReader(
    packattrid_t packAttrId) const
{
    if (!mAttrReaderContainer)
    {
        return AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER;
    }
    return mAttrReaderContainer->GetPackAttributeReader(packAttrId);
}

IndexReaderPtr OnlinePartitionReader::GetIndexReader() const
{
    return mIndexReader;
}

const IndexReaderPtr& OnlinePartitionReader::GetIndexReader(
        const string& indexName) const
{
    if (mIndexReader)
    {
        return mIndexReader->GetIndexReader(indexName);
    }
    return RET_EMPTY_INDEX_READER;
}

const IndexReaderPtr& OnlinePartitionReader::GetIndexReader(
        indexid_t indexId) const
{
    if (mIndexReader)
    {
        return mIndexReader->GetIndexReaderWithIndexId(indexId);
    }
    return RET_EMPTY_INDEX_READER;
}

void OnlinePartitionReader::InitDeletionMapReader()
{
    mDeletionMapReader = mPartitionData->GetDeletionMapReader();
}

void OnlinePartitionReader::InitIndexAccessoryReader()
{
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    if (indexSchema)
    {
        mIndexAccessoryReader.reset(new IndexAccessoryReader(indexSchema));
        if (!mIndexAccessoryReader->Open(mPartitionData))
        {
            INDEXLIB_FATAL_ERROR(InitializeFailed,
                    "Failed to open accessory reader");
        }
    }
}



IndexReaderPtr OnlinePartitionReader::CreateIndexReader(
        const IndexConfigPtr& indexConfig,
        const PartitionDataPtr& partitionData) const
{
    IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
    assert(shardingType != IndexConfig::IST_IS_SHARDING);
    IndexReaderPtr indexReader;
    if (shardingType == IndexConfig::IST_NO_SHARDING)
    {
        indexReader.reset(IndexReaderFactory::CreateIndexReader(
                        indexConfig->GetIndexType()));
        indexReader->Open(indexConfig, partitionData);
        return indexReader;
    }

    if (shardingType == IndexConfig::IST_NEED_SHARDING)
    {
        indexReader.reset(new MultiShardingIndexReader());
        indexReader->Open(indexConfig, partitionData);
        return indexReader;
    }

    assert(false);
    return indexReader;
}


SummaryReader* OnlinePartitionReader::CreateSummaryReader(
        const SummarySchemaPtr& summarySchema)
{
    return new SummaryReaderImpl(summarySchema);
}

void OnlinePartitionReader::InitAttributeReaders(bool lazyLoad, bool needPackAttrReader)
{
    if (mSchema->GetTableType() == tt_kkv || mSchema->GetTableType() == tt_kv)
    {
        return;
    }
    AttributeMetrics* attrMetrics = NULL;
    if (mOnlinePartMetrics)
    {
        attrMetrics = mOnlinePartMetrics->GetAttributeMetrics();
    }

    IE_PREFIX_LOG(INFO, "Init Attribute container begin");
    mAttrReaderContainer.reset(new IE_NAMESPACE(index)::AttributeReaderContainer(mSchema));
    mAttrReaderContainer->Init(mPartitionData, attrMetrics, lazyLoad, needPackAttrReader,
                               mOptions.GetOnlineConfig().GetInitReaderThreadCount());
    IE_PREFIX_LOG(INFO, "Init Attribute container end");
}

void OnlinePartitionReader::InitSummaryReader()
{
    const SummarySchemaPtr summarySchema = mSchema->GetSummarySchema();
    if (!summarySchema)
    {
        return;
    }

    IE_PREFIX_LOG(INFO, "InitSummaryReader begin");
    mSummaryReader.reset(CreateSummaryReader(summarySchema));
    mSummaryReader->Open(mPartitionData, GetPrimaryKeyReader());
    AddAttrReadersToSummaryReader();
    IE_PREFIX_LOG(INFO, "InitSummaryReader end");
}

void OnlinePartitionReader::InitSortedDocidRangeSearcher()
{
    if (mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv)
    {
        return;
    }

    IE_PREFIX_LOG(INFO, "InitSortedDocidRangeSearcher begin");
    mSortedDocidRangeSearcher->Init(mAttrReaderContainer->GetAttrReaders(),
                                    mPartitionData->GetPartitionMeta());
    IE_PREFIX_LOG(INFO, "InitSortedDocidRangeSearcher end");
}

void OnlinePartitionReader::AddAttrReadersToSummaryReader()
{
    if (mSummaryReader)
    {
        const SummarySchemaPtr summarySchema = mSchema->GetSummarySchema();

        SummarySchema::Iterator it;
        for (it = summarySchema->Begin(); it != summarySchema->End(); ++it)
        {
            const string& fieldName = (*it)->GetSummaryName();
            const FieldSchemaPtr& fieldSchema  = mSchema->GetFieldSchema();
            fieldid_t fieldId = fieldSchema->GetFieldId(fieldName);

            AttributeReaderPtr attrReader = GetAttributeReader(fieldName);
            if (attrReader)
            {
                assert(fieldId != INVALID_FIELDID);
                mSummaryReader->AddAttrReader(fieldId, attrReader);
                continue;
            }
            const AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
            if (!attrSchema)
            {
                continue;
            }
            const AttributeConfigPtr& attrConfig =
                attrSchema->GetAttributeConfigByFieldId(fieldId);
            if (!attrConfig)
            {
                continue;
            }
            PackAttributeConfig* packConfig = attrConfig->GetPackAttributeConfig();
            if (packConfig)
            {
                mSummaryReader->AddPackAttrReader(
                        fieldId, GetPackAttributeReader(packConfig->GetAttrName()));
            }
        }
    }
}

PartitionInfoPtr OnlinePartitionReader::GetPartitionInfo() const
{
    return mPartitionData->GetPartitionInfo();
}

bool OnlinePartitionReader::GetSortedDocIdRanges(
        const DimensionDescriptionVector& dimensions,
        const DocIdRange& rangeLimits,
        DocIdRangeVector& resultRanges) const
{
    return mSortedDocidRangeSearcher->GetSortedDocIdRanges(
            dimensions, rangeLimits, resultRanges);
}

bool OnlinePartitionReader::GetPartedDocIdRanges(const DocIdRangeVector& rangeHint,
    size_t totalWayCount, size_t wayIdx, DocIdRangeVector& ranges) const
{
    return index::DocRangePartitioner::GetPartedDocIdRanges(
        rangeHint, mPartitionData->GetPartitionInfo(), totalWayCount, wayIdx, ranges);
}

bool OnlinePartitionReader::GetPartedDocIdRanges(const DocIdRangeVector& rangeHint,
    size_t totalWayCount, std::vector<DocIdRangeVector>& rangeVectors) const
{
    return index::DocRangePartitioner::GetPartedDocIdRanges(
        rangeHint, mPartitionData->GetPartitionInfo(), totalWayCount, rangeVectors);
}

const OnlinePartitionReader::AccessCounterMap&
OnlinePartitionReader::GetAttributeAccessCounters() const
{
    if (mOnlinePartMetrics && mOnlinePartMetrics->GetAttributeMetrics())
    {
        return mOnlinePartMetrics->GetAttributeMetrics()->GetAccessCounters();
    }
    static AccessCounterMap counters;
    return counters;
}

const OnlinePartitionReader::AccessCounterMap&
OnlinePartitionReader::GetIndexAccessCounters() const
{
    if (mOnlinePartMetrics && mOnlinePartMetrics->GetIndexMetrics())
    {
        return mOnlinePartMetrics->GetIndexMetrics()->GetAccessCounters();
    }
    static AccessCounterMap counters;
    return counters;
}

void OnlinePartitionReader::InitSubPartitionReader()
{
    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();
    if (!subSchema)
    {
        return;
    }

    IE_PREFIX_LOG(INFO, "InitSubPartitionReader begin");

    OnlinePartitionReaderPtr partitionReader(new OnlinePartitionReader(
                    mOptions, subSchema, mSearchCache, mOnlinePartMetrics));
    partitionReader->Open(mPartitionData->GetSubPartitionData());
    mSubPartitionReader = partitionReader;

    //TODO: refine attribute reader factory
    AttributeReaderPtr mainAttrReader = GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    JoinDocidAttributeReaderPtr mainJoinDocidAttrReader =
        DYNAMIC_POINTER_CAST(JoinDocidAttributeReader, mainAttrReader);
    assert(mainJoinDocidAttrReader);
    mainJoinDocidAttrReader->InitJoinBaseDocId(mPartitionData->GetSubPartitionData());

    AttributeReaderPtr subAttrReader = mSubPartitionReader->GetAttributeReader(
            SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME);
    JoinDocidAttributeReaderPtr subJoinDocidAttrReader =
        DYNAMIC_POINTER_CAST(JoinDocidAttributeReader, subAttrReader);
    assert(subJoinDocidAttrReader);
    subJoinDocidAttrReader->InitJoinBaseDocId(mPartitionData);
    IE_PREFIX_LOG(INFO, "InitSubPartitionReader end");
}

void OnlinePartitionReader::EnableAccessCountors()
{
    if (mAttrReaderContainer && mAttrReaderContainer->GetAttrReaders())
    {
        mAttrReaderContainer->GetAttrReaders()->EnableAccessCountors();
    }
    if (mIndexReader)
    {
        mIndexReader->EnableAccessCountors();
    }
    if (mSubPartitionReader)
    {
        mSubPartitionReader->EnableAccessCountors();
    }
}

void OnlinePartitionReader::SwitchToLinkDirectoryForRtSegments(
        const PartitionDataPtr& partitionData)
{
    assert(partitionData);
    if (mLatestValidRtLinkSegId == INVALID_SEGMENTID)
    {
        return;
    }

    if (!partitionData->SwitchToLinkDirectoryForRtSegments(mLatestValidRtLinkSegId))
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "SwitchToLinkDirectoryForRtSegments fail!");
    }
}

void OnlinePartitionReader::InitSwitchRtSegments(
        const index_base::PartitionDataPtr& partitionData, segmentid_t lastRtLinkSegId)
{
    if (lastRtLinkSegId == INVALID_SEGMENTID)
    {
        return;
    }

    PartitionData::Iterator iter = partitionData->Begin();
    for (; iter != partitionData->End(); iter++)
    {
        segmentid_t segId = (*iter).GetSegmentId();
        if (RealtimeSegmentDirectory::IsRtSegmentId(segId) && segId <= lastRtLinkSegId)
        {
            mSwitchRtSegIds.push_back(segId);
        }
    }
}

const AttributeReaderContainerPtr& OnlinePartitionReader::GetAttributeReaderContainer() const
{
    return mAttrReaderContainer;
}

index_base::Version OnlinePartitionReader::GetVersion() const
{
    assert(mPartitionData);
    return mPartitionData->GetVersion();
}

index_base::Version OnlinePartitionReader::GetOnDiskVersion() const
{
    assert(mPartitionData);
    return mPartitionData->GetOnDiskVersion();
}

#undef IE_LOG_PREFIX

IE_NAMESPACE_END(partition);
