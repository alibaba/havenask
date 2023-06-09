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
#include "indexlib/partition/online_partition_reader.h"

#include <memory>

#include "autil/TimeUtility.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/kkv/kkv_reader.h"
#include "indexlib/index/kv/kv_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_index_reader.h"
#include "indexlib/index/normal/sorted_docid_range_searcher.h"
#include "indexlib/index/normal/source/source_reader.h"
#include "indexlib/index/normal/source/source_reader_impl.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/index/normal/summary/summary_reader_impl.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/doc_range_partitioner.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OnlinePartitionReader);

#define IE_LOG_PREFIX mPartitionName.c_str()

OnlinePartitionReader::OnlinePartitionReader(const IndexPartitionOptions& options,
                                             const IndexPartitionSchemaPtr& schema,
                                             const SearchCachePartitionWrapperPtr& searchCache,
                                             OnlinePartitionMetrics* onlinePartMetrics,
                                             segmentid_t latestValidRtLinkSegId, const string& partitionName,
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
    mKKVReader.reset();
    mKVReader.reset();

    mIndexAccessoryReader.reset();
    mSortedDocidRangeSearcher.reset();
    mSubPartitionReader.reset();

    // reset partition data after readers
    // ensure inMemorySegment(segmentWriter) destruct after reader destruction
    mPartitionData.reset();
}

void OnlinePartitionReader::Open(const index_base::PartitionDataPtr& partitionData) { Open(partitionData, nullptr); }

void OnlinePartitionReader::Open(const index_base::PartitionDataPtr& partitionData,
                                 const OnlinePartitionReader* hintReader)
{
    IE_PREFIX_LOG(INFO, "OnlinePartitionReader open begin");
    mPartitionData = partitionData;

    SwitchToLinkDirectoryForRtSegments(mPartitionData);
    InitPartitionVersion(mPartitionData, mLatestValidRtLinkSegId);
    InitSwitchRtSegments(mPartitionData, mLatestValidRtLinkSegId);

    try {
        InitDeletionMapReader();
        InitAttributeReaders(ReadPreference::RP_RANDOM_PREFER, true, hintReader);
        InitIndexReaders(partitionData, hintReader);
        InitKKVReader();
        InitKVReader();
        InitSummaryReader(hintReader);
        InitSourceReader(hintReader);
        InitSortedDocidRangeSearcher();
        InitSubPartitionReader(hintReader);
    } catch (const FileIOException& ioe) {
        // do not catch FileIOException, just throw it out to searcher
        // searcher will decide how to handle this exception
        IE_PREFIX_LOG(ERROR, "caught FileIOException when opening index partition");
        throw;
    } catch (const ExceptionBase& e) {
        stringstream ss;
        ss << "FAIL to Load latest version: " << e.ToString();
        IE_PREFIX_LOG(WARN, "%s", ss.str().data());
        throw;
    }
    IE_PREFIX_LOG(INFO, "OnlinePartitionReader open end");
}

InvertedIndexReaderPtr
OnlinePartitionReader::InitPrimaryKeyIndexReader(const index_base::PartitionDataPtr& partitionData)
{
    const IndexSchemaPtr indexSchema = mSchema->GetIndexSchema();
    assert(indexSchema);
    IndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    if (!indexConfig) {
        return nullptr;
    }

    InvertedIndexType type = indexConfig->GetInvertedIndexType();
    if (type == it_primarykey64 || type == it_primarykey128 || type == it_trie) {
        auto indexReader = CreateIndexReader(indexConfig, mPartitionData, nullptr);
        mPrimaryKeyIndexReader = DYNAMIC_POINTER_CAST(PrimaryKeyIndexReader, indexReader);
        assert(mPrimaryKeyIndexReader);
        return indexReader;
    }
    return nullptr;
}

void OnlinePartitionReader::InitIndexReaders(const index_base::PartitionDataPtr& partitionData,
                                             const OnlinePartitionReader* hintPartReader)
{
    autil::ScopedTime2 timer;
    IE_PREFIX_LOG(INFO, "InitIndexReaders begin");
    const index::legacy::MultiFieldIndexReader* hintReader = GET_IF_NOT_NULL(hintPartReader, mIndexReader.get());
    const IndexSchemaPtr indexSchema = mSchema->GetIndexSchema();
    assert(indexSchema);

    IndexMetrics* indexMetrics = NULL;
    if (mOnlinePartMetrics) {
        indexMetrics = mOnlinePartMetrics->GetIndexMetrics();
    }
    index::legacy::MultiFieldIndexReaderPtr multiIndexReader(new index::legacy::MultiFieldIndexReader(indexMetrics));
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    for (; iter != indexConfigs->End(); iter++) {
        const IndexConfigPtr& indexConfig = *iter;
        auto* hintIndexReader = GET_IF_NOT_NULL(hintReader, GetIndexReaderWithIndexId(indexConfig->GetIndexId()).get());
        auto indexReader = InitSingleIndexReader(indexConfig, partitionData, hintIndexReader);
        if (indexReader) {
            multiIndexReader->AddIndexReader(indexConfig, indexReader);
        }
    }

    InitIndexAccessoryReader();
    multiIndexReader->SetLegacyAccessoryReader(mIndexAccessoryReader);
    mIndexReader = multiIndexReader;
    IE_PREFIX_LOG(INFO, "InitIndexReaders end, used[%.3f]s", timer.done_sec());
}

const SummaryReaderPtr& OnlinePartitionReader::GetSummaryReader() const { return mSummaryReader; }

const SourceReaderPtr& OnlinePartitionReader::GetSourceReader() const { return mSourceReader; }

const AttributeReaderPtr& OnlinePartitionReader::GetAttributeReader(const string& field) const
{
    if (!mAttrReaderContainer) {
        return AttributeReaderContainer::RET_EMPTY_ATTR_READER;
    }
    return mAttrReaderContainer->GetAttributeReader(field);
}

const PackAttributeReaderPtr& OnlinePartitionReader::GetPackAttributeReader(const string& packAttrName) const
{
    if (!mAttrReaderContainer) {
        return AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER;
    }
    return mAttrReaderContainer->GetPackAttributeReader(packAttrName);
}

const PackAttributeReaderPtr& OnlinePartitionReader::GetPackAttributeReader(packattrid_t packAttrId) const
{
    if (!mAttrReaderContainer) {
        return AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER;
    }
    return mAttrReaderContainer->GetPackAttributeReader(packAttrId);
}

InvertedIndexReaderPtr OnlinePartitionReader::GetInvertedIndexReader() const { return mIndexReader; }

const InvertedIndexReaderPtr& OnlinePartitionReader::GetInvertedIndexReader(const string& indexName) const
{
    if (mIndexReader) {
        return mIndexReader->GetInvertedIndexReader(indexName);
    }
    return RET_EMPTY_INDEX_READER;
}

const InvertedIndexReaderPtr& OnlinePartitionReader::GetInvertedIndexReader(indexid_t indexId) const
{
    if (mIndexReader) {
        return mIndexReader->GetIndexReaderWithIndexId(indexId);
    }
    return RET_EMPTY_INDEX_READER;
}

void OnlinePartitionReader::InitDeletionMapReader() { mDeletionMapReader = mPartitionData->GetDeletionMapReader(); }

void OnlinePartitionReader::InitIndexAccessoryReader()
{
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    if (indexSchema) {
        mIndexAccessoryReader.reset(new index::LegacyIndexAccessoryReader(indexSchema));
        if (!mIndexAccessoryReader->Open(mPartitionData)) {
            INDEXLIB_FATAL_ERROR(InitializeFailed, "Failed to open accessory reader");
        }
    }
}

InvertedIndexReaderPtr OnlinePartitionReader::CreateIndexReader(const IndexConfigPtr& indexConfig,
                                                                const PartitionDataPtr& partitionData,
                                                                const InvertedIndexReader* hintReader) const
{
    autil::ScopedTime2 timer;
    IE_PREFIX_LOG(INFO, "CreateIndexReader [%s] begin", indexConfig->GetIndexName().c_str());
    IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
    assert(shardingType != IndexConfig::IST_IS_SHARDING);
    index::IndexMetrics* indexMetrics = nullptr;
    if (mOnlinePartMetrics) {
        indexMetrics = mOnlinePartMetrics->GetIndexMetrics();
    }
    if (shardingType == IndexConfig::IST_NO_SHARDING) {
        std::shared_ptr<InvertedIndexReader> indexReader(
            IndexReaderFactory::CreateIndexReader(indexConfig->GetInvertedIndexType(), indexMetrics));
        const auto& legacyReader = std::dynamic_pointer_cast<LegacyIndexReaderInterface>(indexReader);
        assert(legacyReader);
        legacyReader->Open(indexConfig, partitionData, hintReader);
        IE_PREFIX_LOG(INFO, "CreateIndexReader [%s] end, used[%.3f]s", indexConfig->GetIndexName().c_str(),
                      timer.done_sec());
        return indexReader;
    }

    if (shardingType == IndexConfig::IST_NEED_SHARDING) {
        std::shared_ptr<LegacyIndexReader> indexReader(new MultiShardingIndexReader(indexMetrics));
        indexReader->Open(indexConfig, partitionData, hintReader);
        IE_PREFIX_LOG(INFO, "CreateIndexReader [%s] end, used[%.3f]s", indexConfig->GetIndexName().c_str(),
                      timer.done_sec());
        return indexReader;
    }

    assert(false);
    IE_PREFIX_LOG(INFO, "CreateIndexReader [%s] end, used[%.3f]s", indexConfig->GetIndexName().c_str(),
                  timer.done_sec());
    return nullptr;
}

SummaryReader* OnlinePartitionReader::CreateSummaryReader(const SummarySchemaPtr& summarySchema)
{
    return new SummaryReaderImpl(summarySchema);
}

SourceReader* OnlinePartitionReader::CreateSourceReader(const SourceSchemaPtr& sourceSchema)
{
    return new SourceReaderImpl(sourceSchema);
}

void OnlinePartitionReader::InitAttributeReaders(ReadPreference readPreference, bool needPackAttrReader,
                                                 const OnlinePartitionReader* hintPartReader)
{
    if (mSchema->GetTableType() == tt_kkv || mSchema->GetTableType() == tt_kv) {
        return;
    }
    index::AttributeReaderContainer* hintContainer = GET_IF_NOT_NULL(hintPartReader, mAttrReaderContainer.get());
    AttributeMetrics* attrMetrics = NULL;
    if (mOnlinePartMetrics) {
        attrMetrics = mOnlinePartMetrics->GetAttributeMetrics();
    }

    autil::ScopedTime2 timer;
    IE_PREFIX_LOG(INFO, "Init Attribute container begin");
    mAttrReaderContainer.reset(new indexlib::index::AttributeReaderContainer(mSchema));
    mAttrReaderContainer->Init(mPartitionData, attrMetrics, readPreference, needPackAttrReader,
                               mOptions.GetOnlineConfig().GetInitReaderThreadCount(), hintContainer);
    IE_PREFIX_LOG(INFO, "Init Attribute container end, used[%.3f]s", timer.done_sec());
}

void OnlinePartitionReader::InitSummaryReader(const OnlinePartitionReader* hintReader)
{
    const SummarySchemaPtr summarySchema = mSchema->GetSummarySchema();
    if (!summarySchema || summarySchema->IsAllFieldsDisabled()) {
        return;
    }

    autil::ScopedTime2 timer;
    IE_PREFIX_LOG(INFO, "InitSummaryReader begin");
    mSummaryReader.reset(CreateSummaryReader(summarySchema));
    mSummaryReader->Open(mPartitionData, GetPrimaryKeyReader(), GET_IF_NOT_NULL(hintReader, mSummaryReader.get()));
    AddAttrReadersToSummaryReader();
    IE_PREFIX_LOG(INFO, "InitSummaryReader end, used[%.3f]s", timer.done_sec());
}

void OnlinePartitionReader::InitSourceReader(const OnlinePartitionReader* hintReader)
{
    const SourceSchemaPtr sourceSchema = mSchema->GetSourceSchema();
    if (!sourceSchema || sourceSchema->IsAllFieldsDisabled()) {
        return;
    }

    IE_PREFIX_LOG(INFO, "InitSourceReader begin");
    mSourceReader.reset(CreateSourceReader(sourceSchema));
    mSourceReader->Open(mPartitionData, GET_IF_NOT_NULL(hintReader, mSourceReader.get()));
    IE_PREFIX_LOG(INFO, "InitSourceReader end");
}

void OnlinePartitionReader::InitSortedDocidRangeSearcher()
{
    if (mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv) {
        return;
    }

    autil::ScopedTime2 timer;
    IE_PREFIX_LOG(INFO, "InitSortedDocidRangeSearcher begin");
    vector<string> sortAttributes;
    PartitionMeta partitionMeta = mPartitionData->GetPartitionMeta();
    const indexlibv2::config::SortDescriptions& sortDescs = partitionMeta.GetSortDescriptions();
    const AttributeSchemaPtr attributeSchema = mSchema->GetAttributeSchema();
    for (size_t i = 0; i < sortDescs.size(); i++) {
        const string& sortFieldName = sortDescs[i].GetSortFieldName();
        assert(attributeSchema);
        if (!attributeSchema->GetAttributeConfig(sortFieldName)->IsDisable()) {
            sortAttributes.push_back(sortFieldName);
        }
    }
    mSortedDocidRangeSearcher->Init(mAttrReaderContainer->GetAttrReaders(), partitionMeta, sortAttributes);
    IE_PREFIX_LOG(INFO, "InitSortedDocidRangeSearcher end, used[%.3f]s", timer.done_sec());
}

void OnlinePartitionReader::AddAttrReadersToSummaryReader()
{
    if (mSummaryReader) {
        const SummarySchemaPtr summarySchema = mSchema->GetSummarySchema();

        SummarySchema::Iterator it;
        for (it = summarySchema->Begin(); it != summarySchema->End(); ++it) {
            const string& fieldName = (*it)->GetSummaryName();
            const FieldSchemaPtr& fieldSchema = mSchema->GetFieldSchema();
            fieldid_t fieldId = fieldSchema->GetFieldId(fieldName);

            AttributeReaderPtr attrReader = GetAttributeReader(fieldName);
            if (attrReader) {
                assert(fieldId != INVALID_FIELDID);
                mSummaryReader->AddAttrReader(fieldId, attrReader);
                continue;
            }
            const AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
            if (!attrSchema) {
                continue;
            }
            const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfigByFieldId(fieldId);
            if (!attrConfig) {
                continue;
            }
            PackAttributeConfig* packConfig = attrConfig->GetPackAttributeConfig();
            if (packConfig) {
                mSummaryReader->AddPackAttrReader(fieldId, GetPackAttributeReader(packConfig->GetAttrName()));
            }
        }
    }
}

PartitionInfoPtr OnlinePartitionReader::GetPartitionInfo() const { return mPartitionData->GetPartitionInfo(); }

bool OnlinePartitionReader::GetSortedDocIdRanges(const table::DimensionDescriptionVector& dimensions,
                                                 const DocIdRange& rangeLimits, DocIdRangeVector& resultRanges) const
{
    return mSortedDocidRangeSearcher->GetSortedDocIdRanges(dimensions, rangeLimits, resultRanges);
}

bool OnlinePartitionReader::GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount, size_t wayIdx,
                                                 DocIdRangeVector& ranges) const
{
    return DocRangePartitioner::GetPartedDocIdRanges(rangeHint, mPartitionData->GetPartitionInfo(), totalWayCount,
                                                     wayIdx, ranges);
}

bool OnlinePartitionReader::GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount,
                                                 std::vector<DocIdRangeVector>& rangeVectors) const
{
    return DocRangePartitioner::GetPartedDocIdRanges(rangeHint, mPartitionData->GetPartitionInfo(), totalWayCount,
                                                     rangeVectors);
}

const OnlinePartitionReader::AccessCounterMap& OnlinePartitionReader::GetAttributeAccessCounters() const
{
    if (mOnlinePartMetrics && mOnlinePartMetrics->GetAttributeMetrics()) {
        return mOnlinePartMetrics->GetAttributeMetrics()->GetAccessCounters();
    }
    static AccessCounterMap counters;
    return counters;
}

const OnlinePartitionReader::AccessCounterMap& OnlinePartitionReader::GetIndexAccessCounters() const
{
    if (mOnlinePartMetrics && mOnlinePartMetrics->GetIndexMetrics()) {
        return mOnlinePartMetrics->GetIndexMetrics()->GetAccessCounters();
    }
    static AccessCounterMap counters;
    return counters;
}

void OnlinePartitionReader::InitSubPartitionReader(const OnlinePartitionReader* hintReader)
{
    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();
    if (!subSchema) {
        return;
    }

    autil::ScopedTime2 timer;
    IE_PREFIX_LOG(INFO, "InitSubPartitionReader begin");

    OnlinePartitionReaderPtr partitionReader(
        new OnlinePartitionReader(mOptions, subSchema, mSearchCache, mOnlinePartMetrics));
    partitionReader->Open(mPartitionData->GetSubPartitionData(),
                          GET_IF_NOT_NULL(hintReader, mSubPartitionReader.get()));
    mSubPartitionReader = partitionReader;

    // TODO: refine attribute reader factory
    AttributeReaderPtr mainAttrReader = GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    JoinDocidAttributeReaderPtr mainJoinDocidAttrReader =
        DYNAMIC_POINTER_CAST(JoinDocidAttributeReader, mainAttrReader);
    assert(mainJoinDocidAttrReader);
    mainJoinDocidAttrReader->InitJoinBaseDocId(mPartitionData->GetSubPartitionData());

    AttributeReaderPtr subAttrReader = mSubPartitionReader->GetAttributeReader(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME);
    JoinDocidAttributeReaderPtr subJoinDocidAttrReader = DYNAMIC_POINTER_CAST(JoinDocidAttributeReader, subAttrReader);
    assert(subJoinDocidAttrReader);
    subJoinDocidAttrReader->InitJoinBaseDocId(mPartitionData);
    IE_PREFIX_LOG(INFO, "InitSubPartitionReader end, used[%.3f]", timer.done_sec());
}

void OnlinePartitionReader::EnableAccessCountors(bool needReportTemperatureDetail)
{
    if (mAttrReaderContainer && mAttrReaderContainer->GetAttrReaders()) {
        mAttrReaderContainer->GetAttrReaders()->EnableAccessCountors(needReportTemperatureDetail);
    }
    if (mIndexReader) {
        mIndexReader->EnableAccessCountors();
    }
    if (mSubPartitionReader) {
        mSubPartitionReader->EnableAccessCountors(needReportTemperatureDetail);
    }
}

void OnlinePartitionReader::InitKVReader()
{
    if (mSchema->GetTableType() != tt_kv) {
        return;
    }
    mKVReader.reset(new MultiRegionKVReader);
    SearchCachePartitionWrapperPtr searchCache;
    if (mOptions.GetOnlineConfig().enableSearchCache) {
        searchCache = mSearchCache;
    }
    mKVReader->Open(mSchema, mPartitionData, searchCache, mLatestNeedSkipIncTs);
    if (mOptions.GetOnlineConfig().useHighPriorityCache) {
        mKKVReader->SetSearchCachePriority(autil::CacheBase::Priority::HIGH);
    }
}

void OnlinePartitionReader::InitKKVReader()
{
    if (mSchema->GetTableType() != tt_kkv) {
        return;
    }
    mKKVReader.reset(new MultiRegionKKVReader);
    SearchCachePartitionWrapperPtr searchCache;
    if (mOptions.GetOnlineConfig().enableSearchCache) {
        searchCache = mSearchCache;
    }
    mKKVReader->Open(mSchema, mPartitionData, searchCache, mLatestNeedSkipIncTs);
    if (mOptions.GetOnlineConfig().useHighPriorityCache) {
        mKKVReader->SetSearchCachePriority(autil::CacheBase::Priority::HIGH);
    }
}

void OnlinePartitionReader::SwitchToLinkDirectoryForRtSegments(const PartitionDataPtr& partitionData)
{
    assert(partitionData);
    if (mLatestValidRtLinkSegId == INVALID_SEGMENTID) {
        return;
    }

    if (!partitionData->SwitchToLinkDirectoryForRtSegments(mLatestValidRtLinkSegId)) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "SwitchToLinkDirectoryForRtSegments fail!");
    }
}

void OnlinePartitionReader::InitSwitchRtSegments(const index_base::PartitionDataPtr& partitionData,
                                                 segmentid_t lastRtLinkSegId)
{
    if (lastRtLinkSegId == INVALID_SEGMENTID) {
        return;
    }

    PartitionData::Iterator iter = partitionData->Begin();
    for (; iter != partitionData->End(); iter++) {
        segmentid_t segId = (*iter).GetSegmentId();
        if (RealtimeSegmentDirectory::IsRtSegmentId(segId) && segId <= lastRtLinkSegId) {
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

InvertedIndexReaderPtr OnlinePartitionReader::InitSingleIndexReader(const IndexConfigPtr& indexConfig,
                                                                    const PartitionDataPtr& partitionData,
                                                                    const InvertedIndexReader* hintReader)
{
    assert(indexConfig);
    if (indexConfig->GetShardingType() == IndexConfig::IST_IS_SHARDING) {
        return nullptr;
    }
    InvertedIndexType type = indexConfig->GetInvertedIndexType();
    if (type == it_kkv || type == it_kv) {
        return nullptr;
    }
    if (type == it_primarykey64 || type == it_primarykey128 || type == it_trie) {
        return InitPrimaryKeyIndexReader(partitionData);
    }
    if (type == it_customized && mOptions.GetOnlineConfig().disableLoadCustomizedIndex) {
        IE_LOG(WARN, "disable load customized index[%s] from table[%s]", indexConfig->GetIndexName().c_str(),
               mSchema->GetSchemaName().c_str());
        return nullptr;
    }

    InvertedIndexReaderPtr indexReader = CreateIndexReader(indexConfig, partitionData, hintReader);
    if (indexConfig->GetInvertedIndexType() == it_range) {
        AddAttributeReader<index::legacy::RangeIndexReader>(indexReader, indexConfig);
    } else if (indexConfig->GetInvertedIndexType() == it_spatial) {
        AddAttributeReader<index::legacy::SpatialIndexReader>(indexReader, indexConfig);
    } else if (indexConfig->GetInvertedIndexType() == it_datetime) {
        AddAttributeReader<index::legacy::DateIndexReader>(indexReader, indexConfig);
    }
    return indexReader;
}

void OnlinePartitionReader::InitPartitionVersion(const index_base::PartitionDataPtr& partitionData,
                                                 segmentid_t lastLinkSegId)
{
    assert(partitionData);
    BuildingPartitionDataPtr buildingPartData = std::dynamic_pointer_cast<BuildingPartitionData>(partitionData);
    std::set<segmentid_t> dumpingSegmentIds;
    if (buildingPartData != nullptr) {
        const DumpSegmentContainerPtr dumpSegmentContainer = buildingPartData->GetDumpSegmentContainer();
        std::vector<InMemorySegmentPtr> dumpingSegments;
        if (dumpSegmentContainer != nullptr) {
            dumpSegmentContainer->GetDumpingSegments(dumpingSegments);
            for (const auto& seg : dumpingSegments) {
                dumpingSegmentIds.insert(seg->GetSegmentId());
            }
        }
    }

    index_base::InMemorySegmentPtr inMemSegment = partitionData->GetInMemorySegment();
    segmentid_t buildingSegId = inMemSegment ? inMemSegment->GetSegmentId() : INVALID_SEGMENTID;
    mPartitionVersion = std::make_unique<PartitionVersion>(partitionData->GetVersion(), buildingSegId, lastLinkSegId,
                                                           dumpingSegmentIds);
    mPartitionVersionHashId = mPartitionVersion->GetHashId();
    return;
}

#undef IE_LOG_PREFIX
}} // namespace indexlib::partition
