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
#pragma once

#include <map>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/kkv/multi_region_kkv_reader.h"
#include "indexlib/index/kv/multi_region_kv_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/table/normal_table/DimensionDescription.h"

DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);
DECLARE_REFERENCE_CLASS(index, InvertedIndexReader);
DECLARE_REFERENCE_CLASS(index, LegacyIndexReader);
DECLARE_REFERENCE_CLASS(index, MultiPackAttributeReader);
DECLARE_REFERENCE_CLASS(index, MultiFieldAttributeReader);
DECLARE_REFERENCE_CLASS(index, SortedDocidRangeSearcher);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace index {
class LegacyIndexAccessoryReader;
namespace legacy {
class MultiFieldIndexReader;
}
}} // namespace indexlib::index

namespace indexlib { namespace partition {

class OnlinePartitionReader;
DEFINE_SHARED_PTR(OnlinePartitionReader);

class OnlinePartitionReader : public IndexPartitionReader
{
public:
    OnlinePartitionReader(const config::IndexPartitionOptions& options, const config::IndexPartitionSchemaPtr& schema,
                          const util::SearchCachePartitionWrapperPtr& searchCache,
                          partition::OnlinePartitionMetrics* onlinePartitionMetrics = NULL,
                          segmentid_t latestValidRtLinkSegId = INVALID_SEGMENTID, const std::string& partitionName = "",
                          int64_t latestNeedSkipIncTs = INVALID_TIMESTAMP);

    virtual ~OnlinePartitionReader();

public:
    void Open(const index_base::PartitionDataPtr& partitionData) override;
    void Open(const index_base::PartitionDataPtr& partitionData, const OnlinePartitionReader* hintReader);

    index_base::PartitionDataPtr GetPartitionData() const override { return mPartitionData; }

    const index::SummaryReaderPtr& GetSummaryReader() const override;

    const index::SourceReaderPtr& GetSourceReader() const override;

    const index::AttributeReaderPtr& GetAttributeReader(const std::string& field) const override;

    const index::PackAttributeReaderPtr& GetPackAttributeReader(const std::string& packAttrName) const override;

    const index::PackAttributeReaderPtr& GetPackAttributeReader(packattrid_t packAttrId) const override;

    const index::AttributeReaderContainerPtr& GetAttributeReaderContainer() const override;

    index::InvertedIndexReaderPtr GetInvertedIndexReader() const override;
    const index::InvertedIndexReaderPtr& GetInvertedIndexReader(const std::string& indexName) const override;
    const index::InvertedIndexReaderPtr& GetInvertedIndexReader(indexid_t indexId) const override;

    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyReader() const override { return mPrimaryKeyIndexReader; }

    const index::KKVReaderPtr& GetKKVReader(const std::string& regionName) const override
    {
        if (mKKVReader) {
            return mKKVReader->GetReader(regionName);
        }
        return RET_EMPTY_KKV_READER;
    }

    const index::KKVReaderPtr& GetKKVReader(regionid_t regionId = DEFAULT_REGIONID) const override
    {
        if (mKKVReader) {
            return mKKVReader->GetReader(regionId);
        }
        return RET_EMPTY_KKV_READER;
    }

    const index::KVReaderPtr& GetKVReader(const std::string& regionName) const override
    {
        if (mKVReader) {
            return mKVReader->GetReader(regionName);
        }
        return RET_EMPTY_KV_READER;
    }

    const index::KVReaderPtr& GetKVReader(regionid_t regionId = DEFAULT_REGIONID) const override
    {
        if (mKVReader) {
            return mKVReader->GetReader(regionId);
        }
        return RET_EMPTY_KV_READER;
    }
    index_base::Version GetVersion() const override;
    index_base::Version GetOnDiskVersion() const override;

    const index::DeletionMapReaderPtr& GetDeletionMapReader() const override;
    index::PartitionInfoPtr GetPartitionInfo() const override;
    IndexPartitionReaderPtr GetSubPartitionReader() const override { return mSubPartitionReader; }

    bool GetSortedDocIdRanges(const std::vector<std::shared_ptr<table::DimensionDescription>>& dimensions,
                              const DocIdRange& rangeLimits, DocIdRangeVector& resultRanges) const override;

    bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount, size_t wayIdx,
                              DocIdRangeVector& ranges) const override;

    bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount,
                              std::vector<DocIdRangeVector>& rangeVectors) const override;
    const AccessCounterMap& GetIndexAccessCounters() const override;

    const AccessCounterMap& GetAttributeAccessCounters() const override;

    void EnableAccessCountors(bool needReportTemperatureDetail);
    void InitPartitionVersion(const index_base::PartitionDataPtr& partitionData, segmentid_t lastLinkSegId) override;

protected:
    void InitIndexReaders(const index_base::PartitionDataPtr& partitionData,
                          const OnlinePartitionReader* hintReader = nullptr);

    index::InvertedIndexReaderPtr InitSingleIndexReader(const config::IndexConfigPtr& indexConfig,
                                                        const index_base::PartitionDataPtr& partitionData,
                                                        const index::InvertedIndexReader* hintReader = nullptr);

    index::InvertedIndexReaderPtr InitPrimaryKeyIndexReader(const index_base::PartitionDataPtr& partitionData);

    void InitSummaryReader(const OnlinePartitionReader* hintPartReader = nullptr);

    void InitSourceReader(const OnlinePartitionReader* hintPartReader = nullptr);

    virtual void InitAttributeReaders(config::ReadPreference readPreference, bool needPackAttributeReaders,
                                      const OnlinePartitionReader* hintPartReader);

    void InitIndexAccessoryReader();

    void InitSortedDocidRangeSearcher();

    void AddAttrReadersToSummaryReader();

    void InitSubPartitionReader(const OnlinePartitionReader* hintReader);

    void InitDeletionMapReader();

    void InitKKVReader();

    void InitKVReader();

    index::InvertedIndexReaderPtr CreateIndexReader(const config::IndexConfigPtr& indexConfig,
                                                    const index_base::PartitionDataPtr& partitionData,
                                                    const index::InvertedIndexReader* hintReader) const;
    index::SummaryReader* CreateSummaryReader(const config::SummarySchemaPtr& summarySchema);

    index::SourceReader* CreateSourceReader(const config::SourceSchemaPtr& SourceSchema);

    void SwitchToLinkDirectoryForRtSegments(const index_base::PartitionDataPtr& partitionData);

public:
    int64_t TEST_GetLatestNeedSkipIncTs() const { return mLatestNeedSkipIncTs; }

private:
    void InitSwitchRtSegments(const index_base::PartitionDataPtr& partitionData, segmentid_t lastRtLinkSegId);
    template <typename T>
    void AddAttributeReader(const index::InvertedIndexReaderPtr& indexReader,
                            const config::IndexConfigPtr& indexConfig);

protected:
    config::IndexPartitionOptions mOptions;
    util::SearchCachePartitionWrapperPtr mSearchCache;
    partition::OnlinePartitionMetrics* mOnlinePartMetrics;

    index::SummaryReaderPtr mSummaryReader;
    index::SourceReaderPtr mSourceReader;
    index::AttributeReaderContainerPtr mAttrReaderContainer;
    std::shared_ptr<index::legacy::MultiFieldIndexReader> mIndexReader;

    index::DeletionMapReaderPtr mDeletionMapReader;
    index::PrimaryKeyIndexReaderPtr mPrimaryKeyIndexReader;
    index::MultiRegionKKVReaderPtr mKKVReader;
    index::MultiRegionKVReaderPtr mKVReader;

    std::shared_ptr<index::LegacyIndexAccessoryReader> mIndexAccessoryReader;
    index::SortedDocidRangeSearcherPtr mSortedDocidRangeSearcher;
    index_base::PartitionDataPtr mPartitionData;

    OnlinePartitionReaderPtr mSubPartitionReader;
    segmentid_t mLatestValidRtLinkSegId;
    std::string mPartitionName;
    int64_t mLatestNeedSkipIncTs;

private:
    friend class OnlinePartitionReaderTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnlinePartitionReader);

////////////////////////////////////////////////////////
inline const index::DeletionMapReaderPtr& OnlinePartitionReader::GetDeletionMapReader() const
{
    return mDeletionMapReader;
}

template <typename T>
void OnlinePartitionReader::AddAttributeReader(const std::shared_ptr<index::InvertedIndexReader>& indexReader,
                                               const config::IndexConfigPtr& indexConfig)
{
    config::AttributeConfigPtr attrConfig;
    auto singleFieldIndexConfig = DYNAMIC_POINTER_CAST(config::SingleFieldIndexConfig, indexConfig);
    assert(singleFieldIndexConfig);
    auto attrReader = GetAttributeReader(singleFieldIndexConfig->GetFieldConfig()->GetFieldName());

    // TODO: multi value attribute reader
    auto compositeReader = DYNAMIC_POINTER_CAST(T, indexReader);
    assert(compositeReader);
    compositeReader->AddAttributeReader(attrReader);
}
}} // namespace indexlib::partition
