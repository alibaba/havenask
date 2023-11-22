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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/table/normal_table/DimensionDescription.h"

DECLARE_REFERENCE_CLASS(table, BuildingSegmentReader);
DECLARE_REFERENCE_CLASS(table, TableResource);
DECLARE_REFERENCE_CLASS(table, TableWriter);
DECLARE_REFERENCE_CLASS(table, TableReader);
DECLARE_REFERENCE_CLASS(table, TableFactoryWrapper);
DECLARE_REFERENCE_CLASS(table, SegmentMeta);
DECLARE_REFERENCE_CLASS(table, BuiltSegmentReader);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(index, InvertedIndexReader);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeReaderContainer);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);
DECLARE_REFERENCE_CLASS(index, SummaryReader);
DECLARE_REFERENCE_CLASS(index, SourceReader);
DECLARE_REFERENCE_CLASS(index, IndexMetricsBase);
DECLARE_REFERENCE_CLASS(partition, CustomPartitionData);
DECLARE_REFERENCE_CLASS(index_base, SegmentDirectory);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);

namespace future_lite {
class Executor;
}

namespace indexlib { namespace partition {

class CustomOnlinePartitionReader : public IndexPartitionReader
{
public:
    CustomOnlinePartitionReader(const config::IndexPartitionSchemaPtr& schema,
                                const config::IndexPartitionOptions& options, const std::string& partitionName,
                                const table::TableFactoryWrapperPtr& tableFactory,
                                const table::TableWriterPtr& tableWriter,
                                segmentid_t latestValidRtLinkSegId = INVALID_SEGMENTID);
    ~CustomOnlinePartitionReader();

public:
    void Open(const index_base::PartitionDataPtr& partitionData) override;
    void OpenWithHeritage(const index_base::PartitionDataPtr& partitionData,
                          const table::TableReaderPtr& lastTableReader,
                          const table::TableReaderPtr& preloadTableReader);

    // inherit segments from lastTableReader, but not segments in excludeSet
    void OpenWithHeritage(const index_base::PartitionDataPtr& partitionData,
                          const table::TableReaderPtr& lastTableReader, const std::set<segmentid_t>& excludeSet);
    index_base::Version GetVersion() const override;
    index::PartitionInfoPtr GetPartitionInfo() const override; // ?
    index_base::PartitionDataPtr GetPartitionData() const override;
    const table::TableReaderPtr& GetTableReader() const override;

public:
    index_base::Version GetOnDiskVersion() const override;
    const index::SummaryReaderPtr& GetSummaryReader() const override;
    const index::SourceReaderPtr& GetSourceReader() const override;
    const index::AttributeReaderPtr& GetAttributeReader(const std::string& field) const override;
    const index::PackAttributeReaderPtr& GetPackAttributeReader(const std::string& packAttrName) const override;
    const index::PackAttributeReaderPtr& GetPackAttributeReader(packattrid_t packAttrId) const override;
    const index::AttributeReaderContainerPtr& GetAttributeReaderContainer() const override;
    std::shared_ptr<index::InvertedIndexReader> GetInvertedIndexReader() const override;
    const index::InvertedIndexReaderPtr& GetInvertedIndexReader(const std::string& indexName) const override;
    const index::InvertedIndexReaderPtr& GetInvertedIndexReader(indexid_t indexId) const override;
    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyReader() const override;
    ;
    const index::DeletionMapReaderPtr& GetDeletionMapReader() const override;
    IndexPartitionReaderPtr GetSubPartitionReader() const override;
    bool GetSortedDocIdRanges(const std::vector<std::shared_ptr<table::DimensionDescription>>& dimensions,
                              const DocIdRange& rangeLimits, DocIdRangeVector& resultRanges) const override;
    const AccessCounterMap& GetIndexAccessCounters() const override;
    const AccessCounterMap& GetAttributeAccessCounters() const override;
    using IndexPartitionReader::InitPartitionVersion;
    void InitPartitionVersion(const index_base::PartitionDataPtr& partitionData, segmentid_t buildingSegId,
                              segmentid_t lastLinkSegId);
    void SetForceSeekInfo(const std::pair<int64_t, int64_t>& forceSeekInfo);
    bool IsUsingSegment(segmentid_t segmentId) const override;
    void SwitchToLinkDirectoryForRtSegments(const CustomPartitionDataPtr& partitionData);
    segmentid_t GetNextValidSegIdToLink();
    size_t GetInMemRtSegmentCount() const { return mInMemRtSegmentCount; }
    size_t GetOnDiskRtSegmentCount() const { return mOnDiskRtSegmentCount; }
    size_t GetUsedOnDiskRtSegmentCount() const { return mUsedOnDiskRtSegmentCount; }
    const std::map<segmentid_t, std::string>& GetBuiltSegmentLifecycles() const { return mSegmentLifecycles; }

private:
    std::shared_ptr<future_lite::Executor> CreateExecutor(const table::TableFactoryWrapperPtr& factoryWrapper,
                                                          const config::IndexPartitionSchemaPtr& schema,
                                                          const config::IndexPartitionOptions& options) const;

    void DoOpenWithHeritage(const index_base::PartitionDataPtr& partitionData,
                            const table::TableReaderPtr& lastTableReader,
                            const table::TableReaderPtr& preloadTableReader, const std::set<segmentid_t>& excludeSet);

    void InitDependentSegments(const index_base::PartitionDataPtr& partitionData,
                               const std::vector<table::SegmentMetaPtr>& segMetas,
                               const std::vector<table::BuildingSegmentReaderPtr>& buildingStateSegments,
                               segmentid_t buildingSegId);

    bool CreateBuiltSegmentMetas(const CustomPartitionDataPtr& partitionData,
                                 std::vector<table::SegmentMetaPtr>& segMetas);

    bool CreateBuiltSegmentReaders(const table::TableReaderPtr& currentTableReader,
                                   const table::TableReaderPtr& lastTableReader,
                                   const table::TableReaderPtr& preloadTableReader,
                                   const std::set<segmentid_t>& excludeSet,
                                   const std::vector<table::SegmentMetaPtr>& segMetas,
                                   std::vector<table::BuiltSegmentReaderPtr>& builtSegmentReaders);

private:
    config::IndexPartitionOptions mOptions;
    std::string mPartitionName;
    table::TableFactoryWrapperPtr mTableFactory;
    table::TableWriterPtr mTableWriter;
    table::TableReaderPtr mTableReader;
    segmentid_t mLatestValidRtLinkSegId;
    CustomPartitionDataPtr mPartitionData;
    util::BlockMemoryQuotaControllerPtr mReaderMemController;
    std::set<segmentid_t> mDependentSegIds;
    std::vector<segmentid_t> mRtSegmentIds;
    std::pair<int64_t, int64_t> mForceSeekInfo;
    size_t mInMemRtSegmentCount;
    size_t mOnDiskRtSegmentCount;
    size_t mUsedOnDiskRtSegmentCount;
    std::shared_ptr<future_lite::Executor> mFutureExecutor;
    std::map<segmentid_t, std::string> mSegmentLifecycles;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomOnlinePartitionReader);
}} // namespace indexlib::partition
