#ifndef __INDEXLIB_CUSTOM_ONLINE_PARTITION_READER_H
#define __INDEXLIB_CUSTOM_ONLINE_PARTITION_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/config/index_partition_options.h"

DECLARE_REFERENCE_CLASS(table, BuildingSegmentReader);
DECLARE_REFERENCE_CLASS(table, TableResource);
DECLARE_REFERENCE_CLASS(table, TableWriter);
DECLARE_REFERENCE_CLASS(table, TableReader);
DECLARE_REFERENCE_CLASS(table, TableFactoryWrapper);
DECLARE_REFERENCE_CLASS(table, SegmentMeta);
DECLARE_REFERENCE_CLASS(table, BuiltSegmentReader);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(index, IndexReader);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeReaderContainer);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);
DECLARE_REFERENCE_CLASS(index, SummaryReader);
DECLARE_REFERENCE_CLASS(index, IndexMetricsBase);
DECLARE_REFERENCE_CLASS(partition, CustomPartitionData);
DECLARE_REFERENCE_CLASS(index_base, SegmentDirectory);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index_base, DimensionDescription);
DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);

IE_NAMESPACE_BEGIN(partition);

class CustomOnlinePartitionReader : public IndexPartitionReader
{
public:
    CustomOnlinePartitionReader(const config::IndexPartitionSchemaPtr& schema,
                                const config::IndexPartitionOptions& options,
                                const std::string& partitionName,
                                const table::TableFactoryWrapperPtr& tableFactory,
                                const table::TableWriterPtr& tableWriter);
    ~CustomOnlinePartitionReader();
public:
    void Open(const index_base::PartitionDataPtr& partitionData) override;
    void OpenWithHeritage(const index_base::PartitionDataPtr& partitionData,
                          const table::TableReaderPtr& lastTableReader,
                          const table::TableReaderPtr& preloadTableReader); 

    index_base::Version GetVersion() const override;
    index::PartitionInfoPtr GetPartitionInfo() const override;  // ?
    const table::TableReaderPtr& GetTableReader() const override;
public:
    index_base::Version GetOnDiskVersion() const override;
    const index::SummaryReaderPtr& GetSummaryReader() const override;
    const index::AttributeReaderPtr& GetAttributeReader(
        const std::string& field) const override;
    const index::PackAttributeReaderPtr& GetPackAttributeReader(
        const std::string& packAttrName) const override;
    const index::PackAttributeReaderPtr& GetPackAttributeReader(
        packattrid_t packAttrId) const override;
    const index::AttributeReaderContainerPtr& GetAttributeReaderContainer() const override;
    index::IndexReaderPtr GetIndexReader() const override;
    const index::IndexReaderPtr& GetIndexReader(
        const std::string& indexName) const override;
    const index::IndexReaderPtr& GetIndexReader(indexid_t indexId) const override;
    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyReader() const override;;
    const index::DeletionMapReaderPtr& GetDeletionMapReader() const override;
    IndexPartitionReaderPtr GetSubPartitionReader() const override;
    bool GetSortedDocIdRanges(
        const std::vector<index_base::DimensionDescriptionPtr>& dimensions,
        const DocIdRange& rangeLimits,
        DocIdRangeVector& resultRanges) const override;
    const index::IndexMetricsBase::AccessCounterMap& GetIndexAccessCounters() const override;
    const index::IndexMetricsBase::AccessCounterMap& GetAttributeAccessCounters() const override;
    void InitPartitionVersion(const index_base::PartitionDataPtr& partitionData,
                              segmentid_t buildingSegId) override; 
    void SetForceSeekInfo(const std::pair<int64_t, int64_t>& forceSeekInfo);
    bool IsUsingSegment(segmentid_t segmentId) const override;

private:
    void InitDependentSegments(
        const index_base::PartitionDataPtr& partitionData,
        const std::vector<table::BuildingSegmentReaderPtr>& dumpingSegReaders,
        const table::TableWriterPtr& tableWriter);
    
    bool CreateBuiltSegmentMetas(
        const CustomPartitionDataPtr& partitionData,
        std::vector<table::SegmentMetaPtr>& segMetas);

    bool CreateBuiltSegmentReaders(
        const table::TableReaderPtr& currentTableReader,
        const table::TableReaderPtr& lastTableReader,
        const table::TableReaderPtr& preloadTableReader,
        const std::vector<table::SegmentMetaPtr>& segMetas,
        std::vector<table::BuiltSegmentReaderPtr>& builtSegmentReaders);

private:
    config::IndexPartitionOptions mOptions;
    std::string mPartitionName;
    table::TableFactoryWrapperPtr mTableFactory;
    table::TableWriterPtr mTableWriter;
    table::TableReaderPtr mTableReader;
    CustomPartitionDataPtr mPartitionData;
    util::BlockMemoryQuotaControllerPtr mReaderMemController;
    std::set<segmentid_t> mDependentSegIds;
    std::pair<int64_t, int64_t> mForceSeekInfo;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomOnlinePartitionReader);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_CUSTOM_ONLINE_PARTITION_READER_H
