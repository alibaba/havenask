#ifndef __INDEXLIB_ONLINE_PARTITION_READER_H
#define __INDEXLIB_ONLINE_PARTITION_READER_H

#include <map>
#include <tr1/memory>
#include <autil/AtomicCounter.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/online_partition_metrics.h"

DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);
DECLARE_REFERENCE_CLASS(index, IndexReader);
DECLARE_REFERENCE_CLASS(index, IndexAccessoryReader);
DECLARE_REFERENCE_CLASS(index, MultiFieldIndexReader);
DECLARE_REFERENCE_CLASS(index, MultiPackAttributeReader);
DECLARE_REFERENCE_CLASS(index, MultiFieldAttributeReader);
DECLARE_REFERENCE_CLASS(index, SortedDocidRangeSearcher);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index_base, DimensionDescription);

IE_NAMESPACE_BEGIN(partition);

class OnlinePartitionReader;
DEFINE_SHARED_PTR(OnlinePartitionReader);

class OnlinePartitionReader : public IndexPartitionReader
{
public:
    OnlinePartitionReader(const config::IndexPartitionOptions& options,
                          const config::IndexPartitionSchemaPtr& schema,
                          const util::SearchCachePartitionWrapperPtr& searchCache,
                          partition::OnlinePartitionMetrics* onlinePartitionMetrics = NULL,
                          segmentid_t latestValidRtLinkSegId = INVALID_SEGMENTID,
                          const std::string& partitionName = "",
                          int64_t latestNeedSkipIncTs = INVALID_TIMESTAMP);

    virtual ~OnlinePartitionReader();

public:
    void Open(const index_base::PartitionDataPtr& partitionData) override;

    index_base::PartitionDataPtr GetPartitionData() const override
    { return mPartitionData; }

    const index::SummaryReaderPtr& GetSummaryReader() const override;

    const index::AttributeReaderPtr& GetAttributeReader(
        const std::string& field) const override;

    const index::PackAttributeReaderPtr& GetPackAttributeReader(
        const std::string& packAttrName) const override;

    const index::PackAttributeReaderPtr& GetPackAttributeReader(
            packattrid_t packAttrId) const override;

    const index::AttributeReaderContainerPtr& GetAttributeReaderContainer() const override;

    index::IndexReaderPtr GetIndexReader() const override;
    const index::IndexReaderPtr& GetIndexReader(const std::string& indexName) const override;
    const index::IndexReaderPtr& GetIndexReader(indexid_t indexId) const override;

    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyReader() const override
    { return mPrimaryKeyIndexReader; }

    index_base::Version GetVersion() const override;
    index_base::Version GetOnDiskVersion() const override;

    const index::DeletionMapReaderPtr& GetDeletionMapReader() const override;
    index::PartitionInfoPtr GetPartitionInfo() const override;
    IndexPartitionReaderPtr GetSubPartitionReader() const override
    { return mSubPartitionReader; }

    bool GetSortedDocIdRanges(
            const std::vector<index_base::DimensionDescriptionPtr>& dimensions,
            const DocIdRange& rangeLimits,
            DocIdRangeVector& resultRanges) const override;

    bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount,
        size_t wayIdx, DocIdRangeVector& ranges) const override;

    bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount,
                              std::vector<DocIdRangeVector>& rangeVectors) const override;
    const AccessCounterMap& GetIndexAccessCounters() const override;

    const AccessCounterMap& GetAttributeAccessCounters() const override;

    void EnableAccessCountors();

    const std::vector<segmentid_t>& GetSwitchRtSegments() const
    { return mSwitchRtSegIds; }

protected:
    void InitIndexReaders(const index_base::PartitionDataPtr& partitionData);

    void InitPrimaryKeyIndexReader(const index_base::PartitionDataPtr& partitionData);

    void InitSummaryReader();

    void InitAttributeReaders(bool lazyLoad = false, bool needPackAttributeReaders = true);

    void InitIndexAccessoryReader();

    void InitSortedDocidRangeSearcher();

    void AddAttrReadersToSummaryReader();

    void InitSubPartitionReader();

    void InitDeletionMapReader();

    index::IndexReaderPtr CreateIndexReader(
            const config::IndexConfigPtr& indexConfig,
            const index_base::PartitionDataPtr& partitionData) const;

    index::SummaryReader* CreateSummaryReader(
            const config::SummarySchemaPtr& summarySchema);

    void SwitchToLinkDirectoryForRtSegments(
            const index_base::PartitionDataPtr& partitionData);

private:
    void InitSwitchRtSegments(const index_base::PartitionDataPtr& partitionData,
                              segmentid_t lastRtLinkSegId);
    template<typename T>
    void AddAttributeReader(
        const index::IndexReaderPtr& indexReader,
        const config::IndexConfigPtr& indexConfig);
protected:
    config::IndexPartitionOptions mOptions;
    util::SearchCachePartitionWrapperPtr mSearchCache;
    partition::OnlinePartitionMetrics* mOnlinePartMetrics;

    index::SummaryReaderPtr mSummaryReader;
    index::AttributeReaderContainerPtr mAttrReaderContainer;
    index::MultiFieldIndexReaderPtr mIndexReader;

    index::DeletionMapReaderPtr mDeletionMapReader;
    index::PrimaryKeyIndexReaderPtr mPrimaryKeyIndexReader;

    index::IndexAccessoryReaderPtr mIndexAccessoryReader;
    index::SortedDocidRangeSearcherPtr mSortedDocidRangeSearcher;
    index_base::PartitionDataPtr mPartitionData;

    OnlinePartitionReaderPtr mSubPartitionReader;
    segmentid_t mLatestValidRtLinkSegId;
    std::vector<segmentid_t> mSwitchRtSegIds;
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

template<typename T>
void OnlinePartitionReader::AddAttributeReader(
    const index::IndexReaderPtr& indexReader,
    const config::IndexConfigPtr& indexConfig)
{
    config::AttributeConfigPtr attrConfig;
    auto singleFieldIndexConfig = DYNAMIC_POINTER_CAST(
        config::SingleFieldIndexConfig, indexConfig);
    assert(singleFieldIndexConfig);
    auto attrReader = GetAttributeReader(
        singleFieldIndexConfig->GetFieldConfig()->GetFieldName());
    auto compositeReader = DYNAMIC_POINTER_CAST(T, indexReader);
    assert(compositeReader);
    compositeReader->AddAttributeReader(attrReader);
}

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ONLINE_PARTITION_READER_H
