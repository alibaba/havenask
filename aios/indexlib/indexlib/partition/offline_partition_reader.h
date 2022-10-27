#ifndef __INDEXLIB_OFFLINE_PARTITION_READER_H
#define __INDEXLIB_OFFLINE_PARTITION_READER_H

#include <tr1/memory>
#include <map>
#include <autil/AtomicCounter.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/online_partition_reader.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(index, SummaryReader);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);

IE_NAMESPACE_BEGIN(partition);

class OfflinePartitionReader : public OnlinePartitionReader
{
public:
    OfflinePartitionReader(const config::IndexPartitionOptions& options,
                           const config::IndexPartitionSchemaPtr& schema,
                           const util::SearchCachePartitionWrapperPtr& searchCache =
                           util::SearchCachePartitionWrapperPtr());
    ~OfflinePartitionReader() {}
public:
    void Open(const index_base::PartitionDataPtr& partitionData) override;

    IndexPartitionReaderPtr GetSubPartitionReader() const override
    {
        return IndexPartitionReaderPtr();
    }

    bool GetSortedDocIdRanges(
            const std::vector<index_base::DimensionDescriptionPtr>& dimensions,
            const DocIdRange& rangeLimits,
            DocIdRangeVector& resultRanges) const override
    { assert(false); return false; }

    const index::SummaryReaderPtr& GetSummaryReader() const override;

    const index::AttributeReaderPtr& GetAttributeReader(const std::string& field) const override;

    const index::PackAttributeReaderPtr& GetPackAttributeReader(
            const std::string& packAttrName) const override;

    const index::PackAttributeReaderPtr& GetPackAttributeReader(
            packattrid_t packAttrId) const override;

    index::IndexReaderPtr GetIndexReader() const override;
    const index::IndexReaderPtr& GetIndexReader(const std::string& indexName) const override;
    const index::IndexReaderPtr& GetIndexReader(indexid_t indexId) const override;

    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyReader() const override;

private:
    const index::AttributeReaderPtr& InitOneFieldAttributeReader(const std::string& field);

private:
    mutable autil::RecursiveThreadMutex mLock;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflinePartitionReader);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OFFLINE_PARTITION_READER_H
