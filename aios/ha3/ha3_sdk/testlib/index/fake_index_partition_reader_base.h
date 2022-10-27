#ifndef __INDEXLIB_FAKE_INDEX_PARTITION_READER_BASE_H
#define __INDEXLIB_FAKE_INDEX_PARTITION_READER_BASE_H

#include <indexlib/common_define.h>
#include <indexlib/indexlib.h>
#include <indexlib/partition/index_partition_reader.h>
#include <indexlib/index/partition_info.h>
#include <indexlib/index_base/index_meta/dimension_description.h>
#include <tr1/memory>

IE_NAMESPACE_BEGIN(partition);

class FakeIndexPartitionReaderBase : public IndexPartitionReader
{
public:
    FakeIndexPartitionReaderBase() {}
    ~FakeIndexPartitionReaderBase() {}
public:
    void Open(const index_base::PartitionDataPtr& partitionData)
    {
        return;
    }

    void SetSchema(config::IndexPartitionSchemaPtr &schema) {
        mSchema = schema;
    }

    const index::SummaryReaderPtr &GetSummaryReader() const
    { return _summaryReader; }

    const index::AttributeReaderPtr &GetAttributeReader(
            const std::string& field) const
    { return _attributeReader; }

    index::AttributeReaderPtr GetAttributeReader(
            fieldid_t fieldId) const
    { return index::AttributeReaderPtr(); }

    index::IndexReaderPtr GetIndexReader() const
    { return index::IndexReaderPtr(); }

    const index::IndexReaderPtr &GetIndexReader(
            const std::string& indexName) const
    { return _indexReader; }

    const index::PrimaryKeyIndexReaderPtr &GetPrimaryKeyReader() const
    { return _primaryKeyIndexReader; }

    index_base::Version GetVersion() const
    { return index_base::Version(); }

    const index::DeletionMapReaderPtr &GetDeletionMapReader() const
    {
        return _deletionMapReader;
    }

    /* override*/ index::PartitionInfoPtr GetPartitionInfo() const
    {
        return mPartitionInfo;
    }

    /* override*/ IndexPartitionReaderPtr GetSubPartitionReader() const
    { return mSubIndexPartitionReader; }

    /* override*/ bool GetSortedDocIdRanges(
            const index_base::DimensionDescriptionVector& dimensions,
            const DocIdRange& rangeLimits,
            DocIdRangeVector& resultRanges) const
    { return false; }

    /* override*/ const AccessCounterMap& GetIndexAccessCounters() const
    {
        static AccessCounterMap map;
        return map;
    }

    /* override*/ const AccessCounterMap& GetAttributeAccessCounters() const
    {
        static AccessCounterMap map;
        return map;
    }

    void SetSubIndexPartitionReader(const IndexPartitionReaderPtr& subReader)
    { mSubIndexPartitionReader = subReader; }

    /* override*/ const index::AttributeReaderContainerPtr& GetAttributeReaderContainer() const
    {
        static index::AttributeReaderContainerPtr emptyContainer;
        return emptyContainer;
    }
    index_base::Version GetOnDiskVersion() const override { return {}; }

protected:
    index::PartitionInfoPtr mPartitionInfo;
    IndexPartitionReaderPtr mSubIndexPartitionReader;
    index::SummaryReaderPtr _summaryReader;
    index::AttributeReaderPtr _attributeReader;
    index::IndexReaderPtr _indexReader;
    index::PrimaryKeyIndexReaderPtr _primaryKeyIndexReader;
    index::DeletionMapReaderPtr _deletionMapReader;
    index::PackAttributeReaderPtr _packAttributeReader;
};

DEFINE_SHARED_PTR(FakeIndexPartitionReaderBase);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_FAKE_INDEX_PARTITION_READER_BASE_H
