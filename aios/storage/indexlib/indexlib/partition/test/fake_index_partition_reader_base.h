#ifndef __INDEXLIB_FAKE_INDEX_PARTITION_READER_BASE_H
#define __INDEXLIB_FAKE_INDEX_PARTITION_READER_BASE_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_container.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/table/normal_table/DimensionDescription.h"

namespace indexlib { namespace partition {

class FakeIndexPartitionReaderBase : public IndexPartitionReader
{
public:
    FakeIndexPartitionReaderBase() {}
    FakeIndexPartitionReaderBase(config::IndexPartitionSchemaPtr schema, versionid_t fakeIncVersionId = 0)
        : IndexPartitionReader(schema)
        , mFakeIncVersionId(fakeIncVersionId)
    {
    }
    ~FakeIndexPartitionReaderBase() {}

public:
    void Open(const index_base::PartitionDataPtr& partitionData) override { return; }

    const index::SummaryReaderPtr& GetSummaryReader() const override { return RET_EMPTY_SUMMARY_READER; }

    const index::SourceReaderPtr& GetSourceReader() const override { return RET_EMPTY_SOURCE_READER; }

    const index::AttributeReaderPtr& GetAttributeReader(const std::string& field) const override
    {
        return index::AttributeReaderContainer::RET_EMPTY_ATTR_READER;
    }

    virtual const index::PackAttributeReaderPtr& GetPackAttributeReader(const std::string& packAttrName) const override
    {
        return index::AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER;
    }

    virtual const index::PackAttributeReaderPtr& GetPackAttributeReader(packattrid_t packAttrId) const override
    {
        return index::AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER;
    }

    virtual const index::AttributeReaderContainerPtr& GetAttributeReaderContainer() const override
    {
        return index::AttributeReaderContainer::RET_EMPTY_ATTR_READER_CONTAINER;
    }

    index::InvertedIndexReaderPtr GetInvertedIndexReader() const override { return index::InvertedIndexReaderPtr(); }

    const index::InvertedIndexReaderPtr& GetInvertedIndexReader(const std::string& indexName) const override
    {
        return RET_EMPTY_INDEX_READER;
    }

    const index::InvertedIndexReaderPtr& GetInvertedIndexReader(indexid_t indexId) const override
    {
        return RET_EMPTY_INDEX_READER;
    }

    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyReader() const override { return RET_EMPTY_PRIMARY_KEY_READER; }

    index_base::Version GetVersion() const override { return index_base::Version(); }
    index_base::Version GetOnDiskVersion() const override { return index_base::Version(); }

    const index::DeletionMapReaderPtr& GetDeletionMapReader() const override { return RET_EMPTY_DELETIONMAP_READER; }

    index::PartitionInfoPtr GetPartitionInfo() const override { return mPartitionInfo; }

    IndexPartitionReaderPtr GetSubPartitionReader() const override { return mSubIndexPartitionReader; }

    bool GetSortedDocIdRanges(const table::DimensionDescriptionVector& dimensions, const DocIdRange& rangeLimits,
                              DocIdRangeVector& resultRanges) const override
    {
        return false;
    }

    const AccessCounterMap& GetIndexAccessCounters() const override
    {
        static AccessCounterMap map;
        return map;
    }

    const AccessCounterMap& GetAttributeAccessCounters() const override
    {
        static AccessCounterMap map;
        return map;
    }
    virtual versionid_t GetIncVersionId() const override { return mFakeIncVersionId; }
    void SetSubIndexPartitionReader(const IndexPartitionReaderPtr& subReader) { mSubIndexPartitionReader = subReader; }
    void SetSchema(const config::IndexPartitionSchemaPtr& schema)
    {
        mSchema = schema;
        mTabletSchema = std::make_shared<indexlib::config::LegacySchemaAdapter>(schema);
    }

protected:
    index::PartitionInfoPtr mPartitionInfo;
    IndexPartitionReaderPtr mSubIndexPartitionReader;
    versionid_t mFakeIncVersionId;
};

DEFINE_SHARED_PTR(FakeIndexPartitionReaderBase);
}} // namespace indexlib::partition

#endif //__INDEXLIB_FAKE_INDEX_PARTITION_READER_BASE_H
