#pragma once

#include <string>

#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/table/normal_table/DimensionDescription.h"

namespace indexlib {
namespace partition {

class FakeIndexPartitionReaderBase : public IndexPartitionReader {
public:
    FakeIndexPartitionReaderBase() {}
    ~FakeIndexPartitionReaderBase() {}

public:
    void Open(const index_base::PartitionDataPtr &partitionData) override {
        return;
    }

    void SetSchema(config::IndexPartitionSchemaPtr &schema) {
        mSchema = schema;
        mTabletSchema = std::make_shared<indexlib::config::LegacySchemaAdapter>(schema);
    }

    const indexlib::index::SummaryReaderPtr &GetSummaryReader() const override {
        return _summaryReader;
    }

    const indexlib::index::SourceReaderPtr &GetSourceReader() const override {
        return _sourceReader;
    }

    const indexlib::index::AttributeReaderPtr &
    GetAttributeReader(const std::string &field) const override {
        return _attributeReader;
    }

    indexlib::index::AttributeReaderPtr GetAttributeReader(fieldid_t fieldId) const {
        return indexlib::index::AttributeReaderPtr();
    }

    indexlib::index::InvertedIndexReaderPtr GetInvertedIndexReader() const override {
        return indexlib::index::InvertedIndexReaderPtr();
    }

    const indexlib::index::InvertedIndexReaderPtr &
    GetInvertedIndexReader(const std::string &indexName) const override {
        return _indexReader;
    }

    const indexlib::index::PrimaryKeyIndexReaderPtr &GetPrimaryKeyReader() const override {
        return _primaryKeyIndexReader;
    }

    index_base::Version GetVersion() const override {
        return index_base::Version();
    }

    const indexlib::index::DeletionMapReaderPtr &GetDeletionMapReader() const override {
        return _deletionMapReader;
    }

    /* override*/ indexlib::index::PartitionInfoPtr GetPartitionInfo() const override {
        return mPartitionInfo;
    }

    /* override*/ IndexPartitionReaderPtr GetSubPartitionReader() const override {
        return mSubIndexPartitionReader;
    }

    /* override*/ bool
    GetSortedDocIdRanges(const indexlib::table::DimensionDescriptionVector &dimensions,
                         const indexlib::DocIdRange &rangeLimits,
                         indexlib::DocIdRangeVector &resultRanges) const override {
        return false;
    }

    /* override*/ const AccessCounterMap &GetIndexAccessCounters() const override {
        static AccessCounterMap map;
        return map;
    }

    /* override*/ const AccessCounterMap &GetAttributeAccessCounters() const override {
        static AccessCounterMap map;
        return map;
    }

    void SetSubIndexPartitionReader(const IndexPartitionReaderPtr &subReader) {
        mSubIndexPartitionReader = subReader;
    }

    /* override*/ const indexlib::index::AttributeReaderContainerPtr &
    GetAttributeReaderContainer() const override {
        static indexlib::index::AttributeReaderContainerPtr emptyContainer;
        return emptyContainer;
    }
    index_base::Version GetOnDiskVersion() const override {
        return {};
    }

protected:
    indexlib::index::PartitionInfoPtr mPartitionInfo;
    IndexPartitionReaderPtr mSubIndexPartitionReader;
    indexlib::index::SummaryReaderPtr _summaryReader;
    indexlib::index::SourceReaderPtr _sourceReader;
    indexlib::index::AttributeReaderPtr _attributeReader;
    indexlib::index::InvertedIndexReaderPtr _indexReader;
    indexlib::index::PrimaryKeyIndexReaderPtr _primaryKeyIndexReader;
    indexlib::index::DeletionMapReaderPtr _deletionMapReader;
    indexlib::index::PackAttributeReaderPtr _packAttributeReader;
};

DEFINE_SHARED_PTR(FakeIndexPartitionReaderBase);

} // namespace partition
} // namespace indexlib
