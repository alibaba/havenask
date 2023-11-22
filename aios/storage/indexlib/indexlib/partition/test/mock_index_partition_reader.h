#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/table/normal_table/DimensionDescription.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class MockIndexPartitionReader : public partition::OnlinePartitionReader
{
public:
    MockIndexPartitionReader()
        : partition::OnlinePartitionReader(config::IndexPartitionOptions(), config::IndexPartitionSchemaPtr(),
                                           util::SearchCachePartitionWrapperPtr())
    {
    }
    ~MockIndexPartitionReader() {}

public:
    MOCK_METHOD(void, Open, (const index_base::PartitionDataPtr& partitionData), (override));
    MOCK_METHOD(const index::SummaryReaderPtr&, GetSummaryReader, (), (const, override));
    MOCK_METHOD(const index::AttributeReaderPtr&, GetAttributeReader, (const std::string& field), (const, override));
    MOCK_METHOD(const index::AttributeReaderPtr&, GetAttributeReader, (fieldid_t fieldId), (const));
    MOCK_METHOD(index::InvertedIndexReaderPtr, GetInvertedIndexReader, (), (const, override));
    MOCK_METHOD(const index::InvertedIndexReaderPtr&, GetInvertedIndexReader, (const std::string& indexName),
                (const, override));
    MOCK_METHOD(const index::InvertedIndexReaderPtr&, GetInvertedIndexReader, (indexid_t indexId), (const, override));
    MOCK_METHOD(const index::PrimaryKeyIndexReaderPtr&, GetPrimaryKeyReader, (), (const, override));
    MOCK_METHOD(index_base::Version, GetVersion, (), (const, override));
    MOCK_METHOD(const index::DeletionMapReaderPtr&, GetDeletionMapReader, (), (const, override));
    MOCK_METHOD(IndexPartitionReaderPtr, GetSubPartitionReader, (), (const, override));
    MOCK_METHOD(index::PartitionInfoPtr, GetPartitionInfo, (), (const, override));
    MOCK_METHOD(bool, GetSortedDocIdRanges,
                (const table::DimensionDescriptionVector& dimensions, const DocIdRange& rangeLimits,
                 DocIdRangeVector& resultRanges),
                (const, override));
    MOCK_METHOD(const AccessCounterMap&, GetIndexAccessCounters, (), (const, override));
    MOCK_METHOD(const AccessCounterMap&, GetAttributeAccessCounters, (), (const, override));
    MOCK_METHOD(std::shared_ptr<indexlibv2::framework::VersionDeployDescription>, GetVersionDeployDescription, (),
                (const, override));

public:
    void AddSwitchRtSegId(segmentid_t segId) { mSwitchRtSegIds.push_back(segId); }
};

DEFINE_SHARED_PTR(MockIndexPartitionReader);
}} // namespace indexlib::partition
