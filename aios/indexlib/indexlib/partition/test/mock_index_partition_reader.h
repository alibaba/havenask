#ifndef __INDEXLIB_MOCK_INDEX_PARTITION_READER_H
#define __INDEXLIB_MOCK_INDEX_PARTITION_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/index_base/index_meta/dimension_description.h"

IE_NAMESPACE_BEGIN(partition);

class MockIndexPartitionReader : public partition::OnlinePartitionReader
{
public:
    MockIndexPartitionReader()
        : partition::OnlinePartitionReader(
                config::IndexPartitionOptions(),
                config::IndexPartitionSchemaPtr(),
                util::SearchCachePartitionWrapperPtr())
    {}
    ~MockIndexPartitionReader() {}
public:
    MOCK_METHOD1(Open, void(const index_base::PartitionDataPtr& partitionData));
    MOCK_CONST_METHOD0(GetSummaryReader, const index::SummaryReaderPtr&());
    MOCK_CONST_METHOD1(GetAttributeReader, const index::AttributeReaderPtr&(const std::string& field));
    MOCK_CONST_METHOD1(GetAttributeReader, const index::AttributeReaderPtr&(fieldid_t fieldId));
    MOCK_CONST_METHOD0(GetIndexReader, index::IndexReaderPtr());
    MOCK_CONST_METHOD1(GetIndexReader, const index::IndexReaderPtr&(const std::string& indexName));
    MOCK_CONST_METHOD1(GetIndexReader, const index::IndexReaderPtr&(indexid_t indexId));
    MOCK_CONST_METHOD0(GetPrimaryKeyReader, const index::PrimaryKeyIndexReaderPtr&());
    MOCK_CONST_METHOD0(GetVersion, index_base::Version());
    MOCK_CONST_METHOD0(GetDeletionMapReader, const index::DeletionMapReaderPtr&());
    MOCK_CONST_METHOD0(GetSubPartitionReader, IndexPartitionReaderPtr());
    MOCK_CONST_METHOD0(GetPartitionInfo, index::PartitionInfoPtr());
    MOCK_CONST_METHOD3(GetSortedDocIdRanges, bool(
                           const index_base::DimensionDescriptionVector& dimensions,
                           const DocIdRange& rangeLimits, DocIdRangeVector& resultRanges));
    MOCK_CONST_METHOD0(GetIndexAccessCounters, const AccessCounterMap&());
    MOCK_CONST_METHOD0(GetAttributeAccessCounters, const AccessCounterMap&());

public:
    void AddSwitchRtSegId(segmentid_t segId)
    { mSwitchRtSegIds.push_back(segId); }
    
};

DEFINE_SHARED_PTR(MockIndexPartitionReader);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MOCK_INDEX_PARTITION_READER_H
