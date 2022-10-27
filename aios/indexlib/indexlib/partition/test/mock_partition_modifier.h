#ifndef __INDEXLIB_MOCK_PARTITION_MODIFIER_H
#define __INDEXLIB_MOCK_PARTITION_MODIFIER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/modifier/partition_modifier.h"

IE_NAMESPACE_BEGIN(partition);

class MockPartitionModifier : public PartitionModifier
{
public:
    MockPartitionModifier(const config::IndexPartitionSchemaPtr& schema)
        : PartitionModifier(schema)
    { }

public:
    MOCK_METHOD1(UpdateDocument, bool(const document::DocumentPtr&));
    MOCK_METHOD1(RemoveDocument, bool(const document::DocumentPtr&));
    MOCK_METHOD1(DedupDocument, bool(const document::DocumentPtr&));
    MOCK_METHOD1(RemoveDocument, bool(docid_t));
    MOCK_METHOD2(Dump, void(const file_system::DirectoryPtr& , segmentid_t));
    MOCK_CONST_METHOD0(IsDirty, bool());
    MOCK_METHOD3(UpdateField, bool(docid_t, fieldid_t, const autil::ConstString&));
    MOCK_METHOD3(UpdatePackField, bool(docid_t, packattrid_t, const autil::ConstString&));
    MOCK_CONST_METHOD0(EstimateMaxMemoryUseOfCurrentSegment, size_t());
    MOCK_CONST_METHOD0(GetPartitionInfo, index::PartitionInfoPtr& ());

public:
    void SetPrimaryKeyIndexReader(const index::PrimaryKeyIndexReaderPtr& reader)
    { mPrimaryKeyIndexReader = reader; }
    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyIndexReader() const override
    { return mPrimaryKeyIndexReader; }

    void Init(const index_base::PartitionDataPtr& partitionData) {}

private:
    index::PrimaryKeyIndexReaderPtr mPrimaryKeyIndexReader;
};

DEFINE_SHARED_PTR(MockPartitionModifier);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MOCK_PARTITION_MODIFIER_H
