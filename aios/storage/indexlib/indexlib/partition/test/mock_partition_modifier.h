#ifndef __INDEXLIB_MOCK_PARTITION_MODIFIER_H
#define __INDEXLIB_MOCK_PARTITION_MODIFIER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class MockPartitionModifier : public PartitionModifier
{
public:
    MockPartitionModifier(const config::IndexPartitionSchemaPtr& schema) : PartitionModifier(schema) {}
    ~MockPartitionModifier() {}

public:
    MOCK_METHOD(bool, UpdateDocument, (const document::DocumentPtr&), (override));
    MOCK_METHOD(bool, RemoveDocument, (const document::DocumentPtr&), (override));
    MOCK_METHOD(bool, UpdateIndex, (index::IndexUpdateTermIterator*), (override));
    MOCK_METHOD(bool, RedoIndex, (docid_t docId, const document::ModifiedTokens& modifiedTokens), (override));
    MOCK_METHOD(bool, DedupDocument, (const document::DocumentPtr&), (override));
    MOCK_METHOD(bool, RemoveDocument, (docid_t), (override));
    MOCK_METHOD(void, Dump, (const file_system::DirectoryPtr&, segmentid_t), (override));
    MOCK_METHOD(bool, IsDirty, (), (const, override));
    MOCK_METHOD(bool, UpdateField, (docid_t, fieldid_t, const autil::StringView&, bool isNull), (override));
    MOCK_METHOD(bool, UpdatePackField, (docid_t, packattrid_t, const autil::StringView&), (override));
    MOCK_METHOD(docid_t, GetBuildingSegmentBaseDocId, (), (const, override));
    MOCK_METHOD(size_t, EstimateMaxMemoryUseOfCurrentSegment, (), (const));
    MOCK_METHOD(index::PartitionInfoPtr, GetPartitionInfo, (), (const, override));
    MOCK_METHOD(PartitionModifierDumpTaskItemPtr, GetDumpTaskItem, (const PartitionModifierPtr&), (const, override));

public:
    void SetPrimaryKeyIndexReader(const index::PrimaryKeyIndexReaderPtr& reader) { mPrimaryKeyIndexReader = reader; }
    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyIndexReader() const override { return mPrimaryKeyIndexReader; }

    void Init(const index_base::PartitionDataPtr& partitionData) {}

private:
    index::PrimaryKeyIndexReaderPtr mPrimaryKeyIndexReader;
};

DEFINE_SHARED_PTR(MockPartitionModifier);
}} // namespace indexlib::partition

#endif //__INDEXLIB_MOCK_PARTITION_MODIFIER_H
