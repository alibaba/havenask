#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/partition/operation_queue/update_field_operation.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class UpdateFieldOperationTest : public INDEXLIB_TESTBASE
{
public:
    UpdateFieldOperationTest();
    ~UpdateFieldOperationTest();

    DECLARE_CLASS_NAME(UpdateFieldOperationTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestClone();
    void TestProcess();
    void TestGetMemoryUse();
    void TestSerialize();

private:
    template <typename T>
    void DoTestProcess(bool isNull);

    template <typename T>
    std::shared_ptr<UpdateFieldOperation<T>> MakeOperation(segmentid_t segmentId = INVALID_DOCID, bool isNull = false);

private:
    autil::mem_pool::Pool mPool;
    std::shared_ptr<UpdateFieldOperation<uint64_t>> mOperation;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(UpdateFieldOperationTest, TestClone);
INDEXLIB_UNIT_TEST_CASE(UpdateFieldOperationTest, TestProcess);
INDEXLIB_UNIT_TEST_CASE(UpdateFieldOperationTest, TestGetMemoryUse);
INDEXLIB_UNIT_TEST_CASE(UpdateFieldOperationTest, TestSerialize);

//////////////////////////////////////////////////////////////////////////

namespace {
template <typename T>
class MockPrimaryKeyIndexReader : public index::LegacyPrimaryKeyReader<T>
{
public:
    MOCK_METHOD(docid_t, LookupWithPKHash, (const autil::uint128_t&, future_lite::Executor* executor),
                (const, override));
};

class MockPatchModifier : public partition::PatchModifier
{
public:
    MockPatchModifier(const config::IndexPartitionSchemaPtr& schema) : PatchModifier(schema) {}

    MOCK_METHOD(index::PrimaryKeyIndexReaderPtr, LoadPrimaryKeyIndexReader, (const index_base::PartitionDataPtr&),
                (override));
    MOCK_METHOD(bool, UpdateField, (docid_t, fieldid_t, const autil::StringView&, bool), (override));
    MOCK_METHOD(index::PartitionInfoPtr, GetPartitionInfo, (), (const, override));
};
}; // namespace

template <typename T>
std::shared_ptr<UpdateFieldOperation<T>> UpdateFieldOperationTest::MakeOperation(segmentid_t segmentId, bool isNull)
{
    std::string value = isNull ? "" : "abc";
    autil::mem_pool::Pool* pool = &mPool;
    OperationItem* items = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, OperationItem, 1);
    items[0].first = 3;
    items[0].second = autil::MakeCString(value, pool);
    T hashValue(12345);
    std::shared_ptr<UpdateFieldOperation<T>> operation(new UpdateFieldOperation<T>(10));
    operation->Init(hashValue, items, 1, segmentId);
    return operation;
}

template <typename T>
void UpdateFieldOperationTest::DoTestProcess(bool isNull)
{
    std::shared_ptr<UpdateFieldOperation<T>> operationPtr = MakeOperation<T>(1, isNull);
    config::IndexPartitionSchemaPtr schema(new config::IndexPartitionSchema);
    MockPrimaryKeyIndexReader<T>* mockPkIndexReader = new MockPrimaryKeyIndexReader<T>();
    index::PrimaryKeyIndexReaderPtr pkIndexReader(mockPkIndexReader);
    MockPatchModifier* mockModifier = new MockPatchModifier(schema);

    partition::PartitionModifierPtr modifier(mockModifier);

    EXPECT_CALL(*mockModifier, LoadPrimaryKeyIndexReader(_)).WillOnce(Return(pkIndexReader));

    index_base::PartitionDataPtr simplePartitionData = test::PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM());
    simplePartitionData->CreateNewSegment();
    mockModifier->Init(simplePartitionData);

    index::PartitionInfoPtr partInfo(new index::PartitionInfo);
    std::vector<docid_t> baseDocIds;
    baseDocIds.push_back(0);
    baseDocIds.push_back(10);
    partInfo->SetBaseDocIds(baseDocIds);
    partInfo->TEST_SetTotalDocCount(20);
    partInfo->SetDeletionMapReader(index::DeletionMapReaderPtr(new index::DeletionMapReader()));
    index_base::Version version;
    version.AddSegment(1);
    version.AddSegment(2);
    partInfo->SetVersion(version);
    EXPECT_CALL(*mockModifier, GetPartitionInfo()).WillRepeatedly(Return(partInfo));

    EXPECT_CALL(*mockPkIndexReader, LookupWithPKHash(_, _)).WillOnce(Return(INVALID_DOCID));
    EXPECT_CALL(*mockModifier, UpdateField(_, _, _, _)).Times(0);
    OperationRedoHint hint;

    operationPtr->Process(modifier, hint, nullptr);
    ASSERT_EQ(1u, operationPtr->GetSegmentId());

    // case2: update normal case
    EXPECT_CALL(*mockPkIndexReader, LookupWithPKHash(_, _)).WillOnce(Return(10));
    EXPECT_CALL(*mockModifier, UpdateField(10, 3, operationPtr->mItems[0].second, isNull)).WillOnce(Return(true));

    operationPtr->Process(modifier, hint, nullptr);
    ASSERT_EQ((segmentid_t)1, operationPtr->GetSegmentId());
    modifier.reset();
    simplePartitionData.reset();
}
}} // namespace indexlib::partition
