#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/partition/operation_queue/remove_operation.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class RemoveOperationTest : public INDEXLIB_TESTBASE
{
public:
    RemoveOperationTest();
    ~RemoveOperationTest();

    DECLARE_CLASS_NAME(RemoveOperationTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestProcess();
    void TestClone();
    void TestGetMemoryUse();
    void TestSerialize();

private:
    template <typename T>
    void DoTestProcess();

private:
    autil::mem_pool::Pool mPool;
    std::shared_ptr<RemoveOperation<uint64_t>> mOperation;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RemoveOperationTest, TestProcess);
INDEXLIB_UNIT_TEST_CASE(RemoveOperationTest, TestClone);
INDEXLIB_UNIT_TEST_CASE(RemoveOperationTest, TestGetMemoryUse);
INDEXLIB_UNIT_TEST_CASE(RemoveOperationTest, TestSerialize);

//////////////////////////////////////////////////////////////////////////

namespace {
template <typename T>
class MockPrimaryKeyIndexReader : public index::LegacyPrimaryKeyReader<T>
{
public:
    MOCK_METHOD(docid64_t, LookupWithPKHash, (const autil::uint128_t&, future_lite::Executor* executor),
                (const, override));
};

class MockPatchModifier : public partition::PatchModifier
{
public:
    MockPatchModifier(const config::IndexPartitionSchemaPtr& schema) : PatchModifier(schema) {}

    MOCK_METHOD(index::PrimaryKeyIndexReaderPtr, LoadPrimaryKeyIndexReader, (const index_base::PartitionDataPtr&),
                (override));
    MOCK_METHOD(bool, RemoveDocument, (docid_t), (override));
    MOCK_METHOD(index::PartitionInfoPtr, GetPartitionInfo, (), (const, override));
};
}; // namespace

template <typename T>
void RemoveOperationTest::DoTestProcess()
{
    T hashValue(12345);
    std::shared_ptr<RemoveOperation<T>> operationPtr;
    operationPtr.reset(new RemoveOperation<T>(10));
    operationPtr->Init(hashValue, 1);
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
    index_base::Version version;
    version.AddSegment(1);
    version.AddSegment(2);
    partInfo->SetVersion(version);
    partInfo->TEST_SetTotalDocCount(20);
    EXPECT_CALL(*mockModifier, GetPartitionInfo()).WillRepeatedly(Return(partInfo));

    EXPECT_CALL(*mockPkIndexReader, LookupWithPKHash(_, _)).WillOnce(Return(INVALID_DOCID));
    EXPECT_CALL(*mockModifier, RemoveDocument(INVALID_DOCID)).WillOnce(Return(false));

    OperationRedoHint hint;
    operationPtr->Process(modifier, hint, nullptr);
    ASSERT_EQ(1u, operationPtr->GetSegmentId());

    EXPECT_CALL(*mockPkIndexReader, LookupWithPKHash(_, _)).WillOnce(Return(10));
    EXPECT_CALL(*mockModifier, RemoveDocument(10)).WillOnce(Return(true));

    operationPtr->Process(modifier, hint, nullptr);

    modifier.reset();
    simplePartitionData.reset();
}
}} // namespace indexlib::partition
