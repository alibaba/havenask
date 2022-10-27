#ifndef __INDEXLIB_REMOVEOPERATIONTEST_H
#define __INDEXLIB_REMOVEOPERATIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/operation_queue/remove_operation.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/test/partition_data_maker.h"

IE_NAMESPACE_BEGIN(partition);

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
    template<typename T>
    void DoTestProcess();

private:
    autil::mem_pool::Pool mPool;
    std::tr1::shared_ptr<RemoveOperation<uint64_t> > mOperation;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RemoveOperationTest, TestProcess);
INDEXLIB_UNIT_TEST_CASE(RemoveOperationTest, TestClone);
INDEXLIB_UNIT_TEST_CASE(RemoveOperationTest, TestGetMemoryUse);
INDEXLIB_UNIT_TEST_CASE(RemoveOperationTest, TestSerialize);

//////////////////////////////////////////////////////////////////////////

namespace
{
template<typename T>
class MockPrimaryKeyIndexReader : public index::PrimaryKeyIndexReaderTyped<T>
{
public:
    MOCK_CONST_METHOD1(LookupWithPKHash, docid_t(const autil::uint128_t&));
};

class MockPatchModifier : public partition::PatchModifier
{
public:
    MockPatchModifier(const config::IndexPartitionSchemaPtr& schema)
        : PatchModifier(schema)
    {}

    MOCK_METHOD1(LoadPrimaryKeyIndexReader,
                 index::PrimaryKeyIndexReaderPtr(const index_base::PartitionDataPtr&));
    MOCK_METHOD1(RemoveDocument, bool(docid_t));
    MOCK_CONST_METHOD0(GetPartitionInfo, const index::PartitionInfoPtr&());
};
};

template<typename T>
void RemoveOperationTest::DoTestProcess()
{
    T hashValue(12345);
    std::tr1::shared_ptr<RemoveOperation<T> > operationPtr;
    operationPtr.reset(new RemoveOperation<T>(10));
    operationPtr->Init(hashValue, 1);
    config::IndexPartitionSchemaPtr schema(new config::IndexPartitionSchema);
    MockPrimaryKeyIndexReader<T> *mockPkIndexReader = 
        new MockPrimaryKeyIndexReader<T>();
    index::PrimaryKeyIndexReaderPtr pkIndexReader(mockPkIndexReader);
    MockPatchModifier *mockModifier = new MockPatchModifier(schema);

    partition::PartitionModifierPtr modifier(mockModifier);

    EXPECT_CALL(*mockModifier, LoadPrimaryKeyIndexReader(_))
        .WillOnce(Return(pkIndexReader));
    index_base::PartitionDataPtr simplePartitionData = 
        test::PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM());
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
    partInfo->SetTotalDocCount(20);
    EXPECT_CALL(*mockModifier, GetPartitionInfo())
        .WillRepeatedly(ReturnRef(partInfo));

    EXPECT_CALL(*mockPkIndexReader, LookupWithPKHash(_))
        .WillOnce(Return(INVALID_DOCID));
    EXPECT_CALL(*mockModifier, RemoveDocument(INVALID_DOCID))
        .WillOnce(Return(false));
    
    OperationRedoHint hint;
    operationPtr->Process(modifier, hint);
    ASSERT_EQ(INVALID_SEGMENTID, operationPtr->GetSegmentId());

    EXPECT_CALL(*mockPkIndexReader, LookupWithPKHash(_))
        .WillOnce(Return(10));
    EXPECT_CALL(*mockModifier, RemoveDocument(10))
        .WillOnce(Return(true));

    operationPtr->Process(modifier, hint);

    modifier.reset();
    simplePartitionData.reset();
}

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_REMOVEOPERATIONTEST_H
