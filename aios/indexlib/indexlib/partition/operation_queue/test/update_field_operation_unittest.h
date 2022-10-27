#ifndef __INDEXLIB_UPDATEFIELDOPERATIONTEST_H
#define __INDEXLIB_UPDATEFIELDOPERATIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

#include "indexlib/partition/operation_queue/update_field_operation.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"

IE_NAMESPACE_BEGIN(partition);

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
    template<typename T>
    void DoTestProcess();

    template<typename T>
    std::tr1::shared_ptr<UpdateFieldOperation<T> > MakeOperation(
            segmentid_t segmentId = INVALID_DOCID);

private:
    autil::mem_pool::Pool mPool;
    std::tr1::shared_ptr<UpdateFieldOperation<uint64_t> > mOperation;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(UpdateFieldOperationTest, TestClone);
INDEXLIB_UNIT_TEST_CASE(UpdateFieldOperationTest, TestProcess);
INDEXLIB_UNIT_TEST_CASE(UpdateFieldOperationTest, TestGetMemoryUse);
INDEXLIB_UNIT_TEST_CASE(UpdateFieldOperationTest, TestSerialize);

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
    MOCK_METHOD3(UpdateField, bool(docid_t, fieldid_t, const autil::ConstString&));
    MOCK_CONST_METHOD0(GetPartitionInfo, const index::PartitionInfoPtr&());

};
};

template<typename T>
std::tr1::shared_ptr<UpdateFieldOperation<T> > UpdateFieldOperationTest::MakeOperation(
        segmentid_t segmentId)
{
    std::string value = "abc";
    autil::mem_pool::Pool* pool = &mPool;
    OperationItem* items = IE_POOL_COMPATIBLE_NEW_VECTOR(
            pool, OperationItem, 1);
    items[0].first = 3;
    items[0].second = autil::ConstString(value, pool);
    T hashValue(12345);
    std::tr1::shared_ptr<UpdateFieldOperation<T> > operation(
        new UpdateFieldOperation<T>(10));
    operation->Init(hashValue, items, 1, segmentId);
    return operation;
}

template<typename T>
void UpdateFieldOperationTest::DoTestProcess()
{
    std::tr1::shared_ptr<UpdateFieldOperation<T> > operationPtr = 
        MakeOperation<T>(1);
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
    partInfo->SetTotalDocCount(20);
    partInfo->SetDeletionMapReader(index::DeletionMapReaderPtr(new index::DeletionMapReader()));
    index_base::Version version;
    version.AddSegment(1);
    version.AddSegment(2);
    partInfo->SetVersion(version);
    EXPECT_CALL(*mockModifier, GetPartitionInfo())
        .WillRepeatedly(ReturnRef(partInfo));

    EXPECT_CALL(*mockPkIndexReader, LookupWithPKHash(_))
        .WillOnce(Return(INVALID_DOCID));
    EXPECT_CALL(*mockModifier, UpdateField(_,_,_))
        .Times(0);
    OperationRedoHint hint;

    operationPtr->Process(modifier, hint);
    ASSERT_EQ(INVALID_SEGMENTID, operationPtr->GetSegmentId());
    
    //case2: update normal case
    EXPECT_CALL(*mockPkIndexReader, LookupWithPKHash(_))
        .WillOnce(Return(10));
    EXPECT_CALL(*mockModifier, UpdateField(10,3,operationPtr->mItems[0].second))
        .WillOnce(Return(true));

    operationPtr->Process(modifier, hint);
    ASSERT_EQ((segmentid_t)2, operationPtr->GetSegmentId());
    modifier.reset();
    simplePartitionData.reset();
}

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_UPDATEFIELDOPERATIONTEST_H
