#include "indexlib/partition/operation_queue/test/sub_doc_operation_creator_unittest.h"

#include "indexlib/index/test/document_maker.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/sub_doc_operation.h"
#include "indexlib/partition/operation_queue/sub_doc_operation_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/HashString.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::test;
using namespace indexlib::document;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SubDocOperationCreatorTest);

namespace {

class MockOperationCreator : public OperationCreator
{
public:
    MockOperationCreator(const config::IndexPartitionSchemaPtr& schema) : OperationCreator(schema) {}

    MOCK_METHOD(bool, Create, (const document::NormalDocumentPtr&, autil::mem_pool::Pool*, OperationBase**),
                (override));
};
DEFINE_SHARED_PTR(MockOperationCreator);

}; // namespace

SubDocOperationCreatorTest::SubDocOperationCreatorTest() {}

SubDocOperationCreatorTest::~SubDocOperationCreatorTest() {}

void SubDocOperationCreatorTest::CaseSetUp()
{
    string field = "price:int32;pk:int64";
    string index = "pk:primarykey64:pk";
    string attr = "price;pk";
    string summary = "";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);
    IndexPartitionSchemaPtr subSchema =
        SchemaMaker::MakeSchema("sub_pk:string;sub_long:uint32;", "sub_pk:primarykey64:sub_pk", "sub_pk;sub_long;", "");
    mSchema->SetSubIndexPartitionSchema(subSchema);
    mSubSchema = subSchema;

    mDoc = DYNAMIC_POINTER_CAST(NormalDocument, DocumentMaker::MakeDeletionDocument("10", 100));
    mSubDoc = DYNAMIC_POINTER_CAST(NormalDocument, DocumentMaker::MakeDeletionDocument("101", 100));
}

void SubDocOperationCreatorTest::CaseTearDown() {}

void SubDocOperationCreatorTest::TestCreateNoOpearation()
{
    MockOperationCreatorPtr mainCreator(new MockOperationCreator(mSchema));
    MockOperationCreatorPtr subCreator(new MockOperationCreator(mSubSchema));
    // EXPECT_CALL(*mainCreator, Create(_, _)).WillOnce(Return((MockOperation*)(NULL)));
    EXPECT_CALL(*mainCreator, Create(_, _, _))
        .WillOnce(DoAll(::testing::SetArgPointee<2>((MockOperation*)(nullptr)), Return(true)));
    EXPECT_CALL(*subCreator, Create(_, _, _)).Times(0);

    SubDocOperationCreator creator(mSchema, mainCreator, subCreator);
    OperationBase* operation = nullptr;
    ASSERT_TRUE(creator.Create(mDoc, &mPool, &operation));
    ASSERT_EQ(operation, nullptr);
}

void SubDocOperationCreatorTest::TestCreateNoMainCreator()
{
    MockOperationCreatorPtr subCreator(new MockOperationCreator(mSubSchema));
    MockOperation* subOperation1 = MakeOperation(30, &mPool);
    MockOperation* subOperation2 = MakeOperation(40, &mPool);

    // EXPECT_CALL(*subCreator, Create(_, _))
    //     .WillOnce(Return(subOperation1))
    //     .WillOnce(Return(subOperation2));
    EXPECT_CALL(*subCreator, Create(_, _, _))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(subOperation1), Return(true)))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(subOperation2), Return(true)));

    SubDocOperationCreator creator(mSchema, OperationCreatorPtr(), subCreator);

    mDoc->AddSubDocument(mSubDoc);
    mDoc->AddSubDocument(mSubDoc);
    mDoc->SetDocOperateType(DELETE_SUB_DOC);
    OperationBase* operation = nullptr;
    ASSERT_TRUE(creator.Create(mDoc, &mPool, &operation));
    ASSERT_TRUE(operation);
    ASSERT_EQ(DELETE_SUB_DOC, operation->GetDocOperateType());
    ASSERT_EQ(100, operation->GetTimestamp());

    SubDocOperation* subDocOp = dynamic_cast<SubDocOperation*>(operation);
    ASSERT_TRUE(subDocOp);
    ASSERT_EQ((OperationBase*)NULL, subDocOp->mMainOperation);
    ASSERT_EQ((size_t)2, subDocOp->mSubOperationCount);
    ASSERT_EQ(subOperation1, subDocOp->mSubOperations[0]);
    ASSERT_EQ(subOperation2, subDocOp->mSubOperations[1]);
}

void SubDocOperationCreatorTest::TestCreateNoSubDoc()
{
    MockOperationCreatorPtr mainCreator(new MockOperationCreator(mSchema));
    MockOperationCreatorPtr subCreator(new MockOperationCreator(mSubSchema));

    MockOperation* mainOperation = MakeOperation(20, &mPool);
    // EXPECT_CALL(*mainCreator, Create(_, _))
    //     .WillOnce(Return(mainOperation));
    EXPECT_CALL(*mainCreator, Create(_, _, _))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(mainOperation), Return(true)));
    EXPECT_CALL(*subCreator, Create(_, _, _)).Times(0);
    SubDocOperationCreator creator(mSchema, mainCreator, subCreator);

    OperationBase* operation = nullptr;
    ASSERT_TRUE(creator.Create(mDoc, &mPool, &operation));
    ASSERT_TRUE(operation);
    ASSERT_EQ(DELETE_DOC, operation->GetDocOperateType());
    ASSERT_EQ(100, operation->GetTimestamp());
}

void SubDocOperationCreatorTest::TestCreateMainAndSub()
{
    MockOperationCreatorPtr mainCreator(new MockOperationCreator(mSchema));
    MockOperationCreatorPtr subCreator(new MockOperationCreator(mSubSchema));
    MockOperation* mainOperation = MakeOperation(20, &mPool);
    MockOperation* subOperation1 = MakeOperation(30, &mPool);
    MockOperation* subOperation2 = MakeOperation(40, &mPool);
    // EXPECT_CALL(*mainCreator, Create(_, _, _))
    //     .WillOnce(Return(mainOperation));
    // EXPECT_CALL(*subCreator, Create(_, _))
    //     .WillOnce(Return(subOperation1))
    //     .WillOnce(Return(subOperation2));
    EXPECT_CALL(*mainCreator, Create(_, _, _))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(mainOperation), Return(true)));
    EXPECT_CALL(*subCreator, Create(_, _, _))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(subOperation1), Return(true)))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(subOperation2), Return(true)));
    SubDocOperationCreator creator(mSchema, mainCreator, subCreator);

    mDoc->AddSubDocument(mSubDoc);
    mDoc->AddSubDocument(mSubDoc);
    OperationBase* operation = nullptr;
    ASSERT_TRUE(creator.Create(mDoc, &mPool, &operation));
    ASSERT_NE(operation, nullptr);
    ASSERT_EQ(DELETE_DOC, operation->GetDocOperateType());
    ASSERT_EQ(100, operation->GetTimestamp());

    SubDocOperation* subDocOp = dynamic_cast<SubDocOperation*>(operation);
    ASSERT_TRUE(subDocOp);
    ASSERT_EQ(mainOperation, subDocOp->mMainOperation);
    ASSERT_EQ((size_t)2, subDocOp->mSubOperationCount);
    ASSERT_EQ(subOperation1, subDocOp->mSubOperations[0]);
    ASSERT_EQ(subOperation2, subDocOp->mSubOperations[1]);
}

MockOperation* SubDocOperationCreatorTest::MakeOperation(int64_t ts, autil::mem_pool::Pool* pool)
{
    MockOperation* operation = IE_POOL_COMPATIBLE_NEW_CLASS(pool, MockOperation, ts);
    return operation;
}
}} // namespace indexlib::partition
