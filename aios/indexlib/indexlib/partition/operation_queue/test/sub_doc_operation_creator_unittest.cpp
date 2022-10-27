#include "indexlib/partition/operation_queue/test/sub_doc_operation_creator_unittest.h"
#include "indexlib/partition/operation_queue/sub_doc_operation_creator.h"
#include "indexlib/partition/operation_queue/sub_doc_operation.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/hash_string.h"


using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SubDocOperationCreatorTest);

namespace {

class MockOperationCreator : public OperationCreator
{
public:
    MockOperationCreator(const config::IndexPartitionSchemaPtr& schema)
        : OperationCreator(schema)
    {}

    MOCK_METHOD2(Create, OperationBase*(const document::NormalDocumentPtr&,
                    autil::mem_pool::Pool*));
};
DEFINE_SHARED_PTR(MockOperationCreator);

};

SubDocOperationCreatorTest::SubDocOperationCreatorTest()
{
}

SubDocOperationCreatorTest::~SubDocOperationCreatorTest()
{
}

void SubDocOperationCreatorTest::CaseSetUp()
{
    string field = "price:int32;pk:int64";
    string index = "pk:primarykey64:pk";
    string attr = "price;pk";
    string summary = "";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            "sub_pk:string;sub_long:uint32;",
            "sub_pk:primarykey64:sub_pk",
            "sub_pk;sub_long;",
            "");
    mSchema->SetSubIndexPartitionSchema(subSchema);
    mSubSchema = subSchema;

    mDoc = DYNAMIC_POINTER_CAST(NormalDocument,
                                DocumentMaker::MakeDeletionDocument("10", 100));
    mSubDoc = DYNAMIC_POINTER_CAST(NormalDocument,
                                   DocumentMaker::MakeDeletionDocument("101", 100));
}

void SubDocOperationCreatorTest::CaseTearDown()
{
}

void SubDocOperationCreatorTest::TestCreateNoOpearation()
{
    MockOperationCreatorPtr mainCreator(new MockOperationCreator(mSchema));
    MockOperationCreatorPtr subCreator(new MockOperationCreator(mSubSchema));
    EXPECT_CALL(*mainCreator, Create(_, _))
        .WillOnce(Return((MockOperation*)(NULL)));
    EXPECT_CALL(*subCreator, Create(_, _))
        .Times(0);

    SubDocOperationCreator creator(mSchema, mainCreator, subCreator);
    OperationBase* operation = creator.Create(mDoc, &mPool);
    ASSERT_FALSE(operation);
}

void SubDocOperationCreatorTest::TestCreateNoMainCreator()
{
    MockOperationCreatorPtr subCreator(new MockOperationCreator(mSubSchema));
    MockOperation* subOperation1 = MakeOperation(30, &mPool);
    MockOperation* subOperation2 = MakeOperation(40, &mPool);

    EXPECT_CALL(*subCreator, Create(_, _))
        .WillOnce(Return(subOperation1))
        .WillOnce(Return(subOperation2));
    SubDocOperationCreator creator(mSchema, OperationCreatorPtr(), subCreator);

    mDoc->AddSubDocument(mSubDoc);
    mDoc->AddSubDocument(mSubDoc);
    mDoc->SetDocOperateType(DELETE_SUB_DOC);
    OperationBase* operation = creator.Create(mDoc, &mPool);
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
    EXPECT_CALL(*mainCreator, Create(_, _))
        .WillOnce(Return(mainOperation));
    EXPECT_CALL(*subCreator, Create(_, _))
        .Times(0);
    SubDocOperationCreator creator(mSchema, mainCreator, subCreator);

    OperationBase* operation = creator.Create(mDoc, &mPool);
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

    EXPECT_CALL(*mainCreator, Create(_, _))
        .WillOnce(Return(mainOperation));
    EXPECT_CALL(*subCreator, Create(_, _))
        .WillOnce(Return(subOperation1))
        .WillOnce(Return(subOperation2));
    SubDocOperationCreator creator(mSchema, mainCreator, subCreator);

    mDoc->AddSubDocument(mSubDoc);
    mDoc->AddSubDocument(mSubDoc);
    OperationBase* operation = creator.Create(mDoc, &mPool);
    ASSERT_TRUE(operation);
    ASSERT_EQ(DELETE_DOC, operation->GetDocOperateType());
    ASSERT_EQ(100, operation->GetTimestamp());

    SubDocOperation* subDocOp = dynamic_cast<SubDocOperation*>(operation);
    ASSERT_TRUE(subDocOp);
    ASSERT_EQ(mainOperation, subDocOp->mMainOperation);
    ASSERT_EQ((size_t)2, subDocOp->mSubOperationCount);
    ASSERT_EQ(subOperation1, subDocOp->mSubOperations[0]);
    ASSERT_EQ(subOperation2, subDocOp->mSubOperations[1]);
}

MockOperation* SubDocOperationCreatorTest::MakeOperation(
        int64_t ts, autil::mem_pool::Pool* pool)
{
    MockOperation *operation = 
        IE_POOL_COMPATIBLE_NEW_CLASS(pool, MockOperation, ts);
    return operation;
}

IE_NAMESPACE_END(operation_queue);

