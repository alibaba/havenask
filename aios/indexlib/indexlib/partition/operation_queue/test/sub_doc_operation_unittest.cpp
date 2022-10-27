#include "indexlib/partition/operation_queue/test/sub_doc_operation_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/document_creator.h"

#include "indexlib/partition/operation_queue/operation_factory.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SubDocOperationTest);

SubDocOperationTest::SubDocOperationTest()
{
}

SubDocOperationTest::~SubDocOperationTest()
{
}

void SubDocOperationTest::CaseSetUp()
{
}

void SubDocOperationTest::CaseTearDown()
{
}

void SubDocOperationTest::TestClone()
{    
    MockOperation* mainOperation = MakeMockOperation(10, &mPool);
    MockOperation* subOperation1 = MakeMockOperation(10, &mPool);
    MockOperation* subOperation2 = MakeMockOperation(10, &mPool);
    OperationBase** subOperations =
        IE_POOL_COMPATIBLE_NEW_VECTOR((&mPool), OperationBase*, 2);
    subOperations[0] = subOperation1;
    subOperations[1] = subOperation2;

    SubDocOperation op(20, it_primarykey64, it_primarykey64);
    op.Init(DELETE_DOC, mainOperation, subOperations, 2);

    OperationBase* clonedOp = op.Clone(&mPool);
    SubDocOperation* clonedSubOp = dynamic_cast<SubDocOperation*>(clonedOp);
    ASSERT_TRUE(clonedSubOp);
    ASSERT_EQ((int64_t)20, clonedSubOp->GetTimestamp());

    ASSERT_EQ(10, clonedSubOp->mMainOperation->GetTimestamp());    
    ASSERT_EQ((size_t)2, clonedSubOp->mSubOperationCount);
    ASSERT_EQ(10, clonedSubOp->mSubOperations[0]->GetTimestamp());
    ASSERT_EQ(10, clonedSubOp->mSubOperations[1]->GetTimestamp());
}

void SubDocOperationTest::TestGetMemoryUse()
{
    MockOperation* mainOperation = MakeMockOperation(10, &mPool);
    EXPECT_CALL(*mainOperation, GetMemoryUse())
        .WillRepeatedly(Return(100));
    
    #define COUNT 10
    OperationBase* subOperations[COUNT];
    for (size_t i = 1; i <= COUNT; i++)
    {
        MockOperation* subOperation = MakeMockOperation(10, &mPool);
        EXPECT_CALL(*subOperation, GetMemoryUse())
            .WillRepeatedly(Return(i));

        subOperations[i - 1] = subOperation;
    }

    SubDocOperation subDocOperation(10, it_primarykey64, it_primarykey64);
    subDocOperation.Init(UNKNOWN_OP, mainOperation, subOperations, COUNT);
    size_t expectSize = 100 + (1 + COUNT) * COUNT / 2 
                        + sizeof(SubDocOperation) + sizeof(subOperations);
    ASSERT_EQ(expectSize, subDocOperation.GetMemoryUse());
}

void SubDocOperationTest::TestSerialize()
{
    string field = "pk:string:pk;price:uint32";
    string index = "pk:primarykey64:pk;";
    string attr = "price;";
    IndexPartitionSchemaPtr mSchema =
        SchemaMaker::MakeSchema(field, index, attr, "");
    string subfield = "sub_pk:string;sub_price:uint32;";
    string subindex = "sub_pk:primarykey64:sub_pk;";
    string subattr = "sub_pk;sub_price";

    IndexPartitionSchemaPtr subSchema =
        SchemaMaker::MakeSchema(subfield, subindex, subattr, "");
    mSchema->SetSubIndexPartitionSchema(subSchema);

    string docString = "cmd=delete_sub,pk=hello,sub_pk=hello1;"
        "cmd=update_field,pk=kitty,price=2,sub_pk=kitty1,sub_price=3;";

    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(mSchema, docString);

    char buffer[1024];
    OperationFactory opFactory;
    Pool pool;
    opFactory.Init(mSchema);
    char* cur = buffer;
    for (size_t i = 0; i < docs.size(); ++i)
    {
        OperationBase* op = opFactory.CreateOperation(docs[i], &pool);
        cur += op->Serialize(cur, 1024 - (cur - buffer));
    }
    
    SubDocOperation op1(0, it_primarykey64, it_primarykey64);
    cur = buffer;
    ASSERT_TRUE(op1.Load(&pool, cur));
    ASSERT_EQ(DELETE_SUB_DOC, op1.GetDocOperateType());
    ASSERT_FALSE(op1.mMainOperation);
    ASSERT_EQ(size_t(1), op1.mSubOperationCount);
    ASSERT_TRUE(op1.mSubOperations[0]);
    ASSERT_EQ(DELETE_DOC, op1.mSubOperations[0]->GetDocOperateType());
    
    SubDocOperation op2(0, it_primarykey64, it_primarykey64);
    ASSERT_TRUE(op2.Load(&pool, cur));
    ASSERT_EQ(UPDATE_FIELD, op2.GetDocOperateType());
    ASSERT_TRUE(op2.mMainOperation);
    ASSERT_EQ(UPDATE_FIELD, op2.mMainOperation->GetDocOperateType());
    ASSERT_EQ(size_t(1), op2.mSubOperationCount);
    ASSERT_TRUE(op2.mSubOperations[0]);
    ASSERT_EQ(UPDATE_FIELD, op2.mSubOperations[0]->GetDocOperateType());

    ASSERT_EQ(op1.GetSerializeSize() + op2.GetSerializeSize(),
              size_t(cur - buffer));
}

MockOperation* SubDocOperationTest::MakeMockOperation(int64_t ts, Pool* pool)
{
    MockOperation *operation = 
        IE_POOL_COMPATIBLE_NEW_CLASS(pool, MockOperation, ts);
    return operation;
}

IE_NAMESPACE_END(partition);

