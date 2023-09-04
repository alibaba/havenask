#include "indexlib/partition/operation_queue/test/operation_factory_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/remove_operation.h"
#include "indexlib/partition/operation_queue/remove_operation_creator.h"
#include "indexlib/partition/operation_queue/sub_doc_operation.h"
#include "indexlib/partition/operation_queue/sub_doc_operation_creator.h"
#include "indexlib/partition/operation_queue/update_field_operation.h"
#include "indexlib/partition/operation_queue/update_field_operation_creator.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::index;
using namespace indexlib::document;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OperationFactoryTest);

OperationFactoryTest::OperationFactoryTest() {}

OperationFactoryTest::~OperationFactoryTest() {}

void OperationFactoryTest::CaseSetUp() {}

void OperationFactoryTest::CaseTearDown() {}

OperationBase* OperationFactoryTest::CreateOperation(OperationFactory& factory, const NormalDocumentPtr& doc,
                                                     Pool* pool)
{
    OperationBase* operation = nullptr;
    if (!factory.CreateOperation(doc, pool, &operation)) {
        return nullptr;
    }
    return operation;
}

void OperationFactoryTest::TestSimpleProcess()
{
    string field = "string1:string;string2:string;price:int32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    string attribute = "price";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    OperationFactory factory;
    factory.Init(schema);

    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=delete,string1=hello,price=5;"
                       "cmd=update_field,string1=hello,price=6;";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);

    OperationBase* op = CreateOperation(factory, docs[0], docs[0]->GetPool());
    ASSERT_EQ(DELETE_DOC, op->GetDocOperateType());

    op = CreateOperation(factory, docs[1], docs[1]->GetPool());
    ASSERT_EQ(DELETE_DOC, op->GetDocOperateType());

    op = CreateOperation(factory, docs[2], docs[2]->GetPool());
    ASSERT_EQ(UPDATE_FIELD, op->GetDocOperateType());
}

void OperationFactoryTest::TestInit()
{
    {
        // init with single partition
        IndexPartitionSchemaPtr schema = CreateSchema(false);
        OperationFactory factory;
        factory.Init(schema);

        CheckOperationCreator(schema, factory.mRemoveSubOperationCreator, "null");
        CheckOperationCreator(schema, factory.mRemoveOperationCreator, "remove");
        CheckOperationCreator(schema, factory.mUpdateOperationCreator, "update");
    }

    {
        // init with sub doc
        IndexPartitionSchemaPtr schema = CreateSchema(true);
        OperationFactory factory;
        factory.Init(schema);

        CheckOperationCreator(schema, factory.mRemoveOperationCreator, "remove");
        CheckOperationCreator(schema, factory.mRemoveSubOperationCreator, "subDoc:null:remove");
        CheckOperationCreator(schema, factory.mUpdateOperationCreator, "subDoc:update:update");
    }
}

void OperationFactoryTest::TestCreateOperationForAddDocument()
{
    {
        // for single doc
        IndexPartitionSchemaPtr schema = CreateSchema(false);
        OperationFactory factory;
        factory.Init(schema);

        string docString = "cmd=add,pkstr=pk1,text1=m";
        vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);

        OperationBase* oper = CreateOperation(factory, docs[0], &mPool);
        CheckOperationType(oper, "remove");
    }

    {
        // for sub doc
        IndexPartitionSchemaPtr schema = CreateSchema(true);
        OperationFactory factory;
        factory.Init(schema);

        string docString = "cmd=add,pkstr=pk1,text1=m,subpkstr=sub1^sub2,substr1=s^s;"
                           "cmd=add,pkstr=pk2,text1=m;";
        vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);

        OperationBase* oper1 = CreateOperation(factory, docs[0], &mPool);
        OperationBase* oper2 = CreateOperation(factory, docs[1], &mPool);

        CheckOperationType(oper1, "remove");
        CheckOperationType(oper2, "remove");
    }
}

void OperationFactoryTest::TestCreateOperationForDeleteDocument()
{
    {
        // for single doc
        IndexPartitionSchemaPtr schema = CreateSchema(false);
        OperationFactory factory;
        factory.Init(schema);

        string docString = "cmd=delete,pkstr=pk1;"
                           "cmd=delete_sub,pkstr=pk1";
        vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);

        OperationBase* oper = CreateOperation(factory, docs[0], &mPool);
        CheckOperationType(oper, "remove");
        oper = CreateOperation(factory, docs[1], &mPool);
        CheckOperationType(oper, "null");
    }

    {
        // for sub doc
        IndexPartitionSchemaPtr schema = CreateSchema(true);
        OperationFactory factory;
        factory.Init(schema);

        string docString = "cmd=delete,pkstr=pk1;"                        // delete : main Pk
                           "cmd=delete,pkstr=pk1,subpkstr=sub1^sub2;"     // delete: main & sub pk
                           "cmd=delete_sub,pkstr=pk1,subpkstr=sub1^sub2;" // delete_sub: main & sub pk
                           "cmd=delete_sub,subpkstr=sub1^sub2";           // delete_sub: only sub pk
        vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);

        OperationBase* oper0 = CreateOperation(factory, docs[0], &mPool);
        OperationBase* oper1 = CreateOperation(factory, docs[1], &mPool);
        OperationBase* oper2 = CreateOperation(factory, docs[2], &mPool);
        OperationBase* oper3 = CreateOperation(factory, docs[3], &mPool);

        CheckOperationType(oper0, "remove");
        CheckOperationType(oper1, "remove");
        CheckOperationType(oper2, "subDoc:null:remove");
        CheckOperationType(oper3, "null");
    }
}

void OperationFactoryTest::TestCreateOperationForUpdateDocument()
{
    {
        // for single doc
        IndexPartitionSchemaPtr schema = CreateSchema(false);
        OperationFactory factory;
        factory.Init(schema);

        string docString = "cmd=update_field,pkstr=pk1;"
                           "cmd=update_field,pkstr=pk1,long1=123;";
        vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);

        OperationBase* oper = CreateOperation(factory, docs[0], &mPool);
        CheckOperationType(oper, "null");
        oper = CreateOperation(factory, docs[1], &mPool);
        CheckOperationType(oper, "update");
    }

    {
        // for sub doc
        IndexPartitionSchemaPtr schema = CreateSchema(true);
        OperationFactory factory;
        factory.Init(schema);

        string docString = "cmd=update_field,pkstr=pk1,long1=123;"
                           "cmd=update_field,pkstr=pk1,long1=123,subpkstr=sub1^sub2;"
                           "cmd=update_field,pkstr=pk1,subpkstr=sub1^sub2,sub_long=123^345;"
                           "cmd=update_field,pkstr=pk1,long1=123,subpkstr=sub1^sub2,sub_long=123^345;"
                           "cmd=update_field,subpkstr=sub1^sub2,sub_long=123^345;";

        vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);

        OperationBase* oper0 = CreateOperation(factory, docs[0], &mPool);
        OperationBase* oper1 = CreateOperation(factory, docs[1], &mPool);
        OperationBase* oper2 = CreateOperation(factory, docs[2], &mPool);
        OperationBase* oper3 = CreateOperation(factory, docs[3], &mPool);
        OperationBase* oper4 = CreateOperation(factory, docs[4], &mPool);

        CheckOperationType(oper0, "subDoc:update:null");
        CheckOperationType(oper1, "subDoc:update:null");
        CheckOperationType(oper2, "subDoc:null:update");
        CheckOperationType(oper3, "subDoc:update:update");
        CheckOperationType(oper4, "null");
    }
}

IndexPartitionSchemaPtr OperationFactoryTest::CreateSchema(bool hasSub)
{
    string field = "pkstr:string;text1:text;long1:uint32;multi_long:uint32:true;"
                   "updatable_multi_long:uint32:true:true;";
    string index = "pk:primarykey64:pkstr;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "pkstr;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    if (hasSub) {
        IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            "substr1:string;subpkstr:string;sub_long:uint32;", "subindex1:string:substr1;sub_pk:primarykey64:subpkstr",
            "substr1;subpkstr;sub_long;", "");
        schema->SetSubIndexPartitionSchema(subSchema);
        SchemaRewriter::RewriteForSubTable(schema);
    }
    return schema;
}

void OperationFactoryTest::CheckOperationCreator(const IndexPartitionSchemaPtr& schema,
                                                 OperationCreatorPtr operationCreator, const string& typeStr)
{
    vector<string> creatorTypeInfo;
    StringUtil::fromString(typeStr, creatorTypeInfo, ":");

    if (creatorTypeInfo.size() == 1) {
        CheckSingleOperationCreator(schema, operationCreator, creatorTypeInfo[0]);
        return;
    }

    if (creatorTypeInfo.size() == 3) {
        assert(creatorTypeInfo[0] == "subDoc");
        CheckSubDocOperationCreator(schema, operationCreator, creatorTypeInfo[1], creatorTypeInfo[2]);
        return;
    }

    assert(false);
}

void OperationFactoryTest::CheckSingleOperationCreator(const IndexPartitionSchemaPtr& schema,
                                                       OperationCreatorPtr operationCreator, const string& typeStr)
{
    if (typeStr == "null") {
        ASSERT_FALSE(operationCreator);
        return;
    }

    if (typeStr == "update") {
        UpdateFieldOperationCreatorPtr updateOperCreator =
            DYNAMIC_POINTER_CAST(UpdateFieldOperationCreator, operationCreator);
        ASSERT_TRUE(updateOperCreator);
    } else if (typeStr == "remove") {
        RemoveOperationCreatorPtr removeOperCreator = DYNAMIC_POINTER_CAST(RemoveOperationCreator, operationCreator);
        ASSERT_TRUE(removeOperCreator);
    } else {
        assert(false);
    }

    ASSERT_EQ(schema->GetIndexSchema()->GetPrimaryKeyIndexType(), operationCreator->GetPkIndexType());
}

void OperationFactoryTest::CheckSubDocOperationCreator(const IndexPartitionSchemaPtr& schema,
                                                       OperationCreatorPtr operationCreator, const string& mainTypeStr,
                                                       const string& subTypeStr)
{
    ASSERT_TRUE(operationCreator);
    SubDocOperationCreatorPtr subDocOperCreator = DYNAMIC_POINTER_CAST(SubDocOperationCreator, operationCreator);
    ASSERT_TRUE(subDocOperCreator);

    CheckSingleOperationCreator(schema, subDocOperCreator->mMainCreator, mainTypeStr);
    CheckSingleOperationCreator(schema->GetSubIndexPartitionSchema(), subDocOperCreator->mSubCreator, subTypeStr);
}

void OperationFactoryTest::CheckOperationType(OperationBase* oper, const string& typeStr)
{
    vector<string> operTypeInfo;
    StringUtil::fromString(typeStr, operTypeInfo, ":");
    if (operTypeInfo.size() == 1) {
        CheckSingleOperationType(oper, operTypeInfo[0]);
        return;
    }

    if (operTypeInfo.size() == 3) {
        assert(operTypeInfo[0] == "subDoc");
        CheckSubDocOperationType(oper, operTypeInfo[1], operTypeInfo[2]);
        return;
    }
    assert(false);
}

void OperationFactoryTest::CheckSingleOperationType(OperationBase* oper, const string& typeStr)
{
    if (typeStr == "null") {
        ASSERT_FALSE(oper);
        return;
    }

    if (typeStr == "update") {
        UpdateFieldOperation<uint64_t>* updateOper = dynamic_cast<UpdateFieldOperation<uint64_t>*>(oper);
        ASSERT_TRUE(updateOper);
        return;
    }

    if (typeStr == "remove") {
        RemoveOperation<uint64_t>* removeOper = dynamic_cast<RemoveOperation<uint64_t>*>(oper);
        ASSERT_TRUE(removeOper);
        return;
    }
    assert(false);
}

void OperationFactoryTest::CheckSubDocOperationType(OperationBase* oper, const string& mainTypeStr,
                                                    const string& subTypeStr)
{
    SubDocOperation* subDocOper = dynamic_cast<SubDocOperation*>(oper);
    ASSERT_TRUE(subDocOper);

    CheckSingleOperationType(subDocOper->mMainOperation, mainTypeStr);
    for (size_t i = 0; i < subDocOper->mSubOperationCount; i++) {
        CheckSingleOperationType(subDocOper->mSubOperations[i], subTypeStr);
    }

    if (subTypeStr != "null") {
        ASSERT_TRUE(subDocOper->mSubOperationCount > 0);
    }

    if (subTypeStr == "remove") {
        ASSERT_EQ(DELETE_SUB_DOC, subDocOper->mDocOperateType);
    }

    if (subTypeStr == "update") {
        ASSERT_EQ(UPDATE_FIELD, subDocOper->mDocOperateType);
    }
}
}} // namespace indexlib::partition
