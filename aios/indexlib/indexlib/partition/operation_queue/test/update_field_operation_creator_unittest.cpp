#include <autil/LongHashValue.h>
#include "indexlib/partition/operation_queue/test/update_field_operation_creator_unittest.h"
#include "indexlib/partition/operation_queue/update_field_operation.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/util/hash_string.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, UpdateFieldOperationCreatorTest);

UpdateFieldOperationCreatorTest::UpdateFieldOperationCreatorTest()
{
}

UpdateFieldOperationCreatorTest::~UpdateFieldOperationCreatorTest()
{
}

void UpdateFieldOperationCreatorTest::CaseSetUp()
{
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(mSchema,
            "title:text;price:int32;sale:int32;nid:int64", // this is field schema
            "nid:primarykey64:nid;title:text:title", // this is index schema
            "price;sale;nid", // this is attribute schema
            "" );// this is summary schema

    string docStr = "{ 0,"
                    "[price: (3)],"                                     \
                    "[nid: (1)],"                                       \
                    "[sale: (10)]"                                      \
                    "}";

    DocumentMaker::MockIndexPart answer;
    mDoc = DYNAMIC_POINTER_CAST(NormalDocument, DocumentMaker::MakeDocument(
                    0, docStr, mSchema, answer));
    mDoc->SetTimestamp(10);

}

void UpdateFieldOperationCreatorTest::CaseTearDown()
{
}

void UpdateFieldOperationCreatorTest::TestCreateOperationItems()
{
    {
        //check invalid field
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, 
                "nid:int64;price:string;sale:int32", 
                "nid:primarykey64:nid", "price;sale", "");

        UpdateFieldOperationCreator creator(schema);
        AttributeDocumentPtr attrDoc = mDoc->GetAttributeDocument();
        uint32_t itemCount = 0; 
        OperationItem* items = creator.CreateOperationItems(&mPool, attrDoc, itemCount);
        ASSERT_EQ((uint32_t)0, itemCount);
        ASSERT_TRUE(items == NULL);
    }

    {
        UpdateFieldOperationCreator creator(mSchema);
        //check ignore fields: pk, price:empty field value
        AttributeDocumentPtr attrDoc = mDoc->GetAttributeDocument();
        ConstString emptyValue;
        attrDoc->SetField(1, emptyValue);
        uint32_t itemCount = 0; 
        OperationItem* items = creator.CreateOperationItems(&mPool, attrDoc, itemCount);
        ASSERT_EQ((uint32_t)1, itemCount);
        ASSERT_TRUE(items != NULL);
    }
}

void UpdateFieldOperationCreatorTest::TestCreate()
{    
    {
        //check no attribute doc
        NormalDocumentPtr doc(new NormalDocument);
        IndexDocumentPtr indexDoc(new IndexDocument(doc->GetPool()));
        indexDoc->SetPrimaryKey("abc");
        doc->SetIndexDocument(indexDoc);

        UpdateFieldOperationCreator creator(mSchema);
        OperationBase* operation = creator.Create(doc, &mPool);        
        ASSERT_EQ(NULL, operation);
    }

    {
        // no attribute schema
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema,
                "nid:int64", "nid:primarykey64:nid", "", "");

        UpdateFieldOperationCreator creator(schema);
        OperationBase* operation = creator.Create(mDoc, &mPool);
        ASSERT_EQ(NULL, operation);
    }

    {
        //normal case
        UpdateFieldOperationCreator creator(mSchema);
        OperationBase* operation = creator.Create(mDoc, &mPool);
        UpdateFieldOperation<uint64_t>* updateOperation = 
            dynamic_cast<UpdateFieldOperation<uint64_t>*>(operation);
        ASSERT_TRUE(updateOperation);

        HashString hasher;
        uint64_t hashValue;
        const IndexDocumentPtr &indexDoc = mDoc->GetIndexDocument();
        const string &pkString = indexDoc->GetPrimaryKey();
        hasher.Hash(hashValue, pkString.c_str(), pkString.size());

        uint128_t hashValue128;
        hashValue128.value[1] = hashValue;
        ASSERT_EQ(hashValue128, updateOperation->mPkHash);
        ASSERT_EQ((int64_t)10, updateOperation->mTimestamp);
        ASSERT_EQ((size_t)2, updateOperation->mItemSize);
        ASSERT_EQ((fieldid_t)1, updateOperation->mItems[0].first);
        ASSERT_EQ((fieldid_t)2, updateOperation->mItems[1].first);

        int32_t price = *(int32_t*)updateOperation->mItems[0].second.data();
        int32_t sale = *(int32_t*)updateOperation->mItems[1].second.data();
        ASSERT_EQ((int32_t)3, price);
        ASSERT_EQ((int32_t)10, sale);
    }
}

IE_NAMESPACE_END(partition);

