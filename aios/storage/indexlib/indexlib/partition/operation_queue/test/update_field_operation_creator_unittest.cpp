#include "indexlib/partition/operation_queue/test/update_field_operation_creator_unittest.h"

#include "autil/LongHashValue.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/partition/operation_queue/update_field_operation.h"
#include "indexlib/util/HashString.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::document;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, UpdateFieldOperationCreatorTest);

UpdateFieldOperationCreatorTest::UpdateFieldOperationCreatorTest() {}

UpdateFieldOperationCreatorTest::~UpdateFieldOperationCreatorTest() {}

void UpdateFieldOperationCreatorTest::CaseSetUp()
{
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(
        mSchema,
        "title:text;price:int32;sale:int32;nid:int64;promotion:int64",                         // this is field schema
        "nid:primarykey64:nid;title:text:title;promotion:number:promotion;price:number:price", // this is index schema
        "price;sale;nid", // this is attribute schema
        "");              // this is summary schema

    string docStr = "{ 0,"
                    "[price: (3)],"
                    "[nid: (1)],"
                    "[sale: (10)]"
                    "}";

    mDoc = DYNAMIC_POINTER_CAST(NormalDocument, DocumentMaker::MakeDocument(docStr, mSchema));
    mDoc->SetTimestamp(10);
}

void UpdateFieldOperationCreatorTest::CaseTearDown() {}

void UpdateFieldOperationCreatorTest::TestCreateOperationItems()
{
    {
        // check invalid field
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, "nid:int64;price:string;sale:int32", "nid:primarykey64:nid",
                                         "price;sale", "");

        UpdateFieldOperationCreator creator(schema);
        AttributeDocumentPtr attrDoc = mDoc->GetAttributeDocument();
        uint32_t itemCount = 0;
        OperationItem* items = nullptr;
        bool r = creator.CreateOperationItems(&mPool, mDoc, &items, &itemCount);
        ASSERT_FALSE(r);
        EXPECT_EQ((uint32_t)0, itemCount);
        EXPECT_EQ(items, nullptr);
    }

    {
        UpdateFieldOperationCreator creator(mSchema);
        // check ignore fields: pk, price:empty field value
        AttributeDocumentPtr attrDoc = mDoc->GetAttributeDocument();
        StringView emptyValue;
        attrDoc->SetField(1, emptyValue);
        uint32_t itemCount = 0;
        OperationItem* items = nullptr;
        bool r = creator.CreateOperationItems(&mPool, mDoc, &items, &itemCount);
        ASSERT_TRUE(r);
        EXPECT_EQ((uint32_t)1, itemCount);
        EXPECT_NE(items, nullptr);
    }
}

void UpdateFieldOperationCreatorTest::TestCreate()
{
    {
        // check no attribute doc
        NormalDocumentPtr doc(new NormalDocument);
        IndexDocumentPtr indexDoc(new IndexDocument(doc->GetPool()));
        indexDoc->SetPrimaryKey("abc");
        doc->SetIndexDocument(indexDoc);

        UpdateFieldOperationCreator creator(mSchema);
        OperationBase* operation;
        EXPECT_FALSE(creator.Create(doc, &mPool, &operation));
    }

    {
        // no attribute schema
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, "nid:int64", "nid:primarykey64:nid", "", "");

        UpdateFieldOperationCreator creator(schema);
        OperationBase* op;
        bool r = creator.Create(mDoc, &mPool, &op);
        EXPECT_FALSE(r);
    }

    {
        // normal case
        UpdateFieldOperationCreator creator(mSchema);
        OperationBase* operation;
        bool r = creator.Create(mDoc, &mPool, &operation);
        ASSERT_TRUE(r);
        ASSERT_TRUE(operation);
        UpdateFieldOperation<uint64_t>* updateOperation = dynamic_cast<UpdateFieldOperation<uint64_t>*>(operation);
        ASSERT_TRUE(updateOperation);

        HashString hasher;
        uint64_t hashValue;
        const IndexDocumentPtr& indexDoc = mDoc->GetIndexDocument();
        const string& pkString = indexDoc->GetPrimaryKey();
        hasher.Hash(hashValue, pkString.c_str(), pkString.size());

        uint128_t hashValue128;
        hashValue128.value[1] = hashValue;
        EXPECT_EQ(hashValue128, updateOperation->mPkHash);
        EXPECT_EQ((int64_t)10, updateOperation->mTimestamp);
        ASSERT_EQ((size_t)2, updateOperation->mItemSize);
        EXPECT_EQ((fieldid_t)1, updateOperation->mItems[0].first);
        EXPECT_EQ((fieldid_t)2, updateOperation->mItems[1].first);

        int32_t price = *(int32_t*)updateOperation->mItems[0].second.data();
        int32_t sale = *(int32_t*)updateOperation->mItems[1].second.data();
        EXPECT_EQ((int32_t)3, price);
        EXPECT_EQ((int32_t)10, sale);
    }
}

void UpdateFieldOperationCreatorTest::TestUpdateIndex()
{
    string docStr = "{ 0,"
                    "[nid: (1)],"
                    "[title: (test_title)],"
                    "[promotion: (999)]"
                    "}";
    document::NormalDocumentPtr doc =
        DYNAMIC_POINTER_CAST(NormalDocument, DocumentMaker::MakeDocument(docStr, mSchema));
    doc->SetTimestamp(10);
    ModifiedTokens titleTokens = ModifiedTokens::TEST_CreateModifiedTokens(/*fieldId=*/0, {0});
    ModifiedTokens promotionTokens = ModifiedTokens::TEST_CreateModifiedTokens(/*fieldId=*/4, {0});
    doc->GetIndexDocument()->SetModifiedTokens(
        {titleTokens, ModifiedTokens(), ModifiedTokens(), ModifiedTokens(), promotionTokens});

    UpdateFieldOperationCreator creator(mSchema);
    OperationBase* operation;
    bool r = creator.Create(doc, &mPool, &operation);
    ASSERT_TRUE(r);
    ASSERT_TRUE(operation);
    UpdateFieldOperation<uint64_t>* updateOperation = dynamic_cast<UpdateFieldOperation<uint64_t>*>(operation);
    ASSERT_TRUE(updateOperation);

    HashString hasher;
    uint64_t hashValue;
    const IndexDocumentPtr& indexDoc = mDoc->GetIndexDocument();
    const string& pkString = indexDoc->GetPrimaryKey();
    hasher.Hash(hashValue, pkString.c_str(), pkString.size());

    uint128_t hashValue128;
    hashValue128.value[1] = hashValue;
    EXPECT_EQ(hashValue128, updateOperation->mPkHash);
    EXPECT_EQ((int64_t)10, updateOperation->mTimestamp);
    ASSERT_EQ((size_t)3, updateOperation->mItemSize);
    EXPECT_EQ(INVALID_FIELDID, updateOperation->mItems[0].first);
    EXPECT_EQ((fieldid_t)0, updateOperation->mItems[1].first);
    EXPECT_EQ((fieldid_t)4, updateOperation->mItems[2].first);

    {
        ModifiedTokens deserializedTokens1;
        autil::DataBuffer buffer((char*)updateOperation->mItems[1].second.data(),
                                 updateOperation->mItems[1].second.size());
        deserializedTokens1.Deserialize(buffer);
        ASSERT_TRUE(ModifiedTokens::Equal(titleTokens, deserializedTokens1));
    }
    {
        ModifiedTokens deserializedTokens2;
        autil::DataBuffer buffer((char*)updateOperation->mItems[2].second.data(),
                                 updateOperation->mItems[2].second.size());
        deserializedTokens2.Deserialize(buffer);
        ASSERT_TRUE(ModifiedTokens::Equal(promotionTokens, deserializedTokens2));
    }
}

void UpdateFieldOperationCreatorTest::TestUpdateIndexAndAttr()
{
    string docStr = "{ 0,"
                    "[nid: (1)],"
                    "[title: (test_title)],"
                    "[promotion: (999)],"
                    "[price: (123)]"
                    "}";
    document::NormalDocumentPtr doc =
        DYNAMIC_POINTER_CAST(NormalDocument, DocumentMaker::MakeDocument(docStr, mSchema));
    doc->SetTimestamp(10);
    // Ideally modified tokens should be generated in doc parser phase, and termKey in this test
    // should be the same for one doc. Here termKeys are set to be different in order to verify
    // update operation serialization.
    ModifiedTokens titleTokens = ModifiedTokens::TEST_CreateModifiedTokens(/*fieldId=*/0, {123});
    ModifiedTokens promotionTokens = ModifiedTokens::TEST_CreateModifiedTokens(/*fieldId=*/4, {456});
    ModifiedTokens priceTokens = ModifiedTokens::TEST_CreateModifiedTokens(/*fieldId=*/1, {789});
    doc->GetIndexDocument()->SetModifiedTokens(
        {titleTokens, priceTokens, ModifiedTokens(), ModifiedTokens(), promotionTokens});

    UpdateFieldOperationCreator creator(mSchema);
    OperationBase* operation;
    bool r = creator.Create(doc, &mPool, &operation);
    ASSERT_TRUE(r);
    ASSERT_TRUE(operation);
    UpdateFieldOperation<uint64_t>* updateOperation = dynamic_cast<UpdateFieldOperation<uint64_t>*>(operation);
    ASSERT_TRUE(updateOperation);

    HashString hasher;
    uint64_t hashValue;
    const IndexDocumentPtr& indexDoc = mDoc->GetIndexDocument();
    const string& pkString = indexDoc->GetPrimaryKey();
    hasher.Hash(hashValue, pkString.c_str(), pkString.size());

    uint128_t hashValue128;
    hashValue128.value[1] = hashValue;
    EXPECT_EQ(hashValue128, updateOperation->mPkHash);
    EXPECT_EQ((int64_t)10, updateOperation->mTimestamp);
    ASSERT_EQ((size_t)5, updateOperation->mItemSize);

    EXPECT_EQ((fieldid_t)1, updateOperation->mItems[0].first); // price attr
    int32_t price = *(int32_t*)updateOperation->mItems[0].second.data();
    EXPECT_EQ((int32_t)123, price);

    EXPECT_EQ(INVALID_FIELDID, updateOperation->mItems[1].first); // seperator

    EXPECT_EQ((fieldid_t)0, updateOperation->mItems[2].first); // title index
    EXPECT_EQ((fieldid_t)1, updateOperation->mItems[3].first); // price index
    EXPECT_EQ((fieldid_t)4, updateOperation->mItems[4].first); // promotion index

    {
        ModifiedTokens deserializedTokens1;
        autil::DataBuffer buffer((char*)updateOperation->mItems[2].second.data(),
                                 updateOperation->mItems[2].second.size());
        deserializedTokens1.Deserialize(buffer);
        EXPECT_TRUE(ModifiedTokens::Equal(titleTokens, deserializedTokens1));
    }
    {
        ModifiedTokens deserializedTokens2;
        autil::DataBuffer buffer((char*)updateOperation->mItems[3].second.data(),
                                 updateOperation->mItems[3].second.size());
        deserializedTokens2.Deserialize(buffer);
        EXPECT_TRUE(ModifiedTokens::Equal(priceTokens, deserializedTokens2));
    }
    {
        ModifiedTokens deserializedTokens3;
        autil::DataBuffer buffer((char*)updateOperation->mItems[4].second.data(),
                                 updateOperation->mItems[4].second.size());
        deserializedTokens3.Deserialize(buffer);
        EXPECT_TRUE(ModifiedTokens::Equal(promotionTokens, deserializedTokens3));
    }
}
}} // namespace indexlib::partition
