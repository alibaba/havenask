#include "indexlib/document/document_rewriter/test/sub_doc_add_to_update_document_rewriter_unittest.h"

#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, SubDocAddToUpdateDocumentRewriterTest);

void SubDocAddToUpdateDocumentRewriterTest::CaseSetUp()
{
    string field = "price:int32;pk:int64;index:string";
    string index = "pk:primarykey64:pk;index:string:index;price:number:price";
    string attr = "price;pk";
    string summary = "";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);
    mSchema->GetIndexSchema()->GetIndexConfig("price")->TEST_SetIndexUpdatable(true);
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
        /*fieldNames=*/"sub_pk:string;sub_int:uint32;sub_index:string",
        /*indexNames=*/"sub_pk:primarykey64:sub_pk;sub_index:string:sub_index;sub_int:number:sub_int",
        /*attributeNames=*/"sub_pk;sub_int;", /*summmaryNames=*/"");
    subSchema->GetIndexSchema()->GetIndexConfig("sub_int")->TEST_SetIndexUpdatable(true);
    mSchema->SetSubIndexPartitionSchema(subSchema);
    mSubSchema = subSchema;

    vector<TruncateOptionConfigPtr> truncOptionConfigs(1);
    vector<indexlibv2::config::SortDescriptions> partMetas(1);

    AddToUpdateDocumentRewriter* mainRewriter = new AddToUpdateDocumentRewriter;
    mainRewriter->Init(mSchema, truncOptionConfigs, partMetas);
    AddToUpdateDocumentRewriter* subRewriter = new AddToUpdateDocumentRewriter;
    subRewriter->Init(mSubSchema, truncOptionConfigs, partMetas);
    mRewriter.reset(new SubDocAddToUpdateDocumentRewriter());
    mRewriter->Init(mSchema, mainRewriter, subRewriter);
}

void SubDocAddToUpdateDocumentRewriterTest::TestRewriteMainDoc()
{
    NormalDocumentPtr doc = CreateDocument("cmd=add,pk=10,price=1,modify_fields=price");
    // fieldId 0 is price
    AddModifiedToken(doc, /*fieldId=*/0);
    mRewriter->Rewrite(doc);
    ASSERT_EQ(UPDATE_FIELD, doc->GetDocOperateType());
    ASSERT_EQ(ADD_DOC, doc->GetOriginalOperateType());
    EXPECT_EQ((uint32_t)1, doc->GetAttributeDocument()->GetNotEmptyFieldCount());
    // IndexDocument is cleared after rewrite.
    // The new and old index tokens are saved in modified tokens.
    EXPECT_EQ((uint32_t)0, doc->GetIndexDocument()->GetFieldCount());
    EXPECT_NE(nullptr, doc->GetIndexDocument()->GetFieldModifiedTokens(/*fieldId=*/0));
    CheckDocumentModifiedFields(doc);
}

void SubDocAddToUpdateDocumentRewriterTest::TestRewriteSubDoc()
{
    NormalDocumentPtr doc = CreateDocument("cmd=add,pk=10,sub_pk=sub1^sub2,sub_int=11^12,"
                                           "modify_fields=sub_int,sub_modify_fields=^sub_int");
    // fieldId 1 for sub schema is sub_int
    AddModifiedToken(doc->GetSubDocuments()[1], /*fieldId=*/1);

    mRewriter->Rewrite(doc);
    ASSERT_EQ(UPDATE_FIELD, doc->GetDocOperateType());
    ASSERT_EQ(ADD_DOC, doc->GetOriginalOperateType());
    ASSERT_EQ((uint32_t)0, doc->GetAttributeDocument()->GetNotEmptyFieldCount());
    EXPECT_EQ((uint32_t)0, doc->GetIndexDocument()->GetFieldCount());
    const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    ASSERT_EQ((uint32_t)1, subDocs.size());
    EXPECT_EQ((uint32_t)1, subDocs[0]->GetAttributeDocument()->GetNotEmptyFieldCount());
    // IndexDocument is cleared after rewrite.
    // The new and old index tokens are saved in modified tokens.
    EXPECT_EQ((uint32_t)0, subDocs[0]->GetIndexDocument()->GetFieldCount());
    EXPECT_NE(nullptr, subDocs[0]->GetIndexDocument()->GetFieldModifiedTokens(/*fieldId=*/1));
    CheckDocumentModifiedFields(doc);
}

void SubDocAddToUpdateDocumentRewriterTest::TestRewriteMainAndSub()
{
    NormalDocumentPtr doc = CreateDocument("cmd=add,pk=10,price=10,sub_pk=sub1^sub2^sub3,sub_int=11^12^13,"
                                           "modify_fields=price#sub_int,sub_modify_fields=^sub_int^sub_int");
    // fieldId 0 is price
    // fieldId 1 is sub_int
    AddModifiedToken(doc, /*fieldId=*/0);
    // fieldId 1 for sub schema is sub_int
    AddModifiedToken(doc->GetSubDocuments()[1], /*fieldId=*/1);
    AddModifiedToken(doc->GetSubDocuments()[2], /*fieldId=*/1);

    mRewriter->Rewrite(doc);
    ASSERT_EQ(UPDATE_FIELD, doc->GetDocOperateType());
    ASSERT_EQ(ADD_DOC, doc->GetOriginalOperateType());
    ASSERT_EQ((uint32_t)1, doc->GetAttributeDocument()->GetNotEmptyFieldCount());
    EXPECT_NE(nullptr, doc->GetIndexDocument()->GetFieldModifiedTokens(/*fieldId=*/0));
    ASSERT_EQ("10", doc->GetPrimaryKey());

    const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    ASSERT_EQ((uint32_t)2, subDocs.size());
    ASSERT_EQ((uint32_t)1, subDocs[0]->GetAttributeDocument()->GetNotEmptyFieldCount());
    // IndexDocument is cleared after rewrite.
    // The new and old index tokens are saved in modified tokens.
    EXPECT_EQ((uint32_t)0, subDocs[0]->GetIndexDocument()->GetFieldCount());
    EXPECT_NE(nullptr, subDocs[0]->GetIndexDocument()->GetFieldModifiedTokens(/*fieldId=*/1));
    ASSERT_EQ("sub2", subDocs[0]->GetPrimaryKey());
    ASSERT_EQ((uint32_t)1, subDocs[1]->GetAttributeDocument()->GetNotEmptyFieldCount());
    EXPECT_EQ((uint32_t)0, subDocs[0]->GetIndexDocument()->GetFieldCount());
    EXPECT_NE(nullptr, subDocs[0]->GetIndexDocument()->GetFieldModifiedTokens(/*fieldId=*/1));
    ASSERT_EQ("sub3", subDocs[1]->GetPrimaryKey());

    CheckDocumentModifiedFields(doc);
}

void SubDocAddToUpdateDocumentRewriterTest::TestNonAddDoc()
{
    NormalDocumentPtr doc = CreateDocument("cmd=delete,pk=10");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(DELETE_DOC, doc->GetDocOperateType());
}

void SubDocAddToUpdateDocumentRewriterTest::TestEmptyModifiedFields()
{
    NormalDocumentPtr doc = CreateDocument("cmd=add,pk=10,price=10,sub_pk=sub1,sub_int=11");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
}

void SubDocAddToUpdateDocumentRewriterTest::TestRewriteMainFail()
{
    // main pk changed
    NormalDocumentPtr doc = CreateDocument("cmd=add,pk=10,price=10,sub_pk=sub1,sub_int=11,modify_fields=pk");
    // fieldId 0 is price
    AddModifiedToken(doc, /*fieldId=*/0);

    mRewriter->Rewrite(doc);
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
    CheckDocumentModifiedFields(doc);

    // non updatable index changed
    doc = CreateDocument("cmd=add,pk=10,index=1,sub_pk=sub1,sub_int=11,modify_fields=index");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
    CheckDocumentModifiedFields(doc);
}

void SubDocAddToUpdateDocumentRewriterTest::TestRewriteSubFail()
{
    // sub pk chaned
    NormalDocumentPtr doc = CreateDocument("cmd=add,pk=10,price=10,sub_pk=sub1,sub_int=11,"
                                           "modify_fields=sub_pk,sub_modify_fields=sub_pk");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
    CheckDocumentModifiedFields(doc);

    // non updatable sub index changed
    doc = CreateDocument("cmd=add,pk=10,price=10,sub_pk=sub1,sub_int=11,sub_index=1,"
                         "modify_fields=sub_index,sub_modify_fields=sub_index");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
    CheckDocumentModifiedFields(doc);
}

void SubDocAddToUpdateDocumentRewriterTest::TestInvalidModifiedFields()
{
    // sub_int in main modified fields, but not in sub modified fields.
    NormalDocumentPtr doc = CreateDocument("cmd=add,pk=10,price=10,sub_pk=sub1,sub_int=11,"
                                           "modify_fields=sub_int");

    mRewriter->Rewrite(doc);
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
    CheckDocumentModifiedFields(doc);
}

void SubDocAddToUpdateDocumentRewriterTest::AddModifiedToken(const NormalDocumentPtr& doc, fieldid_t fieldId)
{
    // termKey is unused
    doc->GetIndexDocument()->PushModifiedToken(/*fieldId=*/fieldId, /*termKey=*/-1,
                                               document::ModifiedTokens::Operation::ADD);
}

NormalDocumentPtr SubDocAddToUpdateDocumentRewriterTest::CreateDocument(string docStr)
{
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(mSchema, docStr);
    return doc;
}

void SubDocAddToUpdateDocumentRewriterTest::CheckDocumentModifiedFields(const NormalDocumentPtr& doc)
{
    const NormalDocument::FieldIdVector& modifiedFields = doc->GetModifiedFields();
    const NormalDocument::FieldIdVector& subModifiedFields = doc->GetSubModifiedFields();

    ASSERT_TRUE(modifiedFields.empty());
    ASSERT_TRUE(subModifiedFields.empty());

    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocuments.size(); i++) {
        CheckDocumentModifiedFields(subDocuments[i]);
    }
}
}} // namespace indexlib::document
