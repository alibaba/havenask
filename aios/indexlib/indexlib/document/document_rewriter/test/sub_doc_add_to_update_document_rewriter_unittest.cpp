#include "indexlib/document/document_rewriter/test/sub_doc_add_to_update_document_rewriter_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/document_creator.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, SubDocAddToUpdateDocumentRewriterTest);

SubDocAddToUpdateDocumentRewriterTest::SubDocAddToUpdateDocumentRewriterTest()
{
}

SubDocAddToUpdateDocumentRewriterTest::~SubDocAddToUpdateDocumentRewriterTest()
{
}

void SubDocAddToUpdateDocumentRewriterTest::CaseSetUp()
{
    string field = "price:int32;pk:int64;index:string";
    string index = "pk:primarykey64:pk;index:string:index";
    string attr = "price;pk";
    string summary = "";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            "sub_pk:string;sub_int:uint32;sub_index:string",
            "sub_pk:primarykey64:sub_pk;sub_index:string:sub_index",
            "sub_pk;sub_int;",
            "");
    mSchema->SetSubIndexPartitionSchema(subSchema);
    mSubSchema = subSchema;

    vector<TruncateOptionConfigPtr> truncOptionConfigs(1);
    vector<SortDescriptions> partMetas(1);

    AddToUpdateDocumentRewriter* mainRewriter = new AddToUpdateDocumentRewriter;
    mainRewriter->Init(mSchema, truncOptionConfigs, partMetas);
    AddToUpdateDocumentRewriter* subRewriter = new AddToUpdateDocumentRewriter;
    subRewriter->Init(mSubSchema, truncOptionConfigs, partMetas);
    mRewriter.reset(new SubDocAddToUpdateDocumentRewriter());
    mRewriter->Init(mSchema, mainRewriter, subRewriter);
}

void SubDocAddToUpdateDocumentRewriterTest::CaseTearDown()
{
}

void SubDocAddToUpdateDocumentRewriterTest::TestRewriteMainDoc()
{
    NormalDocumentPtr doc = CreateDocument("cmd=add,pk=10,price=1,modify_fields=price");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(UPDATE_FIELD, doc->GetDocOperateType());
    ASSERT_EQ(ADD_DOC, doc->GetOriginalOperateType());
    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    ASSERT_EQ((uint32_t)1, attrDoc->GetNotEmptyFieldCount());

    CheckDocumentModifiedFields(doc);
}

void SubDocAddToUpdateDocumentRewriterTest::TestRewriteSubDoc()
{
    NormalDocumentPtr doc = CreateDocument(
            "cmd=add,pk=10,sub_pk=sub1^sub2,sub_int=11^12,"
            "modify_fields=sub_int,sub_modify_fields=^sub_int");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(UPDATE_FIELD, doc->GetDocOperateType());
    ASSERT_EQ(ADD_DOC, doc->GetOriginalOperateType());
    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    ASSERT_EQ((uint32_t)0, attrDoc->GetNotEmptyFieldCount());
    const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    ASSERT_EQ((uint32_t)1, subDocs.size());
    AttributeDocumentPtr subAttrDoc = subDocs[0]->GetAttributeDocument();
    ASSERT_EQ((uint32_t)1, subAttrDoc->GetNotEmptyFieldCount());

    CheckDocumentModifiedFields(doc);
}

void SubDocAddToUpdateDocumentRewriterTest::TestRewriteMainAndSub()
{
    NormalDocumentPtr doc = CreateDocument(
            "cmd=add,pk=10,price=10,sub_pk=sub1^sub2^sub3,sub_int=11^12^13,"
            "modify_fields=price#sub_int,sub_modify_fields=^sub_int^sub_int");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(UPDATE_FIELD, doc->GetDocOperateType());
    ASSERT_EQ(ADD_DOC, doc->GetOriginalOperateType());
    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    ASSERT_EQ((uint32_t)1, attrDoc->GetNotEmptyFieldCount());
    ASSERT_EQ("10", doc->GetPrimaryKey());

    const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    ASSERT_EQ((uint32_t)2, subDocs.size());
    AttributeDocumentPtr subAttrDoc0 = subDocs[0]->GetAttributeDocument();
    ASSERT_EQ((uint32_t)1, subAttrDoc0->GetNotEmptyFieldCount());
    ASSERT_EQ("sub2", subDocs[0]->GetPrimaryKey());
    AttributeDocumentPtr subAttrDoc1 = subDocs[1]->GetAttributeDocument();
    ASSERT_EQ((uint32_t)1, subAttrDoc1->GetNotEmptyFieldCount());
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
    NormalDocumentPtr doc = CreateDocument(
            "cmd=add,pk=10,price=10,sub_pk=sub1,sub_int=11");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
}

void SubDocAddToUpdateDocumentRewriterTest::TestRewriteMainFail()
{
    // main pk changed
    NormalDocumentPtr doc = CreateDocument(
            "cmd=add,pk=10,price=10,sub_pk=sub1,sub_int=11,modify_fields=pk");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
    CheckDocumentModifiedFields(doc);

    // sub index changed
    doc = CreateDocument("cmd=add,pk=10,index=1,sub_pk=sub1,sub_int=11,modify_fields=index");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
    CheckDocumentModifiedFields(doc);
}

void SubDocAddToUpdateDocumentRewriterTest::TestRewriteSubFail()
{
    // sub pk chaned
    NormalDocumentPtr doc = CreateDocument(
            "cmd=add,pk=10,price=10,sub_pk=sub1,sub_int=11,"
            "modify_fields=sub_pk,sub_modify_fields=sub_pk");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
    CheckDocumentModifiedFields(doc);

    // sub index changed
    doc = CreateDocument(
            "cmd=add,pk=10,price=10,sub_pk=sub1,sub_int=11,sub_index=1,"
            "modify_fields=sub_index,sub_modify_fields=sub_index");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
    CheckDocumentModifiedFields(doc);
}

void SubDocAddToUpdateDocumentRewriterTest::TestInvalidModifiedFields()
{
    // sub_int in main modified fields, but not in sub modified fields.
    NormalDocumentPtr doc = CreateDocument(
            "cmd=add,pk=10,price=10,sub_pk=sub1,sub_int=11,"
            "modify_fields=sub_int");
    mRewriter->Rewrite(doc);
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
    CheckDocumentModifiedFields(doc);
}

NormalDocumentPtr SubDocAddToUpdateDocumentRewriterTest::CreateDocument(string docStr)
{
    return DocumentCreator::CreateDocument(mSchema, docStr);
}

void SubDocAddToUpdateDocumentRewriterTest::CheckDocumentModifiedFields(
        const NormalDocumentPtr& doc)
{
    const NormalDocument::FieldIdVector& modifiedFields = doc->GetModifiedFields();
    const NormalDocument::FieldIdVector& subModifiedFields = 
        doc->GetSubModifiedFields();

    ASSERT_TRUE(modifiedFields.empty());
    ASSERT_TRUE(subModifiedFields.empty());

    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocuments.size(); i++)
    {
        CheckDocumentModifiedFields(subDocuments[i]);
    }
}

IE_NAMESPACE_END(document);

