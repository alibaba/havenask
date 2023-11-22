#include "indexlib/document/document_rewriter/test/section_attribute_rewriter_unittest.h"

#include "autil/DataBuffer.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, SectionAttributeRewriterTest);

SectionAttributeRewriterTest::SectionAttributeRewriterTest() {}

SectionAttributeRewriterTest::~SectionAttributeRewriterTest() {}

void SectionAttributeRewriterTest::CaseSetUp() {}

void SectionAttributeRewriterTest::CaseTearDown() {}

void SectionAttributeRewriterTest::TestRewriteWithNoSubDoc()
{
    string docStr = "cmd=add,field0=hello_fid0,field1=hello_fid1";
    {
        // test main schema has section attribute
        IndexPartitionSchemaPtr schema = CreateSchema(false, true, false);
        InnerTestRewrite(schema, docStr);
    }

    {
        // test main schema has no section attribute
        IndexPartitionSchemaPtr schema = CreateSchema(false, false, false);
        InnerTestRewrite(schema, docStr);
    }
}

void SectionAttributeRewriterTest::TestRewriteWithSubDoc()
{
    string docStr = "cmd=add,field0=hello_fid0,field1=hello_fid1,"
                    "sub_field0=hello_subfid0^hello_subfid21,sub_field1=hello_subfid1^hello_subfid22";
    {
        // test main has section attribute, sub has section attribute
        IndexPartitionSchemaPtr schema = CreateSchema(true, true, true);
        InnerTestRewrite(schema, docStr);
    }

    {
        // test main has no section attribute, sub has section attribute
        IndexPartitionSchemaPtr schema = CreateSchema(true, false, true);
        InnerTestRewrite(schema, docStr);
    }

    {
        // test main has section attribute, sub has no section attribute
        IndexPartitionSchemaPtr schema = CreateSchema(true, true, false);
        InnerTestRewrite(schema, docStr);
    }

    {
        // test main has no section attribute, sub has no section attribute
        IndexPartitionSchemaPtr schema = CreateSchema(true, false, false);
        InnerTestRewrite(schema, docStr);
    }
}

void SectionAttributeRewriterTest::TestRewriteWithException()
{
    IndexPartitionSchemaPtr schema = CreateSchema(false, true, false);
    {
        string docStr = "cmd=add,field0=hello_fid0,field1=hello_fid1";
        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docStr, false);
        doc->SetIndexDocument(IndexDocumentPtr());
        SectionAttributeRewriter sectionAttrRewriter;
        sectionAttrRewriter.Init(schema);
        ASSERT_THROW(sectionAttrRewriter.Rewrite(doc), UnSupportedException);
    }
    {
        string docStr = "cmd=add,field0=hello_fid0,field1=hello_fid1";
        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docStr, false);
        doc->ModifyDocOperateType(UPDATE_FIELD);
        SectionAttributeRewriter sectionAttrRewriter;
        sectionAttrRewriter.Init(schema);
        sectionAttrRewriter.Rewrite(doc);
        ASSERT_EQ(INVALID_INDEXID, doc->GetIndexDocument()->GetMaxIndexIdInSectionAttribute());
    }
}

void SectionAttributeRewriterTest::TestRewriteWithDeserializeDoc()
{
    string docStr = "cmd=add,field0=hello_fid0,field1=hello_fid1";
    // test main schema has section attribute
    IndexPartitionSchemaPtr schema = CreateSchema(false, true, false);
    InnerTestRewrite(schema, docStr, true);

    // test main schema has no section attribute
    schema = CreateSchema(false, false, false);
    InnerTestRewrite(schema, docStr, true);

    string docStr2 = "cmd=add,field0=hello_fid0,field1=hello_fid1,"
                     "sub_field0=hello_subfid0,sub_field1=hello_subfid1";

    // test main has section attribute, sub has section attribute
    schema = CreateSchema(true, true, true);
    InnerTestRewrite(schema, docStr2, true);

    // test main has no section attribute, sub has section attribute
    schema = CreateSchema(true, false, true);
    InnerTestRewrite(schema, docStr2, true);

    // test main has section attribute, sub has no section attribute
    schema = CreateSchema(true, true, false);
    InnerTestRewrite(schema, docStr2, true);

    // test main has no section attribute, sub has no section attribute
    schema = CreateSchema(true, false, false);
    InnerTestRewrite(schema, docStr2, true);
}

void SectionAttributeRewriterTest::InnerTestRewrite(const IndexPartitionSchemaPtr& schema, const string& docStr,
                                                    bool serializeDoc)
{
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docStr, false);
    AttributeDocumentPtr attDoc(new AttributeDocument());
    doc->SetAttributeDocument(attDoc);
    for (size_t i = 0; i < doc->GetSubDocuments().size(); ++i) {
        doc->GetSubDocuments()[i]->SetAttributeDocument(attDoc);
    }
    if (serializeDoc) {
        DataBuffer dataBuffer;
        doc->serialize(dataBuffer);
        NormalDocumentPtr deserializeDoc(new NormalDocument());
        deserializeDoc->deserialize(dataBuffer);
        doc = deserializeDoc;
    }

    SectionAttributeRewriter sectionAttrRewriter;
    sectionAttrRewriter.Init(schema);
    sectionAttrRewriter.Rewrite(doc);
    CheckDocument(schema, doc);
}

void SectionAttributeRewriterTest::CheckDocument(const IndexPartitionSchemaPtr& schema,
                                                 const NormalDocumentPtr& document)
{
    assert(schema);
    // check main doc SectionAttribute
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexDocumentPtr indexDocument = document->GetIndexDocument();
    assert(indexDocument);
    IndexSchema::Iterator it;
    for (it = indexSchema->Begin(); it != indexSchema->End(); ++it) {
        const StringView& sectionAttr = indexDocument->GetSectionAttribute((*it)->GetIndexId());
        InvertedIndexType indexType = (*it)->GetInvertedIndexType();

        if (indexType == it_expack || indexType == it_pack) {
            PackageIndexConfigPtr packIndexConfig = DYNAMIC_POINTER_CAST(PackageIndexConfig, *it);

            if (packIndexConfig->HasSectionAttribute()) {
                ASSERT_FALSE(sectionAttr.empty());
                continue;
            }
        }
        ASSERT_EQ(StringView::empty_instance(), sectionAttr);
    }

    const NormalDocument::DocumentVector& subDocs = document->GetSubDocuments();
    for (size_t i = 0; i < subDocs.size(); ++i) {
        CheckDocument(schema->GetSubIndexPartitionSchema(), subDocs[i]);
    }
}

IndexPartitionSchemaPtr SectionAttributeRewriterTest::CreateSchema(bool hasSubSchema, bool mainHasSectionAttr,
                                                                   bool subHasSectionAttr)
{
    string field = "field0:text;field1:text;";
    string index = "index0:pack:field0,field1;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");

    if (!mainHasSectionAttr) {
        IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index0");
        PackageIndexConfigPtr packIndexConfig = DYNAMIC_POINTER_CAST(PackageIndexConfig, indexConfig);
        packIndexConfig->SetHasSectionAttributeFlag(false);
    }

    if (hasSubSchema) {
        string subField = "sub_field0:text;sub_field1:text;";
        string subIndex = "sub_index0:pack:sub_field0,sub_field1;";
        IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(subField, subIndex, "", "");
        if (!subHasSectionAttr) {
            IndexConfigPtr indexConfig = subSchema->GetIndexSchema()->GetIndexConfig("sub_index0");
            PackageIndexConfigPtr packIndexConfig = DYNAMIC_POINTER_CAST(PackageIndexConfig, indexConfig);
            packIndexConfig->SetHasSectionAttributeFlag(false);
        }
        schema->SetSubIndexPartitionSchema(subSchema);
    }
    return schema;
}
}} // namespace indexlib::document
