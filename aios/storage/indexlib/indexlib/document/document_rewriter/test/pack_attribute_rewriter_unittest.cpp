#include "indexlib/document/document_rewriter/test/pack_attribute_rewriter_unittest.h"

#include "indexlib/common/field_format/attribute/string_attribute_convertor.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/test/document_creator.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, PackAttributeRewriterTest);

PackAttributeRewriterTest::PackAttributeRewriterTest() {}

PackAttributeRewriterTest::~PackAttributeRewriterTest() {}

void PackAttributeRewriterTest::CaseSetUp()
{
    mDocStr = "cmd=add,int8_single=0,int8_multi=0 1,"
              "sub_int8_single=2,sub_int8_multi=2 3";
}

void PackAttributeRewriterTest::CaseTearDown() {}

void PackAttributeRewriterTest::TestRewriteWithSubDoc()
{
    // test both main / sub shemas have pack attribute config
    InnerTestRewrite(1, 1);
    // test neither main / sub shema has pack attribute config
    InnerTestRewrite(0, 0);
    // test only main schema has pack attribute config
    InnerTestRewrite(1, 0);
    // test only sub schema has pack attribute config
    InnerTestRewrite(0, 1);
}

void PackAttributeRewriterTest::TestRewriteCheckPackField()
{
    string field = "pkey:string;skey:int32;value:uint32";
    IndexPartitionSchemaPtr fixSchema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value");
    IndexPartitionSchemaPtr varSchema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;pkey;");
    string docStr = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;";

    Pool pool;
    string str(6, 'a');
    StringAttributeConvertor convertor;
    StringView newValue = convertor.Encode(StringView(str), &pool);

    {
        // fix
        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(fixSchema, docStr);
        PackAttributeRewriter rewriter;
        ASSERT_TRUE(rewriter.Init(fixSchema));

        rewriter.Rewrite(doc);
        ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());

        rewriter.Rewrite(doc);
        ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());

        doc->GetAttributeDocument()->SetPackField(0, newValue);
        rewriter.Rewrite(doc);
        ASSERT_EQ(SKIP_DOC, doc->GetDocOperateType());
    }

    {
        // var
        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(varSchema, docStr);
        PackAttributeRewriter rewriter;
        ASSERT_TRUE(rewriter.Init(varSchema));

        rewriter.Rewrite(doc);
        ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());

        rewriter.Rewrite(doc);
        ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());

        doc->GetAttributeDocument()->SetPackField(0, newValue);
        rewriter.Rewrite(doc);
        ASSERT_EQ(SKIP_DOC, doc->GetDocOperateType());
    }
}

IndexPartitionSchemaPtr PackAttributeRewriterTest::CreateSchema(bool mainSchemaHasPack, bool subSchemaHasPack)
{
    IndexPartitionSchemaPtr mainSchema;
    if (mainSchemaHasPack) {
        mainSchema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/", "main_schema_with_pack.json");
    } else {
        mainSchema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/", "main_schema_with_nopack.json");
    }

    IndexPartitionSchemaPtr subSchema;
    if (subSchemaHasPack) {
        subSchema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/", "sub_schema_with_pack.json");
    } else {
        subSchema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/", "sub_schema_with_nopack.json");
    }
    mainSchema->SetSubIndexPartitionSchema(subSchema);
    return mainSchema;
}

void PackAttributeRewriterTest::InnerTestRewrite(bool mainSchemaHasPack, bool subSchemaHasPack)
{
    IndexPartitionSchemaPtr schema = CreateSchema(mainSchemaHasPack, subSchemaHasPack);
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, mDocStr);
    PackAttributeRewriter rewriter;

    if (!mainSchemaHasPack && !subSchemaHasPack) {
        ASSERT_FALSE(rewriter.Init(schema));
        return;
    }

    ASSERT_TRUE(rewriter.Init(schema));

    rewriter.Rewrite(doc);
    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();

    if (mainSchemaHasPack) {
        ASSERT_EQ(size_t(1), attrDoc->GetPackFieldCount());
    } else {
        ASSERT_EQ(size_t(0), attrDoc->GetPackFieldCount());
    }

    ASSERT_EQ(size_t(1), doc->GetSubDocuments().size());
    NormalDocumentPtr subDoc = doc->GetSubDocuments()[0];
    attrDoc = subDoc->GetAttributeDocument();
    if (subSchemaHasPack) {
        ASSERT_EQ(size_t(1), attrDoc->GetPackFieldCount());
    } else {
        ASSERT_EQ(size_t(0), attrDoc->GetPackFieldCount());
    }
}
}} // namespace indexlib::document
