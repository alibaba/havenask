#include "indexlib/document/document_rewriter/test/document_rewriter_chain_unittest.h"

#include "autil/ConstString.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::test;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, DocumentRewriterChainTest);

DocumentRewriterChainTest::DocumentRewriterChainTest() {}

DocumentRewriterChainTest::~DocumentRewriterChainTest() {}

void DocumentRewriterChainTest::CaseSetUp()
{
    mSchema = SchemaMaker::MakeSchema("string0:string;price:int32", // Field schema
                                      "pk:primarykey64:string0;",   // Index schema
                                      "string0;price",              // attribute
                                      "");

    IndexPartitionSchemaPtr rtSchema(mSchema->Clone());
    SchemaLoader::RewriteToRtSchema(rtSchema);
    mRtSchema = rtSchema;
}

void DocumentRewriterChainTest::CaseTearDown() {}

void DocumentRewriterChainTest::TestAddToUpdate()
{
    IndexPartitionOptions options;
    DocumentRewriterChain chain(mRtSchema, options);
    chain.Init();

    ASSERT_EQ((size_t)2, chain.mRewriters.size());

    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(mSchema, "cmd=add,string0=hello,price=32,ts=1234,"
                                                                           "modify_fields=price");

    IndexDocumentPtr indexDoc = doc->GetIndexDocument();
    ASSERT_EQ((uint32_t)1, indexDoc->GetFieldCount());

    chain.Rewrite(doc);
    ASSERT_EQ((uint32_t)0, indexDoc->GetFieldCount());
    ASSERT_EQ(UPDATE_FIELD, doc->GetDocOperateType());
    ASSERT_EQ(ADD_DOC, doc->GetOriginalOperateType());
}

void DocumentRewriterChainTest::TestAddTimestamp()
{
    IndexPartitionOptions options;
    DocumentRewriterChain chain(mRtSchema, options);
    chain.Init();

    ASSERT_EQ((size_t)2, chain.mRewriters.size());

    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(mSchema, "cmd=add,string0=hello,price=32,ts=1234,"
                                                                           "modify_fields=string0#price");

    IndexDocumentPtr indexDoc = doc->GetIndexDocument();
    ASSERT_EQ((uint32_t)1, indexDoc->GetFieldCount());
    chain.Rewrite(doc);

    // add timestamp
    ASSERT_EQ((uint32_t)2, indexDoc->GetFieldCount());
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
}

void DocumentRewriterChainTest::TestAppendSectionAttribute()
{
    IndexPartitionOptions options;
    string field = "field0:text;field1:text;";
    string index = "index0:pack:field0,field1;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    DocumentRewriterChain chain(schema, options);
    chain.Init();
    NormalDocumentPtr doc =
        DocumentCreator::CreateNormalDocument(schema, "cmd=add,field0=hello_fid0,field1=hello_fid1", false);

    const IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    ASSERT_EQ(INVALID_INDEXID, indexDoc->GetMaxIndexIdInSectionAttribute());

    chain.Rewrite(doc);
    ASSERT_EQ((indexid_t)0, indexDoc->GetMaxIndexIdInSectionAttribute());
}

void DocumentRewriterChainTest::TestAppendPackAttributes()
{
    IndexPartitionSchemaPtr schema =
        SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema_with_all_type_pack_attributes.json");

    string docStr = "cmd=add,"
                    "int8_single=0,int8_multi=0 1,"
                    "int16_single=1,int16_multi=1 2,"
                    "int32_single=2,int32_multi=2 3,"
                    "int64_single=3,int64_multi=3 4,"
                    "uint8_single=5,uint8_multi=5 6,"
                    "uint16_single=6,uint16_multi=6 7,"
                    "uint32_single=7,uint32_multi=7 8,"
                    "uint64_single=8,uint64_multi=8 9,"
                    "float_single=9,float_multi=9 10,"
                    "double_single=10,double_multi=10 11,"
                    "str_single=abc,str_multi=abc def,"
                    "int8_single_nopack=23,str_multi_nopack=i don't pack";

    IndexPartitionOptions options;
    DocumentRewriterChain chain(schema, options);
    chain.Init();

    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docStr);
    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    ASSERT_EQ(size_t(0), attrDoc->GetPackFieldCount());

    chain.Rewrite(doc);

    ASSERT_EQ(size_t(2), attrDoc->GetPackFieldCount());

    // test repeat rewrite will not take effects
    const StringView& packField0 = attrDoc->GetPackField(0);
    const StringView& packField1 = attrDoc->GetPackField(1);
    chain.Rewrite(doc);
    ASSERT_EQ(size_t(2), attrDoc->GetPackFieldCount());
    ASSERT_EQ(packField0, attrDoc->GetPackField(0));
    ASSERT_EQ(packField1, attrDoc->GetPackField(1));
}

void DocumentRewriterChainTest::TestForKKVDocument()
{
    string field = "pkey:string;skey:string;value:uint32;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    IndexPartitionOptions options;
    DocumentRewriterChain chain(schema, options);
    chain.Init();

    ASSERT_EQ((size_t)2, chain.mRewriters.size());

    NormalDocumentPtr doc =
        DocumentCreator::CreateNormalDocument(schema, "cmd=add,pkey=hello,skey=world,value=10,ts=1234;");

    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    ASSERT_EQ((uint32_t)2, attrDoc->GetNotEmptyFieldCount());
    ASSERT_EQ((size_t)0, attrDoc->GetPackFieldCount());
    chain.Rewrite(doc);
    ASSERT_EQ((uint32_t)0, attrDoc->GetNotEmptyFieldCount());
    ASSERT_EQ((size_t)1, attrDoc->GetPackFieldCount());
}
}} // namespace indexlib::document
