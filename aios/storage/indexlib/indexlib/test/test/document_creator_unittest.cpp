#include "indexlib/test/test/document_creator_unittest.h"

#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/document_parser/kv_parser/kkv_keys_extractor.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"

using namespace std;
using namespace autil;

using namespace indexlib::document;
using namespace indexlib::config;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, DocumentCreatorTest);

DocumentCreatorTest::DocumentCreatorTest() {}

DocumentCreatorTest::~DocumentCreatorTest() {}

void DocumentCreatorTest::CaseSetUp() {}

void DocumentCreatorTest::CaseTearDown() {}

void DocumentCreatorTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");
    string fieldSchemaString = "text1:text;"
                               "string1:string;"
                               "long1:uint32;";
    string index = "pack1:pack:text1;"
                   "pk:primarykey64:string1";
    string attr = "long1;";
    string summary = "string1;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(fieldSchemaString, index, attr, summary);
    INDEXLIB_TEST_TRUE(schema);

    string docString = "cmd=add,string1=pk2,long1=2,ts=100,text1=t1 t2 t3;";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);

    INDEXLIB_TEST_TRUE(doc);
    INDEXLIB_TEST_EQUAL(ADD_DOC, doc->GetDocOperateType());
    INDEXLIB_TEST_EQUAL((int64_t)100, doc->GetTimestamp());

    IndexDocumentPtr indexDoc = doc->GetIndexDocument();
    INDEXLIB_TEST_TRUE(indexDoc);

    INDEXLIB_TEST_EQUAL("pk2", indexDoc->GetPrimaryKey());

    FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
    fieldid_t fid = fieldSchema->GetFieldId("long1");
    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    uint32_t value = 2;
    INDEXLIB_TEST_EQUAL(StringView((char*)&value, sizeof(uint32_t)), attrDoc->GetField(fid));
}

void DocumentCreatorTest::TestCreateSection()
{
    SCOPED_TRACE("Failed");
    string fieldSchemaString = "text1:text;";
    string index = "pack1:pack:text1;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(fieldSchemaString, index, "", "");
    INDEXLIB_TEST_TRUE(schema);
    string docString = "cmd=add,text1=t1 t2 t3;";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);

    INDEXLIB_TEST_TRUE(doc);
    INDEXLIB_TEST_EQUAL(ADD_DOC, doc->GetDocOperateType());

    IndexDocumentPtr indexDoc = doc->GetIndexDocument();
    INDEXLIB_TEST_TRUE(indexDoc);

    IndexTokenizeField* field = dynamic_cast<IndexTokenizeField*>(indexDoc->GetField(0));
    INDEXLIB_TEST_TRUE(field);

    size_t sectionCount = field->GetSectionCount();
    INDEXLIB_TEST_EQUAL((size_t)1, sectionCount);

    Section* section = field->GetSection(0);
    INDEXLIB_TEST_TRUE(section);
    INDEXLIB_TEST_EQUAL((size_t)3, section->GetTokenCount());

    Token* token = section->GetToken(0);
    INDEXLIB_TEST_TRUE(token);
    INDEXLIB_TEST_EQUAL((uint64_t)8816114993438044119, token->GetHashKey());
}

void DocumentCreatorTest::TestCreateKKVDocument()
{
    string field = "pkey:string;skey:string;value:uint32;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value");
    ASSERT_TRUE(schema);
    string docString = "cmd=add,pkey=pkey1,skey=skey1,value=1,ts=100";
    auto doc = DocumentCreator::CreateKVDocument(schema, docString, /*region*/ "");
    KVIndexDocument& kvIndexDocument = doc->back();
    const config::IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(config::KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KKVKeysExtractorPtr keysExtractor(new KKVKeysExtractor(kkvConfig));
    uint64_t pkeyHash = 0;
    keysExtractor->HashPrefixKey("pkey1", pkeyHash);
    uint64_t skeyHash = 0;
    keysExtractor->HashSuffixKey("skey1", skeyHash);
    ASSERT_EQ(pkeyHash, kvIndexDocument.GetPKeyHash());
    ASSERT_EQ(skeyHash, kvIndexDocument.GetSKeyHash());
    ASSERT_TRUE(kvIndexDocument.HasSKey());
}
}} // namespace indexlib::test
