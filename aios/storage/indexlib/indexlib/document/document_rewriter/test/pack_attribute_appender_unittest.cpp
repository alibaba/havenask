#include "indexlib/document/document_rewriter/test/pack_attribute_appender_unittest.h"

#include "indexlib/config/test/schema_loader.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, PackAttributeAppenderTest);

PackAttributeAppenderTest::PackAttributeAppenderTest() {}

PackAttributeAppenderTest::~PackAttributeAppenderTest() {}

void PackAttributeAppenderTest::CaseSetUp()
{
    mSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema_with_all_type_pack_attributes.json");

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

    mDoc = DocumentCreator::CreateNormalDocument(mSchema, docStr);
}

void PackAttributeAppenderTest::CaseTearDown() {}

void PackAttributeAppenderTest::TestSimpleProcess()
{
    PackAttributeAppender packAttrAppender;
    packAttrAppender.Init(mSchema);
    AttributeDocumentPtr attrDocument = mDoc->GetAttributeDocument();
    ASSERT_EQ(size_t(24), attrDocument->GetNotEmptyFieldCount());

    ASSERT_EQ(size_t(0), attrDocument->GetPackFieldCount());
    ASSERT_TRUE(packAttrAppender.AppendPackAttribute(attrDocument, mDoc->GetPool()));

    ASSERT_EQ(size_t(2), attrDocument->GetPackFieldCount());
    ASSERT_FALSE(attrDocument->GetPackField(0).empty());
    ASSERT_FALSE(attrDocument->GetPackField(1).empty());

    // in-pack fields will be reclaimed.
    ASSERT_EQ(size_t(2), attrDocument->GetNotEmptyFieldCount());
    size_t nonEmptyFieldCount = 0;
    fieldid_t fieldId;
    AttributeDocument::Iterator iter = attrDocument->CreateIterator();
    while (iter.HasNext()) {
        nonEmptyFieldCount++;
        iter.Next(fieldId);
    }
    ASSERT_EQ(size_t(2), nonEmptyFieldCount);
}

void PackAttributeAppenderTest::TestForKKVTable()
{
    string field = "pkey:string;skey:string;value:uint32;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value");
    ASSERT_TRUE(schema);
    string docString = "cmd=add,pkey=pkey1,skey=skey1,value=1,ts=100";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);

    AttributeDocumentPtr attrDocument = doc->GetAttributeDocument();
    ASSERT_EQ(size_t(1), attrDocument->GetNotEmptyFieldCount());
    ASSERT_EQ(size_t(0), attrDocument->GetPackFieldCount());

    PackAttributeAppender packAttrAppender;
    packAttrAppender.Init(schema);
    ASSERT_TRUE(packAttrAppender.AppendPackAttribute(attrDocument, doc->GetPool()));

    ASSERT_EQ(size_t(0), attrDocument->GetNotEmptyFieldCount());
    ASSERT_EQ(size_t(1), attrDocument->GetPackFieldCount());
    ASSERT_FALSE(attrDocument->GetPackField(0).empty());
}
}} // namespace indexlib::document
