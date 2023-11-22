#include "indexlib/document/document_parser/normal_parser/test/null_field_appender_unittest.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/raw_document/default_raw_document.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, NullFieldAppenderTest);

NullFieldAppenderTest::NullFieldAppenderTest() {}

NullFieldAppenderTest::~NullFieldAppenderTest() {}

void NullFieldAppenderTest::CaseSetUp() {}

void NullFieldAppenderTest::CaseTearDown() {}

void NullFieldAppenderTest::TestSimpleProcess()
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("string1:string;long1:uint32;body:text", "index1:string:string1;", "", "long1;body");

    NullFieldAppender appender;
    RawDocumentPtr rawDoc(new DefaultRawDocument);
    rawDoc->setDocOperateType(ADD_DOC);

    ASSERT_FALSE(appender.Init(schema->GetFieldSchema()));

    SchemaMaker::EnableNullFields(schema, "long1;body");
    ASSERT_TRUE(appender.Init(schema->GetFieldSchema()));
    appender.Append(rawDoc);
    ASSERT_TRUE(rawDoc->exist("long1"));
    ASSERT_EQ(string("__NULL__"), rawDoc->getField("long1"));

    ASSERT_TRUE(rawDoc->exist("body"));
    ASSERT_EQ(string("__NULL__"), rawDoc->getField("body"));

    ASSERT_FALSE(rawDoc->exist("string1"));
}
}} // namespace indexlib::document
