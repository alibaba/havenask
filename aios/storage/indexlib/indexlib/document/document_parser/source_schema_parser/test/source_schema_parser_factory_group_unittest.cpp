#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/source_schema_parser/source_schema_parser_factory_group.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;
using namespace indexlib::file_system;

using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlib { namespace document {

class SourceSchemaParserFactoryGroupTest : public INDEXLIB_TESTBASE
{
public:
    SourceSchemaParserFactoryGroupTest();
    ~SourceSchemaParserFactoryGroupTest();

    DECLARE_CLASS_NAME(SourceSchemaParserFactoryGroupTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestException();

private:
    string mTestDataPath;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SourceSchemaParserFactoryGroupTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SourceSchemaParserFactoryGroupTest, TestException);
IE_LOG_SETUP(document, SourceSchemaParserFactoryGroupTest);

SourceSchemaParserFactoryGroupTest::SourceSchemaParserFactoryGroupTest() {}

SourceSchemaParserFactoryGroupTest::~SourceSchemaParserFactoryGroupTest() {}

void SourceSchemaParserFactoryGroupTest::CaseSetUp() { mTestDataPath = GET_PRIVATE_TEST_DATA_PATH(); }

void SourceSchemaParserFactoryGroupTest::CaseTearDown() {}

void SourceSchemaParserFactoryGroupTest::TestSimpleProcess()
{
    string pluginPath = FslibWrapper::JoinPath(mTestDataPath, "plugins");
    string schemaFilePath = FslibWrapper::JoinPath(mTestDataPath, "schema_with_source.json");
    string schemaStr;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(schemaFilePath, schemaStr).Code());
    Any any = ParseJson(schemaStr);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);

    SourceSchemaParserFactoryGroupPtr factoryGroup(new SourceSchemaParserFactoryGroup);
    ASSERT_TRUE(factoryGroup->Init(schema, pluginPath));
    SourceSchemaParserFactoryGroup::SourceSchemaParserGroup parserGroup = factoryGroup->CreateParserGroup();
    ASSERT_EQ(3, parserGroup.size());
    {
        vector<string> srcSchema;
        parserGroup[0]->CreateDocSrcSchema(RawDocumentPtr(), srcSchema);
        ASSERT_EQ(vector<string>({"string1", "string2"}), srcSchema);
    }
    {
        vector<string> srcSchema;
        RawDocumentPtr rawDoc(new DefaultRawDocument());
        rawDoc->setField("schema_field", "test_field1,test_field2");
        parserGroup[1]->CreateDocSrcSchema(rawDoc, srcSchema);
        ASSERT_EQ(vector<string>({"test_field1", "test_field2"}), srcSchema);
    }
    {
        vector<string> srcSchema;
        parserGroup[2]->CreateDocSrcSchema(RawDocumentPtr(), srcSchema);
        ASSERT_EQ(vector<string>({"string1", "string2", "price"}), srcSchema);
    }
}

void SourceSchemaParserFactoryGroupTest::TestException()
{
    string pluginPath = FslibWrapper::JoinPath(mTestDataPath, "plugins");
    string schemaFilePath = FslibWrapper::JoinPath(mTestDataPath, "schema_with_source_exception.json");
    string schemaStr;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(schemaFilePath, schemaStr).Code());
    Any any = ParseJson(schemaStr);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);

    SourceSchemaParserFactoryGroupPtr factoryGroup(new SourceSchemaParserFactoryGroup);
    ASSERT_FALSE(factoryGroup->Init(schema, pluginPath));
}
}} // namespace indexlib::document
