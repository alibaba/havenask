#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/document/document_parser/source_schema_parser/builtin_source_schema_parser.h"
#include "indexlib/document/document_parser/source_schema_parser/builtin_source_schema_parser_factory.h"
#include "indexlib/document/document_parser/source_schema_parser/source_schema_parser.h"
#include "indexlib/document/document_parser/source_schema_parser/source_schema_parser_factory_wrapper.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace indexlib::config;

namespace indexlib { namespace document {

class SourceSchemaParserFactoryWrapperTest : public INDEXLIB_TESTBASE
{
public:
    SourceSchemaParserFactoryWrapperTest();
    ~SourceSchemaParserFactoryWrapperTest();

    DECLARE_CLASS_NAME(SourceSchemaParserFactoryWrapperTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SourceSchemaParserFactoryWrapperTest, TestSimpleProcess);
IE_LOG_SETUP(document, SourceSchemaParserFactoryWrapperTest);

SourceSchemaParserFactoryWrapperTest::SourceSchemaParserFactoryWrapperTest() {}

SourceSchemaParserFactoryWrapperTest::~SourceSchemaParserFactoryWrapperTest() {}

void SourceSchemaParserFactoryWrapperTest::CaseSetUp() {}

void SourceSchemaParserFactoryWrapperTest::CaseTearDown() {}

void SourceSchemaParserFactoryWrapperTest::TestSimpleProcess()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema, "field1:long;field2:uint32", "", "field1;field2", "");
    SourceGroupConfigPtr sourceGroupConfig(new SourceGroupConfig);
    sourceGroupConfig->SetFieldMode(SourceGroupConfig::SourceFieldMode::ALL_FIELD);
    SourceSchemaParserFactoryWrapper wrapper(schema, sourceGroupConfig);
    wrapper.Init();
    SourceSchemaParserPtr parser = wrapper.CreateSourceSchemaParser();
    BuiltinSourceSchemaParser* targetParser = dynamic_cast<BuiltinSourceSchemaParser*>(parser.get());
    ASSERT_TRUE(targetParser);
    vector<string> srcSchema;
    parser->CreateDocSrcSchema(RawDocumentPtr(), srcSchema);
    ASSERT_EQ(2, srcSchema.size());
    ASSERT_EQ(vector<string>({"field1", "field2"}), srcSchema);
}
}} // namespace indexlib::document
