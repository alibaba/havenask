#include <autil/StringUtil.h>
#include "indexlib/document/test/document_factory_wrapper_unittest.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/document/document_parser/normal_parser/normal_document_parser.h"
#include "indexlib/document/document_parser/kv_parser/kv_document_parser.h"
#include "indexlib/document/document_parser/kv_parser/kkv_document_parser.h"
#include "indexlib/document/document_parser/line_data_parser/line_data_document_parser.h"
#include "indexlib/document/test/example_customized_document.h"
#include "indexlib/testlib/schema_maker.h"

using namespace std;
using namespace indexlib_example;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(testlib);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, DocumentFactoryWrapperTest);

DocumentFactoryWrapperTest::DocumentFactoryWrapperTest()
{
}

DocumentFactoryWrapperTest::~DocumentFactoryWrapperTest()
{
}

void DocumentFactoryWrapperTest::CaseSetUp()
{
}

void DocumentFactoryWrapperTest::CaseTearDown()
{
}

void DocumentFactoryWrapperTest::TestInitBuiltInFactory()
{
    {
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
                "pk:int32;", "pk:primarykey64:pk", "", "");
        DocumentFactoryWrapper wrapper(schema);
        wrapper.Init();
        DocumentParser* parser = wrapper.CreateDocumentParser();
        NormalDocumentParser* targetParser = dynamic_cast<NormalDocumentParser*>(parser);
        ASSERT_TRUE(targetParser);
        delete parser;
    }
    {
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(
                "key:string;value:uint64;", "key", "value");
        DocumentFactoryWrapper wrapper(schema);
        wrapper.Init();
        DocumentParser* parser = wrapper.CreateDocumentParser();
        KvDocumentParser* targetParser = dynamic_cast<KvDocumentParser*>(parser);
        ASSERT_TRUE(targetParser);
        delete parser;
    }
    {
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(
                "pkey:uint64;skey:uint32;value:uint32;", "pkey", "skey", "value");
        DocumentFactoryWrapper wrapper(schema);
        wrapper.Init();
        DocumentParser* parser = wrapper.CreateDocumentParser();
        KkvDocumentParser* targetParser = dynamic_cast<KkvDocumentParser*>(parser);
        ASSERT_TRUE(targetParser);
        delete parser;
    }
    {
        IndexPartitionSchemaPtr schema= SchemaMaker::MakeSchema(
                "pk:int32;", "pk:primarykey64:pk", "", "");
        schema->SetTableType("line_data");
        DocumentFactoryWrapper wrapper(schema);
        wrapper.Init();
        DocumentParser* parser = wrapper.CreateDocumentParser();
        LineDataDocumentParser* targetParser = dynamic_cast<LineDataDocumentParser*>(parser);
        ASSERT_TRUE(targetParser);
        delete parser;
    }

    {
        // custimzed table without customized_doc_config, use normal doc parser by default
        IndexPartitionSchemaPtr schema= SchemaMaker::MakeSchema(
                "pk:int32;", "pk:primarykey64:pk", "", "");
        schema->SetTableType("customized");
        DocumentFactoryWrapper wrapper(schema);
        wrapper.Init();

        DocumentParser* parser = wrapper.CreateDocumentParser();
        NormalDocumentParser* targetParser = dynamic_cast<NormalDocumentParser*>(parser);
        ASSERT_TRUE(targetParser);
        delete parser;

        DocumentInitParam::KeyValueMap kvMap;
        RawDocumentParser* rawDocParser = wrapper.CreateRawDocumentParser(kvMap);
        ASSERT_FALSE(rawDocParser);
        delete rawDocParser;
    }

    {
        // empty schema: can create default raw document
        DocumentFactoryWrapper wrapper;
        wrapper.Init();

        RawDocument* rawDoc = wrapper.CreateRawDocument();
        ASSERT_TRUE(rawDoc);
        delete rawDoc;
        
        DocumentParser* parser = wrapper.CreateDocumentParser();
        ASSERT_FALSE(parser);
    }
}

void DocumentFactoryWrapperTest::TestInitPluginInFactory()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            "pk:int32;", "pk:primarykey64:pk", "", "");

    CustomizedConfigVector docFactoryConfig;
    CustomizedConfigPtr cfg(new CustomizedConfig);
    cfg->SetPluginName("example");
    cfg->SetId(CUSTOMIZED_DOCUMENT_CONFIG_PARSER);

    CustomizedConfigPtr cfg1(new CustomizedConfig);
    cfg1->SetPluginName("example2");
    cfg1->SetId(CUSTOMIZED_DOCUMENT_CONFIG_RAWDOCUMENT);

    docFactoryConfig.push_back(cfg);
    docFactoryConfig.push_back(cfg1);
    schema->SetCustomizedDocumentConfig(docFactoryConfig);

    string pluginPath = string(TEST_DATA_PATH) + "/example/plugins";
    DocumentFactoryWrapperPtr wrapper1 =
        DocumentFactoryWrapper::CreateDocumentFactoryWrapper(
                schema, CUSTOMIZED_DOCUMENT_CONFIG_PARSER, pluginPath);
    DocumentFactoryWrapperPtr wrapper2 =
        DocumentFactoryWrapper::CreateDocumentFactoryWrapper(
                schema, CUSTOMIZED_DOCUMENT_CONFIG_RAWDOCUMENT, pluginPath);

    DocumentInitParam::KeyValueMap kvMap;
    RawDocumentParser* parser1 = wrapper1->CreateRawDocumentParser(kvMap);
    RawDocumentParser* parser2 = wrapper2->CreateRawDocumentParser(kvMap);
    ASSERT_TRUE(parser1);
    ASSERT_TRUE(parser2);

    MyRawDocumentParser* rawDocParser1 = dynamic_cast<MyRawDocumentParser*>(parser1);
    MyRawDocumentParser* rawDocParser2 = dynamic_cast<MyRawDocumentParser*>(parser2);
    ASSERT_TRUE(rawDocParser1);
    ASSERT_TRUE(rawDocParser2);

    ASSERT_EQ(string("0"), rawDocParser1->getId());
    ASSERT_EQ(string("1"), rawDocParser2->getId());
    delete parser1;
    delete parser2;
}


IE_NAMESPACE_END(document);

