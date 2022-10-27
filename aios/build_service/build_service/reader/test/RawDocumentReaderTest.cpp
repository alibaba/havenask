#include "build_service/test/unittest.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/StandardRawDocumentParser.h"
#include "build_service/reader/CombinedRawDocumentParser.h"
#include "build_service/reader/IdleDocumentParser.h"
#include "build_service/reader/test/FakeRawDocumentReader.h"
#include "build_service/reader/DocumentSeparators.h"
#include "build_service/reader/test/CustomizedRawDocParser.h"
#include "build_service/config/CLIOptionNames.h"
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/util/counter/accumulative_counter.h>
#include <indexlib/util/counter/state_counter.h>

using namespace std;
using namespace testing;
using namespace build_service::config;
using namespace build_service::document;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(document);

namespace build_service {
namespace reader {

class RawDocumentReaderTest : public BUILD_SERVICE_TESTBASE {};

class MockRawDocumentParser : public RawDocumentParser
{
public:
    MockRawDocumentParser() {}
    ~MockRawDocumentParser() {}
public:
    MOCK_METHOD2(parse, bool(const std::string&, document::RawDocument&));
    bool parseMultiMessage(
        const std::vector<IE_NAMESPACE(document)::RawDocumentParser::Message>& msgs,
        IE_NAMESPACE(document)::RawDocument& rawDoc) override {
        assert(false);
        return false;
    }        
};

TEST_F(RawDocumentReaderTest, testCreateRawDocumentParser) {
    {
        ReaderInitParam params;
        FakeRawDocumentReader reader;
        reader.initialize(params);
        unique_ptr<StandardRawDocumentParser> parser(ASSERT_CAST_AND_RETURN(
                        StandardRawDocumentParser, reader.createRawDocumentParser(params)));
        EXPECT_EQ(RAW_DOCUMENT_HA3_FIELD_SEP, parser->_fieldSep._sep);
        EXPECT_EQ(RAW_DOCUMENT_HA3_KV_SEP, parser->_keyValueSep._sep);
    }
    {
        ReaderInitParam params;
        params.kvMap[RAW_DOCUMENT_KV_SEP] = "";
        FakeRawDocumentReader reader;
        reader.initialize(params);
        ASSERT_FALSE(reader.createRawDocumentParser(params));
    }
    {
        ReaderInitParam params;
        params.kvMap[RAW_DOCUMENT_FIELD_SEP] = "";
        FakeRawDocumentReader reader;
        reader.initialize(params);
        ASSERT_FALSE(reader.createRawDocumentParser(params));
    }
    {
        ReaderInitParam params;
        params.kvMap[RAW_DOCUMENT_FIELD_SEP] = "a";
        params.kvMap[RAW_DOCUMENT_KV_SEP] = "b";
        FakeRawDocumentReader reader;
        reader.initialize(params);
        unique_ptr<StandardRawDocumentParser> parser(ASSERT_CAST_AND_RETURN(
                        StandardRawDocumentParser, reader.createRawDocumentParser(params)));
        EXPECT_EQ("a", parser->_fieldSep._sep);
        EXPECT_EQ("b", parser->_keyValueSep._sep);
    }
    {
        ReaderInitParam params;
        params.kvMap[RAW_DOCUMENT_FORMAT] = RAW_DOCUMENT_ISEARCH_DOCUMENT_FORMAT;
        params.kvMap[RAW_DOCUMENT_FIELD_SEP] = "a";
        params.kvMap[RAW_DOCUMENT_KV_SEP] = "b";
        FakeRawDocumentReader reader;
        reader.initialize(params);
        unique_ptr<StandardRawDocumentParser> parser(ASSERT_CAST_AND_RETURN(
                        StandardRawDocumentParser, reader.createRawDocumentParser(params)));
        EXPECT_EQ(RAW_DOCUMENT_ISEARCH_FIELD_SEP, parser->_fieldSep._sep);
        EXPECT_EQ(RAW_DOCUMENT_ISEARCH_KV_SEP, parser->_keyValueSep._sep);
    }
    {
        ReaderInitParam params;
        params.kvMap[RAW_DOCUMENT_FORMAT] = RAW_DOCUMENT_HA3_DOCUMENT_FORMAT;
        params.kvMap[RAW_DOCUMENT_FIELD_SEP] = "a";
        params.kvMap[RAW_DOCUMENT_KV_SEP] = "b";
        FakeRawDocumentReader reader;
        reader.initialize(params);
        unique_ptr<StandardRawDocumentParser> parser(ASSERT_CAST_AND_RETURN(
                        StandardRawDocumentParser, reader.createRawDocumentParser(params)));
        EXPECT_EQ(RAW_DOCUMENT_HA3_FIELD_SEP, parser->_fieldSep._sep);
        EXPECT_EQ(RAW_DOCUMENT_HA3_KV_SEP, parser->_keyValueSep._sep);
    }
    {
        ReaderInitParam params;
        params.kvMap[RAW_DOCUMENT_FORMAT] = "unknown";
        params.kvMap[RAW_DOCUMENT_FIELD_SEP] = "a";
        params.kvMap[RAW_DOCUMENT_KV_SEP] = "b";
        FakeRawDocumentReader reader;
        reader.initialize(params);
        EXPECT_FALSE(reader.createRawDocumentParser(params));
    }
    {
        ReaderInitParam params;
        params.kvMap[RAW_DOCUMENT_FORMAT] = "self_explain";
        FakeRawDocumentReader reader;
        reader.initialize(params);
        unique_ptr<IdleDocumentParser> parser(ASSERT_CAST_AND_RETURN(IdleDocumentParser,
                        reader.createRawDocumentParser(params)));
        EXPECT_EQ("data", parser->_fieldName);
    }
    {
        ReaderInitParam params;
        params.kvMap[RAW_DOCUMENT_FORMAT] = "self_explain";
        params.kvMap[DOC_STRING_FIELD_NAME] = "doc_string";
        FakeRawDocumentReader reader;
        reader.initialize(params);
        unique_ptr<IdleDocumentParser> parser(ASSERT_CAST_AND_RETURN(IdleDocumentParser,
                        reader.createRawDocumentParser(params)));
        EXPECT_EQ("doc_string", parser->_fieldName);
    }
    {
        string dsJsonStr = R"({
                "src" : "file",
                "type" : "file", 
                "data" : "hdfs://xxx/mainse/xxx-file",
                "parser_config" : [
                    {
                        "type" : "customized",
                        "parameters" : {
                            "field_separator" : "%%",
                            "kv_separator" : "="        
                        }
                    },
                    {
                        "type" : "isearch",
                        "parameters" : {
                            "field_separator" : "%%",
                            "kv_separator" : "="        
                        }
                    }                    
                ]
        })";
        // """
        ReaderInitParam params;
        params.kvMap[DATA_DESCRIPTION_KEY] = dsJsonStr;
        FakeRawDocumentReader reader;
        reader.initialize(params);
        unique_ptr<CombinedRawDocumentParser> parser(ASSERT_CAST_AND_RETURN(CombinedRawDocumentParser,
                        reader.createRawDocumentParser(params)));
        EXPECT_EQ(2u, parser->_parsers.size());
        auto typedParser = dynamic_pointer_cast<StandardRawDocumentParser>(parser->_parsers[0]);
        ASSERT_TRUE(typedParser);
        EXPECT_EQ(string("%%"), typedParser->_fieldSep._sep); 
        EXPECT_EQ(string("="), typedParser->_keyValueSep._sep);
        typedParser = dynamic_pointer_cast<StandardRawDocumentParser>(parser->_parsers[1]);
        ASSERT_TRUE(typedParser);
        EXPECT_EQ(string("\n"), typedParser->_fieldSep._sep); 
        EXPECT_EQ(string("="), typedParser->_keyValueSep._sep);         
    }
}

TEST_F(RawDocumentReaderTest, testTimestamp) {
    ReaderInitParam params;
    params.counterMap.reset(new CounterMap());
    params.kvMap[TIMESTAMP_FIELD] = "test_timestamp_field";
    FakeRawDocumentReader reader;
    ASSERT_TRUE(reader.initialize(params));
    reader.addRawDocument("ha_reserved_timestamp=123\ntest_timestamp_field=12345\n\n", 111);
    RawDocumentPtr rawDoc(new DefaultRawDocument());
    int64_t offset = 0;
    ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader.read(rawDoc, offset));
    ASSERT_EQ(int64_t(12345), rawDoc->getDocTimestamp());
}

TEST_F(RawDocumentReaderTest, testExtendField) {
    ReaderInitParam params;
    params.counterMap.reset(new CounterMap());
    params.kvMap[EXTEND_FIELD_NAME] = "doc_type";
    params.kvMap[EXTEND_FIELD_VALUE] = "odpsrecord_single";

    FakeRawDocumentReader reader;
    ASSERT_TRUE(reader.initialize(params));

    reader.addRawDocument("ha_reserved_timestamp=123\ntest_timestamp_field=12345\n\n", 111);
    RawDocumentPtr rawDoc(new DefaultRawDocument());
    int64_t offset = 0;
    ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader.read(rawDoc, offset));
    ASSERT_EQ("odpsrecord_single", rawDoc->getField("doc_type"));
}

TEST_F(RawDocumentReaderTest, testInit) {
    {
        ReaderInitParam params;
        params.kvMap[RAW_DOCUMENT_FIELD_SEP] = "";
        FakeRawDocumentReader reader;
        params.counterMap.reset(new CounterMap());        
        ASSERT_FALSE(reader.initialize(params));
    }
}

TEST_F(RawDocumentReaderTest, testParseFail) {
    MockRawDocumentParser *parser = new MockRawDocumentParser();
    EXPECT_CALL(*parser, parse(_, _)).WillOnce(Return(false));
    ReaderInitParam params;
    CounterMapPtr counterMap(new CounterMap());
    params.counterMap = counterMap;
    params.kvMap[EXTEND_FIELD_NAME] = "doc_type";
    params.kvMap[EXTEND_FIELD_VALUE] = "odpsrecord_single";

    FakeRawDocumentReader reader;
    ASSERT_TRUE(reader.initialize(params));
    delete reader._parser;
    reader._parser = parser;

    reader.addRawDocument("ha_reserved_timestamp=123\ntest_timestamp_field=12345\n\n", 111);
    RawDocumentPtr rawDoc(new DefaultRawDocument());
    int64_t offset = 0;
    ASSERT_EQ(RawDocumentReader::ERROR_PARSE, reader.read(rawDoc, offset));
    ASSERT_EQ(1, GET_ACC_COUNTER(counterMap, parseFailCount)->Get());
}

TEST_F(RawDocumentReaderTest, testCustomizedRawDoc) {
    ReaderInitParam params;
    params.counterMap.reset(new CounterMap());
    params.kvMap[TIMESTAMP_FIELD] = "test_timestamp_field";
    params.kvMap[RAW_DOCUMENT_SCHEMA_NAME] = "customized_raw_doc";
    params.kvMap[DATA_DESCRIPTION_KEY] = R"(
            {
                "src" : "file",
                "type" : "file", 
                "data" : "xxx",
                "parser_config" : [
                    {
                       "type": "indexlib_parser",
                       "parameters": {}
                    }
                ]
            }
        )";
    ResourceReaderPtr resourceReader(new ResourceReader(TEST_DATA_PATH
                                                        "/custom_rawdoc_reader_test"));
    params.resourceReader = resourceReader;
    FakeRawDocumentReader reader;
    reader._resetParser = false;
    ASSERT_TRUE(reader.initialize(params));
    
    float data[4];
    for (size_t i = 0; i < 4; i++)
    {
        data[i] = 0.1 * i;
    }

    string dataStr((char*)data, 16);
    reader.addRawDocument(dataStr, 111);
    RawDocumentPtr rawDoc(new MyRawDocument());
    int64_t offset = 0;
    ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader.read(rawDoc, offset));
    MyRawDocumentPtr myRawDoc = DYNAMIC_POINTER_CAST(MyRawDocument, rawDoc);
    float* actualData = myRawDoc->getData();
    for (size_t i = 0; i < 4; i++)
    {
        ASSERT_FLOAT_EQ(data[i], actualData[i]);
    }
    BS_LOG(INFO, "raw doc data: %s", myRawDoc->toString().c_str());
}

TEST_F(RawDocumentReaderTest, testReadError) {
    RawDocumentPtr rawDoc(new MyRawDocument());
    int64_t offset = 0;
    FakeRawDocumentReader reader;
    // read to eof
    ASSERT_EQ(RawDocumentReader::ERROR_EOF,
              reader.read(rawDoc, offset));
    
    // read without documentFactory
    reader.addRawDocument("testdoc", 1);
    ASSERT_EQ(RawDocumentReader::ERROR_EXCEPTION,
              reader.read(rawDoc, offset));
    
    // read with error documentFactory
    DocumentFactoryWrapperPtr wrapper(new DocumentFactoryWrapper);
    wrapper->mPluginFactory = new FakeDocumentFactory;
    reader._documentFactoryWrapper = wrapper;
    ASSERT_EQ(RawDocumentReader::ERROR_EXCEPTION,
              reader.read(rawDoc, offset));
}

}
}
