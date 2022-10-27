#include "build_service/test/unittest.h"
#include "build_service/reader/SwiftRawDocumentReader.h"
#include "build_service/reader/test/MockSwiftReader.h"
#include "build_service/reader/test/MockSwiftClientCreator.h"
#include "build_service/reader/StandardRawDocumentParser.h"
#include "build_service/reader/SwiftFieldFilterRawDocumentParser.h"
#include "build_service/config/CLIOptionNames.h"
#include <autil/StringUtil.h>
#include <swift/client/SwiftReaderConfig.h>
#include "build_service/reader/test/FakeSwiftRawDocumentReader.h"
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/util/counter/state_counter.h>
#include <indexlib/util/counter/accumulative_counter.h>
#include <indexlib/document/raw_document/default_raw_document.h>
#include <indexlib/document/built_in_document_factory.h>

using namespace std;
using namespace autil;
using namespace testing;

SWIFT_USE_NAMESPACE(client);
SWIFT_USE_NAMESPACE(protocol);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(document);
using namespace build_service::config;
using namespace build_service::document;
using namespace build_service::util;

namespace build_service {
namespace reader {

class SwiftRawDocumentReaderTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
public:
    static string createMessage(const string &pk);
    static string createMessage(const string &pk, const string &timestamp);
    SwiftRawDocumentReaderPtr createSwiftReader(
            int32_t docCount, int64_t timestampOffset = 0,
            const string &topicName = "topic_name",
            bool hasReservedTimestamp = false,
            const string& reservedTimestamp = "");
private:
    ReaderInitParam _params;
};

void SwiftRawDocumentReaderTest::setUp() {
    // clear initParam
    _params = ReaderInitParam();
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
    _params.counterMap = counterMap;
    string zfsRoot = "zfs://swift_root";
    _params.kvMap[SWIFT_ZOOKEEPER_ROOT] = zfsRoot;

    string topicName = "topic_name";
    _params.kvMap[SWIFT_TOPIC_NAME] = topicName;

    string readerConfig = "swift_reader_config";
    _params.kvMap[SWIFT_READER_CONFIG] = readerConfig;
    _params.kvMap[SWIFT_CLIENT_CONFIG] = "swift_client_config";
    _params.counterMap.reset(new CounterMap());
}

void SwiftRawDocumentReaderTest::tearDown() {
}

class FakeMultiMessageRawDoc : public IE_NAMESPACE(document)::DefaultRawDocument {
public:
    std::vector<IE_NAMESPACE(document)::RawDocumentParser::Message> msgs;
};

class FakeRawDocParser : public StandardRawDocumentParser {
public:
    FakeRawDocParser() : StandardRawDocumentParser("", "") {}
    bool parseMultiMessage(
        const std::vector<IE_NAMESPACE(document)::RawDocumentParser::Message> &msgs,
        IE_NAMESPACE(document)::RawDocument &rawDoc) override {
        auto &typedRawDoc = dynamic_cast<FakeMultiMessageRawDoc &>(rawDoc);
        typedRawDoc.msgs = msgs;
        return true;
    }
};    

class FakeMultiDocumentFactory : public IE_NAMESPACE(document)::BuiltInDocumentFactory {
public:
    RawDocument *CreateMultiMessageRawDocument(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema) override {
        auto rawDoc = new FakeMultiMessageRawDoc();
        return rawDoc;
    }
    RawDocumentParser *CreateRawDocumentParser(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
        const IE_NAMESPACE(document)::DocumentInitParamPtr &initParam) override {
        return new FakeRawDocParser();
    }
};


class MockSwiftRawDocumentReader : public SwiftRawDocumentReader {
public:
    MockSwiftRawDocumentReader(MockSwiftClient* swiftClient)
        : SwiftRawDocumentReader(SwiftClientCreatorPtr())
        , _mockSwiftClient(swiftClient)
    {
    }
    ~MockSwiftRawDocumentReader()
    {
        DELETE_AND_SET_NULL(_mockSwiftClient);
    }
public:
    MOCK_METHOD2(createSwiftClient, SwiftClient*(const string&, const string&));
    MOCK_METHOD2(createSwiftReader, SwiftReaderWrapper*(const std::string &, ErrorInfo *));
    MOCK_METHOD0(getFreshness, int64_t());

private:
    MockSwiftClient* _mockSwiftClient;
};

TEST_F(SwiftRawDocumentReaderTest, testSwiftClientConfig) {
    MockSwiftRawDocumentReader reader(NULL);
    NiceMockSwiftReader *mockSwiftReader = new NiceMockSwiftReader;
    ON_CALL(reader, createSwiftClient(_, _)).WillByDefault(
            Invoke(std::bind(MockSwiftClientCreator::createMockClientWithConfig,
                            std::placeholders::_1, std::placeholders::_2)));
    ON_CALL(reader, createSwiftReader(_, _)).WillByDefault(Return(new SwiftReaderWrapper(mockSwiftReader)));
    EXPECT_CALL(reader, createSwiftClient(_, _));
    EXPECT_CALL(reader, createSwiftReader(_, _));
    EXPECT_TRUE(reader.initialize(_params));
    SWIFT_NS(client)::SwiftClient *swiftClient = reader._swiftClient;
    MockSwiftClient* client = dynamic_cast<MockSwiftClient*>(swiftClient);
    ASSERT_EQ(client->getConfigStr(), "zkPath=zfs://swift_root;swift_client_config");
    DELETE_AND_SET_NULL(swiftClient);
}

TEST_F(SwiftRawDocumentReaderTest, testSimpleProcess) {
    _params.kvMap[SRC_SIGNATURE] = "0";
    SwiftRawDocumentReaderPtr reader = createSwiftReader(3);
    int64_t offset;
    for (size_t i = 0; i < 3; ++i) {
        DefaultRawDocument rawDoc(reader->_hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader->read(rawDoc, offset));
        string id = StringUtil::toString(i);
        EXPECT_EQ(id, rawDoc.getField("id"));
    }
    DefaultRawDocument rawDoc(reader->_hashMapManager);
    EXPECT_EQ(RawDocumentReader::ERROR_WAIT, reader->read(rawDoc, offset));
    NiceMockSwiftReader *mockSwiftReader = ASSERT_CAST_AND_RETURN(NiceMockSwiftReader, reader->_swiftReader->_reader);
    EXPECT_CALL(*mockSwiftReader, read(_, _, _)).WillOnce(Return(SWIFT_NS(protocol)::ERROR_BROKER_BUSY));
    EXPECT_EQ(RawDocumentReader::ERROR_WAIT, reader->read(rawDoc, offset));
    EXPECT_CALL(*mockSwiftReader, read(_, _, _)).WillOnce(Return(SWIFT_NS(protocol)::ERROR_CLIENT_NO_MORE_MESSAGE));
    EXPECT_EQ(RawDocumentReader::ERROR_WAIT, reader->read(rawDoc, offset));
}

TEST_F(SwiftRawDocumentReaderTest, testReadMultiMessage) {
    _params.kvMap[SRC_SIGNATURE] = "0";
    SwiftRawDocumentReaderPtr reader = createSwiftReader(10);
    int64_t offset;

    reader->_documentFactoryWrapper->mBuiltInFactory = nullptr;
    reader->_documentFactoryWrapper->mPluginFactory = new FakeMultiDocumentFactory();
    reader->_batchBuildSize = 6;
    reader->_parser = new FakeRawDocParser();

    {
        RawDocumentPtr rawDoc;
        auto ec = reader->read(rawDoc, offset);
        EXPECT_EQ(RawDocumentReader::ERROR_NONE, ec);
        auto typedDoc = dynamic_pointer_cast<FakeMultiMessageRawDoc>(rawDoc);
        EXPECT_TRUE(typedDoc.get());
        EXPECT_EQ(6u, typedDoc->msgs.size());
    }
    {
        RawDocumentPtr rawDoc;
        auto ec = reader->read(rawDoc, offset);
        EXPECT_EQ(RawDocumentReader::ERROR_NONE, ec);
        auto typedDoc = dynamic_pointer_cast<FakeMultiMessageRawDoc>(rawDoc);
        EXPECT_TRUE(typedDoc.get());
        EXPECT_EQ(4u, typedDoc->msgs.size());
    }
    {
        RawDocumentPtr rawDoc;
        auto ec = reader->read(rawDoc, offset);
        EXPECT_EQ(RawDocumentReader::ERROR_WAIT, ec);
        auto typedDoc = dynamic_pointer_cast<FakeMultiMessageRawDoc>(rawDoc);
        EXPECT_TRUE(typedDoc.get());
        EXPECT_EQ(0u, typedDoc->msgs.size());
    }        
}

TEST_F(SwiftRawDocumentReaderTest, testSwiftReadDelayCounter) {
    _params.kvMap[SRC_SIGNATURE] = "0";
    SwiftRawDocumentReaderPtr reader = createSwiftReader(3);
    int64_t offset;

    for (size_t i = 0; i < 3; ++i) {
        DefaultRawDocument rawDoc(reader->_hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader->read(rawDoc, offset));
        string id = StringUtil::toString(i);
        EXPECT_EQ(id, rawDoc.getField("id"));
    }
    EXPECT_EQ(100, GET_STATE_COUNTER(_params.counterMap, processor.swiftReadDelay)->Get());
}

TEST_F(SwiftRawDocumentReaderTest, testGetMaxTimestampAfterStartTimestamp) {
    FakeSwiftRawDocumentReader reader;
    reader._startTimestamp = 12137;
    int64_t timestamp = 0;
    ASSERT_TRUE(reader.getMaxTimestampAfterStartTimestamp(timestamp));
    ASSERT_EQ(12138, timestamp);
    reader._startTimestamp = 12138;
    ASSERT_TRUE(reader.getMaxTimestampAfterStartTimestamp(timestamp));
    ASSERT_EQ(12138, timestamp);
    reader._startTimestamp = 12139;
    ASSERT_TRUE(reader.getMaxTimestampAfterStartTimestamp(timestamp));
    ASSERT_EQ(-1, timestamp);
}

TEST_F(SwiftRawDocumentReaderTest, testWithCheckpoint) {
    _params.kvMap[CHECKPOINT] = "150";
    SwiftRawDocumentReaderPtr reader = createSwiftReader(5);
    // ts is 0, 100, 200, 300, 400
    // checkpoint is 150, read doc will be 200, 300, 400
    int64_t offset;
    for (size_t i = 0; i < 3; i++) {
        DefaultRawDocument rawDoc(reader->_hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader->read(rawDoc,offset));
        string id = StringUtil::toString(i+2);
        EXPECT_EQ(id, rawDoc.getField("id"));
    }
    DefaultRawDocument rawDoc(reader->_hashMapManager);
    EXPECT_EQ(RawDocumentReader::ERROR_WAIT, reader->read(rawDoc, offset));
}

TEST_F(SwiftRawDocumentReaderTest, testInitReaderWithSuspendTime) {
    _params.kvMap[SWIFT_SUSPEND_TIMESTAMP] = "150";
    SwiftRawDocumentReaderPtr reader = createSwiftReader(5);
    // ts is 0, 100, 200, 300, 400
    // checkpoint is 150, read doc will be 200, 300, 400
    int64_t offset;
    for (size_t i = 0; i < 2; i++) {
        DefaultRawDocument rawDoc(reader->_hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader->read(rawDoc,offset));
        string id = StringUtil::toString(i);
        EXPECT_EQ(id, rawDoc.getField("id"));
    }
    DefaultRawDocument rawDoc(reader->_hashMapManager);
    EXPECT_EQ(RawDocumentReader::ERROR_WAIT, reader->read(rawDoc, offset));
}

TEST_F(SwiftRawDocumentReaderTest, testCreateParser) {
    SwiftRawDocumentReaderPtr reader;
    {
        _params.kvMap[NEED_FIELD_FILTER] = "";
        reader = createSwiftReader(0);
        StandardRawDocumentParser *parser = ASSERT_CAST_AND_RETURN(
                StandardRawDocumentParser, reader->_parser); (void) parser;
    }
    {
        _params.kvMap[NEED_FIELD_FILTER] = "false";
        reader = createSwiftReader(0);
        StandardRawDocumentParser *parser = ASSERT_CAST_AND_RETURN(
                StandardRawDocumentParser, reader->_parser); (void) parser;
    }
    {
        _params.kvMap[READ_SRC_TYPE] = "swift";
        _params.kvMap[NEED_FIELD_FILTER] = "true";
        reader = createSwiftReader(0);
        SwiftFieldFilterRawDocumentParser *parser = ASSERT_CAST_AND_RETURN(
                SwiftFieldFilterRawDocumentParser, reader->_parser); (void) parser;
    }
    {
        _params.kvMap[NEED_FIELD_FILTER] = "xxx unknown";
        MockSwiftRawDocumentReader reader(NULL);
        EXPECT_FALSE(reader.initialize(_params));
    }
}

TEST_F(SwiftRawDocumentReaderTest, testSeek) {
    SwiftRawDocumentReaderPtr reader = createSwiftReader(3);
    int64_t offset;
    {
        DefaultRawDocument rawDoc(reader->_hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader->read(rawDoc, offset));
        string id = StringUtil::toString(0);
        EXPECT_EQ(id, rawDoc.getField("id"));
    }
    {
        DefaultRawDocument rawDoc(reader->_hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader->read(rawDoc, offset));
        string id = StringUtil::toString(1);
        EXPECT_EQ(id, rawDoc.getField("id"));
    }

    SwiftRawDocumentReaderPtr newReader = createSwiftReader(3);
    ASSERT_TRUE(newReader->seek(offset));
    // seek will reread doc 1 for now, because of swift bug.
    for (size_t i = 1; i < 3; i++) {
        DefaultRawDocument rawDoc(reader->_hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, newReader->read(rawDoc, offset));
        string id = StringUtil::toString(i);
        EXPECT_EQ(id, rawDoc.getField("id"));
        rawDoc.clear();
    }
}

TEST_F(SwiftRawDocumentReaderTest, testSuspendReadWithStopTime) {
    SwiftRawDocumentReaderPtr reader = createSwiftReader(10);
    reader->suspendReadAtTimestamp(700, RawDocumentReader::ETA_STOP);

    int64_t offset;
    for (size_t i = 0; i <= 7; i++) {
        DefaultRawDocument rawDoc(reader->_hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader->read(rawDoc, offset));
        string id = StringUtil::toString(i);
        EXPECT_EQ(id, rawDoc.getField("id"));
        ASSERT_FALSE(reader->isEof());
    }

    DefaultRawDocument rawDoc(reader->_hashMapManager);
    ASSERT_EQ(RawDocumentReader::ERROR_EOF, reader->read(rawDoc, offset));
    ASSERT_FALSE(reader->isEof());
}

TEST_F(SwiftRawDocumentReaderTest, testSuspendReadAtTimestamp) {
    _params.kvMap[SWIFT_SUSPEND_TIMESTAMP] = "500";    
    SwiftRawDocumentReaderPtr reader = createSwiftReader(10);
    ASSERT_FALSE(reader->isExceedSuspendTimestamp());    

    int64_t offset;
    for (size_t i = 0; i <= 5; i++) {
        DefaultRawDocument rawDoc(reader->_hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader->read(rawDoc, offset));
        string id = StringUtil::toString(i);
        EXPECT_EQ(id, rawDoc.getField("id"));
    }
    {
        DefaultRawDocument rawDoc(reader->_hashMapManager);    
        ASSERT_EQ(RawDocumentReader::ERROR_WAIT, reader->read(rawDoc, offset));
        ASSERT_TRUE(reader->isExceedSuspendTimestamp());
    }

    reader->suspendReadAtTimestamp(700, RawDocumentReader::ETA_SUSPEND);
    ASSERT_FALSE(reader->isExceedSuspendTimestamp()); 
    for (size_t i = 6; i <= 7; i++) {
        DefaultRawDocument rawDoc(reader->_hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader->read(rawDoc, offset));
        string id = StringUtil::toString(i);
        EXPECT_EQ(id, rawDoc.getField("id"));
    }
    {
        DefaultRawDocument rawDoc(reader->_hashMapManager);    
        ASSERT_EQ(RawDocumentReader::ERROR_WAIT, reader->read(rawDoc, offset)); 
        ASSERT_TRUE(reader->isExceedSuspendTimestamp()); 
    }
    reader->suspendReadAtTimestamp(700, RawDocumentReader::ETA_STOP);
    ASSERT_FALSE(reader->isExceedSuspendTimestamp()); 
    {
        DefaultRawDocument rawDoc(reader->_hashMapManager); 
        ASSERT_EQ(RawDocumentReader::ERROR_EOF, reader->read(rawDoc, offset));
    }
}
    
TEST_F(SwiftRawDocumentReaderTest, testWithStartTimestamp) {
    int64_t offset;
    {
        // ts is 0, 100, 200, 300, 400
        // startTimestamp is 150, checkpoint is 0
        // rawDoc will be 200, 300, 400
        _params.kvMap[SWIFT_START_TIMESTAMP] = "150";
        _params.kvMap[CHECKPOINT] = "0";
        SwiftRawDocumentReaderPtr reader = createSwiftReader(5);
        for (size_t i = 0; i < 3; i++) {
            DefaultRawDocument rawDoc(reader->_hashMapManager);
            ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader->read(rawDoc, offset));
            string id = StringUtil::toString(i+2);
            EXPECT_EQ(id, rawDoc.getField("id"));
        }
        DefaultRawDocument rawDoc(reader->_hashMapManager);
        EXPECT_EQ(RawDocumentReader::ERROR_WAIT, reader->read(rawDoc, offset));
    }

    {
        // ts is 0, 100, 200, 300, 400
        // startTimestamp is 150, checkpoint is 250
        // rawDoc will be 200, 300, 400
        _params.kvMap[SWIFT_START_TIMESTAMP] = "150";
        _params.kvMap[CHECKPOINT] = "250";
        SwiftRawDocumentReaderPtr reader = createSwiftReader(5);
        for (size_t i = 3; i < 5; i++) {
            DefaultRawDocument rawDoc;
            ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader->read(rawDoc, offset));
            string id = StringUtil::toString(i);
            EXPECT_EQ(id, rawDoc.getField("id"));
        }
        DefaultRawDocument rawDoc;
        EXPECT_EQ(RawDocumentReader::ERROR_WAIT, reader->read(rawDoc, offset));
    }

    {
        // ts is 0, 100, 200, 300, 400
        // startTimestamp is 0, checkpoint is 150
        // rawDoc will be 200, 300, 400
        _params.kvMap[SWIFT_START_TIMESTAMP] = "0";
        _params.kvMap[CHECKPOINT] = "150";
        SwiftRawDocumentReaderPtr reader = createSwiftReader(5);
        for (size_t i = 0; i < 3; i++) {
            DefaultRawDocument rawDoc(reader->_hashMapManager);
            ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader->read(rawDoc, offset));
            string id = StringUtil::toString(i+2);
            EXPECT_EQ(id, rawDoc.getField("id"));
        }
        DefaultRawDocument rawDoc(reader->_hashMapManager);
        EXPECT_EQ(RawDocumentReader::ERROR_WAIT, reader->read(rawDoc, offset));
    }
    {
        // ts is 0, 100, 200, 300, 400
        // startTimestamp is 500, checkpoint is 150 
        // no more rawDoc
        _params.kvMap[SWIFT_START_TIMESTAMP] = "500";
        _params.kvMap[CHECKPOINT] = "150";
        SwiftRawDocumentReaderPtr reader = createSwiftReader(5);
        DefaultRawDocument rawDoc(reader->_hashMapManager);
        EXPECT_EQ(RawDocumentReader::ERROR_WAIT, reader->read(rawDoc, offset));
    }
}

TEST_F(SwiftRawDocumentReaderTest, testWithReservedTimestamp) {
    int64_t offset;
    {
        // ha_reserved_timestamp is 90
        // message.timestamp() is 100
        // ha_reserved_timestamp will be 90 after read
        SwiftRawDocumentReaderPtr reader = createSwiftReader(3, 0,
                "topic_name", true, "90");
        for (size_t i = 0; i < 3; i++) {
            DefaultRawDocument rawDoc(reader->_hashMapManager);
            reader->read(rawDoc, offset);
            string id = StringUtil::toString(i);
            EXPECT_EQ(id, rawDoc.getField("id"));
            EXPECT_EQ(true, rawDoc.exist(HA_RESERVED_TIMESTAMP));
            EXPECT_EQ(90, rawDoc.getDocTimestamp());
        }
    }
    {
        // ha_reserved_timestamp is empty
        // docTimestamp will be swift timestamp[0,100,200]
        SwiftRawDocumentReaderPtr reader = createSwiftReader(3, 0,
                "topic_name", true);
        for (size_t i = 0; i < 3; i++) {
            DefaultRawDocument rawDoc(reader->_hashMapManager);
            reader->read(rawDoc, offset);
            string id = StringUtil::toString(i);
            EXPECT_EQ(id, rawDoc.getField("id"));
            EXPECT_EQ(true, rawDoc.exist(HA_RESERVED_TIMESTAMP));
            EXPECT_EQ((int64_t)i*100, rawDoc.getDocTimestamp());
        }
    }
    {
        // ha_reserved_timestamp is invalid
        // docTimestamp will be swift timestamp[0,100,200]
        SwiftRawDocumentReaderPtr reader = createSwiftReader(3, 0, 
                "topic_name", true, "-1");
        for (size_t i = 0; i < 3; i++) {
            DefaultRawDocument rawDoc(reader->_hashMapManager);
            reader->read(rawDoc, offset);
            string id = StringUtil::toString(i);
            EXPECT_EQ(id, rawDoc.getField("id"));
            EXPECT_EQ(true, rawDoc.exist(HA_RESERVED_TIMESTAMP));
            EXPECT_EQ((int64_t)i*100, rawDoc.getDocTimestamp());
        }
    }
    {
        // ha_reserved_timestamp do not exist
        // docTimestamp will be swift timestamp[0,100,200]
        SwiftRawDocumentReaderPtr reader = createSwiftReader(3);
        for (size_t i = 0; i < 3; i++) {
            DefaultRawDocument rawDoc(reader->_hashMapManager);
            reader->read(rawDoc, offset);
            string id = StringUtil::toString(i);
            EXPECT_EQ(id, rawDoc.getField("id"));
            EXPECT_EQ((int64_t)i*100, rawDoc.getDocTimestamp());
        }
    }
}

string SwiftRawDocumentReaderTest::createMessage(const string &pk) {
    stringstream ss;
    ss << "CMD=ADD\n"
       << "id=" << pk << "\n"
       << "subject=Mobile 商务笔记本\n"
       << "\n";
    return ss.str();
}

string SwiftRawDocumentReaderTest::createMessage(const string &pk, const string &timestamp) {
    stringstream ss;
    ss << "CMD=ADD\n"
       << "id=" << pk << "\n"
       << "subject=Mobile 商务笔记本\n"
       << HA_RESERVED_TIMESTAMP << "=" << timestamp << "\n"
       << "\n";
    return ss.str();
}

SwiftRawDocumentReaderPtr SwiftRawDocumentReaderTest::createSwiftReader(
        int32_t docCount, int64_t timestampOffset, const string &topicName, 
        bool hasReservedTimestamp, const string& reservedTimestamp)
{
    MockSwiftClient *mockSwiftClient = new MockSwiftClient;
    MockSwiftRawDocumentReader *reader = new MockSwiftRawDocumentReader(mockSwiftClient);
    NiceMockSwiftReader *mockSwiftReader = new NiceMockSwiftReader;

    ON_CALL(*reader, createSwiftClient(_, _)).WillByDefault(Return(mockSwiftClient));
    ON_CALL(*reader, createSwiftReader(_, _)).WillByDefault(Return(new SwiftReaderWrapper(mockSwiftReader)));
    EXPECT_CALL(*reader, createSwiftClient(_, _));
    EXPECT_CALL(*reader, createSwiftReader(_, _));
    EXPECT_CALL(*reader, getFreshness()).WillRepeatedly(Return(100 * 1000 * 1000));

    for (int32_t i = 0; i < docCount; i++) {
        if (hasReservedTimestamp) {
            mockSwiftReader->addMessage(
                    createMessage(StringUtil::toString(i), reservedTimestamp),
                    i * 100,
                    i * 100 + timestampOffset);
        } else {
            mockSwiftReader->addMessage(
                    createMessage(StringUtil::toString(i)),
                    i * 100,
                    i * 100 + timestampOffset);
        }
    }

    if (!reader->initialize(_params)) {
        return SwiftRawDocumentReaderPtr();
    }

    return SwiftRawDocumentReaderPtr(reader);
}

}
}
