#include <autil/StringUtil.h>
#include "build_service/test/unittest.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "build_service/reader/test/MockSwiftReader.h"
#include "build_service/document/test/DocumentTestHelper.h"
#include "build_service/workflow/SwiftProcessedDocConsumer.h"
#include "build_service/workflow/test/CustomizedDocParser.h"
#include <indexlib/config/index_partition_schema_maker.h>
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>
#include <indexlib/document/document_parser/normal_parser/normal_document_parser.h>

using namespace std;
using namespace testing;
using namespace autil;

SWIFT_USE_NAMESPACE(protocol);
SWIFT_USE_NAMESPACE(client);

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);

using namespace build_service::document;
using namespace build_service::reader;
using namespace build_service::common;
using namespace build_service::config;

namespace build_service {
namespace workflow {

class SwiftProcessedDocProducerTest : public BUILD_SERVICE_TESTBASE {
protected:
    string createDocStr(const string &pkStr);
    void checkDocument(int32_t from, int32_t to,
                       ProcessedDocProducer *producer);
    MockSwiftReader *createSwiftReader(int32_t docCount);
public:
    void setUp() {
        _counterMap.reset(new IE_NAMESPACE(util)::CounterMap);
        _startTimestamp = 0;
        _stopTimestamp = numeric_limits<int64_t>::max();
        _maxCommitInterval = 10;
        _noMoreMsgPeriod = 100000000;

        _schema.reset(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(
            _schema, "int:INT32", "pk:primarykey64:int", "int", "" );
    }
    void tearDown() {
    }
private:
    int64_t _startTimestamp;
    int64_t _stopTimestamp;
    IE_NAMESPACE(util)::CounterMapPtr _counterMap;
    int64_t _noMoreMsgPeriod;
    int64_t _maxCommitInterval;
    IndexPartitionSchemaPtr _schema;
};

class FakeDocumentForTest : public IE_NAMESPACE(document)::NormalDocument
{
};
DEFINE_SHARED_PTR(FakeDocument);
    
class FakeDocumentParser : public IE_NAMESPACE(document)::DocumentParser
{
public:
    FakeDocumentParser()
        : DocumentParser(IE_NAMESPACE(config)::IndexPartitionSchemaPtr())
    {}
    ~FakeDocumentParser() {}
public:
    bool DoInit() { return true; }
    IE_NAMESPACE(document)::DocumentPtr Parse(
        const IndexlibExtendDocumentPtr& extendDoc)
    { return DocumentPtr(); }
    // dataStr: serialized data which from call document serialize interface
    IE_NAMESPACE(document)::DocumentPtr Parse(
        const autil::ConstString& serializedData)
    { return DocumentPtr(new FakeDocumentForTest); }
};

TEST_F(SwiftProcessedDocProducerTest, testSimple) {
    MockSwiftReader *reader = createSwiftReader(2);
    SwiftProcessedDocProducer producer(reader, _schema, "");
    ASSERT_TRUE(producer.init(NULL, "",
                              _counterMap, _startTimestamp, _stopTimestamp,
                              _noMoreMsgPeriod, _maxCommitInterval));

    for (int32_t i = 0; i < 2; ++i) {
        FakeDocument fakeDoc(StringUtil::toString(i));
        ProcessedDocumentVecPtr processedDocVecPtr;
        //EXPECT_CALL(*reader, getSwiftPartitionStatus()).WillOnce(Return(status));
        ASSERT_EQ(FE_OK, producer.produce(processedDocVecPtr));
        ASSERT_TRUE(processedDocVecPtr);
        ASSERT_EQ(size_t(1), processedDocVecPtr->size());
        DocumentTestHelper::checkDocument(fakeDoc, (*processedDocVecPtr)[0]->getDocument());
    }
    producer.stop();
    ProcessedDocumentVecPtr processedDocVecPtr;
    ASSERT_EQ(FE_WAIT, producer.produce(processedDocVecPtr));
}

TEST_F(SwiftProcessedDocProducerTest, testSeek) {
    MockSwiftReader *reader = createSwiftReader(3);

    SwiftProcessedDocProducer producer(reader, _schema, "testSeek");
    ASSERT_TRUE(producer.init(NULL, "",
                              _counterMap, _startTimestamp, _stopTimestamp,
                              _noMoreMsgPeriod, _maxCommitInterval));

    ProcessedDocumentVecPtr processedDocVecPtr;

    {
        //EXPECT_CALL(*reader, getSwiftPartitionStatus()).WillOnce(Return(status));
        ASSERT_EQ(FE_OK, producer.produce(processedDocVecPtr));
        ASSERT_TRUE(processedDocVecPtr);
        ASSERT_EQ(size_t(1), processedDocVecPtr->size());
        FakeDocument fakeDoc("0");
        DocumentTestHelper::checkDocument(fakeDoc, (*processedDocVecPtr)[0]->getDocument());
    }
    {
        //EXPECT_CALL(*reader, getSwiftPartitionStatus()).WillOnce(Return(status));
        ASSERT_EQ(FE_OK, producer.produce(processedDocVecPtr));
        ASSERT_TRUE(processedDocVecPtr);
        ASSERT_EQ(size_t(1), processedDocVecPtr->size());
        FakeDocument fakeDoc("1");
        DocumentTestHelper::checkDocument(fakeDoc, (*processedDocVecPtr)[0]->getDocument());
    }

    MockSwiftReader *newReader = createSwiftReader(3);
    SwiftProcessedDocProducer newProducer(newReader, _schema, "");
    ASSERT_TRUE(newProducer.init(NULL, "",
                              _counterMap, _startTimestamp, _stopTimestamp,
                              _noMoreMsgPeriod, _maxCommitInterval));


    common::Locator locator = (*processedDocVecPtr)[0]->getLocator();
    newProducer.seek(locator);
    //EXPECT_CALL(*newReader, getSwiftPartitionStatus()).WillOnce(Return(status));
    ASSERT_EQ(FE_OK, newProducer.produce(processedDocVecPtr));
    ASSERT_TRUE(processedDocVecPtr);
    ASSERT_EQ(size_t(1), processedDocVecPtr->size());
    // seek will reread doc 1 for now, because of swift bug.
    FakeDocument fakeDoc("1");
    DocumentTestHelper::checkDocument(fakeDoc, (*processedDocVecPtr)[0]->getDocument());
}

TEST_F(SwiftProcessedDocProducerTest, testException) {
    MockSwiftReader *reader = new NiceMockSwiftReader;
    ON_CALL(*reader, read(_, _, _)).WillByDefault(Return(ERROR_CLIENT_READ_MESSAGE_TIMEOUT));
    SwiftProcessedDocProducer producer(reader, _schema, "");
    ASSERT_TRUE(producer.init(NULL, "",
                              _counterMap, _startTimestamp, _stopTimestamp,
                              _noMoreMsgPeriod, _maxCommitInterval));

    ProcessedDocumentVecPtr docVec;
    EXPECT_EQ(FE_EXCEPTION, producer.produce(docVec));
}

TEST_F(SwiftProcessedDocProducerTest, testSuspendAndResume) {
    MockSwiftReader *reader = new NiceMockSwiftReader;
    SwiftProcessedDocProducer producer(reader, _schema, "");
    ASSERT_TRUE(producer.init(NULL, "",
                              _counterMap, _startTimestamp, _stopTimestamp,
                              _noMoreMsgPeriod, _maxCommitInterval));


    EXPECT_CALL(*reader, setTimestampLimit(_, _)).WillOnce(SetArgReferee<1>(2));
    EXPECT_EQ(int64_t(2), producer.suspendReadAtTimestamp(1));
    EXPECT_CALL(*reader, setTimestampLimit(numeric_limits<int64_t>::max(), _));
    producer.resumeRead();
}

TEST_F(SwiftProcessedDocProducerTest, testCreateProcessedDocument) {
    MockSwiftReader *reader = new NiceMockSwiftReader;
    SwiftProcessedDocProducer producer(reader, _schema, "");
    ASSERT_TRUE(producer.init(NULL, "",
                              _counterMap, _startTimestamp, _stopTimestamp,
                              _noMoreMsgPeriod, _maxCommitInterval));

    producer._startTimestamp = 1024;

    FakeDocument fakeDoc("pk", ADD_DOC, true, true, 0, 0, 3);
    DocumentPtr document = DocumentTestHelper::createDocument(fakeDoc);
    document->SetTimestamp(100);
    string docStr = SwiftProcessedDocConsumer::transToDocStr(document);

    //EXPECT_CALL(*reader, getSwiftPartitionStatus()).WillOnce(Return(status));
    ProcessedDocumentVec *processedDocumentVec = producer.createProcessedDocument(docStr, 190, 200);

    NormalDocumentPtr resultDoc =
        DYNAMIC_POINTER_CAST(NormalDocument,
                             (*processedDocumentVec)[0]->getDocument());
    EXPECT_EQ(190, resultDoc->GetTimestamp());
    EXPECT_EQ(100, resultDoc->GetUserTimestamp());
    EXPECT_EQ(200, (*processedDocumentVec)[0]->getLocator().getOffset());
    EXPECT_EQ((uint64_t)1024, (*processedDocumentVec)[0]->getLocator().getSrc());
    EXPECT_EQ(size_t(3), resultDoc->GetSubDocuments().size());
    EXPECT_EQ(190, resultDoc->GetSubDocuments()[0]->GetTimestamp());
    EXPECT_EQ(190, resultDoc->GetSubDocuments()[1]->GetTimestamp());
    EXPECT_EQ(190 , resultDoc->GetSubDocuments()[2]->GetTimestamp());
    delete processedDocumentVec;
}

TEST_F(SwiftProcessedDocProducerTest, testSwiftMessageCorrupted) {
    MockSwiftReader *reader = new NiceMockSwiftReader;
    reader->addMessage("corrupted message", 0, 0);
    string swiftReaderStr = createDocStr("1");
    reader->addMessage(swiftReaderStr, 1, 1);
    SwiftProcessedDocProducer producer(reader, _schema, "");
    ASSERT_TRUE(producer.init(NULL, "",
                              _counterMap, _startTimestamp, _stopTimestamp,
                              _noMoreMsgPeriod, _maxCommitInterval));

    ProcessedDocumentVecPtr processedDocVecPtr;
    //EXPECT_CALL(*reader, getSwiftPartitionStatus()).WillOnce(Return(status));
    EXPECT_EQ(FE_EXCEPTION, producer.produce(processedDocVecPtr));
    EXPECT_FALSE(processedDocVecPtr);
    //EXPECT_CALL(*reader, getSwiftPartitionStatus()).WillOnce(Return(status));
    EXPECT_EQ(FE_OK, producer.produce(processedDocVecPtr));
    EXPECT_TRUE(processedDocVecPtr);
    ASSERT_EQ(size_t(1), processedDocVecPtr->size());
    FakeDocument fakeDoc("1");
    DocumentTestHelper::checkDocument(fakeDoc, (*processedDocVecPtr)[0]->getDocument());
}

ACTION_P3(SET_AND_RETURN, param0, param1, ret) {
    arg0 = param0;
    arg1 = param1;
    return ret;
}

TEST_F(SwiftProcessedDocProducerTest, testBlock) {
    auto reader = new NiceMockSwiftReader;
    SwiftProcessedDocProducer producer(reader, _schema, "");
    ASSERT_TRUE(producer.init(NULL, "",
                              _counterMap, _startTimestamp, _stopTimestamp,
                              _noMoreMsgPeriod, _maxCommitInterval));

    EXPECT_CALL(*reader, read(_, _, _))
        .WillOnce(Return(ERROR_CLIENT_NO_MORE_MESSAGE))
        .WillOnce(Return(ERROR_CLIENT_NO_MORE_MESSAGE));
    producer._noMoreMsgPeriod = 2 * 1000000; // 2 seconds

    ProcessedDocumentVecPtr processedDocVecPtr;
    ASSERT_EQ(FE_WAIT, producer.produce(processedDocVecPtr));
    sleep(2);
    ASSERT_EQ(FE_WAIT, producer.produce(processedDocVecPtr));
    ASSERT_TRUE(producer.needUpdateCommittedCheckpoint());

    string pkStr = StringUtil::toString(0);
    string swiftReaderStr = createDocStr(pkStr);

    MockSwiftReader::Message message;
    message.set_data(swiftReaderStr);
    message.set_timestamp(0);

    EXPECT_CALL(*reader, read(_, _, _))
        .WillOnce(SET_AND_RETURN(0, message, ERROR_NONE));
    ASSERT_EQ(FE_OK, producer.produce(processedDocVecPtr));
    ASSERT_FALSE(producer.needUpdateCommittedCheckpoint());
}

TEST_F(SwiftProcessedDocProducerTest, testIntervalCommit) {
    auto reader = new NiceMockSwiftReader;
    SwiftProcessedDocProducer producer(reader, _schema, "");
    ASSERT_TRUE(producer.init(NULL, "",
                              _counterMap, _startTimestamp, _stopTimestamp,
                              _noMoreMsgPeriod, _maxCommitInterval));

    producer._noMoreMsgPeriod = std::numeric_limits<int64_t>::max(); // never
    producer._maxCommitInterval = 5 * 1000 * 1000; // 5s


    MockSwiftReader::Message message;
    string pkStr = StringUtil::toString(0);
    string swiftReaderStr = createDocStr(pkStr);
    message.set_data(swiftReaderStr);
    message.set_timestamp(0);

    auto curTs = TimeUtility::currentTime();
    EXPECT_CALL(*reader, read(_, _, _))
        .WillOnce(SET_AND_RETURN(curTs, message, ERROR_NONE));

    EXPECT_CALL(*reader, updateCommittedCheckpoint(curTs))
        .WillRepeatedly(Return(true));

    ProcessedDocumentVecPtr processedDocVecPtr;
    ASSERT_EQ(FE_OK, producer.produce(processedDocVecPtr));

    ASSERT_FALSE(producer.needUpdateCommittedCheckpoint());
    ASSERT_TRUE(producer.updateCommittedCheckpoint(curTs));
    ASSERT_FALSE(producer.needUpdateCommittedCheckpoint());
    sleep(5);
    ASSERT_TRUE(producer.needUpdateCommittedCheckpoint());

    // slow read
    EXPECT_CALL(*reader, read(_, _, _))
        .WillOnce(SET_AND_RETURN(curTs - SwiftProcessedDocProducer::MAX_READ_DELAY,
                        message, ERROR_NONE));
    ASSERT_EQ(FE_OK, producer.produce(processedDocVecPtr));
    ASSERT_FALSE(producer.needUpdateCommittedCheckpoint());
    ASSERT_TRUE(producer.updateCommittedCheckpoint(curTs));
    ASSERT_FALSE(producer.needUpdateCommittedCheckpoint());
}

TEST_F(SwiftProcessedDocProducerTest, testCreateCustomizedProcessedDoc) {
    MockSwiftReader *reader = new NiceMockSwiftReader;
    int32_t docCount = 20;
    for (int32_t i = 0; i < docCount; i++) {
        MyDocumentPtr doc(new MyDocument);
        doc->SetDocId(i);
        doc->SetData(i * 10);
        string swiftReaderStr = SwiftProcessedDocConsumer::transToDocStr(doc);
        reader->addMessage(swiftReaderStr, i * 100, i * 100);
    }

    string configPath = TEST_DATA_PATH"/swift_broker_factory_test/customized_doc_parser_config/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));
    IndexPartitionSchemaPtr schema = resourceReader->getSchema("simple_cluster");
    SwiftProcessedDocProducer producer(reader, schema, "");
    ASSERT_TRUE(producer.init(NULL, resourceReader->getPluginPath(),
                              _counterMap, _startTimestamp, _stopTimestamp,
                              _noMoreMsgPeriod, _maxCommitInterval));
    
    for (int32_t i = 0; i < docCount; ++i) {
        ProcessedDocumentVecPtr processedDocVecPtr;
        ASSERT_EQ(FE_OK, producer.produce(processedDocVecPtr));
        ASSERT_TRUE(processedDocVecPtr);
        ASSERT_EQ(size_t(1), processedDocVecPtr->size());
        MyDocumentPtr doc =
            DYNAMIC_POINTER_CAST(MyDocument, (*processedDocVecPtr)[0]->getDocument());
        ASSERT_TRUE(doc);
        ASSERT_EQ((docid_t)i, doc->GetDocId());
        ASSERT_EQ((int64_t)i * 10, doc->GetData());
    }
    producer.stop();
    ProcessedDocumentVecPtr processedDocVecPtr;
    ASSERT_EQ(FE_WAIT, producer.produce(processedDocVecPtr));
}

TEST_F(SwiftProcessedDocProducerTest, testTransDocStrToDocument) {
    MockSwiftReader *reader = new NiceMockSwiftReader;
    SwiftProcessedDocProducer producer(reader, IndexPartitionSchemaPtr(), "");
    ASSERT_FALSE(producer.transDocStrToDocument(""));
    producer._docParser.reset(new FakeDocumentParser);
    DocumentPtr doc = producer.transDocStrToDocument("");
    ASSERT_TRUE(doc);
    ASSERT_TRUE(DYNAMIC_POINTER_CAST(FakeDocumentForTest, doc));
}

TEST_F(SwiftProcessedDocProducerTest, testInitDocumentParser) {
    {
        // init without schema
        SwiftProcessedDocProducer producer(NULL, IndexPartitionSchemaPtr(), "");
        ASSERT_FALSE(producer.initDocumentParser(NULL, _counterMap, ""));
    }
    {
        // init wrapper failed
        SwiftProcessedDocProducer producer(NULL, _schema, "");
        CustomizedConfigPtr docParserConfig(new CustomizedConfig);
        docParserConfig->SetId(CUSTOMIZED_DOCUMENT_CONFIG_PARSER);
        CustomizedConfigVector configs;
        configs.push_back(docParserConfig);
        _schema->SetCustomizedDocumentConfig(configs);
        ASSERT_FALSE(producer.initDocumentParser(NULL, _counterMap, ""));
    }
    {
        // init customized doc parser success
        SwiftProcessedDocProducer producer(
            NULL, _schema,
            TEST_DATA_PATH"/swift_broker_factory_test/customized_doc_parser_config/");
        CustomizedConfigPtr docParserConfig(new CustomizedConfig);
        docParserConfig->SetId(CUSTOMIZED_DOCUMENT_CONFIG_PARSER);
        docParserConfig->SetPluginName("customized_doc_parser_for_workflow");
        CustomizedConfigVector configs;
        configs.push_back(docParserConfig);
        _schema->SetCustomizedDocumentConfig(configs);
        ASSERT_TRUE(producer.initDocumentParser(NULL, _counterMap, ""));
        MyDocumentParserPtr parser = DYNAMIC_POINTER_CAST(MyDocumentParser,
                                                          producer._docParser);
        ASSERT_TRUE(parser);
    }
    {
        // init builtin parser
        SwiftProcessedDocProducer producer(
            NULL, _schema, "");
        CustomizedConfigVector configs;
        _schema->SetCustomizedDocumentConfig(configs);
        ASSERT_TRUE(producer.initDocumentParser(NULL, _counterMap, ""));
        NormalDocumentParserPtr parser = DYNAMIC_POINTER_CAST(NormalDocumentParser,
                                                              producer._docParser);
        ASSERT_TRUE(parser);
    }
}

string SwiftProcessedDocProducerTest::createDocStr(const string &pkStr) {
    FakeDocument fakeDoc(pkStr);
    DocumentPtr document = DocumentTestHelper::createDocument(fakeDoc);
    return SwiftProcessedDocConsumer::transToDocStr(document);
}

MockSwiftReader *SwiftProcessedDocProducerTest::createSwiftReader(int32_t docCount) {
    MockSwiftReader *reader = new NiceMockSwiftReader;
    for (int32_t i = 0; i < docCount; i++) {
        string pkStr = StringUtil::toString(i);
        string swiftReaderStr = createDocStr(pkStr);
        reader->addMessage(swiftReaderStr, i * 100, i * 100);
    }
    return reader;
}

void SwiftProcessedDocProducerTest::checkDocument(int32_t from, int32_t to,
        ProcessedDocProducer *producer)
{
    for (int32_t i = from; i < to; ++i) {
        FakeDocument fakeDoc(StringUtil::toString(i));
        ProcessedDocumentVecPtr processedDocVecPtr;
        ASSERT_EQ(FE_OK, producer->produce(processedDocVecPtr));
        ASSERT_TRUE(processedDocVecPtr);
        ASSERT_EQ(size_t(1), processedDocVecPtr->size());
        DocumentTestHelper::checkDocument(fakeDoc, (*processedDocVecPtr)[0]->getDocument());
    }
    producer->stop();
    ProcessedDocumentVecPtr processedDocVecPtr;
    ASSERT_EQ(FE_EXCEPTION, producer->produce(processedDocVecPtr));
}

}
}
