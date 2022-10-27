#include "build_service/test/unittest.h"
#include "build_service/workflow/SwiftBrokerFactory.h"
#include "build_service/reader/test/MockSwiftClientCreator.h"
#include "build_service/reader/test/MockSwiftReader.h"
#include "build_service/workflow/test/MockSwiftWriter.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "build_service/workflow/SwiftProcessedDocConsumer.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/common/CounterFileSynchronizer.h"
#include "build_service/common/CounterSynchronizerCreator.h"
#include <autil/StringTokenizer.h>
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/test/schema_maker.h>
#include "build_service/proto/DataDescriptions.h"
#include "CustomizedDocParser.h"

using namespace std;
using namespace testing;
using namespace autil;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(config);

SWIFT_USE_NAMESPACE(client);
SWIFT_USE_NAMESPACE(protocol);

using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::reader;
using namespace build_service::common;
using namespace build_service::util;
using namespace autil::legacy;

namespace build_service {
namespace workflow {

class SwiftBrokerFactoryTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp() {
        _initParam.kvMap[PROCESSED_DOC_SWIFT_ROOT] = "zfs://processed_doc_swift_root/";
        _initParam.kvMap[PROCESSED_DOC_SWIFT_TOPIC_PREFIX] = "user_name_special_service_name";
        _initParam.kvMap[PROCESSED_DOC_SWIFT_READER_CONFIG] = "swift_reader_config";
        _initParam.kvMap[PROCESSED_DOC_SWIFT_WRITER_CONFIG] = "swift_writer_config";
        _initParam.kvMap[SRC_SIGNATURE] = "0";

        _initParam.partitionId.mutable_range()->set_from(22222);
        _initParam.partitionId.mutable_range()->set_to(33333);
        *_initParam.partitionId.add_clusternames() = "cluster1";
        string configPath = TEST_DATA_PATH"/swift_broker_factory_test/config/";
        _initParam.resourceReader.reset(new ResourceReader(configPath));
        _initParam.partitionId.mutable_buildid()->set_datatable("simple");
        _initParam.partitionId.mutable_buildid()->set_generationid(10);

        string counterFilePath = GET_TEST_DATA_PATH() +  "/counters";
        CounterConfigPtr counterConfig(new CounterConfig());
        counterConfig->position = "zookeeper";
        counterConfig->params[COUNTER_PARAM_FILEPATH] = counterFilePath;

        bool loadFromExisted;
        IE_NAMESPACE(util)::CounterMapPtr counterMap = CounterSynchronizerCreator::loadCounterMap(
                counterConfig, loadFromExisted);
        CounterSynchronizerPtr syner(CounterSynchronizerCreator::create(
                        counterMap, counterConfig));
        _initParam.counterSynchronizer = syner;
        _initParam.counterMap = syner->getCounterMap();
        
        string field = "string1:string;string2:string;price:uint32";
        string index = "index2:string:string2;"
            "pk:primarykey64:string1";
        string attribute = "string1;price";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
        _initParam.schema = schema;
        
        DataDescription ds;
        ds[READ_SRC] = "swift";
        ds[READ_SRC_TYPE] = "swift";
        _initParam.kvMap[DATA_DESCRIPTION_KEY] = ToJsonString(ds);
    }
protected:
    BrokerFactory::RoleInitParam _initParam;
};

class SwiftBrokerFactoryCreateProducerTest : public SwiftBrokerFactoryTest {
protected:
    void setUp() {
        SwiftBrokerFactoryTest::setUp();
        _initParam.partitionId.set_role(ROLE_BUILDER);
    }
protected:

    static SwiftReader *createReader(
            const string &topicName,
            const Filter &filter,
            const string &readerConfig,
            const string &expectTopicName,
            const proto::Range &partRange,
            const string &expectReaderConfig);
    static void checkReaderParam(const string &topicName,
                                 const Filter &filter,
                                 const string &readerConfig,
                                 const string &expectTopicName,
                                 const proto::Range &partRange,
                                 const string &expectReaderConfig);

};

class SwiftBrokerFactoryCreateConsumerTest : public SwiftBrokerFactoryTest {
protected:
    void setUp() {
        SwiftBrokerFactoryTest::setUp();
        _initParam.kvMap[CHECKPOINT_ROOT] = GET_TEST_DATA_PATH();
        _initParam.kvMap[READ_SRC] = "swift";
        _initParam.partitionId.set_role(ROLE_PROCESSOR);
    }
protected:
    static SwiftWriter *createWriter(const string &topicName, const string &writerConfig,
            vector<string> *allTopicNames);
    static void checkWriterParam(const string &writerConfig, const string &clusterName);
};

class MockSwiftBrokerFactory : public SwiftBrokerFactory {
public:
    MockSwiftBrokerFactory()
        : SwiftBrokerFactory(SwiftClientCreatorPtr(new SwiftClientCreator))
    {
        writerSucceedCount = 1000;
        ON_CALL(*this, createClient(_,_)).WillByDefault(
                Invoke(std::bind(&MockSwiftBrokerFactory::createMockClientWithConfig,
                                this, std::placeholders::_1, std::placeholders::_2)));
        ON_CALL(*this, createReader(_, _)).WillByDefault(
                Invoke(std::bind(MockSwiftBrokerFactory::createReaderForMock)));
        ON_CALL(*this, createWriter(_, _, _)).WillByDefault(
                Invoke(std::bind(MockSwiftBrokerFactory::createWriterForMock, &writerSucceedCount)));
    }
    ~MockSwiftBrokerFactory() {}
public:
    MOCK_METHOD2(createClient, SwiftClient*(const string &, const string& swiftClientConfig));
    MOCK_METHOD2(createReader, SwiftReader*(SWIFT_NS(client)::SwiftClient* client, const string &));
    MOCK_METHOD3(createWriter, SwiftWriter*(SWIFT_NS(client)::SwiftClient* client,
                    const string &, const string &));
public:
    SwiftClient* createMockClientWithConfig(const std::string& zkPath,
            const std::string& clientConfig) {
        std::string config = "zkPath=" + zkPath;
        if (!clientConfig.empty()) {
            config += ";" + clientConfig;
        }
        if (!_lastSwiftConfig.empty()) {
            _lastSwiftConfig += "#";
        }
        _lastSwiftConfig += config;
        _swiftClientPtr.reset(new NiceMockSwiftClient());
        return _swiftClientPtr.get();
    }
    
    static SwiftReader *createReaderForMock() {
        return new NiceMockSwiftReader;
    }
    static SwiftWriter *createWriterForMock(int *succeedCount) {
        if (*succeedCount > 0) {
            (*succeedCount)--;
            return new NiceMockSwiftWriter;
        }
        return NULL;
    }
    const string& getLastSwiftConfig() {
        return _lastSwiftConfig;
    }
private:
    std::unique_ptr<SwiftClient> _swiftClientPtr;
    int32_t writerSucceedCount;
    string _lastSwiftConfig;
};

typedef StrictMock<MockSwiftBrokerFactory> StrictMockSwiftBrokerFactory;


TEST_F(SwiftBrokerFactoryTest, testInitCounterMap) {
    BrokerFactory::RoleInitParam initParam;
    SwiftClientCreatorPtr swiftClientCreator(new SwiftClientCreator());
    SwiftBrokerFactory factory(swiftClientCreator);

    // initParam does not specify COUNTER_CONFIG_JSON_STR
    EXPECT_FALSE(factory.initCounterMap(initParam));

    string counterFilePath = GET_TEST_DATA_PATH() + "app.simple.33.processor.full.0.666";
    CounterConfig counterConfig;
    counterConfig.position = "zookeeper";
    counterConfig.params[COUNTER_PARAM_FILEPATH] = counterFilePath;
    initParam.kvMap[COUNTER_CONFIG_JSON_STR] = ToJsonString(counterConfig);
    initParam.partitionId.mutable_range()->set_from(0);
    initParam.partitionId.mutable_range()->set_to(666);
    initParam.partitionId.mutable_buildid()->set_datatable("simple");
    initParam.partitionId.mutable_buildid()->set_generationid(33); 
    initParam.partitionId.mutable_buildid()->set_appname("app");
    initParam.partitionId.set_role(ROLE_PROCESSOR);
    initParam.partitionId.set_taskid("1");    
    *initParam.partitionId.add_clusternames() = "good_cluster";
    
    EXPECT_TRUE(factory.initCounterMap(initParam));
    EXPECT_TRUE(initParam.counterMap);
    ASSERT_TRUE(initParam.counterSynchronizer);
    auto counterFileSynchronizer = dynamic_pointer_cast<CounterFileSynchronizer>(
            initParam.counterSynchronizer);
    ASSERT_TRUE(counterFileSynchronizer);
    EXPECT_EQ(counterFilePath, counterFileSynchronizer->_counterFilePath);
}

TEST_F(SwiftBrokerFactoryCreateProducerTest, testCreateProducer) {
    StrictMockSwiftBrokerFactory factory;

    EXPECT_CALL(factory, createClient(_,_));

    _initParam.kvMap[CHECKPOINT] = "123456";
    _initParam.kvMap[PROCESSED_DOC_SWIFT_STOP_TIMESTAMP] = "234567";

    NiceMockSwiftReader *reader = new NiceMockSwiftReader;
    EXPECT_CALL(*reader, seekByTimestamp(123456, false))
        .WillOnce(Return(SWIFT_NS(protocol)::ERROR_NONE));
    EXPECT_CALL(*reader, setTimestampLimit(234567, _));

    EXPECT_CALL(factory, createReader(_, _)).WillOnce(Return(reader));

    unique_ptr<ProcessedDocProducer> producer(factory.createProcessedDocProducer(_initParam));
    ASSERT_TRUE(producer.get());
    SwiftProcessedDocProducer *swiftProducer = dynamic_cast<SwiftProcessedDocProducer*>(producer.get());
    ASSERT_TRUE(swiftProducer);
    ASSERT_EQ((int64_t)123456, swiftProducer->_startTimestamp);
    ASSERT_EQ(factory.getLastSwiftConfig(),
              "zkPath=zfs://processed_doc_swift_root/;cluster1_default_client_config");
}

TEST_F(SwiftBrokerFactoryCreateProducerTest, testCreateProducerWithDifferentTopicConfigs) {
    StrictMockSwiftBrokerFactory factory;
    BrokerFactory::RoleInitParam initParam = _initParam;
    string configPath = TEST_DATA_PATH"/swift_broker_factory_test/config_diff_topics/";
    initParam.resourceReader.reset(new ResourceReader(configPath));
    initParam.kvMap[CHECKPOINT] = "123456";
    initParam.kvMap[PROCESSED_DOC_SWIFT_STOP_TIMESTAMP] = "234567";
    {
        initParam.partitionId.set_step(BUILD_STEP_FULL);
        EXPECT_CALL(factory, createClient(_,_));
        NiceMockSwiftReader *reader = new NiceMockSwiftReader;
        EXPECT_CALL(*reader, seekByTimestamp(123456, false))
            .WillOnce(Return(SWIFT_NS(protocol)::ERROR_NONE));
        EXPECT_CALL(*reader, setTimestampLimit(234567, _));
        string expectTopicName = ProtoUtil::getProcessedDocTopicName(
                initParam.kvMap[PROCESSED_DOC_SWIFT_TOPIC_PREFIX] + "_full",
                initParam.partitionId.buildid(),
                initParam.partitionId.clusternames(0));
        stringstream ss;
        ss << "topicName=" << expectTopicName << ";";
        ss << "from=" << initParam.partitionId.range().from() << ";"
           << "to=" << initParam.partitionId.range().to() << ";"
           << "uint8FilterMask=" << (uint32_t)initParam.swiftFilterMask << ";"
           << "uint8MaskResult=" << (uint32_t)initParam.swiftFilterResult;
        string swiftReadConfig = ss.str();
        
        EXPECT_CALL(factory, createReader(_,swiftReadConfig)).WillOnce(Return(reader));
        unique_ptr<ProcessedDocProducer> producer(factory.createProcessedDocProducer(initParam));
        ASSERT_TRUE(producer.get());
        SwiftProcessedDocProducer *swiftProducer = dynamic_cast<SwiftProcessedDocProducer*>(producer.get());
        ASSERT_TRUE(swiftProducer);
    }
    {
        initParam.partitionId.set_step(BUILD_STEP_INC);
        NiceMockSwiftReader *reader = new NiceMockSwiftReader;
        EXPECT_CALL(*reader, seekByTimestamp(123456, false))
            .WillOnce(Return(SWIFT_NS(protocol)::ERROR_NONE));
        EXPECT_CALL(factory, createClient(_,_));
        EXPECT_CALL(*reader, setTimestampLimit(234567, _));        
        string expectTopicName = ProtoUtil::getProcessedDocTopicName(
                initParam.kvMap[PROCESSED_DOC_SWIFT_TOPIC_PREFIX],
                initParam.partitionId.buildid(),
                initParam.partitionId.clusternames(0));

        stringstream ss;
        ss << "topicName=" << expectTopicName << ";";
        ss << "from=" << initParam.partitionId.range().from() << ";"
           << "to=" << initParam.partitionId.range().to() << ";"
           << "uint8FilterMask=" << (uint32_t)initParam.swiftFilterMask << ";"
           << "uint8MaskResult=" << (uint32_t)initParam.swiftFilterResult;
        string swiftReaderConfig = ss.str();
        EXPECT_CALL(factory, createReader(_,swiftReaderConfig)).WillOnce(Return(reader));
        unique_ptr<ProcessedDocProducer> producer(factory.createProcessedDocProducer(initParam));
        ASSERT_TRUE(producer.get());
        SwiftProcessedDocProducer *swiftProducer = dynamic_cast<SwiftProcessedDocProducer*>(producer.get());
        ASSERT_TRUE(swiftProducer);        
    }
    {
        // test shared topic
        initParam.partitionId.set_step(BUILD_STEP_INC);
        initParam.kvMap[SHARED_TOPIC_CLUSTER_NAME] = "shared_topic";
        NiceMockSwiftReader *reader = new NiceMockSwiftReader;
        EXPECT_CALL(*reader, seekByTimestamp(123456, false))
            .WillOnce(Return(SWIFT_NS(protocol)::ERROR_NONE));
        EXPECT_CALL(factory, createClient(_,_));
        EXPECT_CALL(*reader, setTimestampLimit(234567, _));        
        string expectTopicName = ProtoUtil::getProcessedDocTopicName(
                initParam.kvMap[PROCESSED_DOC_SWIFT_TOPIC_PREFIX],
                initParam.partitionId.buildid(), "shared_topic");

        stringstream ss;
        ss << "topicName=" << expectTopicName << ";";
        ss << "from=" << initParam.partitionId.range().from() << ";"
           << "to=" << initParam.partitionId.range().to() << ";"
           << "uint8FilterMask=" << (uint32_t)initParam.swiftFilterMask << ";"
           << "uint8MaskResult=" << (uint32_t)initParam.swiftFilterResult;
        string swiftReaderConfig = ss.str();
        EXPECT_CALL(factory, createReader(_,swiftReaderConfig)).WillOnce(Return(reader));
        unique_ptr<ProcessedDocProducer> producer(factory.createProcessedDocProducer(initParam));
        ASSERT_TRUE(producer.get());
        SwiftProcessedDocProducer *swiftProducer = dynamic_cast<SwiftProcessedDocProducer*>(producer.get());
        ASSERT_TRUE(swiftProducer);        
    }    
}


TEST_F(SwiftBrokerFactoryCreateProducerTest, testCreateProducerSeekFailed) {
    StrictMockSwiftBrokerFactory factory;

    EXPECT_CALL(factory, createClient(_,_));

    _initParam.kvMap[CHECKPOINT] = "123456";
    _initParam.kvMap[PROCESSED_DOC_SWIFT_STOP_TIMESTAMP] = "234567";

    NiceMockSwiftReader *reader = new NiceMockSwiftReader;
    EXPECT_CALL(*reader, seekByTimestamp(123456, false))
        .WillOnce(Return(SWIFT_NS(protocol)::ERROR_BROKER_BUSY));
    EXPECT_CALL(*reader, setTimestampLimit(234567, _)).Times(0);

    EXPECT_CALL(factory, createReader(_,_)).WillOnce(Return(reader));

    unique_ptr<ProcessedDocProducer> producer(factory.createProcessedDocProducer(_initParam));
    ASSERT_FALSE(producer.get());
}

TEST_F(SwiftBrokerFactoryCreateProducerTest, testCreateProducerParamInvalid) {
    StrictMockSwiftBrokerFactory factory;

    EXPECT_CALL(factory, createClient(_,_));

    _initParam.kvMap[CHECKPOINT] = "123456";
    _initParam.kvMap[PROCESSED_DOC_SWIFT_STOP_TIMESTAMP] = "1234";

    NiceMockSwiftReader *reader = new NiceMockSwiftReader;
    EXPECT_CALL(*reader, seekByTimestamp(123456, false)).Times(0);
    EXPECT_CALL(*reader, setTimestampLimit(234567, _)).Times(0);

    EXPECT_CALL(factory, createReader(_,_)).WillOnce(Return(reader));

    unique_ptr<ProcessedDocProducer> producer(factory.createProcessedDocProducer(_initParam));
    ASSERT_FALSE(producer.get());
}

TEST_F(SwiftBrokerFactoryCreateProducerTest, testCreateProducerCreateClientFailed) {
    StrictMockSwiftBrokerFactory factory;
    ON_CALL(factory, createClient(_,_)).WillByDefault(ReturnNull());
    EXPECT_CALL(factory, createClient(_,_));
    EXPECT_FALSE(factory.createProcessedDocProducer(_initParam));
}

TEST_F(SwiftBrokerFactoryCreateProducerTest, testCreateProducerCreateReaderFailed) {
    StrictMockSwiftBrokerFactory factory;
    EXPECT_CALL(factory, createClient(_,_));

    ON_CALL(factory, createReader(_,_)).WillByDefault(ReturnNull());
    EXPECT_CALL(factory, createReader(_,_));

    EXPECT_FALSE(factory.createProcessedDocProducer(_initParam));
}

TEST_F(SwiftBrokerFactoryCreateProducerTest, testCreateProducerEmptyClusternames) {
    StrictMockSwiftBrokerFactory factory;
    _initParam.partitionId.clear_clusternames();
    EXPECT_FALSE(factory.createProcessedDocProducer(_initParam));
}

TEST_F(SwiftBrokerFactoryCreateProducerTest, testCreateProducerMultiClusternames) {
    StrictMockSwiftBrokerFactory factory;
    _initParam.partitionId.add_clusternames();
    EXPECT_FALSE(factory.createProcessedDocProducer(_initParam));
}

TEST_F(SwiftBrokerFactoryCreateConsumerTest, testCreateConsumer) {
    _initParam.kvMap[READ_SRC] = "";
    {
        StrictMockSwiftBrokerFactory factory;
        EXPECT_CALL(factory, createClient(_,_)).Times(0);
        EXPECT_CALL(factory, createWriter(_, _, _)).Times(0);
        EXPECT_FALSE(factory.createProcessedDocConsumer(_initParam));
    }
    _initParam.kvMap[READ_SRC] = "swift";
    _initParam.kvMap[COUNTER_CONFIG_JSON_STR] = "";
    {
        // no check point root
        StrictMockSwiftBrokerFactory factory;
        EXPECT_CALL(factory, createClient(_,_)).Times(0);
        EXPECT_CALL(factory, createWriter(_, _, _)).Times(0);
        EXPECT_FALSE(factory.initCounterMap(_initParam));
    }

    _initParam.kvMap[READ_SRC] = "swift";
    CounterConfig counterConfig;
    counterConfig.position = "zookeeper";
    counterConfig.params[COUNTER_PARAM_FILEPATH] = GET_TEST_DATA_PATH() + "/counters";
    _initParam.kvMap[COUNTER_CONFIG_JSON_STR] = ToJsonString(counterConfig);
    
    {
        StrictMockSwiftBrokerFactory factory;
        EXPECT_CALL(factory, createClient(_,_)).Times(1);
        vector<string> topicNames;
        EXPECT_CALL(factory, createWriter(_, _, _)).Times(1).WillRepeatedly(
                Invoke(std::bind(createWriter, std::placeholders::_2, std::placeholders::_3, &topicNames)));
        unique_ptr<ProcessedDocConsumer> consumer(factory.createProcessedDocConsumer(_initParam));
        ASSERT_TRUE(consumer.get());
        EXPECT_THAT(topicNames, UnorderedElementsAre(string("user_name_special_service_name_processed_10_cluster1")));
        SwiftProcessedDocConsumer *swiftConsumer =
            ASSERT_CAST_AND_RETURN(SwiftProcessedDocConsumer, consumer.get());
        auto counterFileSynchronizer = dynamic_pointer_cast<CounterFileSynchronizer>(
                swiftConsumer->_counterSynchronizer);
        ASSERT_TRUE(counterFileSynchronizer);        
        EXPECT_EQ(string(GET_TEST_DATA_PATH() + "/counters"),
                  counterFileSynchronizer->_counterFilePath);
    }
    {
        // two clusters
        StrictMockSwiftBrokerFactory factory;
        EXPECT_CALL(factory, createClient(_,_)).Times(2);;
        vector<string> topicNames;
        EXPECT_CALL(factory, createWriter(_, _, _)).Times(2).WillRepeatedly(
                Invoke(std::bind(createWriter, std::placeholders::_2, std::placeholders::_3, &topicNames)));
        *_initParam.partitionId.add_clusternames() = "cluster2";
        _initParam.partitionId.mutable_buildid()->set_datatable("simple");
        _initParam.partitionId.mutable_buildid()->set_generationid(20);
        unique_ptr<ProcessedDocConsumer> consumer(factory.createProcessedDocConsumer(_initParam));
        ASSERT_TRUE(consumer.get());
        EXPECT_THAT(topicNames, UnorderedElementsAre(
                        string("user_name_special_service_name_processed_20_cluster1"),
                        string("user_name_special_service_name_processed_20_cluster2")));
        SwiftProcessedDocConsumer *swiftConsumer =
            ASSERT_CAST_AND_RETURN(SwiftProcessedDocConsumer, consumer.get());
        auto counterFileSynchronizer = dynamic_pointer_cast<CounterFileSynchronizer>(
                swiftConsumer->_counterSynchronizer);
        ASSERT_TRUE(counterFileSynchronizer);
        EXPECT_EQ(string(GET_TEST_DATA_PATH() + "/counters"),
                  counterFileSynchronizer->_counterFilePath);
        ASSERT_EQ(factory.getLastSwiftConfig(),
              "zkPath=zfs://processed_doc_swift_root/;"
                  "cluster1_default_client_config#"
                  "zkPath=zfs://processed_doc_swift_root/;"
                  "cluster2_default_client_config");
    }
}

TEST_F(SwiftBrokerFactoryCreateConsumerTest, testCreateWritersWithDiffTopicConfigs) {
    BrokerFactory::RoleInitParam initParam = _initParam;
    string configPath = TEST_DATA_PATH"/swift_broker_factory_test/config_diff_topics/";
    initParam.resourceReader.reset(new ResourceReader(configPath));

    initParam.kvMap[READ_SRC] = "swift";
    initParam.kvMap[CHECKPOINT_ROOT] = GET_TEST_DATA_PATH();    
    *initParam.partitionId.add_clusternames() = "cluster2";
    initParam.partitionId.mutable_buildid()->set_datatable("simple");
    initParam.partitionId.mutable_buildid()->set_generationid(20);
    {
        // two clusters
        StrictMockSwiftBrokerFactory factory;
        EXPECT_CALL(factory, createClient(_,_)).Times(2);;
        vector<string> topicNames;
        EXPECT_CALL(factory, createWriter(_, _, _)).Times(2).WillRepeatedly(
                Invoke(std::bind(createWriter, std::placeholders::_2, std::placeholders::_3, &topicNames)));
        initParam.partitionId.set_step(BUILD_STEP_FULL);
        unique_ptr<ProcessedDocConsumer> consumer(factory.createProcessedDocConsumer(initParam));
        ASSERT_TRUE(consumer.get());
        EXPECT_THAT(topicNames, UnorderedElementsAre(
                        string("user_name_special_service_name_full_processed_20_cluster1"),
                        string("user_name_special_service_name_full_processed_20_cluster2")));
    }
    {
        // two clusters
        StrictMockSwiftBrokerFactory factory;
        EXPECT_CALL(factory, createClient(_,_)).Times(2);
        vector<string> topicNames;
        EXPECT_CALL(factory, createWriter(_, _, _)).Times(2).WillRepeatedly(
                Invoke(std::bind(createWriter, std::placeholders::_2, std::placeholders::_3,  &topicNames)));
        initParam.partitionId.set_step(BUILD_STEP_INC);
        unique_ptr<ProcessedDocConsumer> consumer(factory.createProcessedDocConsumer(initParam));
        ASSERT_TRUE(consumer.get());
        EXPECT_THAT(topicNames, UnorderedElementsAre(
                        string("user_name_special_service_name_processed_20_cluster1"),
                        string("user_name_special_service_name_processed_20_cluster2")));
    }    
}


TEST_F(SwiftBrokerFactoryCreateConsumerTest, testCreateConsumerCreateClientFailed) {
    StrictMockSwiftBrokerFactory factory;
    ON_CALL(factory, createClient(_,_)).WillByDefault(ReturnNull());
    EXPECT_CALL(factory, createClient(_,_));
    EXPECT_FALSE(factory.createProcessedDocConsumer(_initParam));
}

TEST_F(SwiftBrokerFactoryCreateConsumerTest, testCreateConsumerCreateWriterFailed) {
    StrictMockSwiftBrokerFactory factory;
    EXPECT_CALL(factory, createClient(_,_)).Times(2);

    *_initParam.partitionId.add_clusternames() = "cluster2";
    int succeedCount = 1;

    ON_CALL(factory, createWriter(_,_, _)).WillByDefault(
            Invoke(std::bind(MockSwiftBrokerFactory::createWriterForMock, &succeedCount)));
    EXPECT_CALL(factory, createWriter(_,_, _)).Times(2);
    EXPECT_FALSE(factory.createProcessedDocConsumer(_initParam));
}

TEST_F(SwiftBrokerFactoryCreateConsumerTest, testCreateConsumerEmptyClusternames) {
    StrictMockSwiftBrokerFactory factory;
    _initParam.partitionId.clear_clusternames();
    EXPECT_FALSE(factory.createProcessedDocConsumer(_initParam));
}

TEST_F(SwiftBrokerFactoryCreateProducerTest, testCreateCustomizedDocProducer) {
    StrictMockSwiftBrokerFactory factory;
    EXPECT_CALL(factory, createClient(_,_));

    _initParam.kvMap[CHECKPOINT] = "123456";
    _initParam.kvMap[PROCESSED_DOC_SWIFT_STOP_TIMESTAMP] = "234567";
    _initParam.partitionId.clear_clusternames();
    *_initParam.partitionId.add_clusternames() = "simple_cluster";
    string configPath = TEST_DATA_PATH"/swift_broker_factory_test/customized_doc_parser_config/";
    _initParam.resourceReader.reset(new ResourceReader(configPath));
    _initParam.schema = _initParam.resourceReader->getSchema("simple_cluster");

    NiceMockSwiftReader *reader = new NiceMockSwiftReader;
    EXPECT_CALL(*reader, seekByTimestamp(123456, false))
        .WillOnce(Return(SWIFT_NS(protocol)::ERROR_NONE));
    EXPECT_CALL(*reader, setTimestampLimit(234567, _));

    EXPECT_CALL(factory, createReader(_, _)).WillOnce(Return(reader));

    unique_ptr<ProcessedDocProducer> producer(factory.createProcessedDocProducer(_initParam));
    ASSERT_TRUE(producer.get());
    SwiftProcessedDocProducer *swiftProducer = dynamic_cast<SwiftProcessedDocProducer*>(producer.get());
    ASSERT_TRUE(swiftProducer);
    ASSERT_TRUE(swiftProducer->_docFactoryWrapper);
    ASSERT_TRUE(swiftProducer->_docParser);
    MyDocumentParserPtr expectedDocParser =
        DYNAMIC_POINTER_CAST(MyDocumentParser, swiftProducer->_docParser);
    ASSERT_TRUE(expectedDocParser);
}


SwiftReader *SwiftBrokerFactoryCreateProducerTest::createReader(
        const string &topicName, const Filter &filter, const string &readerConfig,
        const string &expectTopicName, const proto::Range &partRange, const string &expectReaderConfig)
{
    checkReaderParam(topicName, filter, readerConfig, expectTopicName, partRange, expectReaderConfig);
    return new NiceMockSwiftReader;
}

void SwiftBrokerFactoryCreateProducerTest::checkReaderParam(
        const string &topicName, const Filter &filter, const string &readerConfig,
        const string &expectTopicName, const proto::Range &partRange, const string &expectReaderConfig)
{
    EXPECT_EQ(expectTopicName, topicName);
    EXPECT_EQ(partRange.from(), filter.from());
    EXPECT_EQ(partRange.to(), filter.to());
    EXPECT_EQ(expectReaderConfig, readerConfig);
}

SwiftWriter *SwiftBrokerFactoryCreateConsumerTest::createWriter(
        const string &topicName, const string &writerConfig,
        vector<string> *allTopicNames)
{
    allTopicNames->push_back(topicName);
    vector<string> vec = autil::StringTokenizer::tokenize(
            autil::ConstString(topicName), "_");
    checkWriterParam(writerConfig, vec.back());
    return new NiceMockSwiftWriter;
}

void SwiftBrokerFactoryCreateConsumerTest::checkWriterParam(
        const string &writerConfig, const string &clusterName)
{
    EXPECT_EQ(string("maxBufferSize=536870912;swift_writer_config_") + clusterName, writerConfig);
}

}
}
