#include "build_service/test/unittest.h"
#include "build_service/workflow/BrokerFactory.h"
#include "build_service/workflow/FlowFactory.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/test/MockBuildFlow.h"
#include "build_service/workflow/test/MockBrokerFactory.h"
#include "build_service/workflow/test/FakeProcessedDocProducer.h"
#include "build_service/workflow/test/MockBrokerFactory.h"
#include "build_service/workflow/test/CustomizedIndex.h"
#include "build_service/workflow/DocBuilderConsumer.h"
#include "build_service/workflow/DocProcessorConsumer.h"
#include "build_service/builder/AsyncBuilder.h"
#include "build_service/builder/BuilderCreator.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/reader/RawDocumentReaderCreator.h"
#include "build_service/reader/test/MockRawDocumentReader.h"
#include "build_service/processor/test/MockProcessor.h"
#include "build_service/processor/BatchProcessor.h"
#include "build_service/task_base/MergeTask.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/util/FileUtil.h"
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/util/counter/accumulative_counter.h>
#include <indexlib/util/counter/state_counter.h>
#include <indexlib/partition/custom_offline_partition_writer.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/test/partition_state_machine.h>

using namespace std;
using namespace testing;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(partition);

using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::common;
using namespace build_service::reader;
using namespace build_service::processor;
using namespace build_service::builder;
using namespace build_service::document;
using namespace build_service::util;
using namespace build_service::task_base;


namespace {
    
class FakeDocBuilderConsumer : public build_service::workflow::DocBuilderConsumer {
public:
    FakeDocBuilderConsumer()
        : DocBuilderConsumer(NULL) {}
    ~FakeDocBuilderConsumer() {}
};

class FakeDocProducer : public build_service::workflow::SwiftProcessedDocProducer
{
public:
    FakeDocProducer()
        : SwiftProcessedDocProducer(NULL, IndexPartitionSchemaPtr(), "")
        , _timestamp(0)
        , _useSelfTimestamp(false)
        {}
    ~FakeDocProducer() {}
public:
    void setSuspendReadAtTimestamp(int64_t timestamp) {
        _timestamp = timestamp;
        _useSelfTimestamp = true;
    }
    int64_t suspendReadAtTimestamp(int64_t timestamp) override {
        if (_useSelfTimestamp) {
            return _timestamp;
        }
        return timestamp;
    }
private:
    int64_t _timestamp;
    bool _useSelfTimestamp;
};
}

namespace build_service {
namespace workflow {

class BuildFlowTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    static reader::RawDocumentReader *createMockReader() {
        return new reader::NiceMockRawDocumentReader();
    }
    static processor::Processor *createMockProcessor() {
        return new processor::NiceMockProcessor();
    }
protected:
    void testLoopStartRetry(WorkflowMode mode);
    void testFatalErrorHappenedAfterStart(WorkflowMode mode);
    void testinitRoleInitParam();
    RawDocConsumer* createRawDocConsumer();
private:
    void doTestinitRoleInitParam(WorkflowMode mode, bool documentFilter,
                                 uint8_t expectSwiftFilterMask);
    void prepareBinaryRawData(uint64_t docCount, uint64_t partCount,
                              uint64_t recordSize, uint32_t partId, const string& fileName);
    void buildSinglePartData(uint64_t expectDocCount, const KeyValueMap& kvMap,
                             const string& configPath, uint16_t from, uint16_t to);
private:
    int64_t _restartTimes;
};


void BuildFlowTest::setUp() {
    _restartTimes = 0;
}

void BuildFlowTest::tearDown() {
}

TEST_F(BuildFlowTest, testMode) {
    ResourceReaderPtr resourceReader(new ResourceReader(""));
    KeyValueMap kvMap;
    kvMap[SRC_SIGNATURE] = "1234";

    proto::PartitionId partitionId;
    {
        NiceMockBuildFlow buildFlow;
        StrictMockBrokerFactory mockBrokerFactory;
        EXPECT_CALL(mockBrokerFactory, createRawDocConsumer(_));

        ASSERT_TRUE(buildFlow.startBuildFlow(
                        resourceReader, kvMap, partitionId, &mockBrokerFactory,
                        BuildFlow::READER, JOB, NULL));
    }
    {
        NiceMockBuildFlow buildFlow;
        StrictMockBrokerFactory mockBrokerFactory;

        EXPECT_CALL(mockBrokerFactory, createProcessedDocConsumer(_));
        ASSERT_TRUE(buildFlow.startBuildFlow(resourceReader, kvMap,
                        partitionId, &mockBrokerFactory, BuildFlow::READER_AND_PROCESSOR,
                        JOB, NULL));
    }
    {
        NiceMockBuildFlow buildFlow;
        StrictMockBrokerFactory mockBrokerFactory;

        ASSERT_TRUE(buildFlow.startBuildFlow(resourceReader, kvMap,
                        partitionId, &mockBrokerFactory, BuildFlow::ALL,
                        JOB, NULL));
    }
    {
        NiceMockBuildFlow buildFlow;
        StrictMockBrokerFactory mockBrokerFactory;

        EXPECT_CALL(mockBrokerFactory, createRawDocProducer(_));
        EXPECT_CALL(mockBrokerFactory, createProcessedDocConsumer(_));

        ASSERT_TRUE(buildFlow.startBuildFlow(resourceReader, kvMap,
                        partitionId, &mockBrokerFactory, BuildFlow::PROCESSOR,
                        JOB, NULL));
    }
    {
        NiceMockBuildFlow buildFlow;
        StrictMockBrokerFactory mockBrokerFactory;

        EXPECT_CALL(mockBrokerFactory, createProcessedDocProducer(_));

        ASSERT_TRUE(buildFlow.startBuildFlow(resourceReader, kvMap,
                        partitionId, &mockBrokerFactory, BuildFlow::BUILDER,
                        JOB, NULL));
    }
    {
        NiceMockBuildFlow buildFlow;
        StrictMockBrokerFactory mockBrokerFactory;

        EXPECT_CALL(mockBrokerFactory, createRawDocProducer(_));

        ASSERT_TRUE(buildFlow.startBuildFlow(resourceReader, kvMap,
                        partitionId, &mockBrokerFactory, BuildFlow::PROCESSOR_AND_BUILDER,
                        JOB, NULL));
    }
}

TEST_F(BuildFlowTest, testAsyncBuild) {
    string configPath = TEST_DATA_PATH"/build_flow_test/config_async_build/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));
    KeyValueMap kvMap;
    kvMap[GENERATION_ID] = "1";
    kvMap[CLUSTER_NAMES] = "simple_cluster";
    kvMap[DATA_PATH] = TEST_DATA_PATH"/build_flow_test/data/simple_data";
    kvMap[READ_SRC] = "file";
    kvMap[READ_SRC_TYPE] = "file";
    kvMap[SRC_SIGNATURE] = "0";
    kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH() + "/build_flow_test/index/";

    proto::PartitionId partitionId;
    partitionId.mutable_buildid()->set_datatable("simple");
    *partitionId.add_clusternames() = "simple_cluster";

    BuildFlow buildFlow;

    buildFlow.startWorkLoop(resourceReader, kvMap, partitionId,
                            NULL, BuildFlow::ALL, JOB, NULL);
    ASSERT_TRUE(buildFlow.waitFinish());

    FlowFactory *flowFactory = dynamic_cast<FlowFactory*>(buildFlow._flowFactory.get());
    ASSERT_TRUE(flowFactory);

    uint64_t docNumBuild = flowFactory->_builder->_indexBuilder
                           ->GetBuilderMetrics().GetSuccessDocCount();
    AsyncBuilder* asyncBuilder = dynamic_cast<AsyncBuilder*>(flowFactory->_builder);
    ASSERT_TRUE(asyncBuilder);
    ASSERT_EQ(uint64_t(6), docNumBuild);    
}

TEST_F(BuildFlowTest, testSimpleProcess) {
    string configPath = TEST_DATA_PATH"/build_flow_test/config/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    KeyValueMap kvMap;
    kvMap[GENERATION_ID] = "1";
    kvMap[CLUSTER_NAMES] = "simple_cluster";
    kvMap[DATA_PATH] = TEST_DATA_PATH"/build_flow_test/data/simple_data";
    kvMap[READ_SRC] = "file";
    kvMap[READ_SRC_TYPE] = "file";
    kvMap[SRC_SIGNATURE] = "0";
    kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH() + "/build_flow_test/index/";

    proto::PartitionId partitionId;
    partitionId.mutable_buildid()->set_datatable("simple");
    *partitionId.add_clusternames() = "simple_cluster";

    BuildFlow buildFlow;

    buildFlow.startWorkLoop(resourceReader, kvMap, partitionId,
                            NULL, BuildFlow::ALL, JOB, NULL);
    ASSERT_TRUE(buildFlow.waitFinish());

    FlowFactory *flowFactory = dynamic_cast<FlowFactory*>(buildFlow._flowFactory.get());
    ASSERT_TRUE(flowFactory);

    uint64_t docNumBuild = flowFactory->_builder->_indexBuilder
                           ->GetBuilderMetrics().GetSuccessDocCount();
    ASSERT_EQ(uint64_t(6), docNumBuild);
}

TEST_F(BuildFlowTest, testSimpleBatchProcess) {
    string configPath = TEST_DATA_PATH"/build_flow_test/config_batch_processor/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    KeyValueMap kvMap;
    kvMap[GENERATION_ID] = "1";
    kvMap[CLUSTER_NAMES] = "simple_cluster";
    kvMap[DATA_PATH] = TEST_DATA_PATH"/build_flow_test/data/simple_data";
    kvMap[READ_SRC] = "file";
    kvMap[READ_SRC_TYPE] = "file";
    kvMap[SRC_SIGNATURE] = "0";
    kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH() + "/build_flow_test/index/";

    proto::PartitionId partitionId;
    partitionId.mutable_buildid()->set_datatable("simple");
    *partitionId.add_clusternames() = "simple_cluster";

    BuildFlow buildFlow;
    StrictMockBrokerFactory mockBrokerFactory;
    EXPECT_CALL(mockBrokerFactory, initCounterMap(_))
        .WillOnce(Return(true));
    unique_ptr<NiceMockProcessedDocConsumer> mockConsumer(new NiceMockProcessedDocConsumer);
    EXPECT_CALL(mockBrokerFactory, createProcessedDocConsumer(_))
        .WillOnce(Return(mockConsumer.release()));
    //EXPECT_CALL(mockBrokerFactory, createProcessedDocConsumer(_))
    //    .WillOnce(Return(NULL));
    buildFlow.startWorkLoop(resourceReader, kvMap, partitionId,
                            &mockBrokerFactory, BuildFlow::READER_AND_PROCESSOR, SERVICE, NULL);
    ASSERT_TRUE(buildFlow.waitFinish());

    FlowFactory* flowFactory = dynamic_cast<FlowFactory*>(buildFlow._flowFactory.get());
    ASSERT_TRUE(flowFactory);

    //uint64_t docNumBuild = flowFactory->_builder->_indexBuilder
    //                       ->GetBuilderMetrics().GetSuccessDocCount();
    DocProcessorConsumer* consumer = dynamic_cast<DocProcessorConsumer*>((buildFlow._readToProcessorFlow)->getConsumer());
    BatchProcessor* processor = dynamic_cast<BatchProcessor*>(consumer->_processor);
    ASSERT_TRUE(processor);
    int64_t lastFlushTime = processor->_lastFlushTime;
    EXPECT_TRUE(lastFlushTime);
}

TEST_F(BuildFlowTest, testProcessErrorCounter) {
    string configPath = TEST_DATA_PATH"/build_flow_test/config/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    KeyValueMap kvMap;
    kvMap[GENERATION_ID] = "1";
    kvMap[CLUSTER_NAMES] = "simple_cluster";
    kvMap[DATA_PATH] = TEST_DATA_PATH"/build_flow_test/data/simple_data_fail";
    kvMap[READ_SRC] = "file";
    kvMap[READ_SRC_TYPE] = "file";
    kvMap[SRC_SIGNATURE] = "0";
    kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH() + "/build_flow_test/index/";

    proto::PartitionId partitionId;
    partitionId.mutable_buildid()->set_datatable("simple");
    *partitionId.add_clusternames() = "simple_cluster";

    BuildFlow buildFlow;

    buildFlow.startWorkLoop(resourceReader, kvMap, partitionId,
                            NULL, BuildFlow::ALL, JOB, NULL);
    ASSERT_TRUE(buildFlow.waitFinish());

    FlowFactory *flowFactory = dynamic_cast<FlowFactory*>(buildFlow._flowFactory.get());
    ASSERT_TRUE(flowFactory);

    uint64_t docNumBuild = flowFactory->_builder->_indexBuilder
                           ->GetBuilderMetrics().GetSuccessDocCount();
    ASSERT_EQ(uint64_t(6), docNumBuild);
    ASSERT_EQ(7, GET_ACC_COUNTER(buildFlow._counterMap, processError)->Get());
}

TEST_F(BuildFlowTest, testConvertError) {
    string configPath = TEST_DATA_PATH"/build_flow_test/error_config/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    KeyValueMap kvMap;
    kvMap[GENERATION_ID] = "1";
    kvMap[CLUSTER_NAMES] = "simple_cluster";
    kvMap[DATA_PATH] = TEST_DATA_PATH"/build_flow_test/data/data.ha3.checkcounters";
    kvMap[READ_SRC] = "file";
    kvMap[READ_SRC_TYPE] = "file";
    kvMap[SRC_SIGNATURE] = "0";
    kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH() + "/build_flow_test/index/";

    proto::PartitionId partitionId;
    partitionId.mutable_buildid()->set_datatable("simple");
    *partitionId.add_clusternames() = "simple_cluster";

    BuildFlow buildFlow;

    buildFlow.startWorkLoop(resourceReader, kvMap, partitionId,
                            NULL, BuildFlow::ALL, JOB, NULL);
    ASSERT_TRUE(buildFlow.waitFinish());

    FlowFactory *flowFactory = dynamic_cast<FlowFactory*>(buildFlow._flowFactory.get());
    ASSERT_TRUE(flowFactory);

    uint64_t docNumBuild = flowFactory->_builder->_indexBuilder
                           ->GetBuilderMetrics().GetSuccessDocCount();
    ASSERT_EQ(uint64_t(4), docNumBuild);
    ASSERT_EQ(2, GET_ACC_COUNTER(buildFlow._counterMap, processor.attributeConvertError)->Get());
}

RawDocConsumer* BuildFlowTest::createRawDocConsumer() {
    _restartTimes++;
    cout << "restart times" << _restartTimes << endl;
    return NULL;
}

TEST_F(BuildFlowTest, testLoopStartStopped) {
    ResourceReaderPtr resourceReader(new ResourceReader(""));
    KeyValueMap kvMap;

    proto::PartitionId partitionId;
    NiceMockBuildFlow buildFlow;
    StrictMockBrokerFactory mockBrokerFactory;
    EXPECT_CALL(mockBrokerFactory, createRawDocConsumer(_)).WillRepeatedly(
            Invoke(std::tr1::bind(&BuildFlowTest::createRawDocConsumer, this)));
    buildFlow.startWorkLoop(resourceReader, kvMap, partitionId,
                            &mockBrokerFactory, BuildFlow::READER, SERVICE, NULL);
    usleep(int64_t(4.5 * 1000 * 1000));
    ASSERT_TRUE(_restartTimes > 1);
    buildFlow.stop();
}

ACTION(CREATE_FLOWFACTORY) {
    NiceMockRawDocProducer *producer = new NiceMockRawDocProducer;

    EXPECT_CALL(*producer, produce(_))
        .WillOnce(Return(FE_OK))
        .WillOnce(Return(FE_OK))
        .WillOnce(Return(FE_OK))
        .WillOnce(Return(FE_EOF));

    MockFlowFactory *flowFactory = new MockFlowFactory;
    EXPECT_CALL(*flowFactory, createRawDocProducer(_))
        .WillOnce(Return(producer));

    return flowFactory;
}

void BuildFlowTest::testLoopStartRetry(WorkflowMode mode) {
    ResourceReaderPtr resourceReader(new ResourceReader(""));
    KeyValueMap kvMap;

    proto::PartitionId partitionId;
    NiceMockBuildFlow buildFlow;
    StrictMockBrokerFactory mockBrokerFactory;

    EXPECT_CALL(buildFlow, createFlowFactory())
        .WillOnce(Return(new NiceMockFlowFactory))
        .WillOnce(Return(new NiceMockFlowFactory))
        .WillOnce(CREATE_FLOWFACTORY());

    EXPECT_CALL(mockBrokerFactory, createRawDocConsumer(_))
        .WillOnce(ReturnNull())
        .WillOnce(ReturnNull())
        .WillOnce(Return(new NiceMockRawDocConsumer));

    buildFlow.startWorkLoop(resourceReader, kvMap,
                            partitionId, &mockBrokerFactory, BuildFlow::READER,
                            mode, NULL);

    while (!buildFlow.isStarted()) usleep(1);

    ASSERT_TRUE(buildFlow._readToProcessorFlow.get());
    ASSERT_FALSE(buildFlow._processorToBuildFlow.get());
    ASSERT_TRUE(buildFlow._flowFactory.get());

    ASSERT_TRUE(buildFlow.waitFinish());
}

TEST_F(BuildFlowTest, testLoopStartRetry) {
    testLoopStartRetry(REALTIME);
}

void BuildFlowTest::testFatalErrorHappenedAfterStart(WorkflowMode mode) {
    ResourceReaderPtr resourceReader(new ResourceReader(""));
    KeyValueMap kvMap;

    proto::PartitionId partitionId;
    StrictMockBrokerFactory mockBrokerFactory;
    NiceMockBuildFlow buildFlow;
    NiceMockRawDocConsumer *consumer = new NiceMockRawDocConsumer;

    EXPECT_CALL(*consumer, stop(_))
        .WillOnce(Return(false));

    EXPECT_CALL(mockBrokerFactory, createRawDocConsumer(_))
        .WillOnce(Return(consumer));

    EXPECT_CALL(buildFlow, createFlowFactory())
        .WillOnce(CREATE_FLOWFACTORY());

    buildFlow.startWorkLoop(resourceReader, kvMap,
                            partitionId, &mockBrokerFactory, BuildFlow::READER,
                            mode, NULL);
    if (REALTIME == mode) {
        while (!buildFlow.hasFatalError()) usleep(1);
        ASSERT_FALSE(buildFlow.isFinished());
        buildFlow.stop();
    } else {
        ASSERT_FALSE(buildFlow.waitFinish());
    }

    ASSERT_TRUE(buildFlow._readToProcessorFlow.get());
    ASSERT_FALSE(buildFlow._processorToBuildFlow.get());
    ASSERT_TRUE(buildFlow._flowFactory.get());

    ASSERT_TRUE(buildFlow.hasFatalError());
}

TEST_F(BuildFlowTest, testFatalErrorHappenedAfterStart) {
    testFatalErrorHappenedAfterStart(SERVICE);
    testFatalErrorHappenedAfterStart(REALTIME);
    testFatalErrorHappenedAfterStart(JOB);
}

MATCHER_P(MatchLocator, locator, "") {
    return arg.toString() == locator.toString();
}


TEST_F(BuildFlowTest, testSeek) {
#define PREPARE_BUILDFLOW()                                             \
    MockConsumer<int> *consumer1 = new StrictMock<MockConsumer<int> > ; \
    MockProducer<int> *producer1 = new StrictMock<MockProducer<int> >;  \
    MockConsumer<string> *consumer2 = new StrictMock<MockConsumer<string> >; \
    MockProducer<string> *producer2 = new StrictMock<MockProducer<string> >; \
    Workflow<int> flow1(producer1, consumer1);                          \
    Workflow<string> flow2(producer2, consumer2);                       \
    NiceMockBuildFlow buildFlow

    proto::PartitionId partitionId;
    partitionId.mutable_buildid()->set_datatable("simple");
    partitionId.mutable_buildid()->set_generationid(1);
    *partitionId.add_clusternames() = "simple_cluster";

    {
        // only flow2
        PREPARE_BUILDFLOW();
        EXPECT_CALL(*consumer2, getLocator(_))
            .WillOnce(DoAll(SetArgReferee<0>(Locator(100, 200)),
                            Return(true)));
        EXPECT_CALL(*producer2, seek(MatchLocator(Locator(100, 200))))
            .WillOnce(Return(true));
        buildFlow.seek<int, string, int>(NULL, &flow2, NULL, partitionId);
    }
    {
        // only flow1
        PREPARE_BUILDFLOW();
        EXPECT_CALL(*consumer1, getLocator(_))
            .WillOnce(DoAll(SetArgReferee<0>(Locator(100, 200)),
                            Return(true)));
        EXPECT_CALL(*producer1, seek(MatchLocator(Locator(100, 200))))
            .WillOnce(Return(true));
        buildFlow.seek<int, string, int>(&flow1, NULL, NULL, partitionId);
    }
    {
        // flow1 and flow2
        PREPARE_BUILDFLOW();
        EXPECT_CALL(*consumer1, getLocator(_))
            .WillOnce(DoAll(SetArgReferee<0>(Locator(100, 200)),
                            Return(true)));
        EXPECT_CALL(*consumer2, getLocator(_))
            .WillOnce(DoAll(SetArgReferee<0>(Locator(300, 400)),
                            Return(true)));
        EXPECT_CALL(*producer1, seek(MatchLocator(Locator(300, 400))))
            .WillOnce(Return(true));
        buildFlow.seek<int, string, int>(&flow1, &flow2, NULL, partitionId);
    }
}

void BuildFlowTest::testinitRoleInitParam() {
    doTestinitRoleInitParam(REALTIME, true, 
                            ProcessedDocument::SWIFT_FILTER_BIT_REALTIME);
    doTestinitRoleInitParam(SERVICE, true, 0);
    doTestinitRoleInitParam(JOB, true, 0);

    doTestinitRoleInitParam(REALTIME, false, 0);
    doTestinitRoleInitParam(SERVICE, false, 0);
    doTestinitRoleInitParam(JOB, false, 0);
}

void BuildFlowTest::doTestinitRoleInitParam(WorkflowMode mode,
        bool documentFilter, uint8_t expectSwiftFilterMask)
{
    string testPath = GET_TEST_DATA_PATH();
    config::ResourceReaderPtr resourceReader(new ResourceReader(testPath));
    KeyValueMap kvMap;
    proto::PartitionId partitionId;
    *partitionId.add_clusternames() = "simple";

    string jsonStr = "{\"cluster_config\" : {\"table_name\" : \"simple_table\"},"
                     "\"build_option_config\" : {\"document_filter\" : ";
    if (documentFilter) {
        jsonStr += "true";
    } else {
        jsonStr += "false";
    }
    jsonStr += "}}";
    FileUtil::writeFile(testPath + "/cluster/simple_cluster.json", jsonStr);

    BrokerFactory::RoleInitParam param;
    BuildFlow buildFlow;
    MockBrokerFactory* swiftBrokerFactory = new MockBrokerFactory();
    MockBrokerFactory* flowFactory = new MockBrokerFactory();
    buildFlow._brokerFactory = swiftBrokerFactory;
    buildFlow._flowFactory.reset(flowFactory);
    if (mode & BuildFlow::BUILDER) {
        EXPECT_CALL(*flowFactory, initCounterMap(_))
            .WillOnce(Return(true));
    } else {
        EXPECT_CALL(*swiftBrokerFactory, initCounterMap(_))
            .WillOnce(Return(true));
    }
    buildFlow.initRoleInitParam(resourceReader, kvMap, partitionId, mode,
                                NULL, param);

    ASSERT_EQ(resourceReader.get(), param.resourceReader.get());
    ASSERT_EQ(kvMap, param.kvMap);
    ASSERT_EQ(expectSwiftFilterMask, expectSwiftFilterMask);
    ASSERT_EQ((uint8_t)0, param.swiftFilterResult);
    ASSERT_TRUE(param.metricProvider == nullptr);
    delete swiftBrokerFactory;
}

TEST_F(BuildFlowTest, testInitCounterMapFailed)
{
    string testPath = GET_TEST_DATA_PATH();
    config::ResourceReaderPtr resourceReader(new ResourceReader(testPath));
    KeyValueMap kvMap;
    proto::PartitionId partitionId;
    *partitionId.add_clusternames() = "simple";

    string jsonStr = "{\"cluster_config\" : {\"table_name\" : \"simple_table\"},"
                     "\"build_option_config\" : {\"document_filter\" : true}}";
    FileUtil::writeFile(testPath + "/cluster/simple_cluster.json", jsonStr);

    BrokerFactory::RoleInitParam param;
    BuildFlow buildFlow;
    MockBrokerFactory* swiftBrokerFactory = new MockBrokerFactory();
    MockBrokerFactory* flowFactory = new MockBrokerFactory();
    buildFlow._brokerFactory = swiftBrokerFactory;
    buildFlow._flowFactory.reset(flowFactory);
    auto mode = SERVICE;
    EXPECT_CALL(*flowFactory, initCounterMap(_))
        .WillRepeatedly(Return(false));
    EXPECT_CALL(*swiftBrokerFactory, initCounterMap(_))
            .WillRepeatedly(Return(false));
    ASSERT_TRUE(buildFlow.initRoleInitParam(resourceReader, kvMap, partitionId, mode,
                    NULL, param));

    delete swiftBrokerFactory;
}

TEST_F(BuildFlowTest, testinitRoleInitParam) {
    testinitRoleInitParam();
}

TEST_F(BuildFlowTest, testStartBuildFlowAsSeekFailed) {
    string configPath = TEST_DATA_PATH"/build_flow_test/config_async_build/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));
    KeyValueMap kvMap;
    kvMap[GENERATION_ID] = "1";
    kvMap[CLUSTER_NAMES] = "simple_cluster";
    kvMap[DATA_PATH] = TEST_DATA_PATH"/build_flow_test/data/simple_data";
    kvMap[READ_SRC] = "file";
    kvMap[READ_SRC_TYPE] = "file";
    kvMap[SRC_SIGNATURE] = "0";
    kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH() + "/build_flow_test/index/";

    proto::PartitionId partitionId;
    partitionId.mutable_buildid()->set_datatable("simple");
    *partitionId.add_clusternames() = "simple_cluster";

    NiceMockBuildFlow buildFlow;
    StrictMockBrokerFactory mockBrokerFactory;

    unique_ptr<NiceMockProcessedDocProducer> producer(new NiceMockProcessedDocProducer);
    EXPECT_CALL(*producer, seek(_)).WillOnce(Return(false));
    EXPECT_CALL(mockBrokerFactory, createProcessedDocProducer(_)).WillOnce(Return(producer.release()));

    builder::NiceMockBuilder *builder = new NiceMockBuilder();
    EXPECT_CALL(*builder, getLastLocator(_)).WillOnce(
            DoAll(SetArgReferee<0>(Locator(100, 200)), Return(true)));

    NiceMockFlowFactory *flowFactory = new NiceMockFlowFactory();
    EXPECT_CALL(*flowFactory, createBuilder(_)).WillOnce(Return(builder));

    EXPECT_CALL(buildFlow, createFlowFactory()).WillOnce(Return(flowFactory));

    ASSERT_FALSE(buildFlow.buildFlowWorkLoop(resourceReader, kvMap, partitionId,
                    &mockBrokerFactory, BuildFlow::BUILDER, SERVICE, NULL));
    ASSERT_FALSE(buildFlow.isFinished());
    ASSERT_FALSE(buildFlow.hasFatalError());
    buildFlow.stop();
}

TEST_F(BuildFlowTest, suspendReadAtTimestamp) {
    NiceMockBuildFlow buildFlow;
    buildFlow._mode = BuildFlow::READER_AND_PROCESSOR;
    NiceMockRawDocumentReader reader;
    EXPECT_CALL(buildFlow, getReader())
        .WillOnce(Return(&reader));
    EXPECT_CALL(reader, suspendReadAtTimestamp((int64_t)10, RawDocumentReader::ETA_SUSPEND))
        .Times(1);

    ASSERT_TRUE(buildFlow.suspendReadAtTimestamp((int64_t)10, RawDocumentReader::ETA_SUSPEND));
}

TEST_F(BuildFlowTest, setEndBuildTimetamp) {
    BuildFlow buildFlow;
    buildFlow._mode = BuildFlow::BUILDER;
    FakeDocProducer* producer = new FakeDocProducer;
    producer->setSuspendReadAtTimestamp(101);
    FakeDocBuilderConsumer* consumer = new FakeDocBuilderConsumer;
    buildFlow._processorToBuildFlow.reset(new ProcessedDocWorkflow(producer, consumer));
    ASSERT_TRUE(buildFlow.suspendReadAtTimestamp(100, RawDocumentReader::ETA_STOP));
    ASSERT_EQ(100, consumer->_endTimestamp);
}    

TEST_F(BuildFlowTest, testCustomizedIndex) {
    string configPath = TEST_DATA_PATH"/build_flow_test/customized_index_config/";

    KeyValueMap kvMap;
    kvMap[GENERATION_ID] = "1";
    kvMap[CLUSTER_NAMES] = "simple_cluster";
    kvMap[RAW_DOCUMENT_SCHEMA_NAME] = "simple_table";
    kvMap[READ_SRC] = "file";
    kvMap[READ_SRC_TYPE] = "fix_length_binary_file";
    kvMap[DATA_DESCRIPTION_KEY] = R"(
            {
                "src" : "file",
                "type" : "file",
                "parser_config" : [
                    {
                       "type": "indexlib_parser",
                       "parameters": {}
                    }
                ]
            }
        )";
    // set data src
    kvMap[DATA_PATH] = string(TEST_DATA_PATH"/build_flow_test/data/customized_index");

    string(TEST_DATA_PATH"/build_flow_test/data/customized_index_data2");

    kvMap["length"] = "20";
    kvMap[SRC_SIGNATURE] = "0";
    kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH() + "/build_flow_test/index/";

    uint64_t docCount = 3000;
    uint64_t partCount = 2;
    uint64_t recordSize = 20;
    string rawData1 = kvMap[DATA_PATH] + "/data1";
    string rawData2 = kvMap[DATA_PATH] + "/data2";
    // prepare raw data
    prepareBinaryRawData(docCount, partCount, recordSize, 0, rawData1);
    prepareBinaryRawData(docCount, partCount, recordSize, 1, rawData2);
    // build index
    buildSinglePartData(1000, kvMap, configPath, 0, 32767);
    buildSinglePartData(1000, kvMap, configPath, 32768, 65535);
    // merge index
    {
        proto::PartitionId partitionId;
        partitionId.mutable_buildid()->set_datatable("simple");
        partitionId.mutable_range()->set_from(0);
        partitionId.mutable_range()->set_to(65535);
        *partitionId.add_clusternames() = "simple_cluster";

        IndexPartitionOptions options;
        options.SetIsOnline(false);
        options.GetBuildConfig().enablePackageFile = false;

        build_service::task_base::MergeTask mergeTask;
        mergeTask._jobConfig._config.buildParallelNum = 2;
        mergeTask._jobConfig._config.mergeParallelNum = 1;
        mergeTask._jobConfig._config.partitionCount = 1;
        mergeTask._jobConfig._clusterName = "simple_cluster";
        mergeTask._resourceReader.reset(new ResourceReader(configPath));
        kvMap[MERGE_TIMESTAMP] = "ts1";
        mergeTask._kvMap = kvMap;
        string targetIndexPath = kvMap[INDEX_ROOT_PATH] + "/simple_cluster/generation_0/partition_0_65535";
        std::pair<build_service::task_base::MergeTask::MergeStep, uint32_t> mergeStep =
            make_pair(build_service::task_base::MergeTask::MergeStep::MS_INIT_MERGE, uint32_t(0));
        mergeTask.mergeMultiPart(partitionId, kvMap[INDEX_ROOT_PATH], targetIndexPath,
                                 options, mergeStep, true, CounterConfigPtr());
        mergeStep = make_pair(
            build_service::task_base::MergeTask::MergeStep::MS_DO_MERGE, uint32_t(0));
        mergeTask.mergeMultiPart(partitionId, kvMap[INDEX_ROOT_PATH], targetIndexPath,
                                 options, mergeStep, true, CounterConfigPtr());
        mergeStep = make_pair(
            build_service::task_base::MergeTask::MergeStep::MS_END_MERGE, uint32_t(0));
        mergeTask.mergeMultiPart(partitionId, kvMap[INDEX_ROOT_PATH], targetIndexPath,
                                 options, mergeStep, true, CounterConfigPtr());
    }

    {
        // inc build
        proto::PartitionId partitionId;
        partitionId.mutable_buildid()->set_datatable("simple");
        *partitionId.add_clusternames() = "simple_cluster";
        // test builder reload ondisk segment data
        ResourceReaderPtr resourceReader(new ResourceReader(configPath));
        BuilderCreator creator;
        Builder* incBuilder = creator.create(resourceReader, kvMap, partitionId, NULL);
        ASSERT_TRUE(incBuilder);
        CustomOfflinePartitionWriterPtr partWriter =
            DYNAMIC_POINTER_CAST(CustomOfflinePartitionWriter,
                                 incBuilder->_indexBuilder->mIndexPartitionWriter);
        ASSERT_TRUE(partWriter);
        MyTableWriterPtr tableWriter =
            DYNAMIC_POINTER_CAST(MyTableWriter, partWriter->mTableWriter);
        ASSERT_TRUE(tableWriter);
        for (docid_t docId = 0; docId < (docid_t)tableWriter->mMaxDocCount; docId++)
        {
            for (size_t i = 0; i < 4; i++)
            {
                float value = 0.1 * i * docId;
                ASSERT_EQ(value, tableWriter->mData[docId][i]);
            }
        }
        // update document
        docid_t docId = 999;
        float newvalue[4] = {0.2, 0.7, 1.4, 1.1};
        MyRawDocumentPtr doc(new MyRawDocument);
        doc->SetDocId(docId);
        doc->setData(newvalue);
        doc->SetDocOperateType(DocOperateType::ADD_DOC);
        ASSERT_TRUE(incBuilder->build(doc));

        // check value
        for (size_t i = 0; i < 4; i++)
        {
            ASSERT_EQ(newvalue[i], tableWriter->mData[docId][i]);
        }
        delete incBuilder;
    }
}

void BuildFlowTest::prepareBinaryRawData(uint64_t docCount, uint64_t partCount,
                                         uint64_t recordSize, uint32_t partId,
                                         const string& fileName)
{
    size_t dataLen = docCount / partCount * recordSize;
    char data[dataLen];
    for (docid_t docId = 0; docId < (docid_t)docCount; docId++)
    {
        if (docId % partCount == partId) {
            continue;
        }
        uint32_t dataIdx = docId / partCount;
        std::memcpy(data + dataIdx*recordSize, &docId, 4);
        for (size_t i = 0; i < 4; i++)
        {
            float value = 0.1 * i * docId;
            std::memcpy(data + recordSize*dataIdx + 4*(i+1), &value, 4);
        }
    }
    string dataStr(data, dataLen);
    ASSERT_TRUE(FileUtil::writeFile(fileName, dataStr));
}

void BuildFlowTest::buildSinglePartData(uint64_t expectDocCount,
                                        const KeyValueMap& kvMap,
                                        const string& configPath,
                                        uint16_t from, uint16_t to)
{
    proto::PartitionId partitionId;
    partitionId.mutable_buildid()->set_datatable("simple");
    partitionId.mutable_range()->set_from(from);
    partitionId.mutable_range()->set_to(to);
    *partitionId.add_clusternames() = "simple_cluster";
        
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));
    BuildFlow buildFlow;
    buildFlow.startWorkLoop(resourceReader, kvMap, partitionId,
                            NULL, BuildFlow::ALL, JOB, NULL);
    ASSERT_TRUE(buildFlow.waitFinish());
    FlowFactory *flowFactory = dynamic_cast<FlowFactory*>(buildFlow._flowFactory.get());
    ASSERT_TRUE(flowFactory);
    sleep(5);
    uint64_t docNumBuild = flowFactory->_builder->_indexBuilder
        ->GetBuilderMetrics().GetSuccessDocCount();
    ASSERT_EQ(expectDocCount, docNumBuild);
}

}
}
