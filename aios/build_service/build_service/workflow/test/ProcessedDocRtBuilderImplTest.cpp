#include "build_service/test/unittest.h"
#include "build_service/workflow/ProcessedDocRtBuilderImpl.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/proto/test/ProtoCreator.h"
#include "build_service/workflow/test/FakeProcessedDocProducer.h"
#include "build_service/workflow/test/MockProcessedDocRtBuilderImpl.h"
#include "build_service/workflow/test/MockBuildFlow.h"
#include "build_service/workflow/test/MockFlowFactory.h"
#include "build_service/workflow/test/MockBrokerFactory.h"
#include "build_service/workflow/test/MockIndexPartition.h"
#include "build_service/builder/test/MockBuilder.h"
#include "build_service/workflow/test/FakeBuilder.h"
#include "build_service/proto/BasicDefs.pb.h"
#include <indexlib/partition/online_partition.h>
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::common;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::builder;
using namespace build_service::util;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index);

namespace build_service {
namespace workflow {

class MockSwiftProcessedDocProducer : public SwiftProcessedDocProducer {
public:
    MockSwiftProcessedDocProducer()
        : SwiftProcessedDocProducer(
            NULL, IE_NAMESPACE(config)::IndexPartitionSchemaPtr(), "")
    {
    }
    ~MockSwiftProcessedDocProducer() {}
public:
    MOCK_METHOD1(getMaxTimestamp, bool(int64_t &));
    MOCK_METHOD1(getLastReadTimestamp, bool(int64_t &));
    MOCK_METHOD1(seek, bool(const Locator &));
};

typedef std::tr1::shared_ptr<MockProcessedDocRtBuilderImpl> MockRealtimeBuilderPtr;
typedef std::tr1::shared_ptr<FakeBuilder> FakeBuilderPtr;
typedef NiceMock<MockSwiftProcessedDocProducer> NiceMockSwiftProcessedDocProducer;
typedef StrictMock<MockSwiftProcessedDocProducer> StrictMockSwiftProcessedDocProducer;

class ProcessedDocRtBuilderImplTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    void prepareFakes(const BuildFlowThreadResource& buildFlowThreadResource =
                      BuildFlowThreadResource());
    void prepareMocks();
    static std::pair<MockRealtimeBuilderPtr, FakeBuilder*> makeFakeBuilder(const std::string& configPath,
            const BuildFlowThreadResource& buildFlowThreadResource);
protected:
    common::Locator createLocator(int64_t timestamp) {
        Locator locator(timestamp);
        return locator;
    }
    common::Locator createLocatorWithSrc(uint64_t src, int64_t timestamp) {
        Locator locator(src, timestamp);
        return locator;
    }
    void waitBuildDoc(FakeBuilder* builder, uint32_t docCount,
        int64_t times = 100);
protected:
    string _configPath;
    MockIndexPartition* _mockIndexPartition;
    IndexPartitionPtr _indexPartition;
    MockBuildFlow *_mockBuildFlow;
    unique_ptr<MockBrokerFactory> _mockBrokerFactory;
    unique_ptr<MockProcessedDocRtBuilderImpl> _realtimeBuilder;
    unique_ptr<MockFlowFactory> _mockFlowFactory;
    FakeProcessedDocProducer *_fakeProducer;
    FakeBuilder *_fakeBuilder;
    unique_ptr<MockSwiftProcessedDocProducer> _mockProducer;
    unique_ptr<MockBuilder> _mockBuilder;
    KeyValueMap _kvMap;
};

ACTION_P2(SET0_AND_RETURN, param0, ret) {
    arg0 = param0;
    return ret;
}

ACTION_P(collectPartitionId, partitionId) {
    partitionId->CopyFrom(arg0.partitionId);
}

void ProcessedDocRtBuilderImplTest::setUp() {
    _configPath = TEST_DATA_PATH"/realtime_builder_test";
}

void ProcessedDocRtBuilderImplTest::tearDown() {
}

std::pair<MockRealtimeBuilderPtr, FakeBuilder*> ProcessedDocRtBuilderImplTest::makeFakeBuilder(
        const std::string& configPath,
        const BuildFlowThreadResource& buildFlowThreadResource) {
    auto mockPartition = new NiceMockIndexPartition();
    IndexPartitionPtr indexPartition(mockPartition);
    
    RealtimeBuilderResource buildResource(
            kmonitor::MetricsReporterPtr(), IE_NAMESPACE(util)::TaskSchedulerPtr(
                    new IE_NAMESPACE(util)::TaskScheduler),
            SwiftClientCreatorPtr(new SwiftClientCreator),
            buildFlowThreadResource);
    MockRealtimeBuilderPtr realtimeBuilder(
        new NiceMock<MockProcessedDocRtBuilderImpl>(
                    configPath, indexPartition, buildResource));
    MockBuildFlow* mockBuildFlow = new NiceMockBuildFlow(indexPartition, buildFlowThreadResource);
    EXPECT_CALL(*realtimeBuilder, createBuildFlow(_,_))
        .WillOnce(Return(mockBuildFlow));
    
    unique_ptr<MockBrokerFactory> mockBrokerFactory(new NiceMockBrokerFactory());
    unique_ptr<MockFlowFactory> mockFlowFactory(new NiceMockFlowFactory());

    auto fakeProducer = new FakeProcessedDocProducer(10);

    EXPECT_CALL(*mockBrokerFactory, createProcessedDocProducer(_))
        .WillOnce(Return(fakeProducer));

    FakeBuilder* fakeBuilder = new FakeBuilder();
    EXPECT_CALL(*mockFlowFactory, createBuilder(_))
        .WillOnce(Return(fakeBuilder));

    EXPECT_CALL(*realtimeBuilder, createBrokerFactory())
        .WillOnce(Return(mockBrokerFactory.release()));
    EXPECT_CALL(*mockBuildFlow, createFlowFactory())
        .WillOnce(Return(mockFlowFactory.release()));
    EXPECT_CALL(*mockPartition, CheckMemoryStatus())
        .WillRepeatedly(Return(IndexPartition::MS_OK));
    return std::make_pair(realtimeBuilder, fakeBuilder);
}

void ProcessedDocRtBuilderImplTest::prepareFakes(
        const BuildFlowThreadResource& buildFlowThreadResource) {
    _mockIndexPartition = new NiceMockIndexPartition();
    _indexPartition.reset(_mockIndexPartition);

    RealtimeBuilderResource buildResource(
            kmonitor::MetricsReporterPtr(), IE_NAMESPACE(util)::TaskSchedulerPtr(
                    new IE_NAMESPACE(util)::TaskScheduler),
            SwiftClientCreatorPtr(new SwiftClientCreator),
            buildFlowThreadResource);

    auto builder =
        new NiceMock<MockProcessedDocRtBuilderImpl>(_configPath, _indexPartition, buildResource);
    _realtimeBuilder.reset(builder);

    _mockBuildFlow = new NiceMockBuildFlow(_indexPartition, buildFlowThreadResource);
    EXPECT_CALL(*_realtimeBuilder, createBuildFlow(_,_))
        .WillOnce(Return(_mockBuildFlow));
    
    _mockBrokerFactory.reset(new NiceMockBrokerFactory());
    _mockFlowFactory.reset(new NiceMockFlowFactory());

    _fakeProducer = new FakeProcessedDocProducer(100);

    EXPECT_CALL(*_mockBrokerFactory, createProcessedDocProducer(_))
        .WillOnce(Return(_fakeProducer));

    _fakeBuilder = new FakeBuilder();
    EXPECT_CALL(*_mockFlowFactory, createBuilder(_))
        .WillOnce(Return(_fakeBuilder));

    EXPECT_CALL(*_realtimeBuilder, createBrokerFactory())
        .WillOnce(Return(_mockBrokerFactory.release()));
    EXPECT_CALL(*_mockBuildFlow, createFlowFactory())
        .WillOnce(Return(_mockFlowFactory.release()));
}

void ProcessedDocRtBuilderImplTest::prepareMocks() {
    _mockIndexPartition = new NiceMockIndexPartition();
    _indexPartition.reset(_mockIndexPartition);

    _mockBrokerFactory.reset(new MockBrokerFactory());
    _mockFlowFactory.reset(new MockFlowFactory());

    _mockProducer.reset(new NiceMockSwiftProcessedDocProducer);
    _mockBuilder.reset(new NiceMockBuilder);

    _realtimeBuilder.reset(new NiceMock<MockProcessedDocRtBuilderImpl>("", _indexPartition));
    _realtimeBuilder->_builder = _mockBuilder.get();
    _realtimeBuilder->_producer = _mockProducer.get();
}

void ProcessedDocRtBuilderImplTest::waitBuildDoc(FakeBuilder* builder, uint32_t docCount,
                                                 int64_t times) {
    for (int64_t i = 0; i < times; i ++) {
        if (docCount == builder->getBuildCount()) {
            return;
        }
        usleep(100000);
    }
    ASSERT_EQ(docCount, builder->getBuildCount());
}


TEST_F(ProcessedDocRtBuilderImplTest, testSkipRtBeforeTimestamp) {
    prepareMocks();

    _realtimeBuilder->setTimestampToSkip(30);
    EXPECT_CALL(*_mockBuilder, getIncVersionTimestampNonBlocking(_)).WillRepeatedly(
            SET0_AND_RETURN(20, Builder::RS_OK));
    EXPECT_CALL(*_mockBuilder, getLastLocatorNonBlocking(_)).WillRepeatedly(
            SET0_AND_RETURN(createLocator(10), Builder::RS_OK));
    EXPECT_CALL(*_mockProducer, seek(createLocator(30))).WillOnce(Return(true));
    _realtimeBuilder->skipRtBeforeTimestamp();
    EXPECT_EQ(ERROR_NONE, _realtimeBuilder->_errorCode);
}

TEST_F(ProcessedDocRtBuilderImplTest, testSkipToLatestLocator) {
    prepareMocks();

    // test first calibrate : use rt Locator seek
    EXPECT_CALL(*_mockBuilder, getIncVersionTimestampNonBlocking(_)).WillOnce(
            SET0_AND_RETURN(20, Builder::RS_OK));
    EXPECT_CALL(*_mockBuilder, getLastLocatorNonBlocking(_)).WillOnce(
            SET0_AND_RETURN(createLocator(30), Builder::RS_OK));
    EXPECT_CALL(*_mockProducer, seek(createLocator(30))).WillOnce(Return(true));
    _realtimeBuilder->skipToLatestLocator();

    // test already calibrate, not seek
    EXPECT_CALL(*_mockBuilder, getIncVersionTimestampNonBlocking(_)).WillOnce(
            SET0_AND_RETURN(20, Builder::RS_OK));
    EXPECT_CALL(*_mockBuilder, getLastLocatorNonBlocking(_)).WillOnce(
            SET0_AND_RETURN(createLocator(30), Builder::RS_OK));
    _realtimeBuilder->skipToLatestLocator();

    // test inc cover rt, use inc ts seek
    EXPECT_CALL(*_mockBuilder, getIncVersionTimestampNonBlocking(_)).WillOnce(
            SET0_AND_RETURN(40, Builder::RS_OK));
    EXPECT_CALL(*_mockBuilder, getLastLocatorNonBlocking(_)).WillOnce(
            SET0_AND_RETURN(createLocator(30), Builder::RS_OK));
    EXPECT_CALL(*_mockProducer, seek(createLocator(40))).WillOnce(Return(true));
    _realtimeBuilder->skipToLatestLocator();
}

TEST_F(ProcessedDocRtBuilderImplTest, testRtAfterInc) {
    prepareMocks();

    EXPECT_CALL(*_mockBuilder, getIncVersionTimestampNonBlocking(_)).WillRepeatedly(
            SET0_AND_RETURN(20, Builder::RS_OK));
    EXPECT_CALL(*_mockBuilder, getLastLocatorNonBlocking(_)).WillRepeatedly(
            SET0_AND_RETURN(createLocator(10), Builder::RS_OK));
    EXPECT_CALL(*_mockProducer, seek(createLocator(20))).WillOnce(Return(true));
    _realtimeBuilder->_isRecovered = true;
    _realtimeBuilder->executeBuildControlTask();
    EXPECT_EQ(ERROR_NONE, _realtimeBuilder->_errorCode);
}

TEST_F(ProcessedDocRtBuilderImplTest, testRtAfterIncSeekFailed) {
    prepareMocks();

    EXPECT_CALL(*_mockBuilder, getIncVersionTimestampNonBlocking(_)).WillRepeatedly(
            SET0_AND_RETURN(20, Builder::RS_OK));
    EXPECT_CALL(*_mockBuilder, getLastLocatorNonBlocking(_)).WillRepeatedly(
            SET0_AND_RETURN(createLocator(10), Builder::RS_OK));
    EXPECT_CALL(*_mockProducer, seek(createLocator(20))).WillOnce(Return(false));
    _realtimeBuilder->_isRecovered = true;
    _realtimeBuilder->executeBuildControlTask();
    EXPECT_EQ(ERROR_SWIFT_SEEK, _realtimeBuilder->_errorCode);
}

TEST_F(ProcessedDocRtBuilderImplTest, testGetLatestLocator) {
    prepareMocks();

    EXPECT_CALL(*_mockBuilder, getLastLocatorNonBlocking(_)).WillOnce(
            Return(Builder::RS_FAIL));
    common::Locator locator;
    bool fromInc;
    EXPECT_FALSE(_realtimeBuilder->getLatestLocator(locator, fromInc));
    EXPECT_FALSE(fromInc);
    EXPECT_EQ(INVALID_LOCATOR, locator);

    EXPECT_CALL(*_mockBuilder, getLastLocatorNonBlocking(_)).WillOnce(
            Return(Builder::RS_LOCK_BUSY));
    EXPECT_FALSE(_realtimeBuilder->getLatestLocator(locator, fromInc));
    EXPECT_FALSE(fromInc);
    EXPECT_EQ(INVALID_LOCATOR, locator);

    EXPECT_CALL(*_mockBuilder, getLastLocatorNonBlocking(_)).WillOnce(
            SET0_AND_RETURN(createLocator(10), Builder::RS_OK));
    
    EXPECT_CALL(*_mockBuilder, getIncVersionTimestampNonBlocking(_)).WillOnce(
            SET0_AND_RETURN(9, Builder::RS_OK));
    EXPECT_TRUE(_realtimeBuilder->getLatestLocator(locator, fromInc));
    EXPECT_FALSE(fromInc);
    EXPECT_EQ(createLocator(10), locator);

    EXPECT_CALL(*_mockBuilder, getLastLocatorNonBlocking(_)).WillOnce(
            SET0_AND_RETURN(createLocator(10), Builder::RS_OK));
    
    EXPECT_CALL(*_mockBuilder, getIncVersionTimestampNonBlocking(_)).WillOnce(
            SET0_AND_RETURN(20, Builder::RS_OK));
    EXPECT_TRUE(_realtimeBuilder->getLatestLocator(locator, fromInc));
    EXPECT_TRUE(fromInc);
    EXPECT_EQ(createLocator(20), locator);
}

TEST_F(ProcessedDocRtBuilderImplTest, testCheckRecoverWithoutBuilderOrProducer) {
    prepareMocks();

    _realtimeBuilder->_producer = NULL;
    _realtimeBuilder->_builder = NULL;
    _realtimeBuilder->_maxRecoverTime = 0;
    _realtimeBuilder->_startRecoverTime = TimeUtility::currentTimeInSeconds() - 1; // early 1s
    _realtimeBuilder->_isRecovered = false;
    _realtimeBuilder->executeBuildControlTask();
    EXPECT_EQ(ERROR_NONE, _realtimeBuilder->_errorCode);
    EXPECT_TRUE(_realtimeBuilder->_isRecovered);
}

TEST_F(ProcessedDocRtBuilderImplTest, testDumpCheckRecoverTakeMuchTime) {
    prepareMocks();

    EXPECT_CALL(*_mockBuilder, getIncVersionTimestampNonBlocking(_)).WillRepeatedly(
            SET0_AND_RETURN(21, Builder::RS_OK));
    EXPECT_CALL(*_mockBuilder, getLastLocatorNonBlocking(_)).WillRepeatedly(
            SET0_AND_RETURN(createLocator(20), Builder::RS_OK));
    EXPECT_CALL(*_mockProducer, seek(_)).WillOnce(Return(true));
    _realtimeBuilder->_maxRecoverTime = 0;
    _realtimeBuilder->_startRecoverTime = TimeUtility::currentTimeInSeconds() - 1; // early 1s
    _realtimeBuilder->_isRecovered = false;
    _realtimeBuilder->executeBuildControlTask();
    EXPECT_EQ(ERROR_NONE, _realtimeBuilder->_errorCode);
    EXPECT_TRUE(_realtimeBuilder->_isRecovered);
}

TEST_F(ProcessedDocRtBuilderImplTest, testCheckRecoverGetTimestampFail) {
    prepareMocks();

    EXPECT_CALL(*_mockProducer, getMaxTimestamp(_)).WillOnce(Return(false));
    _realtimeBuilder->_maxRecoverTime = 5; // 5s
    _realtimeBuilder->_startRecoverTime = TimeUtility::currentTimeInSeconds();
    _realtimeBuilder->_isRecovered = false;
    _realtimeBuilder->checkRecoverBuild(); // ignore input
    EXPECT_EQ(ERROR_NONE, _realtimeBuilder->_errorCode);
    EXPECT_FALSE(_realtimeBuilder->_isRecovered);
}

TEST_F(ProcessedDocRtBuilderImplTest, testCheckRecoverMaxTimeStamp) {
    prepareMocks();

    EXPECT_CALL(*_mockProducer, getMaxTimestamp(_)).WillOnce(SET0_AND_RETURN(int64_t(-1), true));
    _realtimeBuilder->_maxRecoverTime = 5; // 5s
    _realtimeBuilder->_startRecoverTime = TimeUtility::currentTimeInSeconds();
    _realtimeBuilder->_isRecovered = false;
    _realtimeBuilder->checkRecoverBuild(); // ignore input
    EXPECT_EQ(ERROR_NONE, _realtimeBuilder->_errorCode);
    EXPECT_TRUE(_realtimeBuilder->_isRecovered);
}

TEST_F(ProcessedDocRtBuilderImplTest, testCheckRecoverSuccess) {
    prepareMocks();

    EXPECT_CALL(*_mockProducer, getMaxTimestamp(_)).WillOnce(
            SET0_AND_RETURN(TimeUtility::currentTime() - 10 * 1000 * 1000, true)); // current time - 10s
    EXPECT_CALL(*_mockProducer, getLastReadTimestamp(_)).WillOnce(
            SET0_AND_RETURN(TimeUtility::currentTime(), true));

    _realtimeBuilder->_maxRecoverTime = 50; // 50s
    _realtimeBuilder->_startRecoverTime = TimeUtility::currentTimeInSeconds();
    _realtimeBuilder->_isRecovered = false;

    _realtimeBuilder->checkRecoverBuild();
    EXPECT_EQ(ERROR_NONE, _realtimeBuilder->_errorCode);
    EXPECT_TRUE(_realtimeBuilder->_isRecovered);
}

TEST_F(ProcessedDocRtBuilderImplTest, testCheckRecoverSuccessWithSmallTimeGap) {
    prepareMocks();
    {
        int64_t lastTsInProducer = TimeUtility::currentTime() - 10 * 1000 * 1000;
        EXPECT_CALL(*_mockProducer, getMaxTimestamp(_)).WillOnce(SET0_AND_RETURN(lastTsInProducer, true));
        _realtimeBuilder->_maxRecoverTime = 5; // 5s
        _realtimeBuilder->_startRecoverTime = TimeUtility::currentTimeInSeconds();
        _realtimeBuilder->_isRecovered = false;

        int64_t latestReadTimeInProducer = lastTsInProducer - 1 * 1000 * 1000;
        EXPECT_CALL(*_mockProducer, getLastReadTimestamp(_))
            .WillOnce(SET0_AND_RETURN(latestReadTimeInProducer, true));
        _realtimeBuilder->checkRecoverBuild();
        EXPECT_EQ(ERROR_NONE, _realtimeBuilder->_errorCode);
        EXPECT_TRUE(_realtimeBuilder->_isRecovered);
    }
    {
        int64_t lastTsInProducer = TimeUtility::currentTime() - 10 * 1000 * 1000;
        EXPECT_CALL(*_mockProducer, getMaxTimestamp(_)).WillOnce(SET0_AND_RETURN(lastTsInProducer, true)); 
        _realtimeBuilder->_maxRecoverTime = 5; // 5s
        _realtimeBuilder->_startRecoverTime = TimeUtility::currentTimeInSeconds();
        _realtimeBuilder->_isRecovered = false;

        int64_t latestReadTimeInProducer = lastTsInProducer - 2 * 1000 * 1000;
        EXPECT_CALL(*_mockProducer, getLastReadTimestamp(_))
            .WillOnce(SET0_AND_RETURN(latestReadTimeInProducer, true));
        _realtimeBuilder->checkRecoverBuild();
        EXPECT_EQ(ERROR_NONE, _realtimeBuilder->_errorCode);
        EXPECT_FALSE(_realtimeBuilder->_isRecovered);
    }
}

TEST_F(ProcessedDocRtBuilderImplTest, testRecoverLatencyConfig){
    prepareFakes();
    EXPECT_CALL(*_mockIndexPartition, CheckMemoryStatus())
        .WillRepeatedly(Return(IndexPartition::MS_OK));

    PartitionId partitionId =
        ProtoCreator::createPartitionId(
                ROLE_BUILDER, BUILD_STEP_FULL, 0, 0, "data_table", 1, "mainse_searcher");
    _realtimeBuilder->start(partitionId, _kvMap);
    EXPECT_EQ(200*1000, _realtimeBuilder->_recoverLatency);
}

TEST_F(ProcessedDocRtBuilderImplTest, testCheckRecoverFail) {
    prepareMocks();

    EXPECT_CALL(*_mockProducer, getMaxTimestamp(_)).WillOnce(
            SET0_AND_RETURN(TimeUtility::currentTime() - 10 * 1000 * 1000, true)); // current time - 10s
    _realtimeBuilder->_maxRecoverTime = 5; // 5s
    _realtimeBuilder->_startRecoverTime = TimeUtility::currentTimeInSeconds();
    _realtimeBuilder->_isRecovered = false;
    // current time - 20s

    int64_t latestTime = TimeUtility::currentTime() - int64_t(15 * 1000 * 1000);
    EXPECT_CALL(*_mockBuilder, getIncVersionTimestampNonBlocking(_))
        .WillRepeatedly(SET0_AND_RETURN(latestTime, Builder::RS_OK));
    EXPECT_CALL(*_mockBuilder, getLastLocatorNonBlocking(_))
        .WillRepeatedly(SET0_AND_RETURN(createLocator(20), Builder::RS_OK));

    _realtimeBuilder->checkRecoverBuild();
    EXPECT_EQ(ERROR_NONE, _realtimeBuilder->_errorCode);
    EXPECT_FALSE(_realtimeBuilder->_isRecovered);
}



TEST_F(ProcessedDocRtBuilderImplTest, testSimpleBuildWithThreadPool) {
    WorkflowThreadPoolPtr threadPool(new WorkflowThreadPool(1, 10));
    EXPECT_TRUE(threadPool->start());

    BuildFlowThreadResource threadResource(threadPool);

    prepareFakes(threadPool);
    EXPECT_CALL(*_mockIndexPartition, CheckMemoryStatus())
        .WillRepeatedly(Return(IndexPartition::MS_OK));

    PartitionId partitionId =
        ProtoCreator::createPartitionId(
                ROLE_BUILDER, BUILD_STEP_FULL, 0, 0, "data_table", 1, "mainse_searcher");

    _realtimeBuilder->start(partitionId, _kvMap);

    waitBuildDoc(_fakeBuilder, 100);
    common::Locator lastLocator;
    EXPECT_EQ(Builder::RS_OK, _fakeBuilder->getLastLocatorNonBlocking(lastLocator));
    EXPECT_EQ((int64_t)99, lastLocator.getOffset());
    ASSERT_EQ((size_t)1, threadPool->getMaxItemCount());
    _realtimeBuilder->stop();
    ASSERT_EQ((size_t)0, threadPool->getItemCount());
}

TEST_F(ProcessedDocRtBuilderImplTest, testSimpleBuild) {
    prepareFakes();
    EXPECT_CALL(*_mockIndexPartition, CheckMemoryStatus())
        .WillRepeatedly(Return(IndexPartition::MS_OK));

    PartitionId partitionId =
        ProtoCreator::createPartitionId(
                ROLE_BUILDER, BUILD_STEP_FULL, 0, 0, "data_table", 1, "mainse_searcher");

    _realtimeBuilder->start(partitionId, _kvMap);

    sleep(1);
    EXPECT_EQ((uint32_t)100, _fakeBuilder->getBuildCount());
    common::Locator lastLocator;
    EXPECT_EQ(Builder::RS_OK, _fakeBuilder->getLastLocatorNonBlocking(lastLocator));
    EXPECT_EQ((int64_t)99, lastLocator.getOffset());
}

MATCHER_P2(matchIncStep, from, to, "") {
    return arg.partitionId.step() == BUILD_STEP_INC
        && arg.partitionId.range().from() == from
        && arg.partitionId.range().to() == to;
}

TEST_F(ProcessedDocRtBuilderImplTest, testStart) {
    _mockIndexPartition = new NiceMockIndexPartition();
    _indexPartition.reset(_mockIndexPartition);
    
    _realtimeBuilder.reset(new NiceMock<MockProcessedDocRtBuilderImpl>(_configPath, _indexPartition));

    _mockBuildFlow = new NiceMockBuildFlow(_indexPartition);
    EXPECT_CALL(*_realtimeBuilder, createBuildFlow(_,_))
        .WillOnce(Return(_mockBuildFlow));
    
    _mockBrokerFactory.reset(new NiceMockBrokerFactory());
    _mockFlowFactory.reset(new NiceMockFlowFactory());

    _fakeProducer = new FakeProcessedDocProducer(100);

    PartitionId partitionId =
        ProtoCreator::createPartitionId(
                ROLE_BUILDER, BUILD_STEP_FULL, 0, 500, "data_table", 1, "mainse_searcher");

    EXPECT_CALL(*_mockBrokerFactory, createProcessedDocProducer(matchIncStep(0u,500u)))
        .WillOnce(Return(_fakeProducer));

    _fakeBuilder = new FakeBuilder();
    EXPECT_CALL(*_mockFlowFactory, createBuilder(_))
        .WillOnce(Return(_fakeBuilder));

    EXPECT_CALL(*_realtimeBuilder, createBrokerFactory())
        .WillOnce(Return(_mockBrokerFactory.release()));
    EXPECT_CALL(*_mockBuildFlow, createFlowFactory())
        .WillOnce(Return(_mockFlowFactory.release()));

    EXPECT_CALL(*_mockIndexPartition, CheckMemoryStatus())
        .WillRepeatedly(Return(IndexPartition::MS_OK));

    _realtimeBuilder->start(partitionId, _kvMap);
}

TEST_F(ProcessedDocRtBuilderImplTest, TestExecuteBuildControlTaskTryLock)
{
    _mockIndexPartition = new NiceMockIndexPartition();
    _indexPartition.reset(_mockIndexPartition);
    
    _realtimeBuilder.reset(new NiceMock<MockProcessedDocRtBuilderImpl>(_configPath, _indexPartition));
    autil::ThreadPtr thread = Thread::createThread(
        tr1::bind(&MockProcessedDocRtBuilderImpl::lockRealtimeMutex, _realtimeBuilder.get()));
    usleep(500000);
    EXPECT_CALL(*_mockIndexPartition, CheckMemoryStatus())
        .Times(0);
    _realtimeBuilder->executeBuildControlTask();
}

TEST_F(ProcessedDocRtBuilderImplTest, TestAutoSuspend) {
    prepareFakes();
    PartitionId partitionId =
        ProtoCreator::createPartitionId(
                ROLE_BUILDER, BUILD_STEP_FULL, 0, 0, "data_table", 1, "mainse_searcher");

    EXPECT_CALL(*_mockIndexPartition, CheckMemoryStatus())
        .WillOnce(Return(IndexPartition::MS_OK))
        .WillOnce(Return(IndexPartition::MS_OK))
        .WillOnce(Return(IndexPartition::MS_OK))
        .WillOnce(Return(IndexPartition::MS_OK))
        .WillOnce(Return(IndexPartition::MS_OK))
        .WillOnce(Return(IndexPartition::MS_OK)) // 3s
        .WillRepeatedly(Return(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT));

    _realtimeBuilder->start(partitionId, _kvMap);
    ProcessedDocWorkflow* processedDocWorkFlow = NULL;
    while(!processedDocWorkFlow)
    {
        processedDocWorkFlow = _mockBuildFlow->getProcessedDocWorkflow();
        usleep(100);
    }

    ASSERT_FALSE(processedDocWorkFlow->isSuspended()); // check ok
    _realtimeBuilder->suspendBuild();              // admin suspend
    ASSERT_TRUE(processedDocWorkFlow->isSuspended());
    sleep(1);
    ASSERT_TRUE(processedDocWorkFlow->isSuspended());  // auto resume for admin suspend not effect

    _realtimeBuilder->resumeBuild();               // admin resume
    ASSERT_FALSE(processedDocWorkFlow->isSuspended());

    sleep(5);
    ASSERT_TRUE(processedDocWorkFlow->isSuspended());  // auto suspend
}

TEST_F(ProcessedDocRtBuilderImplTest, TestAutoResume) {
    prepareFakes();
    PartitionId partitionId =
        ProtoCreator::createPartitionId(
                ROLE_BUILDER, BUILD_STEP_FULL, 0, 0, "data_table", 1, "mainse_searcher");

    EXPECT_CALL(*_mockIndexPartition, CheckMemoryStatus())
        .WillOnce(Return(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT))
        .WillOnce(Return(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT))
        .WillOnce(Return(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT))
        .WillRepeatedly(Return(IndexPartition::MS_OK));

    _realtimeBuilder->start(partitionId, _kvMap);
    ProcessedDocWorkflow* processedDocWorkFlow = NULL;
    while(!processedDocWorkFlow) {
        processedDocWorkFlow = _mockBuildFlow->getProcessedDocWorkflow();
        usleep(1);
    }

    while (!processedDocWorkFlow->isSuspended()) usleep(1); // auto suspend
    _realtimeBuilder->resumeBuild();
    while (!processedDocWorkFlow->isSuspended()) usleep(1); // resume not effect when auto suspend
    _realtimeBuilder->suspendBuild();             // admin suspend

    while (!processedDocWorkFlow->isSuspended()) usleep(1); // auto resume, but admin suspend
    _realtimeBuilder->resumeBuild();
    while (processedDocWorkFlow->isSuspended()) usleep(1); // after admin resume, auto resume
}

TEST_F(ProcessedDocRtBuilderImplTest, TestSuspendWhenStart) {
    prepareFakes();
    PartitionId partitionId =
        ProtoCreator::createPartitionId(
                ROLE_BUILDER, BUILD_STEP_FULL, 0, 0, "data_table", 1, "mainse_searcher");

    _fakeProducer->_sleep = 2;
    _realtimeBuilder->start(partitionId, _kvMap);
    _realtimeBuilder->suspendBuild();

    ProcessedDocWorkflow* processedDocWorkFlow = NULL;
    while(!processedDocWorkFlow) {
        processedDocWorkFlow = _mockBuildFlow->getProcessedDocWorkflow();
        usleep(1);
    }

    EXPECT_TRUE(_mockBuildFlow->_isSuspend);
    while (!processedDocWorkFlow->isSuspended()) usleep(1);
    _realtimeBuilder->resumeBuild();
    EXPECT_FALSE(_mockBuildFlow->_isSuspend);
    while (processedDocWorkFlow->isSuspended()) usleep(1);
}

ACTION(CreateFailFlowFactory) {
    NiceMockFlowFactory *flowFactory = new NiceMockFlowFactory;
    EXPECT_CALL(*flowFactory, createBuilder(_)).WillRepeatedly(ReturnNull());
    return flowFactory;
}

ACTION(CreateBrokerFactory) {
    return new NiceMockBrokerFactory;
}

TEST_F(ProcessedDocRtBuilderImplTest, testStartFailRecover) {
    setenv("max_retry_interval", "3", true);
    _mockIndexPartition = new NiceMockIndexPartition();
    _indexPartition.reset(_mockIndexPartition);
    _realtimeBuilder.reset(new NiceMock<MockProcessedDocRtBuilderImpl>(_configPath, _indexPartition));

    EXPECT_CALL(*_realtimeBuilder, createBrokerFactory())
        .WillRepeatedly(CreateBrokerFactory());

    _mockBuildFlow = new NiceMockBuildFlow(_indexPartition);
    EXPECT_CALL(*_realtimeBuilder, createBuildFlow(_,_))
        .WillOnce(Return(_mockBuildFlow));

    EXPECT_CALL(*_mockBuildFlow, createFlowFactory())
        .WillRepeatedly(CreateFailFlowFactory());

    PartitionId partitionId =
        ProtoCreator::createPartitionId(
                ROLE_BUILDER, BUILD_STEP_FULL, 0, 0, "data_table", 1, "mainse_searcher");
    _realtimeBuilder->start(partitionId, _kvMap);

    usleep(1 * 1000 * 1000);
    EXPECT_FALSE(_realtimeBuilder->isRecovered());
    usleep(3 * 1000 * 1000);
    EXPECT_TRUE(_realtimeBuilder->isRecovered());
    EXPECT_FALSE(_mockBuildFlow->isStarted());
    {
        autil::ScopedLock lock(_realtimeBuilder->_realtimeMutex);
        EXPECT_CALL(*_mockBuildFlow, createFlowFactory())
            .WillRepeatedly(Return(new NiceMockFlowFactory));
    }

    usleep(8 * 1000 * 1000);
    EXPECT_TRUE(_realtimeBuilder->isRecovered());
    EXPECT_TRUE(_mockBuildFlow->_processorToBuildFlow.get() != NULL);
    EXPECT_TRUE(_realtimeBuilder->_builder);
    unsetenv("max_retry_interval");
}

TEST_F(ProcessedDocRtBuilderImplTest, testStartFailStop) {
    _mockIndexPartition = new NiceMockIndexPartition();
    _indexPartition.reset(_mockIndexPartition);
    _realtimeBuilder.reset(new NiceMock<MockProcessedDocRtBuilderImpl>(_configPath, _indexPartition));

    EXPECT_CALL(*_realtimeBuilder, createBrokerFactory())
        .WillRepeatedly(CreateBrokerFactory());

    _mockBuildFlow = new NiceMockBuildFlow(_indexPartition);
    EXPECT_CALL(*_realtimeBuilder, createBuildFlow(_,_))
        .WillOnce(Return(_mockBuildFlow));

    EXPECT_CALL(*_mockBuildFlow, createFlowFactory())
        .WillRepeatedly(CreateFailFlowFactory());

    PartitionId partitionId =
        ProtoCreator::createPartitionId(
                ROLE_BUILDER, BUILD_STEP_FULL, 0, 0, "data_table", 1, "mainse_searcher");
    _realtimeBuilder->start(partitionId, _kvMap);

    usleep(1 * 1000 * 1000);
    EXPECT_FALSE(_realtimeBuilder->isRecovered());
    usleep(3 * 1000 * 1000);
    EXPECT_TRUE(_realtimeBuilder->isRecovered());
    EXPECT_FALSE(_mockBuildFlow->_processorToBuildFlow.get() != NULL);
}

TEST_F(ProcessedDocRtBuilderImplTest, testStartSuccessRecover) {
    _mockIndexPartition = new NiceMockIndexPartition();
    _indexPartition.reset(_mockIndexPartition);
    _realtimeBuilder.reset(new NiceMock<MockProcessedDocRtBuilderImpl>(_configPath, _indexPartition));

    EXPECT_CALL(*_realtimeBuilder, createBrokerFactory())
        .WillRepeatedly(CreateBrokerFactory());

    _mockBuildFlow = new NiceMockBuildFlow(_indexPartition);
    EXPECT_CALL(*_realtimeBuilder, createBuildFlow(_,_))
        .WillOnce(Return(_mockBuildFlow));

    EXPECT_CALL(*_mockBuildFlow, createFlowFactory())
        .WillRepeatedly(Return(new NiceMockFlowFactory));

    PartitionId partitionId =
        ProtoCreator::createPartitionId(
                ROLE_BUILDER, BUILD_STEP_FULL, 0, 0, "data_table", 1, "mainse_searcher");
    _realtimeBuilder->start(partitionId, _kvMap);

    usleep(1 * 1000 * 1000);
    EXPECT_FALSE(_realtimeBuilder->isRecovered());
    usleep(3 * 1000 * 1000);
    EXPECT_TRUE(_realtimeBuilder->isRecovered());
    EXPECT_TRUE(_mockBuildFlow->_processorToBuildFlow.get() != NULL);
    EXPECT_TRUE(_realtimeBuilder->_builder);
}

TEST_F(ProcessedDocRtBuilderImplTest, testForceRecoverAndSetTimestampToSkip) {
    prepareMocks();
    ASSERT_FALSE(_realtimeBuilder->_isRecovered);
    ASSERT_EQ(-1, _realtimeBuilder->_timestampToSkip);
    _realtimeBuilder->forceRecover();
    ASSERT_TRUE(_realtimeBuilder->_isRecovered);
    _realtimeBuilder->setTimestampToSkip(10);
    ASSERT_EQ(10, _realtimeBuilder->_timestampToSkip);
}

TEST_F(ProcessedDocRtBuilderImplTest, testMultiBuildWithThreadPool) {
    WorkflowThreadPoolPtr threadPool(new WorkflowThreadPool(1, 10));
    EXPECT_TRUE(threadPool->start());

    BuildFlowThreadResource threadResource(threadPool);

    prepareFakes(threadResource);
    EXPECT_CALL(*_mockIndexPartition, CheckMemoryStatus())
        .WillRepeatedly(Return(IndexPartition::MS_OK));

    PartitionId partitionId =
        ProtoCreator::createPartitionId(
                ROLE_BUILDER, BUILD_STEP_FULL, 0, 0, "data_table", 1, "mainse_searcher");
    auto builders = makeFakeBuilder(TEST_DATA_PATH"/realtime_builder_test2",
                                            threadResource);
    auto fakeBuilder2 = builders.second;
    auto realtimeBuilder2 = builders.first;
    assert(fakeBuilder2);
    PartitionId partitionId2 =
        ProtoCreator::createPartitionId(
                ROLE_BUILDER, BUILD_STEP_FULL, 0, 0, "data2_table", 1, "mainse_searcher2");
    KeyValueMap tmpKV;
    realtimeBuilder2->start(partitionId2, tmpKV);
    _realtimeBuilder->start(partitionId, _kvMap);

    waitBuildDoc(_fakeBuilder, 100);
    waitBuildDoc(fakeBuilder2, 10);
    common::Locator lastLocator, lastLocator2;
    EXPECT_EQ(Builder::RS_OK, _fakeBuilder->getLastLocatorNonBlocking(lastLocator));
    EXPECT_EQ(Builder::RS_OK, fakeBuilder2->getLastLocatorNonBlocking(lastLocator2));
    EXPECT_EQ((int64_t)99, lastLocator.getOffset());
    EXPECT_EQ((int64_t)9, lastLocator2.getOffset());
    ASSERT_EQ((size_t)2, threadPool->getMaxItemCount());
    _realtimeBuilder->stop();
    ASSERT_EQ((size_t)0, threadPool->getItemCount());
}

TEST_F(ProcessedDocRtBuilderImplTest, testResetStartSkipTimestamp) {
    prepareMocks();
    EXPECT_CALL(*_realtimeBuilder, getStartSkipTimestamp())
        .WillOnce(Return(0))
        .WillOnce(Return(10));

    KeyValueMap kvMap;
    _realtimeBuilder->resetStartSkipTimestamp(kvMap);
    ASSERT_TRUE(kvMap.find(CHECKPOINT) == kvMap.end());

    _realtimeBuilder->resetStartSkipTimestamp(kvMap);
    ASSERT_TRUE(kvMap.find(CHECKPOINT) != kvMap.end());
    ASSERT_EQ(string("10"), kvMap[CHECKPOINT]);

    _realtimeBuilder->resetStartSkipTimestamp(kvMap);
    ASSERT_EQ(string("10"), kvMap[CHECKPOINT]);    
}

TEST_F(ProcessedDocRtBuilderImplTest, testProducerSeek) {
    prepareMocks();
    _mockProducer->_startTimestamp = 10000;

    Locator targetLocator(0, 20000);
    EXPECT_CALL(*_mockProducer, seek(Locator(10000, 20000)))
        .WillOnce(Return(true));
    EXPECT_CALL(*_mockProducer, seek(Locator(10000, 20001)))
        .WillOnce(Return(false));
    
    ASSERT_TRUE(_realtimeBuilder->producerSeek(targetLocator));

    Locator targetLocator1(1, 20000);
    ASSERT_TRUE(_realtimeBuilder->producerSeek(targetLocator1));

    Locator targetLocator2(2, 20001);
    ASSERT_FALSE(_realtimeBuilder->producerSeek(targetLocator2));

    ASSERT_EQ(Locator(10000, 20000), _realtimeBuilder->_lastSkipedLocator);
}


}
}
