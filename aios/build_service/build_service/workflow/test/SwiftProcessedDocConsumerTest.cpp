#include "build_service/test/unittest.h"
#include "build_service/workflow/SwiftProcessedDocConsumer.h"
#include "build_service/workflow/test/MockSwiftWriter.h"
#include "build_service/document/test/DocumentTestHelper.h"
#include "build_service/common/Locator.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/CounterFileSynchronizer.h"
#include "build_service/common/CounterSynchronizerCreator.h"
#include "build_service/common/test/MockCounterSynchronizer.h"
#include "build_service/util/FileUtil.h"
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/util/counter/state_counter.h>

using namespace std;
using namespace testing;
BS_NAMESPACE_USE(common);
BS_NAMESPACE_USE(config);
BS_NAMESPACE_USE(util);
IE_NAMESPACE_USE(document);
SWIFT_USE_NAMESPACE(protocol);
SWIFT_USE_NAMESPACE(client);

using namespace build_service::document;

namespace build_service {
namespace workflow {

class SwiftProcessedDocConsumerTest : public BUILD_SERVICE_TESTBASE {};

TEST_F(SwiftProcessedDocConsumerTest, testSimpleProcess) {
    shared_ptr<MockSwiftWriter> swiftWriter1(new NiceMockSwiftWriter);
    shared_ptr<MockSwiftWriter> swiftWriter2(new NiceMockSwiftWriter);

    map<string, SwiftWriterWithMetric> writers;
    writers["cluster1"].swiftWriter = swiftWriter1;
    writers["cluster2"].swiftWriter = swiftWriter2;

    SwiftProcessedDocConsumer consumer;
    string counterFilePath = FileUtil::joinFilePath(GET_TEST_DATA_PATH(), "counter");

    CounterConfigPtr counterConfig(new CounterConfig());
    counterConfig->params[COUNTER_PARAM_FILEPATH] = counterFilePath;
    bool loadFromExisted;
    IE_NAMESPACE(util)::CounterMapPtr counterMap = CounterSynchronizerCreator::loadCounterMap(
            counterConfig, loadFromExisted);
    CounterSynchronizerPtr counterSynchronizer(CounterSynchronizerCreator::create(
                    counterMap, counterConfig));
    
    ASSERT_TRUE(counterSynchronizer);
    ASSERT_TRUE(consumer.init(writers, counterSynchronizer, 1000, 0));
    ProcessedDocumentVecPtr item(new ProcessedDocumentVec);

    FakeProcessedDoc fakeProcessedDoc1(FakeDocument("pk_1"), 1, "cluster1:100");
    ProcessedDocumentPtr doc1 = DocumentTestHelper::createProcessedDocument(fakeProcessedDoc1);
    item->push_back(doc1);

    FakeProcessedDoc fakeProcessedDoc2(FakeDocument("pk_2"), 2, "cluster1:101,cluster2:102");
    ProcessedDocumentPtr doc2 = DocumentTestHelper::createProcessedDocument(fakeProcessedDoc2);
    item->push_back(doc2);

    FakeProcessedDoc fakeProcessedDoc3(FakeDocument("pk_3"), 3, "cluster2:103,not_exist_cluster:104,cluster1:105");
    ProcessedDocumentPtr doc3 = DocumentTestHelper::createProcessedDocument(fakeProcessedDoc3);
    item->push_back(doc3);

    ASSERT_EQ(FE_OK, consumer.consume(item));

    vector<MessageInfo> messages1 = swiftWriter1->getMessages();
    ASSERT_EQ(size_t(3), messages1.size());

    EXPECT_EQ(SwiftProcessedDocConsumer::transToDocStr(doc1->getDocument()), messages1[0].data);
    EXPECT_EQ(100, messages1[0].uint16Payload);
    EXPECT_EQ(1, messages1[0].checkpointId);

    EXPECT_EQ(SwiftProcessedDocConsumer::transToDocStr(doc2->getDocument()), messages1[1].data);
    EXPECT_EQ(101, messages1[1].uint16Payload);
    EXPECT_EQ(2, messages1[1].checkpointId);

    EXPECT_EQ(SwiftProcessedDocConsumer::transToDocStr(doc3->getDocument()), messages1[2].data);
    EXPECT_EQ(105, messages1[2].uint16Payload);
    EXPECT_EQ(3, messages1[2].checkpointId);

    vector<MessageInfo> messages2 = swiftWriter2->getMessages();

    ASSERT_EQ(size_t(2), messages2.size());
    EXPECT_EQ(SwiftProcessedDocConsumer::transToDocStr(doc2->getDocument()), messages2[0].data);
    EXPECT_EQ(102, messages2[0].uint16Payload);
    EXPECT_EQ(2, messages2[0].checkpointId);

    EXPECT_EQ(SwiftProcessedDocConsumer::transToDocStr(doc3->getDocument()), messages2[1].data);
    EXPECT_EQ(103, messages2[1].uint16Payload);
    EXPECT_EQ(3, messages2[1].checkpointId);
}

TEST_F(SwiftProcessedDocConsumerTest, testGetLocator) {
    string counterFilePath = GET_TEST_DATA_PATH() + "/counterFile";
    {
        // test load Counters from non-exist counter file path
        CounterConfigPtr counterConfig(new CounterConfig());
        counterConfig->position = "zookeeper";
        counterConfig->params[COUNTER_PARAM_FILEPATH] = counterFilePath;
        bool loadFromExisted;
        IE_NAMESPACE(util)::CounterMapPtr counterMap = CounterSynchronizerCreator::loadCounterMap(
                counterConfig, loadFromExisted);
        ASSERT_FALSE(loadFromExisted);
        CounterSynchronizerPtr counterSynchronizer(CounterSynchronizerCreator::create(
                        counterMap, counterConfig));
        ASSERT_TRUE(counterSynchronizer);
        SwiftProcessedDocConsumer consumer;
        ASSERT_TRUE(consumer.initCounters(counterSynchronizer->getCounterMap()));
        
        EXPECT_EQ(0,  consumer._srcCounter->Get());
        EXPECT_EQ(-1,  consumer._checkpointCounter->Get()); 

        common::Locator locator(101, 16);
        EXPECT_TRUE(consumer.getLocator(locator));
        // test locator is un-touched
        EXPECT_EQ(101u, locator.getSrc());
        EXPECT_EQ(16, locator.getOffset());

        consumer._srcCounter->Set(1);
        consumer._checkpointCounter->Set(18);

        ASSERT_TRUE(counterSynchronizer->sync());
    }
    {
        SwiftProcessedDocConsumer consumer;
        // test load counters from existed counter file
        bool loadFromExisted;
        CounterConfigPtr counterConfig(new CounterConfig());
        counterConfig->position = "zookeeper";
        counterConfig->params[COUNTER_PARAM_FILEPATH] = counterFilePath;
        IE_NAMESPACE(util)::CounterMapPtr counterMap = CounterSynchronizerCreator::loadCounterMap(
                counterConfig, loadFromExisted);
        ASSERT_TRUE(loadFromExisted);
        CounterSynchronizerPtr counterSynchronizer(CounterSynchronizerCreator::create(
                        counterMap, counterConfig));
        ASSERT_TRUE(counterSynchronizer);
        ASSERT_TRUE(consumer.initCounters(counterSynchronizer->getCounterMap()));
        EXPECT_EQ(1,  consumer._srcCounter->Get());
        EXPECT_EQ(18,  consumer._checkpointCounter->Get());
        common::Locator locator(101, 16);
        EXPECT_TRUE(consumer.getLocator(locator));
        // test locator is changed
        EXPECT_EQ(1u, locator.getSrc());
        EXPECT_EQ(18, locator.getOffset());
    }
    {
        SwiftProcessedDocConsumer consumer;
        IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
        MockCounterSynchronizer* counterSynchronizer = new NiceMockCounterSynchronizer();
        counterSynchronizer->_counterMap = counterMap;
        ASSERT_TRUE(consumer.initCounters(counterSynchronizer->getCounterMap()));
        consumer._counterSynchronizer.reset(counterSynchronizer);
        
        shared_ptr<MockSwiftWriter> swiftWriter(new NiceMockSwiftWriter);
        consumer._writers["cluster"].swiftWriter = swiftWriter;
        consumer._src = 1;
        // test writer return checkpoint = -1 
        swiftWriter->setCheckpoint(-1);
        common::Locator locator(6, 66);
        EXPECT_CALL(*counterSynchronizer, sync()).Times(0);
        EXPECT_TRUE(consumer.syncCounters(consumer.getMinCheckpoint()));
        EXPECT_TRUE(consumer.getLocator(locator));
        EXPECT_EQ(6u, locator.getSrc());
        EXPECT_EQ(66, locator.getOffset());        
        // test sync success, locator will be updated
        swiftWriter->setCheckpoint(90);
        EXPECT_CALL(*counterSynchronizer, sync()).WillOnce(Return(true));
        EXPECT_TRUE(consumer.syncCounters(consumer.getMinCheckpoint()));
        EXPECT_TRUE(consumer.getLocator(locator));
        EXPECT_EQ(1u, locator.getSrc()); 
        EXPECT_EQ(90, locator.getOffset());
        // test sync failed, cached locator updated
        consumer._src = 2;
        swiftWriter->setCheckpoint(100);
        EXPECT_CALL(*counterSynchronizer, sync()).WillOnce(Return(false));
        EXPECT_FALSE(consumer.syncCounters(consumer.getMinCheckpoint()));
        EXPECT_TRUE(consumer.getLocator(locator));
        EXPECT_EQ(2u, locator.getSrc()); 
        EXPECT_EQ(100, locator.getOffset());
        // test sync success, 
        consumer._src = 2;
        swiftWriter->setCheckpoint(101);
        EXPECT_CALL(*counterSynchronizer, sync()).WillOnce(Return(true));
        EXPECT_TRUE(consumer.syncCounters(consumer.getMinCheckpoint()));
        EXPECT_TRUE(consumer.getLocator(locator));
        EXPECT_EQ(2u, locator.getSrc()); 
        EXPECT_EQ(101, locator.getOffset());         
    }
}

TEST_F(SwiftProcessedDocConsumerTest, testSyncLocator) {
    SwiftProcessedDocConsumer consumer;
    string counterFilePath = GET_TEST_DATA_PATH() + "/counterFile";

    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
    auto locatorSrc = counterMap->GetStateCounter(BS_COUNTER(locatorSource));
    auto locatorCheckpoint = counterMap->GetStateCounter(BS_COUNTER(locatorCheckpoint));
    locatorSrc->Set(1);
    locatorCheckpoint->Set(100);
    ASSERT_TRUE(util::FileUtil::writeFile(counterFilePath, counterMap->ToJsonString()));

    CounterFileSynchronizer* syner = new CounterFileSynchronizer();
    syner->init(counterMap, counterFilePath);
    consumer._counterSynchronizer.reset(syner);
    consumer.initCounters(syner->getCounterMap());
    
    shared_ptr<MockSwiftWriter> swiftWriter1(new NiceMockSwiftWriter);
    shared_ptr<MockSwiftWriter> swiftWriter2(new NiceMockSwiftWriter);

    consumer._writers["cluster"].swiftWriter = swiftWriter1;
    consumer._writers["cluster2"].swiftWriter = swiftWriter2;
    consumer._src = 1;

    EXPECT_CALL(*swiftWriter1, getCommittedCheckpointId())
        .WillOnce(Return(100))
        .WillOnce(Return(200))
        .WillOnce(Return(250))
        .WillOnce(Return(300));
    swiftWriter1->_writeMetrics.uncommittedMsgCount = 0;
    EXPECT_CALL(*swiftWriter2, getCommittedCheckpointId())
        .WillOnce(Return(-1))
        .WillOnce(Return(-1))
        .WillOnce(Return(150))
        .WillOnce(Return(200));
    swiftWriter2->_writeMetrics.uncommittedMsgCount = 0;
    //test all send
    ASSERT_TRUE(consumer.syncCounters(consumer.getMinCheckpoint()));
    common::Locator locator;
    consumer.getLocator(locator);
    ASSERT_EQ(100u, locator.getOffset());
    //test part send part not send, checkpoint not decrease
    swiftWriter2->_writeMetrics.uncommittedMsgCount = 1;
    ASSERT_TRUE(consumer.syncCounters(consumer.getMinCheckpoint()));
    consumer.getLocator(locator);
    ASSERT_EQ(100u, locator.getOffset());
    //test part send part sending
    ASSERT_TRUE(consumer.syncCounters(consumer.getMinCheckpoint()));
    consumer.getLocator(locator);
    ASSERT_EQ(150u, locator.getOffset());
    //test all sending
    swiftWriter1->_writeMetrics.uncommittedMsgCount = 1;
    ASSERT_TRUE(consumer.syncCounters(consumer.getMinCheckpoint()));
    consumer.getLocator(locator);
    ASSERT_EQ(200u, locator.getOffset());
}

TEST_F(SwiftProcessedDocConsumerTest, testSerializeCheckpoint) {
    SwiftProcessedDocConsumer consumer;
    string counterFilePath = GET_TEST_DATA_PATH() + "/counterFile";

    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
    auto locatorSrc = counterMap->GetStateCounter(BS_COUNTER(locatorSource));
    auto locatorCheckpoint = counterMap->GetStateCounter(BS_COUNTER(locatorCheckpoint));
    locatorSrc->Set(1);
    locatorCheckpoint->Set(100);
    ASSERT_TRUE(util::FileUtil::writeFile(counterFilePath, counterMap->ToJsonString()));

    CounterFileSynchronizer* syner = new CounterFileSynchronizer();
    syner->init(counterMap, counterFilePath);
    consumer._counterSynchronizer.reset(syner);
    consumer.initCounters(syner->getCounterMap());
    
    shared_ptr<MockSwiftWriter> swiftWriter(new NiceMockSwiftWriter);

    consumer._writers["cluster"].swiftWriter = swiftWriter;
    consumer._src = 1;
    {
        // checkpoint exist, use checkpoint if checkpoint > swift checkpoint
        swiftWriter->setCheckpoint(90);
        ASSERT_TRUE(consumer.syncCounters(consumer.getMinCheckpoint()));
        common::Locator locator;
        ASSERT_TRUE(consumer.getLocator(locator));
        EXPECT_EQ(int64_t(100), locator.getOffset());
    }
    {
        // checkpoint exist, if checkpoint < swift checkpoint, use swift check point
        swiftWriter->setCheckpoint(110);
        ASSERT_TRUE(consumer.syncCounters(consumer.getMinCheckpoint()));        
        common::Locator locator;
        ASSERT_TRUE(consumer.getLocator(locator));
        EXPECT_EQ(int64_t(110), locator.getOffset());
    }
    {
        consumer._src = 10;
        swiftWriter->setCheckpoint(10);
        ASSERT_TRUE(consumer.syncCounters(consumer.getMinCheckpoint()));        
        common::Locator locator;
        ASSERT_TRUE(consumer.getLocator(locator));
        EXPECT_EQ(uint64_t(10), locator.getSrc());
        EXPECT_EQ(int64_t(10), locator.getOffset());
    }
}

TEST_F(SwiftProcessedDocConsumerTest, testInitFromInvalidCheckpoint) {
    SwiftProcessedDocConsumer consumer;
    string counterFilePath = GET_TEST_DATA_PATH() + "/counterFile";

    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
    auto locatorSrc = counterMap->GetStateCounter(BS_COUNTER(locatorSource));
    auto locatorCheckpoint = counterMap->GetStateCounter(BS_COUNTER(locatorCheckpoint));
    locatorSrc->Set(0);
    locatorCheckpoint->Set(1000);
    ASSERT_TRUE(util::FileUtil::writeFile(counterFilePath, counterMap->ToJsonString()));

    CounterFileSynchronizer* syner = new CounterFileSynchronizer();
    syner->init(counterMap, counterFilePath);
    consumer._counterSynchronizer.reset(syner);
    
    shared_ptr<MockSwiftWriter> swiftWriter(new NiceMockSwiftWriter);

    consumer._writers["cluster"].swiftWriter = swiftWriter;
    consumer._src = 1;
    consumer.initCounters(syner->getCounterMap());
    EXPECT_EQ(0, consumer._cachedLocator.getSrc());
    EXPECT_EQ(-1, consumer._cachedLocator.getOffset());

    swiftWriter->setCheckpoint(20);
    ASSERT_TRUE(consumer.syncCounters(consumer.getMinCheckpoint()));
    common::Locator locator;
    ASSERT_TRUE(consumer.getLocator(locator));
    EXPECT_EQ(1, locator.getSrc());
    EXPECT_EQ(20, locator.getOffset());

    EXPECT_EQ(1, consumer._srcCounter->Get());
    EXPECT_EQ(20, consumer._checkpointCounter->Get());
}

TEST_F(SwiftProcessedDocConsumerTest, testSerializeCheckpointFailed) {
    SwiftProcessedDocConsumer consumer;
    string counterFilePath = GET_TEST_DATA_PATH() + "/counterFile";

    shared_ptr<MockSwiftWriter> swiftWriter(new NiceMockSwiftWriter);

    consumer._writers["cluster"].swiftWriter = swiftWriter;
    consumer._src = 1;
    {
        swiftWriter->setCheckpoint(90);
        ASSERT_TRUE(consumer.syncCounters(consumer.getMinCheckpoint()));
        common::Locator locator;
        ASSERT_TRUE(consumer.getLocator(locator));
        EXPECT_EQ(int64_t(90), locator.getOffset());
    }
}

TEST_F(SwiftProcessedDocConsumerTest, testStop) {
    map<string, SwiftWriterWithMetric> writers;
    shared_ptr<MockSwiftWriter> swiftWriter(new NiceMockSwiftWriter);
    writers["cluster1"].swiftWriter = swiftWriter;
    SwiftProcessedDocConsumer consumer;
    EXPECT_CALL(*swiftWriter, getCommittedCheckpointId())
        .WillOnce(Return(100))
        .WillOnce(Return(200));
    EXPECT_CALL(*swiftWriter, waitFinished(_))
        .WillOnce(Return(ERROR_CLIENT_SOME_MESSAGE_SEND_FAILED))
        .WillOnce(Return(ERROR_CLIENT_SOME_MESSAGE_SEND_FAILED))
        .WillOnce(Return(ERROR_CLIENT_SOME_MESSAGE_SEND_FAILED))
        .WillOnce(Return(ERROR_NONE));

    string counterFilePath = FileUtil::joinFilePath(GET_TEST_DATA_PATH(), "counter");
    CounterConfigPtr counterConfig(new CounterConfig());
    counterConfig->params[COUNTER_PARAM_FILEPATH] = counterFilePath;
    bool loadFromExisted;
    IE_NAMESPACE(util)::CounterMapPtr counterMap = CounterSynchronizerCreator::loadCounterMap(
            counterConfig, loadFromExisted);
    CounterSynchronizerPtr counterSynchronizer(CounterSynchronizerCreator::create(
                    counterMap, counterConfig)); 
    
    ASSERT_TRUE(consumer.init(writers, counterSynchronizer, 10, 0));
    consumer.stop(FE_EOF);
    common::Locator locator;
    EXPECT_FALSE(consumer._syncCounterThread);
    EXPECT_TRUE(consumer.getLocator(locator));
    EXPECT_EQ(int64_t(200), locator.getOffset());
}

TEST_F(SwiftProcessedDocConsumerTest, testSkipDocuments) {
    shared_ptr<MockSwiftWriter> swiftWriter1(new NiceMockSwiftWriter);
    shared_ptr<MockSwiftWriter> swiftWriter2(new NiceMockSwiftWriter);

    map<string, SwiftWriterWithMetric> writers;
    writers["cluster1"].swiftWriter = swiftWriter1;
    writers["cluster2"].swiftWriter = swiftWriter2;

    SwiftProcessedDocConsumer consumer;
    string counterFilePath = FileUtil::joinFilePath(GET_TEST_DATA_PATH(), "counter");
    CounterConfigPtr counterConfig(new CounterConfig());
    counterConfig->params[COUNTER_PARAM_FILEPATH] = counterFilePath;

    bool loadFromExisted;
    IE_NAMESPACE(util)::CounterMapPtr counterMap = CounterSynchronizerCreator::loadCounterMap(
            counterConfig, loadFromExisted);
    CounterSynchronizerPtr counterSynchronizer(CounterSynchronizerCreator::create(
                    counterMap, counterConfig));
    
    ASSERT_TRUE(consumer.init(writers, counterSynchronizer, 1, 0));
    ProcessedDocumentVecPtr item(new ProcessedDocumentVec);

    FakeProcessedDoc fakeProcessedDoc1(FakeDocument("pk_1"), 1, "cluster1:100");
    ProcessedDocumentPtr doc1 = DocumentTestHelper::createProcessedDocument(fakeProcessedDoc1);
    item->push_back(doc1);

    FakeProcessedDoc fakeProcessedDoc2(FakeDocument("pk_2"), 2, "cluster1:101,cluster2:102");
    ProcessedDocumentPtr doc2 = DocumentTestHelper::createProcessedDocument(fakeProcessedDoc2);
    item->push_back(doc2);

    FakeProcessedDoc fakeProcessedDoc3(FakeDocument("pk_3"), 3, "cluster2:103,not_exist_cluster:104,cluster1:105");
    ProcessedDocumentPtr doc3 = DocumentTestHelper::createProcessedDocument(fakeProcessedDoc3);
    item->push_back(doc3);

    FakeProcessedDoc fakeProcessedDoc4(FakeDocument("pk_4"), 4, "cluster2:103,cluster1:105");
    ProcessedDocumentPtr doc4 = DocumentTestHelper::createProcessedDocument(fakeProcessedDoc4);
    doc4->setDocument(DocumentPtr());
    item->push_back(doc4);

    ASSERT_EQ(FE_OK, consumer.consume(item));
    swiftWriter1->setCheckpoint(3);
    swiftWriter2->setCheckpoint(3);
    sleep(2);
    auto srcCounter = counterSynchronizer->getCounterMap()->GetStateCounter(BS_COUNTER(locatorSrc));
    auto checkpointCounter = counterSynchronizer->getCounterMap()->GetStateCounter(BS_COUNTER(locatorCheckpoint));
    EXPECT_EQ(0, srcCounter->Get());
    EXPECT_EQ(3, checkpointCounter->Get());

    vector<MessageInfo> messages1 = swiftWriter1->getMessages();
    ASSERT_EQ(size_t(3), messages1.size());

    EXPECT_EQ(SwiftProcessedDocConsumer::transToDocStr(doc1->getDocument()), messages1[0].data);
    EXPECT_EQ(100, messages1[0].uint16Payload);
    EXPECT_EQ(1, messages1[0].checkpointId);

    EXPECT_EQ(SwiftProcessedDocConsumer::transToDocStr(doc2->getDocument()), messages1[1].data);
    EXPECT_EQ(101, messages1[1].uint16Payload);
    EXPECT_EQ(2, messages1[1].checkpointId);

    EXPECT_EQ(SwiftProcessedDocConsumer::transToDocStr(doc3->getDocument()), messages1[2].data);
    EXPECT_EQ(105, messages1[2].uint16Payload);
    EXPECT_EQ(3, messages1[2].checkpointId);

    vector<MessageInfo> messages2 = swiftWriter2->getMessages();

    ASSERT_EQ(size_t(2), messages2.size());
    EXPECT_EQ(SwiftProcessedDocConsumer::transToDocStr(doc2->getDocument()), messages2[0].data);
    EXPECT_EQ(102, messages2[0].uint16Payload);
    EXPECT_EQ(2, messages2[0].checkpointId);

    EXPECT_EQ(SwiftProcessedDocConsumer::transToDocStr(doc3->getDocument()), messages2[1].data);
    EXPECT_EQ(103, messages2[1].uint16Payload);
    EXPECT_EQ(3, messages2[1].checkpointId);

    consumer.stop(FE_EOF);

    EXPECT_EQ(0, srcCounter->Get());
    EXPECT_EQ(4, checkpointCounter->Get());
}

}
}
