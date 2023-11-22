#include "swift/broker/service/TopicPartitionSupervisor.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <map>
#include <memory>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "ext/alloc_traits.h"
#include "swift/broker/service/BrokerPartition.h"
#include "swift/broker/storage/MessageBrain.h"
#include "swift/broker/storage/MessageGroup.h"
#include "swift/common/Common.h"
#include "swift/common/MemoryMessage.h"
#include "swift/config/BrokerConfig.h"
#include "swift/heartbeat/ThreadSafeTaskStatus.h"
#include "swift/heartbeat/ZkConnectionStatus.h"
#include "swift/log/BrokerLogClosure.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageComparator.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/test/MessageCreator.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/ReaderRec.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::storage;
using namespace swift::common;
using namespace swift::util;
using namespace swift::heartbeat;

namespace swift {
namespace service {

class TopicPartitionSupervisorTest : public TESTBASE {
public:
    TopicPartitionSupervisorTest();
    ~TopicPartitionSupervisorTest();

public:
    void setUp();
    void tearDown();

protected:
    protocol::ProductionRequest *prepareRecycleData(uint32_t size);

protected:
    config::BrokerConfig _config;

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, TopicPartitionSupervisorTest);

TopicPartitionSupervisorTest::TopicPartitionSupervisorTest() {}

TopicPartitionSupervisorTest::~TopicPartitionSupervisorTest() {}

void TopicPartitionSupervisorTest::setUp() { AUTIL_LOG(DEBUG, "setUp!"); }

void TopicPartitionSupervisorTest::tearDown() { AUTIL_LOG(DEBUG, "tearDown!"); }

TEST_F(TopicPartitionSupervisorTest, testGetBatchPartition) {
    config::BrokerConfig config;
    TopicPartitionSupervisor supervisor(&config, NULL, NULL);
    supervisor.start();
    supervisor._delExpiredFileThread.reset();
    int partCount = 101;
    vector<PartitionId> partIdVec;
    vector<TaskInfo> taskInfoVec;
    for (int i = 0; i < partCount; i++) {
        PartitionId partId = MessageCreator::createPartitionId("topic1", i);
        partIdVec.push_back(partId);
        TaskInfo taskInfo;
        taskInfo.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
        *taskInfo.mutable_partitionid() = partId;
        taskInfoVec.push_back(taskInfo);
    }
    supervisor._delCount = 0;
    supervisor.loadBrokerPartition(taskInfoVec);

    int64_t batchCnt = 10;
    for (int k = 0; k < 100; k++) {
        for (int i = 0; i < partCount / batchCnt; i++) {
            vector<BrokerPartitionPtr> todelPartVec = supervisor.getBatchPartition(batchCnt, supervisor._delCount);
            ASSERT_EQ(batchCnt, todelPartVec.size());
            for (int j = 0; j < batchCnt; j++) {
                ASSERT_TRUE(partIdVec[i * batchCnt + j] == todelPartVec[j]->getPartitionId()) << i << ", " << j;
            }
        }
        vector<BrokerPartitionPtr> todelPartVec = supervisor.getBatchPartition(batchCnt, supervisor._delCount);
        ASSERT_EQ(1, todelPartVec.size());
        ASSERT_TRUE(partIdVec[100] == todelPartVec[0]->getPartitionId());
    }
}

TEST_F(TopicPartitionSupervisorTest, testSimpleProcess) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    TopicPartitionSupervisor supervisor(&_config, NULL, NULL);
    PartitionId partId0 = MessageCreator::createPartitionId("topic1_0");
    PartitionId partId1 = MessageCreator::createPartitionId("topic1_1");
    PartitionId partId2 = MessageCreator::createPartitionId("topic1_2");

    TaskInfo taskInfo;
    taskInfo.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
    *taskInfo.mutable_partitionid() = partId0;
    ASSERT_EQ(ERROR_NONE, supervisor.loadBrokerPartition(taskInfo));

    vector<PartitionId> partitionIds;
    supervisor.getAllPartitions(partitionIds);
    ASSERT_EQ(size_t(1), partitionIds.size());
    ASSERT_TRUE(partId0 == partitionIds[0]);

    *taskInfo.mutable_partitionid() = partId1;
    ASSERT_EQ(ERROR_NONE, supervisor.loadBrokerPartition(taskInfo));

    supervisor.getAllPartitions(partitionIds);
    ASSERT_EQ(size_t(2), partitionIds.size());
    ASSERT_TRUE(partId0 == partitionIds[0]);
    ASSERT_TRUE(partId1 == partitionIds[1]);

    // load partition 1 again.
    ASSERT_EQ(ERROR_BROKER_REPEATED_ADD_TOPIC_PARTITION, supervisor.loadBrokerPartition(taskInfo));
    supervisor.getAllPartitions(partitionIds);
    ASSERT_EQ(size_t(2), partitionIds.size());
    ASSERT_TRUE(partId0 == partitionIds[0]);
    ASSERT_TRUE(partId1 == partitionIds[1]);

    ASSERT_EQ(ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND, supervisor.unLoadBrokerPartition(partId2));
    supervisor.getAllPartitions(partitionIds);
    ASSERT_EQ(size_t(2), partitionIds.size());
    ASSERT_TRUE(partId0 == partitionIds[0]);
    ASSERT_TRUE(partId1 == partitionIds[1]);

    HeartbeatInfo heartbeatInfo;
    supervisor.getPartitionStatus(heartbeatInfo);
    ASSERT_EQ(ROLE_TYPE_BROKER, heartbeatInfo.role());
    ASSERT_EQ(int(2), heartbeatInfo.tasks_size());

    TaskStatus taskStatus;
    *(taskInfo.mutable_partitionid()) = partId0;
    taskStatus.set_status(PARTITION_STATUS_RUNNING);
    taskStatus.set_errcode(ERROR_NONE);
    taskStatus.set_errtime(-1);
    taskStatus.set_errmsg("");
    taskStatus.mutable_taskinfo()->set_topicmode(TOPIC_MODE_MEMORY_ONLY);

    *(taskStatus.mutable_taskinfo()->mutable_partitionid()) = partId0;
    ASSERT_FALSE(taskStatus == heartbeatInfo.tasks(0));
    taskStatus.set_sessionid(supervisor._brokerPartitionMap[partId0].second->getSessionId());
    ASSERT_TRUE(taskStatus == heartbeatInfo.tasks(0));

    *(taskStatus.mutable_taskinfo()->mutable_partitionid()) = partId1;
    taskStatus.set_sessionid(supervisor._brokerPartitionMap[partId1].second->getSessionId());
    ASSERT_TRUE(taskStatus == heartbeatInfo.tasks(1));

    ASSERT_EQ(ERROR_NONE, supervisor.unLoadBrokerPartition(partId0));
    supervisor.getAllPartitions(partitionIds);
    ASSERT_EQ(size_t(1), partitionIds.size());
    ASSERT_TRUE(partId1 == partitionIds[0]);
}

TEST_F(TopicPartitionSupervisorTest, testRecycleData) {
    AUTIL_LOG(DEBUG, "Begin Test!");
    config::BrokerConfig config;
    config.setConfigUnlimited("true");
    config.fileBufferUseRatio = 0.5;
    config.setTotalBufferSize((double)MEMORY_MESSAGE_META_SIZE * 40000 / 1024 /
                              1024); // message total buffer size 40000 * 56 byte
    config.setBufferBlockSize((double)MEMORY_MESSAGE_META_SIZE / 1024 / 1024); // block size 56 byte
    config.setPartitionMaxBufferSize((double)MEMORY_MESSAGE_META_SIZE * 20000 / 1024 /
                                     1024); // max buffer size 20000 * 56 byte
    config.setPartitionMinBufferSize((double)MEMORY_MESSAGE_META_SIZE * 2000 / 1024 /
                                     1024); // min buffer size 2000 * 56 byte
    config.setBufferMinReserveSize((double)MEMORY_MESSAGE_META_SIZE * 4000 / 1024 / 1024);
    TopicPartitionSupervisor supervisor(&config, NULL, NULL);
    supervisor.start();
    supervisor._recycleBufferThread.reset();
    ASSERT_EQ(int64_t(0), supervisor._centerBlockPool->getUnusedBlockCount());
    ASSERT_EQ(int64_t(0), supervisor._centerBlockPool->getUsedBlockCount());
    PartitionId partId0 = MessageCreator::createPartitionId("topic1_0");
    PartitionId partId1 = MessageCreator::createPartitionId("topic1_1");
    PartitionId partId2 = MessageCreator::createPartitionId("topic1_2");
    TaskInfo taskInfo0;
    taskInfo0.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
    *taskInfo0.mutable_partitionid() = partId0;
    ASSERT_EQ(ERROR_NONE, supervisor.loadBrokerPartition(taskInfo0));
    TaskInfo taskInfo1;
    taskInfo1.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
    *taskInfo1.mutable_partitionid() = partId1;
    ASSERT_EQ(ERROR_NONE, supervisor.loadBrokerPartition(taskInfo1));
    vector<PartitionId> partitionIds;
    supervisor.getAllPartitions(partitionIds);
    ASSERT_EQ(size_t(2), partitionIds.size());
    BrokerPartitionPtr part0 = supervisor.getBrokerPartition(partId0);
    BrokerPartitionPtr part1 = supervisor.getBrokerPartition(partId1);
    ASSERT_TRUE(part0 != NULL);
    ASSERT_TRUE(part1 != NULL);
    ReaderDes reader;
    reader.setIpPort("10.101.193.199:32");
    reader.from = 0;
    reader.to = 65535;
    ReaderInfoPtr info(new ReaderInfo);
    info->nextTimestamp = 0;
    info->nextMsgId = 0;
    info->requestTime = autil::TimeUtility::currentTime();
    part0->_messageBrain->_readerInfoMap[reader] = info;
    part1->_messageBrain->_readerInfoMap[reader] = info;
    ProductionRequest *request0 = prepareRecycleData(10000); // need 20000 blocks, 1 msg need 2 blocks
    ProductionRequest *request1 = prepareRecycleData(10000);
    MessageResponse *response0 = new MessageResponse();
    MessageResponse *response1 = new MessageResponse();
    ASSERT_EQ(ERROR_NONE, part0->sendMessage(new ProductionLogClosure(request0, response0, NULL, "sendMessage")));
    ASSERT_EQ(ERROR_NONE, part1->sendMessage(new ProductionLogClosure(request1, response1, NULL, "sendMessage")));
    ASSERT_EQ(int64_t(0), supervisor._centerBlockPool->getUnusedBlockCount());
    ASSERT_EQ(int64_t(40000), supervisor._centerBlockPool->getUsedBlockCount());
    sleep(1); // commit back thread
    for (size_t i = 0; i < 10; i++) {
        supervisor.recycleBuffer(); // 100% reserve block in parition pool
    }
    ASSERT_EQ(int64_t(5552), supervisor._centerBlockPool->getUnusedBlockCount());
    sleep(10);
    delete request0;
    request0 = prepareRecycleData(500);
    ASSERT_EQ(ERROR_NONE, part0->sendMessage(new ProductionLogClosure(request0, response0, NULL, "sendMessage")));
    TaskInfo taskInfo2;
    taskInfo2.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
    *taskInfo2.mutable_partitionid() = partId2;
    ASSERT_EQ(ERROR_NONE, supervisor.loadBrokerPartition(taskInfo2));
    ProductionRequest *request2 = prepareRecycleData(10000);
    MessageResponse *response2 = new MessageResponse();
    BrokerPartitionPtr part2 = supervisor.getBrokerPartition(partId2);
    part2->_messageBrain->_readerInfoMap[reader] = info;
    ASSERT_EQ(ERROR_BROKER_BUSY,
              part2->sendMessage(new ProductionLogClosure(request2, response2, NULL, "sendMessage")));
    ASSERT_EQ(uint32_t(2776), response2->acceptedmsgcount());
    ASSERT_EQ(int64_t(0), supervisor._centerBlockPool->getUnusedBlockCount());
    sleep(1); // wait for commit message
    supervisor.recycleBuffer();
    ASSERT_EQ(int64_t(1720), supervisor._centerBlockPool->getUnusedBlockCount());
    delete request0;
    delete response0;
    delete request1;
    delete response1;
    delete request2;
    delete response2;
}

TEST_F(TopicPartitionSupervisorTest, testRecycleDataHasReader) {
    AUTIL_LOG(DEBUG, "Begin Test!");
    config::BrokerConfig config;
    config.setConfigUnlimited("true");
    config.fileBufferUseRatio = 0.5;
    config.setTotalBufferSize((double)MEMORY_MESSAGE_META_SIZE * 40000 / 1024 /
                              1024); // message total buffer size 40000 * 56 byte
    config.setBufferBlockSize((double)MEMORY_MESSAGE_META_SIZE / 1024 / 1024); // block size 56 byte
    config.setPartitionMaxBufferSize((double)MEMORY_MESSAGE_META_SIZE * 20000 / 1024 /
                                     1024); // max buffer size 20000 * 56 byte
    config.setPartitionMinBufferSize((double)MEMORY_MESSAGE_META_SIZE * 2000 / 1024 /
                                     1024); // min buffer size 2000 * 56 byte
    config.setBufferMinReserveSize((double)MEMORY_MESSAGE_META_SIZE * 30000 / 1024 / 1024);
    TopicPartitionSupervisor supervisor(&config, NULL, NULL);
    supervisor.start();
    supervisor._recycleBufferThread.reset();
    ASSERT_EQ(int64_t(0), supervisor._centerBlockPool->getUnusedBlockCount());
    ASSERT_EQ(int64_t(0), supervisor._centerBlockPool->getUsedBlockCount());
    PartitionId partId0 = MessageCreator::createPartitionId("topic1_0");
    PartitionId partId1 = MessageCreator::createPartitionId("topic1_1");
    TaskInfo taskInfo0;
    taskInfo0.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
    *taskInfo0.mutable_partitionid() = partId0;
    ASSERT_EQ(ERROR_NONE, supervisor.loadBrokerPartition(taskInfo0));
    TaskInfo taskInfo1;
    taskInfo1.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
    *taskInfo1.mutable_partitionid() = partId1;
    ASSERT_EQ(ERROR_NONE, supervisor.loadBrokerPartition(taskInfo1));
    vector<PartitionId> partitionIds;
    supervisor.getAllPartitions(partitionIds);
    ASSERT_EQ(size_t(2), partitionIds.size());

    // part0 no reader, part1 has reader, part2 has reader and then expire deleted
    BrokerPartitionPtr part0 = supervisor.getBrokerPartition(partId0);
    BrokerPartitionPtr part1 = supervisor.getBrokerPartition(partId1);
    ASSERT_TRUE(part0 != NULL);
    ASSERT_TRUE(part1 != NULL);
    ReaderDes reader;
    reader.setIpPort("10.101.193.199:32");
    reader.from = 0;
    reader.to = 65535;
    ReaderInfoPtr info(new ReaderInfo);
    info->nextTimestamp = 0;
    info->nextMsgId = 0;
    int64_t curTime = autil::TimeUtility::currentTime();
    info->requestTime = curTime;
    part1->_messageBrain->_readerInfoMap[reader] = info;
    ProductionRequest *request0 = prepareRecycleData(10000); // need 20000 blocks, 1 msg need 2 blocks
    ProductionRequest *request1 = prepareRecycleData(10000);
    MessageResponse *response0 = new MessageResponse();
    MessageResponse *response1 = new MessageResponse();
    ASSERT_EQ(ERROR_NONE, part0->sendMessage(new ProductionLogClosure(request0, response0, NULL, "sendMessage")));
    ASSERT_EQ(ERROR_NONE, part1->sendMessage(new ProductionLogClosure(request1, response1, NULL, "sendMessage")));
    ASSERT_EQ(int64_t(0), supervisor._centerBlockPool->getUnusedBlockCount());
    ASSERT_EQ(int64_t(40000), supervisor._centerBlockPool->getUsedBlockCount());
    sleep(1); // commit back thread

    supervisor.recycleBuffer(); // part0 recycle all, part1 recycle 10%
    ASSERT_EQ(18000, supervisor._centerBlockPool->getUnusedBlockCount());
    // let part1 reader expire
    const int64_t OBSOLETE_METRIC_INTERVAL = 600000000; // 10min
    info->requestTime = curTime - OBSOLETE_METRIC_INTERVAL;
    supervisor.recycleBuffer(); // part0 cannot recycle, part1 recycle 10% and will delete reader
    ASSERT_EQ(18000, supervisor._centerBlockPool->getUnusedBlockCount());
    part0->_messageBrain->recycleObsoleteReader();
    part1->_messageBrain->recycleObsoleteReader();

    supervisor.recycleBuffer(); // part0 cannot recycle, part1 recycle all
    ASSERT_EQ(36000, supervisor._centerBlockPool->getUnusedBlockCount());
    delete request0;
    delete response0;
    delete request1;
    delete response1;
}

ProductionRequest *TopicPartitionSupervisorTest::prepareRecycleData(uint32_t size) {
    ProductionRequest *request = new ProductionRequest;
    for (uint32_t i = 0; i < size; i++) {
        Message *msg = request->add_msgs();
        msg->set_data(string(MEMORY_MESSAGE_META_SIZE, i % 26 + 'a'));
    }
    return request;
}

TEST_F(TopicPartitionSupervisorTest, testLoadUnloadPartitionMutilThread) {
    AUTIL_LOG(DEBUG, "Begin Test!");
    config::BrokerConfig config;
    config.loadThreadNum = 10;
    config.unloadThreadNum = 10;
    TopicPartitionSupervisor supervisor(&config, NULL, NULL);
    supervisor.start();
    int partCount = 100;
    vector<PartitionId> partIdVec;
    vector<TaskInfo> taskInfoVec;
    for (int i = 0; i < partCount; i++) {
        PartitionId partId = MessageCreator::createPartitionId("topic1", i);
        partIdVec.push_back(partId);
        TaskInfo taskInfo;
        taskInfo.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
        *taskInfo.mutable_partitionid() = partId;
        taskInfoVec.push_back(taskInfo);
    }
    supervisor.loadBrokerPartition(taskInfoVec);
    vector<PartitionId> partitionIds;
    supervisor.getAllPartitions(partitionIds);
    ASSERT_EQ(size_t(partCount), partitionIds.size());
    for (int i = 0; i < partCount; i++) {
        ASSERT_TRUE(partIdVec[i] == partitionIds[i]);
    }
    supervisor.unLoadBrokerPartition(partIdVec);
    supervisor.getAllPartitions(partitionIds);
    ASSERT_EQ(size_t(0), partitionIds.size());
    // reload and unload
    supervisor.loadBrokerPartition(taskInfoVec);
    supervisor.getAllPartitions(partitionIds);
    ASSERT_EQ(size_t(partCount), partitionIds.size());
    for (int i = 0; i < partCount; i++) {
        ASSERT_TRUE(partIdVec[i] == partitionIds[i]);
    }
    supervisor.unLoadBrokerPartition(partIdVec);
    supervisor.getAllPartitions(partitionIds);
    ASSERT_EQ(size_t(0), partitionIds.size());
}

TEST_F(TopicPartitionSupervisorTest, testUnloadPartitionMutilThreadWhenStop) {
    AUTIL_LOG(DEBUG, "Begin Test!");
    config::BrokerConfig config;
    config.unloadThreadNum = 2;
    TopicPartitionSupervisor supervisor(&config, NULL, NULL);
    supervisor.start();
    int partCount = 100;
    vector<PartitionId> partIdVec;
    vector<TaskInfo> taskInfoVec;
    for (int i = 0; i < partCount; i++) {
        PartitionId partId = MessageCreator::createPartitionId("topic1", i);
        partIdVec.push_back(partId);
        TaskInfo taskInfo;
        taskInfo.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
        *taskInfo.mutable_partitionid() = partId;
        taskInfoVec.push_back(taskInfo);
    }
    supervisor.loadBrokerPartition(taskInfoVec);
    vector<PartitionId> partitionIds;
    supervisor.getAllPartitions(partitionIds);
    ASSERT_EQ(size_t(partCount), partitionIds.size());
    for (int i = 0; i < partCount; i++) {
        ASSERT_TRUE(partIdVec[i] == partitionIds[i]);
    }

    ThreadPtr thread = Thread::createThread(std::bind(&TopicPartitionSupervisor::stop, &supervisor));
    usleep(1000 * 200);
    supervisor.unLoadBrokerPartition(partIdVec);
    thread->join();
    supervisor.getAllPartitions(partitionIds);
    ASSERT_EQ(size_t(0), partitionIds.size());
}

TEST_F(TopicPartitionSupervisorTest, testGetRecycleFileThreshold) {
    TopicPartitionSupervisor supervisor(NULL, NULL, NULL);
    int64_t threshold = 0;

    vector<int64_t> fileBlocks1;
    for (int64_t index = 0; index < 10; ++index) {
        fileBlocks1.push_back(10 - index);
    }
    supervisor.getRecycleFileThreshold(threshold, fileBlocks1);
    ASSERT_EQ(9, threshold);

    vector<int64_t> fileBlocks2;
    fileBlocks2.push_back(1);
    supervisor.getRecycleFileThreshold(threshold, fileBlocks2);
    ASSERT_EQ(1, threshold);

    vector<int64_t> fileBlocks3;
    fileBlocks3.push_back(2);
    fileBlocks3.push_back(1);
    supervisor.getRecycleFileThreshold(threshold, fileBlocks3);
    ASSERT_EQ(2, threshold);

    vector<int64_t> fileBlocks4;
    supervisor.getRecycleFileThreshold(threshold, fileBlocks4);
    ASSERT_EQ(-1, threshold);
}

TEST_F(TopicPartitionSupervisorTest, testGetPartitionInMetrics) {
    config::BrokerConfig config;
    ZkConnectionStatus zkStatus;
    TopicPartitionSupervisor supervisor(&config, &zkStatus, NULL);
    supervisor.start();
    supervisor._delExpiredFileThread.reset();
    int partCount = 10;
    vector<PartitionId> partIdVec;
    vector<TaskInfo> taskInfoVec;
    for (int i = 0; i < partCount; i++) {
        PartitionId partId = MessageCreator::createPartitionId("topic1", i);
        partIdVec.push_back(partId);
        TaskInfo taskInfo;
        taskInfo.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
        *taskInfo.mutable_partitionid() = partId;
        taskInfoVec.push_back(taskInfo);
    }
    supervisor.loadBrokerPartition(taskInfoVec);

    uint32_t interval = 100;
    BrokerInMetric metric;
    int64_t rejectTime = 0;
    supervisor.getPartitionInMetrics(interval, metric.mutable_partinmetrics(), rejectTime);
    EXPECT_EQ(-1, rejectTime);
    EXPECT_EQ(partCount, metric.partinmetrics_size());

    int64_t currentTime = TimeUtility::currentTime();
    zkStatus.setHeartbeatStatus(false, currentTime - 400 * 1000);
    rejectTime = 0;
    supervisor.getPartitionInMetrics(interval, metric.mutable_partinmetrics(), rejectTime);
    EXPECT_EQ(-1, rejectTime);

    zkStatus._heartbeatLostTimestamp = currentTime - 500 * 1000;
    rejectTime = 0;
    supervisor.getPartitionInMetrics(interval, metric.mutable_partinmetrics(), rejectTime);
    EXPECT_THAT(rejectTime, ::testing::Ge(500000));
}

} // namespace service
} // namespace swift
