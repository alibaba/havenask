#include "swift/broker/storage/MessageBrain.h"

#include <algorithm>
#include <atomic>
#include <functional>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <map>
#include <memory>
#include <ostream>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Singleton.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "ext/alloc_traits.h"
#include "flatbuffers/flatbuffers.h"
#include "flatbuffers/stl_emulation.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "swift/broker/service/test/FakeClosure.h"
#include "swift/broker/storage/CommitManager.h"
#include "swift/broker/storage/InnerPartMetric.h"
#include "swift/broker/storage/MessageDeque.h"
#include "swift/broker/storage/MessageGroup.h"
#include "swift/broker/storage/RequestItemQueue.h"
#include "swift/broker/storage/TimestampAllocator.h"
#include "swift/broker/storage_dfs/FileManager.h"
#include "swift/broker/storage_dfs/FsMessageReader.h"
#include "swift/broker/storage_dfs/MessageCommitter.h"
#include "swift/common/Common.h"
#include "swift/common/FieldGroupReader.h"
#include "swift/common/FieldGroupWriter.h"
#include "swift/common/FilePair.h"
#include "swift/common/MemoryMessage.h"
#include "swift/common/ThreadBasedObjectPool.h"
#include "swift/config/PartitionConfig.h"
#include "swift/heartbeat/ThreadSafeTaskStatus.h"
#include "swift/log/BrokerLogClosure.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/MessageComparator.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/PermissionCenter.h"
#include "swift/util/ReaderRec.h"
#include "swift/util/TimeoutChecker.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace flatbuffers;

using namespace swift::protocol;
using namespace swift::monitor;
using namespace swift::service;
using namespace swift::common;
using namespace swift::util;
using namespace swift::heartbeat;

using ::testing::Ge;

namespace swift {
namespace storage {

class MessageBrainTest : public TESTBASE {
public:
    void setUp() {
        _permissionCenter = NULL;
        fslib::ErrorCode ec;
        ec = fslib::fs::FileSystem::remove(GET_TEMPLATE_DATA_PATH());
        ASSERT_TRUE(ec == fslib::EC_OK || ec == fslib::EC_NOENT);
        ec = fslib::fs::FileSystem::mkDir(GET_TEMPLATE_DATA_PATH(), true);
        ASSERT_TRUE(ec == fslib::EC_OK);
        _timeoutChecker = new TimeoutChecker();
        _timeoutChecker->setExpireTimeout(1000 * 1000 * 1000);
        _permissionCenter = new PermissionCenter();
        _centerPool = new BlockPool(64 * 1024 * 1024, 1024 * 1024);
        _fileCachePool = new BlockPool(_centerPool, 32 * 1024 * 1024, 1024 * 1024);

        _topicName = "topic1";
        _partitionId = 0;
        TaskInfo taskInfo;
        taskInfo.mutable_partitionid()->set_topicname(_topicName);
        taskInfo.mutable_partitionid()->set_id(_partitionId);
        taskInfo.mutable_partitionid()->set_from(0);
        taskInfo.mutable_partitionid()->set_to(128);
        taskInfo.set_topicmode(TOPIC_MODE_MEMORY_ONLY);

        _taskStatus.reset(new ThreadSafeTaskStatus(taskInfo));
    }
    void tearDown() {
        DELETE_AND_SET_NULL(_timeoutChecker);
        DELETE_AND_SET_NULL(_fileCachePool);
        DELETE_AND_SET_NULL(_centerPool);
        DELETE_AND_SET_NULL(_permissionCenter);
        _taskStatus.reset();
    }

private:
    struct ProductRequestStruct {
        protocol::ProductionRequest request;
        protocol::MessageResponse response;
        service::FakeClosure done;
    };
    void checkMsgForSecurityMode(std::vector<ProductRequestStruct *> &productStructVec, MessageBrain *msgBrain);
    void checkMsgForSecurityModeFile(std::vector<ProductRequestStruct *> &productStructVec, MessageBrain *msgBrain);
    void prepareMessages(const config::PartitionConfig &config,
                         util::BlockPool *centerPool,
                         monitor::BrokerMetricsReporter *amonReporter);
    void addMessageForDFS(config::PartitionConfig config);
    template <typename T>
    T *createFBMessage(uint32_t msgCount, bool hasError = false);

private:
    util::TimeoutChecker *_timeoutChecker;
    util::PermissionCenter *_permissionCenter;
    std::string _topicName;
    uint32_t _partitionId;
    ThreadSafeTaskStatusPtr _taskStatus;
    util::BlockPool *_centerPool;
    util::BlockPool *_fileCachePool;
    BrokerMetricsReporter _metricReporter;
};

template <typename T>
T *MessageBrainTest::createFBMessage(uint32_t msgCount, bool hasError) {
    ThreadBasedObjectPool<FBMessageWriter> *objectPool =
        Singleton<ThreadBasedObjectPool<FBMessageWriter>>::getInstance();
    FBMessageWriter *writer = objectPool->getObject();
    writer->reset();
    for (uint32_t i = 0; i < msgCount; ++i) {
        writer->addMessage(string(i + 2, 'c'), i, 0, 0, 0, hasError);
    }
    T *request = new T();
    request->set_fbmsgs(writer->data(), writer->size());
    request->set_messageformat(MF_FB);
    return request;
}

TEST_F(MessageBrainTest, testSecurityModeWithDataDirEmpty) {
    {
        MessageBrain msgBrain;
        config::PartitionConfig config;
        config.setTopicMode(TOPIC_MODE_SECURITY);
        ASSERT_EQ(ERROR_BROKER_DATA_DIR_EMPTY,
                  msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, NULL, _taskStatus));
    }
    {
        MessageBrain msgBrain;
        config::PartitionConfig config;
        config.setTopicMode(TOPIC_MODE_MEMORY_ONLY);
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
        EXPECT_TRUE(msgBrain._messageGroup->getIsMemOnly());
    }
}

TEST_F(MessageBrainTest, testAdd_GetMessage_PB) {
    BlockPool centerPool(1 << 29, 1 << 20);
    BlockPool fileCachePool(32 * 1024 * 1024, 1024 * 1024);
    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);
    {
        MessageBrain msgBrain;
        config::PartitionConfig config;
        config.setDataRoot(GET_TEMPLATE_DATA_PATH());
        config.setPartitionMinBufferSize(1 << 29); // 512M
        config.setPartitionMaxBufferSize(1 << 29); // 512M
        config.setBlockSize(1 << 20);              // 1M

        ASSERT_FALSE(msgBrain._enableFastRecover);
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, &centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
        EXPECT_TRUE(!msgBrain._messageGroup->getIsMemOnly());
        for (int k = 0; k < 2; ++k) {
            ProductionRequest pReq;
            MessageResponse response;
            for (uint64_t i = 0; i < 1024; ++i) {
                protocol::Message *msg = pReq.add_msgs();
                msg->set_data(std::string(1024 * 4, 'x'));
            }
            usleep(100 * 1000);
            FakeClosure done;
            ErrorCode errCode =
                msgBrain.addMessage(new ProductionLogClosure(&pReq, &response, &done, "sendMessage"), writeCollector);

            EXPECT_TRUE(done.getDoneStatus());
            ASSERT_EQ(errCode, ERROR_NONE);
            ASSERT_EQ(response.acceptedmsgcount(), (uint32_t)1024);
        }
    }
    // 1个block能放256条消息，总共2048条，需要8个block
    // block:msgid  0:0~255, 1:256~511, 2:512~767, 3:768~1023, 4:1024~1279, ...
    {
        MessageBrain msgBrain;
        config::PartitionConfig config;
        config.setDataRoot(GET_TEMPLATE_DATA_PATH());
        config.setPartitionMinBufferSize(1 << 29);
        config.setPartitionMaxBufferSize(1 << 29);
        config.setBlockSize(1 << 20);

        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, &centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));

        const auto &filePairVec = msgBrain._fsMessageReader->_fileManager->_filePairVec;
        ASSERT_EQ(1, filePairVec.size());
        ErrorCode errCode;
        string ipPort[2] = {"1.1.1.1:11", "1.1.1.2:22"};
        for (int i = 1000; i < 1100; i++) {
            ConsumptionRequest request;
            request.set_startid(i);
            request.set_count(10);
            MessageResponse response;
            errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, &ipPort[i % 2], collector);
            ASSERT_EQ(ERROR_NONE, errCode);
            ASSERT_EQ(10, response.msgs_size());
            ASSERT_EQ(int64_t(2047), response.maxmsgid());
            ReaderDes des = msgBrain.getReaderDescription(&request, ipPort[i % 2]);
            ReaderInfoPtr info = msgBrain._readerInfoMap[des];
            ASSERT_EQ(i + 10, info->nextMsgId);
            ASSERT_EQ(response.nexttimestamp(), info->nextTimestamp);
            ASSERT_TRUE(info->getReadFile());
            ASSERT_EQ(0, info->metaInfo->blockId);
            ASSERT_EQ(filePairVec[0]->metaFileName, info->metaInfo->fileName);
            ASSERT_EQ(filePairVec[0]->dataFileName, info->dataInfo->fileName);
            if (i <= 1014) {
                ASSERT_EQ(3, info->dataInfo->blockId);
            } else {
                ASSERT_EQ(4, info->dataInfo->blockId);
            }
        }
        ASSERT_EQ(2, msgBrain._readerInfoMap.size());
        ConsumptionRequest request;
        request.set_startid(1023);
        request.set_count(2048);
        MessageResponse response;
        errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, &ipPort[0], collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(1025, response.msgs_size());
        ReaderDes des1 = msgBrain.getReaderDescription(&request, ipPort[0]);
        ReaderInfoPtr info1 = msgBrain._readerInfoMap[des1];
        ASSERT_EQ(2048, info1->nextMsgId);
        ASSERT_EQ(response.nexttimestamp(), info1->nextTimestamp);
        ASSERT_TRUE(info1->getReadFile());
        ASSERT_EQ(0, info1->metaInfo->blockId);
        ASSERT_EQ(filePairVec[0]->metaFileName, info1->metaInfo->fileName);
        ASSERT_EQ(filePairVec[0]->dataFileName, info1->dataInfo->fileName);
        ASSERT_EQ(7, info1->dataInfo->blockId);

        ReaderDes des2 = msgBrain.getReaderDescription(&request, ipPort[1]);
        ReaderInfoPtr info2 = msgBrain._readerInfoMap[des2];
        request.set_count(-1);
        response.Clear();
        errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        //没传ip参数，不影响已有reader
        ASSERT_EQ(int64_t(2047), response.maxmsgid());
        ASSERT_EQ(2, msgBrain._readerInfoMap.size());
        ASSERT_EQ(2048, info1->nextMsgId);
        ASSERT_EQ(0, info1->metaInfo->blockId);
        ASSERT_EQ(7, info1->dataInfo->blockId);
        ASSERT_EQ(1109, info2->nextMsgId);
        ASSERT_EQ(0, info2->metaInfo->blockId);
        ASSERT_EQ(4, info2->dataInfo->blockId);

        request.set_count(0);
        response.Clear();
        errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(int64_t(2047), response.maxmsgid());

        MessageIdResponse idResponse;
        errCode = msgBrain.getMinMessageIdByTime(int64_t(0), &idResponse, _timeoutChecker);
        ASSERT_EQ(errCode, ERROR_NONE);
        ASSERT_EQ(int64_t(0), idResponse.msgid());
    }
}

TEST_F(MessageBrainTest, testAdd_GetMessage_FB) {
    BlockPool centerPool(1 << 29, 1 << 20);

    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);
    {
        MessageBrain msgBrain;
        config::PartitionConfig config;
        config.setDataRoot(GET_TEMPLATE_DATA_PATH());
        config.setPartitionMinBufferSize(1 << 29); // 512M
        config.setPartitionMaxBufferSize(1 << 29); // 512M
        config.setBlockSize(1 << 20);              // 1M

        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, &centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
        EXPECT_TRUE(!msgBrain._messageGroup->getIsMemOnly());
        for (int k = 0; k < 2; ++k) {
            MessageResponse response;
            shared_ptr<ProductionRequest> pReq(createFBMessage<ProductionRequest>(1024));
            usleep(100 * 1000);

            FakeClosure done;
            ErrorCode errCode = msgBrain.addMessage(
                new ProductionLogClosure(pReq.get(), &response, &done, "sendMessage"), writeCollector);
            EXPECT_TRUE(done.getDoneStatus());
            ASSERT_EQ(errCode, ERROR_NONE);
            ASSERT_EQ(response.acceptedmsgcount(), (uint32_t)1024);
        }
    }
    {
        MessageBrain msgBrain;
        config::PartitionConfig config;
        config.setDataRoot(GET_TEMPLATE_DATA_PATH());
        config.setPartitionMinBufferSize(1 << 29);
        config.setPartitionMaxBufferSize(1 << 29);
        config.setBlockSize(1 << 20);

        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, &centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
        const auto &filePairVec = msgBrain._fsMessageReader->_fileManager->_filePairVec;
        ASSERT_EQ(1, filePairVec.size());

        ErrorCode errCode;
        FBMessageReader reader;
        string ipPort[2] = {"1.1.1.1:11", "1.1.1.2:22"};
        for (int i = 1000; i < 1100; i++) {
            ConsumptionRequest request;
            request.mutable_versioninfo()->set_supportfb(true);
            request.set_startid(i);
            request.set_count(10);
            MessageResponse response;
            errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, &ipPort[i % 2], collector);
            ASSERT_TRUE(reader.init(response.fbmsgs()));
            ASSERT_EQ(10, reader.size());
            ASSERT_EQ(ERROR_NONE, errCode);
            ASSERT_EQ(int64_t(2047), response.maxmsgid());

            ReaderDes des = msgBrain.getReaderDescription(&request, ipPort[i % 2]);
            ReaderInfoPtr info = msgBrain._readerInfoMap[des];
            ASSERT_EQ(i + 10, info->nextMsgId);
            ASSERT_EQ(response.nexttimestamp(), info->nextTimestamp);
            ASSERT_TRUE(info->getReadFile());
            ASSERT_EQ(0, info->metaInfo->blockId);
            ASSERT_EQ(filePairVec[0]->metaFileName, info->metaInfo->fileName);
            ASSERT_EQ(filePairVec[0]->dataFileName, info->dataInfo->fileName);
            ASSERT_EQ(0, info->dataInfo->blockId);
        }
        ASSERT_EQ(2, msgBrain._readerInfoMap.size());
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_startid(1023);
        request.set_count(2048);
        MessageResponse response;
        errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, &ipPort[0], collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(0, response.msgs_size());
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(1025, reader.size());
        ReaderDes des1 = msgBrain.getReaderDescription(&request, ipPort[0]);
        ReaderInfoPtr info1 = msgBrain._readerInfoMap[des1];
        ASSERT_EQ(2048, info1->nextMsgId);
        ASSERT_EQ(response.nexttimestamp(), info1->nextTimestamp);
        ASSERT_TRUE(info1->getReadFile());
        ASSERT_EQ(0, info1->metaInfo->blockId);
        ASSERT_EQ(filePairVec[0]->metaFileName, info1->metaInfo->fileName);
        ASSERT_EQ(filePairVec[0]->dataFileName, info1->dataInfo->fileName);
        ASSERT_EQ(1, info1->dataInfo->blockId);

        ReaderDes des2 = msgBrain.getReaderDescription(&request, ipPort[1]);
        ReaderInfoPtr info2 = msgBrain._readerInfoMap[des2];
        request.set_count(-1);
        response.Clear();
        errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(int64_t(2047), response.maxmsgid());
        //没传ip参数，不影响已有reader
        ASSERT_EQ(int64_t(2047), response.maxmsgid());
        ASSERT_EQ(2, msgBrain._readerInfoMap.size());
        ASSERT_EQ(2048, info1->nextMsgId);
        ASSERT_EQ(0, info1->metaInfo->blockId);
        ASSERT_EQ(1, info1->dataInfo->blockId);
        ASSERT_EQ(1109, info2->nextMsgId);
        ASSERT_EQ(0, info2->metaInfo->blockId);
        ASSERT_EQ(0, info2->dataInfo->blockId);

        request.set_count(0);
        response.Clear();
        errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(int64_t(2047), response.maxmsgid());

        MessageIdResponse idResponse;
        errCode = msgBrain.getMinMessageIdByTime(int64_t(0), &idResponse, _timeoutChecker);
        ASSERT_EQ(errCode, ERROR_NONE);
        ASSERT_EQ(int64_t(0), idResponse.msgid());
    }
}

TEST_F(MessageBrainTest, testGetMessageNoData) {
    BlockPool centerPool(1 << 29, 1 << 20);

    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);
    MessageBrain msgBrain;
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setPartitionMinBufferSize(1 << 29); // 512M
    config.setPartitionMaxBufferSize(1 << 29); // 512M
    config.setBlockSize(1 << 20);              // 1M
    ASSERT_EQ(ERROR_NONE,
              msgBrain.init(config, &centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    {
        ConsumptionRequest request;
        request.set_startid(1023);
        int64_t curTime = TimeUtility::currentTime();
        request.set_starttimestamp(TimeUtility::currentTime());
        request.set_count(2048);
        MessageResponse response;
        ErrorCode errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_BROKER_NO_DATA, errCode);
        ASSERT_EQ(0, response.msgs_size());
        ASSERT_TRUE(response.has_nextmsgid());
        ASSERT_TRUE(response.has_nexttimestamp());
        ASSERT_EQ(0, response.nextmsgid());
        ASSERT_TRUE(response.nexttimestamp() >= curTime);
    }
    {
        ConsumptionRequest request;
        request.set_startid(0);
        int64_t curTime = TimeUtility::currentTime();
        request.set_starttimestamp(TimeUtility::currentTime());
        request.set_count(2048);
        MessageResponse response;
        ErrorCode errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_BROKER_NO_DATA, errCode);
        ASSERT_EQ(0, response.msgs_size());
        ASSERT_TRUE(response.has_nextmsgid());
        ASSERT_TRUE(response.has_nexttimestamp());
        ASSERT_EQ(0, response.nextmsgid());
        ASSERT_TRUE(response.nexttimestamp() >= curTime);
    }
}

TEST_F(MessageBrainTest, testAddMessageInSecurityMode) {
    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);
    MessageBrain *msgBrain = new MessageBrain;
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setPartitionMinBufferSize(1 << 29); // 512M
    config.setPartitionMaxBufferSize(1 << 29); // 512M
    config.setBlockSize(1 << 20);              // 1M
    config.setTopicMode(TOPIC_MODE_SECURITY);
    config.setMaxFileSize(1 << 30);
    config.setMaxCommitTimeAsError(1000 * 1000 * 1000);
    BlockPool *centerPool = new BlockPool(1 << 29, 1 << 20);
    ASSERT_EQ(ERROR_NONE,
              msgBrain->init(config, centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));

    vector<ProductRequestStruct *> productStructVec;
    for (int k = 0; k < 50; ++k) {
        ProductRequestStruct *pReqStruct = new ProductRequestStruct;
        for (uint64_t i = 0; i < 1024; ++i) {
            protocol::Message *msg = pReqStruct->request.add_msgs();
            msg->set_data(std::string((i + 1) * 4, 'x'));
        }

        productStructVec.push_back(pReqStruct);
    }

    ThreadPtr checkMsgThreadPtr =
        Thread::createThread(bind(&MessageBrainTest::checkMsgForSecurityMode, this, productStructVec, msgBrain));
    int interval = 0;
    for (vector<ProductRequestStruct *>::iterator it = productStructVec.begin(); it != productStructVec.end(); ++it) {
        if (++interval % 5 == 0) {
            usleep(300 * 1000);
        }

        ErrorCode errCode = msgBrain->addMessage(
            new ProductionLogClosure(&(*it)->request, &(*it)->response, &(*it)->done, "sendMessage"), writeCollector);
        ASSERT_EQ(errCode, ERROR_NONE);
    }
    checkMsgThreadPtr->join();
    checkMsgThreadPtr.reset();
    checkMsgForSecurityModeFile(productStructVec, msgBrain);
    delete msgBrain;
    delete centerPool;
    for (vector<ProductRequestStruct *>::iterator it = productStructVec.begin(); it != productStructVec.end(); ++it) {
        delete (*it);
    }
    productStructVec.clear();
}

void MessageBrainTest::checkMsgForSecurityMode(vector<ProductRequestStruct *> &productStructVec,
                                               MessageBrain *msgBrain) {

    size_t beginPos = 0;
    int64_t startId = 0;
    while (beginPos != productStructVec.size()) {
        if (productStructVec[beginPos]->done.getDoneStatus()) {
            int msgCount = productStructVec[beginPos]->request.msgs_size();
            ConsumptionRequest request;
            request.set_startid(startId);
            request.set_count(msgCount);
            MessageResponse response;
            ReadMetricsCollector collector(_topicName, _partitionId);
            ErrorCode errCode = msgBrain->getMessage(&request, &response, _timeoutChecker, NULL, collector);
            ASSERT_EQ(ERROR_NONE, errCode);
            ASSERT_EQ(msgCount, response.msgs_size());
            for (int i = 0; i < msgCount; ++i) {
                ASSERT_EQ(productStructVec[beginPos]->request.msgs(i).data(), response.msgs(i).data());
            }
            startId += msgCount;
            beginPos++;
        } else {
            usleep(500); // 0.5 ms
        }
    }
}

void MessageBrainTest::checkMsgForSecurityModeFile(vector<ProductRequestStruct *> &productStructVec,
                                                   MessageBrain *msgBrain) {
    ReaderDes des("10.1.1.1:100", 0, 20);
    ReaderInfoPtr info(new ReaderInfo());
    ReaderInfoMap readerInfoMap;
    readerInfoMap[des] = info;

    size_t beginPos = 0;
    int64_t startId = 0;
    while (beginPos != productStructVec.size()) {
        if (productStructVec[beginPos]->done.getDoneStatus()) {
            int msgCount = productStructVec[beginPos]->request.msgs_size();
            ConsumptionRequest request;
            request.set_startid(startId);
            request.set_count(msgCount);
            MessageResponse response;
            FsMessageReader *fsMessageReader = msgBrain->_fsMessageReader;
            ErrorCode errCode =
                fsMessageReader->getMessage(&request, &response, _timeoutChecker, info.get(), &readerInfoMap);
            ASSERT_EQ(ERROR_NONE, errCode);
            ASSERT_EQ(msgCount, response.msgs_size());
            for (int i = 0; i < msgCount; ++i) {
                ASSERT_EQ(productStructVec[beginPos]->request.msgs(i).data(), response.msgs(i).data());
            }
            startId += msgCount;
            beginPos++;
        } else {
            usleep(500); // 0.5 ms
        }
    }
}

TEST_F(MessageBrainTest, testCheckTimeOutWriteRequest) {
    vector<ProductRequestStruct *> productStructVec;
    vector<MessageBrain::WriteRequestItem *> requestItemVec;
    int32_t size = 3;
    int64_t currTime = autil::TimeUtility::currentTime();
    for (int32_t i = 0; i < size; ++i) {
        ProductRequestStruct *pReqStruct = new ProductRequestStruct;
        productStructVec.push_back(pReqStruct);

        MessageBrain::WriteRequestItem *requestItem = new MessageBrain::WriteRequestItem;
        requestItem->closure =
            new ProductionLogClosure(&pReqStruct->request, &pReqStruct->response, &pReqStruct->done, "sendMessage");
        requestItem->collector.reset(new WriteMetricsCollector(_topicName, _partitionId));
        if (i < 2) {
            requestItem->receivedTime = currTime - MessageBrain::DEFAULT_TIMETOUT_LIMIT - (2 - i);
        } else {
            requestItem->receivedTime = currTime;
        }
        requestItemVec.push_back(requestItem);
    }

    config::PartitionConfig config;
    config.setTopicMode(TOPIC_MODE_MEMORY_ONLY);
    MessageBrain msgBrain;
    ASSERT_EQ(ERROR_NONE,
              msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    msgBrain.checkTimeOutWriteRequest(requestItemVec);
    for (int32_t i = 0; i < size; ++i) {
        ProductRequestStruct *pReqStruct = productStructVec[i];
        MessageBrain::WriteRequestItem *requestItem = requestItemVec[i];
        if (i < 2) {
            ASSERT_EQ(ERROR_BROKER_BUSY, pReqStruct->response.errorinfo().errcode());
            EXPECT_TRUE(pReqStruct->done.getDoneStatus());
            EXPECT_TRUE(NULL == requestItem);
        } else {
            ASSERT_EQ(ERROR_NONE, pReqStruct->response.errorinfo().errcode());
            EXPECT_TRUE(!pReqStruct->done.getDoneStatus());
            EXPECT_TRUE(NULL != requestItem);
            DELETE_AND_SET_NULL(requestItem->closure);
            DELETE_AND_SET_NULL(requestItem);
        }
    }

    for (int32_t i = 0; i < size; ++i) {
        delete productStructVec[i];
    }
}

TEST_F(MessageBrainTest, testAddWriteRequestToMemory) {
    MessageBrain msgBrain;
    ReadMetricsCollector collector(_topicName, _partitionId);
    config::PartitionConfig config;
    config.setConfigUnlimited(true);
    int64_t bufferSize = MEMORY_MESSAGE_META_SIZE * 8;
    config.setPartitionMinBufferSize(bufferSize);
    config.setPartitionMaxBufferSize(bufferSize);
    config.setBlockSize(bufferSize / 2);
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    ASSERT_EQ(ERROR_NONE, msgBrain.init(config, NULL, NULL, _permissionCenter, &_metricReporter, _taskStatus));
    vector<ProductRequestStruct *> productStructVec;
    vector<MessageBrain::WriteRequestItem *> requestItemVec;
    requestItemVec.push_back(NULL);
    int32_t size = 3;
    for (int32_t i = 0; i < size; ++i) {
        ProductRequestStruct *pReqStruct = new ProductRequestStruct;
        for (int32_t j = 0; j < 3; ++j) {
            protocol::Message *msg = pReqStruct->request.add_msgs();
            msg->set_data(std::string(5, 'x'));
        }
        productStructVec.push_back(pReqStruct);
        MessageBrain::WriteRequestItem *requestItem = new MessageBrain::WriteRequestItem;
        requestItem->closure =
            new ProductionLogClosure(&pReqStruct->request, &pReqStruct->response, &pReqStruct->done, "sendMessage");
        requestItem->collector.reset(new WriteMetricsCollector(_topicName, _partitionId));
        requestItemVec.push_back(requestItem);
    }
    msgBrain.addWriteRequestToMemory(requestItemVec);

    ASSERT_EQ(ERROR_NONE, productStructVec[0]->response.errorinfo().errcode());
    ASSERT_EQ((uint32_t)3, productStructVec[0]->response.acceptedmsgcount());
    EXPECT_TRUE(!productStructVec[0]->done.getDoneStatus());
    EXPECT_TRUE(NULL != requestItemVec[1]);
    DELETE_AND_SET_NULL(requestItemVec[1]->closure);
    DELETE_AND_SET_NULL(requestItemVec[1]);

    ASSERT_EQ(ERROR_BROKER_BUSY, productStructVec[1]->response.errorinfo().errcode());
    ASSERT_EQ((uint32_t)1, productStructVec[1]->response.acceptedmsgcount());
    EXPECT_TRUE(!productStructVec[1]->done.getDoneStatus());
    EXPECT_TRUE(NULL != requestItemVec[2]);
    DELETE_AND_SET_NULL(requestItemVec[2]->closure);
    DELETE_AND_SET_NULL(requestItemVec[2]);

    ASSERT_EQ(ERROR_BROKER_BUSY, productStructVec[2]->response.errorinfo().errcode());
    ASSERT_EQ((uint32_t)0, productStructVec[2]->response.acceptedmsgcount());
    EXPECT_TRUE(productStructVec[2]->done.getDoneStatus());
    EXPECT_TRUE(NULL == requestItemVec[3]);

    for (int32_t i = 0; i < size; ++i) {
        delete productStructVec[i];
    }
}

TEST_F(MessageBrainTest, testAddMessageWithError) {
    MessageBrain msgBrain;

    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);

    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setTopicMode(TOPIC_MODE_SECURITY);
    ASSERT_EQ(ERROR_NONE,
              msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));

    // checkRequest error
    vector<ProductRequestStruct *> productStructVec;
    ProductRequestStruct *pReqStruct = new ProductRequestStruct;
    protocol::Message *msg = pReqStruct->request.add_msgs();
    msg->set_data(std::string(common::MAX_MESSAGE_SIZE + 1, 'x'));
    productStructVec.push_back(pReqStruct);

    ErrorCode ec = msgBrain.addMessage(
        new ProductionLogClosure(
            &productStructVec[0]->request, &productStructVec[0]->response, &productStructVec[0]->done, "sendMessage"),
        writeCollector);

    ASSERT_EQ(ERROR_BROKER_MSG_LENGTH_EXCEEDED, ec);
    ASSERT_EQ(ERROR_BROKER_MSG_LENGTH_EXCEEDED, productStructVec[0]->response.errorinfo().errcode());
    EXPECT_TRUE(productStructVec[0]->done.getDoneStatus());

    delete productStructVec[0];
    productStructVec.clear();

    // out of limit of writeRequestItemQueue
    msgBrain._writeRequestItemQueue.setRequestItemLimit(0);
    pReqStruct = new ProductRequestStruct;
    msg = pReqStruct->request.add_msgs();
    msg->set_data(std::string(5, 'x'));
    productStructVec.push_back(pReqStruct);

    ec = msgBrain.addMessage(
        new ProductionLogClosure(
            &productStructVec[0]->request, &productStructVec[0]->response, &productStructVec[0]->done, "sendMessage"),
        writeCollector);

    ASSERT_EQ(ERROR_BROKER_BUSY, ec);
    ASSERT_EQ(ERROR_BROKER_BUSY, productStructVec[0]->response.errorinfo().errcode());
    EXPECT_TRUE(productStructVec[0]->done.getDoneStatus());

    delete productStructVec[0];
    productStructVec.clear();

    // checkRequest error:ERROR_BROKER_MESSAGE_FORMAT_INVALID
    msgBrain._needFieldFilter = true;
    pReqStruct = new ProductRequestStruct;
    msg = pReqStruct->request.add_msgs();
    msg->set_data(std::string("abcdef"));
    productStructVec.push_back(pReqStruct);

    ec = msgBrain.addMessage(
        new ProductionLogClosure(
            &productStructVec[0]->request, &productStructVec[0]->response, &productStructVec[0]->done, "sendMessage"),
        writeCollector);
    ASSERT_EQ(ERROR_BROKER_MESSAGE_FORMAT_INVALID, ec);

    delete productStructVec[0];
    productStructVec.clear();

    // checkRequest no error:ERROR_BROKER_MESSAGE_FORMAT_INVALID for close check filedfilter msg
    msgBrain._writeRequestItemQueue.setRequestItemLimit(1);
    msgBrain._needFieldFilter = true;
    msgBrain._checkFieldFilterMsg = false;
    pReqStruct = new ProductRequestStruct;
    msg = pReqStruct->request.add_msgs();
    msg->set_data(std::string("abcdef"));
    productStructVec.push_back(pReqStruct);

    ec = msgBrain.addMessage(
        new ProductionLogClosure(
            &productStructVec[0]->request, &productStructVec[0]->response, &productStructVec[0]->done, "sendMessage"),
        writeCollector);

    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(1, msgBrain._writeRequestItemQueue.getQueueSize());
    vector<MessageBrain::WriteRequestItem *> requestItemVec;
    msgBrain._writeRequestItemQueue.popRequestItem(requestItemVec, 1);
    ASSERT_EQ(1, requestItemVec.size());
    requestItemVec[0]->closure->Run();
    DELETE_AND_SET_NULL(requestItemVec[0]);
    delete productStructVec[0];
    productStructVec.clear();
}

TEST_F(MessageBrainTest, testCheckProductionRequest) {
    MessageBrain msgBrain;
    WriteMetricsCollector writeCollector(_topicName, _partitionId);
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setTopicMode(TOPIC_MODE_MEMORY_ONLY);
    ASSERT_EQ(ERROR_NONE,
              msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    {
        ProductionRequest request;
        protocol::Message *msg = request.add_msgs();
        msg->set_data(std::string(common::MAX_MESSAGE_SIZE + 1, 'x'));
        int64_t totalSize = 0;
        int64_t msgCount = 0;
        int64_t msgCountWithMerged = 0;
        ErrorCode ec =
            msgBrain.checkProductionRequest(&request, totalSize, msgCount, msgCountWithMerged, writeCollector);
        ASSERT_EQ(ERROR_BROKER_MSG_LENGTH_EXCEEDED, ec);
    }
    {
        ProductionRequest request;
        request.set_messageformat(MF_FB);
        FBMessageWriter writer;
        writer.addMessage(std::string(common::MAX_MESSAGE_SIZE + 1, 'x'));
        writer.finish();
        request.set_fbmsgs(writer.data(), writer.size());
        int64_t totalSize = 0;
        int64_t msgCount = 0;
        int64_t msgCountWithMerged = 0;
        ErrorCode ec =
            msgBrain.checkProductionRequest(&request, totalSize, msgCount, msgCountWithMerged, writeCollector);
        ASSERT_EQ(ERROR_BROKER_MSG_LENGTH_EXCEEDED, ec);
    }
    {
        msgBrain._needFieldFilter = true;
        ProductionRequest request;
        protocol::Message *msg = request.add_msgs();
        msg->set_data(std::string("xxxx"));
        int64_t totalSize = 0;
        int64_t msgCount = 0;
        int64_t msgCountWithMerged = 0;
        ErrorCode ec =
            msgBrain.checkProductionRequest(&request, totalSize, msgCount, msgCountWithMerged, writeCollector);
        ASSERT_EQ(ERROR_BROKER_MESSAGE_FORMAT_INVALID, ec);
    }
    {
        msgBrain._needFieldFilter = true;
        ProductionRequest request;
        request.set_messageformat(MF_FB);
        FBMessageWriter writer;
        writer.addMessage(std::string("ssss"));
        writer.finish();
        request.set_fbmsgs(writer.data(), writer.size());
        int64_t totalSize = 0;
        int64_t msgCount = 0;
        int64_t msgCountWithMerged = 0;
        ErrorCode ec =
            msgBrain.checkProductionRequest(&request, totalSize, msgCount, msgCountWithMerged, writeCollector);
        ASSERT_EQ(ERROR_BROKER_MESSAGE_FORMAT_INVALID, ec);
    }
    {
        msgBrain._needFieldFilter = true;
        ProductionRequest request;
        protocol::Message *msg = request.add_msgs();
        msg->set_merged(true);
        msg->set_data(std::string("xxxx"));
        int64_t totalSize = 0;
        int64_t msgCount = 0;
        int64_t msgCountWithMerged = 0;
        ErrorCode ec =
            msgBrain.checkProductionRequest(&request, totalSize, msgCount, msgCountWithMerged, writeCollector);
        ASSERT_EQ(ERROR_NONE, ec);
    }
    {
        msgBrain._needFieldFilter = true;
        ProductionRequest request;
        request.set_messageformat(MF_FB);
        FBMessageWriter writer;
        writer.addMessage(std::string("ssss"), 0, 0, 0, 0, false, true);
        writer.finish();
        request.set_fbmsgs(writer.data(), writer.size());
        int64_t totalSize = 0;
        int64_t msgCount = 0;
        int64_t msgCountWithMerged = 0;
        ErrorCode ec =
            msgBrain.checkProductionRequest(&request, totalSize, msgCount, msgCountWithMerged, writeCollector);
        ASSERT_EQ(ERROR_NONE, ec);
    }

    {
        msgBrain._needFieldFilter = true;
        msgBrain._checkFieldFilterMsg = false;
        ProductionRequest request;
        protocol::Message *msg = request.add_msgs();
        msg->set_data(std::string("ssss"));
        int64_t totalSize = 0;
        int64_t msgCount = 0;
        int64_t msgCountWithMerged = 0;
        ErrorCode ec =
            msgBrain.checkProductionRequest(&request, totalSize, msgCount, msgCountWithMerged, writeCollector);
        ASSERT_EQ(ERROR_NONE, ec);
    }
    {
        msgBrain._needFieldFilter = true;
        msgBrain._checkFieldFilterMsg = false;
        ProductionRequest request;
        request.set_messageformat(MF_FB);
        FBMessageWriter writer;
        writer.addMessage(std::string("xxxx"));
        writer.finish();
        request.set_fbmsgs(writer.data(), writer.size());
        int64_t totalSize = 0;
        int64_t msgCount = 0;
        int64_t msgCountWithMerged = 0;
        ErrorCode ec =
            msgBrain.checkProductionRequest(&request, totalSize, msgCount, msgCountWithMerged, writeCollector);
        ASSERT_EQ(ERROR_NONE, ec);
    }
}

TEST_F(MessageBrainTest, testRecoverWithReadLastMessagesFailed) {
    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);
    {
        MessageBrain msgBrain;
        config::PartitionConfig config;
        config.setDataRoot(GET_TEMPLATE_DATA_PATH());
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
        for (int k = 0; k < 2; ++k) {
            ProductionRequest pReq;
            MessageResponse response;
            for (uint64_t i = 0; i < 1024; ++i) {
                protocol::Message *msg = pReq.add_msgs();
                msg->set_data("xxx");
            }
            usleep(100 * 1000);
            FakeClosure done;
            ErrorCode errCode =
                msgBrain.addMessage(new ProductionLogClosure(&pReq, &response, &done, "sendMessage"), writeCollector);

            EXPECT_TRUE(done.getDoneStatus());
            ASSERT_EQ(errCode, ERROR_NONE);
            ASSERT_EQ(response.acceptedmsgcount(), (uint32_t)1024);
        }
    }
    {
        int64_t curTime = TimeUtility::currentTime();
        fslib::FileList fileList;
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::listDir(GET_TEMPLATE_DATA_PATH(), fileList));
        ASSERT_EQ(size_t(2), fileList.size());
        string dataName;
        for (size_t i = 0; i < 2; ++i) {
            if (autil::StringUtil::endsWith(fileList[i], ".data")) {
                dataName = fileList[i];
                break;
            }
        }
        EXPECT_TRUE(!dataName.empty());
        string cmd = "echo xx > " + GET_TEMPLATE_DATA_PATH() + "/" + dataName;
        (void)system(cmd.c_str());
        MessageBrain msgBrain;
        config::PartitionConfig config;
        config.setDataRoot(GET_TEMPLATE_DATA_PATH());

        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));

        ConsumptionRequest request;
        request.set_startid(2048);
        request.set_count(1);
        MessageResponse response;
        ErrorCode errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(1, response.msgs_size());
        ASSERT_EQ(int64_t(2048), response.maxmsgid());
        Message msg = response.msgs(0);
        ASSERT_EQ(int64_t(2048), msg.msgid());
        EXPECT_TRUE(curTime <= msg.timestamp());
        ASSERT_EQ(string(""), msg.data());
    }
}

TEST_F(MessageBrainTest, testGetMessageWithRange) {
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setPartitionMinBufferSize(1 << 21); // 2M
    config.setPartitionMaxBufferSize(1 << 21); // 2M
    config.setBlockSize(4 * 1024);             // 4K
    config.setMaxCommitTimeAsError(100 * 1000 * 1000);
    BlockPool centerPool(1 << 21, 4 * 1024);

    ReadMetricsCollector collector(_topicName, _partitionId);
    prepareMessages(config, &centerPool, &_metricReporter);

    {
        BlockPool fileCachePool(&centerPool, 1 << 20, 1 << 18);
        MessageBrain msgBrain;
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, &centerPool, &fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(1000);

        MessageResponse response;
        ErrorCode errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(256, response.msgs_size()); // block size 4096/16 = 256
    }

    {
        BlockPool fileCachePool(&centerPool, 1 << 20, 1 << 18);
        MessageBrain msgBrain;
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, &centerPool, &fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(20);
        request.mutable_filter()->set_from(0);
        request.mutable_filter()->set_to(1);

        MessageResponse response;
        ErrorCode errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(2, response.msgs_size());
    }

    {
        BlockPool fileCachePool(&centerPool, 1 << 20, 1 << 18);
        MessageBrain msgBrain;
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, &centerPool, &fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(20);
        request.mutable_filter()->set_from(0);
        request.mutable_filter()->set_to(50);
        request.mutable_filter()->set_uint8filtermask(0xff);
        request.mutable_filter()->set_uint8maskresult(0x13);

        MessageResponse response;
        ErrorCode errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(1, response.msgs_size());
    }
}

TEST_F(MessageBrainTest, testRecycleBuffer) {
    config::PartitionConfig config;
    config.setDataRoot("");
    config.setPartitionMinBufferSize(sizeof(MemoryMessage) << 20); // 56M ->0.5M message
    config.setPartitionMaxBufferSize(sizeof(MemoryMessage) << 20); // 4M
    config.setBlockSize(sizeof(MemoryMessage) << 10);              // 56K ->1024 message or meta
    config.setTopicMode(TOPIC_MODE_MEMORY_ONLY);

    BlockPool *centerPool = new BlockPool(sizeof(MemoryMessage) << 21, sizeof(MemoryMessage) << 10);
    BlockPool *fileCachePool = new BlockPool(centerPool, sizeof(MemoryMessage) << 20, sizeof(MemoryMessage) << 18);

    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);
    MessageBrain *msgBrain = new MessageBrain;
    ASSERT_EQ(ERROR_NONE,
              msgBrain->init(config, centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    string data(sizeof(MemoryMessage), 'a');
    ProductionRequest request; // 512 message each request, need one block
    for (int j = 0; j < 512; ++j) {
        Message *msg = request.add_msgs();
        msg->set_data(data);
    }
    MessageResponse response;
    for (int i = 0; i < 1024; ++i) {
        FakeClosure done;
        ErrorCode errCode =
            msgBrain->addMessage(new ProductionLogClosure(&request, &response, &done, "sendMessage"), writeCollector);
        EXPECT_TRUE(done.getDoneStatus());
        ASSERT_EQ(ERROR_NONE, errCode);
        if (i % 2 == 0) {
            ASSERT_EQ(int64_t(i + 2), centerPool->getUsedBlockCount());
        } else {
            ASSERT_EQ(int64_t(i + 1), centerPool->getUsedBlockCount());
        }
    }
    FakeClosure done;
    ErrorCode errCode =
        msgBrain->addMessage(new ProductionLogClosure(&request, &response, &done, "sendMessage"), writeCollector);
    EXPECT_TRUE(done.getDoneStatus());
    ASSERT_EQ(ERROR_BROKER_BUSY, errCode);
    ASSERT_EQ((uint32_t)0, response.acceptedmsgcount());
    msgBrain->commitMessage();
    for (int i = 0; i < 1024 * 10; ++i) {
        FakeClosure done;
        ErrorCode errCode =
            msgBrain->addMessage(new ProductionLogClosure(&request, &response, &done, "sendMessage"), writeCollector);
        EXPECT_TRUE(done.getDoneStatus());
        ASSERT_EQ(ERROR_NONE, errCode);
        if (i % 512 == 0) {
            msgBrain->commitMessage();
        }
    }
    delete msgBrain;
    delete fileCachePool;
    delete centerPool;
}

TEST_F(MessageBrainTest, testCommitMessage) {
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setPartitionMinBufferSize(1 << 21); // 2M
    config.setPartitionMaxBufferSize(1 << 21); // 2M
    config.setBlockSize(4 * 1024);             // 4K
    config.setMaxCommitTimeAsError(100 * 1000 * 1000);
    config.setMaxCommitInterval(200 * 1000);
    MessageBrain msgBrain;
    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ASSERT_EQ(ERROR_NONE, msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, NULL, _taskStatus));
    ProductionRequest productionRequest;
    for (int i = 0; i < 2048; ++i) {
        Message *msg = productionRequest.add_msgs();
        FieldGroupWriter fieldGroupWriter;
        fieldGroupWriter.addProductionField("CMD", "add", i % 2);
        fieldGroupWriter.addProductionField("title", "title" + autil::StringUtil::toString(i), (i + 1) % 2);
        fieldGroupWriter.addProductionField("body", "body" + autil::StringUtil::toString(i), i % 2);
        string data = fieldGroupWriter.toString();
        msg->set_data(data);
    }
    MessageResponse response;
    FakeClosure done;
    EXPECT_FALSE(msgBrain.needCommitMessage());
    ErrorCode errCode = msgBrain.addMessage(
        new ProductionLogClosure(&productionRequest, &response, &done, "sendMessage"), writeCollector);

    EXPECT_TRUE(done.getDoneStatus());
    ASSERT_EQ(ERROR_NONE, errCode);
    EXPECT_TRUE(msgBrain.needCommitMessage());
    EXPECT_TRUE(ERROR_NONE == msgBrain.commitMessage());
    EXPECT_TRUE(msgBrain.needCommitMessage());
    usleep(210000);
    EXPECT_TRUE(ERROR_NONE == msgBrain.commitMessage());
    EXPECT_FALSE(msgBrain.needCommitMessage());
}

TEST_F(MessageBrainTest, testGetFieldFilterMessage) {
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setNeedFieldFilter(true);
    addMessageForDFS(config);

    MessageBrain msgBrain;
    ReadMetricsCollector collector(_topicName, _partitionId);
    MessageResponse response;
    ASSERT_EQ(ERROR_NONE,
              msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    {
        ConsumptionRequest consumptionRequest;
        consumptionRequest.set_startid(0);
        consumptionRequest.set_count(10);
        consumptionRequest.set_maxtotalsize(120);
        string *fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "CMD";
        fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "title";

        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(3, response.msgs_size()); // raw message size exceed max total size
        ASSERT_EQ((int64_t)3, response.nextmsgid());
        ASSERT_EQ((int64_t)2047, response.maxmsgid());
        FieldGroupReader fieldGroupReader;
        for (int i = 0; i < 3; ++i) {
            fieldGroupReader.fromConsumptionString(response.msgs(i).data());
            ASSERT_EQ((size_t)2, fieldGroupReader.getFieldSize());
            const Field *field = fieldGroupReader.getField(0);
            EXPECT_TRUE(field->name.empty());
            string data;
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("add"), data);
            EXPECT_TRUE(i % 2 ? field->isUpdated : !field->isUpdated);
            field = fieldGroupReader.getField(1);
            EXPECT_TRUE(field->name.empty());
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("title") + autil::StringUtil::toString(i), data);
            EXPECT_TRUE(i % 2 ? !field->isUpdated : field->isUpdated);
        }
    }
    // doGetMessage twice from DFS
    {
        ConsumptionRequest consumptionRequest;
        consumptionRequest.set_startid(0);
        consumptionRequest.set_count(10);
        string *fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "CMD";
        fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "body";

        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(5, response.msgs_size()); // anothor 5 msg is filtered
        ASSERT_EQ((int64_t)10, response.nextmsgid());
        ASSERT_EQ((int64_t)2047, response.maxmsgid());
        FieldGroupReader fieldGroupReader;
        for (int i = 0; i < 5; ++i) {
            fieldGroupReader.fromConsumptionString(response.msgs(i).data());
            ASSERT_EQ((size_t)2, fieldGroupReader.getFieldSize());
            const Field *field = fieldGroupReader.getField(0);
            EXPECT_TRUE(field->name.empty());
            string data;
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("add"), data);
            EXPECT_TRUE(field->isUpdated);
            field = fieldGroupReader.getField(1);
            EXPECT_TRUE(field->name.empty());
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("body") + autil::StringUtil::toString(2 * i + 1), data);
            EXPECT_TRUE(field->isUpdated);
        }
    }

    // doGetMessage twice, one from dfs, another from group
    {
        ConsumptionRequest consumptionRequest;
        consumptionRequest.set_startid(1014);
        consumptionRequest.set_count(20);
        string *fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "CMD";
        fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "body";

        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(10, response.msgs_size());
        ASSERT_EQ((int64_t)1034, response.nextmsgid());
        ASSERT_EQ((int64_t)2047, response.maxmsgid());
        FieldGroupReader fieldGroupReader;
        for (int i = 0; i < 10; ++i) {
            fieldGroupReader.fromConsumptionString(response.msgs(i).data());
            ASSERT_EQ((size_t)2, fieldGroupReader.getFieldSize());
            const Field *field = fieldGroupReader.getField(0);
            EXPECT_TRUE(field->name.empty());
            string data;
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("add"), data);
            EXPECT_TRUE(field->isUpdated);
            field = fieldGroupReader.getField(1);
            EXPECT_TRUE(field->name.empty());
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("body") + autil::StringUtil::toString(1014 + 2 * i + 1), data);
            EXPECT_TRUE(field->isUpdated);
        }
    }

    // doGetMessage twice, one from dfs, another from group
    {
        ConsumptionRequest consumptionRequest;
        consumptionRequest.set_startid(2037);
        consumptionRequest.set_count(20);
        string *fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "CMD";
        fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "body";

        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(6, response.msgs_size());
        ASSERT_EQ((int64_t)2048, response.nextmsgid());
        ASSERT_EQ((int64_t)2047, response.maxmsgid());
        FieldGroupReader fieldGroupReader;
        for (int i = 0; i < 6; ++i) {
            fieldGroupReader.fromConsumptionString(response.msgs(i).data());
            ASSERT_EQ((size_t)2, fieldGroupReader.getFieldSize());
            const Field *field = fieldGroupReader.getField(0);
            EXPECT_TRUE(field->name.empty());
            string data;
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("add"), data);
            EXPECT_TRUE(field->isUpdated);
            field = fieldGroupReader.getField(1);
            EXPECT_TRUE(field->name.empty());
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("body") + autil::StringUtil::toString(2037 + 2 * i), data);
            EXPECT_TRUE(field->isUpdated);
        }
    }
}

TEST_F(MessageBrainTest, testGetFieldFilterMessage_FB) {
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setNeedFieldFilter(true);
    addMessageForDFS(config);

    MessageBrain msgBrain;
    ReadMetricsCollector collector(_topicName, _partitionId);
    MessageResponse response;
    ASSERT_EQ(ERROR_NONE,
              msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    {
        ConsumptionRequest consumptionRequest;
        consumptionRequest.set_startid(0);
        consumptionRequest.set_count(10);
        consumptionRequest.set_maxtotalsize(120);
        consumptionRequest.mutable_versioninfo()->set_supportfb(true);
        string *fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "CMD";
        fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "title";

        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(3, reader.size());
        ASSERT_EQ((int64_t)3, response.nextmsgid());
        ASSERT_EQ((int64_t)2047, response.maxmsgid());
        FieldGroupReader fieldGroupReader;
        for (int i = 0; i < 3; ++i) {
            const flat::Message *msg = reader.read(i);
            fieldGroupReader.fromConsumptionString(msg->data()->c_str(), msg->data()->size());
            ASSERT_EQ((size_t)2, fieldGroupReader.getFieldSize());
            const Field *field = fieldGroupReader.getField(0);
            EXPECT_TRUE(field->name.empty());
            string data;
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("add"), data);
            EXPECT_TRUE(i % 2 ? field->isUpdated : !field->isUpdated);
            field = fieldGroupReader.getField(1);
            EXPECT_TRUE(field->name.empty());
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("title") + autil::StringUtil::toString(i), data);
            EXPECT_TRUE(i % 2 ? !field->isUpdated : field->isUpdated);
        }
    }
    // doGetMessage twice from DFS
    {
        ConsumptionRequest consumptionRequest;
        consumptionRequest.mutable_versioninfo()->set_supportfb(true);
        consumptionRequest.set_startid(0);
        consumptionRequest.set_count(10);
        string *fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "CMD";
        fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "body";

        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(5, reader.size()); // anothor 5 msg is filtered
        ASSERT_EQ((int64_t)10, response.nextmsgid());
        ASSERT_EQ((int64_t)2047, response.maxmsgid());
        FieldGroupReader fieldGroupReader;
        for (int i = 0; i < 5; ++i) {
            const flat::Message *msg = reader.read(i);
            fieldGroupReader.fromConsumptionString(msg->data()->c_str(), msg->data()->size());
            ASSERT_EQ((size_t)2, fieldGroupReader.getFieldSize());
            const Field *field = fieldGroupReader.getField(0);
            EXPECT_TRUE(field->name.empty());
            string data;
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("add"), data);
            EXPECT_TRUE(field->isUpdated);
            field = fieldGroupReader.getField(1);
            EXPECT_TRUE(field->name.empty());
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("body") + autil::StringUtil::toString(2 * i + 1), data);
            EXPECT_TRUE(field->isUpdated);
        }
    }

    // doGetMessage twice, one from dfs, another from group
    {
        ConsumptionRequest consumptionRequest;
        consumptionRequest.mutable_versioninfo()->set_supportfb(true);
        consumptionRequest.set_startid(1014);
        consumptionRequest.set_count(20);
        string *fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "CMD";
        fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "body";

        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(10, reader.size());
        ASSERT_EQ((int64_t)1034, response.nextmsgid());
        ASSERT_EQ((int64_t)2047, response.maxmsgid());
        FieldGroupReader fieldGroupReader;
        for (int i = 0; i < 10; ++i) {
            const flat::Message *msg = reader.read(i);
            fieldGroupReader.fromConsumptionString(msg->data()->c_str(), msg->data()->size());
            ASSERT_EQ((size_t)2, fieldGroupReader.getFieldSize());
            const Field *field = fieldGroupReader.getField(0);
            EXPECT_TRUE(field->name.empty());
            string data;
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("add"), data);
            EXPECT_TRUE(field->isUpdated);
            field = fieldGroupReader.getField(1);
            EXPECT_TRUE(field->name.empty());
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("body") + autil::StringUtil::toString(1014 + 2 * i + 1), data);
            EXPECT_TRUE(field->isUpdated);
        }
    }

    // doGetMessage twice, one from dfs, another from group
    {
        ConsumptionRequest consumptionRequest;
        consumptionRequest.mutable_versioninfo()->set_supportfb(true);
        consumptionRequest.set_startid(2037);
        consumptionRequest.set_count(20);
        string *fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "CMD";
        fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "body";

        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(6, reader.size());
        ASSERT_EQ((int64_t)2048, response.nextmsgid());
        ASSERT_EQ((int64_t)2047, response.maxmsgid());
        FieldGroupReader fieldGroupReader;
        for (int i = 0; i < 6; ++i) {
            const flat::Message *msg = reader.read(i);
            fieldGroupReader.fromConsumptionString(msg->data()->c_str(), msg->data()->size());
            ASSERT_EQ((size_t)2, fieldGroupReader.getFieldSize());
            const Field *field = fieldGroupReader.getField(0);
            EXPECT_TRUE(field->name.empty());
            string data;
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("add"), data);
            EXPECT_TRUE(field->isUpdated);
            field = fieldGroupReader.getField(1);
            EXPECT_TRUE(field->name.empty());
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("body") + autil::StringUtil::toString(2037 + 2 * i), data);
            EXPECT_TRUE(field->isUpdated);
        }
    }
}

TEST_F(MessageBrainTest, testGetFieldFilterMessageWithException) {
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setNeedFieldFilter(true);
    {
        MessageBrain msgBrain;
        ReadMetricsCollector collector(_topicName, _partitionId);
        MessageResponse response;
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
        {
            ConsumptionRequest consumptionRequest;
            consumptionRequest.set_startid(0);
            consumptionRequest.set_count(10);
            string *fieldName = consumptionRequest.add_requiredfieldnames();
            *fieldName = "CMD";
            fieldName = consumptionRequest.add_requiredfieldnames();
            *fieldName = "body";

            ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
            ASSERT_EQ(ERROR_BROKER_NO_DATA, errCode);
            ASSERT_EQ(0, response.msgs_size());
        }
        {
            ConsumptionRequest consumptionRequest;
            consumptionRequest.set_startid(0);
            consumptionRequest.set_count(10);
            consumptionRequest.set_fieldfilterdesc("body IN body1");
            ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
            ASSERT_EQ(ERROR_BROKER_NO_DATA, errCode);
            ASSERT_EQ(0, response.msgs_size());
        }
        {
            ConsumptionRequest consumptionRequest;
            consumptionRequest.set_startid(0);
            consumptionRequest.set_count(10);
            consumptionRequest.set_fieldfilterdesc("body");
            ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
            ASSERT_EQ(ERROR_BROKER_INIT_FIELD_FILTER_FAILD, errCode);
            ASSERT_EQ(0, response.msgs_size());
        }
    }
    addMessageForDFS(config);
    {
        MessageBrain msgBrain;
        ReadMetricsCollector collector(_topicName, _partitionId);
        MessageResponse response;
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
        {
            ConsumptionRequest consumptionRequest;
            consumptionRequest.set_startid(0);
            consumptionRequest.set_count(10);
            ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
            ASSERT_EQ(ERROR_NONE, errCode);
        }
    }

    {
        MessageBrain msgBrain;
        ReadMetricsCollector collector(_topicName, _partitionId);
        MessageResponse response;
        config.setNeedFieldFilter(false);
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));

        ConsumptionRequest consumptionRequest;
        consumptionRequest.set_startid(0);
        consumptionRequest.set_count(10);
        string *fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "CMD";
        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_BROKER_TOPIC_NOT_SUPPORT_FIELD_FILTER, errCode);

        config.setNeedFieldFilter(true);
    }
    {
        MessageBrain msgBrain;
        ReadMetricsCollector collector(_topicName, _partitionId);
        MessageResponse response;
        config.setNeedFieldFilter(false);
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));

        ConsumptionRequest consumptionRequest;
        consumptionRequest.set_startid(0);
        consumptionRequest.set_count(10);
        consumptionRequest.set_fieldfilterdesc("body IN body1");

        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_BROKER_TOPIC_NOT_SUPPORT_FIELD_FILTER, errCode);

        config.setNeedFieldFilter(true);
    }
}

TEST_F(MessageBrainTest, testGetFieldFilterDescMessage) {
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setNeedFieldFilter(true);
    addMessageForDFS(config);
    MessageBrain msgBrain;
    ReadMetricsCollector collector(_topicName, _partitionId);
    ASSERT_EQ(ERROR_NONE,
              msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    { // get message only use field filter
        ConsumptionRequest consumptionRequest;
        consumptionRequest.set_startid(0);
        consumptionRequest.set_count(10);
        consumptionRequest.set_fieldfilterdesc("title IN title12|title20|title100");
        MessageResponse response;
        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(0, response.msgs_size());
        ASSERT_EQ((int64_t)10, response.nextmsgid());
        ASSERT_EQ((int64_t)2047, response.maxmsgid());
    }
    { // get message only use field filter
        ConsumptionRequest consumptionRequest;
        consumptionRequest.set_startid(0);
        consumptionRequest.set_count(200);
        consumptionRequest.set_fieldfilterdesc("title IN title2|title10|title100");
        MessageResponse response;
        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(3, response.msgs_size());
        ASSERT_EQ((int64_t)200, response.nextmsgid());
        ASSERT_EQ((int64_t)2047, response.maxmsgid());
        FieldGroupReader fieldGroupReader;
        int id[3] = {2, 10, 100};
        for (int i = 0; i < 3; ++i) {
            fieldGroupReader.fromProductionString(response.msgs(i).data());
            ASSERT_EQ((size_t)3, fieldGroupReader.getFieldSize());
            const Field *field = fieldGroupReader.getField(0);
            ASSERT_EQ(string("CMD"), field->name.to_string());
            string data;
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("add"), data);
            EXPECT_TRUE(!field->isUpdated);
            field = fieldGroupReader.getField(1);
            ASSERT_EQ(string("title"), field->name.to_string());
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("title") + autil::StringUtil::toString(id[i]), data);
            EXPECT_TRUE(field->isUpdated);
            field = fieldGroupReader.getField(2);
            ASSERT_EQ(string("body"), field->name.to_string());
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("body") + autil::StringUtil::toString(id[i]), data);
            EXPECT_TRUE(!field->isUpdated);
        }
    }
    { // get message with field filter desc and require fileds
        ConsumptionRequest consumptionRequest;
        consumptionRequest.set_startid(0);
        consumptionRequest.set_count(200);
        string *fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "CMD";
        fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "title";
        consumptionRequest.set_fieldfilterdesc("title IN title2|title10|title100");
        MessageResponse response;
        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(3, response.msgs_size());
        ASSERT_EQ((int64_t)200, response.nextmsgid());
        ASSERT_EQ((int64_t)2047, response.maxmsgid());
        FieldGroupReader fieldGroupReader;
        int id[3] = {2, 10, 100};
        for (int i = 0; i < 3; ++i) {
            fieldGroupReader.fromConsumptionString(response.msgs(i).data());
            ASSERT_EQ((size_t)2, fieldGroupReader.getFieldSize());
            const Field *field = fieldGroupReader.getField(0);
            EXPECT_TRUE(field->name.empty());
            string data;
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("add"), data);
            EXPECT_TRUE(!field->isUpdated);
            field = fieldGroupReader.getField(1);
            EXPECT_TRUE(field->name.empty());
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("title") + autil::StringUtil::toString(id[i]), data);
            EXPECT_TRUE(field->isUpdated);
        }
    }
    { // get message with field filter desc and require fileds
        ConsumptionRequest consumptionRequest;
        MessageResponse response;
        consumptionRequest.set_startid(0);
        consumptionRequest.set_count(200);
        string *fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "CMD";
        fieldName = consumptionRequest.add_requiredfieldnames();
        *fieldName = "title";
        consumptionRequest.set_fieldfilterdesc("title IN title2|title100|title10 AND body IN body2");

        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(1, response.msgs_size());
        ASSERT_EQ((int64_t)200, response.nextmsgid());
        ASSERT_EQ((int64_t)2047, response.maxmsgid());
        FieldGroupReader fieldGroupReader;
        int id[3] = {2};
        for (int i = 0; i < 1; ++i) {
            fieldGroupReader.fromConsumptionString(response.msgs(i).data());
            ASSERT_EQ((size_t)2, fieldGroupReader.getFieldSize());
            const Field *field = fieldGroupReader.getField(0);
            EXPECT_TRUE(field->name.empty());
            string data;
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("add"), data);
            EXPECT_TRUE(!field->isUpdated);
            field = fieldGroupReader.getField(1);
            EXPECT_TRUE(field->name.empty());
            data.assign(field->value.begin(), field->value.end());
            ASSERT_EQ(string("title") + autil::StringUtil::toString(id[i]), data);
            EXPECT_TRUE(field->isUpdated);
        }
    }
}

TEST_F(MessageBrainTest, testPermission) {
    {
        // reach write limit
        BlockPool centerPool(1 << 29, 1 << 20);
        BlockPool fileCachePool(&centerPool, 1 << 29, 1 << 20);

        WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
        ReadMetricsCollector collector(_topicName, _partitionId);
        MessageBrain msgBrain;
        config::PartitionConfig config;
        config.setTopicMode(TOPIC_MODE_MEMORY_ONLY);
        PermissionCenter permissionCenter(1, 10, 1, 0);
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, &centerPool, &fileCachePool, &permissionCenter, &_metricReporter, _taskStatus));
        ProductionRequest pReq;
        MessageResponse response;
        for (uint64_t i = 0; i < 10; ++i) {
            protocol::Message *msg = pReq.add_msgs();
            msg->set_data(std::string(1024, 'x'));
        }
        FakeClosure done;
        ErrorCode errCode =
            msgBrain.addMessage(new ProductionLogClosure(&pReq, &response, &done, "sendMessage"), writeCollector);

        EXPECT_TRUE(done.getDoneStatus());
        ASSERT_EQ(ERROR_BROKER_BUSY, errCode);
        ASSERT_EQ((uint32_t)0, response.acceptedmsgcount());
    }
    { // get max message id reach read limit
        BlockPool centerPool(1 << 29, 1 << 20);
        BlockPool fileCachePool(&centerPool, 1 << 29, 1 << 20);
        config::PartitionConfig config;
        config.setDataRoot(GET_TEMPLATE_DATA_PATH());
        MessageBrain msgBrain;
        PermissionCenter permissionCenter(1, 10, 0, 1);

        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, &centerPool, &fileCachePool, &permissionCenter, &_metricReporter, _taskStatus));
        MessageIdRequest idRequest;
        MessageIdResponse response;
        idRequest.set_topicname(_topicName);
        idRequest.set_partitionid(0);
        ErrorCode errCode = msgBrain.getMaxMessageId(&idRequest, &response);
        ASSERT_EQ(ERROR_BROKER_BUSY, errCode);
        ASSERT_EQ(0, response.msgid());
    }
    { // get message no limit
        BlockPool centerPool(1 << 29, 1 << 20);
        BlockPool fileCachePool(&centerPool, 1 << 29, 1 << 20);
        config::PartitionConfig config;
        config.setDataRoot(GET_TEMPLATE_DATA_PATH());
        MessageBrain msgBrain;
        PermissionCenter permissionCenter(1, 10, 0, 1);

        ReadMetricsCollector collector(_topicName, _partitionId);
        MessageResponse response;
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, &centerPool, &fileCachePool, &permissionCenter, &_metricReporter, _taskStatus));
        ConsumptionRequest consumptionRequest;
        consumptionRequest.set_startid(0);
        consumptionRequest.set_count(10);
        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_BROKER_NO_DATA, errCode);
        ASSERT_EQ(0, response.msgs_size());
    }
    { // reach read file limit
        BlockPool centerPool(1 << 29, 1 << 20);
        BlockPool fileCachePool(&centerPool, 1 << 29, 1 << 20);
        config::PartitionConfig config;
        config.setDataRoot(GET_TEMPLATE_DATA_PATH());
        config.setNeedFieldFilter(true);
        addMessageForDFS(config);
        MessageBrain msgBrain;
        ReadMetricsCollector collector(_topicName, _partitionId);
        PermissionCenter permissionCenter(1, 10, 1, 1); // read file permission must greater than zero
        MessageResponse response;
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, &centerPool, &fileCachePool, &permissionCenter, &_metricReporter, _taskStatus));
        ConsumptionRequest consumptionRequest;
        consumptionRequest.set_startid(0);
        consumptionRequest.set_count(10);
        ErrorCode errCode = msgBrain.getMessage(&consumptionRequest, &response, _timeoutChecker, NULL, collector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(10, response.msgs_size());
    }
}

TEST_F(MessageBrainTest, testValidateMessageId) {
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setPartitionMinBufferSize(1 << 21); // 2M
    config.setPartitionMaxBufferSize(1 << 21); // 2M
    config.setBlockSize(4 * 1024);             // 4K
    config.setMaxCommitTimeAsError(100 * 1000 * 1000);
    BlockPool centerPool(1 << 22, 512 * 1024);
    BlockPool fileCachePool(&centerPool, 1 << 21, 1 << 18);
    ReadMetricsCollector collector(_topicName, _partitionId);
    prepareMessages(config, &centerPool, &_metricReporter);

    MessageBrain msgBrain;
    ASSERT_EQ(ERROR_NONE,
              msgBrain.init(config, &centerPool, &fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));

    // read all message out
    int msgCount = 3 * 1024;
    protocol::ConsumptionRequest req;
    req.set_startid(0);
    req.set_partitionid(_partitionId);
    req.set_topicname(_topicName);
    req.set_count(msgCount);
    req.set_maxtotalsize(1024 * 1024 * 1024);

    protocol::MessageResponse res;
    ASSERT_EQ(ERROR_NONE, msgBrain.getMessage(&req, &res, NULL, NULL, collector));
    ASSERT_EQ(msgCount, res.msgs_size());

    int64_t lastMsgId = res.msgs(msgCount - 1).msgid();
    int64_t lastMsgTime = res.msgs(msgCount - 1).timestamp();

    req.Clear();

    EXPECT_TRUE(!msgBrain.messageIdValid(&req, _timeoutChecker)); // no msgId

    req.set_startid(0); // msgId = 0
    EXPECT_TRUE(msgBrain.messageIdValid(&req, _timeoutChecker));

    req.set_startid(lastMsgId + 1); // no msgTime
    EXPECT_TRUE(msgBrain.messageIdValid(&req, _timeoutChecker));

    req.set_starttimestamp(lastMsgTime + 1); // normal situation
    EXPECT_TRUE(msgBrain.messageIdValid(&req, _timeoutChecker));

    for (int i = 0; i < msgCount; i++) {
        req.set_startid(res.msgs(i).msgid());
        req.set_starttimestamp(res.msgs(i).timestamp());
        EXPECT_TRUE(msgBrain.messageIdValid(&req, _timeoutChecker));
    }
    for (int i = 1; i < msgCount; i++) {
        req.set_startid(res.msgs(i).msgid());
        req.set_starttimestamp(res.msgs(i).timestamp());
        EXPECT_TRUE(msgBrain.messageIdValid(&req, _timeoutChecker));
        if (i != msgCount - 1) {
            req.set_starttimestamp(res.msgs(i + 1).timestamp());
            EXPECT_FALSE(msgBrain.messageIdValid(&req, _timeoutChecker));
        }
    }
}

TEST_F(MessageBrainTest, testValidateMessageIdInMemory) {
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setPartitionMinBufferSize(1 << 21); // 2M
    config.setPartitionMaxBufferSize(1 << 21); // 2M
    config.setBlockSize(4 * 1024);             // 4K
    config.setMaxCommitTimeAsError(100 * 1000 * 1000);
    BlockPool centerPool(1 << 22, 512 * 1024);
    BlockPool fileCachePool(&centerPool, 1 << 21, 1 << 18);
    ReadMetricsCollector collector(_topicName, _partitionId);
    prepareMessages(config, &centerPool, &_metricReporter);

    MessageBrain msgBrain;
    ASSERT_EQ(ERROR_NONE,
              msgBrain.init(config, &centerPool, &fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    // read all message out
    int msgCount = 3 * 1024;
    protocol::ConsumptionRequest req;
    req.set_startid(0);
    req.set_partitionid(_partitionId);
    req.set_topicname(_topicName);
    req.set_count(msgCount);
    req.set_maxtotalsize(1024 * 1024 * 1024);

    protocol::MessageResponse res;
    ASSERT_EQ(ERROR_NONE, msgBrain.getMessage(&req, &res, NULL, NULL, collector));
    ASSERT_EQ(msgCount, res.msgs_size());

    int64_t lastMsgId = res.msgs(msgCount - 1).msgid();
    int64_t lastMsgTime = res.msgs(msgCount - 1).timestamp();

    req.Clear();
    EXPECT_TRUE(!msgBrain.messageIdValidInMemory(&req)); // no msgId

    req.set_startid(0); // msgId = 0
    EXPECT_TRUE(msgBrain.messageIdValidInMemory(&req));

    req.set_startid(lastMsgId + 1); // no msgTime
    EXPECT_TRUE(msgBrain.messageIdValidInMemory(&req));

    req.set_starttimestamp(lastMsgTime + 1); // normal situation
    EXPECT_TRUE(msgBrain.messageIdValidInMemory(&req));

    for (int i = 1; i < msgCount - 1; i++) {
        req.set_startid(res.msgs(i).msgid());
        req.set_starttimestamp(res.msgs(i).timestamp());
        EXPECT_FALSE(msgBrain.messageIdValidInMemory(&req)) << i;
    }
    req.set_startid(res.msgs(msgCount - 1).msgid());
    req.set_starttimestamp(res.msgs(msgCount - 1).timestamp());
    EXPECT_TRUE(msgBrain.messageIdValidInMemory(&req));
}

TEST_F(MessageBrainTest, testGetMessageWithSeek) {
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setPartitionMinBufferSize(1 << 21); // 2M
    config.setPartitionMaxBufferSize(1 << 21); // 2M
    config.setBlockSize(4 * 1024);             // 4K
    config.setMaxCommitTimeAsError(100 * 1000 * 1000);
    BlockPool centerPool(1 << 22, 512 * 1024);
    BlockPool fileCachePool(&centerPool, 1 << 21, 1 << 19);
    ReadMetricsCollector collector(_topicName, _partitionId);

    prepareMessages(config, &centerPool, &_metricReporter);
    MessageBrain msgBrain;
    ASSERT_EQ(ERROR_NONE,
              msgBrain.init(config, &centerPool, &fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));

    // read all message out
    int msgCount = 3 * 1024;
    protocol::ConsumptionRequest req;
    req.set_startid(0);
    req.set_partitionid(_partitionId);
    req.set_topicname(_topicName);
    req.set_count(msgCount);
    req.set_maxtotalsize(1024 * 1024 * 1024);

    protocol::MessageResponse res;
    ASSERT_EQ(ERROR_NONE, msgBrain.getMessage(&req, &res, NULL, NULL, collector));
    ASSERT_EQ(msgCount, res.msgs_size());
    for (int step = 1; step <= 5; step++) {
        req.set_count(step);
        for (int i = step; i < msgCount; i++) {
            protocol::MessageResponse res2;
            req.set_startid(res.msgs(i).msgid());
            req.set_starttimestamp(res.msgs(i - step).timestamp());
            ASSERT_EQ(ERROR_NONE, msgBrain.getMessage(&req, &res2, NULL, NULL, collector));
            ASSERT_EQ(step, res2.msgs_size());
            for (int j = 0; j < step; j++) {
                const protocol::Message &expected = res.msgs(i - step + j);
                const protocol::Message &actual = res2.msgs(j);
                EXPECT_TRUE(expected == actual) << j;
            }
            ASSERT_EQ(res.msgs(i).msgid(), res2.nextmsgid());
            ASSERT_EQ(res.msgs(i).timestamp(), res2.nexttimestamp());
        }
    }
    req.set_startid(msgCount);
    req.set_starttimestamp(res.msgs(msgCount - 1).timestamp());
    protocol::MessageResponse res2;
    ASSERT_EQ(ERROR_NONE, msgBrain.getMessage(&req, &res2, NULL, NULL, collector));
    ASSERT_EQ(1, res2.msgs_size());
    EXPECT_TRUE(res.msgs(msgCount - 1) == res2.msgs(0));
    ASSERT_EQ(res.msgs(msgCount - 1).msgid() + 1, res2.nextmsgid());
    EXPECT_TRUE(res.msgs(msgCount - 1).timestamp() < res2.nexttimestamp());
}

TEST_F(MessageBrainTest, testGetMessageFlowCtrol) {
    BlockPool centerPool(1 << 29, 1 << 20);
    BlockPool fileCachePool(32 * 1024 * 1024, 1024 * 1024);
    ReadMetricsCollector readCollector(_topicName, _partitionId);
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setPartitionMinBufferSize(1 << 29); // 512M
    config.setPartitionMaxBufferSize(1 << 29); // 512M
    config.setBlockSize(1 << 20);              // 1M
    config.setMaxReadSizeSec(1);               // 1M
    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    { //读内存无限制
        //生产数据
        MessageBrain msgBrain;
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, &centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));

        ProductionRequest pReq;
        MessageResponse response;
        for (uint64_t i = 0; i < 1024; ++i) {
            protocol::Message *msg = pReq.add_msgs();
            msg->set_data(std::string(2048, 'x')); // 2k
        }
        usleep(100 * 1000);
        FakeClosure done;
        ErrorCode errCode =
            msgBrain.addMessage(new ProductionLogClosure(&pReq, &response, &done, "sendMessage"), writeCollector);
        EXPECT_TRUE(done.getDoneStatus());
        ASSERT_EQ(errCode, ERROR_NONE);
        ASSERT_EQ(response.acceptedmsgcount(), (uint32_t)1024);

        string ipPort("1.1.1.1:11");
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(512);
        errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, &ipPort, readCollector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(512, response.msgs_size());
        ASSERT_EQ(1023, response.maxmsgid());

        response = MessageResponse();
        request.set_startid(512);
        errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, &ipPort, readCollector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(512, response.msgs_size());
    }
    { // 读dfs文件限速
        MessageBrain msgBrain;
        ASSERT_EQ(ERROR_NONE,
                  msgBrain.init(config, &centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));

        const auto &filePairVec = msgBrain._fsMessageReader->_fileManager->_filePairVec;
        ASSERT_EQ(1, filePairVec.size());
        string ipPort("1.1.1.1:11");
        MessageResponse response;
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(512);
        ErrorCode errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, &ipPort, readCollector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(512, response.msgs_size());
        ASSERT_EQ(1023, response.maxmsgid());

        response = MessageResponse();
        request.set_startid(512);
        request.set_count(1);
        errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, &ipPort, readCollector);
        ASSERT_EQ(ERROR_BROKER_BUSY, errCode);
        ASSERT_EQ(0, response.msgs_size());
        usleep(1000 * 1000); //等待flowctrol过期
        errCode = msgBrain.getMessage(&request, &response, _timeoutChecker, &ipPort, readCollector);
        ASSERT_EQ(ERROR_NONE, errCode);
        ASSERT_EQ(1, response.msgs_size());
    }
}

TEST_F(MessageBrainTest, testCommitMessageForMemoryOnlyTopic) {
    config::PartitionConfig config;
    config.setTopicMode(TOPIC_MODE_MEMORY_ONLY);
    BlockPool *centerPool = new BlockPool(sizeof(MemoryMessage) << 21, sizeof(MemoryMessage) << 10);
    BlockPool *fileCachePool = new BlockPool(centerPool, 1 << 21, 1 << 18);
    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);
    MessageBrain *msgBrain = new MessageBrain;
    ASSERT_EQ(ERROR_NONE,
              msgBrain->init(config, centerPool, fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    string data(sizeof(MemoryMessage), 'a');
    ProductionRequest request; // 512 message each request, need one block
    for (int j = 0; j < 16; ++j) {
        Message *msg = request.add_msgs();
        msg->set_data(data);
    }
    MessageResponse response;
    FakeClosure done;
    ErrorCode errCode =
        msgBrain->addMessage(new ProductionLogClosure(&request, &response, &done, "sendMessage"), writeCollector);
    EXPECT_TRUE(done.getDoneStatus());
    ASSERT_EQ(ERROR_NONE, errCode);
    ASSERT_EQ(-1, msgBrain->_messageGroup->_msgDeque->getCommittedMsgId());
    ASSERT_EQ(-1, response.committedid());
    ASSERT_TRUE(msgBrain->needCommitMessage());
    msgBrain->commitMessage();
    ASSERT_TRUE(msgBrain->needCommitMessage());
    ASSERT_EQ(15, msgBrain->_messageGroup->_msgDeque->getCommittedMsgId());
    errCode = msgBrain->addMessage(new ProductionLogClosure(&request, &response, &done, "sendMessage"), writeCollector);
    ASSERT_EQ(15, response.committedid());

    delete msgBrain;
    delete fileCachePool;
    delete centerPool;
}

TEST_F(MessageBrainTest, testCommitMessageForMemoryOnlyTopicWithDFS) {
    config::PartitionConfig config;
    config.setTopicMode(TOPIC_MODE_MEMORY_ONLY);
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    BlockPool *centerPool = new BlockPool(sizeof(MemoryMessage) << 21, sizeof(MemoryMessage) << 10);
    BlockPool *fileCachePool = new BlockPool(centerPool, 1 << 21, 1 << 18);
    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);
    MessageBrain *msgBrain = new MessageBrain;
    ASSERT_EQ(ERROR_NONE,
              msgBrain->init(config, centerPool, fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    string data(sizeof(MemoryMessage), 'a');
    ProductionRequest request; // 512 message each request, need one block
    for (int j = 0; j < 16; ++j) {
        Message *msg = request.add_msgs();
        msg->set_data(data);
    }
    MessageResponse response;
    FakeClosure done;
    ErrorCode errCode =
        msgBrain->addMessage(new ProductionLogClosure(&request, &response, &done, "sendMessage"), writeCollector);
    EXPECT_TRUE(done.getDoneStatus());
    ASSERT_EQ(ERROR_NONE, errCode);
    ASSERT_EQ(-1, msgBrain->_messageGroup->_msgDeque->getCommittedMsgId());
    ASSERT_EQ(-1, response.committedid());
    msgBrain->commitMessage();
    ASSERT_EQ(15, msgBrain->_messageGroup->_msgDeque->getCommittedMsgId());
    errCode = msgBrain->addMessage(new ProductionLogClosure(&request, &response, &done, "sendMessage"), writeCollector);
    ASSERT_EQ(15, response.committedid());

    delete msgBrain;
    delete fileCachePool;
    delete centerPool;
}

TEST_F(MessageBrainTest, testCommitMessageForNormalTopic) {
    config::PartitionConfig config;
    config.setConfigUnlimited(true);
    config.setMaxCommitSize(16);
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    BlockPool *centerPool = new BlockPool(sizeof(MemoryMessage) << 21, sizeof(MemoryMessage) << 10);
    BlockPool *fileCachePool = new BlockPool(centerPool, 1 << 21, 1 << 18);
    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);
    MessageBrain *msgBrain = new MessageBrain;
    ASSERT_EQ(ERROR_NONE,
              msgBrain->init(config, centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    string data(sizeof(MemoryMessage), 'a');
    ProductionRequest request; // 512 message each request, need one block
    for (int j = 0; j < 16; ++j) {
        Message *msg = request.add_msgs();
        msg->set_data(data);
    }
    MessageResponse response;
    FakeClosure done;
    ErrorCode errCode =
        msgBrain->addMessage(new ProductionLogClosure(&request, &response, &done, "sendMessage"), writeCollector);
    EXPECT_TRUE(done.getDoneStatus());
    ASSERT_EQ(ERROR_NONE, errCode);
    ASSERT_EQ(-1, msgBrain->_messageGroup->_msgDeque->getCommittedMsgId());
    msgBrain->commitMessage();
    ASSERT_EQ(15, msgBrain->_messageGroup->_msgDeque->getCommittedMsgId());
    delete msgBrain;
    delete fileCachePool;
    delete centerPool;
}

TEST_F(MessageBrainTest, testCommitMessageForMemoryPreferTopicTimeout) {
    config::PartitionConfig config;
    config.setConfigUnlimited(true);
    config.setMaxCommitSize(16);
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setTopicMode(TOPIC_MODE_MEMORY_PREFER);
    config.setMaxCommitIntervalForMemoryPreferTopic(3 * 1000 * 1000);
    BlockPool *centerPool = new BlockPool(sizeof(MemoryMessage) << 21, sizeof(MemoryMessage) << 10);
    BlockPool *fileCachePool = new BlockPool(centerPool, 1 << 21, 1 << 18);
    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);
    MessageBrain *msgBrain = new MessageBrain;
    ASSERT_EQ(ERROR_NONE,
              msgBrain->init(config, centerPool, fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    string data(sizeof(MemoryMessage), 'a');
    ProductionRequest request;
    for (int j = 0; j < 16; ++j) {
        Message *msg = request.add_msgs();
        msg->set_data(data);
    }
    MessageResponse response;
    FakeClosure done;
    ErrorCode errCode =
        msgBrain->addMessage(new ProductionLogClosure(&request, &response, &done, "sendMessage"), writeCollector);
    EXPECT_TRUE(done.getDoneStatus());
    ASSERT_EQ(ERROR_NONE, errCode);
    uint32_t retryTimes = 3;
    int64_t committedId = -1;
    while (retryTimes-- != 0) {
        committedId = msgBrain->_messageGroup->_msgDeque->getCommittedMsgId();
        if (15 == committedId) {
            break;
        }
        EXPECT_EQ(-1, committedId);
        msgBrain->commitMessage();
        usleep(1000 * 1000);
    }
    EXPECT_EQ(15, committedId);

    delete msgBrain;
    delete fileCachePool;
    delete centerPool;
}

TEST_F(MessageBrainTest, testCommitMessageForMemoryPreferTopic) {
    config::PartitionConfig config;
    config.setConfigUnlimited(true);
    config.setMaxCommitSize(5);
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setTopicMode(TOPIC_MODE_MEMORY_PREFER);
    config.setMaxCommitIntervalForMemoryPreferTopic(2 * 1000 * 1000);
    config.setMaxCommitIntervalWhenDelay(1000 * 1000); // 1s
    BlockPool *centerPool = new BlockPool(sizeof(MemoryMessage) << 21, sizeof(MemoryMessage) << 10);
    BlockPool *fileCachePool = new BlockPool(centerPool, 1 << 21, 1 << 18);
    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);
    MessageBrain *msgBrain = new MessageBrain();
    ASSERT_EQ(ERROR_NONE,
              msgBrain->init(config, centerPool, fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    string data(sizeof(MemoryMessage), 'a');
    ProductionRequest request; // 512 message each request, need one block
    for (int j = 0; j < 16; ++j) {
        Message *msg = request.add_msgs();
        msg->set_data(data);
    }
    MessageResponse response;
    FakeClosure done;
    ErrorCode errCode =
        msgBrain->addMessage(new ProductionLogClosure(&request, &response, &done, "sendMessage"), writeCollector);
    EXPECT_TRUE(done.getDoneStatus());
    ASSERT_EQ(ERROR_NONE, errCode);
    CommitInfo &info = (msgBrain->_commitManager->_rangeInfo)[CommitManager::Range(0, 128)];
    info.commitMsgId = 10;

    ASSERT_EQ(-1, msgBrain->_messageGroup->_msgDeque->getCommittedMsgId());

    msgBrain->commitMessage();
    ASSERT_TRUE(msgBrain->needCommitMessage());
    ASSERT_EQ(10, msgBrain->_messageGroup->_msgDeque->getCommittedMsgId());
    usleep(3 * 1000 * 1000);
    msgBrain->commitMessage();
    ASSERT_EQ(15, msgBrain->_messageGroup->_msgDeque->getCommittedMsgId());
    ASSERT_FALSE(msgBrain->needCommitMessage());
    delete msgBrain;
    delete fileCachePool;
    delete centerPool;
}

TEST_F(MessageBrainTest, testCommitMessageForMemoryPreferTopicWhenDelay) {
    config::PartitionConfig config;
    config.setConfigUnlimited(true);
    config.setMaxCommitSize(5);
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setTopicMode(TOPIC_MODE_MEMORY_PREFER);
    config.setMaxCommitIntervalWhenDelay(2 * 1000 * 1000);
    BlockPool *centerPool = new BlockPool(sizeof(MemoryMessage) << 21, sizeof(MemoryMessage) << 10);
    BlockPool *fileCachePool = new BlockPool(centerPool, 1 << 21, 1 << 18);
    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);
    MessageBrain *msgBrain = new MessageBrain();
    ASSERT_EQ(ERROR_NONE,
              msgBrain->init(config, centerPool, fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    string data(sizeof(MemoryMessage), 'a');
    ProductionRequest request;
    for (int j = 0; j < 16; ++j) {
        Message *msg = request.add_msgs();
        msg->set_data(data);
    }
    MessageResponse response;
    FakeClosure done;
    ErrorCode errCode =
        msgBrain->addMessage(new ProductionLogClosure(&request, &response, &done, "sendMessage"), writeCollector);
    EXPECT_TRUE(done.getDoneStatus());
    ASSERT_EQ(ERROR_NONE, errCode);
    CommitInfo &info = (msgBrain->_commitManager->_rangeInfo)[CommitManager::Range(0, 128)];
    info.commitMsgId = 10;
    ASSERT_EQ(-1, msgBrain->_messageGroup->_msgDeque->getCommittedMsgId());
    msgBrain->commitMessage();
    ASSERT_EQ(10, msgBrain->_messageGroup->_msgDeque->getCommittedMsgId());
    usleep(3 * 1000 * 1000);
    msgBrain->commitMessage();
    ASSERT_EQ(15, msgBrain->_messageGroup->_msgDeque->getCommittedMsgId());
    delete msgBrain;
    delete fileCachePool;
    delete centerPool;
}

void MessageBrainTest::prepareMessages(const config::PartitionConfig &config,
                                       BlockPool *centerPool,
                                       BrokerMetricsReporter *amonReporter) {
    MessageBrain msgBrain;
    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    BlockPool fileCachePool(centerPool, 1 << 16, 1 << 12);
    ASSERT_EQ(ERROR_NONE,
              msgBrain.init(config, centerPool, &fileCachePool, _permissionCenter, amonReporter, _taskStatus));
    for (int k = 0; k < 3; ++k) {
        ProductionRequest pReq;
        MessageResponse response;
        for (uint64_t i = 0; i < 1024; ++i) {
            protocol::Message *msg = pReq.add_msgs();
            msg->set_data(string(512, 'x'));
            msg->set_uint16payload(i);
            msg->set_uint8maskpayload(i % 256);
        }
        usleep(1000 * 1000);
        FakeClosure done;
        ErrorCode errCode =
            msgBrain.addMessage(new ProductionLogClosure(&pReq, &response, &done, "sendMessage"), writeCollector);

        EXPECT_TRUE(done.getDoneStatus());
        ASSERT_EQ(errCode, ERROR_NONE);
        ASSERT_EQ(response.acceptedmsgcount(), (uint32_t)1024);
    }
}

void MessageBrainTest::addMessageForDFS(config::PartitionConfig config) {
    MessageBrain msgBrain;
    WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector(_topicName, _partitionId));
    ReadMetricsCollector collector(_topicName, _partitionId);
    config.setMaxCommitTimeAsError(100 * 1000 * 1000);
    ASSERT_EQ(ERROR_NONE,
              msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, _taskStatus));
    ProductionRequest productionRequest;
    for (int i = 0; i < 2048; ++i) {
        Message *msg = productionRequest.add_msgs();
        FieldGroupWriter fieldGroupWriter;
        fieldGroupWriter.addProductionField("CMD", "add", i % 2);
        fieldGroupWriter.addProductionField("title", "title" + autil::StringUtil::toString(i), (i + 1) % 2);
        fieldGroupWriter.addProductionField("body", "body" + autil::StringUtil::toString(i), i % 2);
        string data = fieldGroupWriter.toString();
        msg->set_data(data);
    }
    MessageResponse response;
    FakeClosure done;
    ErrorCode errCode = msgBrain.addMessage(
        new ProductionLogClosure(&productionRequest, &response, &done, "sendMessage"), writeCollector);

    EXPECT_TRUE(done.getDoneStatus());
    ASSERT_EQ(ERROR_NONE, errCode);
}

TEST_F(MessageBrainTest, testGetReaderDescription) {
    MessageBrain brain;
    {
        protocol::ConsumptionRequest request;
        string srcIpPort;
        ReaderDes reader = brain.getReaderDescription(&request, srcIpPort);
        ASSERT_EQ("0.0.0.0:0, from:0, to:65535", reader.toString());
    }
    {
        protocol::ConsumptionRequest request;
        request.mutable_filter()->set_from(0);
        request.mutable_filter()->set_to(1);
        string srcIpPort("10.1.1.2:100");
        ReaderDes reader = brain.getReaderDescription(&request, srcIpPort);
        ASSERT_EQ("10.1.1.2:100, from:0, to:1", reader.toString());
    }
}

TEST_F(MessageBrainTest, testRecycleObsoleteReader) {
    MessageBrain brain;
    brain._obsoleteReaderInterval = 3 * 1000 * 1000;       // 3s
    brain._obsoleteReaderMetricInterval = 2 * 1000 * 1000; // 2s

    const int READER_SIZE = 5;
    ReaderDes reader;
    for (int index = 0; index < READER_SIZE; ++index) {
        string ipPort("10.101.193.199:");
        ipPort += std::to_string(index);
        reader.setIpPort(ipPort);
        ReaderInfoPtr info(new ReaderInfo);
        for (size_t ii = 0; ii < 10; ++ii) {
            usleep(100 * 1000);
            info->metaInfo->updateRate(index * 10);
            info->dataInfo->updateRate(index * 100);
        }
        int64_t ct = autil::TimeUtility::currentTime();
        info->requestTime = ct;
        brain._readerInfoMap[reader] = info;
    }
    ASSERT_EQ(5, brain._readerInfoMap.size());
    brain.recycleObsoleteReader();
    ASSERT_EQ(3, brain._readerInfoMap.size());

    auto iter = brain._readerInfoMap.begin();
    ASSERT_EQ("10.101.193.199:2", iter->first.toIpPort());
    ASSERT_EQ(0, iter->second->metaInfo->_latedData.size());
    ASSERT_EQ(0, iter->second->dataInfo->_latedData.size());
    ASSERT_EQ(0, iter->second->metaInfo->getReadRate());
    ASSERT_EQ(0, iter->second->dataInfo->getReadRate());
    iter++;
    ASSERT_EQ("10.101.193.199:3", iter->first.toIpPort());
    ASSERT_EQ(10, iter->second->metaInfo->_latedData.size());
    ASSERT_EQ(10, iter->second->dataInfo->_latedData.size());
    ASSERT_EQ(5, iter->second->metaInfo->getReadRate());
    ASSERT_EQ(50, iter->second->dataInfo->getReadRate());
    iter++;
    ASSERT_EQ("10.101.193.199:4", iter->first.toIpPort());
    ASSERT_EQ(10, iter->second->metaInfo->_latedData.size());
    ASSERT_EQ(10, iter->second->dataInfo->_latedData.size());
    ASSERT_EQ(400, iter->second->metaInfo->getReadRate());
    ASSERT_EQ(4000, iter->second->dataInfo->getReadRate());
}

TEST_F(MessageBrainTest, testUpdateReaderInfo) {
    MessageBrain brain;
    protocol::ConsumptionRequest request;
    request.mutable_filter()->set_from(0);
    request.mutable_filter()->set_to(1);
    string srcIpPort("10.1.1.2:100");
    ReaderDes reader = brain.getReaderDescription(&request, srcIpPort);
    ASSERT_EQ("10.1.1.2:100, from:0, to:1", reader.toString());

    protocol::MessageResponse response;
    response.set_nexttimestamp(100);
    response.set_nextmsgid(10);
    string ipPort("1.1.1.1:1");
    ReaderInfoPtr info(new ReaderInfo());

    brain.updateReaderInfo(ERROR_UNKNOWN, &request, &ipPort, &response, info);
    ASSERT_EQ(0, brain._readerInfoMap.size());
    ASSERT_EQ(0, info->nextTimestamp);
    ASSERT_EQ(0, info->nextMsgId);

    brain.updateReaderInfo(ERROR_NONE, &request, NULL, &response, info);
    ASSERT_EQ(0, brain._readerInfoMap.size());
    ASSERT_EQ(0, info->nextTimestamp);
    ASSERT_EQ(0, info->nextMsgId);

    string emptyIpPort;
    brain.updateReaderInfo(ERROR_NONE, &request, &emptyIpPort, &response, info);
    ASSERT_EQ(0, brain._readerInfoMap.size());
    ASSERT_EQ(0, info->nextTimestamp);
    ASSERT_EQ(0, info->nextMsgId);

    brain.updateReaderInfo(ERROR_NONE, &request, &ipPort, &response, info);
    ASSERT_EQ(1, brain._readerInfoMap.size());
    ASSERT_EQ(100, info->nextTimestamp);
    ASSERT_EQ(10, info->nextMsgId);

    string ipPort2("1.1.1.1:2");
    ReaderInfoPtr info2(new ReaderInfo());
    brain.updateReaderInfo(ERROR_BROKER_BUSY, &request, &ipPort2, &response, info2);
    ASSERT_EQ(2, brain._readerInfoMap.size());
    ASSERT_EQ(100, info2->nextTimestamp);
    ASSERT_EQ(10, info2->nextMsgId);
}

TEST_F(MessageBrainTest, testAdd_GetMessage_SealError) {
    { // 1. get message
        ConsumptionRequest request;
        MessageResponse response;
        ReadMetricsCollector readCollector("topic", 0);
        MessageBrain msgBrain;
        msgBrain._enableFastRecover = true;
        ErrorCode ec = msgBrain.getMessage(&request, &response, nullptr, nullptr, readCollector);
        EXPECT_EQ(ERROR_BROKER_MESSAGE_FORMAT_INVALID, ec);

        PartitionId pid;
        config::PartitionConfig config;
        FileManager fileManager;
        msgBrain._messageCommitter = new MessageCommitter(pid, config, &fileManager, nullptr, true);
        ec = msgBrain.getMessage(&request, &response, nullptr, nullptr, readCollector);
        EXPECT_EQ(ERROR_BROKER_MESSAGE_FORMAT_INVALID, ec);

        msgBrain._messageCommitter->_hasSealError = true;
        ec = msgBrain.getMessage(&request, &response, nullptr, nullptr, readCollector);
        EXPECT_EQ(ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND, ec);
    }
    { // 2. add message
        ProductionRequest request;
        MessageResponse response;
        PartitionId pid;
        WriteMetricsCollectorPtr writeCollector(new WriteMetricsCollector("topic", 0));
        util::PermissionCenter permission;
        permission._writeLimit = 10;
        permission._curWriteCount = 20; // not permission
        MessageBrain msgBrain;
        msgBrain._enableFastRecover = true;
        msgBrain._permissionCenter = &permission;
        msgBrain._messageGroup = new MessageGroup();
        msgBrain._messageGroup->_timestampAllocator = new TimestampAllocator();
        msgBrain._messageGroup->_msgDeque = new MessageDeque(_centerPool, msgBrain._messageGroup->_timestampAllocator);
        {
            FakeClosure done;
            ProductionLogClosure *closure = new ProductionLogClosure(&request, &response, &done, "addMessage");
            ErrorCode ec = msgBrain.addMessage(closure, writeCollector);
            EXPECT_EQ(ERROR_BROKER_BUSY, ec);
        }
        {
            FakeClosure done;
            ProductionLogClosure *closure = new ProductionLogClosure(&request, &response, &done, "addMessage");
            config::PartitionConfig config;
            FileManager fileManager;
            msgBrain._messageCommitter = new MessageCommitter(pid, config, &fileManager, nullptr, true);
            ErrorCode ec = msgBrain.addMessage(closure, writeCollector);
            EXPECT_EQ(ERROR_BROKER_BUSY, ec);
        }
        {
            FakeClosure done;
            ProductionLogClosure *closure = new ProductionLogClosure(&request, &response, &done, "addMessage");
            msgBrain._messageCommitter->_hasSealError = true;
            ErrorCode ec = msgBrain.addMessage(closure, writeCollector);
            EXPECT_EQ(ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND, ec);
        }
    }
    { // 3. getMinMessageIdByTime
        util::PermissionCenter permission;
        permission._readLimit = 10;
        permission._curReadCount = 20; // not permission
        MessageBrain msgBrain;
        msgBrain._enableFastRecover = true;
        msgBrain._permissionCenter = &permission;
        MessageIdRequest request;
        MessageIdResponse response;
        ErrorCode ec = msgBrain.getMinMessageIdByTime(&request, &response, _timeoutChecker);
        ASSERT_EQ(ERROR_BROKER_BUSY, ec);

        PartitionId pid;
        config::PartitionConfig config;
        FileManager fileManager;
        msgBrain._messageCommitter = new MessageCommitter(pid, config, &fileManager, nullptr, true);
        ec = msgBrain.getMinMessageIdByTime(&request, &response, _timeoutChecker);
        ASSERT_EQ(ERROR_BROKER_BUSY, ec);

        msgBrain._messageCommitter->_hasSealError = true;
        ec = msgBrain.getMinMessageIdByTime(&request, &response, _timeoutChecker);
        EXPECT_EQ(ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND, ec);
    }
    { // 4. getMaxMessageId
        util::PermissionCenter permission;
        permission._readLimit = 10;
        permission._curReadCount = 20; // not permission
        MessageBrain msgBrain;
        msgBrain._enableFastRecover = true;
        msgBrain._permissionCenter = &permission;
        MessageIdRequest request;
        MessageIdResponse response;
        ErrorCode ec = msgBrain.getMaxMessageId(&request, &response);
        ASSERT_EQ(ERROR_BROKER_BUSY, ec);

        PartitionId pid;
        config::PartitionConfig config;
        FileManager fileManager;
        msgBrain._messageCommitter = new MessageCommitter(pid, config, &fileManager, nullptr, true);
        ec = msgBrain.getMaxMessageId(&request, &response);
        ASSERT_EQ(ERROR_BROKER_BUSY, ec);

        msgBrain._messageCommitter->_hasSealError = true;
        ec = msgBrain.getMaxMessageId(&request, &response);
        EXPECT_EQ(ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND, ec);
    }
}

TEST_F(MessageBrainTest, testGetPartitionInMetric) {
    MessageBrain msgBrain;
    ReadMetricsCollector collector(_topicName, _partitionId);
    config::PartitionConfig config;
    int64_t bufferSize = MEMORY_MESSAGE_META_SIZE * 8;
    config.setPartitionMinBufferSize(bufferSize);
    config.setPartitionMaxBufferSize(bufferSize);
    config.setBlockSize(bufferSize / 2);
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    ASSERT_EQ(ERROR_NONE,
              msgBrain.init(config, _centerPool, _fileCachePool, _permissionCenter, &_metricReporter, NULL));
    msgBrain._partitionId.set_topicname("topic_1");
    msgBrain._partitionId.set_id(1);
    msgBrain._messageGroup->_msgDeque->_lastAcceptedMsgTimeStamp = 2000000;
    msgBrain._lastReadTime = 3000000;
    msgBrain._metricStat.writeSize = 1002;
    msgBrain._metricStat.readSize = 1003;
    msgBrain._metricStat.writeRequestNum = 1004;
    msgBrain._metricStat.readRequestNum = 1005;
    PartitionInMetric pMetric;
    msgBrain.getPartitionInMetric(1, &pMetric);
    EXPECT_EQ("topic_1", pMetric.topicname());
    EXPECT_EQ(1, pMetric.partid());
    EXPECT_EQ(2, pMetric.lastwritetime());
    EXPECT_EQ(3, pMetric.lastreadtime());
    EXPECT_EQ(1002, pMetric.writerate1min());
    EXPECT_EQ(200, pMetric.writerate5min());
    EXPECT_EQ(1003, pMetric.readrate1min());
    EXPECT_EQ(200, pMetric.readrate5min());
    EXPECT_EQ(1004, pMetric.writerequest1min());
    EXPECT_EQ(200, pMetric.writerequest5min());
    EXPECT_EQ(1005, pMetric.readrequest1min());
    EXPECT_EQ(201, pMetric.readrequest5min());
    EXPECT_EQ(0, pMetric.commitdelay());

    FileManager *fileManagerHolder = msgBrain._fileManager;

    pMetric.clear_commitdelay();
    msgBrain._fileManager = nullptr;
    msgBrain.getPartitionInMetric(1, &pMetric);
    EXPECT_EQ(-1, pMetric.commitdelay());

    pMetric.clear_commitdelay();
    msgBrain._fileManager = fileManagerHolder;
    MessageDeque *dequeHandler = msgBrain._messageGroup->_msgDeque;
    MemoryMessage memMsg;
    memMsg.setTimestamp(2001);
    dequeHandler->reserve();
    dequeHandler->pushBack(memMsg);
    int64_t currentTime = TimeUtility::currentTime();
    msgBrain.getPartitionInMetric(1, &pMetric);
    EXPECT_THAT(pMetric.commitdelay(), Ge(currentTime - 2001));

    pMetric.clear_commitdelay();
    memMsg.setTimestamp(20002);
    dequeHandler->reserve();
    dequeHandler->pushBack(memMsg);
    dequeHandler->_committedIndex = 1;
    msgBrain.getPartitionInMetric(1, &pMetric);
    EXPECT_THAT(pMetric.commitdelay(), Ge(currentTime - 20002));

    pMetric.clear_commitdelay();
    dequeHandler->popFront();
    dequeHandler->_committedIndex = 1;
    msgBrain.getPartitionInMetric(1, &pMetric);
    EXPECT_EQ(pMetric.commitdelay(), 0);
}

} // namespace storage
} // namespace swift
