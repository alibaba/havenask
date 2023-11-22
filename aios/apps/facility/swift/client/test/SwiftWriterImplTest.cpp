#include "swift/client/SwiftWriterImpl.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "autil/HashFunctionBase.h"
#include "autil/LoopThread.h"
#include "autil/Thread.h"
#include "autil/ThreadPool.h"
#include "autil/TimeUtility.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/MessageWriteBuffer.h"
#include "swift/client/SingleSwiftWriter.h"
#include "swift/client/SwiftTransportClientCreator.h"
#include "swift/client/SwiftWriter.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/client/fake_client/FakeClientHelper.h"
#include "swift/client/fake_client/FakeSwiftAdminAdapter.h"
#include "swift/client/fake_client/FakeSwiftTransportClient.h"
#include "swift/client/fake_client/SwiftWriterImplMock.h"
#include "swift/common/Common.h"
#include "swift/common/MessageInfo.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/MessageInfoUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace swift::protocol;
using namespace swift::util;
using namespace swift::common;
using namespace swift::network;

namespace swift {
namespace client {

class SwiftWriterImplTest : public TESTBASE {
public:
    void setUp() {
        FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter;
        fakeAdapter->setPartCount(10);
        _topicInfo.set_partitioncount(10);
        _adapter.reset(fakeAdapter);
    }
    void tearDown() {
        SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = false;
        SwiftTransportClientCreator::_fakeDataMap.clear();
        _adapter.reset();
    }

private:
    void
    writeDataToPartition(SwiftWriterImplMock &writer, uint32_t dataSize, uint32_t partitionId, int64_t checkpointId);

    void getWriterMetric(SwiftWriterImpl *writer, int64_t time);
    void addSingleWriter(SwiftWriterImpl *wirter, int64_t time);
    void waitFinished(SwiftWriterImplMock *writer);
    void sendRequest(SwiftWriterImpl *writer);

private:
    SwiftAdminAdapterPtr _adapter;
    TopicInfo _topicInfo;
};

TEST_F(SwiftWriterImplTest, testInit) {
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    ErrorCode ec = ERROR_NONE;
    {
        SwiftWriterImpl writer(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), _topicInfo);
        EXPECT_EQ(-1, writer._topicVersion);
        SwiftWriterConfig config;
        config.topicName = "topic";
        ec = writer.init(config);
        EXPECT_EQ(ERROR_NONE, ec);
    }

    {
        fakeAdminAdapter->setErrorCode(ERROR_NONE);
        _topicInfo.set_modifytime(100);
        SwiftWriterImpl writer(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), _topicInfo);
        EXPECT_EQ(100, writer._topicVersion);
        SwiftWriterConfig config;
        config.topicName = "topic";
        config.functionChain = "wrong;";
        ec = writer.init(config);
        EXPECT_EQ(ERROR_CLIENT_INIT_FAILED, ec);
    }
}

TEST_F(SwiftWriterImplTest, testWrite) {
    SwiftWriterConfig config;
    config.topicName = "topic";
    config.oneRequestSendByteCount = 1;
    config.maxBufferSize = 100;
    config.maxKeepInBufferTime = 1000 * 1000;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 2));
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    EXPECT_EQ(ERROR_NONE, writer.init(config, blockPool));
    auto sendRequestThreadPtr = autil::LoopThread::createLoopThread(
        std::bind(&SwiftWriterImplTest::sendRequest, this, &writer), config.sendRequestLoopInterval, "send_loop");

    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo("data", 5, 0, 0, 1);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    usleep(200 * 1000);
    writer.getClient(1)->finishAsyncCall();
    EXPECT_EQ(ERROR_NONE, writer.waitFinished());
    usleep(200 * 1000);
    EXPECT_EQ(int64_t(5), writer.getCommittedCheckpointId());

    msgInfo = MessageInfoUtil::constructMsgInfo("data", 5, 0, 0, 100);
    EXPECT_EQ(ERROR_CLIENT_INVALID_PARTITIONID, writer.write(msgInfo));

    msgInfo = MessageInfoUtil::constructMsgInfo("data", 8, 0, 0, 1);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    usleep(200 * 1000);
    writer.getClient(1)->finishAsyncCall();
    EXPECT_EQ(ERROR_NONE, writer.waitFinished(10 * 1000 * 1000));
    usleep(200 * 1000);
    EXPECT_EQ(int64_t(8), writer.getCommittedCheckpointId());

    msgInfo = MessageInfoUtil::constructMsgInfo("data", 9, 0, 0, 1);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    usleep(200 * 1000);
    EXPECT_EQ(ERROR_CLIENT_SOME_MESSAGE_SEND_FAILED, writer.waitSent(1000 * 1000));
    writer.getClient(1)->finishAsyncCall();
    EXPECT_EQ(ERROR_NONE, writer.waitSent(1000 * 1000));
    usleep(200 * 1000);
    EXPECT_EQ(int64_t(9), writer.getCommittedCheckpointId());
}

TEST_F(SwiftWriterImplTest, testWriteCompress) {
    SwiftWriterConfig config;
    config.topicName = "topic";
    config.oneRequestSendByteCount = 1;
    config.maxBufferSize = 1000;
    config.compressMsg = true;
    config.compressThresholdInBytes = 50;
    config.maxKeepInBufferTime = 1000 * 1000;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 2));
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    EXPECT_EQ(ERROR_NONE, writer.init(config, blockPool));
    auto sendRequestThreadPtr = autil::LoopThread::createLoopThread(
        std::bind(&SwiftWriterImplTest::sendRequest, this, &writer), config.sendRequestLoopInterval, "send_loop");

    {
        string data;
        data.append(100, 'a');
        MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, 5, 0, 0, 1);
        EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
        usleep(200 * 1000);
        writer.getClient(1)->finishAsyncCall();
        EXPECT_EQ(ERROR_NONE, writer.waitFinished());
        usleep(200 * 1000);
        EXPECT_EQ(1, writer.getClient(1)->getMsgCount());
        protocol::Message message = (*(writer.getClient(1)->_messageVec))[0];
        ASSERT_TRUE(message.compress()); // compress in write
        ASSERT_TRUE(message.data().size() < 100);
    }
    {
        string data;
        data.append(49, 'a');
        MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, 5, 0, 0, 1);
        EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
        usleep(200 * 1000);
        writer.getClient(1)->finishAsyncCall();
        EXPECT_EQ(ERROR_NONE, writer.waitFinished());
        usleep(200 * 1000);
        EXPECT_EQ(2, writer.getClient(1)->getMsgCount());
        protocol::Message message = (*(writer.getClient(1)->_messageVec))[1];
        ASSERT_TRUE(!message.compress());
        ASSERT_TRUE(message.data().size() == 49);
    }
    {
        writer._config.compressMsg = false;
        string data;
        data.append(100, 'a');
        MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, 5, 0, 0, 1);
        EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
        usleep(200 * 1000);
        writer.getClient(1)->finishAsyncCall();
        EXPECT_EQ(ERROR_NONE, writer.waitFinished());
        usleep(200 * 1000);
        EXPECT_EQ(3, writer.getClient(1)->getMsgCount());
        protocol::Message message = (*(writer.getClient(1)->_messageVec))[2];
        ASSERT_TRUE(message.compress()); // compress in request
        ASSERT_TRUE(message.data().size() < 100);
    }
}

TEST_F(SwiftWriterImplTest, testWriteBatchMsg) {
    SwiftWriterConfig config;
    config.topicName = "topic";
    config.isSynchronous = false;
    config.safeMode = true;
    config.oneRequestSendByteCount = 1000;
    config.maxBufferSize = 1000;
    config.maxKeepInBufferTime = 1000 * 1000;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 2));
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    EXPECT_EQ(ERROR_NONE, writer.init(config, blockPool));
    auto sendRequestThreadPtr = autil::LoopThread::createLoopThread(
        std::bind(&SwiftWriterImplTest::sendRequest, this, &writer), config.sendRequestLoopInterval, "send_loop");

    vector<MessageInfo> msgInfoVec;
    msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data5", 5, 0, 0, 1));
    msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data6", 6, 0, 0, 1));
    msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data7", 7, 0, 0, 1));
    EXPECT_EQ(ERROR_NONE, writer.writes(msgInfoVec, false));
    usleep(2000 * 1000);
    writer.getClient(1)->finishAsyncCall();
    EXPECT_EQ(ERROR_NONE, writer.waitSent(1000 * 1000));
    EXPECT_EQ(int64_t(7), writer.getCommittedCheckpointId());

    msgInfoVec.clear();
    msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data8", 8, 0, 0, 100));
    EXPECT_EQ(ERROR_CLIENT_INVALID_PARTITIONID, writer.writes(msgInfoVec, false));

    msgInfoVec.clear();
    msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data8", 8, 0, 0, 1));
    msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data9", 9, 0, 0, 1));
    EXPECT_EQ(ERROR_CLIENT_SOME_MESSAGE_SEND_FAILED, writer.writes(msgInfoVec, true));
    usleep(2000 * 1000);
    writer.getClient(1)->finishAsyncCall();
    EXPECT_EQ(ERROR_NONE, writer.waitSent(1000 * 1000));
    EXPECT_EQ(int64_t(9), writer.getCommittedCheckpointId());

    EXPECT_EQ(ERROR_NONE, writer.writes(msgInfoVec, false));
    usleep(2000 * 1000);
    writer.getClient(1)->finishAsyncCall();
    EXPECT_EQ(ERROR_NONE, writer.waitSent(1000 * 1000));
    EXPECT_EQ(int64_t(9), writer.getCommittedCheckpointId());
}

TEST_F(SwiftWriterImplTest, testWaitFinishedMultiThread) {
    SwiftWriterConfig config;
    config.topicName = "news";
    config.oneRequestSendByteCount = 1200;
    config.maxBufferSize = 1000;
    config.maxKeepInBufferTime = 5000 * 1000;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 2));
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    EXPECT_EQ(ERROR_NONE, writer.init(config, blockPool));
    auto sendRequestThreadPtr = autil::LoopThread::createLoopThread(
        std::bind(&SwiftWriterImplTest::sendRequest, this, &writer), config.sendRequestLoopInterval, "send_loop");

    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo("data1", 5, 0, 0, 1);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    msgInfo = MessageInfoUtil::constructMsgInfo("data2", 6, 0, 0, 1);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));

    autil::ThreadPtr thread1 =
        autil::Thread::createThread(std::bind(&SwiftWriterImplTest::waitFinished, this, &writer));
    autil::ThreadPtr thread2 =
        autil::Thread::createThread(std::bind(&SwiftWriterImplTest::waitFinished, this, &writer));
    writer.getClient(1)->setAutoAsyncCall();
    writer.getClient(1)->finishAsyncCall();
    EXPECT_EQ(ERROR_NONE, writer.waitFinished());
    EXPECT_EQ(int64_t(6), writer.getCommittedCheckpointId());
    EXPECT_EQ((size_t)2, writer.getClient(1)->getMsgCount());
    thread1->join();
    thread2->join();
}

void SwiftWriterImplTest::waitFinished(SwiftWriterImplMock *writer) { writer->waitFinished(); }

void SwiftWriterImplTest::sendRequest(SwiftWriterImpl *writer) {
    bool hasSealError = false;
    writer->sendRequest(hasSealError);
}

TEST_F(SwiftWriterImplTest, testClearBuffer) {
    SwiftWriterConfig config;
    config.topicName = "news";
    config.oneRequestSendByteCount = 1200;
    config.maxBufferSize = 1000;
    config.maxKeepInBufferTime = 5000 * 1000;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 2));
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    EXPECT_EQ(ERROR_NONE, writer.init(config, blockPool));
    int64_t beg = 100;
    int64_t end = -1;
    EXPECT_FALSE(writer.clearBuffer(beg, end));
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo("data1", 2, 0, 0, 1);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    sendRequest(&writer);
    writer.getClient(1)->finishAsyncCall();
    EXPECT_TRUE(writer.clearBuffer(beg, end));
    ASSERT_EQ((int64_t)0, writer._blockPool->getUsedBlockCount());
    EXPECT_EQ(int64_t(1), beg);
    EXPECT_EQ(int64_t(2), end);
    msgInfo = MessageInfoUtil::constructMsgInfo("data2", 6, 0, 0, 1);
    writer.getClient(1)->_supportClientSafeMode = true;
    writer.getClient(1)->setAutoAsyncCall();
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    writer.setForceSend(true);
    sendRequest(&writer);
    sendRequest(&writer);
    EXPECT_EQ(ERROR_NONE, writer.waitFinished());
    EXPECT_EQ(int64_t(6), writer.getCommittedCheckpointId());
    EXPECT_EQ((size_t)1, writer.getClient(1)->getMsgCount());
    EXPECT_FALSE(writer.clearBuffer(beg, end));
}

TEST_F(SwiftWriterImplTest, testWaitFinishedSuccess) {
    SwiftWriterConfig config;
    config.topicName = "news";
    config.oneRequestSendByteCount = 1200;
    config.maxBufferSize = 1000;
    config.maxKeepInBufferTime = 5000 * 1000;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 2));
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    EXPECT_EQ(ERROR_NONE, writer.init(config, blockPool));
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo("data1", 5, 0, 0, 1);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    msgInfo = MessageInfoUtil::constructMsgInfo("data2", 6, 0, 0, 1);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    writer.getClient(1)->setAutoAsyncCall();
    sendRequest(&writer);
    EXPECT_EQ((size_t)0, writer.getClient(1)->getMsgCount());
    EXPECT_EQ(int64_t(4), writer.getCommittedCheckpointId());
    auto sendRequestThreadPtr = autil::LoopThread::createLoopThread(
        std::bind(&SwiftWriterImplTest::sendRequest, this, &writer), config.sendRequestLoopInterval, "send_loop");
    EXPECT_EQ(ERROR_NONE, writer.waitFinished());
    EXPECT_EQ(int64_t(6), writer.getCommittedCheckpointId());
    EXPECT_EQ((size_t)2, writer.getClient(1)->getMsgCount());
}

TEST_F(SwiftWriterImplTest, testWaitSentSuccess) {
    SwiftWriterConfig config;
    config.topicName = "news_1";
    config.oneRequestSendByteCount = 1200;
    config.maxBufferSize = 1000;
    config.maxKeepInBufferTime = 5000 * 1000; // keep in client 5s
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 2));
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    EXPECT_EQ(ERROR_NONE, writer.init(config, blockPool));
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo("data1", 5, 0, 0, 1);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    msgInfo = MessageInfoUtil::constructMsgInfo("data2", 6, 0, 0, 1);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    writer.getClient(1)->setAutoAsyncCall();
    sendRequest(&writer);
    EXPECT_EQ((size_t)0, writer.getClient(1)->getMsgCount());
    EXPECT_EQ(int64_t(4), writer.getCommittedCheckpointId());
    auto sendRequestThreadPtr = autil::LoopThread::createLoopThread(
        std::bind(&SwiftWriterImplTest::sendRequest, this, &writer), config.sendRequestLoopInterval, "send_loop");
    EXPECT_EQ(ERROR_NONE, writer.waitSent(1000 * 1000)); // send msg
    EXPECT_TRUE(writer.isAllSent());
    EXPECT_EQ((size_t)2, writer.getClient(1)->getMsgCount());
}

TEST_F(SwiftWriterImplTest, testMultiPartitionCheckpoint) {
    SwiftWriterConfig config;
    config.topicName = "news";
    config.oneRequestSendByteCount = 5;
    config.maxBufferSize = 10240;
    config.maxKeepInBufferTime = 1000 * 1000 * 1000;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 16));
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    EXPECT_EQ(ERROR_NONE, writer.init(config, blockPool));
    EXPECT_EQ(int64_t(-1), writer.getCommittedCheckpointId());

    writeDataToPartition(writer, 6, 0, 0);
    writer.getClient(0)->setAutoAsyncCall();
    sendRequest(&writer);
    EXPECT_EQ(size_t(1), writer.getClient(0)->_messageVec->size());
    sendRequest(&writer);
    EXPECT_EQ(int64_t(0), writer.getCommittedCheckpointId());

    writeDataToPartition(writer, 6, 1, 1);
    writer.getClient(1)->setAutoAsyncCall();
    sendRequest(&writer);
    EXPECT_EQ(size_t(1), writer.getClient(1)->_messageVec->size());
    sendRequest(&writer);
    EXPECT_EQ(int64_t(1), writer.getCommittedCheckpointId());

    writeDataToPartition(writer, 6, 2, 2);
    EXPECT_EQ(size_t(0), writer.getClient(2)->_messageVec->size());
    EXPECT_TRUE(!writer.getWriter(2)->isBufferEmpty());
    EXPECT_EQ(int64_t(1), writer.getCommittedCheckpointId());
    writer.getClient(2)->setAutoAsyncCall();
    sendRequest(&writer);
    EXPECT_EQ(size_t(1), writer.getClient(2)->_messageVec->size());
    sendRequest(&writer);
    EXPECT_TRUE(writer.getWriter(2)->isBufferEmpty());
    EXPECT_EQ(int64_t(2), writer.getCommittedCheckpointId());

    writeDataToPartition(writer, 3, 0, 3);
    writeDataToPartition(writer, 3, 1, 4);
    writeDataToPartition(writer, 3, 2, 5);
    writeDataToPartition(writer, 3, 0, 6);
    sendRequest(&writer);
    sendRequest(&writer);

    EXPECT_EQ(int64_t(3), writer.getCommittedCheckpointId());

    writeDataToPartition(writer, 3, 1, 7);
    sendRequest(&writer);
    sendRequest(&writer);
    EXPECT_EQ(int64_t(4), writer.getCommittedCheckpointId());

    writeDataToPartition(writer, 3, 2, 8);
    sendRequest(&writer);
    sendRequest(&writer);
    sendRequest(&writer);
    sendRequest(&writer);
    sendRequest(&writer);
    sendRequest(&writer);
    sendRequest(&writer);
    sendRequest(&writer);
    EXPECT_EQ(int64_t(8), writer.getCommittedCheckpointId());

    writeDataToPartition(writer, 3, 2, 9);
    sendRequest(&writer);
    EXPECT_EQ(int64_t(8), writer.getCommittedCheckpointId());
}

TEST_F(SwiftWriterImplTest, testWaitFinishedFailed) {
    SwiftWriterConfig config;
    config.topicName = "news";
    config.oneRequestSendByteCount = 5;
    config.maxBufferSize = 4096;
    config.maxKeepInBufferTime = 4 * 1000 * 1000;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 16));
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    EXPECT_EQ(ERROR_NONE, writer.init(config, blockPool));

    writeDataToPartition(writer, 3, 0, 0);
    writeDataToPartition(writer, 3, 1, 1);
    writeDataToPartition(writer, 3, 2, 2);

    EXPECT_EQ(ERROR_CLIENT_SOME_MESSAGE_SEND_FAILED, writer.waitFinished());
    EXPECT_EQ(int64_t(-1), writer.getCommittedCheckpointId());
}

void SwiftWriterImplTest::writeDataToPartition(SwiftWriterImplMock &writer,
                                               uint32_t dataSize,
                                               uint32_t partitionId,
                                               int64_t checkpointId) {
    string data(dataSize, 'a');
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, checkpointId, 0, 0, partitionId);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    writer.getClient(partitionId)->finishAsyncCall();
}

TEST_F(SwiftWriterImplTest, testInitProcessFuncs) {

    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_TRUE(writerImpl._processFuncs.empty());
    }
    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "update";
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(!ret);
    }

    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "hashId2partId,HASH";
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_EQ((size_t)2, writerImpl._processFuncs.size());
        EXPECT_TRUE(writerImpl._hashFunctionBasePtr);
    }

    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "hashId2partId,HASH,wrong";
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_EQ((size_t)2, writerImpl._processFuncs.size());
        EXPECT_TRUE(writerImpl._hashFunctionBasePtr);
    }

    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "hashId2partId,wrong,HASH";
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(!ret);
    }

    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "hashFunction:HASH,hashId2partId";
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_EQ((size_t)2, writerImpl._processFuncs.size());
        EXPECT_TRUE(writerImpl._hashFunctionBasePtr);
    }

    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "hashFunction:GALAXY_HASH,hashId2partId";
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_EQ((size_t)2, writerImpl._processFuncs.size());
        EXPECT_TRUE(writerImpl._hashFunctionBasePtr);
    }

    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "hashFunction:KINGSO_HASH,hashId2partId";
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_EQ((size_t)2, writerImpl._processFuncs.size());
        EXPECT_TRUE(writerImpl._hashFunctionBasePtr);
    }

    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "hashFunction:KINGSO_HASH#2,hashId2partId";
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_EQ((size_t)2, writerImpl._processFuncs.size());
        EXPECT_TRUE(writerImpl._hashFunctionBasePtr);
    }
    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "hashFunction:HASH,shuffleWithinPart,hashId2partId";
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_EQ((size_t)3, writerImpl._processFuncs.size());
        EXPECT_TRUE(writerImpl._hashFunctionBasePtr);
    }
    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "hashFunction:not_exist,hashId2partId";
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(!ret);
    }

    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "HASH64,hashIdAspartId";
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_EQ((size_t)2, writerImpl._processFuncs.size());
        EXPECT_TRUE(writerImpl._hashFunctionBasePtr);
        EXPECT_EQ((uint32_t)65536, writerImpl._hashFunctionBasePtr->getHashSize());
    }

    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "HASH64:0,hashId2partId";
        writerImpl._partitionCount = 2;
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_EQ((size_t)2, writerImpl._processFuncs.size());
        EXPECT_TRUE(writerImpl._hashFunctionBasePtr);
        EXPECT_EQ((uint32_t)2, writerImpl._hashFunctionBasePtr->getHashSize());
    }
    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "HASH64:1,hashId2partId";
        writerImpl._partitionCount = 2;
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_EQ((size_t)2, writerImpl._processFuncs.size());
        EXPECT_TRUE(writerImpl._hashFunctionBasePtr);
        EXPECT_EQ((uint32_t)1, writerImpl._hashFunctionBasePtr->getHashSize());
    }

    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "HASH64:100,hashId2partId";
        writerImpl._partitionCount = 2;
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_EQ((size_t)2, writerImpl._processFuncs.size());
        EXPECT_TRUE(writerImpl._hashFunctionBasePtr);
        EXPECT_EQ((uint32_t)2, writerImpl._hashFunctionBasePtr->getHashSize());
    }

    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "HASH64:,hashIdAspartId";
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_EQ((size_t)2, writerImpl._processFuncs.size());
        EXPECT_TRUE(writerImpl._hashFunctionBasePtr);
        EXPECT_EQ((uint32_t)65536, writerImpl._hashFunctionBasePtr->getHashSize());
    }
    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.topicName = "news";
        writerImpl._config.functionChain = "HASH64:,HASH:3,hashIdAspartId";
        writerImpl._partitionCount = 4;
        bool ret = writerImpl.initProcessFuncs();
        EXPECT_TRUE(ret);
        EXPECT_EQ((size_t)2, writerImpl._processFuncs.size());
        EXPECT_TRUE(writerImpl._hashFunctionBasePtr);
        EXPECT_EQ((uint32_t)65536, writerImpl._hashFunctionBasePtr->getHashSize());
    }
}

/*
TEST_F(SwiftWriterImplTest, testBuffSizeStatictics) {
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    SwiftWriterConfig config;
    config.topicName = "news";
    config.oneRequestSendByteCount = 1;
    config.maxBufferSize = 150;
    config.maxKeepInBufferTime = 1000 * 1000;
    EXPECT_EQ(ERROR_NONE , writer.init(config));

    uint32_t dataSize = 40;
    uint32_t partitionId = 0;
    int64_t checkpointId = 1;
    string data(dataSize, 'a');
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, checkpointId, 0, 0, partitionId);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    EXPECT_EQ((int64_t)dataSize + sizeof(MessageInfo), writer._sizeStatistics->getSize());
    msgInfo.pid = 1;
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    EXPECT_EQ((int64_t)(dataSize + sizeof(MessageInfo)) * 2,
              writer._sizeStatistics->getSize());
    msgInfo.pid = 2;
    EXPECT_EQ(ERROR_CLIENT_SEND_BUFFER_FULL, writer.write(msgInfo));

    writer.getClient(0)->setAutoAsyncCall();
    writer.getClient(0)->finishAsyncCall();
    usleep(250*1000);                      // wait background thread send request
    EXPECT_EQ((int64_t)dataSize + sizeof(MessageInfo), writer._sizeStatistics->getSize());
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    EXPECT_EQ((int64_t)(dataSize + sizeof(MessageInfo)) * 2,
              writer._sizeStatistics->getSize());

    writer.getClient(1)->setAutoAsyncCall();
    writer.getClient(1)->finishAsyncCall();
    usleep(250*1000);                      // wait background thread send request
    EXPECT_EQ((int64_t)dataSize + sizeof(MessageInfo), writer._sizeStatistics->getSize());

    writer.getClient(2)->setAutoAsyncCall();
    writer.getClient(2)->finishAsyncCall();
    usleep(250*1000);                      // wait background thread send request
    EXPECT_EQ((int64_t)0, writer._sizeStatistics->getSize());
}

*/
TEST_F(SwiftWriterImplTest, testGetCheckPointIdBeforeSendMsg) {
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    bool hasSealError = false;
    writer.sendRequest(hasSealError);
    EXPECT_EQ(int64_t(-1), writer.getCommittedCheckpointId());
}

TEST_F(SwiftWriterImplTest, testValidateMessageInfo) {
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    MessageInfo msgInfo;
    writer._partitionCount = 10;
    EXPECT_EQ(ERROR_NONE, writer.validateMessageInfo(msgInfo));

    msgInfo.checkpointId = -100;
    EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, writer.validateMessageInfo(msgInfo));
    EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, writer.write(msgInfo));

    msgInfo.checkpointId = 2;
    msgInfo.pid = 100;
    EXPECT_EQ(ERROR_CLIENT_INVALID_PARTITIONID, writer.validateMessageInfo(msgInfo));

    msgInfo.pid = 5;
    EXPECT_EQ(ERROR_NONE, writer.validateMessageInfo(msgInfo));
    writer._versionControlPid = 4;
    EXPECT_EQ(ERROR_CLIENT_INVALID_PARTITIONID, writer.validateMessageInfo(msgInfo));
}

TEST_F(SwiftWriterImplTest, testProcessMsgInfo) {
    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.functionChain = "HASH64:1,hashIdAspartId";
        writerImpl._partitionCount = 1;
        EXPECT_TRUE(writerImpl.initProcessFuncs());
        MessageInfo messageInfo;
        messageInfo.hashStr = "";
        messageInfo.pid = 1000;
        EXPECT_EQ(ERROR_NONE, writerImpl.processMsgInfo(messageInfo));
        EXPECT_EQ(uint32_t(0), messageInfo.pid);
    }
    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.functionChain = "";
        writerImpl._partitionCount = 1;
        EXPECT_TRUE(writerImpl.initProcessFuncs());
        MessageInfo messageInfo;
        messageInfo.hashStr = "";
        messageInfo.pid = 1000;
        EXPECT_EQ(ERROR_NONE, writerImpl.processMsgInfo(messageInfo));
        EXPECT_EQ(uint32_t(1000), messageInfo.pid);
    }
    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.functionChain = "HASH64:1,hashIdAspartId";
        writerImpl._partitionCount = 1;
        EXPECT_TRUE(writerImpl.initProcessFuncs());
        MessageInfo messageInfo;
        messageInfo.hashStr = "abc";
        messageInfo.pid = 1000;
        EXPECT_EQ(ERROR_NONE, writerImpl.processMsgInfo(messageInfo));
        EXPECT_EQ(uint32_t(0), messageInfo.pid);
    }
    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.functionChain = "HASH,hashIdAspartId";
        writerImpl._partitionCount = 1;
        EXPECT_TRUE(writerImpl.initProcessFuncs());
        MessageInfo messageInfo;
        messageInfo.hashStr = "abc";
        messageInfo.pid = 1000;
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, writerImpl.processMsgInfo(messageInfo));
        EXPECT_EQ(uint32_t(1000), messageInfo.pid);
    }
    {
        SwiftWriterConfig config;
        config.functionChain = "HASH,hashId2partId";
        SwiftWriterImplMock writerImpl1(_adapter, _topicInfo);
        ASSERT_EQ(ERROR_NONE, writerImpl1.init(config));
        config.functionChain = "HASH,shuffleWithinPart,hashId2partId";
        SwiftWriterImplMock writerImpl2(_adapter, _topicInfo);
        ASSERT_EQ(ERROR_NONE, writerImpl2.init(config));
        MessageInfo messageInfo1;
        messageInfo1.hashStr = "abcd";
        messageInfo1.pid = 1000;
        MessageInfo messageInfo2;
        messageInfo2.hashStr = "abcd";
        messageInfo2.pid = 1000;

        EXPECT_EQ(ERROR_NONE, writerImpl1.processMsgInfo(messageInfo1));
        EXPECT_EQ(ERROR_NONE, writerImpl2.processMsgInfo(messageInfo2));
        EXPECT_NE(1000, messageInfo1.pid);
        EXPECT_NE(1000, messageInfo2.pid);
        EXPECT_EQ(messageInfo1.pid, messageInfo2.pid);
        EXPECT_NE(messageInfo1.uint16Payload, messageInfo2.uint16Payload);
        auto payload = messageInfo2.uint16Payload;
        EXPECT_EQ(ERROR_NONE, writerImpl2.processMsgInfo(messageInfo2));
        EXPECT_NE(payload, messageInfo2.uint16Payload);
    }
}

TEST_F(SwiftWriterImplTest, testGetPartitionIdByHashStr) {
    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.functionChain = "HASH64:1,hashIdAspartId";
        writerImpl._partitionCount = 1;
        EXPECT_TRUE(writerImpl.initProcessFuncs());
        string hashStr = "abc";
        pair<int32_t, uint16_t> info = writerImpl.getPartitionIdByHashStr("", "", hashStr);
        EXPECT_EQ(int32_t(0), info.first);
        EXPECT_EQ(uint16_t(0), info.second);
    }
    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.functionChain = "HASH,hashIdAspartId";
        writerImpl._partitionCount = 1;
        EXPECT_TRUE(writerImpl.initProcessFuncs());
        string hashStr = "abc";
        pair<int32_t, uint16_t> info = writerImpl.getPartitionIdByHashStr("", "", hashStr);
        EXPECT_EQ((int32_t)-1, info.first);
    }
    {
        SwiftWriterImplMock writerImpl(_adapter, _topicInfo);
        writerImpl._config.functionChain = "";
        writerImpl._partitionCount = 1;
        EXPECT_TRUE(writerImpl.initProcessFuncs());
        string hashStr = "abc";
        pair<int32_t, uint16_t> info = writerImpl.getPartitionIdByHashStr("", "", hashStr);
        EXPECT_EQ((int32_t)-1, info.first);
    }
    {
        SwiftWriterImpl writerImpl("HASH,hashId2partId", 2);
        EXPECT_TRUE(writerImpl.initProcessFuncs());
        string hashStr("abc");
        const pair<int32_t, uint16_t> &info = writerImpl.getPartitionIdByHashStr(hashStr);
        EXPECT_EQ(1, info.first);
        EXPECT_EQ(50922, info.second);
    }
    {
        SwiftWriterImpl writerImpl("HASH,hashId2partId", 10);
        EXPECT_TRUE(writerImpl.initProcessFuncs());
        string hashStr("abc");
        const pair<int32_t, uint16_t> &info = writerImpl.getPartitionIdByHashStr(hashStr);
        EXPECT_EQ(7, info.first);
        EXPECT_EQ(50922, info.second);
    }
}

TEST_F(SwiftWriterImplTest, testIsSyncWriter) {
    {
        SwiftWriterImplMock writer(_adapter, _topicInfo);
        SwiftWriterConfig config;
        config.isSynchronous = true;
        config.topicName = "news";
        EXPECT_EQ(ERROR_NONE, writer.init(config));
        auto sendRequestThreadPtr = autil::LoopThread::createLoopThread(
            std::bind(&SwiftWriterImplTest::sendRequest, this, &writer), config.sendRequestLoopInterval, "send_loop");

        EXPECT_TRUE(writer.isSyncWriter());
    }
    {
        SwiftWriterImplMock writer(_adapter, _topicInfo);
        SwiftWriterConfig config;
        config.topicName = "news";
        config.isSynchronous = false;
        EXPECT_EQ(ERROR_NONE, writer.init(config));
        auto sendRequestThreadPtr = autil::LoopThread::createLoopThread(
            std::bind(&SwiftWriterImplTest::sendRequest, this, &writer), config.sendRequestLoopInterval, "send_loop");

        EXPECT_TRUE(!writer.isSyncWriter());
    }
}

TEST_F(SwiftWriterImplTest, testThreadSafe_bug_269918) {
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    BlockPoolPtr blockPool(new BlockPool(204800, 1024));
    SwiftWriterImpl writer(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), _topicInfo);
    writer._blockPool = blockPool;
    uint64_t time = 10 * 1000 * 1000;
    autil::ThreadPtr addThread =
        autil::Thread::createThread(std::bind(&SwiftWriterImplTest::addSingleWriter, this, &writer, time));
    autil::ThreadPtr getThread =
        autil::Thread::createThread(std::bind(&SwiftWriterImplTest::getWriterMetric, this, &writer, time));
    addThread->join();
    getThread->join();
}

TEST_F(SwiftWriterImplTest, testTopicChanged) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    fakeAdminAdapter->setPartCount(2);
    TopicInfo topicInfo;
    topicInfo.set_partitioncount(2);
    SwiftWriterImpl writer(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), topicInfo);
    ErrorCode ec = ERROR_NONE;
    SwiftWriterConfig config;
    config.topicName = "topic";
    config.oneRequestSendByteCount = 1;
    config.isSynchronous = true;
    config.safeMode = false;
    ec = writer.init(config);
    EXPECT_EQ(ERROR_NONE, ec);
    ASSERT_TRUE(writer._rangeUtil != NULL);
    ASSERT_EQ(uint32_t(2), writer.getPartitionCount());
    ASSERT_EQ(uint32_t(0), writer._writers.size());
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo("data", 1, 2, 3, 0); // pid = 0
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    msgInfo = MessageInfoUtil::constructMsgInfo("data", 1, 2, 3, 1);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    msgInfo = MessageInfoUtil::constructMsgInfo("data", 1, 2, 3, 2); // pid = 2
    EXPECT_EQ(ERROR_CLIENT_INVALID_PARTITIONID, writer.write(msgInfo));
    ASSERT_EQ(uint32_t(2), writer._writers.size());
    ASSERT_TRUE(writer._writers.find(1) != writer._writers.end());
    ASSERT_TRUE(writer._writers.find(2) == writer._writers.end());
    FakeSwiftTransportClient *fakeClient0 = FakeClientHelper::getTransportClient(&writer, 0);
    FakeSwiftTransportClient *fakeClient1 = FakeClientHelper::getTransportClient(&writer, 1);
    ASSERT_EQ(1, fakeClient0->_messageVec->size());
    ASSERT_EQ(1, fakeClient1->_messageVec->size());
    fakeClient1->setErrorCode(ERROR_BROKER_SESSION_CHANGED); // to reconnect
    // inc partitionCount
    fakeAdminAdapter->setPartCount(4);

    TopicInfoResponse response;
    msgInfo = MessageInfoUtil::constructMsgInfo("data", 1, 2, 3, 1);
    ec = writer.write(msgInfo); // has error
    fakeAdminAdapter->getTopicInfo(config.topicName, response, 0, {});
    writer.doTopicChanged(response.topicinfo().partitioncount(), response.topicinfo().rangecountinpartition());

    ASSERT_EQ(ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND, ec);
    fakeClient1->setErrorCode(ERROR_NONE);
    ec = writer.write(msgInfo);
    ASSERT_EQ(ERROR_NONE, ec);

    fakeClient0 = FakeClientHelper::getTransportClient(&writer, 0);
    fakeClient1 = FakeClientHelper::getTransportClient(&writer, 1);
    ASSERT_EQ(1, fakeClient0->_messageVec->size());
    ASSERT_EQ(2, fakeClient1->_messageVec->size()); // address not changed

    msgInfo = MessageInfoUtil::constructMsgInfo("data", 1, 2, 3, 2); // pid = 2
    ASSERT_EQ(ERROR_NONE, writer.write(msgInfo));

    ASSERT_TRUE(writer._rangeUtil != NULL);
    ASSERT_EQ(uint32_t(4), writer.getPartitionCount());
    ASSERT_EQ(uint32_t(3), writer._writers.size());
    msgInfo = MessageInfoUtil::constructMsgInfo("data", 1, 2, 3, 3); // pid=3
    ASSERT_EQ(ERROR_NONE, writer.write(msgInfo));
    ASSERT_EQ(uint32_t(4), writer._writers.size());
    ASSERT_TRUE(writer._writers.find(3) != writer._writers.end());
    ASSERT_FALSE(writer._topicChanged);

    // dec partitionCount
    fakeAdminAdapter->setPartCount(3);
    fakeClient1 = FakeClientHelper::getTransportClient(&writer, 1);
    fakeClient1->setErrorCode(ERROR_BROKER_SESSION_CHANGED);         // to reconnect
    msgInfo = MessageInfoUtil::constructMsgInfo("data", 1, 2, 3, 1); // pid=1

    ec = writer.write(msgInfo);
    fakeAdminAdapter->getTopicInfo(config.topicName, response, 0, {});
    writer.doTopicChanged(response.topicinfo().partitioncount(), response.topicinfo().rangecountinpartition());

    ASSERT_EQ(ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND, ec);
    fakeClient1->setErrorCode(ERROR_NONE);
    ec = writer.write(msgInfo);
    ASSERT_EQ(ERROR_NONE, ec);

    ASSERT_TRUE(writer._rangeUtil != NULL);
    ASSERT_EQ(uint32_t(3), writer._partitionCount);

    fakeClient0 = FakeClientHelper::getTransportClient(&writer, 0);
    fakeClient1 = FakeClientHelper::getTransportClient(&writer, 1);
    FakeSwiftTransportClient *fakeClient2 = FakeClientHelper::getTransportClient(&writer, 2);
    ASSERT_EQ(1, fakeClient0->_messageVec->size());
    ASSERT_EQ(3, fakeClient1->_messageVec->size());
    ASSERT_EQ(1, fakeClient2->_messageVec->size());

    msgInfo = MessageInfoUtil::constructMsgInfo("data", 1, 2, 3, 3);
    EXPECT_EQ(ERROR_CLIENT_INVALID_PARTITIONID, writer.write(msgInfo));
    ASSERT_EQ(uint32_t(3), writer._writers.size());
    ASSERT_FALSE(writer._topicChanged);
}

TEST_F(SwiftWriterImplTest, testStealUnsendMessage) {
    {
        SwiftWriterConfig config;
        config.topicName = "topic";
        config.isSynchronous = false;
        config.safeMode = true;
        config.oneRequestSendByteCount = 1000;
        config.maxBufferSize = 1000;
        config.maxKeepInBufferTime = 1000 * 1000;
        BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 2));
        _topicInfo.set_partitioncount(3);
        SwiftWriterImplMock writer(_adapter, _topicInfo);
        EXPECT_EQ(ERROR_NONE, writer.init(config, blockPool));
        auto sendRequestThreadPtr = autil::LoopThread::createLoopThread(
            std::bind(&SwiftWriterImplTest::sendRequest, this, &writer), config.sendRequestLoopInterval, "send_loop");

        vector<MessageInfo> msgInfoVec;
        msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data1", 1, 0, 0, 1));
        msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data2", 2, 0, 0, 1));
        msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data3", 3, 0, 0, 1));
        msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data4", 4, 0, 0, 2));
        msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data5", 5, 0, 0, 2));
        EXPECT_EQ(ERROR_NONE, writer.writes(msgInfoVec, false));
        vector<MessageInfo> unsendMsgInfoVec;
        EXPECT_TRUE(writer.stealUnsendMessage(unsendMsgInfoVec));
        ASSERT_EQ(5, unsendMsgInfoVec.size());
        for (int i = 0; i < 5; i++) {
            EXPECT_EQ("data" + to_string(i + 1), unsendMsgInfoVec[i].data);
            EXPECT_EQ(0, unsendMsgInfoVec[i].uint16Payload);
        }
    }
    {
        SwiftWriterConfig config;
        config.topicName = "topic";
        config.isSynchronous = false;
        config.safeMode = true;
        config.oneRequestSendByteCount = 1000;
        config.maxBufferSize = 1000;
        config.maxKeepInBufferTime = 1000 * 1000;
        BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 2));
        SwiftWriterImplMock writer(_adapter, _topicInfo);
        EXPECT_EQ(ERROR_NONE, writer.init(config, blockPool));
        auto sendRequestThreadPtr = autil::LoopThread::createLoopThread(
            std::bind(&SwiftWriterImplTest::sendRequest, this, &writer), config.sendRequestLoopInterval, "send_loop");

        vector<MessageInfo> msgInfoVec;
        msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data5", 5, 0, 0, 1));
        msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data6", 6, 0, 0, 1));
        msgInfoVec.push_back(MessageInfoUtil::constructMsgInfo("data7", 7, 0, 0, 1));
        EXPECT_EQ(ERROR_NONE, writer.writes(msgInfoVec, false));
        usleep(2000 * 1000);
        writer.getClient(1)->finishAsyncCall();
        EXPECT_EQ(ERROR_NONE, writer.waitSent(1000 * 1000));
        EXPECT_EQ(int64_t(7), writer.getCommittedCheckpointId());

        vector<MessageInfo> unsendMsgInfoVec;
        EXPECT_TRUE(writer.stealUnsendMessage(unsendMsgInfoVec));
        ASSERT_EQ(0, unsendMsgInfoVec.size());
    }
}

TEST_F(SwiftWriterImplTest, testImportMsg) {
    SwiftWriterConfig config;
    config.topicName = "topic";
    config.isSynchronous = false;
    config.safeMode = true;
    config.oneRequestSendByteCount = 1000;
    config.maxBufferSize = 1000;
    config.maxKeepInBufferTime = 1000 * 1000;
    config.functionChain = "hashId2partId,HASH";
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 2));
    _topicInfo.set_partitioncount(3);
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    EXPECT_EQ(ERROR_NONE, writer.init(config, blockPool));
    auto sendRequestThreadPtr = autil::LoopThread::createLoopThread(
        std::bind(&SwiftWriterImplTest::sendRequest, this, &writer), config.sendRequestLoopInterval, "send_loop");

    vector<MessageInfo> msgInfoVec;
    MessageInfo msg1 = MessageInfoUtil::constructMsgInfo("data1", 1, 0, 0, 0);
    MessageInfo msg2 = MessageInfoUtil::constructMsgInfo("data2", 2, 1, 0, 0);
    MessageInfo msg3 = MessageInfoUtil::constructMsgInfo("data3", 3, 30000, 0, 0);
    MessageInfo msg4 = MessageInfoUtil::constructMsgInfo("data4", 4, 35000, 0, 0);
    MessageInfo msg5 = MessageInfoUtil::constructMsgInfo("data5", 5, 50000, 0, 0);
    EXPECT_EQ(ERROR_NONE, writer.importMsg(msg1));
    EXPECT_EQ(ERROR_NONE, writer.importMsg(msg2));
    EXPECT_EQ(ERROR_NONE, writer.importMsg(msg3));
    EXPECT_EQ(ERROR_NONE, writer.importMsg(msg4));
    EXPECT_EQ(ERROR_NONE, writer.importMsg(msg5));
    EXPECT_EQ(3, writer._writers.size());
    EXPECT_EQ(2, writer._writers[0]->_writeBuffer.getUnsendCount());
    EXPECT_EQ(2, writer._writers[1]->_writeBuffer.getUnsendCount());
    EXPECT_EQ(1, writer._writers[2]->_writeBuffer.getUnsendCount());

    MessageInfo msg6 = MessageInfoUtil::constructMsgInfo("data6", 6, 60000, 0, 0);
    EXPECT_EQ(ERROR_NONE, writer.importMsg(msg6));
    EXPECT_EQ(3, writer._writers.size());
    EXPECT_EQ(2, writer._writers[0]->_writeBuffer.getUnsendCount());
    EXPECT_EQ(2, writer._writers[1]->_writeBuffer.getUnsendCount());
    EXPECT_EQ(2, writer._writers[2]->_writeBuffer.getUnsendCount());
    // check checkpoint
    EXPECT_EQ(2, writer._writers[0]->_writeBuffer._writeQueue.size());
    EXPECT_EQ(1, writer._writers[0]->_writeBuffer._writeQueue[0]->checkpointId);
    EXPECT_EQ(2, writer._writers[0]->_writeBuffer._writeQueue[1]->checkpointId);
    EXPECT_EQ(2, writer._writers[1]->_writeBuffer._writeQueue.size());
    EXPECT_EQ(3, writer._writers[1]->_writeBuffer._writeQueue[0]->checkpointId);
    EXPECT_EQ(4, writer._writers[1]->_writeBuffer._writeQueue[1]->checkpointId);
    EXPECT_EQ(2, writer._writers[2]->_writeBuffer._writeQueue.size());
    EXPECT_EQ(5, writer._writers[2]->_writeBuffer._writeQueue[0]->checkpointId);
    EXPECT_EQ(6, writer._writers[2]->_writeBuffer._writeQueue[1]->checkpointId);
}

TEST_F(SwiftWriterImplTest, testInsertSingleWriter) {
    SwiftWriterConfig config;
    config.topicName = "topic";
    config.functionChain = "hashId2partId,HASH";
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 2));
    _topicInfo.set_partitioncount(2);
    SwiftWriterImplMock writer(_adapter, _topicInfo);
    EXPECT_EQ(ERROR_NONE, writer.init(config, blockPool));
    EXPECT_TRUE(nullptr != writer.insertSingleWriter(0));
    EXPECT_TRUE(nullptr != writer.insertSingleWriter(1));
    EXPECT_TRUE(nullptr != writer.insertSingleWriter(2));
}

void SwiftWriterImplTest::addSingleWriter(SwiftWriterImpl *writer, int64_t time) {
    uint64_t count = 1;
    int64_t begTime = autil::TimeUtility::currentTime();
    while (true) {
        int64_t now = autil::TimeUtility::currentTime();
        if (now - begTime > time) {
            break;
        }
        if (count % 100 == 0) {
            writer->clear();
        }
        writer->insertSingleWriter(count);
        count++;
    }
}

void SwiftWriterImplTest::getWriterMetric(SwiftWriterImpl *writer, int64_t time) {
    WriterMetrics writerMetric;
    uint64_t count = 1;
    int64_t begTime = autil::TimeUtility::currentTime();
    while (true) {
        if (count % 1000 == 0) {
            int64_t now = autil::TimeUtility::currentTime();
            if (now - begTime > time) {
                break;
            }
        }
        writer->getWriterMetrics(writer->_config.zkPath, writer->_config.topicName, writerMetric);
        count++;
    }
}

} // namespace client
} // namespace swift
