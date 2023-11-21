#include "swift/client/SingleSwiftWriter.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <ext/alloc_traits.h>
#include <functional>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Thread.h"
#include "autil/ThreadPool.h"
#include "autil/TimeUtility.h"
#include "kmonitor/client/MetricsReporter.h"
#include "swift/client/BufferSizeLimiter.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/MessageWriteBuffer.h"
#include "swift/client/SwiftTransportAdapter.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/client/TransportClosure.h"
#include "swift/client/fake_client/FakeSwiftTransportAdapter.h"
#include "swift/client/fake_client/FakeSwiftTransportClient.h"
#include "swift/common/Common.h"
#include "swift/common/MemoryMessage.h"
#include "swift/monitor/ClientMetricsReporter.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/MessageInfoUtil.h"
#include "swift/util/MessageQueue.h"
#include "unittest/unittest.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace autil;

using namespace swift::protocol;
using namespace swift::util;
using namespace swift::common;
using namespace swift::network;
using namespace swift::monitor;
using ::testing::Field;

namespace swift {
namespace monitor {
class MockClientMetricsReporter : public ClientMetricsReporter {
public:
    MockClientMetricsReporter(const kmonitor::MetricsReporterPtr &reporter) : ClientMetricsReporter(reporter) {}
    MOCK_METHOD2(reportWriterMetrics, void(ClientMetricsCollector &collector, const kmonitor::MetricsTags *tags));
    MOCK_METHOD2(reportReaderMetrics, void(ClientMetricsCollector &collector, const kmonitor::MetricsTags *tags));
    MOCK_METHOD1(reportCommitCallbackQps, void(const kmonitor::MetricsTags *tags));
    MOCK_METHOD2(reportDelay, void(ReaderDelayCollector &collector, const kmonitor::MetricsTags *tags));
};

void expect_call_writer(MockClientMetricsReporter &reporter, ClientMetricsCollector &expectCollector) {
    EXPECT_CALL(
        reporter,
        reportWriterMetrics(AllOf(Field(&ClientMetricsCollector::rpcLatency, expectCollector.rpcLatency),
                                  Field(&ClientMetricsCollector::decompressLatency, expectCollector.decompressLatency),
                                  Field(&ClientMetricsCollector::unsendCount, expectCollector.unsendCount),
                                  Field(&ClientMetricsCollector::uncommitCount, expectCollector.uncommitCount),
                                  Field(&ClientMetricsCollector::requestMsgCount, expectCollector.requestMsgCount),
                                  Field(&ClientMetricsCollector::errorCallBack, expectCollector.errorCallBack),
                                  Field(&ClientMetricsCollector::hasError, expectCollector.hasError),
                                  Field(&ClientMetricsCollector::isDetectCommit, expectCollector.isDetectCommit)),
                            _))
        .Times(1);
}
} // namespace monitor

namespace client {

class SingleSwiftWriterTest : public TESTBASE {
protected:
    void doSendRequest(SingleSwiftWriter *writer, int32_t msgCount);
    void innerTestSendRequestCompress(uint64_t threshold, size_t msgSize, bool result);

private:
    ThreadPoolPtr _threadPool;
};

TEST_F(SingleSwiftWriterTest, testSendRequestMetricsReport) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.topicName = topicName;
    config.oneRequestSendByteCount = 1024;
    config.maxBufferSize = 2048;
    config.maxKeepInBufferTime = 1000 * 1000 * 1000 * 1000L; // 1000s, never happen
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    kmonitor::MetricsReporterPtr kmonReporter(nullptr);
    MockClientMetricsReporter reporter(kmonReporter);
    SingleSwiftWriter writer(SwiftAdminAdapterPtr(),
                             SwiftRpcChannelManagerPtr(),
                             partitionId,
                             config,
                             blockPool,
                             _threadPool,
                             BufferSizeLimiterPtr(),
                             (ClientMetricsReporter *)(&reporter));
    FakeSwiftTransportAdapter<TRT_SENDMESSAGE> *adapter =
        new FakeSwiftTransportAdapter<TRT_SENDMESSAGE>(topicName, partitionId);
    DELETE_AND_SET_NULL(writer._transportAdapter);
    writer._transportAdapter = adapter;
    FakeSwiftTransportClient *fakeClient = adapter->getFakeTransportClient();
    fakeClient->setTimeoutCount(0);
    fakeClient->setAutoAsyncCall();
    fakeClient->_supportClientSafeMode = true;
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo("data", 1);
    bool status = writer._writeBuffer.addWriteMessage(msgInfo);
    ASSERT_EQ(true, status);

    ClientMetricsCollector expectCollector;
    expectCollector.rpcLatency = -1;
    expectCollector.decompressLatency = -1;
    expectCollector.unsendCount = 1;
    expectCollector.uncommitCount = 0;
    expectCollector.requestMsgCount = 1;
    expectCollector.errorCallBack = false;
    expectCollector.hasError = false;
    expectCollector.isDetectCommit = false;
    expect_call_writer(reporter, expectCollector);
    writer.sendRequest(1, true);

    adapter->_closure->_response->set_committedid(1);
    adapter->_closure->_response->set_acceptedmsgbeginid(2);
    expectCollector.rpcLatency = writer._transportAdapter->_closure->getRpcLatency();
    expectCollector.decompressLatency = -1;
    expectCollector.unsendCount = 0;
    expectCollector.uncommitCount = 1;
    expectCollector.requestMsgCount = 0;
    expectCollector.errorCallBack = false;
    expectCollector.hasError = false;
    expectCollector.isDetectCommit = true;
    expect_call_writer(reporter, expectCollector);
    writer.sendRequest(2 + config.commitDetectionInterval, true);

    adapter->_closure->_response->set_committedid(2);
    adapter->_closure->_response->set_acceptedmsgbeginid(2);
    msgInfo = MessageInfoUtil::constructMsgInfo("data", 1);
    status = writer._writeBuffer.addWriteMessage(msgInfo);
    ASSERT_EQ(true, status);
    expectCollector.rpcLatency = writer._transportAdapter->_closure->getRpcLatency();
    expectCollector.decompressLatency = -1;
    expectCollector.unsendCount = 1;
    expectCollector.uncommitCount = 0;
    expectCollector.requestMsgCount = 1;
    expectCollector.errorCallBack = false;
    expectCollector.hasError = false;
    expectCollector.isDetectCommit = false;
    expect_call_writer(reporter, expectCollector);
    writer.sendRequest(2, true);

    msgInfo = MessageInfoUtil::constructMsgInfo("data", 1);
    status = writer._writeBuffer.addWriteMessage(msgInfo);
    ASSERT_EQ(true, status);
    auto _func = [](const std::vector<std::pair<int64_t, int64_t>> &) {};
    writer.setCommittedCallBack(_func);
    expectCollector.rpcLatency = writer._transportAdapter->_closure->getRpcLatency();
    expectCollector.decompressLatency = -1;
    expectCollector.unsendCount = 1;
    expectCollector.uncommitCount = 0;
    expectCollector.requestMsgCount = 1;
    EXPECT_CALL(reporter, reportCommitCallbackQps(_)).Times(1);
    expect_call_writer(reporter, expectCollector);
    writer.sendRequest(3, true);

    writer.setCommittedCallBack(nullptr);
    writer._hasVersionError = true;
    auto errorFunc = [](const ErrorCode &ec) -> void {};
    writer.setErrorCallBack(errorFunc);
    expectCollector.rpcLatency = -1;
    expectCollector.decompressLatency = -1;
    expectCollector.unsendCount = 0;
    expectCollector.uncommitCount = 0;
    expectCollector.requestMsgCount = -1;
    expectCollector.errorCallBack = true;
    expectCollector.hasError = false;
    expectCollector.isDetectCommit = false;
    expect_call_writer(reporter, expectCollector);
    writer.sendRequest(4, true);

    writer._hasVersionError = false;
    msgInfo = MessageInfoUtil::constructMsgInfo("data", 1);
    status = writer._writeBuffer.addWriteMessage(msgInfo);
    ASSERT_EQ(true, status);
    adapter->_ec = ERROR_CLIENT_SYS_STOPPED;
    adapter->resetFakeClient();
    expectCollector.rpcLatency = -1;
    expectCollector.decompressLatency = -1;
    expectCollector.unsendCount = 1;
    expectCollector.uncommitCount = 0;
    expectCollector.requestMsgCount = -1;
    expectCollector.errorCallBack = false;
    expectCollector.hasError = true;
    expectCollector.isDetectCommit = false;
    expect_call_writer(reporter, expectCollector);
    writer.sendRequest(5, true);

    adapter->_ec = ERROR_NONE;
    adapter->_fakeClient = new FakeSwiftTransportClient;
    adapter->_fakeClient->setTimeoutCount(0);
    adapter->_fakeClient->setAutoAsyncCall();
    adapter->_fakeClient->_supportClientSafeMode = true;
    adapter->createTransportClient();
    writer._lastErrorCode = ERROR_NONE;
    for (int i = 0; i < 4; ++i) {
        msgInfo = MessageInfoUtil::constructMsgInfo("data", 1);
        status = writer._writeBuffer.addWriteMessage(msgInfo);
        ASSERT_EQ(true, status);
    }
    expectCollector.rpcLatency = -1;
    expectCollector.decompressLatency = -1;
    expectCollector.unsendCount = 5;
    expectCollector.uncommitCount = 0;
    expectCollector.requestMsgCount = 5;
    expectCollector.errorCallBack = false;
    expectCollector.hasError = false;
    expectCollector.isDetectCommit = false;
    expect_call_writer(reporter, expectCollector);
    writer.sendRequest(6, true);
}

TEST_F(SingleSwiftWriterTest, testSendSyncRequestMetricsReport) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.topicName = topicName;
    config.oneRequestSendByteCount = 1024;
    config.maxBufferSize = 2048;
    config.maxKeepInBufferTime = 1000;
    config.retryTimes = 1;
    config.retryTimeInterval = 1000;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    kmonitor::MetricsReporterPtr kmonReporter(nullptr);
    MockClientMetricsReporter reporter(kmonReporter);
    SingleSwiftWriter writer(SwiftAdminAdapterPtr(),
                             SwiftRpcChannelManagerPtr(),
                             partitionId,
                             config,
                             blockPool,
                             _threadPool,
                             BufferSizeLimiterPtr(),
                             dynamic_cast<ClientMetricsReporter *>(&reporter));

    FakeSwiftTransportAdapter<TRT_SENDMESSAGE> *adapter =
        new FakeSwiftTransportAdapter<TRT_SENDMESSAGE>(topicName, partitionId);
    DELETE_AND_SET_NULL(writer._transportAdapter);
    writer._transportAdapter = adapter;
    FakeSwiftTransportClient *fakeClient = adapter->getFakeTransportClient();
    fakeClient->setTimeoutCount(0);
    fakeClient->setAutoAsyncCall();
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo("data", 1);
    bool status = writer._writeBuffer.addWriteMessage(msgInfo);
    ASSERT_EQ(true, status);
    ClientMetricsCollector collector;
    ClientMetricsCollector expectCollector;
    expectCollector.rpcLatency = -1;
    expectCollector.decompressLatency = -1;
    expectCollector.unsendCount = 1;
    expectCollector.uncommitCount = 0;
    expectCollector.requestMsgCount = 1;
    expectCollector.errorCallBack = false;
    expectCollector.hasError = false;
    expectCollector.isDetectCommit = false;

    expect_call_writer(reporter, expectCollector);
    writer.sendSyncRequest(1, config.retryTimes, collector);

    collector.reset(false);
    msgInfo = MessageInfoUtil::constructMsgInfo("data", 1);
    status = writer._writeBuffer.addWriteMessage(msgInfo);
    ASSERT_EQ(true, status);
    expectCollector.rpcLatency = writer._transportAdapter->_closure->getRpcLatency();
    expectCollector.decompressLatency = -1;
    expectCollector.unsendCount = 1;
    expectCollector.uncommitCount = 0;
    expectCollector.requestMsgCount = 1;
    expectCollector.errorCallBack = false;
    expectCollector.hasError = false;
    expectCollector.isDetectCommit = false;
    expect_call_writer(reporter, expectCollector);
    writer.sendSyncRequest(2, config.retryTimes, collector);

    collector.reset(false);
    msgInfo = MessageInfoUtil::constructMsgInfo("data", 1);
    status = writer._writeBuffer.addWriteMessage(msgInfo);
    ASSERT_EQ(true, status);
    adapter->_ec = ERROR_CLIENT_SYS_STOPPED;
    adapter->resetFakeClient();
    expectCollector.rpcLatency = -1;
    expectCollector.decompressLatency = -1;
    expectCollector.unsendCount = 1;
    expectCollector.uncommitCount = 0;
    expectCollector.requestMsgCount = -1;
    expectCollector.errorCallBack = false;
    expectCollector.hasError = true;
    expectCollector.isDetectCommit = false;
    expect_call_writer(reporter, expectCollector);
    writer.sendSyncRequest(3, config.retryTimes, collector);

    collector.reset(false);
    adapter->_ec = ERROR_NONE;
    adapter->_fakeClient = new FakeSwiftTransportClient;
    adapter->_fakeClient->setTimeoutCount(0);
    adapter->_fakeClient->setAutoAsyncCall(true);
    adapter->createTransportClient();
    writer._lastErrorCode = ERROR_NONE;
    for (int i = 0; i < 4; ++i) {
        msgInfo = MessageInfoUtil::constructMsgInfo("data", 1);
        status = writer._writeBuffer.addWriteMessage(msgInfo);
        ASSERT_EQ(true, status);
    }
    expectCollector.rpcLatency = -1;
    expectCollector.decompressLatency = -1;
    expectCollector.unsendCount = 4;
    expectCollector.uncommitCount = 0;
    expectCollector.requestMsgCount = 4;
    expectCollector.errorCallBack = false;
    expectCollector.hasError = false;
    expectCollector.isDetectCommit = false;
    expect_call_writer(reporter, expectCollector);
    writer.sendSyncRequest(4, config.retryTimes, collector);
}

TEST_F(SingleSwiftWriterTest, testMessageLengthExceeded) {
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.topicName = "topic_name";
    config.oneRequestSendByteCount = 1024;
    config.maxBufferSize = 2048;
    config.maxKeepInBufferTime = 1000 * 1000 * 1000 * 1000L; // 1000s, never happen
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    SingleSwiftWriter writer(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
    string data(common::MAX_MESSAGE_SIZE + 1, 'c');
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, 0);
    ASSERT_EQ(ERROR_BROKER_MSG_LENGTH_EXCEEDED, writer.write(msgInfo));
}

TEST_F(SingleSwiftWriterTest, testWriteHasVersionError) {
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.topicName = "topic_name";
    config.maxKeepInBufferTime = 1000 * 1000 * 1000 * 1000L; // 1000s, never happen
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    SingleSwiftWriter writer(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
    EXPECT_FALSE(writer._hasVersionError);
    writer._hasVersionError = true;
    string data("aaa");
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, 0);
    ASSERT_EQ(ERROR_BROKER_WRITE_VERSION_INVALID, writer.write(msgInfo));
}

TEST_F(SingleSwiftWriterTest, testSimpleWrite) {
    string topicName = "topic_name";
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.safeMode = false;
    config.topicName = topicName;
    config.oneRequestSendByteCount = 1001;
    config.maxBufferSize = 40000;
    config.maxKeepInBufferTime = 1000 * 1000 * 1000 * 1000L; // 1000s, never happen
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    SingleSwiftWriter writer(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
    FakeSwiftTransportAdapter<TRT_SENDMESSAGE> *adapter =
        new FakeSwiftTransportAdapter<TRT_SENDMESSAGE>(topicName, partitionId);
    DELETE_AND_SET_NULL(writer._transportAdapter);
    writer._transportAdapter = adapter;
    FakeSwiftTransportClient *fakeClient = adapter->getFakeTransportClient();

    string data = "test_data0"; // len = 10
    uint32_t i = 0;
    int64_t now = autil::TimeUtility::currentTime();
    uint32_t msgCount = config.oneRequestSendByteCount / data.size();
    for (; i < msgCount; ++i) {
        MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, (int64_t)i);
        ASSERT_EQ(ERROR_NONE, writer.write(msgInfo));
        EXPECT_EQ(ERROR_NONE, writer.sendRequest(now, false));
        EXPECT_EQ(i + 1, writer._writeBuffer._writeQueue.size());
        EXPECT_TRUE(!fakeClient->_sendClosure);
    }
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, (int64_t)i++);
    ASSERT_EQ(ERROR_NONE, writer.write(msgInfo));
    EXPECT_EQ(msgCount + 1, writer._writeBuffer._writeQueue.size());
    EXPECT_EQ(ERROR_NONE, writer.sendRequest(now, false)); // meet request byte count
    EXPECT_TRUE(fakeClient->_sendClosure);
    EXPECT_EQ(0, writer._writeBuffer._writeQueue.size());
    EXPECT_EQ(msgCount + 1, writer._writeBuffer._sendQueue.size()); // data use 1 block

    uint32_t msgCount2 = config.maxBufferSize / 4 / sizeof(ClientMemoryMessage) * 3;
    for (i = 0; i < msgCount2; ++i) {
        MessageInfo msgInfo1 = MessageInfoUtil::constructMsgInfo(data, (int64_t)i);
        ASSERT_EQ(ERROR_NONE, writer.write(msgInfo1));
        EXPECT_EQ(ERROR_NONE, writer.sendRequest(now, false));
        ASSERT_EQ(i + 1, writer._writeBuffer._writeQueue.size());
    }
    writer._writeBuffer.convertMessage();
    EXPECT_EQ(msgCount2, writer._writeBuffer._sendQueue.size());
    EXPECT_EQ(msgCount + 1, writer._writeBuffer._toMergeQueue.size()); // 3 meta block
    msgInfo = MessageInfoUtil::constructMsgInfo(data, (int64_t)i);
    ASSERT_EQ(ERROR_CLIENT_SEND_BUFFER_FULL, writer.write(msgInfo));
    fakeClient->finishAsyncCall();
    EXPECT_EQ(msgCount2, writer._writeBuffer._sendQueue.size());
    EXPECT_EQ(msgCount + 1, writer._writeBuffer._toMergeQueue.size());

    for (int k = 1; k <= 2; k++) {
        EXPECT_EQ(ERROR_NONE, writer.sendRequest(now, false)); // handle last response
        EXPECT_EQ(msgCount2 - msgCount * k - k, writer._writeBuffer._sendQueue.size()) << k;
        EXPECT_EQ(msgCount + 1, writer._writeBuffer._toMergeQueue.size()) << k; // no block left
        EXPECT_TRUE(fakeClient->_sendClosure);
        fakeClient->finishAsyncCall();
        msgInfo = MessageInfoUtil::constructMsgInfo(data, (int64_t)i);
        ASSERT_EQ(ERROR_CLIENT_SEND_BUFFER_FULL, writer.write(msgInfo));
    }
    EXPECT_EQ(ERROR_NONE, writer.sendRequest(now, false)); // handle last response
    EXPECT_EQ(msgCount2 - msgCount * 2 - 2, writer._writeBuffer._sendQueue.size());
    EXPECT_EQ(0, writer._writeBuffer._toMergeQueue.size());
    EXPECT_TRUE(fakeClient->_sendClosure);
    fakeClient->finishAsyncCall();
    msgInfo = MessageInfoUtil::constructMsgInfo(data, (int64_t)i);
    ASSERT_EQ(ERROR_NONE, writer.write(msgInfo));

    writer._hasVersionError = true;
    EXPECT_EQ(ERROR_BROKER_WRITE_VERSION_INVALID, writer.sendRequest(now, false));

    writer._hasVersionError = false;
    writer._writeBuffer.setSealed(true);
    EXPECT_EQ(ERROR_TOPIC_SEALED, writer.sendRequest(now, false));
}

TEST_F(SingleSwiftWriterTest, testSendRequestErrorCallback) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.topicName = topicName;
    config.oneRequestSendByteCount = 1024;
    config.maxBufferSize = 2048;
    config.maxKeepInBufferTime = 1000 * 1000 * 1000 * 1000L; // 1000s, never happen

    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    SingleSwiftWriter writer(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);

    ErrorCode errorCode = ERROR_NONE;
    auto errorFunc = [&errorCode](const ErrorCode &ec) -> void { errorCode = ec; };
    writer.setErrorCallBack(errorFunc);
    ErrorCode retErrorCode = writer.sendRequest(1000, false);

    writer._writeBuffer.setSealed(false);
    writer._hasVersionError = true;
    retErrorCode = writer.sendRequest(1000, false);
    ASSERT_EQ(ERROR_BROKER_WRITE_VERSION_INVALID, retErrorCode);
    ASSERT_EQ(ERROR_BROKER_WRITE_VERSION_INVALID, errorCode);

    writer._hasVersionError = false;
    errorCode = ERROR_NONE;
    writer._lastErrorCode = ERROR_CLIENT_SEND_BUFFER_FULL;
    retErrorCode = writer.sendRequest(1000, false);
    ASSERT_EQ(ERROR_CLIENT_SEND_BUFFER_FULL, retErrorCode);
    ASSERT_EQ(ERROR_NONE, errorCode);
}

TEST_F(SingleSwiftWriterTest, testSendRequestByTime) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.topicName = topicName;
    config.oneRequestSendByteCount = 1024;
    config.maxBufferSize = 204800;
    config.maxKeepInBufferTime = 1000;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    SingleSwiftWriter writer(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
    FakeSwiftTransportAdapter<TRT_SENDMESSAGE> *adapter =
        new FakeSwiftTransportAdapter<TRT_SENDMESSAGE>(topicName, partitionId);
    DELETE_AND_SET_NULL(writer._transportAdapter);
    writer._transportAdapter = adapter;
    FakeSwiftTransportClient *fakeClient = adapter->getFakeTransportClient();
    fakeClient->setAutoAsyncCall();

    writer._lastSendRequestTime = 0;

    string data = "test_data";

    writer.sendRequest(1000, false);
    ASSERT_EQ(int64_t(0), writer._lastSendRequestTime);
    writer.sendRequest(2000, true);
    ASSERT_EQ(int64_t(0), writer._lastSendRequestTime);
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, 1);
    ASSERT_EQ(ERROR_NONE, writer.write(msgInfo));
    writer.sendRequest(3000, false);
    ASSERT_EQ(int64_t(3000), writer._lastSendRequestTime);
    writer.sendRequest(3000, false);

    int64_t checkpoint = -1;
    bool hasMsg = writer.getCommittedCheckpointId(checkpoint);
    EXPECT_TRUE(!hasMsg);
    ASSERT_EQ(int64_t(1), checkpoint);
    msgInfo = MessageInfoUtil::constructMsgInfo(data, 2);
    ASSERT_EQ(ERROR_NONE, writer.write(msgInfo));
    writer.sendRequest(3500, true); // force send
    ASSERT_EQ(int64_t(3500), writer._lastSendRequestTime);
    writer.sendRequest(3500, false); // just for handle response
    checkpoint = -1;
    hasMsg = writer.getCommittedCheckpointId(checkpoint);
    EXPECT_TRUE(!hasMsg);
    ASSERT_EQ(int64_t(2), checkpoint);
    msgInfo = MessageInfoUtil::constructMsgInfo(data, 3);
    ASSERT_EQ(ERROR_NONE, writer.write(msgInfo));
    writer.sendRequest(4000, false); // not enough time, do not send.
    ASSERT_EQ(int64_t(3500), writer._lastSendRequestTime);
    writer.sendRequest(4000, false); // just for handle response
    checkpoint = -1;
    hasMsg = writer.getCommittedCheckpointId(checkpoint);
    EXPECT_TRUE(hasMsg);
    ASSERT_EQ(int64_t(2), checkpoint);

    uint32_t i = 0;
    for (; i < config.oneRequestSendByteCount / data.size(); ++i) {
        msgInfo = MessageInfoUtil::constructMsgInfo(data, (int64_t)(i + 4));
        ASSERT_EQ(ERROR_NONE, writer.write(msgInfo));
        writer.sendRequest(4100, false);
    }
    // update send request time.
    writer.sendRequest(4100, false); // just for handle response
    ASSERT_EQ(int64_t(4100), writer._lastSendRequestTime);
    checkpoint = -1;
    hasMsg = writer.getCommittedCheckpointId(checkpoint);
    EXPECT_TRUE(!hasMsg);
    ASSERT_EQ(int64_t(i + 4 - 1), checkpoint);
}

TEST_F(SingleSwiftWriterTest, testSendRequestBySynchronous) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.isSynchronous = true;
    config.topicName = topicName;
    config.oneRequestSendByteCount = 1024;
    config.maxBufferSize = 2048;
    config.maxKeepInBufferTime = 1000;
    config.retryTimes = 2;
    config.retryTimeInterval = 1000;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    SingleSwiftWriter writer(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
    FakeSwiftTransportAdapter<TRT_SENDMESSAGE> *adapter =
        new FakeSwiftTransportAdapter<TRT_SENDMESSAGE>(topicName, partitionId);
    DELETE_AND_SET_NULL(writer._transportAdapter);
    writer._transportAdapter = adapter;
    FakeSwiftTransportClient *fakeClient = adapter->getFakeTransportClient();
    fakeClient->setTimeoutCount(3);
    fakeClient->setAutoAsyncCall();

    ASSERT_EQ((int64_t)0, writer._blockPool->getUsedBlockCount());
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo("data", 1);
    ErrorCode ec = writer.write(msgInfo, true);
    ASSERT_EQ(ERROR_CLIENT_INVALID_RESPONSE, ec);
    int64_t checkpoint = 0;
    bool hasMsg = writer.getCommittedCheckpointId(checkpoint);
    EXPECT_TRUE(!hasMsg);
    ASSERT_EQ((int64_t)SingleSwiftWriter::INVALID_CHECK_POINT_ID, checkpoint);
    ASSERT_EQ((size_t)0, writer._writeBuffer.getSize());
    ASSERT_EQ((int64_t)1, writer._blockPool->getUsedBlockCount()); // msg converter used

    fakeClient->setTimeoutCount(1);

    ec = writer.write(msgInfo, true);
    ASSERT_EQ(ERROR_NONE, ec);
    checkpoint = 0;
    writer.getCommittedCheckpointId(checkpoint);
    ASSERT_EQ((int64_t)1, checkpoint);
    ASSERT_EQ((size_t)0, writer._writeBuffer.getSize());
    ASSERT_EQ((int64_t)1, writer._blockPool->getUsedBlockCount());
}

TEST_F(SingleSwiftWriterTest, testSendRequestBySynchronousWithMultiThreads) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.topicName = topicName;
    config.oneRequestSendByteCount = 1024;
    config.maxBufferSize = 4096;
    config.maxKeepInBufferTime = 1000;
    config.retryTimes = 2;
    config.retryTimeInterval = 1000;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    SingleSwiftWriter writer(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
    FakeSwiftTransportAdapter<TRT_SENDMESSAGE> *adapter =
        new FakeSwiftTransportAdapter<TRT_SENDMESSAGE>(topicName, partitionId);
    DELETE_AND_SET_NULL(writer._transportAdapter);
    writer._transportAdapter = adapter;
    FakeSwiftTransportClient *fakeClient = adapter->getFakeTransportClient();
    fakeClient->setAutoAsyncCall();

    int32_t msgCount = 10;
    int32_t threadCount = 10;
    vector<ThreadPtr> threadVec;
    for (int32_t i = 0; i < threadCount; ++i) {
        ThreadPtr sendRequestThreadPtr =
            autil::Thread::createThread(std::bind(&SingleSwiftWriterTest::doSendRequest, this, &writer, msgCount));
        threadVec.push_back(sendRequestThreadPtr);
    }

    for (int32_t i = 0; i < threadCount; ++i) {
        threadVec[i]->join();
    }

    ASSERT_EQ(msgCount * threadCount, (int32_t)fakeClient->getMsgCount());
}

void SingleSwiftWriterTest::doSendRequest(SingleSwiftWriter *writer, int32_t msgCount) {
    MessageInfo msgInfo;
    for (int32_t i = 0; i < msgCount; ++i) {
        MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo("data");
        writer->write(msgInfo, true);
    }
}

TEST_F(SingleSwiftWriterTest, testNeedRetreat) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.topicName = topicName;
    config.brokerBusyWaitIntervalMin = 1000 * 1000;
    config.brokerBusyWaitIntervalMax = 2000 * 1000;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    {
        //_lastErrorCode != ERROR_BROKER_BUSY
        SingleSwiftWriter writer(
            SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
        writer._lastErrorCode = ERROR_NONE;
        bool ret = writer.needRetreat(TimeUtility::currentTime());
        EXPECT_TRUE(!ret);
        ASSERT_EQ((int64_t)-1, writer._retreatNextTimestamp);
    }
    {
        //_lastErrorCode == ERROR_BROKER_BUSY && _retreatNextTimestamp <0
        SingleSwiftWriter writer(
            SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
        writer._lastErrorCode = ERROR_BROKER_BUSY;
        writer._retreatNextTimestamp = -1;
        writer._lastSendRequestTime = 5;
        int64_t currentTime = 3;
        bool ret = writer.needRetreat(currentTime);
        EXPECT_TRUE(ret);
        EXPECT_TRUE(writer._retreatNextTimestamp <=
                    (int64_t)(writer._lastSendRequestTime + config.brokerBusyWaitIntervalMax));
        EXPECT_TRUE(writer._retreatNextTimestamp >=
                    (int64_t)(writer._lastSendRequestTime + config.brokerBusyWaitIntervalMin));
    }
    {
        //_lastErrorCode == ERROR_BROKER_BUSY && _retreatNextTimestamp > 0
        SingleSwiftWriter writer(
            SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
        writer._lastErrorCode = ERROR_BROKER_BUSY;
        writer._retreatNextTimestamp = 5;
        writer._lastSendRequestTime = 5;
        int64_t currentTime = 3;
        bool ret = writer.needRetreat(currentTime);
        EXPECT_TRUE(ret);

        currentTime = 6;
        ret = writer.needRetreat(currentTime);
        EXPECT_TRUE(!ret);
        ASSERT_EQ((int64_t)-1, writer._retreatNextTimestamp);
    }
    {
        // brokerBusyWaitIntervalMin == brokerBusyWaitIntervalMax
        SingleSwiftWriter writer(
            SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
        writer._config.brokerBusyWaitIntervalMin = 1000 * 1000;
        writer._config.brokerBusyWaitIntervalMax = 1000 * 1000;
        writer._lastErrorCode = ERROR_BROKER_BUSY;
        writer._retreatNextTimestamp = -1;
        writer._lastSendRequestTime = 5;
        int64_t currentTime = 3;
        bool ret = writer.needRetreat(currentTime);
        EXPECT_TRUE(ret);
        ASSERT_EQ(writer._retreatNextTimestamp,
                  (int64_t)(writer._lastSendRequestTime + writer._config.brokerBusyWaitIntervalMax));
    }
    {
        SingleSwiftWriter writer(
            SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
        writer._config.brokerBusyWaitIntervalMin = 1000 * 1000;
        writer._config.brokerBusyWaitIntervalMax = 1000 * 1000;
        writer._lastErrorCode = ERROR_CLIENT_INVALID_RESPONSE;
        writer._retreatNextTimestamp = -1;
        writer._lastSendRequestTime = 5;
        int64_t currentTime = 3;
        bool ret = writer.needRetreat(currentTime);
        EXPECT_TRUE(ret);
        ASSERT_EQ(writer._retreatNextTimestamp,
                  (int64_t)(writer._lastSendRequestTime + writer._config.brokerBusyWaitIntervalMax));
    }
    {
        SingleSwiftWriter writer(
            SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
        writer._config.brokerBusyWaitIntervalMin = 1000 * 1000;
        writer._config.brokerBusyWaitIntervalMax = 1000 * 1000;
        writer._lastErrorCode = ERROR_BROKER_INVALID_USER;
        writer._retreatNextTimestamp = -1;
        writer._lastSendRequestTime = 5;
        int64_t currentTime = 3;
        bool ret = writer.needRetreat(currentTime);
        EXPECT_TRUE(ret);
        ASSERT_EQ(writer._retreatNextTimestamp,
                  (int64_t)(writer._lastSendRequestTime + writer._config.brokerBusyWaitIntervalMax));
    }
    {
        SingleSwiftWriter writer(
            SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
        writer._config.brokerBusyWaitIntervalMin = 1000 * 1000;
        writer._config.brokerBusyWaitIntervalMax = 1000 * 1000;
        writer._lastErrorCode = ERROR_CLIENT_ARPC_ERROR;
        writer._retreatNextTimestamp = -1;
        writer._lastSendRequestTime = 5;
        int64_t currentTime = 3;
        bool ret = writer.needRetreat(currentTime);
        EXPECT_FALSE(ret);
        ASSERT_EQ(-1, writer._retreatNextTimestamp);
    }
    {
        SingleSwiftWriter writer(
            SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
        writer._config.brokerBusyWaitIntervalMin = 1000 * 1000;
        writer._config.brokerBusyWaitIntervalMax = 1000 * 1000;
        writer._transportAdapter->_brokerAddressIsSame = true;
        writer._lastErrorCode = ERROR_CLIENT_ARPC_ERROR;
        writer._retreatNextTimestamp = -1;
        writer._lastSendRequestTime = 5;
        int64_t currentTime = 3;
        bool ret = writer.needRetreat(currentTime);
        EXPECT_TRUE(ret);
        ASSERT_EQ(writer._retreatNextTimestamp,
                  (int64_t)(writer._lastSendRequestTime + writer._config.brokerBusyWaitIntervalMax));
    }
}

TEST_F(SingleSwiftWriterTest, testPostRequestFailed) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.topicName = topicName;
    config.oneRequestSendByteCount = 1024;
    config.maxBufferSize = 2048;
    config.maxKeepInBufferTime = 1000 * 1000 * 1000 * 1000L; // 1000s, never happen
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    SingleSwiftWriter writer(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
    FakeSwiftTransportAdapter<TRT_SENDMESSAGE> *adapter =
        new FakeSwiftTransportAdapter<TRT_SENDMESSAGE>(topicName, partitionId);
    adapter->setErrorCode(ERROR_UNKNOWN);

    DELETE_AND_SET_NULL(writer._transportAdapter);
    writer._transportAdapter = adapter;

    string data = "test_data";
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, (int64_t)0);
    ASSERT_EQ(ERROR_UNKNOWN, writer.write(msgInfo, true));
}

TEST_F(SingleSwiftWriterTest, testHandleSendMessageResponse_WriterBufferSealed) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.topicName = topicName;
    config.oneRequestSendByteCount = 1024;
    config.maxBufferSize = 2048;
    config.maxKeepInBufferTime = 1000 * 1000 * 1000 * 1000L; // 1000s, never happen

    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    SingleSwiftWriter writer(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);

    // 1. test response sealed, buffer then sealed
    writer._writeBuffer.setSealed(false);
    auto fakeClosure = std::make_shared<TransportClosureTyped<TRT_SENDMESSAGE>>(nullptr, nullptr);
    writer._transportAdapter->_closure = fakeClosure;
    writer._transportAdapter->_lastTransportClosureHandled = false;
    fakeClosure->_response->mutable_errorinfo()->set_errcode(ERROR_TOPIC_SEALED);
    fakeClosure->_done = true;
    writer.handleSendMessageResponse();
    EXPECT_EQ(true, writer._writeBuffer._sealed);

    // 2. response other error, buffer not seal
    fakeClosure->_done = true;
    writer._writeBuffer.setSealed(false);
    writer._transportAdapter->_lastTransportClosureHandled = false;
    fakeClosure->_response = new MessageResponse();
    fakeClosure->_response->mutable_errorinfo()->set_errcode(ERROR_BROKER_SOME_MESSAGE_LOST);
    writer.handleSendMessageResponse();
    EXPECT_EQ(false, writer._writeBuffer._sealed);
    EXPECT_FALSE(writer._hasVersionError);

    // 3. response version errror, set _hasVersionError
    writer._transportAdapter->_lastTransportClosureHandled = false;
    fakeClosure->_done = true;
    fakeClosure->_response = new MessageResponse();
    fakeClosure->_response->mutable_errorinfo()->set_errcode(ERROR_BROKER_WRITE_VERSION_INVALID);
    writer.handleSendMessageResponse();
    EXPECT_EQ(false, writer._writeBuffer._sealed);
    EXPECT_TRUE(writer._hasVersionError);

    // 4. test sendRequest return when buffer sealed
    writer._hasVersionError = false;
    int64_t now = 123;
    writer._lastSendRequestTime = 0;
    fakeClosure->_done = true;
    writer._transportAdapter->_lastTransportClosureHandled = true;
    writer._writeBuffer.setSealed(true);
    writer._lastErrorCode = ERROR_NONE;
    writer._writeBuffer._writeQueue.push_back(new MessageInfo());
    writer.sendRequest(now, true);
    EXPECT_EQ(0, writer._lastSendRequestTime);

    // 5. test sendRequest return when buffer not sealed
    FakeSwiftTransportAdapter<TRT_SENDMESSAGE> *fakeTransportAdapter =
        new FakeSwiftTransportAdapter<TRT_SENDMESSAGE>("topic0", 0);
    DELETE_AND_SET_NULL(writer._transportAdapter);
    writer._transportAdapter = fakeTransportAdapter;
    FakeSwiftTransportClient *fakeClient = fakeTransportAdapter->getFakeTransportClient();
    fakeTransportAdapter->setErrorCode(ERROR_CLIENT_ARPC_ERROR);
    fakeClient->setAutoAsyncCall();
    writer._lastSendRequestTime = 0;
    writer._transportAdapter->_lastTransportClosureHandled = true;
    writer._writeBuffer.setSealed(false);
    writer._lastErrorCode = ERROR_NONE;
    writer.sendRequest(now, true);
    EXPECT_EQ(123, writer._lastSendRequestTime);
}

TEST_F(SingleSwiftWriterTest, testSendRequestCompress) {
    innerTestSendRequestCompress(1024, 512, false);
    innerTestSendRequestCompress(1024, 1024, false);
    innerTestSendRequestCompress(1024, 2048, true);
}

void SingleSwiftWriterTest::innerTestSendRequestCompress(uint64_t threshold, size_t msgSize, bool result) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.isSynchronous = true;
    config.topicName = topicName;
    config.oneRequestSendByteCount = 4096;
    config.maxBufferSize = 40960;
    config.maxKeepInBufferTime = 1000;
    config.retryTimes = 2;
    config.retryTimeInterval = 1000;
    config.compressMsg = true;
    config.compressThresholdInBytes = threshold;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    SingleSwiftWriter writer(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
    FakeSwiftTransportAdapter<TRT_SENDMESSAGE> *adapter =
        new FakeSwiftTransportAdapter<TRT_SENDMESSAGE>(topicName, partitionId);
    DELETE_AND_SET_NULL(writer._transportAdapter);
    writer._transportAdapter = adapter;
    FakeSwiftTransportClient *fakeClient = adapter->getFakeTransportClient();
    fakeClient->setAutoAsyncCall();
    string data(msgSize, 'a');
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, 1);
    ErrorCode ec = writer.write(msgInfo, true);
    ASSERT_EQ(ERROR_NONE, ec);
    int64_t checkpoint = 0;
    writer.getCommittedCheckpointId(checkpoint);
    ASSERT_EQ((int64_t)1, checkpoint);
    ASSERT_EQ((size_t)0, writer._writeBuffer.getSize());
    ASSERT_EQ((int64_t)1, writer._blockPool->getUsedBlockCount()); // msg converter used
    ASSERT_EQ(1, (int32_t)fakeClient->getMsgCount());
    swift::protocol::ConsumptionRequest request;
    request.set_startid(0);
    request.set_count(100);
    swift::protocol::MessageResponse response;
    fakeClient->getMessage(&request, &response);
    ASSERT_EQ(1, (int32_t)response.msgs_size());
    const Message &message = response.msgs(0);
    ASSERT_EQ(result, message.compress());
}

TEST_F(SingleSwiftWriterTest, testClientSafeMode) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftWriterConfig config;
    config.topicName = topicName;
    config.oneRequestSendByteCount = 1024;
    config.maxBufferSize = 2048;
    config.maxKeepInBufferTime = 1000;
    config.retryTimes = 2;
    config.retryTimeInterval = 10;
    config.safeMode = true;
    BlockPoolPtr blockPool(new BlockPool(config.maxBufferSize, config.maxBufferSize / 4));
    SingleSwiftWriter writer(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config, blockPool, _threadPool);
    FakeSwiftTransportAdapter<TRT_SENDMESSAGE> *adapter =
        new FakeSwiftTransportAdapter<TRT_SENDMESSAGE>(topicName, partitionId);
    DELETE_AND_SET_NULL(writer._transportAdapter);
    writer._transportAdapter = adapter;
    FakeSwiftTransportClient *fakeClient = adapter->getFakeTransportClient();
    fakeClient->_supportClientSafeMode = true;
    fakeClient->_nextMsgId = 0;
    vector<pair<int64_t, int64_t>> committedCpTs;
    auto committedFunc = [&committedCpTs](const vector<pair<int64_t, int64_t>> &cpts) -> void {
        committedCpTs.clear();
        committedCpTs = cpts;
    };
    writer.setCommittedCallBack(committedFunc);

    // send 10 message
    string data(10, 'a');
    for (uint32_t i = 0; i < 10; i++) {
        // 构建消息，checkpoint设为i*2, msgid会被设为i, timestamp会被fake设为i*10
        MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, i * 2);
        ErrorCode ec = writer.write(msgInfo);
        ASSERT_EQ(ERROR_NONE, ec);
    }
    int64_t now = TimeUtility::currentTimeInMicroSeconds() + 10000;
    writer.sendRequest(now, false);
    ASSERT_EQ(now, writer._lastSendRequestTime);
    // commit nothing
    fakeClient->finishAsyncCall();
    ASSERT_EQ(size_t(10), fakeClient->_messageVec->size());

    now += 1000 * 1000;
    writer.sendRequest(now, false);
    EXPECT_EQ(fakeClient->_sessionId, writer._sessionId);
    EXPECT_EQ(now, writer._lastSendRequestTime);
    EXPECT_EQ(size_t(10), writer._writeBuffer.getQueueSize());
    EXPECT_EQ(size_t(10), writer._writeBuffer.getUncommittedCount());
    EXPECT_EQ(0, writer._writeBuffer._uncommittedMsgs[0].first.first);
    EXPECT_EQ(9, writer._writeBuffer._uncommittedMsgs[0].first.second);
    EXPECT_EQ(0, committedCpTs.size());

    fakeClient->setCommitMsgId(4); // [0, 5) will be committed
    fakeClient->finishAsyncCall();
    EXPECT_EQ(size_t(10), fakeClient->_messageVec->size());
    for (int i = 0; i < 10; ++i) {
        cout << (*fakeClient->_messageVec)[i].ShortDebugString() << endl;
    }

    now += 1000 * 1000;
    writer.sendRequest(now, false);
    EXPECT_EQ(fakeClient->_sessionId, writer._sessionId);
    EXPECT_EQ(now, writer._lastSendRequestTime);
    EXPECT_EQ(5, writer._writeBuffer.getQueueSize());
    EXPECT_EQ(5, writer._writeBuffer.getUncommittedCount());
    EXPECT_EQ(5, writer._writeBuffer._uncommittedMsgs[0].first.first);
    EXPECT_EQ(9, writer._writeBuffer._uncommittedMsgs[0].first.second);
    fakeClient->setCommitMsgId(7);
    fakeClient->finishAsyncCall();
    EXPECT_EQ(size_t(10), fakeClient->_messageVec->size());

    for (size_t i = 100; i < 105; i++) {
        // msg id 100 ~ 104, cp = msgid * 2, ts = msgid * 10
        MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(data, i * 2);
        ErrorCode ec = writer.write(msgInfo);
        EXPECT_EQ(ERROR_NONE, ec);
    }
    now += 1000 * 1000;
    writer.sendRequest(now, false);
    EXPECT_EQ(7, writer._writeBuffer.getQueueSize()); // 2 + 5
    EXPECT_EQ(2, writer._writeBuffer.getUncommittedCount());
    EXPECT_EQ(5, writer._writeBuffer.getUnsendCount());
    EXPECT_EQ(8, writer._writeBuffer._uncommittedMsgs[0].first.first);
    EXPECT_EQ(9, writer._writeBuffer._uncommittedMsgs[0].first.second);
    EXPECT_EQ(3, committedCpTs.size());
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ((i + 5) * 2, committedCpTs[i].first);
        EXPECT_EQ((i + 5) * 10, committedCpTs[i].second);
    }

    fakeClient->_nextMsgId = 100;
    fakeClient->setCommitMsgId(103);
    fakeClient->finishAsyncCall();

    now += 1000 * 1000;
    writer.sendRequest(now, false);
    EXPECT_EQ(1, writer._writeBuffer.getQueueSize());
    EXPECT_EQ(1, writer._writeBuffer.getUncommittedCount());
    EXPECT_EQ(104, writer._writeBuffer._uncommittedMsgs[0].first.first);
    EXPECT_EQ(104, writer._writeBuffer._uncommittedMsgs[0].first.second);
    EXPECT_EQ(size_t(15), fakeClient->_messageVec->size());
    EXPECT_EQ(6, committedCpTs.size());
    for (int i = 0; i < 2; ++i) {
        EXPECT_EQ((i + 8) * 2, committedCpTs[i].first);
        EXPECT_EQ((i + 8) * 10, committedCpTs[i].second);
    }
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ((i + 100) * 2, committedCpTs[i + 2].first);
        EXPECT_EQ((i + 100) * 10, committedCpTs[i + 2].second);
    }
    fakeClient->_sessionId += 1000;
    fakeClient->setCommitMsgId(106);
    fakeClient->_nextMsgId = 107;
    fakeClient->finishAsyncCall();

    now += 1000 * 1000;
    committedCpTs.clear();
    writer.sendRequest(now, false);
    EXPECT_EQ(ERROR_BROKER_SESSION_CHANGED, writer.getLastErrorCode());
    EXPECT_EQ(1, writer._writeBuffer.getQueueSize());
    EXPECT_EQ(1, writer._writeBuffer.getUnsendCount());
    EXPECT_EQ(0, writer._writeBuffer.getUncommittedCount());
    EXPECT_EQ(0, writer._writeBuffer._uncommittedMsgs.size());
    EXPECT_EQ(0, committedCpTs.size());

    fakeClient->setCommitMsgId(107);
    fakeClient->finishAsyncCall();
    now += 1000 * 1000;
    writer.sendRequest(now, false); // session change重发一条消息
    EXPECT_EQ(ERROR_NONE, writer.getLastErrorCode());
    EXPECT_EQ(0, writer._writeBuffer.getQueueSize());
    EXPECT_EQ(0, writer._writeBuffer.getUncommittedCount());
    EXPECT_EQ(0, writer._writeBuffer._uncommittedMsgs.size());
    EXPECT_EQ(1, committedCpTs.size());
    EXPECT_EQ(104 * 2, committedCpTs[0].first);
    EXPECT_EQ(107 * 10, committedCpTs[0].second);
}

} // namespace client
} // namespace swift
