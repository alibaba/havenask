#include "swift/client/SwiftSinglePartitionReader.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <limits>
#include <memory>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "kmonitor/client/MetricsReporter.h"
#include "swift/client/MessageReadBuffer.h"
#include "swift/client/SwiftPartitionStatus.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/client/SwiftTransportAdapter.h"
#include "swift/client/SwiftTransportClientCreator.h"
#include "swift/client/TransportClosure.h"
#include "swift/client/fake_client/FakeClientHelper.h"
#include "swift/client/fake_client/FakeSwiftAdminAdapter.h"
#include "swift/client/fake_client/FakeSwiftTransportAdapter.h"
#include "swift/client/fake_client/FakeSwiftTransportClient.h"
#include "swift/monitor/ClientMetricsReporter.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/Atomic.h"
#include "unittest/unittest.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::network;
using namespace swift::monitor;
using ::testing::Field;
using ::testing::Ge;

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
void expect_call_reader(MockClientMetricsReporter &reporter, ClientMetricsCollector &expectCollector) {
    EXPECT_CALL(
        reporter,
        reportReaderMetrics(AllOf(Field(&ClientMetricsCollector::rpcLatency, expectCollector.rpcLatency),
                                  Field(&ClientMetricsCollector::decompressLatency, expectCollector.decompressLatency),
                                  Field(&ClientMetricsCollector::requestMsgCount, expectCollector.requestMsgCount),
                                  Field(&ClientMetricsCollector::hasError, expectCollector.hasError)),
                            _))
        .Times(1);
}
} // namespace monitor
namespace client {

class SwiftSinglePartitionReaderTest : public TESTBASE {
public:
    void setUp() {
        _config.oneRequestReadCount = 128;
        _config.readBufferSize = 1024;
        _config.fatalErrorReportInterval = 3 * 1000 * 1000;
        _config.topicName = "topic_name";
        _config.batchReadCount = 1;
    }
    void tearDown() { SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = false; }
    protocol::ErrorCode setTopicChanged(int64_t topicVersion) {
        _changed = true;
        _version = topicVersion;
        return ERROR_NONE;
    }

private:
    SwiftReaderConfig _config;
    bool _changed = false;
    int64_t _version = 0;
};

TEST_F(SwiftSinglePartitionReaderTest, testHandleGetMessageResponseWithError) {
    uint32_t partitionId = 1;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
    FakeSwiftTransportAdapter<TRT_GETMESSAGE> *fakeAdapter =
        new FakeSwiftTransportAdapter<TRT_GETMESSAGE>(_config.topicName, partitionId);
    FakeSwiftTransportClient *fakeClient = fakeAdapter->getFakeTransportClient();
    EXPECT_EQ(100000, reader._transportAdapter->_retryTimeInterval);
    delete reader._transportAdapter;
    reader._transportAdapter = fakeAdapter;
    reader._lastSuccessResponseTime = 0;
    EXPECT_EQ(500000, reader._transportAdapter->_retryTimeInterval);

    // test error code cannot be handled
    ErrorCode ec = ERROR_UNKNOWN;
    fakeClient->setErrorCode(ec);
    fakeClient->setAutoAsyncCall(true);
    ClientMetricsCollector collector;
    EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
    EXPECT_EQ(ec, reader.handleGetMessageResponse(collector));
    EXPECT_EQ((int64_t)0, reader._lastSuccessResponseTime);

    ec = ERROR_BROKER_INVALID_USER;
    fakeClient->setErrorCode(ec);
    EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
    EXPECT_EQ(ec, reader.handleGetMessageResponse(collector));
    EXPECT_EQ((int64_t)0, reader._lastSuccessResponseTime);

    // test fill partition info failed
    ec = ERROR_CLIENT_INVALID_RESPONSE;
    fakeClient->setErrorCode(ec);
    EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
    EXPECT_EQ(ec, reader.handleGetMessageResponse(collector));
    EXPECT_EQ((int64_t)0, reader._lastSuccessResponseTime);

    // test convert ERROR_BROKER_NO_DATA to ERROR_CLIENT_NO_MORE_MESSAGE
    ec = ERROR_BROKER_NO_DATA;
    fakeClient->setErrorCode(ec);
    EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
    EXPECT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.handleGetMessageResponse(collector));
    EXPECT_TRUE(reader._lastSuccessResponseTime > 0);

    // test return ERROR_SEALED_TOPIC_READ_FINISH, not set _sealedTopicReadFinish
    ec = ERROR_SEALED_TOPIC_READ_FINISH;
    fakeClient->setErrorCode(ec);
    reader._sealedTopicReadFinish = false;
    EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
    reader._transportAdapter->_closure->_response->set_maxmsgid(123);
    reader._transportAdapter->_closure->_response->set_nextmsgid(123);
    reader._transportAdapter->_closure->_response->set_maxtimestamp(1234567);
    reader._transportAdapter->_closure->_response->set_nexttimestamp(1234567);
    reader._buffer._unReadMsgCount.add(1);
    EXPECT_EQ(ERROR_SEALED_TOPIC_READ_FINISH, reader.handleGetMessageResponse(collector));
    EXPECT_EQ(false, reader._sealedTopicReadFinish);

    // test return ERROR_SEALED_TOPIC_READ_FINISH, set _sealedTopicReadFinish
    ec = ERROR_SEALED_TOPIC_READ_FINISH;
    fakeClient->setErrorCode(ec);
    EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
    reader._transportAdapter->_closure->_response->set_maxmsgid(123);
    reader._transportAdapter->_closure->_response->set_nextmsgid(123);
    reader._transportAdapter->_closure->_response->set_maxtimestamp(1234567);
    reader._transportAdapter->_closure->_response->set_nexttimestamp(1234567);
    reader._buffer._unReadMsgCount.decrement();
    EXPECT_EQ(ERROR_SEALED_TOPIC_READ_FINISH, reader.handleGetMessageResponse(collector));
    EXPECT_EQ(true, reader._sealedTopicReadFinish);
    EXPECT_TRUE(reader._lastSuccessResponseTime > 0);
}

TEST_F(SwiftSinglePartitionReaderTest, testHandleGetMessageResponseWithoutError) {
    uint32_t partitionId = 1;
    int64_t msgId = 0;
    uint32_t messageCount = 1200;
    int64_t oneRequestReadCount = 128;
    ClientMetricsCollector collector;
    // return msgs not empty
    {
        SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
        EXPECT_EQ(-1, reader._lastTopicVersion);
        FakeSwiftTransportClient *fakeClient =
            FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 0, reader);
        FakeClientHelper::makeData(fakeClient, messageCount, msgId);
        EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
        EXPECT_EQ(ERROR_NONE, reader.handleGetMessageResponse(collector));
        EXPECT_EQ(oneRequestReadCount, reader._nextMsgId);
        EXPECT_EQ(oneRequestReadCount, reader._buffer.getUnReadMsgCount());
    }

    // return msgs is empty, requeired msgs out of left limit of exist msgs
    {
        SwiftSinglePartitionReader reader(
            SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config, 100);
        EXPECT_EQ(100, reader._lastTopicVersion);
        FakeSwiftTransportClient *fakeClient =
            FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 0, reader);
        msgId = 1500;
        messageCount = 500;
        FakeClientHelper::makeData(fakeClient, messageCount, msgId);
        EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
        EXPECT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, reader.handleGetMessageResponse(collector));
        EXPECT_EQ(oneRequestReadCount, reader._nextMsgId);
        EXPECT_EQ((int64_t)0, reader._buffer.getUnReadMsgCount());
    }

    // return msgs is empty, requeired msgs out of right limit of exist msgs
    {
        SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
        FakeSwiftTransportClient *fakeClient =
            FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 0, reader);
        msgId = 1500;
        messageCount = 500;
        FakeClientHelper::makeData(fakeClient, messageCount, msgId);
        reader._nextMsgId = 2500;
        EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
        EXPECT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.handleGetMessageResponse(collector));
        EXPECT_EQ((int64_t)(msgId + messageCount), reader._nextMsgId);
        EXPECT_EQ((int64_t)0, reader._buffer.getUnReadMsgCount());
    }
}

TEST_F(SwiftSinglePartitionReaderTest, testTryFillBufferReturnValue) {
    uint32_t partitionId = 1;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
    FakeSwiftTransportClient *fakeClient =
        FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 0, reader);

    // last request not done
    fakeClient->setAutoAsyncCall(false);
    EXPECT_TRUE(reader._transportAdapter->isLastRequestDone());
    int64_t interval = reader.tryFillBuffer();
    EXPECT_EQ(interval, numeric_limits<int64_t>::max());
    EXPECT_TRUE(!reader._transportAdapter->isLastRequestDone());
    interval = reader.tryFillBuffer();
    EXPECT_EQ(interval, numeric_limits<int64_t>::max());
    fakeClient->finishAsyncCall();
    EXPECT_TRUE(reader._transportAdapter->isLastRequestDone());

    // adapter retry interval > 0
    // last request not handled
    FakeSwiftTransportAdapter<TRT_GETMESSAGE> *fakeAdapter =
        dynamic_cast<FakeSwiftTransportAdapter<TRT_GETMESSAGE> *>(reader._transportAdapter);
    EXPECT_TRUE(fakeAdapter);
    int64_t expectedInterval = 6;
    fakeAdapter->setRetryInterval(expectedInterval);
    interval = reader.tryFillBuffer();
    EXPECT_EQ(expectedInterval, interval);
    // last request handled
    interval = reader.tryFillBuffer();
    EXPECT_EQ(expectedInterval, interval);

    // RETRY_GET_MESSAGE_INTERVAL - ci
    fakeAdapter->setRetryInterval(0);
    reader._nextMsgId = 2;
    reader._partitionStatus.maxMessageId = 1;
    int64_t currentTime = TimeUtility::currentTime();
    reader._partitionStatus.refreshTime = currentTime - 1;
    interval = reader.tryFillBuffer();
    EXPECT_TRUE(interval > 0);
    EXPECT_TRUE(interval < numeric_limits<int64_t>::max());
}

TEST_F(SwiftSinglePartitionReaderTest, testTryFillBufferReturnValueTopicEnableLongPolling) {
    uint32_t partitionId = 1;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
    reader._isTopicLongPollingEnabled = true;
    FakeSwiftTransportClient *fakeClient =
        FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 0, reader);

    // last request not done
    fakeClient->setAutoAsyncCall(false);
    EXPECT_TRUE(reader._transportAdapter->isLastRequestDone());
    int64_t interval = reader.tryFillBuffer();
    EXPECT_EQ(interval, numeric_limits<int64_t>::max());
    EXPECT_TRUE(!reader._transportAdapter->isLastRequestDone());
    interval = reader.tryFillBuffer();
    EXPECT_EQ(interval, numeric_limits<int64_t>::max());
    fakeClient->finishAsyncCall();
    EXPECT_TRUE(reader._transportAdapter->isLastRequestDone());

    // adapter retry interval > 0
    // last request not handled
    FakeSwiftTransportAdapter<TRT_GETMESSAGE> *fakeAdapter =
        dynamic_cast<FakeSwiftTransportAdapter<TRT_GETMESSAGE> *>(reader._transportAdapter);
    EXPECT_TRUE(fakeAdapter);
    int64_t expectedInterval = 6;
    fakeAdapter->setRetryInterval(expectedInterval);
    interval = reader.tryFillBuffer();
    EXPECT_EQ(expectedInterval, interval);
    // last request handled
    interval = reader.tryFillBuffer();
    EXPECT_EQ(expectedInterval, interval);

    // long polling enabled, need not wait for retry, send another GetMessageRequest
    fakeAdapter->setRetryInterval(0);
    reader._nextMsgId = 2;
    reader._partitionStatus.maxMessageId = 1;
    int64_t currentTime = TimeUtility::currentTime();
    reader._partitionStatus.refreshTime = currentTime - 1;
    bool isSent = false;
    interval = reader.tryFillBuffer(currentTime, false, isSent);
    ASSERT_TRUE(isSent);
    ASSERT_EQ(numeric_limits<int64_t>::max(), interval);

    // long polling enabled, get last request has error, need wait for retry
    fakeClient->setErrorCode(ERROR_UNKNOWN);
    fakeClient->finishAsyncCall();
    currentTime = TimeUtility::currentTime();
    reader._partitionStatus.refreshTime = currentTime - 1;
    isSent = false;
    interval = reader.tryFillBuffer(currentTime, false, isSent);
    ASSERT_FALSE(isSent);
    EXPECT_TRUE(interval > 0);
    EXPECT_TRUE(interval < numeric_limits<int64_t>::max());

    // long polling disabled, need wait for retry
    fakeClient->setErrorCode(ERROR_NONE);
    fakeClient->finishAsyncCall();
    fakeAdapter->setRetryInterval(0);
    reader._isTopicLongPollingEnabled = false;
    reader._nextMsgId = 2;
    reader._partitionStatus.maxMessageId = 1;
    currentTime = TimeUtility::currentTime();
    reader._partitionStatus.refreshTime = currentTime - 1;
    isSent = false;
    interval = reader.tryFillBuffer(currentTime, false, isSent);
    ASSERT_FALSE(isSent);
    EXPECT_TRUE(interval > 0);
    EXPECT_TRUE(interval < numeric_limits<int64_t>::max());
}

TEST_F(SwiftSinglePartitionReaderTest, testTryFillBuffer) {
    uint32_t partitionId = 1;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
    FakeSwiftTransportClient *fakeClient =
        FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 0, reader);
    // handle response failed
    ErrorCode ec = ERROR_UNKNOWN;
    fakeClient->setErrorCode(ec);
    reader.tryFillBuffer();
    EXPECT_EQ(ERROR_NONE, reader._lastErrorCode);
    reader.tryFillBuffer();
    EXPECT_EQ(ec, reader._lastErrorCode);

    // should not post read request when buffer >
    int64_t oneRequestReadCount = 128;
    int64_t readBufferCount = 1024;
    fakeClient->setErrorCode(ERROR_NONE);
    int64_t msgId = 0;
    uint32_t messageCount = 3000;
    FakeClientHelper::makeData(fakeClient, messageCount, msgId);

    reader.tryFillBuffer();
    EXPECT_EQ(ec, reader._lastErrorCode);
    ClientMetricsCollector collector;
    EXPECT_EQ(ERROR_NONE, reader.handleGetMessageResponse(collector));
    EXPECT_EQ((int64_t)oneRequestReadCount, reader._buffer.getUnReadMsgCount());
    fakeClient->setAutoAsyncCall(false);
    Messages msgs;
    for (uint32_t i = 0; i < readBufferCount; ++i) {
        EXPECT_TRUE(reader.tryRead(msgs));
        reader.tryFillBuffer();
        EXPECT_EQ(reader._buffer.getUnReadMsgCount() < readBufferCount, NULL != fakeClient->_getClosure);
        fakeClient->finishAsyncCall();
    }
}

TEST_F(SwiftSinglePartitionReaderTest, testGetMinMessageIdByTimeWithPostRequestFailed) {
    uint32_t partitionId = 1;
    int64_t timestamp = 0;
    int64_t msgid = 0;
    int64_t msgTime = -1;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
    FakeSwiftTransportAdapter<TRT_GETMINMESSAGEIDBYTIME> *transportAdapter =
        new FakeSwiftTransportAdapter<TRT_GETMINMESSAGEIDBYTIME>(_config.topicName, partitionId);
    reader.resetMsgIdTransportAdapter(transportAdapter);
    // post request failed
    ErrorCode ec = ERROR_UNKNOWN;
    transportAdapter->setErrorCode(ec);
    EXPECT_EQ(ec, reader.getMinMessageIdByTime(timestamp, msgid, msgTime));
}

TEST_F(SwiftSinglePartitionReaderTest, testPostGetMessageRequestFailed) {
    uint32_t partitionId = 1;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
    DELETE_AND_SET_NULL(reader._transportAdapter);
    FakeSwiftTransportAdapter<TRT_GETMESSAGE> *fakeAdapter =
        new FakeSwiftTransportAdapter<TRT_GETMESSAGE>(_config.topicName, partitionId);
    reader._transportAdapter = fakeAdapter;

    // post request failed
    Message msg;
    ErrorCode ec = ERROR_UNKNOWN;
    fakeAdapter->setErrorCode(ec);
    reader.tryFillBuffer();
    EXPECT_EQ(ec, reader.getLastErrorCode());
}

TEST_F(SwiftSinglePartitionReaderTest, testGetMinMessageIdByTime) {
    uint32_t partitionId = 1;
    int64_t timestamp = 0;
    int64_t msgid = 0;
    int64_t msgTime = -1;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
    FakeSwiftTransportAdapter<TRT_GETMINMESSAGEIDBYTIME> *transportAdapter =
        new FakeSwiftTransportAdapter<TRT_GETMINMESSAGEIDBYTIME>(_config.topicName, partitionId);
    reader.resetMsgIdTransportAdapter(transportAdapter);
    FakeSwiftTransportClient *fakeClient = transportAdapter->getFakeTransportClient();
    fakeClient->setAutoAsyncCall(true);

    // test error code cannot be handled
    ErrorCode ec = ERROR_UNKNOWN;
    fakeClient->setErrorCode(ec);
    EXPECT_EQ(ec, reader.getMinMessageIdByTime(timestamp, msgid, msgTime));

    // test fill partition Info failed
    ec = ERROR_CLIENT_INVALID_RESPONSE;
    fakeClient->setErrorCode(ec);
    EXPECT_EQ(ec, reader.getMinMessageIdByTime(timestamp, msgid, msgTime));

    // test ERROR_NONE
    ec = ERROR_NONE;
    fakeClient->setErrorCode(ec);
    msgid = 0;
    uint32_t messageCount = 2000;
    FakeClientHelper::makeData(fakeClient, messageCount, msgid);
    int64_t expectedMsgId = messageCount / 2;
    timestamp = expectedMsgId * 3;
    EXPECT_EQ(ec, reader.getMinMessageIdByTime(timestamp, msgid, msgTime));
    EXPECT_EQ(expectedMsgId, msgid);
    EXPECT_EQ(timestamp, msgTime);

    // test ERROR_BROKER_TIMESTAMP_TOO_LATEST
    expectedMsgId = messageCount;
    timestamp = (messageCount + 1000) * 3;
    EXPECT_EQ(ERROR_NONE, reader.getMinMessageIdByTime(timestamp, msgid, msgTime));
    EXPECT_EQ(expectedMsgId, msgid);
    EXPECT_EQ(timestamp, msgTime);

    // test ERROR_BROKER_NO_DATA
    ec = ERROR_NONE;
    fakeClient->setErrorCode(ec);
    fakeClient->clearMsg();
    EXPECT_EQ(ERROR_NONE, reader.getMinMessageIdByTime(timestamp, msgid, msgTime));
    EXPECT_EQ(int64_t(0), msgid);
    EXPECT_EQ((int64_t)-1, msgTime);
}

TEST_F(SwiftSinglePartitionReaderTest, testGetPartitionStatus) {
    uint32_t partitionId = 1;
    SwiftPartitionStatus partitionStatus;
    partitionStatus.refreshTime = 100;
    partitionStatus.maxMessageId = 0;
    partitionStatus.maxMessageTimestamp = 0;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
    reader._partitionStatus = partitionStatus;
    FakeSwiftTransportAdapter<TRT_GETMAXMESSAGEID> *fakeAdapter =
        new FakeSwiftTransportAdapter<TRT_GETMAXMESSAGEID>(_config.topicName, partitionId);
    reader.resetMaxMsgIdTransportAdapter(fakeAdapter);
    FakeSwiftTransportClient *fakeClient = fakeAdapter->getFakeTransportClient();
    fakeClient->setAutoAsyncCall(true);
    ErrorCode ec = ERROR_UNKNOWN;
    fakeClient->setErrorCode(ec);
    int64_t curTime = 15 * 1000 * 1000;
    SwiftPartitionStatus testStatus = reader.getSwiftPartitionStatus(curTime);
    EXPECT_EQ((int64_t)100, testStatus.refreshTime);
    EXPECT_EQ((int64_t)0, testStatus.maxMessageId);
    EXPECT_EQ((int64_t)0, testStatus.maxMessageTimestamp);

    reader._partitionStatus.refreshTime = 101;
    curTime = 30 * 1000 * 1000;
    testStatus = reader.getSwiftPartitionStatus(curTime);
    EXPECT_EQ((int64_t)101, testStatus.refreshTime);
    EXPECT_EQ((int64_t)0, testStatus.maxMessageId);
    EXPECT_EQ((int64_t)0, testStatus.maxMessageTimestamp);

    // test fill partition Info failed
    curTime = 45 * 1000 * 1000;
    ec = ERROR_CLIENT_INVALID_RESPONSE;
    fakeClient->setErrorCode(ec);
    testStatus = reader.getSwiftPartitionStatus(curTime);
    EXPECT_EQ((int64_t)101, testStatus.refreshTime);
    EXPECT_EQ((int64_t)0, testStatus.maxMessageId);
    EXPECT_EQ((int64_t)0, testStatus.maxMessageTimestamp);

    // test ERROR_NONE
    curTime = 60 * 1000 * 1000;
    ec = ERROR_NONE;
    fakeClient->setErrorCode(ec);
    int64_t msgid = 10;
    uint32_t messageCount = 1;
    FakeClientHelper::makeData(fakeClient, messageCount, msgid);
    testStatus = reader.getSwiftPartitionStatus(curTime);
    EXPECT_EQ((int64_t)101, testStatus.refreshTime);
    EXPECT_EQ((int64_t)0, testStatus.maxMessageId);
    EXPECT_EQ((int64_t)0, testStatus.maxMessageTimestamp);

    curTime = 75 * 1000 * 1000;
    testStatus = reader.getSwiftPartitionStatus(curTime);
    EXPECT_EQ((int64_t)60 * 1000 * 1000, testStatus.refreshTime);
    EXPECT_EQ((int64_t)10, testStatus.maxMessageId);
    EXPECT_EQ((int64_t)30, testStatus.maxMessageTimestamp);

    curTime = 76 * 1000 * 1000;
    testStatus = reader.getSwiftPartitionStatus(curTime);
    EXPECT_EQ((int64_t)75 * 1000 * 1000, testStatus.refreshTime);
    EXPECT_EQ((int64_t)10, testStatus.maxMessageId);
    EXPECT_EQ((int64_t)30, testStatus.maxMessageTimestamp);

    curTime = 77 * 1000 * 1000;
    testStatus = reader.getSwiftPartitionStatus(curTime);
    EXPECT_EQ((int64_t)75 * 1000 * 1000, testStatus.refreshTime);
    EXPECT_EQ((int64_t)10, testStatus.maxMessageId);
    EXPECT_EQ((int64_t)30, testStatus.maxMessageTimestamp);

    // test ERROR_BROKER_NO_DATA
    curTime = 90 * 1000 * 1000;
    ec = ERROR_NONE;
    fakeClient->setErrorCode(ec);
    fakeClient->clearMsg();
    testStatus = reader.getSwiftPartitionStatus(curTime);
    EXPECT_EQ((int64_t)75 * 1000 * 1000, testStatus.refreshTime);
    EXPECT_EQ((int64_t)10, testStatus.maxMessageId);
    EXPECT_EQ((int64_t)30, testStatus.maxMessageTimestamp);

    curTime = 105 * 1000 * 1000;
    testStatus = reader.getSwiftPartitionStatus(curTime);
    EXPECT_EQ((int64_t)90 * 1000 * 1000, testStatus.refreshTime);
    EXPECT_EQ((int64_t)-1, testStatus.maxMessageId);
    EXPECT_EQ((int64_t)-1, testStatus.maxMessageTimestamp);
}

TEST_F(SwiftSinglePartitionReaderTest, testFillPartitionInfo) {
    uint32_t partitionId = 1;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);

    // test ERROR_BROKER_NO_DATA
    ErrorCode ec = ERROR_BROKER_NO_DATA;
    int64_t refreshTime = 10000;
    MessageResponse response;
    ErrorInfo *errorInfo = response.mutable_errorinfo();
    errorInfo->set_errcode(ec);
    EXPECT_EQ(ERROR_NONE, reader.fillPartitionInfo(refreshTime, response));
    EXPECT_EQ(refreshTime, reader._partitionStatus.refreshTime);
    EXPECT_EQ((int64_t)-1, reader._partitionStatus.maxMessageId);
    EXPECT_EQ((int64_t)-1, reader._partitionStatus.maxMessageTimestamp);

    // test ERROR_CLIENT_INVALID_RESPONSE
    errorInfo->set_errcode(ERROR_NONE);
    EXPECT_EQ(ERROR_CLIENT_INVALID_RESPONSE, reader.fillPartitionInfo(refreshTime, response));

    // test ERROR_NONE
    int64_t maxMsgId = 5;
    int64_t maxTimeStamp = 5000;
    response.set_maxmsgid(maxMsgId);
    response.set_maxtimestamp(maxTimeStamp);
    EXPECT_EQ(ERROR_NONE, reader.fillPartitionInfo(refreshTime, response));
    EXPECT_EQ(maxMsgId, reader._partitionStatus.maxMessageId);
    EXPECT_EQ(maxTimeStamp, reader._partitionStatus.maxMessageTimestamp);
}

TEST_F(SwiftSinglePartitionReaderTest, testSeekByTimestamp) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter();
    fakeAdapter->setErrorCode(ERROR_CLIENT_GET_ADMIN_INFO_FAILED);
    SwiftAdminAdapterPtr adminAdapter(fakeAdapter);
    uint32_t partitionId = 1;
    SwiftSinglePartitionReader reader(adminAdapter, SwiftRpcChannelManagerPtr(), partitionId, _config);

    int64_t t1 = TimeUtility::currentTime();
    ErrorCode ec = reader.seekByTimestamp(TimeUtility::currentTime());
    int64_t t2 = TimeUtility::currentTime();
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);
    EXPECT_TRUE((t2 - t1) < SwiftSinglePartitionReader::SEEKBYTIMESTAMP_ERROR_INTERVAL);

    for (int32_t i = 0; i < 3; ++i) {
        t1 = TimeUtility::currentTime();
        ec = reader.seekByTimestamp(TimeUtility::currentTime());
        t2 = TimeUtility::currentTime();
        EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);
        EXPECT_TRUE((t2 - t1) > SwiftSinglePartitionReader::SEEKBYTIMESTAMP_ERROR_INTERVAL);
    }

    reader._seekByTimestampRecord.lastErrorCode = ERROR_NONE;
    t1 = TimeUtility::currentTime();
    ec = reader.seekByTimestamp(TimeUtility::currentTime());
    t2 = TimeUtility::currentTime();
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);
    EXPECT_TRUE((t2 - t1) < SwiftSinglePartitionReader::SEEKBYTIMESTAMP_ERROR_INTERVAL);

    usleep(SwiftSinglePartitionReader::SEEKBYTIMESTAMP_ERROR_INTERVAL);
    t1 = TimeUtility::currentTime();
    ec = reader.seekByTimestamp(TimeUtility::currentTime());
    t2 = TimeUtility::currentTime();
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);
    EXPECT_TRUE((t2 - t1) < SwiftSinglePartitionReader::SEEKBYTIMESTAMP_ERROR_INTERVAL);
}

TEST_F(SwiftSinglePartitionReaderTest, testSeekByMessageId) {
    uint32_t partitionId = 1;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
    EXPECT_EQ(ERROR_NONE, reader.seekByMessageId((int64_t)0));
    EXPECT_EQ(int64_t(0), reader._nextMsgId);
}

TEST_F(SwiftSinglePartitionReaderTest, testReportFatalError) {
    uint32_t partitionId = 1;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);

    // does not have fatal error
    reader._lastErrorCode = ERROR_NONE;
    int64_t timestamp = TimeUtility::currentTime() - 1;
    reader._lastReportErrorTime = timestamp;
    ErrorCode ec = reader.reportFatalError();
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_TRUE(reader._lastReportErrorTime == timestamp);

    // has fatal error, but not report
    ErrorCode expectedEc = ERROR_UNKNOWN;
    reader._lastSuccessResponseTime = 0;
    reader._lastErrorCode = expectedEc;
    timestamp = TimeUtility::currentTime() - 1;
    reader._lastReportErrorTime = timestamp;
    ec = reader.reportFatalError();
    EXPECT_EQ(ERROR_NONE, ec);

    reader._lastErrorCode = ERROR_BROKER_SOME_MESSAGE_LOST;
    reader._lastReportErrorTime = 0;
    ec = reader.reportFatalError();
    EXPECT_EQ(ERROR_NONE, ec);

    // has fatal error, and report
    reader._lastErrorCode = ERROR_UNKNOWN;
    reader._lastReportErrorTime = 0;
    ec = reader.reportFatalError(false);
    EXPECT_EQ(ERROR_UNKNOWN, ec);
    EXPECT_EQ(ERROR_UNKNOWN, reader._lastErrorCode);
    EXPECT_EQ(0, reader._lastReportErrorTime);
    ec = reader.reportFatalError(true);
    EXPECT_EQ(ERROR_UNKNOWN, ec);
    EXPECT_EQ(ERROR_NONE, reader._lastErrorCode);
    EXPECT_TRUE(0 < reader._lastReportErrorTime);

    // sealed error
    reader._lastErrorCode = ERROR_SEALED_TOPIC_READ_FINISH;
    reader._lastReportErrorTime = 0;
    EXPECT_EQ(ERROR_SEALED_TOPIC_READ_FINISH, reader.reportFatalError(false));
    EXPECT_EQ(ERROR_SEALED_TOPIC_READ_FINISH, reader.reportFatalError(true));
    EXPECT_EQ(ERROR_SEALED_TOPIC_READ_FINISH, reader._lastErrorCode);
    EXPECT_EQ(0, reader._lastReportErrorTime);
}

// test bug#203350
TEST_F(SwiftSinglePartitionReaderTest, testReportFatalErrorMultiTimes) {
    uint32_t partitionId = 1;
    _config.fatalErrorReportInterval = 3 * 1000; // 3 ms
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
    ErrorCode expectedEc = ERROR_UNKNOWN;
    reader._lastErrorCode = expectedEc;
    reader._lastReportErrorTime = 0;
    reader._lastSuccessResponseTime = 0;
    ErrorCode ec = reader.reportFatalError();
    EXPECT_EQ(expectedEc, ec);
    usleep(5 * 1000); // 5 ms
    ec = reader.reportFatalError();
    EXPECT_EQ(ERROR_NONE, ec);
}

TEST_F(SwiftSinglePartitionReaderTest, testCheckCurrentError) {
    uint32_t partitionId = 1;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);

    // has not error
    ErrorCode expectedEc = ERROR_UNKNOWN;
    reader._lastSuccessResponseTime = TimeUtility::currentTime() - 1;
    reader.checkErrorCode(expectedEc);
    ErrorCode ec = ERROR_NONE;
    string errorMsg;
    reader.checkCurrentError(ec, errorMsg);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(string(""), errorMsg);
    reader._lastSuccessResponseTime = 0;
    reader.checkCurrentError(ec, errorMsg);
    EXPECT_EQ(expectedEc, ec);
    EXPECT_TRUE(errorMsg.find("last errorCode[1]") != string::npos);
    EXPECT_TRUE(errorMsg.find("topic name") != string::npos);
}

TEST_F(SwiftSinglePartitionReaderTest, testGetCheckpointTimestampRefreshMode) {
    uint32_t partitionId = 1;
    auto config = _config;
    config.batchReadCount = 5;
    config.checkpointRefreshTimestampOffset = 0;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config);
    FakeSwiftTransportClient *fakeClient =
        FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 0, reader);
    FakeClientHelper::makeData(fakeClient, 10, 0, 1, "", 1);
    reader.tryFillBuffer();
    EXPECT_EQ((int64_t)-1, reader.getNextMsgTimestamp());
    std::pair<int64_t, int32_t> p1(0, 0);
    EXPECT_TRUE(p1 == reader.getCheckpointTimestamp());
    fakeClient->finishAsyncCall();
    EXPECT_TRUE(reader._transportAdapter->isLastRequestDone());
    reader.tryFillBuffer();

    EXPECT_EQ((int64_t)1, reader.getNextMsgTimestamp());
    auto p2 = std::make_pair<int64_t, int32_t>(1, 0);
    EXPECT_EQ(p2, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)11, reader._nextTimestamp);
    Messages msgs;
    EXPECT_TRUE(reader.tryRead(msgs));
    ASSERT_EQ(5, msgs.msgs_size());
    EXPECT_EQ(int64_t(0), msgs.msgs(0).msgid());
    EXPECT_EQ(int64_t(1), msgs.msgs(0).timestamp());
    EXPECT_EQ((int64_t)6, reader.getNextMsgTimestamp());
    auto p3 = std::make_pair<int64_t, int32_t>(6, 0);
    EXPECT_EQ(p3, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)11, reader._nextTimestamp);
    msgs.clear_msgs();
    EXPECT_TRUE(reader.tryRead(msgs));
    ASSERT_EQ(5, msgs.msgs_size());
    EXPECT_EQ(int64_t(5), msgs.msgs(0).msgid());
    EXPECT_EQ(int64_t(6), msgs.msgs(0).timestamp());
    EXPECT_EQ((int64_t)11, reader.getNextMsgTimestamp());
    auto p4 = std::make_pair<int64_t, int32_t>(11, 0);
    EXPECT_EQ(p4, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)11, reader._nextTimestamp);

    reader._nextTimestamp = 100;
    EXPECT_EQ((int64_t)100, reader.getNextMsgTimestamp());
    auto p5 = std::make_pair<int64_t, int32_t>(100, 0);
    EXPECT_EQ(p5, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)100, reader._nextTimestamp);
}

TEST_F(SwiftSinglePartitionReaderTest, testGetCheckpointTimestampWithTimestampOffset) {
    uint32_t partitionId = 1;
    auto config = _config;
    config.batchReadCount = 5;
    config.checkpointRefreshTimestampOffset = 10;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config);
    FakeSwiftTransportClient *fakeClient =
        FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 0, reader);
    FakeClientHelper::makeData(fakeClient, 10, 0, 1, "", 1);
    reader.tryFillBuffer();
    EXPECT_EQ((int64_t)-1, reader.getNextMsgTimestamp());
    auto p0 = std::make_pair<int64_t, int32_t>(0, 0);
    EXPECT_EQ(p0, reader.getCheckpointTimestamp());
    fakeClient->finishAsyncCall();
    EXPECT_TRUE(reader._transportAdapter->isLastRequestDone());
    reader.tryFillBuffer();

    EXPECT_EQ((int64_t)1, reader.getNextMsgTimestamp());
    auto p1 = std::make_pair<int64_t, int32_t>(1, 0);
    EXPECT_EQ(p1, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)11, reader._nextTimestamp);
    Messages msgs;
    EXPECT_TRUE(reader.tryRead(msgs));
    ASSERT_EQ(5, msgs.msgs_size());
    EXPECT_EQ(int64_t(0), msgs.msgs(0).msgid());
    EXPECT_EQ(int64_t(1), msgs.msgs(0).timestamp());
    EXPECT_EQ((int64_t)6, reader.getNextMsgTimestamp());
    auto p2 = std::make_pair<int64_t, int32_t>(6, 0);
    EXPECT_EQ(p2, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)11, reader._nextTimestamp);
    msgs.clear_msgs();
    EXPECT_TRUE(reader.tryRead(msgs));
    ASSERT_EQ(5, msgs.msgs_size());
    EXPECT_EQ(int64_t(5), msgs.msgs(0).msgid());
    EXPECT_EQ(int64_t(6), msgs.msgs(0).timestamp());
    EXPECT_EQ((int64_t)11, reader.getNextMsgTimestamp());
    auto p3 = std::make_pair<int64_t, int32_t>(11, 0);
    EXPECT_EQ(p3, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)11, reader._nextTimestamp);
    reader._nextTimestamp = 100;
    EXPECT_EQ((int64_t)100, reader.getNextMsgTimestamp());
    auto p4 = std::make_pair<int64_t, int32_t>(90, 0);
    EXPECT_EQ(p4, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)100, reader._nextTimestamp);
}

TEST_F(SwiftSinglePartitionReaderTest, testGetCheckpointTimestampReadedMode) {
    uint32_t partitionId = 1;
    auto config = _config;
    config.batchReadCount = 5;
    config.checkpointMode = "readed";
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config);
    FakeSwiftTransportClient *fakeClient =
        FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 0, reader);
    FakeClientHelper::makeData(fakeClient, 10, 0, 1, "", 1);
    reader.tryFillBuffer();
    EXPECT_EQ((int64_t)-1, reader.getNextMsgTimestamp());
    auto p0 = std::make_pair<int64_t, int32_t>(0, 0);
    EXPECT_EQ(p0, reader.getCheckpointTimestamp());
    fakeClient->finishAsyncCall();
    EXPECT_TRUE(reader._transportAdapter->isLastRequestDone());
    reader.tryFillBuffer();

    EXPECT_EQ((int64_t)1, reader.getNextMsgTimestamp());
    auto p1 = std::make_pair<int64_t, int32_t>(1, 0);
    EXPECT_EQ(p1, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)11, reader._nextTimestamp);
    Messages msgs;
    EXPECT_TRUE(reader.tryRead(msgs));
    ASSERT_EQ(5, msgs.msgs_size());
    EXPECT_EQ(int64_t(0), msgs.msgs(0).msgid());
    EXPECT_EQ(int64_t(1), msgs.msgs(0).timestamp());
    EXPECT_EQ((int64_t)6, reader.getNextMsgTimestamp());
    auto p2 = std::make_pair<int64_t, int32_t>(6, 0);
    EXPECT_EQ(p2, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)11, reader._nextTimestamp);
    msgs.clear_msgs();
    EXPECT_TRUE(reader.tryRead(msgs));
    ASSERT_EQ(5, msgs.msgs_size());
    EXPECT_EQ(int64_t(5), msgs.msgs(0).msgid());
    EXPECT_EQ(int64_t(6), msgs.msgs(0).timestamp());
    EXPECT_EQ((int64_t)11, reader.getNextMsgTimestamp());
    auto p3 = std::make_pair<int64_t, int32_t>(11, 0);
    EXPECT_EQ(p3, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)11, reader._nextTimestamp);

    reader._nextTimestamp = 100;
    EXPECT_EQ((int64_t)100, reader.getNextMsgTimestamp());
    auto p4 = std::make_pair<int64_t, int32_t>(11, 0);
    EXPECT_EQ(p4, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)100, reader._nextTimestamp);
}

TEST_F(SwiftSinglePartitionReaderTest, testGetCheckpointTimestampWithMergedMsg) {
    uint32_t partitionId = 1;
    auto config = _config;
    config.batchReadCount = 5;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, config);
    FakeSwiftTransportClient *fakeClient =
        FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 0, reader);
    FakeClientHelper::makeData(fakeClient, 10, 0, 1, "", 1, true);
    reader.tryFillBuffer();
    EXPECT_EQ((int64_t)-1, reader.getNextMsgTimestamp());
    auto p0 = std::make_pair<int64_t, int32_t>(0, 0);
    EXPECT_EQ(p0, reader.getCheckpointTimestamp());
    fakeClient->finishAsyncCall();
    EXPECT_TRUE(reader._transportAdapter->isLastRequestDone());
    reader.tryFillBuffer();

    EXPECT_EQ((int64_t)1, reader.getNextMsgTimestamp());
    auto p1 = std::make_pair<int64_t, int32_t>(1, 0);
    EXPECT_EQ(p1, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)2, reader._nextTimestamp);
    Messages msgs;
    EXPECT_TRUE(reader.tryRead(msgs));
    ASSERT_EQ(5, msgs.msgs_size());
    EXPECT_EQ(int64_t(0), msgs.msgs(0).msgid());
    EXPECT_EQ(int64_t(1), msgs.msgs(0).timestamp());
    EXPECT_EQ((int64_t)1, reader.getNextMsgTimestamp());
    auto p2 = std::make_pair<int64_t, int32_t>(1, 5);
    EXPECT_EQ(p2, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)2, reader._nextTimestamp);
    msgs.clear_msgs();
    EXPECT_TRUE(reader.tryRead(msgs));
    ASSERT_EQ(5, msgs.msgs_size());
    EXPECT_EQ(int64_t(0), msgs.msgs(0).msgid());
    EXPECT_EQ(int64_t(1), msgs.msgs(0).timestamp());
    EXPECT_EQ((int64_t)2, reader.getNextMsgTimestamp());
    auto p3 = std::make_pair<int64_t, int32_t>(2, 0);
    EXPECT_EQ(p3, reader.getCheckpointTimestamp());
    EXPECT_EQ((int64_t)2, reader._nextTimestamp);
    int64_t nextTimestamp = SwiftReaderConfig::DEFAULT_CHECKPOINT_REFRESH_TIMESTAMP_OFFSET + 100;
    reader._nextTimestamp = nextTimestamp;
    EXPECT_EQ(nextTimestamp, reader.getNextMsgTimestamp());
    auto p4 = std::make_pair<int64_t, int32_t>(100, 0);
    EXPECT_EQ(p4, reader.getCheckpointTimestamp());
    EXPECT_EQ(nextTimestamp, reader._nextTimestamp);
}

TEST_F(SwiftSinglePartitionReaderTest, testUpdateNextMsgTimestamp) {
    uint32_t partitionId = 1;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
    FakeSwiftTransportClient *fakeClient =
        FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 0, reader);
    uint32_t messageCount = 2;
    int64_t msgId = 0;
    FakeClientHelper::makeData(fakeClient, messageCount, msgId);

    reader.tryFillBuffer();
    EXPECT_EQ((int64_t)-1, reader.getNextMsgTimestamp());
    fakeClient->finishAsyncCall();
    EXPECT_TRUE(reader._transportAdapter->isLastRequestDone());
    reader.tryFillBuffer();

    EXPECT_EQ((int64_t)0, reader.getNextMsgTimestamp());
    EXPECT_EQ((int64_t)4, reader._nextTimestamp);
    Messages msgs;
    EXPECT_TRUE(reader.tryRead(msgs));
    EXPECT_EQ(int64_t(0), msgs.msgs(0).msgid());
    EXPECT_EQ(int64_t(0), msgs.msgs(0).timestamp());
    EXPECT_EQ((int64_t)3, reader.getNextMsgTimestamp());
    msgs.clear_msgs();
    EXPECT_TRUE(reader.tryRead(msgs));
    EXPECT_EQ(int64_t(1), msgs.msgs(0).msgid());
    EXPECT_EQ(int64_t(3), msgs.msgs(0).timestamp());
    EXPECT_EQ((int64_t)4, reader.getNextMsgTimestamp());
}

TEST_F(SwiftSinglePartitionReaderTest, testUpdateNextMsgTimestampBySeek) {
    SwiftAdminAdapterPtr fakeAdapter(new FakeSwiftAdminAdapter());
    uint32_t partitionId = 1;
    uint32_t messageCount = 5;
    int64_t msgId = 0;
    SwiftSinglePartitionReader *reader =
        new SwiftSinglePartitionReader(fakeAdapter, SwiftRpcChannelManagerPtr(), partitionId, _config);
    FakeSwiftTransportClient *fakeClient = FakeClientHelper::prepareTransportClientForReader(
        _config.topicName, partitionId, true, 0, *reader, messageCount, msgId);
    {
        // seek before read message
        EXPECT_TRUE(ERROR_NONE == reader->seekByTimestamp(9));
        reader->tryFillBuffer();
        EXPECT_EQ((int64_t)9, reader->getNextMsgTimestamp());
        fakeClient->finishAsyncCall();
        EXPECT_TRUE(reader->_transportAdapter->isLastRequestDone());
        reader->tryFillBuffer();
        EXPECT_EQ((int64_t)9, reader->getNextMsgTimestamp());
        EXPECT_EQ((int64_t)13, reader->_nextTimestamp);
        Messages msgs;
        EXPECT_TRUE(reader->tryRead(msgs));
        EXPECT_EQ(int64_t(3), msgs.msgs(0).msgid());
        EXPECT_EQ(int64_t(9), msgs.msgs(0).timestamp());
        EXPECT_EQ((int64_t)12, reader->getNextMsgTimestamp());
        EXPECT_TRUE(reader->tryRead(msgs));
        EXPECT_EQ(int64_t(4), msgs.msgs(0).msgid());
        EXPECT_EQ(int64_t(12), msgs.msgs(0).timestamp());
        EXPECT_EQ((int64_t)13, reader->getNextMsgTimestamp());
    }
    EXPECT_TRUE(ERROR_NONE == reader->seekByTimestamp(-1));
    {
        // read 2 messages and seek to the second message
        reader->tryFillBuffer();
        EXPECT_EQ((int64_t)0, reader->getNextMsgTimestamp());
        fakeClient->finishAsyncCall();
        EXPECT_TRUE(reader->_transportAdapter->isLastRequestDone());
        reader->tryFillBuffer();
        EXPECT_EQ((int64_t)0, reader->getNextMsgTimestamp());
        EXPECT_EQ((int64_t)13, reader->_nextTimestamp);
        Messages msgs;
        EXPECT_TRUE(reader->tryRead(msgs));
        EXPECT_EQ(int64_t(0), msgs.msgs(0).msgid());
        EXPECT_EQ(int64_t(0), msgs.msgs(0).timestamp());
        EXPECT_EQ((int64_t)3, reader->getNextMsgTimestamp());
        EXPECT_TRUE(reader->tryRead(msgs));
        EXPECT_EQ(int64_t(1), msgs.msgs(0).msgid());
        EXPECT_EQ(int64_t(3), msgs.msgs(0).timestamp());
        EXPECT_EQ((int64_t)6, reader->getNextMsgTimestamp());
        reader->seekByTimestamp(3);
        reader->tryFillBuffer();
        EXPECT_EQ((int64_t)3, reader->getNextMsgTimestamp());
        fakeClient->finishAsyncCall();
        EXPECT_TRUE(reader->_transportAdapter->isLastRequestDone());
        reader->tryFillBuffer();
        EXPECT_EQ((int64_t)3, reader->getNextMsgTimestamp());
        EXPECT_EQ((int64_t)13, reader->_nextTimestamp);
    }
    delete reader;
}

TEST_F(SwiftSinglePartitionReaderTest, testSeekFutureTime) {
    SwiftAdminAdapterPtr fakeAdapter(new FakeSwiftAdminAdapter());
    uint32_t partitionId = 1;
    SwiftSinglePartitionReader *reader =
        new SwiftSinglePartitionReader(fakeAdapter, SwiftRpcChannelManagerPtr(), partitionId, _config);
    uint32_t messageCount = 5;
    int64_t msgId = 0;
    FakeClientHelper::prepareTransportClientForReader(
        _config.topicName, partitionId, true, 0, *reader, messageCount, msgId);
    int64_t seekTime = TimeUtility::currentTime() + 100000;
    EXPECT_TRUE(ERROR_NONE == reader->seekByTimestamp(seekTime));
    EXPECT_EQ(reader->_seekTimestamp, seekTime);
    EXPECT_EQ(reader->_nextTimestamp, seekTime);
    DELETE_AND_SET_NULL(reader);
}

TEST_F(SwiftSinglePartitionReaderTest, testIsValidateResponse) {
    uint32_t partitionId = 1;
    _config.retryGetMsgInterval = 200000;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
    EXPECT_EQ(200000, reader._transportAdapter->_retryTimeInterval);
    MessageResponse response;
    ErrorCode ec = ERROR_NONE;
    EXPECT_TRUE(!reader.isValidateResponse(&response, ec));
    ec = ERROR_BROKER_NO_DATA;
    EXPECT_TRUE(reader.isValidateResponse(&response, ec));

    ec = ERROR_BROKER_SOME_MESSAGE_LOST;
    EXPECT_TRUE(!reader.isValidateResponse(&response, ec));
    response.set_nextmsgid(1);
    EXPECT_TRUE(reader.isValidateResponse(&response, ec));

    ec = ERROR_NONE;
    EXPECT_TRUE(!reader.isValidateResponse(&response, ec));
    response.set_nexttimestamp(1);
    EXPECT_TRUE(reader.isValidateResponse(&response, ec));
}

TEST_F(SwiftSinglePartitionReaderTest, testHandleGetMessageResponseWithCompressedResponse) {
    uint32_t partitionId = 1;
    int64_t msgId = 0;
    uint32_t messageCount = 1200;
    int64_t oneRequestReadCount = 128;
    ClientMetricsCollector collector;
    // correctly compressed response
    {
        SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
        FakeSwiftTransportClient *fakeClient =
            FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 1, reader);
        FakeClientHelper::makeData(fakeClient, messageCount, msgId);
        EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
        EXPECT_EQ(ERROR_NONE, reader.handleGetMessageResponse(collector));
        EXPECT_EQ(oneRequestReadCount, reader._nextMsgId);
        EXPECT_EQ(oneRequestReadCount, reader._buffer.getUnReadMsgCount());
    }
    // wrongly compressed response
    {
        SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
        FakeSwiftTransportClient *fakeClient = FakeClientHelper::prepareTransportClientForReader(
            _config.topicName, partitionId, true, 2, reader); // compress flag =2
        FakeClientHelper::makeData(fakeClient, messageCount, msgId);
        EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
        EXPECT_EQ(ERROR_CLIENT_INVALID_RESPONSE, reader.handleGetMessageResponse(collector));
        EXPECT_EQ((int64_t)0, reader._nextMsgId);
        EXPECT_EQ((int64_t)0, reader._buffer.getUnReadMsgCount());
    }
    { // auto decompress message
        _config.canDecompress = true;
        SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
        FakeSwiftTransportClient *fakeClient =
            FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 4, reader);
        FakeClientHelper::makeData(fakeClient, messageCount, msgId);
        EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
        EXPECT_EQ(ERROR_NONE, reader.handleGetMessageResponse(collector));
        EXPECT_EQ(oneRequestReadCount, reader._nextMsgId);
        EXPECT_EQ(oneRequestReadCount, reader._buffer.getUnReadMsgCount());
        for (int i = 0; i < oneRequestReadCount; i++) {
            Message msg;
            EXPECT_TRUE(reader._buffer.read(msg));
            EXPECT_TRUE(!msg.compress());
        }
    }
    { // auto decompress wrong message
        _config.canDecompress = true;
        SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
        FakeSwiftTransportClient *fakeClient =
            FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 8, reader);
        FakeClientHelper::makeData(fakeClient, messageCount, msgId);
        EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
        EXPECT_EQ(ERROR_NONE, reader.handleGetMessageResponse(collector));
        EXPECT_EQ(oneRequestReadCount, reader._nextMsgId);
        EXPECT_EQ(oneRequestReadCount, reader._buffer.getUnReadMsgCount());
        for (int i = 0; i < oneRequestReadCount; i++) {
            Message msg;
            EXPECT_TRUE(reader._buffer.read(msg));
            EXPECT_EQ("wrong message data", msg.data());
            EXPECT_TRUE(msg.compress());
        }
    }
    { // auto decompress message
        _config.canDecompress = false;
        SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config);
        FakeSwiftTransportClient *fakeClient =
            FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 4, reader);
        FakeClientHelper::makeData(fakeClient, messageCount, msgId);
        EXPECT_EQ(ERROR_NONE, reader.postGetMessageRequest(collector));
        EXPECT_EQ(ERROR_NONE, reader.handleGetMessageResponse(collector));
        EXPECT_EQ(oneRequestReadCount, reader._nextMsgId);
        EXPECT_EQ(oneRequestReadCount, reader._buffer.getUnReadMsgCount());
        for (int i = 0; i < oneRequestReadCount; i++) {
            Message msg;
            EXPECT_TRUE(reader._buffer.read(msg));
            EXPECT_FALSE(msg.compress()); // alway decompress message
        }
    }
}

TEST_F(SwiftSinglePartitionReaderTest, testHasFatalError) {
    SwiftReaderConfig config;
    config.fatalErrorTimeLimit = 2 * 1000 * 1000; // 2s
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), 0, config);
    ErrorCode ec = reader.hasFatalError(ERROR_NONE);
    EXPECT_EQ(ERROR_NONE, ec);
    ec = reader.hasFatalError(ERROR_BROKER_SOME_MESSAGE_LOST);
    EXPECT_EQ(ERROR_NONE, ec);
    ec = reader.hasFatalError(ERROR_BROKER_NO_DATA);
    EXPECT_EQ(ERROR_NONE, ec);
    ec = reader.hasFatalError(ERROR_CLIENT_NO_MORE_MESSAGE);
    EXPECT_EQ(ERROR_NONE, ec);
    ec = reader.hasFatalError(ERROR_SEALED_TOPIC_READ_FINISH);
    EXPECT_EQ(ERROR_NONE, ec);

    ec = reader.hasFatalError(ERROR_CLIENT_INVALID_RESPONSE);
    EXPECT_EQ(ERROR_NONE, ec);

    auto curTime = TimeUtility::currentTime();
    reader._lastSuccessResponseTime = curTime - config.fatalErrorTimeLimit;
    ec = reader.hasFatalError(ERROR_NONE);
    EXPECT_EQ(ERROR_NONE, ec);
    ec = reader.hasFatalError(ERROR_BROKER_SOME_MESSAGE_LOST);
    EXPECT_EQ(ERROR_NONE, ec);
    ec = reader.hasFatalError(ERROR_BROKER_NO_DATA);
    EXPECT_EQ(ERROR_NONE, ec);
    ec = reader.hasFatalError(ERROR_CLIENT_NO_MORE_MESSAGE);
    EXPECT_EQ(ERROR_NONE, ec);
    ec = reader.hasFatalError(ERROR_SEALED_TOPIC_READ_FINISH);
    EXPECT_EQ(ERROR_SEALED_TOPIC_READ_FINISH, ec);

    ec = reader.hasFatalError(ERROR_CLIENT_INVALID_RESPONSE);
    EXPECT_EQ(ERROR_CLIENT_INVALID_RESPONSE, ec);
}

TEST_F(SwiftSinglePartitionReaderTest, testLogicTopicNotInBrokerError) {
    uint32_t partitionId = 1;
    _config.partitionStatusRefreshInterval = 0;
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), partitionId, _config, 100);
    _changed = false;
    _version = 0;
    reader.setCallBackFunc(std::bind(&SwiftSinglePartitionReaderTest::setTopicChanged, this, std::placeholders::_1));
    FakeSwiftTransportAdapter<TRT_GETMESSAGE> *fakeAdapter =
        new FakeSwiftTransportAdapter<TRT_GETMESSAGE>(_config.topicName, partitionId);
    FakeSwiftTransportClient *fakeClient = fakeAdapter->getFakeTransportClient();
    delete reader._transportAdapter;
    reader._transportAdapter = fakeAdapter;

    fakeAdapter->setErrorCode(ERROR_CLIENT_LOGIC_TOPIC_NOT_IN_BROKER);
    fakeClient->setAutoAsyncCall(true);
    EXPECT_FALSE(_changed);
    EXPECT_EQ(0, _version);
    ClientMetricsCollector collector;
    EXPECT_EQ(ERROR_CLIENT_LOGIC_TOPIC_NOT_IN_BROKER, reader.postGetMessageRequest(collector));
    EXPECT_TRUE(_changed);
    EXPECT_EQ(100, _version);

    FakeSwiftTransportAdapter<TRT_GETMAXMESSAGEID> *fakeMidAdapter =
        new FakeSwiftTransportAdapter<TRT_GETMAXMESSAGEID>(_config.topicName, partitionId);
    fakeClient = fakeMidAdapter->getFakeTransportClient();
    reader.resetMaxMsgIdTransportAdapter(fakeMidAdapter);
    fakeMidAdapter->setErrorCode(ERROR_CLIENT_LOGIC_TOPIC_NOT_IN_BROKER);
    fakeClient->setAutoAsyncCall(true);

    _changed = false;
    _version = 0;
    reader._partitionStatus.refreshTime = 0;
    reader._lastRefreshMsgIdTime = 0;
    reader.getSwiftPartitionStatus(1);
    EXPECT_TRUE(_changed);
    EXPECT_EQ(100, _version);
}

TEST_F(SwiftSinglePartitionReaderTest, testTryFillBufferMetricsReport) {
    bool isSent = false;
    uint32_t partitionId = 1;
    kmonitor::MetricsReporterPtr kmonReporter(nullptr);
    MockClientMetricsReporter reporter(kmonReporter);
    SwiftSinglePartitionReader reader(SwiftAdminAdapterPtr(),
                                      SwiftRpcChannelManagerPtr(),
                                      partitionId,
                                      _config,
                                      -1,
                                      NULL,
                                      dynamic_cast<ClientMetricsReporter *>(&reporter));
    FakeSwiftTransportClient *fakeClient =
        FakeClientHelper::prepareTransportClientForReader(_config.topicName, partitionId, true, 0, reader);
    fakeClient->setTimeoutCount(0);

    ClientMetricsCollector expectCollector;
    expectCollector.rpcLatency = -1;
    expectCollector.decompressLatency = -1;
    expectCollector.requestMsgCount = -1;
    expectCollector.hasError = false;
    expect_call_reader(reporter, expectCollector);
    reader.tryFillBuffer(1, true, isSent);

    int64_t msgId = 0;
    uint32_t messageCount = 10;
    FakeClientHelper::makeData(fakeClient, messageCount, msgId);
    expectCollector.rpcLatency = reader._transportAdapter->_closure->getRpcLatency();
    expectCollector.decompressLatency = reader._transportAdapter->_closure->getDecompressLatency();
    expectCollector.requestMsgCount = 0;
    expectCollector.hasError = false;
    expect_call_reader(reporter, expectCollector);
    reader.tryFillBuffer(2, true, isSent);

    int64_t lastTimestamp = fakeClient->_messageVec->at(9).timestamp();
    int64_t responseTimestamp = reader._transportAdapter->_closure->_response->maxtimestamp();
    expectCollector.rpcLatency = reader._transportAdapter->_closure->getRpcLatency();
    expectCollector.decompressLatency = reader._transportAdapter->_closure->getDecompressLatency();
    expectCollector.requestMsgCount = 10;
    expectCollector.hasError = false;
    EXPECT_CALL(reporter,
                reportDelay(AllOf(Field(&ReaderDelayCollector::readDelay, responseTimestamp - lastTimestamp),
                                  Field(&ReaderDelayCollector::currentDelay, Ge(responseTimestamp - lastTimestamp))),
                            _))
        .Times(1);
    expect_call_reader(reporter, expectCollector);
    reader.tryFillBuffer(3, true, isSent);

    FakeSwiftTransportAdapter<TRT_GETMESSAGE> *adapter =
        (FakeSwiftTransportAdapter<TRT_GETMESSAGE> *)(reader._transportAdapter);
    adapter->_ec = ERROR_CLIENT_SYS_STOPPED;
    adapter->resetFakeClient();
    expectCollector.rpcLatency = -1;
    expectCollector.decompressLatency = -1;
    expectCollector.requestMsgCount = -1;
    expectCollector.hasError = true;
    expect_call_reader(reporter, expectCollector);
    reader.tryFillBuffer(4, true, isSent);
}
} // namespace client
} // namespace swift
