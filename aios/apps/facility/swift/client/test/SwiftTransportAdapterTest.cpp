#include "swift/client/SwiftTransportAdapter.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <stdint.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "arpc/ANetRPCController.h"
#include "arpc/proto/rpc_extensions.pb.h"
#include "autil/CommonMacros.h"
#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "swift/client/Notifier.h"
#include "swift/client/SwiftTransportClient.h"
#include "swift/client/TransportClosure.h"
#include "swift/client/fake_client/FakeSwiftTransportClient.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace std;
using namespace swift::protocol;
using namespace swift::network;

namespace swift {
namespace client {

class SwiftTransportAdapterTest : public TESTBASE {
public:
    template <typename T>
    void run(T *closure, int sleepTime) {
        if (sleepTime > 0) {
            usleep(sleepTime);
        }
        closure->Run();
    }
};

TEST_F(SwiftTransportAdapterTest, testSendRequest) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftTransportAdapter<TRT_SENDMESSAGE> sendAdapter(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), topicName, partitionId);
    FakeSwiftTransportClient *client = new FakeSwiftTransportClient();
    sendAdapter._transportClient = client;

    EXPECT_TRUE(sendAdapter.isLastRequestDone());
    EXPECT_TRUE(sendAdapter.isLastRequestHandled());
    EXPECT_TRUE(sendAdapter.waitLastRequestDone(1000));
    ProductionRequest *request = new ProductionRequest;
    Message *message = request->add_msgs();
    message->set_data("test_data");
    EXPECT_EQ(ERROR_NONE, sendAdapter.postRequest(request));

    EXPECT_TRUE(!sendAdapter.isLastRequestDone());
    EXPECT_TRUE(!sendAdapter.isLastRequestHandled());

    client->finishAsyncCall();
    EXPECT_TRUE(sendAdapter.isLastRequestDone());
    EXPECT_TRUE(!sendAdapter.isLastRequestHandled());
    EXPECT_TRUE(sendAdapter.waitLastRequestDone(1000));

    MessageResponse *response = NULL;
    EXPECT_EQ(ERROR_NONE, sendAdapter.stealResponse(response));
    EXPECT_TRUE(sendAdapter.isLastRequestHandled());
    EXPECT_TRUE(response);
    EXPECT_TRUE(response->has_errorinfo());
    const ErrorInfo &errorInfo = response->errorinfo();
    EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
    SWIFT_DELETE(response);

    EXPECT_EQ(size_t(1), client->_messageVec->size());
    EXPECT_EQ(string("test_data"), (*(client->_messageVec))[0].data());
}

TEST_F(SwiftTransportAdapterTest, testSendRequestNotDone) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    Notifier notifier;
    SwiftTransportAdapter<TRT_SENDMESSAGE> *sendAdapter = new SwiftTransportAdapter<TRT_SENDMESSAGE>(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), topicName, partitionId, &notifier);
    FakeSwiftTransportClient *client = new FakeSwiftTransportClient();
    client->_finAsyncCall = false;
    sendAdapter->_transportClient = client;

    ProductionRequest *request = new ProductionRequest;
    Message *message = request->add_msgs();
    message->set_data("test_data");
    EXPECT_EQ(ERROR_NONE, sendAdapter->postRequest(request));
    TransportClosureTyped<TRT_SENDMESSAGE> *closeure = client->_sendClosure;
    notifier.notify();
    ThreadPtr sendThreadPtr = Thread::createThread(std::bind(
        &SwiftTransportAdapterTest::run<TransportClosureTyped<TRT_SENDMESSAGE>>, this, closeure, 2000 * 1000));

    DELETE_AND_SET_NULL(sendAdapter);
}

TEST_F(SwiftTransportAdapterTest, testGetRequest) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftTransportAdapter<TRT_GETMESSAGE> getAdapter(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), topicName, partitionId);
    FakeSwiftTransportClient *client = new FakeSwiftTransportClient();
    Message message;
    message.set_data("test_data");
    client->_messageVec->push_back(message);
    getAdapter._transportClient = client;
    EXPECT_EQ(ERROR_NONE, getAdapter.ignoreLastResponse());

    EXPECT_TRUE(getAdapter.isLastRequestDone());
    EXPECT_TRUE(getAdapter.isLastRequestHandled());
    ConsumptionRequest *request = new ConsumptionRequest;
    request->set_startid(0);
    request->set_count(1);
    EXPECT_EQ(ERROR_NONE, getAdapter.postRequest(request));

    EXPECT_TRUE(!getAdapter.isLastRequestDone());
    EXPECT_TRUE(!getAdapter.isLastRequestHandled());

    client->finishAsyncCall();
    EXPECT_TRUE(getAdapter.isLastRequestDone());
    EXPECT_TRUE(!getAdapter.isLastRequestHandled());

    MessageResponse *response = NULL;
    EXPECT_EQ(ERROR_NONE, getAdapter.stealResponse(response));
    EXPECT_TRUE(getAdapter.isLastRequestHandled());
    EXPECT_TRUE(response);
    EXPECT_TRUE(response->has_errorinfo());

    const ErrorInfo &errorInfo = response->errorinfo();
    EXPECT_EQ(ERROR_NONE, errorInfo.errcode());

    EXPECT_EQ(int(1), response->msgs_size());
    EXPECT_EQ(string("test_data"), response->msgs(0).data());

    DELETE_AND_SET_NULL(response);
}

TEST_F(SwiftTransportAdapterTest, testGetRequestSync) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftTransportAdapter<TRT_GETMESSAGE> getAdapter(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), topicName, partitionId);
    FakeSwiftTransportClient *client = new FakeSwiftTransportClient();
    Message message;
    message.set_data("test_data");
    client->_messageVec->push_back(message);
    getAdapter._transportClient = client;

    ConsumptionRequest request;
    request.set_startid(0);
    request.set_count(1);
    MessageResponse response;
    ASSERT_EQ(ERROR_NONE, getAdapter.postRequest(&request, &response));
    EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());
    EXPECT_EQ(1, response.msgs_size());
    EXPECT_EQ(string("test_data"), response.msgs(0).data());
}

TEST_F(SwiftTransportAdapterTest, testGetMinMsgByTimeRequest) {
    string topicName("topic_name");
    uint32_t partitionId = 1;
    SwiftTransportAdapter<TRT_GETMINMESSAGEIDBYTIME> getAdapter(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), topicName, partitionId);
    FakeSwiftTransportClient *client = new FakeSwiftTransportClient();
    Message message;
    message.set_data("test_data");
    message.set_msgid(102);
    message.set_timestamp(1);
    client->_messageVec->push_back(message);
    getAdapter._transportClient = client;

    EXPECT_TRUE(getAdapter.isLastRequestDone());
    EXPECT_TRUE(getAdapter.isLastRequestHandled());
    MessageIdRequest *request = new MessageIdRequest;
    request->set_timestamp(0);
    EXPECT_EQ(ERROR_NONE, getAdapter.postRequest(request));

    EXPECT_TRUE(!getAdapter.isLastRequestDone());
    EXPECT_TRUE(!getAdapter.isLastRequestHandled());

    client->finishAsyncCall();
    EXPECT_TRUE(getAdapter.isLastRequestDone());
    EXPECT_TRUE(!getAdapter.isLastRequestHandled());

    MessageIdResponse *response = NULL;
    EXPECT_EQ(ERROR_NONE, getAdapter.stealResponse(response));
    EXPECT_TRUE(getAdapter.isLastRequestHandled());
    EXPECT_TRUE(response);
    EXPECT_TRUE(response->has_errorinfo());

    const ErrorInfo &errorInfo = response->errorinfo();
    EXPECT_EQ(ERROR_NONE, errorInfo.errcode());

    EXPECT_EQ(int64_t(102), response->msgid());

    DELETE_AND_SET_NULL(response);
}

TEST_F(SwiftTransportAdapterTest, testGetRetryInterval) {
    SwiftTransportAdapter<TRT_GETMINMESSAGEIDBYTIME> getAdapter(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), "topic", 1);
    int64_t currentTime = autil::TimeUtility::currentTime();
    EXPECT_EQ((int64_t)0, getAdapter.getRetryInterval(currentTime));

    getAdapter.updateRetryTime(ERROR_NONE, currentTime);
    EXPECT_EQ((int64_t)0, getAdapter.getRetryInterval(currentTime));
    getAdapter.updateRetryTime(ERROR_BROKER_SOME_MESSAGE_LOST, currentTime);
    EXPECT_EQ((int64_t)0, getAdapter.getRetryInterval(currentTime));
    getAdapter.updateRetryTime(ERROR_UNKNOWN, currentTime);
    EXPECT_EQ((int64_t)500 * 1000, getAdapter.getRetryInterval(currentTime));
}

TEST_F(SwiftTransportAdapterTest, testHandleLastTransportClosure) {
    SwiftTransportAdapter<TRT_SENDMESSAGE> sendAdapter(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), "topic", 0);
    sendAdapter._closure =
        std::make_shared<SwiftTransportAdapter<TRT_SENDMESSAGE>::ClosureType>(nullptr, sendAdapter._notifier);
    sendAdapter._transportClient = new SwiftTransportClient("11", nullptr, "0");

    // 1. has handled, return directly
    ErrorCode ec = sendAdapter.handleLastTransportClosure();
    EXPECT_EQ(ERROR_NONE, ec);

    // 2. controller fail of ARPC_ERROR_PUSH_WORKITEM,
    // replace error ERROR_BROKER_PUSH_WORK_ITEM, not reset transport client
    sendAdapter._lastTransportClosureHandled = false;
    auto controller = sendAdapter._closure->getController();
    controller->SetFailed("rpc fail");
    controller->SetErrorCode(arpc::ARPC_ERROR_PUSH_WORKITEM);
    ec = sendAdapter.handleLastTransportClosure();
    EXPECT_EQ(ERROR_BROKER_PUSH_WORK_ITEM, ec);
    EXPECT_TRUE(sendAdapter._transportClient != nullptr);

    // 3. controller fail of other code,
    // replace error ERROR_CLIENT_ARPC_ERROR and reset transport client
    sendAdapter._lastTransportClosureHandled = false;
    controller->SetFailed("rpc fail");
    controller->SetErrorCode(arpc::ARPC_ERROR_TIMEOUT);
    ec = sendAdapter.handleLastTransportClosure();
    EXPECT_EQ(ERROR_CLIENT_ARPC_ERROR, ec);
    EXPECT_TRUE(sendAdapter._transportClient == nullptr);

    // 4. controller success, closure error is ERROR_BROKER_STOPPED
    // or ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND, reset transport client
    sendAdapter._lastTransportClosureHandled = false;
    controller->Reset();
    sendAdapter._transportClient = new SwiftTransportClient("11", nullptr, "0");
    ErrorInfo *errorInfo = sendAdapter._closure->_response->mutable_errorinfo();
    errorInfo->set_errcode(ERROR_BROKER_STOPPED);
    ec = sendAdapter.handleLastTransportClosure();
    EXPECT_EQ(ERROR_BROKER_STOPPED, ec);
    EXPECT_TRUE(sendAdapter._transportClient == nullptr);

    sendAdapter._lastTransportClosureHandled = false;
    sendAdapter._transportClient = new SwiftTransportClient("11", nullptr, "0");
    errorInfo->set_errcode(ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND);
    ec = sendAdapter.handleLastTransportClosure();
    EXPECT_EQ(ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND, ec);
    EXPECT_TRUE(sendAdapter._transportClient == nullptr);

    // 5. controller success, closure error is other, not reset transport client
    sendAdapter._lastTransportClosureHandled = false;
    sendAdapter._transportClient = new SwiftTransportClient("11", nullptr, "0");
    errorInfo->set_errcode(ERROR_CLIENT_SOME_MESSAGE_SEND_FAILED);
    ec = sendAdapter.handleLastTransportClosure();
    EXPECT_EQ(ERROR_CLIENT_SOME_MESSAGE_SEND_FAILED, ec);
    EXPECT_TRUE(sendAdapter._transportClient != nullptr);

    sendAdapter._closure->Run();
}

} // namespace client
} // namespace swift
