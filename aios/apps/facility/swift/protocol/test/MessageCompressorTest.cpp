#include "swift/protocol/MessageCompressor.h"

#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string>

#include "autil/Singleton.h"
#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "flatbuffers/flatbuffers.h"
#include "flatbuffers/stl_emulation.h"
#include "swift/common/Common.h"
#include "swift/common/MessageInfo.h"
#include "swift/common/ThreadBasedObjectPool.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "unittest/unittest.h"

using namespace swift::common;
using namespace flatbuffers;
using namespace std;
using namespace autil;
namespace swift {
namespace protocol {

using namespace std;

class MessageCompressorTest : public TESTBASE {
public:
    template <typename T>
    T *createFBMessage(uint32_t msgCount, bool hasError = false);
    template <typename T>
    void checkFBMessage(T *request, uint32_t msgCount, bool hasError = false);
};

template <typename T>
T *MessageCompressorTest::createFBMessage(uint32_t msgCount, bool hasError) {
    auto objectPool = Singleton<ThreadBasedObjectPool<FBMessageWriter>>::getInstance();
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

template <typename T>
void MessageCompressorTest::checkFBMessage(T *request, uint32_t msgCount, bool hasError) {
    const std::string &fbdata = request->fbmsgs();
    FBMessageReader reader;
    ASSERT_TRUE(reader.init(fbdata.c_str(), fbdata.length()));
    ASSERT_EQ(msgCount, reader.size());
    for (uint32_t i = 0; i < msgCount; i++) {
        const protocol::flat::Message *fbMsg = reader.read(i);
        string data1(i + 2, 'c');
        string data2(fbMsg->data()->c_str(), fbMsg->data()->size());
        if (data1.size() == data2.size()) {
            EXPECT_EQ(data1, data2);
            if (hasError) {
                EXPECT_TRUE(fbMsg->compress());
            } else {
                EXPECT_TRUE(!fbMsg->compress());
            }
        } else {
            EXPECT_TRUE(fbMsg->compress());
        }
        ASSERT_EQ(i, fbMsg->msgId());
    }
}

TEST_F(MessageCompressorTest, testSimpleProcess) {
    ProductionRequest request;
    int msgCount = 100;
    for (int i = 0; i < msgCount; ++i) {
        string data(i + 100, 'a');
        Message *message = request.add_msgs();
        message->set_data(data.c_str());
        EXPECT_TRUE(!message->compress());
    }
    MessageCompressor::compressProductionMessage(&request, 0);
    EXPECT_EQ(msgCount, request.msgs_size());
    EXPECT_TRUE(!request.has_compressedmsgs());
    for (int i = 0; i < request.msgs_size(); i++) {
        const Message &message = request.msgs(i);
        EXPECT_TRUE(message.compress());
    }
    ProductionRequest requestCpy = request;
    MessageCompressor::decompressProductionMessage(&requestCpy);
    EXPECT_EQ(msgCount, requestCpy.msgs_size());
    for (int i = 0; i < requestCpy.msgs_size(); i++) {
        const Message &message = requestCpy.msgs(i);
        EXPECT_TRUE(!message.compress());
    }

    float ratio;
    MessageCompressor::compressProductionRequest(&request, ratio);
    EXPECT_TRUE(ratio > 0);
    EXPECT_EQ(0, request.msgs_size());
    EXPECT_TRUE(request.has_compressedmsgs());
    EXPECT_TRUE(request.compressedmsgs().size() > 0);

    float ratio1;
    MessageCompressor::decompressProductionRequest(&request, ratio1);
    EXPECT_EQ(msgCount, request.msgs_size());
    MessageResponse response;
    for (int i = 0; i < request.msgs_size(); ++i) {
        const Message &message = request.msgs(i);
        *response.add_msgs() = message;
    }
    Message *normalMsg = response.add_msgs();
    normalMsg->set_data("111111");
    EXPECT_EQ(msgCount + 1, response.msgs_size());
    EXPECT_TRUE(!normalMsg->compress());

    MessageCompressor::compressMessageResponse(&response, ratio);
    EXPECT_TRUE(ratio > 0);
    EXPECT_EQ(0, response.msgs_size());
    EXPECT_TRUE(response.has_compressedmsgs());
    EXPECT_TRUE(response.compressedmsgs().size() > 0);

    MessageCompressor::decompressMessageResponse(&response, ratio1);
    EXPECT_NEAR(ratio, ratio1, 0.00001);
    EXPECT_EQ(msgCount + 1, response.msgs_size());

    MessageCompressor::decompressResponseMessage(&response);
    EXPECT_EQ(msgCount + 1, response.msgs_size());
    for (int i = 0; i < msgCount; ++i) {
        string data(i + 100, 'a');
        const Message &message = response.msgs(i);
        EXPECT_TRUE(!message.compress());
        EXPECT_EQ(data, message.data());
    }
    const Message &message = response.msgs(msgCount);
    EXPECT_TRUE(!message.compress());
    EXPECT_EQ(string("111111"), message.data());
}

TEST_F(MessageCompressorTest, testProductionRequestRequestLevel_PB) {
    ProductionRequest request;
    string data = "aaa";
    int msgCount = 100;
    for (int i = 0; i < msgCount; ++i) {
        Message *message = request.add_msgs();
        message->set_data(data.c_str());
    }
    float ratio;
    MessageCompressor::compressProductionRequest(&request, ratio);
    EXPECT_TRUE(ratio > 0);
    EXPECT_EQ(0, request.msgs_size());
    EXPECT_TRUE(request.has_compressedmsgs());
    EXPECT_TRUE(request.compressedmsgs().size() > 0);
    float ratio1;
    ErrorCode ec = MessageCompressor::decompressProductionRequest(&request, ratio1);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_NEAR(ratio, ratio1, 0.00001);
    EXPECT_EQ(msgCount, request.msgs_size());
    for (int i = 0; i < msgCount; ++i) {
        const Message &message = request.msgs(i);
        EXPECT_EQ(data, message.data());
    }
}

TEST_F(MessageCompressorTest, testProductionRequestRequestLevel_FB) {
    uint32_t msgCount = 1000;
    std::unique_ptr<ProductionRequest> request(createFBMessage<ProductionRequest>(msgCount));
    float ratio;
    MessageCompressor::compressProductionRequest(request.get(), ratio);
    EXPECT_TRUE(ratio > 0);
    EXPECT_EQ(0, request->msgs_size());
    EXPECT_TRUE(request->has_compressedmsgs());
    EXPECT_TRUE(request->compressedmsgs().size() > 0);
    EXPECT_EQ(0, request->fbmsgs().size());
    float ratio1;
    ErrorCode ec = MessageCompressor::decompressProductionRequest(request.get(), ratio1);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_NEAR(ratio, ratio1, 0.00001);
    EXPECT_EQ(0, request->msgs_size());
    checkFBMessage<ProductionRequest>(request.get(), msgCount);
}

TEST_F(MessageCompressorTest, testMessageResponseRequestLevel_PB) {
    MessageResponse response;
    string data = "aaa";
    int msgCount = 100;
    for (int i = 0; i < msgCount; ++i) {
        Message *message = response.add_msgs();
        message->set_data(data.c_str());
    }
    float ratio;
    MessageCompressor::compressMessageResponse(&response, ratio);
    EXPECT_TRUE(ratio > 0);
    EXPECT_EQ(0, response.msgs_size());
    EXPECT_TRUE(response.has_compressedmsgs());
    EXPECT_TRUE(response.compressedmsgs().size() > 0);
    float ratio1;
    ErrorCode ec = MessageCompressor::decompressMessageResponse(&response, ratio1);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_NEAR(ratio, ratio1, 0.00001);
    EXPECT_EQ(msgCount, response.msgs_size());

    for (int i = 0; i < msgCount; ++i) {
        const Message &message = response.msgs(i);
        EXPECT_EQ(data, message.data());
    }
}

TEST_F(MessageCompressorTest, testMessageResponseRequestLevel_FB) {
    uint32_t msgCount = 1000;
    std::unique_ptr<MessageResponse> response(createFBMessage<MessageResponse>(msgCount));
    float ratio;
    MessageCompressor::compressMessageResponse(response.get(), ratio);
    EXPECT_TRUE(ratio > 0);
    EXPECT_EQ(0, response->msgs_size());
    EXPECT_TRUE(response->has_compressedmsgs());
    EXPECT_TRUE(response->compressedmsgs().size() > 0);
    float ratio1;
    ErrorCode ec = MessageCompressor::decompressMessageResponse(response.get(), ratio1);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_NEAR(ratio, ratio1, 0.00001);
    EXPECT_EQ(0, response->msgs_size());
    checkFBMessage<MessageResponse>(response.get(), msgCount);
}

TEST_F(MessageCompressorTest, testProductionRequestMessageLevel_PB) {
    ProductionRequest request;
    int msgCount = 1000;
    for (int i = 0; i < msgCount; ++i) {
        string data(i + 1, 'a');
        Message *message = request.add_msgs();
        message->set_data(data.c_str());
        if (i % 2 == 0) {
            message->set_merged(true);
            message->set_compress(false);
        }
    }
    float ratio;
    MessageCompressor::compressProductionMessage(&request, 0, ratio);
    EXPECT_TRUE(ratio > 0);
    EXPECT_EQ(msgCount, request.msgs_size());
    EXPECT_TRUE(!request.has_compressedmsgs());
    for (int i = 0; i < msgCount; ++i) {
        string data(i + 1, 'a');
        const Message &message = request.msgs(i);
        if (i % 2 == 0) {
            EXPECT_TRUE(message.merged());
            EXPECT_TRUE(!message.compress());
            EXPECT_EQ(data, message.data());
        } else {
            if (message.data().size() == data.size()) {
                EXPECT_EQ(data, message.data());
                EXPECT_TRUE(!message.compress());
            } else {
                EXPECT_TRUE(message.compress());
            }
        }
    }
    float ratio1;
    ErrorCode ec = MessageCompressor::decompressProductionMessage(&request, ratio1);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_NEAR(ratio, ratio1, 0.00001);
    EXPECT_EQ(msgCount, request.msgs_size());

    for (int i = 0; i < msgCount; ++i) {
        string data(i + 1, 'a');
        const Message &message = request.msgs(i);
        EXPECT_EQ(data, message.data());
        EXPECT_TRUE(!message.compress());
    }
}
TEST_F(MessageCompressorTest, testProductionRequestMessageLevel_FB) {
    uint32_t msgCount = 1000;
    std::unique_ptr<ProductionRequest> request(createFBMessage<ProductionRequest>(msgCount));
    float ratio;
    MessageCompressor::compressProductionMessage(request.get(), 0, ratio);
    EXPECT_TRUE(ratio > 0);
    EXPECT_EQ(0, request->msgs_size());
    EXPECT_TRUE(!request->has_compressedmsgs());
    EXPECT_TRUE(request->has_fbmsgs());
    checkFBMessage<ProductionRequest>(request.get(), msgCount);
    float ratio1;
    ErrorCode ec = MessageCompressor::decompressProductionMessage(request.get(), ratio1);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_NEAR(ratio, ratio1, 0.00001);
    EXPECT_EQ(0, request->msgs_size());
    checkFBMessage<ProductionRequest>(request.get(), msgCount);
}

TEST_F(MessageCompressorTest, testMessageResponseMessageLevel_PB) {
    MessageResponse response;
    int msgCount = 1000;
    for (int i = 0; i < msgCount; ++i) {
        string data(i + 1, 'b');
        Message *message = response.add_msgs();
        message->set_data(data.c_str());
    }
    float ratio;
    MessageCompressor::compressResponseMessage(&response, ratio);
    EXPECT_TRUE(ratio > 0);
    EXPECT_EQ(msgCount, response.msgs_size());
    EXPECT_TRUE(!response.has_compressedmsgs());
    for (int i = 0; i < msgCount; ++i) {
        string data(i + 1, 'b');
        const Message &message = response.msgs(i);
        if (message.data().size() == data.size()) {
            EXPECT_EQ(data, message.data());
            EXPECT_TRUE(!message.compress());
        } else {
            EXPECT_TRUE(message.compress());
        }
    }

    float ratio1;
    ErrorCode ec = MessageCompressor::decompressResponseMessage(&response, ratio1);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_NEAR(ratio, ratio1, 0.00001);
    EXPECT_EQ(msgCount, response.msgs_size());
    for (int i = 0; i < msgCount; ++i) {
        string data(i + 1, 'b');
        const Message &message = response.msgs(i);
        EXPECT_EQ(data, message.data());
        EXPECT_TRUE(!message.compress());
    }
}
TEST_F(MessageCompressorTest, testMessageResponseMessageLevel_FB) {
    int msgCount = 1000;
    std::unique_ptr<MessageResponse> response(createFBMessage<MessageResponse>(msgCount));
    float ratio;
    MessageCompressor::compressResponseMessage(response.get(), ratio);
    EXPECT_TRUE(ratio > 0);
    EXPECT_EQ(0, response->msgs_size());
    EXPECT_TRUE(!response->has_compressedmsgs());
    EXPECT_TRUE(response->fbmsgs().size() > 0);
    checkFBMessage<MessageResponse>(response.get(), msgCount);
    float ratio1;
    ErrorCode ec = MessageCompressor::decompressResponseMessage(response.get(), ratio1);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_NEAR(ratio, ratio1, 0.00001);
    EXPECT_EQ(0, response->msgs_size());
    checkFBMessage<MessageResponse>(response.get(), msgCount);
}

TEST_F(MessageCompressorTest, testProductionRequestMessageLevelWithError) {
    ProductionRequest request;
    int msgCount = 100;
    for (int i = 0; i < msgCount; ++i) {
        string data(i, 'a');
        Message *message = request.add_msgs();
        message->set_data(data.c_str());
    }
    float ratio;
    MessageCompressor::compressProductionMessage(&request, 0, ratio);
    EXPECT_TRUE(ratio > 0);
    EXPECT_EQ(msgCount, request.msgs_size());
    EXPECT_TRUE(!request.has_compressedmsgs());
    for (int i = 0; i < msgCount; ++i) {
        string data(i, 'a');
        const Message &message = request.msgs(i);
        if (message.data().size() == data.size()) {
            EXPECT_EQ(data, message.data());
            EXPECT_TRUE(!message.compress());
        } else {
            EXPECT_TRUE(message.compress());
        }
    }
    float ratio1;
    Message *message = request.add_msgs();
    message->set_data("error compress");
    message->set_compress(true);

    ErrorCode ec = MessageCompressor::decompressProductionMessage(&request, ratio1);
    EXPECT_EQ(ERROR_NONE, ec);
    ASSERT_TRUE(message->compress());

    EXPECT_TRUE(1.0 > ratio1);
    EXPECT_EQ(msgCount + 1, request.msgs_size());
    for (int i = 0; i < request.msgs_size() - 1; ++i) {
        string data(i, 'a');
        const Message &message = request.msgs(i);
        EXPECT_EQ(data, message.data());
        EXPECT_TRUE(!message.compress());
    }
    const Message &msg = request.msgs(msgCount);
    EXPECT_EQ(string("error compress"), msg.data());
    EXPECT_TRUE(msg.compress());
}

TEST_F(MessageCompressorTest, testProductionRequestMessageLevelWithError_FB) {
    int32_t msgCount = 100;
    std::unique_ptr<ProductionRequest> request(createFBMessage<ProductionRequest>(msgCount, true));
    float ratio1;
    ErrorCode ec = MessageCompressor::decompressProductionMessage(request.get(), ratio1);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_NEAR(1.0, ratio1, 0.00001);
    EXPECT_EQ(0, request->msgs_size());
    checkFBMessage<ProductionRequest>(request.get(), msgCount, true);
}

TEST_F(MessageCompressorTest, testMergedMessage) {
    string data(10000, 'a');
    ZlibCompressor compressor;
    string compressedData;
    ASSERT_FALSE(MessageCompressor::compressMergedMessage(&compressor, 0, data.c_str(), 1, compressedData));
    ASSERT_EQ(0, compressedData.size());
    ASSERT_FALSE(
        MessageCompressor::compressMergedMessage(&compressor, 10000, data.c_str(), data.size(), compressedData));
    ASSERT_EQ(0, compressedData.size());
    ASSERT_TRUE(MessageCompressor::compressMergedMessage(&compressor, 0, data.c_str(), data.size(), compressedData));
    ASSERT_TRUE(data.size() > compressedData.size());

    string uncompressData;
    ASSERT_FALSE(MessageCompressor::decompressMergedMessage(&compressor, uncompressData.c_str(), 0, uncompressData));
    ASSERT_TRUE(MessageCompressor::decompressMergedMessage(
        &compressor, compressedData.c_str(), compressedData.size(), uncompressData));
    ASSERT_EQ(data, uncompressData);
}

TEST_F(MessageCompressorTest, testDecompressMessageInfo) {
    string data(10000, 'a');
    ZlibCompressor compressor;
    { // only compressed
        string compressedData;
        ASSERT_TRUE(MessageCompressor::compressData(&compressor, data.c_str(), data.size(), compressedData));
        ASSERT_TRUE(data.size() > compressedData.size());

        MessageInfo msgInfo;
        msgInfo.compress = true;
        msgInfo.isMerged = false;
        msgInfo.data = compressedData;
        float ratio = 0.0;
        ErrorCode ec = MessageCompressor::decompressMessageInfo(msgInfo, ratio);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(data.size(), msgInfo.data.size());
        ASSERT_EQ(data, msgInfo.data);
        ASSERT_FALSE(msgInfo.compress);
        ASSERT_FALSE(msgInfo.isMerged);
    }
    { // compressed and merged
        string compressedData;
        ASSERT_TRUE(
            MessageCompressor::compressMergedMessage(&compressor, 0, data.c_str(), data.size(), compressedData));
        ASSERT_TRUE(data.size() > compressedData.size());

        MessageInfo msgInfo;
        msgInfo.compress = true;
        msgInfo.isMerged = true;
        msgInfo.data = compressedData;
        float ratio = 0.0;
        ErrorCode ec = MessageCompressor::decompressMessageInfo(msgInfo, ratio);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(data.size(), msgInfo.data.size());
        ASSERT_EQ(data, msgInfo.data);
        ASSERT_FALSE(msgInfo.compress);
        ASSERT_TRUE(msgInfo.isMerged);
    }
}

} // namespace protocol
} // namespace swift
