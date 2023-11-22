#include "swift/client/MessageReadBuffer.h"

#include <algorithm>
#include <cstddef>
#include <list>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "swift/common/Common.h"
#include "swift/common/FieldGroupWriter.h"
#include "swift/common/MessageInfo.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/Atomic.h"
#include "swift/util/MessageInfoUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace flatbuffers;
using namespace swift::protocol;
using namespace swift::util;
using namespace swift::common;

namespace swift {
namespace client {

class MessageReadBufferTest : public TESTBASE {
public:
    protocol::Message prepareMergeMsg(int64_t threshold = -1, bool fieldFilter = false);
};

TEST_F(MessageReadBufferTest, testReadPbMessage) {
    MessageReadBuffer buffer;
    MessageResponse *response = new MessageResponse;
    Message msg;
    for (int64_t i = 0; i < 3; ++i) {
        Message *msg = response->add_msgs();
        msg->set_msgid(i);
    }

    EXPECT_TRUE(!buffer.read(msg));

    buffer.addResponse(response);
    for (int j = 0; j < 3; ++j) {
        EXPECT_TRUE(buffer.read(msg));
        EXPECT_EQ(int64_t(j), msg.msgid());
        EXPECT_EQ(int64_t(2 - j), buffer.getUnReadMsgCount());
    }
    EXPECT_TRUE(!buffer.read(msg));

    response = new MessageResponse;
    for (int64_t i = 0; i < 3; ++i) {
        Message *msg = response->add_msgs();
        msg->set_msgid(i);
    }
    buffer.addResponse(response);
    buffer.addResponse(new MessageResponse());
    response = new MessageResponse;
    for (int64_t i = 0; i < 5; ++i) {
        Message *msg = response->add_msgs();
        msg->set_msgid(3 + i);
    }
    buffer.addResponse(response);
    for (int j = 0; j < 8; ++j) {
        EXPECT_TRUE(buffer.read(msg));
        EXPECT_EQ(int64_t(j), msg.msgid());
        EXPECT_EQ(int64_t(7 - j), buffer.getUnReadMsgCount());
    }
    EXPECT_TRUE(!buffer.read(msg));
}

TEST_F(MessageReadBufferTest, testReadFBMessage) {
    uint32_t msgCount = 10;
    FBMessageWriter writer;
    for (uint32_t i = 0; i < msgCount; ++i) {
        writer.addMessage(string(i, 'c'), i, i + 1, i + 2, i + 3, false);
    }
    MessageResponse *response = new MessageResponse();
    response->set_fbmsgs(writer.data(), writer.size());
    response->set_messageformat(MF_FB);
    MessageReadBuffer buffer;
    Message msg;
    EXPECT_TRUE(!buffer.read(msg));
    buffer.addResponse(response);
    for (uint32_t j = 0; j < msgCount; ++j) {
        EXPECT_TRUE(buffer.read(msg));
        EXPECT_EQ(int64_t(j), msg.msgid());
        EXPECT_EQ(int64_t(j + 1), msg.timestamp());
        EXPECT_EQ(int64_t(j + 2), msg.uint16payload());
        EXPECT_EQ(int64_t(j + 3), msg.uint8maskpayload());
        EXPECT_EQ(false, msg.compress());
        EXPECT_EQ(string(j, 'c'), msg.data());
        EXPECT_EQ(int64_t(msgCount - j - 1), buffer.getUnReadMsgCount());
    }
    EXPECT_TRUE(!buffer.read(msg));
}

TEST_F(MessageReadBufferTest, testClear) {
    MessageReadBuffer buffer;
    MessageResponse *response = new MessageResponse;
    Message msg;
    for (int64_t i = 0; i < 3; ++i) {
        Message *msg = response->add_msgs();
        msg->set_msgid(i);
    }
    buffer.addResponse(response);
    EXPECT_EQ(size_t(1), buffer._responseList.size());
    EXPECT_EQ(int64_t(3), buffer.getUnReadMsgCount());
    EXPECT_EQ(int32_t(0), buffer._unpackMsgVec.size());
    buffer.clear();
    EXPECT_EQ(size_t(0), buffer._responseList.size());
    EXPECT_EQ(int64_t(0), buffer.getUnReadMsgCount());
    EXPECT_EQ(int32_t(0), buffer._unpackMsgVec.size());
}

TEST_F(MessageReadBufferTest, testSeek) {
    MessageReadBuffer buffer;
    MessageResponse *response = new MessageResponse;
    Message msg;
    for (int64_t i = 0; i < 3; ++i) {
        Message *msg = response->add_msgs();
        msg->set_msgid(i);
    }
    buffer.addResponse(response);
    EXPECT_EQ(size_t(1), buffer._responseList.size());
    EXPECT_EQ(int64_t(3), buffer.getUnReadMsgCount());
    int64_t msgid = 8;
    EXPECT_EQ(msgid, buffer.seek(msgid));
    EXPECT_EQ(size_t(0), buffer._responseList.size());
    EXPECT_EQ(int64_t(0), buffer.getUnReadMsgCount());
}

TEST_F(MessageReadBufferTest, testSeekMergeMsg) {
    MessageReadBuffer buffer;
    MessageResponse *response = new MessageResponse;
    Message mergeMsg = prepareMergeMsg();
    const int32_t totalMessageCount = 5;
    for (int64_t i = 0; i < 3; ++i) {
        Message *msg = response->add_msgs();
        msg->set_msgid(i);
        msg->set_data("AA");
    }
    Message *msg = response->add_msgs();
    msg->CopyFrom(mergeMsg);
    response->set_hasmergedmsg(true);
    response->set_totalmsgcount(totalMessageCount);
    buffer.addResponse(response);
    EXPECT_EQ(size_t(1), buffer._responseList.size());
    EXPECT_EQ(int64_t(totalMessageCount), buffer.getUnReadMsgCount());
    Message tmpMsg;
    for (int idx = 0; idx < totalMessageCount; ++idx) {
        buffer.read(tmpMsg);
    }
    int64_t msgid = 8;
    EXPECT_EQ(msgid, buffer.seek(msgid));
    for (int idx = 0; idx < totalMessageCount; ++idx) {
        buffer.read(tmpMsg);
    }
    EXPECT_EQ(size_t(0), buffer._responseList.size());
    EXPECT_EQ(int64_t(0), buffer.getUnReadMsgCount());
}

TEST_F(MessageReadBufferTest, testGetFirstMsgTimestamp) {
    {
        MessageReadBuffer buffer;

        // empty buffer
        int64_t timestamp = buffer.getFirstMsgTimestamp();
        EXPECT_EQ((int64_t)-1, timestamp);
        MessageResponse *response = new MessageResponse;
        buffer.addResponse(response);
        timestamp = buffer.getFirstMsgTimestamp();
        EXPECT_EQ((int64_t)-1, timestamp);

        // not empty
        response = new MessageResponse;
        Message *msg = response->add_msgs();
        msg->set_timestamp(3);
        buffer.addResponse(response);
        response = new MessageResponse;
        msg = response->add_msgs();
        msg->set_timestamp(1);
        buffer.addResponse(response);
        timestamp = buffer.getFirstMsgTimestamp();
        EXPECT_EQ((int64_t)3, timestamp);
    }
    {
        MessageResponse *response = new MessageResponse();
        Message *msg = response->add_msgs();
        *msg = prepareMergeMsg();
        msg->set_timestamp(10);
        response->set_totalmsgcount(2);
        MessageReadBuffer buffer;

        Message tmpMsg;
        EXPECT_TRUE(!buffer.read(tmpMsg));
        buffer.addResponse(response);
        EXPECT_EQ(2, buffer.getUnReadMsgCount());
        Filter filter;
        filter.set_from(5);
        filter.set_to(5);
        buffer.updateFilter(filter, vector<string>(), "");
        int64_t timestamp = buffer.getFirstMsgTimestamp();
        EXPECT_EQ((int64_t)-1, timestamp);
    }
    {
        MessageResponse *response = new MessageResponse();
        Message *msg = response->add_msgs();
        *msg = prepareMergeMsg();
        msg->set_timestamp(10);
        response->set_totalmsgcount(2);
        MessageReadBuffer buffer;

        Message tmpMsg;
        EXPECT_TRUE(!buffer.read(tmpMsg));
        buffer.addResponse(response);
        EXPECT_EQ(2, buffer.getUnReadMsgCount());
        int64_t timestamp = buffer.getFirstMsgTimestamp();
        EXPECT_EQ((int64_t)10, timestamp);
    }
}

protocol::Message MessageReadBufferTest::prepareMergeMsg(int64_t threshold, bool fieldFilter) {
    vector<MessageInfo *> msgVec;
    string msg1Data = "1111111111";
    string msg2Data = "2222222222";
    {
        if (fieldFilter) {
            FieldGroupWriter writer;
            writer.addProductionField("field_a", "1", true);
            msg1Data = writer.toString();
            writer.reset();
            writer.addProductionField("field_b", "2", true);
            msg2Data = writer.toString();
        }
        MessageInfo *msg1 = new MessageInfo(msg1Data, 2, 3, 4);
        MessageInfo *msg2 = new MessageInfo(msg2Data, 3, 4, 5);
        msgVec.push_back(msg1);
        msgVec.push_back(msg2);
    }
    MessageInfo mergeMsg;
    MessageInfoUtil::mergeMessage(123, msgVec, 0, mergeMsg);
    for (size_t i = 0; i < msgVec.size(); i++) {
        delete msgVec[i];
    }
    if (threshold >= 0) {
        ZlibCompressor compressor;
        MessageInfoUtil::compressMessage(&compressor, threshold, mergeMsg);
    }
    protocol::Message msg;
    msg.set_data(mergeMsg.data);
    msg.set_uint16payload(mergeMsg.uint16Payload);
    msg.set_uint8maskpayload(mergeMsg.uint8Payload);
    msg.set_compress(mergeMsg.compress);
    msg.set_merged(mergeMsg.isMerged);
    msg.set_msgid(1234);
    msg.set_timestamp(12345);
    return msg;
}

TEST_F(MessageReadBufferTest, testUnpackMessage) {
    Message msg = prepareMergeMsg();
    MessageReadBuffer readBuffer;
    int32_t totalCount = 0;
    ASSERT_TRUE(readBuffer.unpackMessage(msg, totalCount));
    ASSERT_EQ(2, readBuffer._unpackMsgVec.size());
    ASSERT_EQ(2, totalCount);
}

TEST_F(MessageReadBufferTest, testReadMergedMessage) {
    MessageResponse *response = new MessageResponse();
    *response->add_msgs() = prepareMergeMsg();
    response->set_totalmsgcount(2);
    MessageReadBuffer buffer;
    Message msg;
    EXPECT_TRUE(!buffer.read(msg));
    buffer.addResponse(response);
    for (uint32_t j = 0; j < 2; ++j) {
        EXPECT_TRUE(buffer.read(msg));
        EXPECT_EQ(1234, msg.msgid());
        EXPECT_EQ(12345, msg.timestamp());
        EXPECT_EQ(int64_t(j + 3), msg.uint16payload());
        EXPECT_EQ(int64_t(j + 2), msg.uint8maskpayload());
        EXPECT_EQ(false, msg.compress());
        if (j == 0) {
            EXPECT_EQ("1111111111", msg.data());
        } else {
            EXPECT_EQ("2222222222", msg.data());
        }
        EXPECT_EQ(int64_t(2 - j - 1), buffer.getUnReadMsgCount());
    }
    EXPECT_TRUE(!buffer.read(msg));
}

TEST_F(MessageReadBufferTest, testReadMergedMessageWithFilter) {
    {
        MessageResponse *response = new MessageResponse();
        *response->add_msgs() = prepareMergeMsg();
        response->set_totalmsgcount(2);
        MessageResponse *response2 = new MessageResponse();
        *response2->add_msgs() = prepareMergeMsg();
        response2->set_totalmsgcount(2);

        MessageReadBuffer buffer;
        Message msg;
        EXPECT_TRUE(!buffer.read(msg));
        buffer.addResponse(response);
        buffer.addResponse(response2);
        EXPECT_EQ(4, buffer.getUnReadMsgCount());
        Filter filter;
        filter.set_from(5);
        filter.set_to(5);
        buffer.updateFilter(filter, vector<string>(), "");
        EXPECT_TRUE(!buffer.read(msg));
        EXPECT_EQ(0, buffer.getUnReadMsgCount());
        EXPECT_TRUE(!buffer.read(msg));
    }
    {
        MessageResponse *response = new MessageResponse();
        *response->add_msgs() = prepareMergeMsg();
        response->set_totalmsgcount(2);
        MessageResponse *response2 = new MessageResponse();
        *response2->add_msgs() = prepareMergeMsg();
        response2->set_totalmsgcount(2);

        MessageReadBuffer buffer;
        Message msg;
        EXPECT_TRUE(!buffer.read(msg));
        buffer.addResponse(response);
        buffer.addResponse(response2);
        EXPECT_EQ(4, buffer.getUnReadMsgCount());
        Filter filter;
        filter.set_from(4);
        filter.set_to(5);
        buffer.updateFilter(filter, vector<string>(), "");
        EXPECT_TRUE(buffer.read(msg));
        EXPECT_EQ(4, msg.uint16payload());
        EXPECT_EQ(2, buffer.getUnReadMsgCount());
        EXPECT_TRUE(buffer.read(msg));
        EXPECT_EQ(4, msg.uint16payload());
        EXPECT_EQ(0, buffer.getUnReadMsgCount());
        EXPECT_TRUE(!buffer.read(msg));
    }
}

} // namespace client
} // namespace swift
