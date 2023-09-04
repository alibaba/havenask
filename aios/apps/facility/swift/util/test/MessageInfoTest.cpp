#include "swift/common/MessageInfo.h"

#include <algorithm>
#include <cstddef>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/MessageInfoUtil.h"
#include "swift/util/MessageUtil.h"
#include "unittest/unittest.h"

using namespace swift::protocol;
using namespace swift::util;
using namespace swift::common;

using namespace std;
using namespace testing;
using namespace autil;

namespace swift {
namespace util {

class MessageInfoUtilTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void MessageInfoUtilTest::setUp() {}

void MessageInfoUtilTest::tearDown() {}

TEST_F(MessageInfoUtilTest, testSize) { ASSERT_EQ(24 + 2 * sizeof(string), sizeof(MessageInfo)); }

TEST_F(MessageInfoUtilTest, testMergeMessage) {

    vector<MessageInfo *> msgVec;
    {
        MessageInfo *msg1 = new MessageInfo("1", 2, 3, 4);
        MessageInfo *msg2 = new MessageInfo("2", 3, 4, 5);
        msgVec.push_back(msg1);
        msgVec.push_back(msg2);
    }
    MessageInfo mergeMsg;
    MessageInfoUtil::mergeMessage(123, msgVec, 0, mergeMsg);
    for (size_t i = 0; i < msgVec.size(); i++) {
        delete msgVec[i];
    }
    ASSERT_EQ(123, mergeMsg.uint16Payload);
    ASSERT_TRUE(!mergeMsg.compress);
    ASSERT_TRUE(mergeMsg.isMerged);
    ASSERT_EQ(3, mergeMsg.uint8Payload);
    ASSERT_EQ(4, mergeMsg.checkpointId);
    ASSERT_EQ(20, mergeMsg.data.size());

    protocol::Message msg;
    msg.set_data(mergeMsg.data);
    msg.set_uint16payload(mergeMsg.uint16Payload);
    msg.set_uint8maskpayload(mergeMsg.uint8Payload);
    msg.set_compress(mergeMsg.compress);
    msg.set_merged(mergeMsg.isMerged);
    msg.set_msgid(1234);
    msg.set_timestamp(12345);
    vector<protocol::Message> msgVec2;
    int32_t count;
    ASSERT_TRUE(MessageUtil::unpackMessage(
        NULL, msg.data().c_str(), msg.data().size(), msg.msgid(), msg.timestamp(), NULL, NULL, msgVec2, count));
    ASSERT_EQ(2, count);
    ASSERT_EQ(2, msgVec2.size());
    protocol::Message msg1 = msgVec2[0];
    protocol::Message msg2 = msgVec2[1];
    ASSERT_EQ(1234, msg1.msgid());
    ASSERT_EQ(12345, msg1.timestamp());
    ASSERT_EQ(3, msg1.uint16payload());
    ASSERT_EQ(2, msg1.uint8maskpayload());
    ASSERT_TRUE(!msg1.merged());
    ASSERT_TRUE(!msg1.compress());
    ASSERT_EQ("1", msg1.data());

    ASSERT_EQ(1234, msg2.msgid());
    ASSERT_EQ(12345, msg2.timestamp());
    ASSERT_EQ(4, msg2.uint16payload());
    ASSERT_EQ(3, msg2.uint8maskpayload());
    ASSERT_TRUE(!msg2.merged());
    ASSERT_TRUE(!msg2.compress());
    ASSERT_EQ("2", msg2.data());
}

TEST_F(MessageInfoUtilTest, testCompressMessage) {
    ZlibCompressor compressor;
    string data(100, 'a');
    MessageInfo msgInfo;
    msgInfo.data = data;

    MessageInfoUtil::compressMessage(NULL, 0, msgInfo);
    ASSERT_TRUE(!msgInfo.compress);
    ASSERT_EQ(data.size(), msgInfo.data.size());

    MessageInfoUtil::compressMessage(&compressor, data.size(), msgInfo);
    ASSERT_TRUE(!msgInfo.compress);
    ASSERT_EQ(data.size(), msgInfo.data.size());

    MessageInfoUtil::compressMessage(&compressor, data.size() - 1, msgInfo);
    ASSERT_TRUE(msgInfo.compress);
    ASSERT_TRUE(data.size() > msgInfo.data.size());
}

TEST_F(MessageInfoUtilTest, testConstructMsgSwapData) {
    protocol::WriteMessageInfo writeMsgInfo;
    writeMsgInfo.set_uint8payload(1);
    writeMsgInfo.set_uint16payload(2);
    writeMsgInfo.set_pid(3);
    writeMsgInfo.set_checkpointid(4);
    writeMsgInfo.set_data("abcd");
    writeMsgInfo.set_hashstr("hhh");
    writeMsgInfo.set_compress(true);
    MessageInfo msgInfo;
    MessageInfoUtil::constructMsgInfoSwapData(writeMsgInfo, msgInfo);
    ASSERT_TRUE(msgInfo.compress);
    ASSERT_FALSE(msgInfo.isMerged);
    ASSERT_EQ(1, msgInfo.uint8Payload);
    ASSERT_EQ(2, msgInfo.uint16Payload);
    ASSERT_EQ(3, msgInfo.pid);
    ASSERT_EQ(4, msgInfo.checkpointId);
    ASSERT_EQ(string("abcd"), msgInfo.data);
    ASSERT_EQ(string("hhh"), msgInfo.hashStr);
    ASSERT_EQ(string(""), writeMsgInfo.data());
    ASSERT_EQ(string(""), writeMsgInfo.hashstr());
}

TEST_F(MessageInfoUtilTest, testConstructMsgSwapData_2) {
    protocol::WriteMessageInfoVec writeMsgInfos;
    protocol::WriteMessageInfo *writeMsgInfo = writeMsgInfos.add_messageinfovec();
    writeMsgInfo->set_uint8payload(1);
    writeMsgInfo->set_uint16payload(2);
    writeMsgInfo->set_pid(3);
    writeMsgInfo->set_checkpointid(4);
    writeMsgInfo->set_data("abcd");
    writeMsgInfo->set_hashstr("hhh");
    writeMsgInfo->set_compress(true);

    vector<MessageInfo> msgInfos;
    MessageInfoUtil::constructMsgInfoSwapData(writeMsgInfos, msgInfos);
    ASSERT_EQ(1, msgInfos.size());
    MessageInfo &msgInfo = msgInfos[0];
    ASSERT_TRUE(msgInfo.compress);
    ASSERT_FALSE(msgInfo.isMerged);
    ASSERT_EQ(1, msgInfo.uint8Payload);
    ASSERT_EQ(2, msgInfo.uint16Payload);
    ASSERT_EQ(3, msgInfo.pid);
    ASSERT_EQ(4, msgInfo.checkpointId);
    ASSERT_EQ(string("abcd"), msgInfo.data);
    ASSERT_EQ(string("hhh"), msgInfo.hashStr);
    ASSERT_EQ(string(""), writeMsgInfos.messageinfovec(0).data());
    ASSERT_EQ(string(""), writeMsgInfos.messageinfovec(0).hashstr());
}

}; // namespace util
}; // namespace swift
