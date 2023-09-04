#include "swift/util/MessageConverter.h"

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "autil/TimeUtility.h"
#include "flatbuffers/flatbuffers.h"
#include "flatbuffers/stl_emulation.h"
#include "swift/common/Common.h"
#include "swift/common/MemoryMessage.h"
#include "swift/common/MessageInfo.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace flatbuffers;
namespace swift {
namespace util {
using namespace swift::protocol;
using namespace swift::common;

class MessageConverterTest : public TESTBASE {};

TEST_F(MessageConverterTest, testDecode_encode_PB) {
    BlockPool pool(191, 10);
    MessageConverter converter(&pool);
    vector<Message> Messages;
    vector<MemoryMessage> MemoryMessages;
    int msgCount = 20;
    for (int i = 0; i < msgCount; i++) {
        Message msg;
        MemoryMessage memMsg;
        msg.set_data(string(10, 'a' + i));
        msg.set_merged(i % 2 == 0);
        Messages.push_back(msg);
        ErrorCode ec = converter.encode(msg, memMsg);
        EXPECT_EQ(ERROR_NONE, ec);
        MemoryMessages.push_back(memMsg);
    }
    {
        Message msg;
        MemoryMessage memMsg;
        msg.set_data(string("x"));
        ErrorCode ec = converter.encode(msg, memMsg);
        EXPECT_EQ(ERROR_BROKER_BUSY, ec);
    }
    for (int i = 0; i < msgCount; i++) {
        if (i % 2 == 0) {
            ASSERT_TRUE(0 != MemoryMessages[i].getMsgCount());
            ASSERT_TRUE(MemoryMessages[i].isMerged());
        } else {
            ASSERT_TRUE(!MemoryMessages[i].isMerged());
            ASSERT_EQ(1, MemoryMessages[i].getMsgCount());
        }
    }
    string buffer;
    for (int i = 0; i < msgCount; i++) {
        Message msg;
        converter.decode(MemoryMessages[i], msg, buffer);
        EXPECT_EQ(string(10, 'a' + i), msg.data());
        if (i % 2 == 0) {
            ASSERT_TRUE(msg.merged());
        } else {
            ASSERT_TRUE(!msg.merged());
        }
    }
}

TEST_F(MessageConverterTest, testDecode_Encode_FB) {
    BlockPool pool(191, 10);
    MessageConverter converter(&pool);
    vector<MemoryMessage> MemoryMessages;

    uint32_t msgCount = 20;
    FBMessageWriter writer;
    for (uint32_t i = 0; i < msgCount; ++i) {
        writer.addMessage(string(10, 'a'), i, i + 1, i + 2, i + 3, false, i % 2 == 0);
    }
    writer.finish();
    FBMessageReader reader;
    ASSERT_TRUE(reader.init(writer.data(), writer.size()));
    ASSERT_EQ(msgCount, reader.size());
    for (uint32_t i = 0; i < msgCount; i++) {
        const flat::Message *msg = reader.read(i);
        MemoryMessage memMsg;
        ErrorCode ec = converter.encode(*msg, memMsg);
        EXPECT_EQ(ERROR_NONE, ec);
        MemoryMessages.push_back(memMsg);
    }
    {
        const flat::Message *msg = reader.read(0);
        MemoryMessage memMsg;
        ErrorCode ec = converter.encode(*msg, memMsg);
        EXPECT_EQ(ERROR_BROKER_BUSY, ec);
    }

    writer.reset();
    string buffer;
    for (uint32_t i = 0; i < msgCount; i++) {
        EXPECT_EQ(i, MemoryMessages[i].getMsgId());
        EXPECT_EQ(i + 1, MemoryMessages[i].getTimestamp());
        EXPECT_EQ(i + 2, MemoryMessages[i].getPayload());
        EXPECT_EQ(i + 3, MemoryMessages[i].getMaskPayload());
        EXPECT_EQ(false, MemoryMessages[i].isCompress());
        EXPECT_EQ(10, MemoryMessages[i].getLen());
        if (i % 2 == 0) {
            EXPECT_TRUE(MemoryMessages[i].isMerged());
            EXPECT_TRUE(MemoryMessages[i].getMsgCount() > 1);
        } else {
            EXPECT_TRUE(!MemoryMessages[i].isMerged());
            EXPECT_EQ(1, MemoryMessages[i].getMsgCount());
        }
        converter.decode(MemoryMessages[i], &writer, buffer);
    }

    reader.init(writer.data(), writer.size());
    for (uint32_t i = 0; i < msgCount; i++) {
        const flat::Message *msg = reader.read(i);
        EXPECT_EQ(i, msg->msgId());
        EXPECT_EQ(i + 1, msg->timestamp());
        EXPECT_EQ(i + 2, msg->uint16Payload());
        EXPECT_EQ(i + 3, msg->uint8MaskPayload());
        EXPECT_EQ(false, msg->compress());
        EXPECT_EQ(i % 2 == 0, msg->merged());
        EXPECT_EQ(string(10, 'a'), string(msg->data()->c_str(), msg->data()->size()));
    }
}

TEST_F(MessageConverterTest, testDecode_Encode_ClientMemMsg_MsgInfo) {
    BlockPool pool(191, 10);
    MessageConverter converter(&pool);

    vector<MessageInfo> messageInfos;
    vector<ClientMemoryMessage> clientMemoryMessages;
    int msgCount = 20;
    for (int i = 0; i < msgCount; i++) {
        MessageInfo msgInfo;
        ClientMemoryMessage clientMemMsg;
        msgInfo.data = string(10, 'a' + i);
        msgInfo.isMerged = (i % 2 == 0);
        messageInfos.push_back(msgInfo);
        ErrorCode ec = converter.encode(msgInfo, clientMemMsg);
        EXPECT_EQ(ERROR_NONE, ec);
        clientMemoryMessages.push_back(clientMemMsg);
    }
    {
        MessageInfo msgInfo;
        ClientMemoryMessage clientMemMsg;
        msgInfo.data = string("x");
        ErrorCode ec = converter.encode(msgInfo, clientMemMsg);
        EXPECT_EQ(ERROR_BROKER_BUSY, ec);
    }
    for (int i = 0; i < msgCount; i++) {
        if (i % 2 == 0) {
            ASSERT_TRUE(clientMemoryMessages[i].isMerged);
        } else {
            ASSERT_TRUE(!clientMemoryMessages[i].isMerged);
        }
    }
    string buffer;
    for (int i = 0; i < msgCount; i++) {
        MessageInfo msgInfo;
        uint32_t ret = converter.decode(clientMemoryMessages[i], msgInfo);
        EXPECT_EQ(string(10, 'a' + i), msgInfo.data);
        if (i % 2 == 0) {
            ASSERT_TRUE(msgInfo.isMerged);
            // assert return value
            EXPECT_EQ(*(uint16_t *)(string(10, 'a' + i).c_str()), ret);
        } else {
            ASSERT_TRUE(!msgInfo.isMerged);
            EXPECT_EQ(1, ret);
        }
    }
}

TEST_F(MessageConverterTest, testDecode_PB_MsgInfo) {
    BlockPool pool(191, 10);
    MessageConverter converter(&pool);
    vector<Message> messages;
    vector<MessageInfo> messageInfos;
    vector<uint32_t> retVec;
    int msgCount = 20;
    for (int i = 0; i < msgCount; i++) {
        Message msg;
        MessageInfo msgInfo;
        msg.set_data(string(10, 'a' + i));
        msg.set_merged(i % 2 == 0);
        messages.push_back(msg);
        uint32_t ret = converter.decode(msg, msgInfo);
        retVec.push_back(ret);
        messageInfos.push_back(msgInfo);
    }
    for (int i = 0; i < msgCount; i++) {
        MessageInfo &msgInfo = messageInfos[i];
        EXPECT_EQ(string(10, 'a' + i), msgInfo.data);
        if (i % 2 == 0) {
            ASSERT_TRUE(msgInfo.isMerged);
            // assert return value
            EXPECT_EQ(*(uint16_t *)(string(10, 'a' + i).c_str()), retVec[i]);
        } else {
            ASSERT_TRUE(!msgInfo.isMerged);
            EXPECT_EQ(1, retVec[i]);
        }
    }
}

TEST_F(MessageConverterTest, testCompress) {
    BlockPool pool(201, 10);
    MessageConverter converter(&pool);
    vector<Message> Messages;
    vector<MemoryMessage> MemoryMessages;
    for (int i = 0; i < 20; i++) {
        Message msg;
        msg.set_compress(true);
        MemoryMessage memMsg;
        msg.set_data(string(10, 'a' + i));
        Messages.push_back(msg);
        ErrorCode ec = converter.encode(msg, memMsg);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(int64_t(10), memMsg.getLen());
        MemoryMessages.push_back(memMsg);
        EXPECT_TRUE(memMsg.isCompress());
    }
    EXPECT_EQ(int64_t(20), pool.getUsedBlockCount());
}

TEST_F(MessageConverterTest, testMessageLongerThanBlockSize) {
    BlockPool pool(112, 10);
    MessageConverter converter(&pool);
    vector<Message> Messages;
    vector<MemoryMessage> MemoryMessages;
    for (int i = 0; i < 5; i++) {
        Message msg;
        MemoryMessage memMsg;
        msg.set_data(string(24, 'a' + i));
        Messages.push_back(msg);
        ErrorCode ec = converter.encode(msg, memMsg);
        EXPECT_EQ(ERROR_NONE, ec);
        MemoryMessages.push_back(memMsg);
    }
    {
        Message msg;
        MemoryMessage memMsg;
        msg.set_data(string("x"));
        ErrorCode ec = converter.encode(msg, memMsg);
        EXPECT_EQ(ERROR_BROKER_BUSY, ec);
    }
    string buffer;
    for (int i = 0; i < 5; i++) {
        Message msg;
        converter.decode(MemoryMessages[i], msg, buffer);
        EXPECT_EQ(string(24, 'a' + i), msg.data());
    }
}

TEST_F(MessageConverterTest, testNullMessage) {
    BlockPool pool(201, 10);
    MessageConverter converter(&pool);
    vector<Message> Messages;
    vector<MemoryMessage> MemoryMessages;
    for (int i = 0; i < 2000; i++) {
        Message msg;
        MemoryMessage memMsg;
        msg.set_data(string(""));
        Messages.push_back(msg);
        ErrorCode ec = converter.encode(msg, memMsg);
        EXPECT_EQ(ERROR_NONE, ec);
        MemoryMessages.push_back(memMsg);
    }
    string buf;
    for (int i = 0; i < 2000; i++) {
        Message msg;
        converter.decode(MemoryMessages[i], msg, buf);
        EXPECT_EQ(string(""), msg.data());
    }
}
TEST_F(MessageConverterTest, testPrepareBlock) {
    BlockPool pool(128, 16);
    MessageConverter converter(&pool);
    ASSERT_EQ(ERROR_NONE, converter.prepareBlock(0));
    ASSERT_EQ(0, converter._lastBlockWritePos);
    ASSERT_TRUE(converter._lastBlock);
    ASSERT_EQ(1, pool.getUsedBlockCount());

    ASSERT_EQ(ERROR_NONE, converter.prepareBlock(7));
    ASSERT_EQ(0, converter._lastBlockWritePos);
    ASSERT_EQ(1, pool.getUsedBlockCount());
    ASSERT_EQ(0, pool.getUnusedBlockCount());

    BlockPtr lastBlock = converter._lastBlock;
    ASSERT_EQ(ERROR_NONE, converter.prepareBlock(128));
    ASSERT_EQ(0, converter._lastBlockWritePos);
    ASSERT_EQ(lastBlock.get(), converter._lastBlock.get());
    ASSERT_EQ(8, pool.getUsedBlockCount());
    ASSERT_EQ(0, pool.getUnusedBlockCount());
    converter._lastBlock->setNext(BlockPtr());
    ASSERT_EQ(7, pool.getUnusedBlockCount());

    ASSERT_EQ(ERROR_BROKER_BUSY, converter.prepareBlock(129));
    ASSERT_EQ(0, converter._lastBlockWritePos);
    ASSERT_EQ(lastBlock.get(), converter._lastBlock.get());
    ASSERT_EQ(1, pool.getUsedBlockCount());
    ASSERT_EQ(7, pool.getUnusedBlockCount());

    ASSERT_EQ(ERROR_NONE, converter.prepareBlock(112));
    ASSERT_EQ(0, converter._lastBlockWritePos);
    ASSERT_EQ(lastBlock.get(), converter._lastBlock.get());
    ASSERT_EQ(7, pool.getUsedBlockCount());
    ASSERT_EQ(1, pool.getUnusedBlockCount());
}

} // namespace util
} // namespace swift
