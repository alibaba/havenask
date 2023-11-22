#include "swift/client/MessageWriteBuffer.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/Singleton.h"
#include "autil/ThreadPool.h"
#include "autil/ZlibCompressor.h"
#include "flatbuffers/flatbuffers.h"
#include "flatbuffers/stl_emulation.h"
#include "swift/client/BufferSizeLimiter.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/RangeUtil.h"
#include "swift/common/Common.h"
#include "swift/common/MemoryMessage.h"
#include "swift/common/MessageInfo.h"
#include "swift/common/MessageMeta.h"
#include "swift/common/ThreadBasedObjectPool.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/MessageCompressor.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/MessageInfoUtil.h"
#include "swift/util/MessageQueue.h"
#include "unittest/unittest.h"

using namespace std;
using namespace swift::protocol;
using namespace swift::util;
using namespace swift::common;
using namespace flatbuffers;
namespace swift {
namespace client {

class MessageWriteBufferTest : public TESTBASE {
private:
    autil::ThreadPoolPtr _threadPool;
};

TEST_F(MessageWriteBufferTest, testSimpleAddMessage) {
    BlockPoolPtr blockPool(new BlockPool(102400, 1024));
    MessageWriteBuffer buffer(blockPool, _threadPool);
    EXPECT_EQ(size_t(0), buffer.getSize());
    std::string data(100, 'a');
    MessageInfo msgInfo1 = MessageInfoUtil::constructMsgInfo(data, 0, -1);
    MessageInfo msgInfo2 = MessageInfoUtil::constructMsgInfo(data, 1, -1);
    buffer.addWriteMessage(msgInfo1);
    buffer.addWriteMessage(msgInfo2);
    std::string oneByteData("a");
    for (uint32_t i = 0; i < 90; ++i) {
        MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo(oneByteData, i + 2, -1);
        buffer.addWriteMessage(msgInfo);
    }
    EXPECT_EQ((size_t)0, buffer._unsendCursor);

    EXPECT_EQ(size_t(290 + 92 * sizeof(MessageInfo)), buffer.getSize());
    EXPECT_EQ(size_t(290), buffer.getUnsendSize());
    EXPECT_EQ(size_t(0), buffer.getUncommittedSize());

    protocol::ProductionRequest request;
    EXPECT_EQ(size_t(100), buffer.fillRequest(request, 10));
    EXPECT_EQ((size_t)0, buffer._unsendCursor);
    EXPECT_EQ(1, request.msgs_size());
    EXPECT_EQ(size_t(290 + 92 * sizeof(ClientMemoryMessage)), buffer.getSize());
    EXPECT_EQ(size_t(0), buffer.getUncommittedSize());
    EXPECT_EQ(size_t(290), buffer.getUnsendSize());
    buffer.updateSendBuffer(1); // accepted
    EXPECT_EQ((size_t)1, buffer._unsendCursor);
    EXPECT_EQ(size_t(100), buffer.getUncommittedSize());
    EXPECT_EQ(size_t(190), buffer.getUnsendSize());

    request.Clear();
    EXPECT_EQ(size_t(150), buffer.fillRequest(request, 150));
    EXPECT_EQ(51, request.msgs_size());
    EXPECT_EQ(size_t(290 + 92 * sizeof(ClientMemoryMessage)), buffer.getSize());
    EXPECT_EQ((size_t)1, buffer._unsendCursor);
    EXPECT_EQ(size_t(100), buffer.getUncommittedSize());
    EXPECT_EQ(size_t(190), buffer.getUnsendSize());

    buffer.resetUnsendCursor();
    request.Clear();
    EXPECT_EQ(size_t(200), buffer.fillRequest(request, 150));
    EXPECT_EQ(2, request.msgs_size());
    EXPECT_EQ(size_t(290 + 92 * sizeof(ClientMemoryMessage)), buffer.getSize());
    EXPECT_EQ((size_t)0, buffer._unsendCursor);
    EXPECT_EQ(size_t(0), buffer.getUncommittedSize());
    EXPECT_EQ(size_t(290), buffer.getUnsendSize());

    buffer.resetUnsendCursor();
    request.Clear();
    EXPECT_EQ(size_t(200), buffer.fillRequest(request, 200));
    EXPECT_EQ(2, request.msgs_size());
    EXPECT_EQ(size_t(290 + 92 * sizeof(ClientMemoryMessage)), buffer.getSize());
    EXPECT_EQ((size_t)0, buffer._unsendCursor);
    EXPECT_EQ(size_t(0), buffer.getUncommittedSize());
    EXPECT_EQ(size_t(290), buffer.getUnsendSize());

    buffer.resetUnsendCursor();
    request.Clear();
    EXPECT_EQ(size_t(250), buffer.fillRequest(request, 250)); // 100 * 2 + 1 * 50
    EXPECT_EQ(52, request.msgs_size());
    EXPECT_EQ(size_t(290 + 92 * sizeof(ClientMemoryMessage)), buffer.getSize());
    EXPECT_EQ((size_t)0, buffer._unsendCursor);
    EXPECT_EQ(size_t(0), buffer.getUncommittedSize());
    EXPECT_EQ(size_t(290), buffer.getUnsendSize());

    buffer.resetUnsendCursor();
    request.Clear();
    EXPECT_EQ(size_t(290), buffer.fillRequest(request, 5000));
    EXPECT_EQ(92, request.msgs_size());
    EXPECT_EQ(size_t(290 + 92 * sizeof(ClientMemoryMessage)), buffer.getSize());
    EXPECT_EQ((size_t)0, buffer._unsendCursor);
    EXPECT_EQ(size_t(0), buffer.getUncommittedSize());
    EXPECT_EQ(size_t(290), buffer.getUnsendSize());

    buffer.updateSendBuffer(1); // accepted 1
    vector<int64_t> committedCp;
    EXPECT_EQ(int64_t(0), buffer.popWriteMessage(1, committedCp));
    EXPECT_EQ(1, committedCp.size());
    EXPECT_EQ(0, committedCp[0]);
    EXPECT_EQ(size_t(190 + 91 * sizeof(ClientMemoryMessage)), buffer.getSize());
    int64_t checkpoint;
    buffer.updateBuffer(3, checkpoint); // accepted 3
    EXPECT_EQ(size_t(88 + 88 * sizeof(ClientMemoryMessage)), buffer.getSize());

    buffer.updateBuffer(1000, checkpoint); // accepted all
    EXPECT_EQ(size_t(0), buffer.getSize());

    EXPECT_EQ(size_t(0), buffer.fillRequest(request, 250));
}

TEST_F(MessageWriteBufferTest, testFillRequest_PB) {
    BlockPoolPtr blockPool(new BlockPool(102400, 1024));
    MessageWriteBuffer buffer(blockPool, _threadPool);
    EXPECT_EQ(size_t(0), buffer.getSize());
    std::string data(100, 'a');
    MessageInfo messageInfo;
    messageInfo.data = data;
    messageInfo.checkpointId = 0;
    EXPECT_TRUE(buffer.addWriteMessage(messageInfo));
    EXPECT_EQ(string(""), buffer._writeQueue.back()->hashStr);
    messageInfo = MessageInfoUtil::constructMsgInfo(data, 0, 1, 2, 3, "aa");
    EXPECT_TRUE(buffer.addWriteMessage(messageInfo));
    EXPECT_EQ(string(""), buffer._writeQueue.back()->hashStr);
    messageInfo = MessageInfoUtil::constructMsgInfo(data, 0, 3, 4, 5, "bb");
    EXPECT_TRUE(buffer.addWriteMessage(messageInfo));
    EXPECT_EQ(string(""), buffer._writeQueue.back()->hashStr);
    protocol::ProductionRequest request;
    EXPECT_EQ(size_t(200), buffer.fillRequest(request, 150));
    EXPECT_EQ((int32_t)2, request.msgs_size());
    protocol::Message *msg = request.mutable_msgs(0);
    EXPECT_TRUE(msg->has_uint16payload());
    EXPECT_EQ((uint32_t)0, msg->uint16payload());
    EXPECT_EQ((uint32_t)0, msg->uint8maskpayload());
    msg = request.mutable_msgs(1);
    EXPECT_TRUE(msg->has_uint16payload());
    EXPECT_EQ((uint32_t)1, msg->uint16payload());
    EXPECT_EQ((uint32_t)2, msg->uint8maskpayload());
    EXPECT_FALSE(msg->compress());
    EXPECT_FALSE(msg->merged());
}

TEST_F(MessageWriteBufferTest, testFillRequest_FB) {
    BlockPoolPtr blockPool(new BlockPool(102400, 1024));
    MessageWriteBuffer buffer(blockPool, _threadPool);
    EXPECT_EQ(size_t(0), buffer.getSize());
    std::string data(100, 'a');
    MessageInfo messageInfo;
    messageInfo.data = data;
    messageInfo.checkpointId = 0;
    EXPECT_TRUE(buffer.addWriteMessage(messageInfo));
    EXPECT_EQ(string(""), buffer._writeQueue.back()->hashStr);
    messageInfo = MessageInfoUtil::constructMsgInfo(data, 0, 1, 2, 3, "aa");
    EXPECT_TRUE(buffer.addWriteMessage(messageInfo));
    EXPECT_EQ(string(""), buffer._writeQueue.back()->hashStr);
    messageInfo = MessageInfoUtil::constructMsgInfo(data, 0, 3, 4);
    EXPECT_TRUE(buffer.addWriteMessage(messageInfo));
    EXPECT_EQ(string(""), buffer._writeQueue.back()->hashStr);
    FBMessageWriter writer;
    int64_t i = 0;
    EXPECT_EQ(size_t(200), buffer.fillRequest(&writer, 150, i));

    const flat::Messages *msgs = flatbuffers::GetRoot<flat::Messages>(writer.data());
    EXPECT_EQ((int32_t)2, msgs->msgs()->size());

    const flat::Message *msg = msgs->msgs()->Get(0);
    EXPECT_EQ((uint32_t)0, msg->uint16Payload());
    EXPECT_EQ((uint32_t)0, msg->uint8MaskPayload());
    msg = msgs->msgs()->Get(1);
    EXPECT_EQ((uint32_t)1, msg->uint16Payload());
    EXPECT_EQ((uint32_t)2, msg->uint8MaskPayload());
    EXPECT_FALSE(msg->compress());
    EXPECT_FALSE(msg->merged());
}

TEST_F(MessageWriteBufferTest, testFillRequestWithMerge) {
    BlockPoolPtr blockPool(new BlockPool(102400, 1024));
    MessageWriteBuffer buffer(blockPool, _threadPool);
    buffer._mergeMsg = true;
    RangeUtilPtr rangeUtil(new RangeUtil(1, 2));
    buffer._rangeUtil = rangeUtil;
    EXPECT_EQ(size_t(0), buffer.getSize());

    std::string data(100, 'a');
    MessageInfo msg1(data, 0, 0, 1);
    MessageInfo msg2(data, 1, 20000, 2);
    MessageInfo msg3(data, 4, 40000, 3);
    MessageInfo msg4(data, 1, 50000, 4);
    buffer.addWriteMessage(msg1);
    buffer.addWriteMessage(msg2);
    buffer.addWriteMessage(msg3);
    buffer.addWriteMessage(msg4);
    {
        protocol::ProductionRequest request;
        EXPECT_EQ(size_t(218), buffer.fillRequest(request, 150)); // msg1+msg2, 200 + 16 + 2
        EXPECT_EQ((int32_t)1, request.msgs_size());
        protocol::Message *msg = request.mutable_msgs(0);
        EXPECT_EQ((uint32_t)32767, msg->uint16payload());
        EXPECT_EQ((uint32_t)1, msg->uint8maskpayload());
        EXPECT_EQ(218, msg->data().size());
    }
    {
        protocol::ProductionRequest request;
        EXPECT_EQ(size_t(436), buffer.fillRequest(request, 1000));
        EXPECT_EQ((int32_t)2, request.msgs_size());
        protocol::Message *msg = request.mutable_msgs(0);
        EXPECT_EQ((uint32_t)32767, msg->uint16payload());
        EXPECT_EQ((uint32_t)1, msg->uint8maskpayload());
        EXPECT_EQ(218, msg->data().size());

        msg = request.mutable_msgs(1);
        EXPECT_EQ((uint32_t)65535, msg->uint16payload());
        EXPECT_EQ((uint32_t)5, msg->uint8maskpayload());
        EXPECT_EQ(218, msg->data().size());
    }
    EXPECT_EQ(size_t(218 * 2 + sizeof(ClientMemoryMessage) * 2), buffer.getSize());
}

TEST_F(MessageWriteBufferTest, testCompressAndAddMsgToSend) {
    swift::common::ThreadBasedObjectPool<autil::ZlibCompressor> *compressorPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<autil::ZlibCompressor>,
                         protocol::ZlibCompressorInstantiation>::getInstance();
    autil::ZlibCompressor *compressor = compressorPool->getObject();
    string data1(8, 'a');
    string data2(16, 'a');
    {
        MessageInfo msg0(data1, 0, 0, 1);
        EXPECT_EQ(8, msg0.data.size());
        MessageInfoUtil::compressMessage(compressor, 5, msg0);
        EXPECT_FALSE(msg0.compress);
        EXPECT_EQ(8, msg0.data.size());
    }
    {
        MessageInfo msg0(data2, 0, 0, 1);
        EXPECT_EQ(16, msg0.data.size());
        MessageInfoUtil::compressMessage(compressor, 5, msg0);
        EXPECT_TRUE(msg0.compress);
        EXPECT_EQ(11, msg0.data.size());
    }
    {
        BlockPoolPtr blockPool(new BlockPool(102400, 1024));
        BufferSizeLimiterPtr limiter(new BufferSizeLimiter(300));
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter);
        buffer._compressThresholdInBytes = 5;
        MessageInfo msg1(data1, 0, 0, 1);
        MessageInfo msgToAdd = msg1;
        buffer.addWriteMessage(msgToAdd);
        EXPECT_EQ(sizeof(MessageInfo) + 8, buffer._limiter->getCurSize());
        EXPECT_EQ(8, buffer._dataSize);
        ASSERT_TRUE(buffer.compressAndAddMsgToSend(&msg1, compressor));
        EXPECT_EQ(sizeof(MessageInfo) + 8, buffer._limiter->getCurSize());
        EXPECT_EQ(8, buffer._dataSize);
        EXPECT_EQ(1, buffer._sendQueue.size());
        ASSERT_FALSE(msg1.compress);
    }
    { // compress
        BlockPoolPtr blockPool(new BlockPool(102400, 1024));
        BufferSizeLimiterPtr limiter(new BufferSizeLimiter(300));
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter);
        buffer._compressThresholdInBytes = 5;
        MessageInfo msg2(data2, 1, 20000, 2);
        MessageInfo msgToAdd = msg2;
        buffer.addWriteMessage(msgToAdd);
        EXPECT_EQ(sizeof(MessageInfo) + 16, buffer._limiter->getCurSize());
        EXPECT_EQ(16, buffer._dataSize);
        ASSERT_TRUE(buffer.compressAndAddMsgToSend(&msg2, compressor));
        EXPECT_EQ(sizeof(MessageInfo) + 11, buffer._limiter->getCurSize());
        EXPECT_EQ(11, buffer._dataSize);
        EXPECT_EQ(1, buffer._sendQueue.size());
        ASSERT_TRUE(msg2.compress);
    }
    { // no compress
        BlockPoolPtr blockPool(new BlockPool(102400, 1024));
        BufferSizeLimiterPtr limiter(new BufferSizeLimiter(300));
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter);
        buffer._compressThresholdInBytes = 5;
        MessageInfo msg2(data2, 1, 20000, 2);
        MessageInfo msgToAdd = msg2;
        buffer.addWriteMessage(msgToAdd);
        EXPECT_EQ(sizeof(MessageInfo) + 16, buffer._limiter->getCurSize());
        EXPECT_EQ(16, buffer._dataSize);
        ASSERT_TRUE(buffer.compressAndAddMsgToSend(&msg2, compressor, false));
        EXPECT_EQ(sizeof(MessageInfo) + data2.size(), buffer._limiter->getCurSize());
        EXPECT_EQ(data2.size(), buffer._dataSize);
        EXPECT_EQ(1, buffer._sendQueue.size());
        ASSERT_TRUE(!msg2.compress);
    }

    { // add to send failed
        BlockPoolPtr blockPool(new BlockPool(sizeof(ClientMemoryMessage), sizeof(ClientMemoryMessage)));
        BufferSizeLimiterPtr limiter(new BufferSizeLimiter(300));
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter);
        buffer._compressThresholdInBytes = 5;
        MessageInfo msg1(data1, 0, 0, 1);
        MessageInfo msgToAdd = msg1;
        buffer.addWriteMessage(msgToAdd);
        EXPECT_EQ(sizeof(MessageInfo) + 8, buffer._limiter->getCurSize());
        EXPECT_EQ(8, buffer._dataSize);
        ASSERT_FALSE(buffer.compressAndAddMsgToSend(&msg1, compressor));
        EXPECT_EQ(sizeof(MessageInfo) + 8, buffer._limiter->getCurSize());
        EXPECT_EQ(8, buffer._dataSize);
        EXPECT_EQ(0, buffer._sendQueue.size());
        ASSERT_FALSE(msg1.compress);
    }
    { // compress, add to send failed
        BlockPoolPtr blockPool(new BlockPool(sizeof(ClientMemoryMessage), sizeof(ClientMemoryMessage)));
        BufferSizeLimiterPtr limiter(new BufferSizeLimiter(300));
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter);
        buffer._compressThresholdInBytes = 5;
        MessageInfo msg2(data2, 1, 20000, 2);
        MessageInfo msgToAdd = msg2;
        buffer.addWriteMessage(msgToAdd);
        EXPECT_EQ(sizeof(MessageInfo) + 16, buffer._limiter->getCurSize());
        EXPECT_EQ(16, buffer._dataSize);
        ASSERT_FALSE(buffer.compressAndAddMsgToSend(&msg2, compressor));
        EXPECT_EQ(sizeof(MessageInfo) + 11, buffer._limiter->getCurSize());
        EXPECT_EQ(11, buffer._dataSize);
        EXPECT_EQ(0, buffer._sendQueue.size());
        ASSERT_TRUE(msg2.compress);
    }
}

TEST_F(MessageWriteBufferTest, testUpdateCommitBuffer) {
    BlockPoolPtr blockPool(new BlockPool(102400, 1024));
    MessageWriteBuffer buffer(blockPool, _threadPool);
    EXPECT_TRUE(buffer._uncommittedMsgs.empty());

    vector<int64_t> timestamps;
    vector<int64_t> committedTs;
    // [0, 100) has been accepted, but has not been committed
    for (int i = 0; i < 100; ++i) {
        timestamps.emplace_back(i * 10);
    }
    int64_t committedCount = buffer.updateCommitBuffer(-1, 0, 100, timestamps, committedTs);
    EXPECT_EQ(0, committedCount);
    EXPECT_EQ(1, buffer._uncommittedMsgs.size());
    EXPECT_EQ(0, buffer._uncommittedMsgs[0].first.first);
    EXPECT_EQ(99, buffer._uncommittedMsgs[0].first.second);
    EXPECT_EQ(0, committedTs.size());

    // [0, 130) has been accepted, [0, 30) has been committed
    timestamps.clear();
    committedTs.clear();
    for (int i = 150; i < 180; ++i) {
        timestamps.emplace_back(i * 10);
    }
    committedCount = buffer.updateCommitBuffer(29, 150, 30, timestamps, committedTs);
    EXPECT_EQ(30, committedCount);
    EXPECT_EQ(2, buffer._uncommittedMsgs.size());
    EXPECT_EQ(30, buffer._uncommittedMsgs[0].first.first);
    EXPECT_EQ(99, buffer._uncommittedMsgs[0].first.second);
    EXPECT_EQ(150, buffer._uncommittedMsgs[1].first.first);
    EXPECT_EQ(179, buffer._uncommittedMsgs[1].first.second);
    EXPECT_EQ(30, committedTs.size());
    for (int i = 0; i < 30; ++i) {
        EXPECT_EQ(i * 10, committedTs[i]);
    }

    // all committed (response from detect request)
    timestamps.clear();
    committedTs.clear();
    committedCount = buffer.updateCommitBuffer(129, 0, 0, timestamps, committedTs);
    EXPECT_EQ(70, committedCount);
    EXPECT_EQ(1, buffer._uncommittedMsgs.size());
    EXPECT_EQ(150, buffer._uncommittedMsgs[0].first.first);
    EXPECT_EQ(179, buffer._uncommittedMsgs[0].first.second);
    EXPECT_EQ(70, committedTs.size());
    for (int i = 0; i < 70; ++i) {
        EXPECT_EQ((i + 30) * 10, committedTs[i]);
    }
    // again
    timestamps.clear();
    committedTs.clear();
    committedCount = buffer.updateCommitBuffer(180, 0, 0, timestamps, committedTs);
    EXPECT_EQ(30, committedCount);
    EXPECT_EQ(0, buffer._uncommittedMsgs.size());
    EXPECT_EQ(30, committedTs.size());
    for (int i = 0; i < 30; ++i) {
        EXPECT_EQ((i + 150) * 10, committedTs[i]);
    }

    timestamps.clear();
    committedTs.clear();
    committedCount = buffer.updateCommitBuffer(180, 0, 0, timestamps, committedTs);
    EXPECT_EQ(0, committedCount);
    EXPECT_EQ(0, committedTs.size());
}

TEST_F(MessageWriteBufferTest, testMergeMessage) {
    {
        BlockPoolPtr blockPool(new BlockPool(102400, 1024));
        BufferSizeLimiterPtr limiter(new BufferSizeLimiter(600));
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter);
        buffer._mergeMsg = true;
        buffer._mergeCount = 2;
        buffer._mergeSize = 3;
        RangeUtilPtr rangeUtil(new RangeUtil(1, 2));
        buffer._rangeUtil = rangeUtil;
        EXPECT_EQ(size_t(0), buffer.getSize());
        MessageInfo msg1("a", 0, 0, 1);
        MessageInfo msg2("b", 1, 20000, 2);
        MessageInfo msg3("abc", 1, 20000, 2);
        MessageInfo msg4("ab", 1, 20000, 2);
        MessageInfo msg5("a", 4, 40000, 3);
        MessageInfo msg6("b", 1, 50000, 4);
        EXPECT_EQ(0, buffer._limiter->getCurSize());
        EXPECT_TRUE(buffer.addWriteMessage(msg1));
        EXPECT_TRUE(buffer.addWriteMessage(msg5));
        EXPECT_TRUE(buffer.addWriteMessage(msg2));
        EXPECT_TRUE(buffer.addWriteMessage(msg3));
        EXPECT_TRUE(buffer.addWriteMessage(msg6));
        EXPECT_TRUE(buffer.addWriteMessage(msg4));
        size_t expectLen = sizeof(MessageInfo) * 6 + 9;
        EXPECT_EQ(expectLen, buffer._limiter->getCurSize());
        EXPECT_EQ(expectLen, buffer.getSize());
        EXPECT_EQ(size_t(6), buffer._writeQueue.size());
        EXPECT_EQ(size_t(0), buffer._sendQueue.size());

        buffer.convertMessage();
        EXPECT_EQ(0, buffer._limiter->getCurSize());
        expectLen =
            9 + sizeof(ClientMemoryMessage) * 4 + 2 * (sizeof(common::MergedMessageMeta) * 2 + sizeof(uint16_t));

        EXPECT_EQ(expectLen, buffer.getSize());
        EXPECT_EQ(size_t(0), buffer._writeQueue.size());
        EXPECT_EQ(size_t(4), buffer._sendQueue.size());
        EXPECT_TRUE(buffer._sendQueue[0].isMerged);
        EXPECT_TRUE(!buffer._sendQueue[1].isMerged);
        EXPECT_TRUE(buffer._sendQueue[2].isMerged);
        EXPECT_TRUE(!buffer._sendQueue[3].isMerged);
    }
    {
        BlockPoolPtr blockPool(new BlockPool(102400, 1024));
        BufferSizeLimiterPtr limiter(new BufferSizeLimiter(300));
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter);
        buffer._mergeMsg = true;
        buffer._mergeCount = 3;
        buffer._mergeSize = 3;
        RangeUtilPtr rangeUtil(new RangeUtil(1, 2));
        buffer._rangeUtil = rangeUtil;
        EXPECT_EQ(size_t(0), buffer.getSize());
        MessageInfo msg1("a", 0, 0, 1);
        MessageInfo msg2("b", 1, 20000, 2);
        MessageInfo msg3("c", 1, 50000, 4);
        EXPECT_TRUE(buffer.addWriteMessage(msg1));
        EXPECT_TRUE(buffer.addWriteMessage(msg2));
        EXPECT_TRUE(buffer.addWriteMessage(msg3));
        size_t expectLen = sizeof(MessageInfo) * 3 + 3;
        EXPECT_EQ(expectLen, buffer._limiter->getCurSize());
        EXPECT_EQ(expectLen, buffer.getSize());
        EXPECT_EQ(size_t(3), buffer._writeQueue.size());
        EXPECT_EQ(size_t(0), buffer._sendQueue.size());
        buffer.convertMessage();
        EXPECT_EQ(0, buffer._limiter->getCurSize());
        expectLen = 3 + sizeof(ClientMemoryMessage) * 2 + sizeof(common::MergedMessageMeta) * 2 + sizeof(uint16_t);
        EXPECT_EQ(expectLen, buffer.getSize());
        EXPECT_EQ(size_t(0), buffer._writeQueue.size());
        EXPECT_EQ(size_t(2), buffer._sendQueue.size());
        EXPECT_TRUE(buffer._sendQueue[0].isMerged);
        EXPECT_TRUE(!buffer._sendQueue[1].isMerged);
    }
}

TEST_F(MessageWriteBufferTest, testMergeMessageLostMessage) {
    BlockPoolPtr blockPool(new BlockPool(60, 60));
    int count = 65600;
    BufferSizeLimiterPtr limiter(new BufferSizeLimiter(sizeof(MessageInfo) * count + count));
    MessageWriteBuffer buffer(blockPool, _threadPool, limiter, 10000000);
    buffer._mergeMsg = true;
    buffer._mergeCount = 64;
    buffer._mergeSize = 512;
    RangeUtilPtr rangeUtil(new RangeUtil(1, 65536));
    buffer._rangeUtil = rangeUtil;
    EXPECT_EQ(size_t(0), buffer.getSize());
    for (int i = 0; i < count; i++) {
        MessageInfo msg1("a", 0, 0, 1);
        EXPECT_TRUE(buffer.addWriteMessage(msg1)) << i;
    }
    size_t expectLen = sizeof(MessageInfo) * count + count;
    EXPECT_EQ(size_t(count), buffer._writeQueue.size());
    EXPECT_EQ(size_t(0), buffer._sendQueue.size());
    EXPECT_EQ(expectLen, buffer._limiter->getCurSize());
    EXPECT_EQ(expectLen, buffer.getSize());

    buffer.convertMessage();
    EXPECT_EQ(expectLen, buffer._limiter->getCurSize());
    EXPECT_EQ(size_t(0), buffer._writeQueue.size());
    EXPECT_EQ(size_t(count - 64), buffer._toMergeQueue.size());
    EXPECT_EQ(size_t(0), buffer._sendQueue.size());

    EXPECT_EQ(size_t(1), buffer._msgMap.size());
    EXPECT_EQ(size_t(64), buffer._msgMap.begin()->second.size());

    for (int i = 0; i < count; i++) {
        buffer.convertMessage();
        EXPECT_EQ(expectLen, buffer._limiter->getCurSize());
        EXPECT_EQ(size_t(0), buffer._writeQueue.size());
        EXPECT_EQ(size_t(count - 64), buffer._toMergeQueue.size());
        EXPECT_EQ(size_t(0), buffer._sendQueue.size());

        EXPECT_EQ(size_t(1), buffer._msgMap.size());
        EXPECT_EQ(size_t(64), buffer._msgMap.begin()->second.size());
    }
}
TEST_F(MessageWriteBufferTest, testMergeMessageWithCompress) {
    BlockPoolPtr blockPool(new BlockPool(102400, 1024));
    BufferSizeLimiterPtr limiter(new BufferSizeLimiter(800));
    MessageWriteBuffer buffer(blockPool, _threadPool, limiter);
    buffer._mergeMsg = true;
    buffer._compressThresholdInBytes = 15;
    buffer._mergeCount = 3;
    buffer._mergeSize = 32;
    RangeUtilPtr rangeUtil(new RangeUtil(1, 2));
    buffer._rangeUtil = rangeUtil;
    EXPECT_EQ(size_t(0), buffer.getSize());
    swift::common::ThreadBasedObjectPool<autil::ZlibCompressor> *compressorPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<autil::ZlibCompressor>,
                         protocol::ZlibCompressorInstantiation>::getInstance();
    autil::ZlibCompressor *compressor = compressorPool->getObject();
    compressor->reset();
    string data1(8, 'a');
    string data2(16, 'a');
    string data3(32, 'a');
    {
        MessageInfo msg0(data1, 0, 0, 1);
        EXPECT_EQ(8, msg0.data.size());
        MessageInfoUtil::compressMessage(compressor, 5, msg0);
        EXPECT_FALSE(msg0.compress);
        EXPECT_EQ(8, msg0.data.size());
    }
    {
        MessageInfo msg0(data2, 0, 0, 1);
        EXPECT_EQ(16, msg0.data.size());
        MessageInfoUtil::compressMessage(compressor, 5, msg0);
        EXPECT_TRUE(msg0.compress);
        EXPECT_EQ(11, msg0.data.size());
    }
    {
        MessageInfo msg0(data3, 0, 0, 1);
        EXPECT_EQ(32, msg0.data.size());
        MessageInfoUtil::compressMessage(compressor, 5, msg0);
        EXPECT_TRUE(msg0.compress);
        EXPECT_EQ(11, msg0.data.size());
    }
    MessageInfo msg1(data1, 0, 0, 1);
    MessageInfo msg2(data1, 1, 20000, 2);
    MessageInfo msg3(data1, 1, 20000, 2);

    MessageInfo msg4(data1, 1, 20000, 2);
    MessageInfo msg5(data2, 4, 20000, 3);

    MessageInfo msg6(data3, 1, 50000, 4);

    MessageInfo msg7(data2, 1, 50000, 4);

    EXPECT_EQ(0, buffer._limiter->getCurSize());
    EXPECT_TRUE(buffer.addWriteMessage(msg1));
    EXPECT_TRUE(buffer.addWriteMessage(msg2));
    EXPECT_TRUE(buffer.addWriteMessage(msg3));
    EXPECT_TRUE(buffer.addWriteMessage(msg4));
    EXPECT_TRUE(buffer.addWriteMessage(msg5));
    EXPECT_TRUE(buffer.addWriteMessage(msg6));
    EXPECT_TRUE(buffer.addWriteMessage(msg7));
    size_t expectLen = sizeof(MessageInfo) * 7 + 8 * 4 + 16 * 2 + 32;
    EXPECT_EQ(expectLen, buffer._limiter->getCurSize());
    EXPECT_EQ(expectLen, buffer.getSize());
    EXPECT_EQ(size_t(7), buffer._writeQueue.size());
    EXPECT_EQ(size_t(0), buffer._sendQueue.size());
    buffer.convertMessage();
    expectLen = sizeof(ClientMemoryMessage) * 4 + 25 + 11 + 26 + 11;
    EXPECT_EQ(0, buffer._limiter->getCurSize());
    EXPECT_EQ(expectLen, buffer.getSize());
    EXPECT_EQ(size_t(0), buffer._writeQueue.size());
    EXPECT_EQ(size_t(4), buffer._sendQueue.size());
    EXPECT_TRUE(buffer._sendQueue[0].isMerged);
    EXPECT_TRUE(!buffer._sendQueue[1].isMerged);
    EXPECT_TRUE(buffer._sendQueue[2].isMerged);
    EXPECT_TRUE(!buffer._sendQueue[3].isMerged);

    EXPECT_EQ(25, buffer._sendQueue[0].msgLen);
    EXPECT_EQ(11, buffer._sendQueue[1].msgLen);
    EXPECT_EQ(26, buffer._sendQueue[2].msgLen);
    EXPECT_EQ(11, buffer._sendQueue[3].msgLen);
}

TEST_F(MessageWriteBufferTest, testMergeMessageWithInvalidMsg) {
    BlockPoolPtr blockPool(new BlockPool(102400, 1024));
    MessageWriteBuffer buffer(blockPool, _threadPool);
    buffer._mergeMsg = true;
    buffer._mergeCount = 3;
    buffer._mergeSize = 3;
    RangeUtilPtr rangeUtil(new RangeUtil(1, 2));
    buffer._rangeUtil = rangeUtil;
    EXPECT_EQ(size_t(0), buffer.getSize());
    MessageInfo msg1("a", 0, 0, 1);
    MessageInfo msg2("b", 1, 20000, 2);
    MessageInfo msg3("c", 1, 20000, 2);
    MessageInfo msg4("d", 1, 20000, 2);
    msg3.compress = true;
    EXPECT_TRUE(buffer.addWriteMessage(msg1));
    EXPECT_TRUE(buffer.addWriteMessage(msg2));
    EXPECT_FALSE(buffer.addWriteMessage(msg3));
    EXPECT_TRUE(buffer.addWriteMessage(msg4));
    size_t expectLen = sizeof(MessageInfo) * 3 + 3;
    EXPECT_EQ(expectLen, buffer.getSize());
    EXPECT_EQ(size_t(3), buffer._writeQueue.size());
    EXPECT_EQ(size_t(0), buffer._sendQueue.size());
    buffer.convertMessage();
    expectLen = sizeof(ClientMemoryMessage) * 1 + 3 + sizeof(common::MergedMessageMeta) * 3 + sizeof(uint16_t);
    EXPECT_EQ(expectLen, buffer.getSize());
    EXPECT_EQ(size_t(0), buffer._writeQueue.size());
    EXPECT_EQ(size_t(1), buffer._sendQueue.size());
    EXPECT_TRUE(buffer._sendQueue[0].isMerged);
}

TEST_F(MessageWriteBufferTest, testPoolFull) {
    size_t msgLen = sizeof(MessageInfo) + 8;
    BufferSizeLimiterPtr limiter(new BufferSizeLimiter(msgLen * 3));
    BlockPoolPtr blockPool(new BlockPool(sizeof(ClientMemoryMessage) * 3, sizeof(ClientMemoryMessage)));
    MessageWriteBuffer buffer(blockPool, _threadPool, limiter);
    EXPECT_EQ(size_t(0), buffer.getSize());
    MessageInfo msg1("aaaaaaaa", 0, 0, 1);
    MessageInfo msg2("bbbbbbbb", 1, 20000, 2);
    MessageInfo msg3("cccccccc", 1, 20000, 2);
    MessageInfo msg4("dddddddd", 1, 20000, 2);
    EXPECT_EQ(0, buffer._limiter->getCurSize());
    EXPECT_TRUE(buffer.addWriteMessage(msg1));
    EXPECT_TRUE(buffer.addWriteMessage(msg2));
    EXPECT_TRUE(buffer.addWriteMessage(msg3));
    EXPECT_EQ(msgLen * 3, buffer._limiter->getCurSize());
    EXPECT_FALSE(buffer.addWriteMessage(msg4)); // temp size limit
    buffer.convertMessage();
    EXPECT_EQ(msgLen, buffer._limiter->getCurSize());
    EXPECT_FALSE(buffer.addWriteMessage(msg4)); // to merge limit

    EXPECT_EQ(sizeof(MessageInfo) + 24 + sizeof(ClientMemoryMessage) * 2, buffer.getSize());
    EXPECT_EQ(size_t(0), buffer._writeQueue.size());
    EXPECT_EQ(size_t(1), buffer._toMergeQueue.size());
    EXPECT_EQ(size_t(2), buffer._sendQueue.size());
    EXPECT_EQ(size_t(3), buffer.getQueueSize());
    int64_t checkPointId;
    buffer.updateBuffer(1, checkPointId);
    EXPECT_EQ(1, checkPointId);

    EXPECT_FALSE(buffer.addWriteMessage(msg4)); // to merge limit
    EXPECT_EQ(sizeof(MessageInfo) + 16 + sizeof(ClientMemoryMessage), buffer.getSize());
    EXPECT_EQ(size_t(0), buffer._writeQueue.size());
    EXPECT_EQ(size_t(1), buffer._toMergeQueue.size());
    EXPECT_EQ(size_t(1), buffer._sendQueue.size());
    EXPECT_EQ(size_t(2), buffer.getQueueSize());
    buffer.convertMessage();
    EXPECT_EQ(0, buffer._limiter->getCurSize());

    EXPECT_EQ(sizeof(ClientMemoryMessage) * 2 + 16, buffer.getSize());
    EXPECT_EQ(size_t(0), buffer._writeQueue.size());
    EXPECT_EQ(size_t(0), buffer._toMergeQueue.size());
    EXPECT_EQ(size_t(2), buffer._sendQueue.size());
    EXPECT_TRUE(buffer.addWriteMessage(msg4));
    EXPECT_EQ(size_t(3), buffer.getQueueSize());
    EXPECT_EQ(msgLen, buffer._limiter->getCurSize());
}

TEST_F(MessageWriteBufferTest, testPoolFullWithCompress) {
    BufferSizeLimiterPtr limiter(new BufferSizeLimiter(sizeof(MessageInfo) * 3 + 44));
    BlockPoolPtr blockPool(new BlockPool(sizeof(ClientMemoryMessage) * 3, sizeof(ClientMemoryMessage)));
    MessageWriteBuffer buffer(blockPool, _threadPool, limiter);
    buffer._compressThresholdInBytes = 5;
    EXPECT_EQ(size_t(0), buffer.getSize());
    swift::common::ThreadBasedObjectPool<autil::ZlibCompressor> *compressorPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<autil::ZlibCompressor>,
                         protocol::ZlibCompressorInstantiation>::getInstance();
    autil::ZlibCompressor *compressor = compressorPool->getObject();
    compressor->reset();

    MessageInfo msg0("aaaaaaaaaaaa", 0, 0, 1);
    EXPECT_EQ(12, msg0.data.size());
    MessageInfoUtil::compressMessage(compressor, 5, msg0);
    EXPECT_EQ(11, msg0.data.size());

    MessageInfo msg1("aaaaaaaaaaaa", 0, 0, 1);
    MessageInfo msg2("aaaaaaaaaaaa", 1, 20000, 2);
    MessageInfo msg3("aaaaaaaaaaaa", 1, 20000, 2);
    MessageInfo msg4("aaaaaaaaaaaa", 1, 20000, 2);
    EXPECT_EQ(0, buffer._limiter->getCurSize());
    EXPECT_TRUE(buffer.addWriteMessage(msg1));
    EXPECT_TRUE(buffer.addWriteMessage(msg2));
    EXPECT_TRUE(buffer.addWriteMessage(msg3));
    size_t expectLen = sizeof(MessageInfo) * 3 + 36;
    EXPECT_EQ(expectLen, buffer._limiter->getCurSize());
    EXPECT_FALSE(buffer.addWriteMessage(msg4)); // temp size limit
    buffer.convertMessage();

    EXPECT_EQ(sizeof(MessageInfo) + 11, buffer._limiter->getCurSize());
    EXPECT_FALSE(buffer.addWriteMessage(msg4)); // to merge limit

    expectLen = sizeof(ClientMemoryMessage) * 2 + 11 * 3 + sizeof(MessageInfo);
    EXPECT_EQ(expectLen, buffer.getSize());
    EXPECT_EQ(size_t(0), buffer._writeQueue.size());
    EXPECT_EQ(size_t(1), buffer._toMergeQueue.size());
    EXPECT_EQ(size_t(2), buffer._sendQueue.size());
    EXPECT_EQ(size_t(3), buffer.getQueueSize());
    int64_t checkPointId;
    buffer.updateBuffer(1, checkPointId);
    EXPECT_EQ(1, checkPointId);

    EXPECT_FALSE(buffer.addWriteMessage(msg4)); // to merge limit
    expectLen = 11 * 2 + sizeof(ClientMemoryMessage) + sizeof(MessageInfo);
    EXPECT_EQ(expectLen, buffer.getSize()); // 11+11 + 1*32 + 1*40
    EXPECT_EQ(size_t(0), buffer._writeQueue.size());
    EXPECT_EQ(size_t(1), buffer._toMergeQueue.size());
    EXPECT_EQ(size_t(1), buffer._sendQueue.size());
    EXPECT_EQ(size_t(2), buffer.getQueueSize());
    buffer.convertMessage();
    EXPECT_EQ(0, buffer._limiter->getCurSize());

    EXPECT_EQ(11 * 2 + sizeof(ClientMemoryMessage) * 2, buffer.getSize()); // 11+11+2*40
    EXPECT_EQ(size_t(0), buffer._writeQueue.size());
    EXPECT_EQ(size_t(0), buffer._toMergeQueue.size());
    EXPECT_EQ(size_t(2), buffer._sendQueue.size());
    EXPECT_TRUE(buffer.addWriteMessage(msg4));
    EXPECT_EQ(size_t(3), buffer.getQueueSize());

    EXPECT_EQ(12 + sizeof(MessageInfo), buffer._limiter->getCurSize());
}

TEST_F(MessageWriteBufferTest, testUpdateBuffer) {
    BlockPoolPtr blockPool(new BlockPool(sizeof(ClientMemoryMessage) * 30, sizeof(ClientMemoryMessage)));
    MessageWriteBuffer buffer(blockPool, _threadPool);
    EXPECT_EQ(size_t(0), buffer.getSize());
    MessageInfo msg1("a", 0, 0, 1); // data, mask, payload, checkpoint
    MessageInfo msg2("b", 1, 20000, 2);
    MessageInfo msg3("c", 2, 20000, 2);
    // checkpoint-timestamp: msg1 1-10, msg2 2-20, msg3 2-30
    EXPECT_TRUE(buffer.addWriteMessage(msg1));
    EXPECT_TRUE(buffer.addWriteMessage(msg2));
    EXPECT_TRUE(buffer.addWriteMessage(msg3));
    EXPECT_EQ(size_t(3), buffer._writeQueue.size());
    EXPECT_EQ(size_t(0), buffer._toMergeQueue.size());
    EXPECT_EQ(size_t(0), buffer._sendQueue.size());
    buffer.convertMessage();
    EXPECT_EQ(size_t(0), buffer._writeQueue.size());
    EXPECT_EQ(size_t(0), buffer._toMergeQueue.size());
    EXPECT_EQ(size_t(3), buffer._sendQueue.size());

    vector<int64_t> timestamps;
    vector<pair<int64_t, int64_t>> committedCpTs;
    int64_t checkPointId;
    buffer.updateBuffer(1, -1, 0, checkPointId, timestamps, committedCpTs);
    size_t expectLen = sizeof(ClientMemoryMessage) * 3 + 3;
    EXPECT_EQ(expectLen, buffer.getSize());
    EXPECT_EQ(size_t(0), buffer._writeQueue.size());
    EXPECT_EQ(size_t(0), buffer._toMergeQueue.size());
    EXPECT_EQ(size_t(3), buffer._sendQueue.size());
    EXPECT_EQ(size_t(3), buffer.getQueueSize());
    EXPECT_EQ(size_t(2), buffer.getUnsendCount());
    EXPECT_EQ(size_t(2), buffer.getUnsendSize());
    EXPECT_EQ(size_t(1), buffer.getUncommittedSize());
    EXPECT_EQ(size_t(1), buffer.getUncommittedCount());
    EXPECT_EQ(-1, checkPointId);
    EXPECT_EQ(0, committedCpTs.size());

    buffer.updateBuffer(1, 0, 1, checkPointId, timestamps, committedCpTs);
    EXPECT_EQ(1, checkPointId);
    EXPECT_EQ(sizeof(ClientMemoryMessage) * 2 + 2, buffer.getSize());
    EXPECT_EQ(size_t(0), buffer._writeQueue.size());
    EXPECT_EQ(size_t(0), buffer._toMergeQueue.size());
    EXPECT_EQ(size_t(2), buffer._sendQueue.size());
    EXPECT_EQ(size_t(2), buffer.getQueueSize());
    EXPECT_EQ(size_t(1), buffer.getUnsendCount());
    EXPECT_EQ(size_t(1), buffer.getUnsendSize());
    EXPECT_EQ(size_t(1), buffer.getUncommittedSize());
    EXPECT_EQ(size_t(1), buffer.getUncommittedCount());
    EXPECT_EQ(1, committedCpTs.size());
    EXPECT_EQ(1, committedCpTs[0].first);
    EXPECT_EQ(-1, committedCpTs[0].second);

    buffer.clear();
    EXPECT_EQ(size_t(0), buffer.getSize());
    EXPECT_EQ(size_t(0), buffer._writeQueue.size());
    EXPECT_EQ(size_t(0), buffer._toMergeQueue.size());
    EXPECT_EQ(size_t(0), buffer._sendQueue.size());
    EXPECT_EQ(size_t(0), buffer.getQueueSize());
    EXPECT_EQ(size_t(0), buffer.getUnsendCount());
    EXPECT_EQ(size_t(0), buffer.getUnsendSize());
    EXPECT_EQ(size_t(0), buffer.getUncommittedSize());
    EXPECT_EQ(size_t(0), buffer.getUncommittedCount());
}

TEST_F(MessageWriteBufferTest, testClear) {
    {
        MessageInfo msg1("a", 0, 0, 1);
        MessageInfo msg2("b", 1, 20000, 2);
        MessageInfo msg3("abc", 1, 20000, 2);
        MessageInfo msg4("ab", 1, 20000, 2);
        MessageInfo msg5("a", 4, 40000, 3);
        MessageInfo msg6("b", 1, 50000, 4);
        BlockPoolPtr blockPool(new BlockPool(102400, 1024));
        BufferSizeLimiterPtr limiter(new BufferSizeLimiter(800));
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter);
        buffer._mergeMsg = true;
        buffer._mergeCount = 2;
        buffer._mergeSize = 3;
        RangeUtilPtr rangeUtil(new RangeUtil(1, 2));
        buffer._rangeUtil = rangeUtil;
        EXPECT_EQ(size_t(0), buffer.getSize());
        EXPECT_EQ(0, buffer._limiter->getCurSize());
        EXPECT_TRUE(buffer.addWriteMessage(msg1));
        EXPECT_TRUE(buffer.addWriteMessage(msg5));
        EXPECT_TRUE(buffer.addWriteMessage(msg2));
        EXPECT_TRUE(buffer.addWriteMessage(msg3));
        EXPECT_TRUE(buffer.addWriteMessage(msg6));
        EXPECT_TRUE(buffer.addWriteMessage(msg4));
        size_t expectLen = sizeof(MessageInfo) * 6 + 9;
        EXPECT_EQ(expectLen, buffer._limiter->getCurSize());
        EXPECT_EQ(expectLen, buffer.getSize());
        EXPECT_EQ(size_t(6), buffer._writeQueue.size());
        EXPECT_EQ(size_t(0), buffer._sendQueue.size());
        buffer.clear();
        EXPECT_EQ(0, buffer._limiter->getCurSize());
        EXPECT_EQ(size_t(0), buffer.getSize());
        EXPECT_EQ(size_t(0), buffer._writeQueue.size());
        EXPECT_EQ(size_t(0), buffer._sendQueue.size());
    }
    {
        MessageInfo msg1("a", 0, 0, 1);
        MessageInfo msg2("b", 1, 20000, 2);
        MessageInfo msg3("abc", 1, 20000, 2);
        MessageInfo msg4("ab", 1, 20000, 2);
        MessageInfo msg5("a", 4, 40000, 3);
        MessageInfo msg6("b", 1, 50000, 4);
        BlockPoolPtr blockPool(new BlockPool(102400, 1024));
        BufferSizeLimiterPtr limiter(new BufferSizeLimiter(800));
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter);
        buffer._mergeMsg = true;
        buffer._mergeCount = 2;
        buffer._mergeSize = 3;
        RangeUtilPtr rangeUtil(new RangeUtil(1, 2));
        buffer._rangeUtil = rangeUtil;
        EXPECT_EQ(size_t(0), buffer.getSize());
        EXPECT_EQ(0, buffer._limiter->getCurSize());
        EXPECT_TRUE(buffer.addWriteMessage(msg1));
        EXPECT_TRUE(buffer.addWriteMessage(msg5));
        EXPECT_TRUE(buffer.addWriteMessage(msg2));
        EXPECT_TRUE(buffer.addWriteMessage(msg3));
        EXPECT_TRUE(buffer.addWriteMessage(msg6));
        EXPECT_TRUE(buffer.addWriteMessage(msg4));
        size_t expectLen = sizeof(MessageInfo) * 6 + 9;
        EXPECT_EQ(expectLen, buffer._limiter->getCurSize());
        EXPECT_EQ(expectLen, buffer.getSize());
        EXPECT_EQ(size_t(6), buffer._writeQueue.size());
        EXPECT_EQ(size_t(0), buffer._sendQueue.size());
        buffer.convertMessage();
        EXPECT_EQ(0, buffer._limiter->getCurSize());
        expectLen = 9 + sizeof(ClientMemoryMessage) * 4 + 2 * (sizeof(MergedMessageMeta) * 2 + sizeof(uint16_t));
        EXPECT_EQ(expectLen, buffer.getSize());
        EXPECT_EQ(size_t(0), buffer._writeQueue.size());
        EXPECT_EQ(size_t(4), buffer._sendQueue.size());
        buffer.clear();
        EXPECT_EQ(0, buffer._limiter->getCurSize());
        EXPECT_EQ(size_t(0), buffer.getSize());
        EXPECT_EQ(size_t(0), buffer._writeQueue.size());
        EXPECT_EQ(size_t(0), buffer._sendQueue.size());
    }
}

TEST_F(MessageWriteBufferTest, testSealedBufferWrite) {
    MessageInfo msg1("a", 0, 0, 1);
    MessageInfo msg2("b", 0, 0, 1);
    BlockPoolPtr blockPool(new BlockPool(102400, 1024));
    BufferSizeLimiterPtr limiter(new BufferSizeLimiter(800));
    MessageWriteBuffer buffer(blockPool, _threadPool, limiter);
    EXPECT_EQ(size_t(0), buffer.getSize());
    EXPECT_EQ(0, buffer._limiter->getCurSize());
    EXPECT_TRUE(buffer.addWriteMessage(msg1));
    EXPECT_EQ(sizeof(MessageInfo) + 1, buffer._limiter->getCurSize());
    buffer._sealed = true;
    EXPECT_FALSE(buffer.addWriteMessage(msg2));
    EXPECT_EQ(sizeof(MessageInfo) + 1, buffer._limiter->getCurSize());
}

TEST_F(MessageWriteBufferTest, testGetUnSendMsgs) {
    MessageInfo msg1("aaaaaaaaaa", 0, 0, 1);
    MessageInfo msg2("bbbbbbbbbb", 0, 0, 1);

    BlockPoolPtr blockPool(new BlockPool(102400, 1024));
    BufferSizeLimiterPtr limiter(new BufferSizeLimiter(800));
    int64_t mergeThreshold = 100;
    MessageWriteBuffer buffer(blockPool, _threadPool, limiter, mergeThreshold);
    EXPECT_EQ(size_t(0), buffer.getSize());
    EXPECT_EQ(0, buffer._limiter->getCurSize());

    std::vector<MessageInfo> msgInfoVec;
    msgInfoVec.clear();
    EXPECT_TRUE(buffer.addWriteMessage(msg1));
    EXPECT_EQ(1, buffer._writeQueue.size());
    EXPECT_EQ(10, buffer._writeQueueSize);
    EXPECT_TRUE(buffer.getUnSendMsgs(msgInfoVec));
    EXPECT_EQ(0, msgInfoVec.size());

    buffer._compressThresholdInBytes = 5;
    EXPECT_TRUE(buffer.addWriteMessage(msg2));
    buffer.convertMessage();
    EXPECT_EQ(0, buffer._writeQueue.size());
    EXPECT_EQ(0, buffer._writeQueueSize);
    EXPECT_EQ(2, buffer._sendQueue.size());
    EXPECT_TRUE(buffer.getUnSendMsgs(msgInfoVec));
    EXPECT_EQ(2, msgInfoVec.size());
    EXPECT_EQ("aaaaaaaaaa", msgInfoVec[0].data);
    EXPECT_FALSE(msgInfoVec[0].compress);
    EXPECT_FALSE(msgInfoVec[0].isMerged);
    EXPECT_EQ("bbbbbbbbbb", msgInfoVec[1].data);
    EXPECT_FALSE(msgInfoVec[1].compress);
    EXPECT_FALSE(msgInfoVec[1].isMerged);

    msgInfoVec.clear();
    MessageInfo msg3("cccccccccc", 0, 0, 1);
    MessageInfo msg4("dddddddddd", 0, 0, 1);
    buffer._mergeMsg = true;
    buffer._compressThresholdInBytes = 100;
    EXPECT_TRUE(buffer.addWriteMessage(msg3));
    buffer._compressThresholdInBytes = 5;
    EXPECT_TRUE(buffer.addWriteMessage(msg4));
    buffer.convertMessage();
    EXPECT_EQ(4, buffer._sendQueue.size());
    EXPECT_TRUE(buffer.getUnSendMsgs(msgInfoVec));
    EXPECT_EQ(4, msgInfoVec.size());
    EXPECT_EQ("aaaaaaaaaa", msgInfoVec[0].data);
    EXPECT_FALSE(msgInfoVec[0].compress);
    EXPECT_FALSE(msgInfoVec[0].isMerged);
    EXPECT_EQ("bbbbbbbbbb", msgInfoVec[1].data);
    EXPECT_FALSE(msgInfoVec[1].compress);
    EXPECT_FALSE(msgInfoVec[1].isMerged);
    EXPECT_EQ("cccccccccc", msgInfoVec[2].data);
    EXPECT_FALSE(msgInfoVec[2].compress);
    EXPECT_FALSE(msgInfoVec[2].isMerged);
    EXPECT_EQ("dddddddddd", msgInfoVec[3].data);
    EXPECT_FALSE(msgInfoVec[3].compress);
    EXPECT_FALSE(msgInfoVec[3].isMerged);

    // multi message merge and compress data
    msgInfoVec.clear();
    MessageInfo msg5("eeeeeeeeee", 0, 0, 1);
    MessageInfo msg6("ffffffffff", 0, 0, 1);
    buffer._compressThresholdInBytes = 20;
    RangeUtilPtr rangeUtil(new RangeUtil(1, 2));
    buffer._rangeUtil = rangeUtil;
    buffer._mergeMsg = true;
    buffer._mergeCount = 2;
    buffer._mergeSize = 20;
    EXPECT_TRUE(buffer.addWriteMessage(msg5));
    EXPECT_TRUE(buffer.addWriteMessage(msg6));
    EXPECT_EQ(2, buffer._writeQueue.size());
    buffer.convertMessage();
    EXPECT_EQ(5, buffer._sendQueue.size());
    EXPECT_TRUE(buffer._sendQueue[4].compress);
    EXPECT_TRUE(buffer.getUnSendMsgs(msgInfoVec));
    EXPECT_EQ(6, msgInfoVec.size());
    EXPECT_EQ("eeeeeeeeee", msgInfoVec[4].data);
    EXPECT_FALSE(msgInfoVec[4].compress);
    EXPECT_FALSE(msgInfoVec[4].isMerged);
    EXPECT_EQ("ffffffffff", msgInfoVec[5].data);
    EXPECT_FALSE(msgInfoVec[5].compress);
    EXPECT_FALSE(msgInfoVec[5].isMerged);

    // multi message merge but not compress data
    msgInfoVec.clear();
    MessageInfo msg7("gggggggggg", 0, 0, 1);
    MessageInfo msg8("hhhhhhhhhh", 0, 0, 1);
    buffer._compressThresholdInBytes = 100;
    rangeUtil.reset(new RangeUtil(1, 2));
    buffer._rangeUtil = rangeUtil;
    buffer._mergeMsg = true;
    buffer._mergeCount = 2;
    buffer._mergeSize = 20;
    EXPECT_TRUE(buffer.addWriteMessage(msg7));
    EXPECT_TRUE(buffer.addWriteMessage(msg8));
    EXPECT_EQ(2, buffer._writeQueue.size());
    buffer.convertMessage();
    EXPECT_EQ(6, buffer._sendQueue.size());
    EXPECT_FALSE(buffer._sendQueue[5].compress);
    EXPECT_TRUE(buffer.getUnSendMsgs(msgInfoVec));
    EXPECT_EQ(8, msgInfoVec.size());
    EXPECT_EQ("gggggggggg", msgInfoVec[6].data);
    EXPECT_FALSE(msgInfoVec[6].compress);
    EXPECT_FALSE(msgInfoVec[6].isMerged);
    EXPECT_EQ("hhhhhhhhhh", msgInfoVec[7].data);
    EXPECT_FALSE(msgInfoVec[7].compress);
    EXPECT_FALSE(msgInfoVec[7].isMerged);
}

TEST_F(MessageWriteBufferTest, testStealUnsendMessage) {
    BlockPoolPtr blockPool(new BlockPool(102400, 1024));
    BufferSizeLimiterPtr limiter(new BufferSizeLimiter(800));
    int64_t mergeThreshold = 100;

    std::vector<MessageInfo> msgInfoVec;
    // no data
    {
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter, mergeThreshold);
        EXPECT_EQ(0, buffer._writeQueue.size());
        EXPECT_EQ(0, buffer._toMergeQueue.size());
        EXPECT_EQ(0, buffer._sendQueue.size());
        EXPECT_TRUE(buffer.stealUnsendMessage(msgInfoVec));
        EXPECT_EQ(0, buffer._writeQueue.size());
        EXPECT_EQ(0, buffer._toMergeQueue.size());
        EXPECT_EQ(0, buffer._sendQueue.size());
        EXPECT_EQ(0, msgInfoVec.size());
    }
    // data in write queue
    {
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter, mergeThreshold);
        MessageInfo msg1("aaaaaaaaaa", 0, 0, 1);
        EXPECT_TRUE(buffer.addWriteMessage(msg1));
        EXPECT_EQ(1, buffer._writeQueue.size());
        EXPECT_EQ(10, buffer._writeQueueSize);
        EXPECT_EQ(0, buffer._toMergeQueue.size());
        EXPECT_EQ(0, buffer._sendQueue.size());
        EXPECT_TRUE(buffer.stealUnsendMessage(msgInfoVec));
        EXPECT_EQ(0, buffer._writeQueue.size());
        EXPECT_EQ(0, buffer._toMergeQueue.size());
        EXPECT_EQ(0, buffer._sendQueue.size());
        EXPECT_EQ(1, msgInfoVec.size());
        EXPECT_EQ("aaaaaaaaaa", msgInfoVec[0].data);
    }
    // data in send queue
    {
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter, mergeThreshold);
        MessageInfo msg2("bbbbbbbbbb", 0, 0, 1);
        EXPECT_TRUE(buffer.addWriteMessage(msg2));
        buffer.convertMessage();
        EXPECT_EQ(0, buffer._writeQueue.size());
        EXPECT_EQ(0, buffer._toMergeQueue.size());
        EXPECT_EQ(1, buffer._sendQueue.size());
        EXPECT_TRUE(buffer.stealUnsendMessage(msgInfoVec));
        EXPECT_EQ(2, msgInfoVec.size());
        EXPECT_EQ("aaaaaaaaaa", msgInfoVec[0].data);
        EXPECT_EQ("bbbbbbbbbb", msgInfoVec[1].data);
    }
    // data in write and send queue
    {
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter, mergeThreshold);
        MessageInfo msg3("cccccccccc", 0, 0, 1);
        MessageInfo msg4("dddddddddd", 0, 0, 1);
        EXPECT_TRUE(buffer.addWriteMessage(msg3));
        buffer.convertMessage();
        EXPECT_TRUE(buffer.addWriteMessage(msg4));
        EXPECT_EQ(1, buffer._writeQueue.size());
        EXPECT_EQ(0, buffer._toMergeQueue.size());
        EXPECT_EQ(1, buffer._sendQueue.size());
        EXPECT_TRUE(buffer.stealUnsendMessage(msgInfoVec));
        EXPECT_EQ(0, buffer._writeQueue.size());
        EXPECT_EQ(0, buffer._toMergeQueue.size());
        EXPECT_EQ(0, buffer._sendQueue.size());
        EXPECT_EQ(4, msgInfoVec.size());
        EXPECT_EQ("aaaaaaaaaa", msgInfoVec[0].data);
        EXPECT_EQ("bbbbbbbbbb", msgInfoVec[1].data);
        EXPECT_EQ("cccccccccc", msgInfoVec[2].data);
        EXPECT_EQ("dddddddddd", msgInfoVec[3].data);
    }
    // data in merge queue
    {
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter, mergeThreshold);
        MessageInfo msg5("eeeeeeeeee", 0, 0, 1);
        EXPECT_TRUE(buffer.addWriteMessage(msg5));
        buffer._toMergeQueue.swap(buffer._writeQueue);
        buffer._writeQueueSize = 0;
        EXPECT_EQ(0, buffer._writeQueue.size());
        EXPECT_EQ(1, buffer._toMergeQueue.size());
        EXPECT_EQ(0, buffer._sendQueue.size());
        EXPECT_TRUE(buffer.stealUnsendMessage(msgInfoVec));
        EXPECT_EQ(0, buffer._writeQueue.size());
        EXPECT_EQ(0, buffer._toMergeQueue.size());
        EXPECT_EQ(0, buffer._sendQueue.size());
        EXPECT_EQ(5, msgInfoVec.size());
        EXPECT_EQ("aaaaaaaaaa", msgInfoVec[0].data);
        EXPECT_EQ("bbbbbbbbbb", msgInfoVec[1].data);
        EXPECT_EQ("cccccccccc", msgInfoVec[2].data);
        EXPECT_EQ("dddddddddd", msgInfoVec[3].data);
        EXPECT_EQ("eeeeeeeeee", msgInfoVec[4].data);
    }
    // data in write and merge queue
    {
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter, mergeThreshold);
        MessageInfo msg6("ffffffffff", 0, 0, 1);
        MessageInfo msg7("gggggggggg", 0, 0, 1);
        EXPECT_TRUE(buffer.addWriteMessage(msg6));
        buffer._toMergeQueue.swap(buffer._writeQueue);
        buffer._writeQueueSize = 0;
        EXPECT_TRUE(buffer.addWriteMessage(msg7));
        EXPECT_EQ(1, buffer._writeQueue.size());
        EXPECT_EQ(1, buffer._toMergeQueue.size());
        EXPECT_EQ(0, buffer._sendQueue.size());
        EXPECT_TRUE(buffer.stealUnsendMessage(msgInfoVec));
        EXPECT_EQ(0, buffer._writeQueue.size());
        EXPECT_EQ(0, buffer._toMergeQueue.size());
        EXPECT_EQ(0, buffer._sendQueue.size());
        EXPECT_EQ(7, msgInfoVec.size());
        EXPECT_EQ("aaaaaaaaaa", msgInfoVec[0].data);
        EXPECT_EQ("bbbbbbbbbb", msgInfoVec[1].data);
        EXPECT_EQ("cccccccccc", msgInfoVec[2].data);
        EXPECT_EQ("dddddddddd", msgInfoVec[3].data);
        EXPECT_EQ("eeeeeeeeee", msgInfoVec[4].data);
        EXPECT_EQ("ffffffffff", msgInfoVec[5].data);
        EXPECT_EQ("gggggggggg", msgInfoVec[6].data);
    }
    // fail case, 1 block
    {
        BlockPoolPtr blockPool(new BlockPool(1024, 1024)); // only one block
        MessageWriteBuffer buffer(blockPool, _threadPool, limiter, mergeThreshold);
        MessageInfo msg8("hhhhhhhhhh", 0, 0, 1);
        EXPECT_TRUE(buffer.addWriteMessage(msg8));
        EXPECT_EQ(1, buffer._writeQueue.size());
        EXPECT_EQ(0, buffer._toMergeQueue.size());
        EXPECT_EQ(0, buffer._sendQueue.size());
        EXPECT_FALSE(buffer.stealUnsendMessage(msgInfoVec));
        EXPECT_EQ(0, buffer._writeQueue.size());
        EXPECT_EQ(1, buffer._toMergeQueue.size());
        EXPECT_EQ(0, buffer._sendQueue.size());
        EXPECT_EQ(7, msgInfoVec.size());
        EXPECT_EQ("aaaaaaaaaa", msgInfoVec[0].data);
        EXPECT_EQ("bbbbbbbbbb", msgInfoVec[1].data);
        EXPECT_EQ("cccccccccc", msgInfoVec[2].data);
        EXPECT_EQ("dddddddddd", msgInfoVec[3].data);
        EXPECT_EQ("eeeeeeeeee", msgInfoVec[4].data);
        EXPECT_EQ("ffffffffff", msgInfoVec[5].data);
        EXPECT_EQ("gggggggggg", msgInfoVec[6].data);
    }
}

TEST_F(MessageWriteBufferTest, testUpdateBufferCpTs) {
    BlockPoolPtr blockPool(new BlockPool(sizeof(ClientMemoryMessage) * 50, sizeof(ClientMemoryMessage)));
    MessageWriteBuffer buffer(blockPool, _threadPool);
    EXPECT_EQ(0, buffer.getSize());
    for (int i = 0; i < 40; i++) {
        MessageInfo msg("a", 0, 0, i); // data, mask, payload, checkpoint
        // checkpoint-timestamp: msgi i-i*10
        EXPECT_TRUE(buffer.addWriteMessage(msg));
    }
    buffer.convertMessage();
    EXPECT_EQ(0, buffer._writeQueue.size());
    EXPECT_EQ(0, buffer._toMergeQueue.size());
    EXPECT_EQ(40, buffer._sendQueue.size());

    vector<int64_t> timestamps;
    vector<pair<int64_t, int64_t>> committedCpTs;
    int64_t checkPointId;
    // 1. accept 10 msgs, no commmit
    for (int i = 0; i < 10; ++i) {
        timestamps.emplace_back(i * 10);
    }
    buffer.updateBuffer(10, -1, 0, checkPointId, timestamps, committedCpTs);
    EXPECT_EQ(40, buffer._sendQueue.size());
    EXPECT_EQ(40, buffer.getQueueSize());
    EXPECT_EQ(-1, checkPointId);
    EXPECT_EQ(0, committedCpTs.size());
    // 2. accept 10 msgs, commmit 5
    timestamps.clear();
    for (int i = 10; i < 20; ++i) {
        timestamps.emplace_back(i * 10);
    }
    buffer.updateBuffer(10, 5, 10, checkPointId, timestamps, committedCpTs);
    EXPECT_EQ(5, checkPointId);
    EXPECT_EQ(34, buffer._sendQueue.size());
    EXPECT_EQ(6, committedCpTs.size());
    for (int i = 0; i < 6; ++i) {
        EXPECT_EQ(i, committedCpTs[i].first);
        EXPECT_EQ(i * 10, committedCpTs[i].second);
    }
    // 3. accept 5 msgs, commmit 19
    timestamps.clear(); // no timestamps after msgid 20
    committedCpTs.clear();
    buffer.updateBuffer(5, 19, 20, checkPointId, timestamps, committedCpTs);
    EXPECT_EQ(19, checkPointId);
    EXPECT_EQ(20, buffer._sendQueue.size());
    EXPECT_EQ(14, committedCpTs.size());
    for (int i = 0; i < 14; ++i) {
        EXPECT_EQ(i + 6, committedCpTs[i].first);
        EXPECT_EQ((i + 6) * 10, committedCpTs[i].second);
    }
    // 4. accept 5 msgs, commmit 19 not change
    timestamps.clear();
    committedCpTs.clear();
    buffer.updateBuffer(5, 19, 25, checkPointId, timestamps, committedCpTs);
    EXPECT_EQ(-1, checkPointId);
    EXPECT_EQ(20, buffer._sendQueue.size());
    EXPECT_EQ(0, committedCpTs.size());

    // 5. accept 10 msgs, commmit 25, no timestamp
    timestamps.clear();
    committedCpTs.clear();
    buffer.updateBuffer(10, 25, 30, checkPointId, timestamps, committedCpTs);
    EXPECT_EQ(25, checkPointId);
    EXPECT_EQ(14, buffer._sendQueue.size());
    EXPECT_EQ(6, committedCpTs.size());
    for (int i = 0; i < 6; ++i) {
        EXPECT_EQ(i + 20, committedCpTs[i].first);
        EXPECT_EQ(-1, committedCpTs[i].second);
    }
}

} // namespace client
} // namespace swift
