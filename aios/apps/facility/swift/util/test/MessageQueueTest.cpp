#include "swift/util/MessageQueue.h"

#include <cstddef>
#include <stdint.h>
#include <vector>

#include "swift/common/Common.h"
#include "swift/common/MemoryMessage.h"
#include "swift/util/BlockPool.h"
#include "swift/util/ObjectBlock.h"
#include "unittest/unittest.h"

namespace swift {
namespace util {
using namespace std;
using namespace swift::common;

class MessageQueueTest : public TESTBASE {};

TEST_F(MessageQueueTest, testOnlyOneObjInBlock) {
    size_t blockSize = sizeof(MemoryMessage);
    BlockPool *pool = new BlockPool(blockSize * 2, blockSize);
    MessageQueue<MemoryMessage> *queue = new MessageQueue<MemoryMessage>(pool);
    EXPECT_TRUE(queue->reserve(2));
    EXPECT_EQ(int64_t(2), pool->getUsedBlockCount());
    MemoryMessage msg1, msg2;
    msg1.setMsgId(1);
    msg2.setMsgId(2);
    queue->push_back(msg1);
    queue->push_back(msg2);
    EXPECT_EQ(int64_t(2), queue->size());
    EXPECT_EQ(int64_t(1), queue->front().getMsgId());
    EXPECT_EQ(int64_t(1), (*queue)[0].getMsgId());
    EXPECT_EQ(int64_t(2), (*queue)[1].getMsgId());
    queue->pop_front();
    EXPECT_EQ(int64_t(1), pool->getUsedBlockCount());
    EXPECT_TRUE(!queue->reserve(2));
    EXPECT_TRUE(queue->reserve(1));
    EXPECT_EQ(int64_t(1), queue->size());
    EXPECT_EQ(int64_t(2), queue->front().getMsgId());
    EXPECT_EQ(int64_t(2), (*queue)[0].getMsgId());
    queue->push_back(msg1);
    EXPECT_EQ(int64_t(2), queue->size());
    EXPECT_EQ(int64_t(2), queue->front().getMsgId());
    EXPECT_EQ(int64_t(2), (*queue)[0].getMsgId());
    EXPECT_EQ(int64_t(1), (*queue)[1].getMsgId());
    queue->pop_front();
    queue->pop_front();
    EXPECT_TRUE(queue->empty());
    EXPECT_TRUE(queue->reserve(2));
    EXPECT_EQ(int64_t(2), pool->getUsedBlockCount());
    delete queue;
    delete pool;
}
TEST_F(MessageQueueTest, testMultObjInBlock) {
    size_t totalMsgCount = 256 * 1024;
    size_t msgCountInBlock = 512;
    size_t msgSize = sizeof(MemoryMessage);
    BlockPool *pool = new BlockPool(msgSize * totalMsgCount, msgSize * msgCountInBlock);
    BlockPool *dataPool = new BlockPool(totalMsgCount, 1);
    MessageQueue<MemoryMessage> *queue = new MessageQueue<MemoryMessage>(pool);
    int64_t popCount = 0;
    int64_t count = 0;
    while (popCount % msgCountInBlock + queue->size() < totalMsgCount) {
        EXPECT_TRUE(queue->reserve(1));
        MemoryMessage msg;
        msg.setMsgId(++count);
        msg.setBlock(dataPool->allocate());
        queue->push_back(msg);
        if (count % 2 == 0) {
            popCount++;
            EXPECT_EQ(int64_t(popCount), queue->front().getMsgId());
            EXPECT_EQ(int64_t(popCount), (*queue)[0].getMsgId());
            queue->pop_front();
        }
        EXPECT_TRUE(!queue->empty());
        EXPECT_EQ(count - popCount, queue->size());
        EXPECT_EQ(int64_t(count), (*queue)[queue->size() - 1].getMsgId());
        EXPECT_EQ(int64_t(count), queue->back().getMsgId());
        int64_t useBlock = (popCount % msgCountInBlock + queue->size() + msgCountInBlock - 1) / msgCountInBlock;
        EXPECT_EQ(useBlock, pool->getUsedBlockCount());
        EXPECT_EQ(queue->size(), dataPool->getUsedBlockCount());
    }
    MessageQueueIterator<MemoryMessage> iter = queue->begin();
    while (iter != queue->end()) {
        EXPECT_EQ(int64_t(++popCount), iter->getMsgId());
        iter++;
    }
    queue->clear();
    EXPECT_EQ(int64_t(0), queue->size());
    EXPECT_EQ(int64_t(0), pool->getUsedBlockCount());
    EXPECT_EQ(int64_t(0), dataPool->getUsedBlockCount());
    delete queue;
    delete pool;
    delete dataPool;
}

TEST_F(MessageQueueTest, testStealBlock) {
    size_t totalMsgCount = 256;
    size_t msgCountInBlock = 1;
    size_t msgSize = sizeof(MemoryMessage);
    BlockPool *pool = new BlockPool(msgSize * totalMsgCount, msgSize * msgCountInBlock);
    BlockPool *dataPool = new BlockPool(totalMsgCount, 1);
    MessageQueue<MemoryMessage> *queue = new MessageQueue<MemoryMessage>(pool);
    size_t count = 0;
    while (count < totalMsgCount) {
        EXPECT_TRUE(queue->reserve(1));
        MemoryMessage msg;
        msg.setMsgId(count++);
        msg.setBlock(dataPool->allocate());
        queue->push_back(msg);
    }
    EXPECT_EQ(totalMsgCount, queue->size());
    EXPECT_EQ(int64_t(256), pool->getUsedBlockCount());
    EXPECT_EQ(int64_t(256), dataPool->getUsedBlockCount());

    vector<ObjectBlock<MemoryMessage> *> objectVec;
    int64_t startOffset, msgCount;
    queue->stealBlock(0, startOffset, msgCount, objectVec);
    ASSERT_EQ(0, startOffset);
    ASSERT_EQ(0, msgCount);
    ASSERT_EQ(0, objectVec.size());

    queue->stealBlock(100, startOffset, msgCount, objectVec);
    ASSERT_EQ(0, startOffset);
    ASSERT_EQ(100, msgCount);
    ASSERT_EQ(100, objectVec.size());
    EXPECT_EQ(int64_t(256), pool->getUsedBlockCount());
    EXPECT_EQ(int64_t(256), dataPool->getUsedBlockCount());
    ASSERT_EQ(156, queue->size());
    for (size_t i = 0; i < objectVec.size(); i++) {
        (*objectVec[i]).destoryBefore(0);
        delete objectVec[i];
    }
    EXPECT_EQ(int64_t(156), pool->getUsedBlockCount());
    EXPECT_EQ(int64_t(156), dataPool->getUsedBlockCount());

    vector<ObjectBlock<MemoryMessage> *> objectVec2;
    queue->stealBlock(156, startOffset, msgCount, objectVec2);
    ASSERT_EQ(0, startOffset);
    ASSERT_EQ(156, msgCount);
    ASSERT_EQ(156, objectVec2.size());
    EXPECT_EQ(int64_t(156), pool->getUsedBlockCount());
    EXPECT_EQ(int64_t(156), dataPool->getUsedBlockCount());
    ASSERT_EQ(0, queue->size());
    for (size_t i = 0; i < objectVec2.size(); i++) {
        (*objectVec2[i]).destoryBefore(0);
        delete objectVec2[i];
    }
    EXPECT_EQ(int64_t(0), pool->getUsedBlockCount());
    EXPECT_EQ(int64_t(0), dataPool->getUsedBlockCount());

    delete queue;
    delete pool;
    delete dataPool;
}

TEST_F(MessageQueueTest, testStealBlock2) {
    size_t totalMsgCount = 256;
    size_t msgCountInBlock = 3;
    size_t msgSize = sizeof(MemoryMessage);
    BlockPool *pool = new BlockPool(msgSize * totalMsgCount, msgSize * msgCountInBlock);
    BlockPool *dataPool = new BlockPool(totalMsgCount, 1);
    MessageQueue<MemoryMessage> *queue = new MessageQueue<MemoryMessage>(pool);
    size_t count = 0;
    while (count < totalMsgCount) {
        EXPECT_TRUE(queue->reserve(1));
        MemoryMessage msg;
        msg.setMsgId(count++);
        msg.setBlock(dataPool->allocate());
        queue->push_back(msg);
    }
    EXPECT_EQ(totalMsgCount, queue->size());
    EXPECT_EQ(int64_t(86), pool->getUsedBlockCount());
    EXPECT_EQ(int64_t(256), dataPool->getUsedBlockCount());

    vector<ObjectBlock<MemoryMessage> *> objectVec;
    int64_t startOffset, msgCount;
    queue->stealBlock(0, startOffset, msgCount, objectVec);
    ASSERT_EQ(0, startOffset);
    ASSERT_EQ(0, msgCount);
    ASSERT_EQ(0, objectVec.size());

    queue->pop_front();
    EXPECT_EQ(totalMsgCount - 1, queue->size());
    queue->stealBlock(6, startOffset, msgCount, objectVec);
    ASSERT_EQ(1, startOffset);
    ASSERT_EQ(5, msgCount);
    ASSERT_EQ(2, objectVec.size());
    EXPECT_EQ(int64_t(86), pool->getUsedBlockCount());
    EXPECT_EQ(int64_t(255), dataPool->getUsedBlockCount());
    ASSERT_EQ(250, queue->size());
    for (size_t i = 0; i < objectVec.size(); i++) {
        (*objectVec[i]).destoryBefore(0);
        delete objectVec[i];
    }
    EXPECT_EQ(int64_t(84), pool->getUsedBlockCount());
    EXPECT_EQ(int64_t(250), dataPool->getUsedBlockCount());
    EXPECT_EQ(6, queue->front().getMsgId());

    vector<ObjectBlock<MemoryMessage> *> objectVec2;
    queue->stealBlock(7, startOffset, msgCount, objectVec2);
    ASSERT_EQ(0, startOffset);
    ASSERT_EQ(6, msgCount);
    ASSERT_EQ(2, objectVec2.size());
    EXPECT_EQ(int64_t(84), pool->getUsedBlockCount());
    EXPECT_EQ(int64_t(250), dataPool->getUsedBlockCount());
    ASSERT_EQ(244, queue->size());
    for (size_t i = 0; i < objectVec2.size(); i++) {
        (*objectVec2[i]).destoryBefore(0);
        delete objectVec2[i];
    }
    EXPECT_EQ(int64_t(82), pool->getUsedBlockCount());
    EXPECT_EQ(int64_t(244), dataPool->getUsedBlockCount());

    vector<ObjectBlock<MemoryMessage> *> objectVec3;
    queue->stealBlock(1000, startOffset, msgCount, objectVec3);
    ASSERT_EQ(0, startOffset);
    ASSERT_EQ(244, msgCount);
    ASSERT_EQ(82, objectVec3.size());
    EXPECT_EQ(int64_t(82), pool->getUsedBlockCount());
    EXPECT_EQ(int64_t(244), dataPool->getUsedBlockCount());
    ASSERT_EQ(0, queue->size());
    for (size_t i = 0; i < objectVec3.size(); i++) {
        (*objectVec3[i]).destoryBefore(0);
        delete objectVec3[i];
    }
    EXPECT_EQ(int64_t(0), pool->getUsedBlockCount());
    EXPECT_EQ(int64_t(0), dataPool->getUsedBlockCount());

    delete queue;
    delete pool;
    delete dataPool;
}

TEST_F(MessageQueueTest, testReserve) {
    size_t blockSize = sizeof(MemoryMessage);
    {
        BlockPool *pool = new BlockPool(blockSize * 2, blockSize);
        MessageQueue<MemoryMessage> *queue = new MessageQueue<MemoryMessage>(pool);
        EXPECT_TRUE(queue->reserve(0));
        EXPECT_TRUE(queue->reserve(1));
        EXPECT_TRUE(queue->reserve(2));
        EXPECT_TRUE(!queue->reserve(3));
        queue->clear();
        EXPECT_TRUE(queue->reserve(2));
        delete queue;
        delete pool;
    }
    {
        BlockPool *pool = new BlockPool(blockSize * 8, blockSize * 4);
        MessageQueue<MemoryMessage> *queue = new MessageQueue<MemoryMessage>(pool);
        EXPECT_TRUE(queue->reserve(0));
        EXPECT_TRUE(queue->reserve(8));
        EXPECT_TRUE(queue->reserve(1));
        EXPECT_TRUE(!queue->reserve(9));
        queue->clear();
        EXPECT_TRUE(queue->reserve(8));
        delete queue;
        delete pool;
    }
}

} // namespace util
} // namespace swift
