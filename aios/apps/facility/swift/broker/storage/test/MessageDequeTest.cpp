#include "swift/broker/storage/MessageDeque.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <vector>

#include "swift/broker/storage/TimestampAllocator.h"
#include "swift/common/Common.h"
#include "swift/common/MemoryMessage.h"
#include "swift/util/BlockPool.h"
#include "swift/util/MessageQueue.h"
#include "unittest/unittest.h"

using namespace std;
using namespace swift::common;
namespace swift {
namespace storage {

class MessageDequeTest : public TESTBASE {
    void setUp() {
        _pool = new util::BlockPool(102400, 128);
        _timestampAllocator = new TimestampAllocator();
    }
    void tearDown() {
        delete _pool;
        delete _timestampAllocator;
    }

private:
    util::BlockPool *_pool = nullptr;
    TimestampAllocator *_timestampAllocator = nullptr;
};

TEST_F(MessageDequeTest, testNotIgnoreNotCommittedMsg) {
    MessageDeque messageDeque(_pool, _timestampAllocator);
    EXPECT_EQ(0, messageDeque.size());
    int64_t i = 0;
    EXPECT_TRUE(messageDeque.reserve(10));
    for (; i < 10; i++) {
        MemoryMessage memoryMessage;
        memoryMessage.setMsgId(i);
        messageDeque.pushBack(memoryMessage);
    }
    messageDeque.setCommittedMsgId(5);

    auto view = messageDeque.createView(true);
    EXPECT_TRUE(!view.empty());
    EXPECT_EQ((int64_t)10, view.size());

    vector<MemoryMessage> msgVec;
    MessageDeque::MessageDequeIter beginPos = view.begin() + 1;
    MessageDeque::MessageDequeIter endPos = view.begin() + 3;
    msgVec.assign(beginPos, endPos);
    EXPECT_EQ((size_t)2, msgVec.size());
    EXPECT_EQ((int64_t)1, msgVec[0].getMsgId());
    EXPECT_EQ((int64_t)2, msgVec[1].getMsgId());

    i = 0;
    for (MessageDeque::MessageDequeIter it = view.begin(); it != view.end(); ++it) {
        EXPECT_EQ(i, (*it).getMsgId());
        i++;
    }
    EXPECT_EQ((int64_t)10, i);

    for (i = 0; i < (int64_t)view.size(); ++i) {
        EXPECT_EQ(i, view[i].getMsgId());
    }
    EXPECT_EQ((int64_t)10, i);

    for (i = 0; i < 10; i++) {
        EXPECT_EQ((int64_t)i, messageDeque.front().getMsgId());
        messageDeque.popFront();
    }
    EXPECT_EQ((int64_t)0, messageDeque.size());
}

TEST_F(MessageDequeTest, testIgnoreNotCommittedMsg) {
    MessageDeque messageDeque(_pool, _timestampAllocator);
    auto view0 = messageDeque.createView(true);
    ASSERT_TRUE(view0.empty());
    EXPECT_EQ(-1, view0.getLastMsgId());
    EXPECT_TRUE(view0.getLastMsgTimestamp() > 100000);
    EXPECT_EQ(0, view0.getNextMsgIdAfterLast());
    EXPECT_TRUE(view0.getNextMsgTimestampAfterLast() > 100000);

    EXPECT_EQ((int64_t)0, messageDeque.size());
    EXPECT_TRUE(messageDeque.reserve(10));
    int64_t i = 0;
    for (; i < 10; i++) {
        MemoryMessage memoryMessage;
        memoryMessage.setMsgId(i);
        memoryMessage.setTimestamp(i + 1);
        messageDeque.pushBack(memoryMessage);
    }
    EXPECT_EQ((int64_t)10, messageDeque.size());
    auto view1 = messageDeque.createView(false);
    EXPECT_TRUE(view1.empty());
    EXPECT_EQ((int64_t)0, view1.size());
    EXPECT_EQ(-1, view1.getLastMsgId());
    EXPECT_EQ(0, view1.getLastMsgTimestamp());
    EXPECT_EQ(0, view1.getNextMsgIdAfterLast());
    EXPECT_EQ(1, view1.getNextMsgTimestampAfterLast());

    messageDeque.setCommittedMsgId(5);
    auto view2 = messageDeque.createView(false);
    EXPECT_FALSE(view2.empty());
    EXPECT_EQ((int64_t)6, view2.size());
    EXPECT_EQ(5, view2.getLastMsgId());
    EXPECT_EQ(6, view2.getLastMsgTimestamp());
    EXPECT_EQ(6, view2.getNextMsgIdAfterLast());
    EXPECT_EQ(7, view2.getNextMsgTimestampAfterLast());

    vector<MemoryMessage> msgVec;
    MessageDeque::MessageDequeIter beginPos = view2.begin() + 1;
    MessageDeque::MessageDequeIter endPos = view2.begin() + 3;
    msgVec.assign(beginPos, endPos);
    EXPECT_EQ((size_t)2, msgVec.size());
    EXPECT_EQ((int64_t)1, msgVec[0].getMsgId());
    EXPECT_EQ((int64_t)2, msgVec[1].getMsgId());
    msgVec.clear();
    msgVec.assign(beginPos, view2.begin() + view2.size());
    EXPECT_EQ((size_t)5, msgVec.size());
    for (i = 1; i < 6; ++i) {
        EXPECT_EQ(i, msgVec[i - 1].getMsgId());
    }

    i = 0;
    for (MessageDeque::MessageDequeIter it = view2.begin(); it != view2.end(); ++it) {
        EXPECT_EQ(i, (*it).getMsgId());
        i++;
    }
    EXPECT_EQ((int64_t)6, i);

    for (i = 0; i < (int64_t)view2.size(); ++i) {
        EXPECT_EQ(i, view2[i].getMsgId());
    }
    EXPECT_EQ((int64_t)6, i);
    EXPECT_TRUE(view2.end() == view2.begin() + 6);

    for (i = 0; i < 6; i++) {
        EXPECT_EQ((int64_t)i, messageDeque.front().getMsgId());
        messageDeque.popFront();
    }
    auto view3 = messageDeque.createView(false);
    EXPECT_EQ(0, view3.size());
    EXPECT_TRUE(view3.empty());
    EXPECT_EQ(4, messageDeque.size());
}

TEST_F(MessageDequeTest, testIgnoreNotCommittedMsgOnBoundaryCondition) {
    MessageDeque messageDeque(_pool, _timestampAllocator);
    EXPECT_TRUE(messageDeque.reserve(10));
    int64_t i = 0;
    for (; i < 10; i++) {
        MemoryMessage memoryMessage;
        memoryMessage.setMsgId(i);
        messageDeque.pushBack(memoryMessage);
    }

    messageDeque.setCommittedMsgId((int64_t)-1);
    auto view = messageDeque.createView(false);
    EXPECT_EQ(0, view.size());
    EXPECT_TRUE(view.empty());

    messageDeque.setCommittedMsgId((int64_t)0);
    ASSERT_EQ(1, messageDeque.getCommittedCount());
    auto view1 = messageDeque.createView(false);
    EXPECT_EQ((int64_t)1, view1.size());
    EXPECT_FALSE(view1.empty());

    messageDeque.setCommittedMsgId((int64_t)9);
    auto view2 = messageDeque.createView(true);
    EXPECT_EQ((int64_t)10, view2.size());
    EXPECT_FALSE(view2.empty());
    EXPECT_EQ(10, messageDeque.getCommittedCount());

    messageDeque.setCommittedMsgId((int64_t)10);
    EXPECT_EQ((int64_t)10, messageDeque.getCommittedCount());
    auto view3 = messageDeque.createView(true);
    EXPECT_FALSE(view3.empty());
}

TEST_F(MessageDequeTest, testGetCommittedTimestamp) {
    MessageDeque messageDeque(_pool, _timestampAllocator);
    EXPECT_EQ(0, messageDeque.getCommittedTimestamp());
    EXPECT_TRUE(messageDeque.reserve(10));
    int64_t i = 0;
    for (; i < 10; i++) {
        MemoryMessage memoryMessage;
        memoryMessage.setMsgId(i);
        memoryMessage.setTimestamp(i + 1);
        messageDeque.pushBack(memoryMessage);
    }
    EXPECT_EQ(0, messageDeque.getCommittedTimestamp());
    messageDeque.setCommittedMsgId((int64_t)-1);
    EXPECT_EQ(0, messageDeque.getCommittedTimestamp());
    messageDeque.setCommittedMsgId((int64_t)0);
    EXPECT_EQ(1, messageDeque.getCommittedTimestamp());
    ASSERT_EQ(1, messageDeque.getCommittedCount());

    messageDeque.setCommittedMsgId((int64_t)9);
    EXPECT_EQ(10, messageDeque.getCommittedTimestamp());
    EXPECT_EQ(10, messageDeque.getCommittedCount());

    messageDeque.setCommittedMsgId((int64_t)10);
    EXPECT_EQ(10, messageDeque.getCommittedTimestamp());
    EXPECT_EQ((int64_t)10, messageDeque.getCommittedCount());
}

struct MessageIdCom {
    bool operator()(const MemoryMessage &first, int64_t second) const { return first.getMsgId() < second; }
};

TEST_F(MessageDequeTest, testUseSceneIgnoreNotCommittedMsg) {
    MessageDeque messageDeque(_pool, _timestampAllocator);
    EXPECT_TRUE(messageDeque.reserve(10));
    int64_t i = 0;
    for (; i < 10; i++) {
        MemoryMessage memoryMessage;
        memoryMessage.setMsgId(i);
        messageDeque.pushBack(memoryMessage);
    }
    messageDeque.setCommittedMsgId(5);

    auto view = messageDeque.createView(false);
    auto it = std::lower_bound(view.begin(), view.end(), -1, MessageIdCom());
    EXPECT_TRUE(it == view.begin());

    it = std::lower_bound(view.begin(), view.end(), 0, MessageIdCom());
    EXPECT_TRUE(it == view.begin());

    it = std::lower_bound(view.begin(), view.end(), 3, MessageIdCom());
    EXPECT_TRUE(it == (view.begin() + 3));

    it = std::lower_bound(view.begin(), view.end(), 5, MessageIdCom());
    EXPECT_TRUE(it == (view.begin() + 5));
    EXPECT_TRUE(it != view.end());

    it = std::lower_bound(view.begin(), view.end(), 6, MessageIdCom());
    EXPECT_TRUE(it == view.begin() + 6);
    EXPECT_TRUE(it == view.end());

    it = std::lower_bound(view.begin(), view.end(), 8, MessageIdCom());
    EXPECT_TRUE(it == view.begin() + 6);
    EXPECT_TRUE(it == view.end());
};

TEST_F(MessageDequeTest, testUseSceneNotIgnoreNotCommittedMsg) {
    MessageDeque messageDeque(_pool, _timestampAllocator);
    EXPECT_TRUE(messageDeque.reserve(10));
    int64_t i = 0;
    for (; i < 10; i++) {
        MemoryMessage memoryMessage;
        memoryMessage.setMsgId(i);
        messageDeque.pushBack(memoryMessage);
    }
    messageDeque.setCommittedMsgId(5);

    auto view = messageDeque.createView(true);
    auto it = std::lower_bound(view.begin(), view.end(), -1, MessageIdCom());
    EXPECT_TRUE(it == view.begin());

    it = std::lower_bound(view.begin(), view.end(), 0, MessageIdCom());
    EXPECT_TRUE(it == view.begin());

    it = std::lower_bound(view.begin(), view.end(), 5, MessageIdCom());
    EXPECT_TRUE(it == (view.begin() + 5));

    it = std::lower_bound(view.begin(), view.end(), 7, MessageIdCom());
    EXPECT_TRUE(it == (view.begin() + 7));
    EXPECT_TRUE(it != view.end());

    it = std::lower_bound(view.begin(), view.end(), 9, MessageIdCom());
    EXPECT_TRUE(it == (view.begin() + 9));
    EXPECT_TRUE(it != view.end());

    it = std::lower_bound(view.begin(), view.end(), 10, MessageIdCom());
    EXPECT_TRUE(it == (view.begin() + 10));
    EXPECT_TRUE(it == view.end());

    it = std::lower_bound(view.begin(), view.end(), 15, MessageIdCom());
    EXPECT_TRUE(it == (view.begin() + 10));
    EXPECT_TRUE(it == view.end());
};

TEST_F(MessageDequeTest, testGetMemoryMessage) {
    MessageDeque messageDeque(_pool, _timestampAllocator);
    EXPECT_TRUE(messageDeque.reserve(1000));
    for (int64_t i = 100000; i < 101000; i++) {
        MemoryMessage memoryMessage;
        memoryMessage.setMsgId(i);
        messageDeque.pushBack(memoryMessage);
    }
    MemoryMessageVector vec;
    messageDeque.getMemoryMessage(100050, 100, vec);
    vec.clear();
    messageDeque.getMemoryMessage(100, 100, vec);
    vec.clear();
    messageDeque.getMemoryMessage(110000, 100, vec);
}

TEST_F(MessageDequeTest, testGetLeftToBeCommittedDataSizeWithNotMemOnlyMode) {
    MessageDeque messageDeque(_pool, _timestampAllocator);
    EXPECT_EQ(0, messageDeque.getLastAcceptedMsgTimeStamp());
    EXPECT_TRUE(messageDeque.reserve(10));
    for (int64_t i = 0; i < 10; i++) {
        MemoryMessage memoryMessage;
        memoryMessage.setMsgId(i);
        memoryMessage.setLen(10);
        memoryMessage.setTimestamp(i);
        messageDeque.pushBack(memoryMessage);
    }

    EXPECT_EQ((int64_t)100, messageDeque.getLeftToBeCommittedDataSize());
    messageDeque.setCommittedMsgId(4);
    EXPECT_EQ((int64_t)50, messageDeque.getLeftToBeCommittedDataSize());
    messageDeque.setCommittedMsgId(10);
    EXPECT_EQ((int64_t)0, messageDeque.getLeftToBeCommittedDataSize());

    EXPECT_EQ((int64_t)9, messageDeque.getLastAcceptedMsgTimeStamp());
}

TEST_F(MessageDequeTest, testRecycle) {
    MessageDeque messageDeque(_pool, _timestampAllocator);
    EXPECT_EQ(0, messageDeque.size());
    EXPECT_TRUE(messageDeque.reserve(10));
    for (int i = 0; i < 10; i++) {
        MemoryMessage memoryMessage;
        memoryMessage.setMsgId(i);
        memoryMessage.setTimestamp(i * 100);
        messageDeque.pushBack(memoryMessage);
    }
    EXPECT_EQ(0, messageDeque._committedIndex);
    EXPECT_EQ(-1, messageDeque._lastCommittedId);
    EXPECT_EQ(10, messageDeque._msgQueue.size());
    int64_t recycleSize = 0;
    int64_t recycleCount = 0;
    int64_t timestamp = 900;
    auto cmp = [timestamp](const common::MemoryMessage &msg) { return msg.getTimestamp() < timestamp; };
    messageDeque.recycle(recycleSize, recycleCount, cmp);
    EXPECT_EQ(0, recycleCount);
    EXPECT_EQ(10, messageDeque._msgQueue.size());
    EXPECT_EQ(0, messageDeque._committedIndex);

    messageDeque.setCommittedMsgId(5);
    EXPECT_EQ(6, messageDeque._committedIndex);
    EXPECT_EQ(5, messageDeque._lastCommittedId);
    messageDeque.recycle(recycleSize, recycleCount, cmp);
    EXPECT_EQ(5, recycleCount);
    EXPECT_EQ(5, messageDeque._msgQueue.size());
    EXPECT_EQ(1, messageDeque._committedIndex);
    EXPECT_EQ(5, messageDeque._lastCommittedId);

    messageDeque.setCommittedMsgId(9);
    EXPECT_EQ(5, messageDeque._committedIndex);
    EXPECT_EQ(9, messageDeque._lastCommittedId);
    messageDeque.recycle(recycleSize, recycleCount, cmp);
    EXPECT_EQ(4, recycleCount);
    EXPECT_EQ(1, messageDeque._msgQueue.size());
    EXPECT_EQ(1, messageDeque._committedIndex);
    EXPECT_EQ(9, messageDeque._lastCommittedId);
}

} // namespace storage
} // namespace swift
