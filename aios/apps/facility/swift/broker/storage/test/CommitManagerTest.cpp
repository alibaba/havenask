#include "swift/broker/storage/CommitManager.h"

#include <cstddef>
#include <cstdint>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <map>
#include <utility>

#include "swift/broker/storage/MessageGroup.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace swift {
namespace storage {

class MockMessageGroup : public MessageGroup {
public:
    MockMessageGroup() {}
    ~MockMessageGroup() {}

private:
    MOCK_METHOD1(findMessageId, int64_t(uint64_t));
};

class CommitManagerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
};

void CommitManagerTest::setUp() {}

void CommitManagerTest::tearDown() {}

TEST_F(CommitManagerTest, testGetCoverRange) {
    CommitManager manager(NULL, 1000, 2000);
    EXPECT_EQ(manager.getCoverRange(0, 500), make_pair((uint16_t)1000, (uint16_t)500));
    EXPECT_EQ(manager.getCoverRange(0, 1000), make_pair((uint16_t)1000, (uint16_t)1000));
    EXPECT_EQ(manager.getCoverRange(0, 1500), make_pair((uint16_t)1000, (uint16_t)1500));
    EXPECT_EQ(manager.getCoverRange(0, 2000), make_pair((uint16_t)1000, (uint16_t)2000));
    EXPECT_EQ(manager.getCoverRange(0, 3000), make_pair((uint16_t)1000, (uint16_t)2000));
    EXPECT_EQ(manager.getCoverRange(1500, 2000), make_pair((uint16_t)1500, (uint16_t)2000));
}

TEST_F(CommitManagerTest, testUpdateCommit) {
    // 1. test needUpdateMsgId, TOPIC_MODE_MEMORY_PREFER
    {
        MockMessageGroup messageGroup;
        CommitManager manager(&messageGroup, 300, 2000);
        protocol::ConsumptionRequest request;
        request.set_committedcheckpoint(123456);
        request.mutable_filter()->set_from(400);
        request.mutable_filter()->set_to(700);
        EXPECT_CALL(messageGroup, findMessageId(_)).Times(1).WillOnce(Return(100));
        manager.updateCommit(&request);
        EXPECT_EQ(1, manager._rangeInfo.size());
        CommitManager::Range range(400, 700);
        CommitInfo info = manager._rangeInfo[range];
        EXPECT_EQ(100, info.commitMsgId);
        EXPECT_EQ(123456, info.commitTimestamp);
        EXPECT_TRUE(info.lastAccessTimestamp > 0);
    }
    // 2. test needUpdateMsgId false, TOPIC_MODE_PERSIST_DATA
    {
        MockMessageGroup messageGroup;
        CommitManager manager(&messageGroup, 300, 2000);
        protocol::ConsumptionRequest request;
        request.set_committedcheckpoint(123456);
        request.mutable_filter()->set_from(400);
        request.mutable_filter()->set_to(700);
        EXPECT_CALL(messageGroup, findMessageId(_)).Times(0);
        manager.updateCommit(&request, false);
        EXPECT_EQ(1, manager._rangeInfo.size());
        CommitManager::Range range(400, 700);
        CommitInfo info = manager._rangeInfo[range];
        EXPECT_EQ(-1, info.commitMsgId);
        EXPECT_EQ(123456, info.commitTimestamp);
        EXPECT_TRUE(info.lastAccessTimestamp > 0);
    }
}

TEST_F(CommitManagerTest, testGetCommitIdAndAccessTime) {
    MockMessageGroup messageGroup;
    int64_t committedId, accessTime, committedTime;
    {
        CommitManager manager(&messageGroup, 1000, 2000);
        manager.getCommitIdAndAccessTime(committedId, accessTime, committedTime);
        EXPECT_EQ(-1, committedId);
        EXPECT_EQ(-1, committedTime);
        EXPECT_EQ(accessTime, manager._startTime);
    }
    { // error range
        CommitManager manager(&messageGroup, 1000, 2000);
        CommitManager::Range range(700, 1500);
        manager._rangeInfo[range];
        manager.getCommitIdAndAccessTime(committedId, accessTime, committedTime);
        EXPECT_EQ(0, manager._rangeInfo.size());
        EXPECT_EQ(-1, committedId);
        EXPECT_EQ(-1, committedTime);
        EXPECT_EQ(accessTime, manager._startTime);
    }
    { // not cover range
        CommitManager manager(&messageGroup, 1000, 2000);
        CommitManager::Range range(1100, 1500);
        CommitInfo &info = manager._rangeInfo[range];
        info.lastAccessTimestamp = 12345;
        info.commitMsgId = 10;
        info.commitTimestamp = 11111;
        manager.getCommitIdAndAccessTime(committedId, accessTime, committedTime);
        EXPECT_EQ(1, manager._rangeInfo.size());
        EXPECT_EQ(-1, committedId);
        EXPECT_EQ(-1, committedTime);
        EXPECT_EQ(accessTime, manager._startTime);
    }
    { // not cover range
        CommitManager manager(&messageGroup, 1000, 2000);
        CommitManager::Range range(1000, 1500);
        CommitInfo &info = manager._rangeInfo[range];
        info.lastAccessTimestamp = 12345;
        info.commitMsgId = 10;
        info.commitTimestamp = 11111;

        CommitManager::Range range1(1501, 2000);
        CommitInfo &info1 = manager._rangeInfo[range1];
        info1.lastAccessTimestamp = 12344;
        info1.commitMsgId = 9;
        info1.commitTimestamp = 11110;

        manager.getCommitIdAndAccessTime(committedId, accessTime, committedTime);
        EXPECT_EQ(2, manager._rangeInfo.size());
        EXPECT_EQ(9, committedId);
        EXPECT_EQ(11110, committedTime);
        EXPECT_EQ(accessTime, 12344);
    }
}

}; // namespace storage
}; // namespace swift
