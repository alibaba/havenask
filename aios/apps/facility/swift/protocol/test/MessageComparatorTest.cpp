#include "swift/protocol/MessageComparator.h"

#include <iosfwd>

#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/test/MessageCreator.h"
#include "unittest/unittest.h"

using namespace std;
namespace swift {
namespace protocol {

#define COMPARE_FUNC_TEST_HELPER(member, value1, value2)                                                               \
    object1.set_##member(value1);                                                                                      \
    EXPECT_TRUE(object1 != object2);                                                                                   \
    object2.set_##member(value2);                                                                                      \
    EXPECT_TRUE(object1 != object2);                                                                                   \
    object2.set_##member(value1);                                                                                      \
    EXPECT_TRUE(object1 == object2)

class MessageComparatorTest : public TESTBASE {};

TEST_F(MessageComparatorTest, testHeartbeatInfoEqual) {
    HeartbeatInfo object1;
    HeartbeatInfo object2;
    object1.set_heartbeatid(10); // heartbeatid not compare
    EXPECT_TRUE(object1 == object2);
    object2.set_heartbeatid(20);

    COMPARE_FUNC_TEST_HELPER(alive, true, false);
    COMPARE_FUNC_TEST_HELPER(role, ROLE_TYPE_ADMIN, ROLE_TYPE_BROKER);
    COMPARE_FUNC_TEST_HELPER(address, "addr1", "addr2");
    COMPARE_FUNC_TEST_HELPER(sessionid, 1, 2);

    object1.add_tasks();
    EXPECT_TRUE(object1 != object2);
    object2.add_tasks();
    EXPECT_TRUE(object1 == object2);

    COMPARE_FUNC_TEST_HELPER(
        errcode, ErrorCode(ERROR_ADMIN_TOPIC_HAS_EXISTED), ErrorCode(ERROR_ADMIN_TOPIC_NOT_EXISTED));
    COMPARE_FUNC_TEST_HELPER(errtime, 2, 3);
    COMPARE_FUNC_TEST_HELPER(errmsg, "abc", " abc ");
}

TEST_F(MessageComparatorTest, testTaskStatusEqual) {
    TaskStatus object1;
    TaskStatus object2;

    TaskInfo *taskInfo1 = object1.mutable_taskinfo();
    EXPECT_TRUE(object1 != object2);
    TaskInfo *taskInfo2 = object2.mutable_taskinfo();
    EXPECT_TRUE(object1 == object2);

    *(taskInfo1->mutable_partitionid()) = MessageCreator::createPartitionId("topic_1");
    EXPECT_TRUE(object1 != object2);
    *(taskInfo2->mutable_partitionid()) = MessageCreator::createPartitionId("topic_1");
    EXPECT_TRUE(object1 == object2);

    COMPARE_FUNC_TEST_HELPER(status, PARTITION_STATUS_WAITING, PARTITION_STATUS_RUNNING);
    COMPARE_FUNC_TEST_HELPER(
        errcode, ErrorCode(ERROR_ADMIN_TOPIC_HAS_EXISTED), ErrorCode(ERROR_ADMIN_TOPIC_NOT_EXISTED));
    COMPARE_FUNC_TEST_HELPER(errtime, 2, 3);
    COMPARE_FUNC_TEST_HELPER(errmsg, "abc", " abc ");
}

TEST_F(MessageComparatorTest, testTaskInfoEqual) {
    TaskInfo object1;
    TaskInfo object2;

    PartitionId *partitionId1 = object1.mutable_partitionid();
    EXPECT_TRUE(object1 != object2);
    PartitionId *partitionId2 = object2.mutable_partitionid();
    EXPECT_TRUE(object1 == object2);

    *partitionId1 = MessageCreator::createPartitionId("topic_1");
    EXPECT_TRUE(object1 != object2);
    *partitionId2 = MessageCreator::createPartitionId("topic_1");
    EXPECT_TRUE(object1 == object2);

    COMPARE_FUNC_TEST_HELPER(minbuffersize, 100, 1000);
    COMPARE_FUNC_TEST_HELPER(maxbuffersize, 100, 1000);
    COMPARE_FUNC_TEST_HELPER(topicmode, TOPIC_MODE_NORMAL, TOPIC_MODE_SECURITY);
}

TEST_F(MessageComparatorTest, testPartitionIdEqual) {
    {
        PartitionId partitionId1;
        partitionId1.set_id(0);
        partitionId1.set_topicname("topic1");
        partitionId1.set_topicgroup("abc");
        PartitionId partitionId2;
        partitionId2.set_id(0);
        partitionId2.set_topicname("topic1");
        partitionId2.set_topicgroup("abc");
        EXPECT_TRUE(partitionId1 == partitionId2);
    }
    {
        PartitionId partitionId1;
        partitionId1.set_id(0);
        partitionId1.set_topicname("topic1");
        PartitionId partitionId2;
        partitionId2.set_id(0);
        partitionId2.set_topicname("topic1");
        EXPECT_TRUE(partitionId1 == partitionId2);
    }
    {
        PartitionId partitionId1;
        partitionId1.set_id(0);
        partitionId1.set_topicname("topic1");
        partitionId1.set_topicgroup("abc");
        PartitionId partitionId2;
        partitionId2.set_id(0);
        partitionId2.set_topicname("topic1");
        partitionId2.set_topicgroup("abc1");
        EXPECT_TRUE(partitionId1 == partitionId2);
    }

    {
        PartitionId partitionId1;
        partitionId1.set_id(0);
        partitionId1.set_topicname("topic1");
        partitionId1.set_version(1);
        PartitionId partitionId2;
        partitionId2.set_id(0);
        partitionId2.set_topicname("topic1");
        EXPECT_TRUE(partitionId1 == partitionId2);
        partitionId2.set_version(1);
        EXPECT_TRUE(partitionId1 == partitionId2);
        partitionId2.set_version(2);
        EXPECT_TRUE(partitionId1 != partitionId2);
    }
}

TEST_F(MessageComparatorTest, testPartitionIdLess) {
    {
        PartitionId partitionId1;
        partitionId1.set_id(0);
        partitionId1.set_topicname("topic1");
        partitionId1.set_topicgroup("abc");
        PartitionId partitionId2;
        partitionId2.set_id(0);
        partitionId2.set_topicname("topic2");
        partitionId2.set_topicgroup("abc");
        EXPECT_TRUE(partitionId1 < partitionId2);
    }
    {
        PartitionId partitionId1;
        partitionId1.set_id(0);
        partitionId1.set_topicname("topic1");
        partitionId1.set_topicgroup("abc");
        PartitionId partitionId2;
        partitionId2.set_id(1);
        partitionId2.set_topicname("topic1");
        partitionId2.set_topicgroup("abc");
        EXPECT_TRUE(partitionId1 < partitionId2);
    }
}

TEST_F(MessageComparatorTest, testPartitionIdGreater) {
    {
        PartitionId partitionId1;
        partitionId1.set_id(0);
        partitionId1.set_topicname("topic1");
        partitionId1.set_topicgroup("abc");
        PartitionId partitionId2;
        partitionId2.set_id(0);
        partitionId2.set_topicname("topic0");
        partitionId2.set_topicgroup("abc");
        EXPECT_TRUE(partitionId1 > partitionId2);
    }
    {
        PartitionId partitionId1;
        partitionId1.set_id(2);
        partitionId1.set_topicname("topic1");
        partitionId1.set_topicgroup("abc");
        PartitionId partitionId2;
        partitionId2.set_id(1);
        partitionId2.set_topicname("topic1");
        partitionId2.set_topicgroup("abc");
        EXPECT_TRUE(partitionId1 > partitionId2);
    }
}

} // namespace protocol
} // namespace swift
