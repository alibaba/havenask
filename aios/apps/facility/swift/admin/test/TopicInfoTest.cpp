#include "swift/admin/TopicInfo.h"

#include <ext/alloc_traits.h>
#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/MessageComparator.h" // IWYU pragma: keep
#include "unittest/unittest.h"

namespace swift {
namespace admin {

class TopicInfoTest : public TESTBASE {
public:
    TopicInfoTest();
    ~TopicInfoTest();

public:
    void setUp();
    void tearDown();

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, TopicInfoTest);
using namespace swift::protocol;
using namespace std;
TopicInfoTest::TopicInfoTest() {}

TopicInfoTest::~TopicInfoTest() {}

void TopicInfoTest::setUp() { AUTIL_LOG(DEBUG, "setUp!"); }

void TopicInfoTest::tearDown() { AUTIL_LOG(DEBUG, "tearDown!"); }

TEST_F(TopicInfoTest, testSimpleProcess) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    string topicName("cloud_search");
    uint32_t partitionCount = 10;
    TopicCreationRequest topicMeta;
    topicMeta.set_topicname(topicName);
    topicMeta.set_partitioncount(partitionCount);
    topicMeta.set_partitionminbuffersize(1234);
    TopicInfo topicInfo(topicMeta);

    ASSERT_EQ(partitionCount, topicInfo.getPartitionCount());
    uint32_t from, to;
    ASSERT_FALSE(topicInfo.getRangeInfo(10, from, to));
    ASSERT_TRUE(topicInfo.getRangeInfo(0, from, to));
    ASSERT_EQ(0, from);
    ASSERT_EQ(6553, to);
    ASSERT_TRUE(topicInfo.getRangeInfo(9, from, to));
    ASSERT_EQ(58983, from);
    ASSERT_EQ(65535, to);
    ASSERT_EQ((uint32_t)1234, topicInfo.getTopicMeta().partitionminbuffersize());
    ASSERT_EQ(topicName, topicInfo.getTopicMeta().topicname());
    topicMeta.set_partitionminbuffersize(2345);
    topicInfo.setTopicMeta(topicMeta);
    ASSERT_EQ((uint32_t)2345, topicInfo.getTopicMeta().partitionminbuffersize());

    ASSERT_TRUE(!topicInfo.needChange(0));

    topicInfo.setCurrentRoleAddress(0, "role_1#_#10.250.12.25:2135");
    ASSERT_EQ(string("role_1#_#10.250.12.25:2135"), topicInfo.getCurrentRoleAddress(0));
    ASSERT_EQ(string("role_1"), topicInfo.getCurrentRole(0));
    ASSERT_TRUE(topicInfo.needChange(0));
    topicInfo.setTargetRoleAddress(0, "role_1#_#10.250.12.25:2135");
    ASSERT_EQ(string("role_1#_#10.250.12.25:2135"), topicInfo.getTargetRoleAddress(0));
    ASSERT_EQ(string("role_1"), topicInfo.getTargetRole(0));
    ASSERT_EQ(string("role_1#_#10.250.12.25:2135"), topicInfo.getLastRoleAddress(0));
    ASSERT_EQ(string("role_1"), topicInfo.getLastRole(0));
    ASSERT_TRUE(!topicInfo.needChange(0));

    topicInfo.setTargetRoleAddress(0, "");
    ASSERT_EQ(string("role_1#_#10.250.12.25:2135"), topicInfo.getLastRoleAddress(0));
    ASSERT_EQ(string("role_1"), topicInfo.getLastRole(0));
    ASSERT_TRUE(topicInfo.needChange(0));

    topicInfo.setTargetRoleAddress(0, "role_1#_#10.250.12.27:2135");
    ASSERT_EQ(string("role_1#_#10.250.12.27:2135"), topicInfo.getTargetRoleAddress(0));
    ASSERT_TRUE(topicInfo.needChange(0));

    topicInfo.setCurrentRoleAddress(9, "role_1#_#10.250.12.25:2135");
    ASSERT_EQ(string("role_1#_#10.250.12.25:2135"), topicInfo.getCurrentRoleAddress(9));
    ASSERT_TRUE(topicInfo.needChange(9));

    topicInfo.setTargetRoleAddress(9, "role_1#_#10.250.12.25:2135");
    ASSERT_EQ(string("role_1#_#10.250.12.25:2135"), topicInfo.getTargetRoleAddress(9));
    EXPECT_FALSE(topicInfo.needChange(9));
    EXPECT_TRUE(topicInfo.needChange(9, true));
    topicInfo._inlineVersions[9].set_partversion(1);
    EXPECT_FALSE(topicInfo.needChange(9, true));
    topicInfo._inlineVersions[9].set_masterversion(1);
    EXPECT_TRUE(topicInfo.needChange(9, true));
    EXPECT_FALSE(topicInfo.needChange(9, true, 1));
    topicInfo._inlineVersions[9].set_masterversion(0);
    topicInfo.setTargetRoleAddress(9, "role_1#_#10.250.12.26:2135");
    ASSERT_EQ(string("role_1#_#10.250.12.26:2135"), topicInfo.getTargetRoleAddress(9));
    ASSERT_TRUE(topicInfo.needChange(9));

    ASSERT_EQ(string(""), topicInfo.getTargetRoleAddress(5));
    ASSERT_EQ(string(""), topicInfo.getTargetRole(5));
    ASSERT_EQ(string(""), topicInfo.getLastRoleAddress(5));
    ASSERT_EQ(string(""), topicInfo.getLastRole(5));
    topicInfo.setTargetRoleAddress(5, "b");
    ASSERT_EQ(string("b"), topicInfo.getTargetRoleAddress(5));
    ASSERT_EQ(string(""), topicInfo.getTargetRole(5));
    ASSERT_EQ(string("b"), topicInfo.getLastRoleAddress(5));
    ASSERT_EQ(string(""), topicInfo.getLastRole(5));
    topicInfo.setTargetRoleAddress(5, "");
    ASSERT_EQ(string(""), topicInfo.getTargetRoleAddress(5));
    ASSERT_EQ(string("b"), topicInfo.getLastRoleAddress(5));
}

TEST_F(TopicInfoTest, testCompareFunc) {
    TopicCreationRequest topicMeta;
    topicMeta.set_topicname("topic_a");
    topicMeta.set_resource(20);
    TopicInfoPtr topicInfo1(new TopicInfo(topicMeta));
    topicMeta.set_topicname("topic_b");
    topicMeta.set_resource(20);
    TopicInfoPtr topicInfo2(new TopicInfo(topicMeta));
    topicMeta.set_topicname("topic_a");
    topicMeta.set_resource(30);
    TopicInfoPtr topicInfo3(new TopicInfo(topicMeta));

    ASSERT_TRUE(topicInfo1 < topicInfo2);
    ASSERT_TRUE(topicInfo2 < topicInfo3);
    ASSERT_TRUE(topicInfo1 < topicInfo3);
}

TEST_F(TopicInfoTest, testTopicStatus) {
    TopicCreationRequest topicMeta;
    topicMeta.set_partitioncount(3);
    TopicInfo topicInfo(topicMeta);
    ASSERT_EQ(TOPIC_STATUS_WAITING, topicInfo.getTopicStatus());
    topicInfo.setStatus(2, PARTITION_STATUS_STARTING);
    ASSERT_EQ(TOPIC_STATUS_WAITING, topicInfo.getTopicStatus());

    topicInfo.setStatus(1, PARTITION_STATUS_RUNNING);
    ASSERT_EQ(TOPIC_STATUS_PARTIAL_RUNNING, topicInfo.getTopicStatus());
    topicInfo.setStatus(0, PARTITION_STATUS_RUNNING);
    topicInfo.setStatus(2, PARTITION_STATUS_RUNNING);
    ASSERT_EQ(TOPIC_STATUS_RUNNING, topicInfo.getTopicStatus());

    topicInfo.setStatus(0, PARTITION_STATUS_STOPPING);
    topicInfo.setStatus(1, PARTITION_STATUS_STOPPING);
    topicInfo.setStatus(2, PARTITION_STATUS_STOPPING);

    ASSERT_EQ(TOPIC_STATUS_STOPPING, topicInfo.getTopicStatus());

    topicInfo.setStatus(0, PARTITION_STATUS_RUNNING);
    topicInfo.setStatus(1, PARTITION_STATUS_RUNNING);
    topicInfo.setStatus(2, PARTITION_STATUS_STARTING);

    ASSERT_EQ(TOPIC_STATUS_PARTIAL_RUNNING, topicInfo.getTopicStatus());

    topicInfo.setStatus(0, PARTITION_STATUS_STARTING);
    topicInfo.setStatus(1, PARTITION_STATUS_STARTING);
    topicInfo.setStatus(2, PARTITION_STATUS_STARTING);

    ASSERT_EQ(TOPIC_STATUS_PARTIAL_RUNNING, topicInfo.getTopicStatus());

    topicInfo.setStatus(0, PARTITION_STATUS_STOPPING);
    ASSERT_EQ(TOPIC_STATUS_PARTIAL_RUNNING, topicInfo.getTopicStatus());
}

TEST_F(TopicInfoTest, testSetResource) {
    TopicCreationRequest topicMeta;
    TopicInfo topicInfo(topicMeta);
    EXPECT_EQ(1, topicInfo.getResource());

    uint32_t oldResource = 0;
    EXPECT_FALSE(topicInfo.setResource(1, oldResource));
    EXPECT_EQ(1, topicInfo.getResource());
    EXPECT_EQ(1, oldResource);

    EXPECT_TRUE(topicInfo.setResource(2, oldResource));
    EXPECT_EQ(1, oldResource);
    EXPECT_EQ(2, topicInfo.getResource());
}

TEST_F(TopicInfoTest, testSetPartitionLimit) {
    TopicCreationRequest topicMeta;
    TopicInfo topicInfo(topicMeta);
    EXPECT_EQ(100000, topicInfo.getPartitionLimit());

    uint32_t oldLimit = 100000;
    EXPECT_FALSE(topicInfo.setPartitionLimit(100000, oldLimit));
    EXPECT_EQ(100000, topicInfo.getPartitionLimit());
    EXPECT_EQ(100000, oldLimit);

    EXPECT_TRUE(topicInfo.setPartitionLimit(2, oldLimit));
    EXPECT_EQ(100000, oldLimit);
    EXPECT_EQ(2, topicInfo.getPartitionLimit());
}

TEST_F(TopicInfoTest, testNeedSchedule) {
    {
        TopicCreationRequest topicMeta;
        ASSERT_EQ(TOPIC_TYPE_NORMAL, topicMeta.topictype());
        TopicInfo topicInfo(topicMeta);
        ASSERT_TRUE(topicInfo.needSchedule());
    }
    {
        TopicCreationRequest topicMeta;
        topicMeta.set_topictype(TOPIC_TYPE_NORMAL);
        TopicInfo topicInfo(topicMeta);
        ASSERT_TRUE(topicInfo.needSchedule());
    }
    {
        TopicCreationRequest topicMeta;
        topicMeta.set_topictype(TOPIC_TYPE_LOGIC);
        TopicInfo topicInfo(topicMeta);
        ASSERT_FALSE(topicInfo.needSchedule());
    }
    {
        TopicCreationRequest topicMeta;
        topicMeta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
        TopicInfo topicInfo(topicMeta);
        ASSERT_TRUE(topicInfo.needSchedule());
    }
    {
        TopicCreationRequest topicMeta;
        topicMeta.set_topictype(TOPIC_TYPE_PHYSIC);
        TopicInfo topicInfo(topicMeta);
        ASSERT_TRUE(topicInfo.needSchedule());
    }
}

TEST_F(TopicInfoTest, testHasSubPhysicTopics) {
    {
        TopicCreationRequest topicMeta;
        topicMeta.set_topictype(TOPIC_TYPE_LOGIC);
        *topicMeta.add_physictopiclst() = "p1";
        TopicInfo topicInfo(topicMeta);
        ASSERT_TRUE(topicInfo.hasSubPhysicTopics());
    }
    {
        TopicCreationRequest topicMeta;
        topicMeta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
        *topicMeta.add_physictopiclst() = "p1";
        TopicInfo topicInfo(topicMeta);
        ASSERT_TRUE(topicInfo.hasSubPhysicTopics());
    }
    {
        TopicCreationRequest topicMeta;
        topicMeta.set_topictype(TOPIC_TYPE_LOGIC);
        TopicInfo topicInfo(topicMeta);
        ASSERT_FALSE(topicInfo.hasSubPhysicTopics());
    }
    {
        TopicCreationRequest topicMeta;
        topicMeta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
        TopicInfo topicInfo(topicMeta);
        ASSERT_FALSE(topicInfo.hasSubPhysicTopics());
    }
    {
        TopicCreationRequest topicMeta;
        topicMeta.set_topictype(TOPIC_TYPE_PHYSIC);
        TopicInfo topicInfo(topicMeta);
        ASSERT_FALSE(topicInfo.hasSubPhysicTopics());
    }
    {
        TopicCreationRequest topicMeta;
        topicMeta.set_topictype(TOPIC_TYPE_PHYSIC);
        *topicMeta.add_physictopiclst() = "p1";
        TopicInfo topicInfo(topicMeta);
        ASSERT_FALSE(topicInfo.hasSubPhysicTopics());
    }
    {
        TopicCreationRequest topicMeta;
        topicMeta.set_topictype(TOPIC_TYPE_NORMAL);
        TopicInfo topicInfo(topicMeta);
        ASSERT_FALSE(topicInfo.hasSubPhysicTopics());
    }
}

TEST_F(TopicInfoTest, testPhysicTopicLst) {
    TopicCreationRequest topicMeta;
    topicMeta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    TopicInfo topicInfo(topicMeta);
    auto pylst = topicInfo.physicTopicLst();
    ASSERT_EQ(0, pylst.size());

    *topicInfo._topicMeta.add_physictopiclst() = "p1";
    pylst = topicInfo.physicTopicLst();
    ASSERT_EQ(1, pylst.size());
    ASSERT_EQ("p1", pylst[0]);

    *topicInfo._topicMeta.add_physictopiclst() = "p2";
    pylst = topicInfo.physicTopicLst();
    ASSERT_EQ(2, pylst.size());
    ASSERT_EQ("p1", pylst[0]);
    ASSERT_EQ("p2", pylst[1]);
}

TEST_F(TopicInfoTest, testGetLastPhysicTopicName) {
    TopicCreationRequest topicMeta;
    topicMeta.set_topicname("topic");
    topicMeta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    TopicInfo topicInfo(topicMeta);
    auto topicName = topicInfo.getLastPhysicTopicName();
    ASSERT_EQ("topic", topicName);

    *topicInfo._topicMeta.add_physictopiclst() = "p1";
    topicName = topicInfo.getLastPhysicTopicName();
    ASSERT_EQ("p1", topicName);

    *topicInfo._topicMeta.add_physictopiclst() = "p2";
    topicName = topicInfo.getLastPhysicTopicName();
    ASSERT_EQ("p2", topicName);

    topicInfo._topicMeta.set_topictype(TOPIC_TYPE_LOGIC);
    topicInfo._topicMeta.clear_physictopiclst();
    topicName = topicInfo.getLastPhysicTopicName();
    ASSERT_TRUE(topicName.empty());
}

TEST_F(TopicInfoTest, testConstructionWithPartitionStatus) {
    {
        TopicCreationRequest topicMeta;
        topicMeta.set_partitioncount(3);
        TopicInfo topicInfo(topicMeta);
        for (int i = 0; i < 3; ++i) {
            ASSERT_EQ(PARTITION_STATUS_WAITING, topicInfo.getStatus(i));
            ASSERT_EQ(PARTITION_STATUS_WAITING, topicInfo._lastStatus[i]);
        }
    }
    {
        TopicCreationRequest topicMeta;
        topicMeta.set_partitioncount(3);
        TopicInfo topicInfo(topicMeta, PARTITION_STATUS_RUNNING);
        for (int i = 0; i < 3; ++i) {
            ASSERT_EQ(PARTITION_STATUS_RUNNING, topicInfo.getStatus(i));
            ASSERT_EQ(PARTITION_STATUS_RUNNING, topicInfo._lastStatus[i]);
        }
    }
}

TEST_F(TopicInfoTest, testSetInlineVersion) {
    TopicCreationRequest meta;
    meta.set_partitioncount(2);
    TopicInfo ti(meta);
    protocol::InlineVersion emptyInlineVersion;
    EXPECT_EQ(emptyInlineVersion, ti.getInlineVersion(0));
    EXPECT_EQ(emptyInlineVersion, ti.getInlineVersion(1));
    protocol::InlineVersion inlineVersion;
    inlineVersion.set_masterversion(10);
    inlineVersion.set_partversion(100);
    ti.setInlineVersion(0, inlineVersion);
    EXPECT_EQ(inlineVersion, ti.getInlineVersion(0));

    // not allow set smaller
    protocol::InlineVersion inlineVersion2;
    inlineVersion2.set_masterversion(1);
    inlineVersion2.set_partversion(1000);
    ti.setInlineVersion(0, inlineVersion2);
    EXPECT_EQ(inlineVersion, ti.getInlineVersion(0));

    // set bigger
    inlineVersion2.set_masterversion(11);
    inlineVersion2.set_partversion(90);
    ti.setInlineVersion(0, inlineVersion2);
    EXPECT_EQ(inlineVersion2, ti.getInlineVersion(0));
    EXPECT_EQ(emptyInlineVersion, ti.getInlineVersion(1));
}

TEST_F(TopicInfoTest, testBrokerChanged) {
    TopicCreationRequest meta;
    meta.set_partitioncount(4);
    TopicInfo ti(meta);
    // ti._currentBrokers[0] and ti._targetBrokers[0] are default
    ti._currentBrokers[1] = "b1";
    ti._targetBrokers[1] = "b1";
    // ti._currentBrokers[2] not set
    ti._targetBrokers[2] = "b2";
    ti._currentBrokers[3] = "b3";
    ti._targetBrokers[3] = "b4";
    EXPECT_FALSE(ti.brokerChanged(0));
    EXPECT_FALSE(ti.brokerChanged(1));
    EXPECT_TRUE(ti.brokerChanged(2));
    EXPECT_TRUE(ti.brokerChanged(3));
}

TEST_F(TopicInfoTest, testSetSessionId) {
    TopicCreationRequest topicMeta;
    topicMeta.set_partitioncount(2);
    TopicInfo topicInfo(topicMeta);
    EXPECT_EQ(2, topicInfo._sessionIds.size());
    EXPECT_EQ(-1, topicInfo.getSessionId(0));
    EXPECT_EQ(-1, topicInfo.getSessionId(1));
    topicInfo.setSessionId(0, 100);
    topicInfo.setSessionId(1, 200);
    EXPECT_EQ(100, topicInfo.getSessionId(0));
    EXPECT_EQ(200, topicInfo.getSessionId(1));
}

} // namespace admin
} // namespace swift
