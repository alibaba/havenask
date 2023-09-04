#include "swift/admin/WorkerInfo.h"

#include <memory>
#include <set>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <type_traits>
#include <utility>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/admin/RoleInfo.h"
#include "swift/admin/TopicInfo.h"
#include "swift/admin/TopicTable.h"
#include "swift/admin/test/MessageConstructor.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/util/CircularVector.h"
#include "unittest/unittest.h"

namespace swift {
namespace admin {

class WorkerInfoTest : public TESTBASE {
public:
    WorkerInfoTest();
    ~WorkerInfoTest();

public:
    void setUp();
    void tearDown();

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, WorkerInfoTest);

using namespace swift::protocol;
using namespace autil;
WorkerInfoTest::WorkerInfoTest() {}

WorkerInfoTest::~WorkerInfoTest() {}

void WorkerInfoTest::setUp() { AUTIL_LOG(DEBUG, "setUp!"); }

void WorkerInfoTest::tearDown() { AUTIL_LOG(DEBUG, "tearDown!"); }

TEST_F(WorkerInfoTest, testSimpleProcess) {
    AUTIL_LOG(DEBUG, "Begin Test!");
    std::string address("Role_1#_#10.250.12.35:5556");
    HeartbeatInfo heartbeatInfo = MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true);
    RoleInfo roleInfo("Role_1", "10.250.12.35", 5555);
    WorkerInfo workerInfo(roleInfo);
    TopicTable topicTable;
    heartbeatInfo.set_sessionid(100);
    workerInfo.setCurrent(heartbeatInfo);
    workerInfo.prepareDecision(0, 0);
    ASSERT_EQ(address, workerInfo.getCurrentRoleAddress());
    ASSERT_TRUE(workerInfo.getFreeResource() == 10000);
    ASSERT_TRUE(workerInfo.needChange(topicTable));
    workerInfo.updateRolePort(5556);
    ASSERT_EQ(address, workerInfo.getTargetRoleAddress());
    EXPECT_TRUE(workerInfo.needChange(topicTable));
    workerInfo.updateLastSessionId();
    EXPECT_FALSE(workerInfo.needChange(topicTable));
    EXPECT_EQ(100, workerInfo.getCurrentSessionId());

    std::string topicName;
    uint32_t partition;

    ASSERT_EQ(size_t(0), workerInfo.getTargetTask().size());

    topicName = "news_search";
    partition = 5;
    WorkerInfo::StatusInfo statusInfo;
    ASSERT_TRUE(!workerInfo.hasTaskInCurrent(topicName, partition, statusInfo));
    ASSERT_TRUE(!workerInfo.addTask2target(topicName, partition, 0, 100));
    ASSERT_TRUE(workerInfo.addTask2target(topicName, partition, 5000, 100));
    ASSERT_TRUE(!workerInfo.addTask2target(topicName, partition, 5000, 100));
    ASSERT_TRUE(workerInfo.addTask2target("news_search2", partition, 5000, 100));
    ASSERT_TRUE(!workerInfo.addTask2target("news_search3", partition, 1, 100));

    ASSERT_TRUE(workerInfo.needChange(topicTable));

    const WorkerInfo::TaskSet &taskSet = workerInfo.getTargetTask();
    ASSERT_EQ(size_t(2), taskSet.size());

    ASSERT_TRUE(taskSet.find(std::make_pair(topicName, partition)) != taskSet.end());
    ASSERT_TRUE(taskSet.find(std::make_pair("news_search2", partition)) != taskSet.end());
}

TEST_F(WorkerInfoTest, testHasTaskInCurrent) {
    std::string address("role_1#_#10.250.12.35:5556");
    std::string topicName = "news_search";
    uint32_t partition = 5;
    HeartbeatInfo heartbeatInfo = MessageConstructor::ConstructHeartbeatInfo(
        address, ROLE_TYPE_BROKER, true, topicName, partition, PARTITION_STATUS_RUNNING, 1000);
    RoleInfo roleInfo("Role_1", "10.250.12.35", 5556);
    WorkerInfo workerInfo(roleInfo);
    workerInfo.setCurrent(heartbeatInfo);
    workerInfo.prepareDecision(0, 0);
    WorkerInfo::StatusInfo statusInfo;
    ASSERT_TRUE(workerInfo.hasTaskInCurrent(topicName, partition, statusInfo));
    EXPECT_EQ(PARTITION_STATUS_RUNNING, statusInfo.status);
    EXPECT_EQ(1000, statusInfo.sessionId);
    ASSERT_TRUE(!workerInfo.hasTaskInCurrent("not_exist", partition, statusInfo));
    ASSERT_TRUE(!workerInfo.hasTaskInCurrent(topicName, partition + 1, statusInfo));
}

TEST_F(WorkerInfoTest, testNeedChange) {
    std::string address("role_1#_#10.250.12.35:5556");
    std::string topicName = "news_search";
    uint32_t partition = 5;
    HeartbeatInfo heartbeatInfo =
        MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true, topicName, partition);
    TopicTable topicTable;
    {
        RoleInfo roleInfo("role_1", "10.250.12.35", 5556);
        WorkerInfo workerInfo(roleInfo);
        workerInfo.setCurrent(heartbeatInfo);
        workerInfo.prepareDecision(0, 0);
        ASSERT_TRUE(workerInfo.addTask2target(topicName, partition, 1, 100));
        ASSERT_TRUE(!workerInfo.needChange(topicTable));

        ASSERT_TRUE(workerInfo.addTask2target(topicName, 0, 1, 100));
        ASSERT_TRUE(workerInfo.needChange(topicTable));
    }
    {
        RoleInfo roleInfo("role_1", "10.250.12.37", 5556);
        WorkerInfo workerInfo(roleInfo);
        workerInfo.setCurrent(heartbeatInfo);
        workerInfo.prepareDecision(0, 0);
        ASSERT_TRUE(workerInfo.addTask2target(topicName, partition, 1, 100));
        ASSERT_TRUE(workerInfo.needChange(topicTable));
    }
    { // topic version change
        heartbeatInfo.mutable_tasks(0)->mutable_taskinfo()->mutable_partitionid()->set_version(1);
        RoleInfo roleInfo("role_1", "10.250.12.35", 5556);
        WorkerInfo workerInfo(roleInfo);
        workerInfo.setCurrent(heartbeatInfo);
        workerInfo.prepareDecision(0, 0);
        ASSERT_TRUE(workerInfo.addTask2target(topicName, partition, 1, 100));
        ASSERT_TRUE(workerInfo.needChange(topicTable));

        TopicCreationRequest request;
        request.set_topicname(topicName);
        request.set_modifytime(1);
        request.set_partitioncount(6);
        topicTable.addTopic(&request);
        EXPECT_FALSE(workerInfo.needChange(topicTable));

        // inline version change
        protocol::InlineVersion version;
        version.set_masterversion(1);
        auto topicInfo = topicTable.findTopic(topicName);
        topicInfo->setInlineVersion(5, version);
        EXPECT_FALSE(workerInfo.needChange(topicTable));
        EXPECT_TRUE(workerInfo.needChange(topicTable, true));
        version.set_masterversion(0);
        version.set_partversion(1);
        topicInfo->setInlineVersion(5, version);
        EXPECT_FALSE(workerInfo.needChange(topicTable, false));
        EXPECT_TRUE(workerInfo.needChange(topicTable, true));

        // topic modifytime change
        version.set_masterversion(0);
        version.set_partversion(0);
        topicInfo->setInlineVersion(5, version);
        topicTable.clear();
        request.set_modifytime(2);
        topicTable.addTopic(&request);
        EXPECT_TRUE(workerInfo.needChange(topicTable));
    }
}

TEST_F(WorkerInfoTest, testIsDead) {
    std::string address("role_1#_#10.250.12.35:5556");
    HeartbeatInfo heartbeatInfo = MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, false);
    RoleInfo roleInfo("role_1", "10.250.12.35", 5556);
    WorkerInfo workerInfo(roleInfo);
    workerInfo.setCurrent(heartbeatInfo);
    workerInfo.prepareDecision(0, 0);
    ASSERT_TRUE(workerInfo.isDead());
}

TEST_F(WorkerInfoTest, testCheckStatus) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    std::string address("role_1#_#10.250.12.35:5556");
    HeartbeatInfo heartbeatInfo = MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true);
    RoleInfo roleInfo("role_1", "10.250.12.35", 5556);
    WorkerInfo workerInfo(roleInfo);
    workerInfo.setCurrent(heartbeatInfo);
    workerInfo._snapshot = workerInfo._current;
    workerInfo.checkStatus(0, 0);
    ASSERT_TRUE(!workerInfo.isDead());
    ASSERT_EQ(ROLE_DS_ALIVE, workerInfo._workerStatus);

    heartbeatInfo.set_alive(false);
    workerInfo.setCurrent(heartbeatInfo);
    workerInfo._snapshot = workerInfo._current;
    workerInfo.checkStatus(10, 15);
    ASSERT_TRUE(!workerInfo.isDead());
    ASSERT_EQ(ROLE_DS_UNKOWN, workerInfo._workerStatus);

    workerInfo.checkStatus(50, 15);
    ASSERT_TRUE(workerInfo.isDead());
    ASSERT_EQ(ROLE_DS_DEAD, workerInfo._workerStatus);

    heartbeatInfo.set_alive(true);
    workerInfo.setCurrent(heartbeatInfo);
    workerInfo._snapshot = workerInfo._current;
    workerInfo.checkStatus(60, 15);
    ASSERT_TRUE(!workerInfo.isDead());
    ASSERT_EQ(ROLE_DS_ALIVE, workerInfo._workerStatus);
}

TEST_F(WorkerInfoTest, testLessOperator) {

    HeartbeatInfo heartbeatInfo1 =
        MessageConstructor::ConstructHeartbeatInfo("Role_1#_#10.250.12.33:123", ROLE_TYPE_BROKER, true);
    RoleInfo roleInfo1("Role_1", "10.250.12.33", 123);
    WorkerInfoPtr workerInfoPtr1(new WorkerInfo(roleInfo1));
    workerInfoPtr1->setCurrent(heartbeatInfo1);
    workerInfoPtr1->prepareDecision(0, 0);

    HeartbeatInfo heartbeatInfo2 =
        MessageConstructor::ConstructHeartbeatInfo("Role_2#_#10.250.12.35:123", ROLE_TYPE_BROKER, true);
    RoleInfo roleInfo2("Role_2", "10.250.12.35", 123);
    WorkerInfoPtr workerInfoPtr2(new WorkerInfo(roleInfo2));
    workerInfoPtr2->setCurrent(heartbeatInfo2);
    workerInfoPtr2->prepareDecision(0, 0);

    ASSERT_TRUE(workerInfoPtr1 < workerInfoPtr2);
    ASSERT_TRUE(workerInfoPtr2->addTask2target("cloud", 1, 5, 100));
    ASSERT_TRUE(workerInfoPtr2 < workerInfoPtr1);
}

TEST_F(WorkerInfoTest, testAddTask2target) {
    AUTIL_LOG(DEBUG, "Begin Test!");
    {
        RoleInfo roleInfo;
        WorkerInfo workerInfo(roleInfo);
        // topic name is empty
        ASSERT_FALSE(workerInfo.addTask2target("", 0, 100, 2));
        // resource is zero
        ASSERT_FALSE(workerInfo.addTask2target("t1", 0, 0, 2));

        // partition limit
        ASSERT_TRUE(workerInfo.addTask2target("t1", 0, 1000, 2));
        ASSERT_FALSE(workerInfo.addTask2target("t1", 0, 1000, 2));
        ASSERT_TRUE(workerInfo.addTask2target("t1", 1, 1000, 2));
        ASSERT_FALSE(workerInfo.addTask2target("t1", 2, 1000, 2));

        // resource limit
        ASSERT_FALSE(workerInfo.addTask2target("t2", 0, 9000, 2));
        ASSERT_TRUE(workerInfo.addTask2target("t2", 1, 8000, 2));
        ASSERT_FALSE(workerInfo.addTask2target("t2", 2, 100, 2));
    }
    {
        RoleInfo roleInfo("abc##role_1", "10.10.101.10", 123);
        WorkerInfo workerInfo(roleInfo);
        // group name not match
        ASSERT_TRUE(workerInfo.addTask2target("t1", 1, 100, 2, "abc"));
        ASSERT_FALSE(workerInfo.addTask2target("t1", 1, 100, 2, ""));
        ASSERT_FALSE(workerInfo.addTask2target("t1", 1, 100, 2, "abcx"));
    }
}

TEST_F(WorkerInfoTest, testAddBrokerInMetric) {
    RoleInfo roleInfo;
    WorkerInfo workerInfo(roleInfo);
    EXPECT_EQ(0, workerInfo._brokerInMetrics.size());

    BrokerInStatus status;
    status.updateTime = 10;
    status.cpu = 18.8;
    status.mem = 19.9;
    status.writeRate1min = 20;
    status.writeRate5min = 30;
    status.readRate1min = 40;
    status.readRate5min = 50;
    status.writeRequest1min = 60;
    status.writeRequest5min = 70;
    status.readRequest1min = 80;
    status.readRequest5min = 90;
    workerInfo.addBrokerInMetric(status);
    EXPECT_EQ(1, workerInfo._brokerInMetrics.size());
    const BrokerInStatus &back = workerInfo._brokerInMetrics.back();
    EXPECT_EQ(10, back.updateTime);
    EXPECT_EQ(18.8, back.cpu);
    EXPECT_EQ(19.9, back.mem);
    EXPECT_EQ(20, back.writeRate1min);
    EXPECT_EQ(30, back.writeRate5min);
    EXPECT_EQ(40, back.readRate1min);
    EXPECT_EQ(50, back.readRate5min);
    EXPECT_EQ(60, back.writeRequest1min);
    EXPECT_EQ(70, back.writeRequest5min);
    EXPECT_EQ(80, back.readRequest1min);
    EXPECT_EQ(90, back.readRequest5min);

    status.updateTime = 100;
    status.cpu = 118.8;
    status.mem = 119.9;
    status.readRequest5min = 900;
    workerInfo.addBrokerInMetric(status);
    EXPECT_EQ(2, workerInfo._brokerInMetrics.size());
    const BrokerInStatus &back2 = workerInfo._brokerInMetrics.back();
    EXPECT_EQ(100, back2.updateTime);
    EXPECT_EQ(118.8, back2.cpu);
    EXPECT_EQ(119.9, back2.mem);
    EXPECT_EQ(20, back2.writeRate1min);
    EXPECT_EQ(30, back2.writeRate5min);
    EXPECT_EQ(40, back2.readRate1min);
    EXPECT_EQ(50, back2.readRate5min);
    EXPECT_EQ(60, back2.writeRequest1min);
    EXPECT_EQ(70, back2.writeRequest5min);
    EXPECT_EQ(80, back2.readRequest1min);
    EXPECT_EQ(900, back2.readRequest5min);
}

TEST_F(WorkerInfoTest, testIsDeadInBrokerStatus) {
    uint32_t timeoutSec = 180;
    { // default role type is ROLE_TYPE_BROKER
        RoleInfo roleInfo;
        EXPECT_EQ(ROLE_TYPE_BROKER, roleInfo.roleType);
        WorkerInfo workerInfo(roleInfo);
        workerInfo._roleInfo.roleType = ROLE_TYPE_BROKER;
        uint32_t currentTime = TimeUtility::currentTimeInSeconds();
        EXPECT_EQ(0, workerInfo._brokerInMetrics.size());
        workerInfo._startTimeSec = currentTime - 200;
        EXPECT_TRUE(workerInfo.isDeadInBrokerStatus(timeoutSec));
        workerInfo._roleInfo.roleType = ROLE_TYPE_NONE;
        EXPECT_FALSE(workerInfo.isDeadInBrokerStatus(timeoutSec));
        workerInfo._roleInfo.roleType = ROLE_TYPE_ALL;
        EXPECT_FALSE(workerInfo.isDeadInBrokerStatus(timeoutSec));
        workerInfo._roleInfo.roleType = ROLE_TYPE_ADMIN;
        EXPECT_FALSE(workerInfo.isDeadInBrokerStatus(timeoutSec));
    }
    { // ROLE_TYPE_BROKER
        RoleInfo roleInfo("broker", "1.1.1.1", 1234, ROLE_TYPE_BROKER);
        WorkerInfo workerInfo(roleInfo);
        uint32_t currentTime = TimeUtility::currentTimeInSeconds();
        EXPECT_EQ(0, workerInfo._brokerInMetrics.size());
        EXPECT_FALSE(workerInfo.isDeadInBrokerStatus(timeoutSec));

        workerInfo._startTimeSec = currentTime - timeoutSec;
        EXPECT_EQ(0, workerInfo._brokerInMetrics.size());
        EXPECT_FALSE(workerInfo.isDeadInBrokerStatus(timeoutSec));

        workerInfo._startTimeSec = currentTime - 181;
        EXPECT_TRUE(workerInfo.isDeadInBrokerStatus(timeoutSec));

        BrokerInStatus status;
        status.updateTime = currentTime - 179;
        workerInfo.addBrokerInMetric(status);
        EXPECT_FALSE(workerInfo.isDeadInBrokerStatus(timeoutSec));

        status.updateTime = currentTime - timeoutSec;
        workerInfo.addBrokerInMetric(status);
        EXPECT_FALSE(workerInfo.isDeadInBrokerStatus(timeoutSec));

        status.updateTime = currentTime - 181;
        workerInfo.addBrokerInMetric(status);
        workerInfo._startTimeSec = currentTime - 179;
        EXPECT_FALSE(workerInfo.isDeadInBrokerStatus(timeoutSec));
        workerInfo._startTimeSec = currentTime - 182;
        EXPECT_TRUE(workerInfo.isDeadInBrokerStatus(timeoutSec));

        const BrokerInStatus &last = workerInfo.getLastBrokerInStatus();
        EXPECT_EQ(currentTime - 181, last.updateTime);
    }
}

TEST_F(WorkerInfoTest, testIsZkDfsTimeout) {
    RoleInfo roleInfo("broker", "1.1.1.1", 1234, ROLE_TYPE_BROKER);
    WorkerInfo workerInfo(roleInfo);
    EXPECT_FALSE(workerInfo.isZkDfsTimeout(0, 0));

    BrokerInStatus status;
    status.zfsTimeout = 10;
    status.commitDelay = 20 * 1000 * 1000;
    workerInfo.addBrokerInMetric(status);
    EXPECT_EQ(1, workerInfo._brokerInMetrics.size());
    EXPECT_FALSE(workerInfo.isZkDfsTimeout(10, 20));
    EXPECT_TRUE(workerInfo.isZkDfsTimeout(9, 20));
    EXPECT_TRUE(workerInfo.isZkDfsTimeout(10, 19));
    EXPECT_TRUE(workerInfo.isZkDfsTimeout(9, 19));
}

} // namespace admin
} // namespace swift
