#include "swift/admin/WorkerTable.h"

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <ostream>
#include <stdint.h>
#include <string>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/File.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/admin/AdminZkDataAccessor.h"
#include "swift/admin/RoleInfo.h"
#include "swift/admin/WorkerInfo.h"
#include "swift/admin/test/MessageConstructor.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "unittest/unittest.h"

using namespace fslib::fs;

namespace swift {
namespace admin {

class WorkerTableTest : public TESTBASE {
public:
    WorkerTableTest();
    ~WorkerTableTest();

public:
    void setUp();
    void tearDown();

protected:
    cm_basic::ZkWrapper *_zkWrapper;
    zookeeper::ZkServer *_zkServer;
    std::string _zkRoot;
    AdminZkDataAccessorPtr _dataAccessor;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, WorkerTableTest);

using namespace std;
using namespace swift::protocol;
WorkerTableTest::WorkerTableTest() : _zkWrapper(NULL) {}

WorkerTableTest::~WorkerTableTest() {}

void WorkerTableTest::setUp() {
    AUTIL_LOG(DEBUG, "setUp!");
    _zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << _zkServer->port();
    _zkRoot = "zfs://" + oss.str() + "/";
    _zkWrapper = new cm_basic::ZkWrapper(oss.str(), 1000);
    ASSERT_TRUE(_zkWrapper->open());
    _dataAccessor.reset(new AdminZkDataAccessor());
    ASSERT_TRUE(_dataAccessor->init(_zkWrapper, _zkRoot));
}

void WorkerTableTest::tearDown() {
    AUTIL_LOG(DEBUG, "tearDown!");
    DELETE_AND_SET_NULL(_zkWrapper);
}

TEST_F(WorkerTableTest, testUpdateWorker_No_DataAccessor) {
    WorkerTable workerTable;
    string address = "role_1#_#10.125.224.30:1234";
    HeartbeatInfo heartbeat = MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true, "topic_1", 0);
    ASSERT_FALSE(workerTable.updateWorker(heartbeat));
}

TEST_F(WorkerTableTest, testUpdateWorker_Bad_address) {
    {
        WorkerTable workerTable;
        string address = "10.125.224.30:1234";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true, "topic_1", 0);
        workerTable.setDataAccessor(_dataAccessor);
        ASSERT_FALSE(workerTable.updateWorker(heartbeat));
    }
    {
        WorkerTable workerTable;
        string address = "role_1#_#10.125.224.30";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true, "topic_1", 0);
        workerTable.setDataAccessor(_dataAccessor);
        ASSERT_FALSE(workerTable.updateWorker(heartbeat));
    }
    {
        WorkerTable workerTable;
        string address = "role_1#_#10.125.224.30:0";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true, "topic_1", 0);
        workerTable.setDataAccessor(_dataAccessor);
        ASSERT_FALSE(workerTable.updateWorker(heartbeat));
    }
    {
        WorkerTable workerTable;
        string address = "role_1#_#:10";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true, "topic_1", 0);
        workerTable.setDataAccessor(_dataAccessor);
        ASSERT_FALSE(workerTable.updateWorker(heartbeat));
    }
}

TEST_F(WorkerTableTest, testUpdateWorker_getLeaderAddreesFailed) {
    { // first heart beat
        string address = "role_1#_#10.125.224.30:1234";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true, "topic_1", 0);
        WorkerTable workerTable;
        workerTable.setDataAccessor(_dataAccessor);
        ASSERT_TRUE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(size_t(1), workerTable.getBrokerWorkers().size());
        ASSERT_EQ(string("role_1#_#10.125.224.30:1234"),
                  workerTable.getBrokerWorkers()["role_1"]->getTargetRoleAddress());
        ASSERT_EQ(string("role_1#_#10.125.224.30:1234"),
                  workerTable.getBrokerWorkers()["role_1"]->getCurrentRoleAddress());
        ASSERT_EQ(string(""), workerTable.getBrokerWorkers()["role_1"]->getLeaderAddress());
    }
    { // work info has exist but not address info
        WorkerTable workerTable;
        workerTable.setDataAccessor(_dataAccessor);
        RoleInfo roleInfo("role_1", "", 0);
        WorkerInfoPtr workerInfo(new WorkerInfo(roleInfo));
        workerTable._brokerWorkerMap["role_1"] = workerInfo;

        string address = "role_1#_#10.125.224.30:1234";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true, "topic_1", 1);
        ASSERT_TRUE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(address, workerInfo->getCurrentRoleAddress());
        ASSERT_EQ(uint16_t(1234), workerInfo->getRoleInfo().port);
        ASSERT_EQ(string("10.125.224.30"), workerInfo->getRoleInfo().ip);
        ASSERT_EQ(string(""), workerTable.getBrokerWorkers()["role_1"]->getLeaderAddress());
        // port changed
        string address2 = "role_1#_#10.125.224.30:1233";
        heartbeat = MessageConstructor::ConstructHeartbeatInfo(address2, ROLE_TYPE_BROKER, true, "topic_1", 1);
        ASSERT_TRUE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(address2, workerInfo->getCurrentRoleAddress());
        ASSERT_EQ(uint16_t(1233), workerInfo->getRoleInfo().port);
        ASSERT_EQ(string("10.125.224.30"), workerInfo->getRoleInfo().ip);
        ASSERT_EQ(string(""), workerTable.getBrokerWorkers()["role_1"]->getLeaderAddress());
        // ip changed
        string address3 = "role_1#_#10.125.224.31:1233";
        heartbeat = MessageConstructor::ConstructHeartbeatInfo(address3, ROLE_TYPE_BROKER, true, "topic_1", 1);
        ASSERT_TRUE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(address3, workerInfo->getCurrentRoleAddress());
        ASSERT_EQ(uint16_t(1233), workerInfo->getRoleInfo().port);
        ASSERT_EQ(string("10.125.224.31"), workerInfo->getRoleInfo().ip);
        ASSERT_EQ(string(""), workerTable.getBrokerWorkers()["role_1"]->getLeaderAddress());
        // 224.30 is dead info come, port is same as current
        string address4 = "role_1#_#10.125.224.30:1233";
        heartbeat = MessageConstructor::ConstructHeartbeatInfo(address4, ROLE_TYPE_BROKER, false, "topic_1", 1);
        ASSERT_FALSE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(address3, workerInfo->getCurrentRoleAddress());
        ASSERT_EQ(uint16_t(1233), workerInfo->getRoleInfo().port);
        ASSERT_EQ(string("10.125.224.31"), workerInfo->getRoleInfo().ip);
        ASSERT_EQ(string(""), workerTable.getBrokerWorkers()["role_1"]->getLeaderAddress());

        // 224.31 is dead info come, port is diff
        string address5 = "role_1#_#10.125.224.31:1232";
        heartbeat = MessageConstructor::ConstructHeartbeatInfo(address5, ROLE_TYPE_BROKER, false, "topic_1", 1);
        ASSERT_FALSE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(address3, workerInfo->getCurrentRoleAddress());
        ASSERT_EQ(uint16_t(1233), workerInfo->getRoleInfo().port);
        ASSERT_EQ(string("10.125.224.31"), workerInfo->getRoleInfo().ip);
        ASSERT_EQ(string(""), workerTable.getBrokerWorkers()["role_1"]->getLeaderAddress());
        // 224.31184 is dead info come
        heartbeat = MessageConstructor::ConstructHeartbeatInfo(address3, ROLE_TYPE_BROKER, false, "topic_1", 1);
        ASSERT_TRUE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(address3, workerInfo->getCurrentRoleAddress());
        ASSERT_EQ(uint16_t(1233), workerInfo->getRoleInfo().port);
        ASSERT_EQ(string("10.125.224.31"), workerInfo->getRoleInfo().ip);
        ASSERT_EQ(string(""), workerTable.getBrokerWorkers()["role_1"]->getLeaderAddress());
    }
}

TEST_F(WorkerTableTest, testUpdateWorker_getLeaderAddreesSuccess) {
    { // first heart beat  leader address equal heartbeat
        string address = "broker_1#_#10.125.224.30:1234";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true, "topic_1", 0);
        WorkerTable workerTable;
        workerTable.setDataAccessor(_dataAccessor);
        ASSERT_TRUE(_dataAccessor->writeLeaderAddress("broker_1", "10.125.224.30:1234"));
        ASSERT_TRUE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(size_t(1), workerTable.getBrokerWorkers().size());
        ASSERT_EQ(string("broker_1#_#10.125.224.30:1234"),
                  workerTable.getBrokerWorkers()["broker_1"]->getTargetRoleAddress());
        ASSERT_EQ(string("broker_1#_#10.125.224.30:1234"),
                  workerTable.getBrokerWorkers()["broker_1"]->getCurrentRoleAddress());
        ASSERT_EQ(string("10.125.224.30:1234"), workerTable.getBrokerWorkers()["broker_1"]->getLeaderAddress());
        ASSERT_EQ(uint16_t(1234), workerTable.getBrokerWorkers()["broker_1"]->getRoleInfo().port);
    }
    { // first heart beat  leader address not equal heartbeat
        string address = "broker_2#_#10.125.224.30:1234";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true, "topic_1", 0);
        WorkerTable workerTable;
        workerTable.setDataAccessor(_dataAccessor);
        ASSERT_TRUE(_dataAccessor->writeLeaderAddress("broker_2", "10.125.224.30:1233"));
        ASSERT_TRUE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(size_t(1), workerTable.getBrokerWorkers().size());
        ASSERT_EQ(string("broker_2#_#10.125.224.30:1233"),
                  workerTable.getBrokerWorkers()["broker_2"]->getTargetRoleAddress());
        ASSERT_EQ(string(""), workerTable.getBrokerWorkers()["broker_2"]->getCurrentRoleAddress());
        ASSERT_EQ(string("10.125.224.30:1233"), workerTable.getBrokerWorkers()["broker_2"]->getLeaderAddress());
        ASSERT_EQ(uint16_t(1233), workerTable.getBrokerWorkers()["broker_2"]->getRoleInfo().port);
    }

    { // work info has exist, and address equal heartbeat address
        // add heartbeat
        WorkerTable workerTable;
        workerTable.setDataAccessor(_dataAccessor);
        RoleInfo roleInfo("broker_3", "", 0);
        WorkerInfoPtr workerInfo(new WorkerInfo(roleInfo));
        workerTable._brokerWorkerMap["broker_3"] = workerInfo;

        string address = "broker_3#_#10.125.224.30:1234";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true, "topic_1", 1);
        ASSERT_TRUE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(address, workerInfo->getCurrentRoleAddress());
        ASSERT_EQ(uint16_t(1234), workerInfo->getRoleInfo().port);
        ASSERT_EQ(string("10.125.224.30"), workerInfo->getRoleInfo().ip);
        ASSERT_EQ(string(""), workerTable.getBrokerWorkers()["broker_3"]->getLeaderAddress());

        // has heartbeat not equal  leader address
        string address2 = "broker_3#_#10.125.224.30:1233";
        ASSERT_TRUE(_dataAccessor->writeLeaderAddress("broker_3", "10.125.224.30:1233"));

        ASSERT_FALSE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(uint16_t(1234), workerInfo->getRoleInfo().port);
        ASSERT_EQ(string("10.125.224.30"), workerInfo->getRoleInfo().ip);
        ASSERT_EQ(string("10.125.224.30:1233"), workerTable.getBrokerWorkers()["broker_3"]->getLeaderAddress());
        // has heartbeat equal  leader address
        HeartbeatInfo heartbeat2 =
            MessageConstructor::ConstructHeartbeatInfo(address2, ROLE_TYPE_BROKER, true, "topic_1", 1);
        ASSERT_TRUE(workerTable.updateWorker(heartbeat2));
        ASSERT_EQ(uint16_t(1233), workerInfo->getRoleInfo().port);
        ASSERT_EQ(string("10.125.224.30"), workerInfo->getRoleInfo().ip);
        ASSERT_EQ(string("10.125.224.30:1233"), workerTable.getBrokerWorkers()["broker_3"]->getLeaderAddress());
    }
}

TEST_F(WorkerTableTest, testUpdateWorker_adminNotReadLeader) {
    // first follower heart beat leader address equal heartbeat
    string heartAddress = "admin#_#10.1.1.1:1111";
    string leaderAddress = "10.2.2.2:2222";
    HeartbeatInfo heartbeat;
    heartbeat.set_address(heartAddress);
    heartbeat.set_role(ROLE_TYPE_ADMIN);
    heartbeat.set_alive(true);
    WorkerTable workerTable;
    workerTable.setDataAccessor(_dataAccessor);
    ASSERT_TRUE(_dataAccessor->writeLeaderAddress(heartAddress, leaderAddress));
    ASSERT_TRUE(workerTable.updateWorker(heartbeat));
    ASSERT_EQ(size_t(1), workerTable.getAdminWorkers().size());
    auto workerInfo = workerTable.getAdminWorkers()[heartAddress];
    EXPECT_EQ(heartAddress, workerInfo->getCurrentRoleAddress());
    EXPECT_EQ(leaderAddress, workerInfo->getLeaderAddress());
    EXPECT_TRUE(workerInfo->_current.alive());

    // follower dead;
    heartbeat.set_alive(false);
    ASSERT_TRUE(workerTable.updateWorker(heartbeat));
    EXPECT_EQ(heartAddress, workerInfo->getCurrentRoleAddress());
    EXPECT_EQ(leaderAddress, workerInfo->getLeaderAddress());
    EXPECT_FALSE(workerInfo->_current.alive());
}

TEST_F(WorkerTableTest, testUpdateWorker_RoleInfo_Init) {
    { // refuse ROLE_TYPE_NONE
        string address = "role_1#_#10.125.224.30:1234";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_NONE, true, "topic_1", 0);
        WorkerTable workerTable;
        workerTable.setDataAccessor(_dataAccessor);
        ASSERT_FALSE(workerTable.updateWorker(heartbeat));
    }
    { // refuse ROLE_TYPE_ALL
        string address = "role_1#_#10.125.224.30:1234";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_ALL, true, "topic_1", 0);
        WorkerTable workerTable;
        workerTable.setDataAccessor(_dataAccessor);
        ASSERT_FALSE(workerTable.updateWorker(heartbeat));
    }
    { // first heart beat broker
        string address = "role_1#_#10.125.224.30:1234";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true, "topic_1", 0);
        WorkerTable workerTable;
        workerTable.setDataAccessor(_dataAccessor);
        ASSERT_TRUE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(ROLE_TYPE_BROKER, workerTable.getBrokerWorkers()["role_1"]->getRoleInfo().roleType);
    }
    { // first heart beat admin
        string address = "role_1#_#10.125.224.30:1234";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_ADMIN, true, "topic_1", 0);
        WorkerTable workerTable;
        workerTable.setDataAccessor(_dataAccessor);
        ASSERT_TRUE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(ROLE_TYPE_ADMIN,
                  workerTable.getAdminWorkers()["role_1#_#10.125.224.30:1234"]->getRoleInfo().roleType);
    }
    { // first heart beat broker and not leader
        string address = "broker_1#_#10.125.224.30:1234";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_BROKER, true, "topic_1", 0);
        WorkerTable workerTable;
        workerTable.setDataAccessor(_dataAccessor);
        ASSERT_TRUE(_dataAccessor->writeLeaderAddress("broker_1", "10.125.224.31:1233"));
        ASSERT_TRUE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(ROLE_TYPE_BROKER, workerTable.getBrokerWorkers()["broker_1"]->getRoleInfo().roleType);
    }
    { // first heart beat admin and not leader
        string address = "admin_1#_#10.125.224.30:1234";
        HeartbeatInfo heartbeat =
            MessageConstructor::ConstructHeartbeatInfo(address, ROLE_TYPE_ADMIN, true, "topic_1", 0);
        WorkerTable workerTable;
        workerTable.setDataAccessor(_dataAccessor);
        ASSERT_TRUE(_dataAccessor->writeLeaderAddress("admin_1#_#10.125.224.30:1234", "10.125.224.31:1233"));
        ASSERT_TRUE(workerTable.updateWorker(heartbeat));
        ASSERT_EQ(ROLE_TYPE_ADMIN,
                  workerTable.getAdminWorkers()["admin_1#_#10.125.224.30:1234"]->getRoleInfo().roleType);
    }
}

TEST_F(WorkerTableTest, testUpdateBrokerRoles) {
    WorkerTable workerTable;
    string role1 = "role1";
    RoleInfo roleInfo1(role1, "", 0);
    WorkerInfoPtr workerInfoPtr1(new WorkerInfo(roleInfo1));
    workerTable._brokerWorkerMap[role1] = workerInfoPtr1;

    string role2 = "role2";
    RoleInfo roleInfo2(role2, "1234", 2);
    WorkerInfoPtr workerInfoPtr2(new WorkerInfo(roleInfo2));
    workerTable._brokerWorkerMap[role2] = workerInfoPtr2;

    string role3 = "role3";
    string role4 = "role4";
    vector<string> roleNames;
    roleNames.push_back(role2);
    roleNames.push_back(role3);
    roleNames.push_back(role4);
    ASSERT_EQ(size_t(2), workerTable.getBrokerWorkers().size());
    workerTable.updateBrokerRoles(roleNames);
    ASSERT_EQ(size_t(3), workerTable.getBrokerWorkers().size());

    ASSERT_EQ(role2, workerTable.getBrokerWorkers()[role2]->getRoleInfo().roleName);
    ASSERT_EQ(string("1234"), workerTable.getBrokerWorkers()[role2]->getRoleInfo().ip);
    ASSERT_EQ(uint16_t(2), workerTable.getBrokerWorkers()[role2]->getRoleInfo().port);

    ASSERT_EQ(role3, workerTable.getBrokerWorkers()[role3]->getRoleInfo().roleName);
    ASSERT_EQ(string(""), workerTable.getBrokerWorkers()[role3]->getRoleInfo().ip);
    ASSERT_EQ(uint16_t(0), workerTable.getBrokerWorkers()[role3]->getRoleInfo().port);

    ASSERT_EQ(role4, workerTable.getBrokerWorkers()[role4]->getRoleInfo().roleName);
    ASSERT_EQ(string(""), workerTable.getBrokerWorkers()[role4]->getRoleInfo().ip);
    ASSERT_EQ(uint16_t(0), workerTable.getBrokerWorkers()[role4]->getRoleInfo().port);
}

TEST_F(WorkerTableTest, testUpdateAdminRoles) {
    WorkerTable workerTable;
    string role1 = "role1";
    RoleInfo roleInfo1(role1, "", 0);
    WorkerInfoPtr workerInfoPtr1(new WorkerInfo(roleInfo1));
    workerInfoPtr1->_workerStatus = ROLE_DS_DEAD;
    workerTable._adminWorkerMap[role1] = workerInfoPtr1;

    string role2 = "role2";
    RoleInfo roleInfo2(role2, "1234", 2);
    WorkerInfoPtr workerInfoPtr2(new WorkerInfo(roleInfo2));
    workerTable._adminWorkerMap[role2] = workerInfoPtr2;
    ASSERT_EQ(size_t(2), workerTable.getAdminWorkers().size());
    workerTable.updateAdminRoles();
    ASSERT_EQ(size_t(1), workerTable.getAdminWorkers().size());
    ASSERT_EQ(role2, workerTable.getAdminWorkers()[role2]->getRoleInfo().roleName);
    ASSERT_EQ(string("1234"), workerTable.getAdminWorkers()[role2]->getRoleInfo().ip);
    ASSERT_EQ(uint16_t(2), workerTable.getAdminWorkers()[role2]->getRoleInfo().port);
}

TEST_F(WorkerTableTest, testClear) {
    WorkerTable table;
    table._brokerWorkerMap["role_1"] = WorkerInfoPtr(new WorkerInfo(RoleInfo()));
    ASSERT_EQ(size_t(1), table.getBrokerWorkers().size());
    table.clear();
    ASSERT_EQ(size_t(0), table.getBrokerWorkers().size());
}

TEST_F(WorkerTableTest, testFindErrorBrokers) {
    WorkerTable table;
    int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
    WorkerInfoPtr role1wi(new WorkerInfo(RoleInfo("role_1", "127.1.1.0", 123)));
    HeartbeatInfo hb1;
    hb1.set_address("default##role_1#_#127.2.2.2:1234");
    hb1.set_sessionid((currentTime - 200) * 1000000);
    role1wi->setCurrent(hb1);
    WorkerInfoPtr role2wi(new WorkerInfo(RoleInfo("role_2", "128.1.1.0", 123)));
    HeartbeatInfo hb2;
    hb1.set_address("default##role_2#_#128.2.2.2:2345");
    hb1.set_sessionid(currentTime * 1000000);
    role2wi->setCurrent(hb1);
    table._brokerWorkerMap["role_1"] = role1wi;
    table._brokerWorkerMap["role_2"] = role2wi;

    vector<string> zombieWorkers, timeoutWorkers;
    uint32_t deadBrokerTimeoutSec = 180;
    int64_t zfsTimeout = 5 * 1000 * 1000;
    int64_t commitDelayThresholdInSec = 10;

    EXPECT_EQ(0,
              table.findErrorBrokers(
                  zombieWorkers, timeoutWorkers, deadBrokerTimeoutSec, zfsTimeout, commitDelayThresholdInSec));
    EXPECT_EQ(0, zombieWorkers.size());

    BrokerInStatus status;
    status.updateTime = currentTime - 10;
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
    status.zfsTimeout = 5000000;
    status.commitDelay = 10000000;
    EXPECT_TRUE(table.addBrokerInMetric("role_1", status));
    EXPECT_FALSE(table.addBrokerInMetric("role_3", status));
    EXPECT_EQ(0,
              table.findErrorBrokers(
                  zombieWorkers, timeoutWorkers, deadBrokerTimeoutSec, zfsTimeout, commitDelayThresholdInSec));
    EXPECT_EQ(0, zombieWorkers.size());

    zombieWorkers.clear();
    status.updateTime = currentTime;
    status.zfsTimeout = 5000001;
    status.commitDelay = 10000000;
    EXPECT_TRUE(table.addBrokerInMetric("role_1", status));
    EXPECT_EQ(1,
              table.findErrorBrokers(
                  zombieWorkers, timeoutWorkers, deadBrokerTimeoutSec, zfsTimeout, commitDelayThresholdInSec));
    EXPECT_EQ(0, zombieWorkers.size());
    EXPECT_EQ(1, timeoutWorkers.size());
    EXPECT_EQ("default##role_1#_#127.2.2.2:1234", timeoutWorkers[0]);

    timeoutWorkers.clear();
    status.updateTime = currentTime;
    status.zfsTimeout = 5000000;
    status.commitDelay = 10000001;
    EXPECT_TRUE(table.addBrokerInMetric("role_1", status));
    EXPECT_EQ(1,
              table.findErrorBrokers(
                  zombieWorkers, timeoutWorkers, deadBrokerTimeoutSec, zfsTimeout, commitDelayThresholdInSec));
    EXPECT_EQ(0, zombieWorkers.size());
    EXPECT_EQ(1, timeoutWorkers.size());
    EXPECT_EQ("default##role_1#_#127.2.2.2:1234", timeoutWorkers[0]);

    timeoutWorkers.clear();
    status.updateTime = currentTime - 181;
    status.zfsTimeout = 5000001;
    status.commitDelay = 10000000;
    EXPECT_TRUE(table.addBrokerInMetric("role_1", status));
    table._brokerWorkerMap["role_1"]->_startTimeSec = status.updateTime;
    EXPECT_EQ(1,
              table.findErrorBrokers(
                  zombieWorkers, timeoutWorkers, deadBrokerTimeoutSec, zfsTimeout, commitDelayThresholdInSec));
    EXPECT_EQ(1, zombieWorkers.size());
    EXPECT_EQ(1, timeoutWorkers.size());
    EXPECT_EQ("default##role_1#_#127.2.2.2:1234", zombieWorkers[0]);
    EXPECT_EQ("default##role_1#_#127.2.2.2:1234", timeoutWorkers[0]);

    zombieWorkers.clear();
    timeoutWorkers.clear();
    EXPECT_TRUE(table.addBrokerInMetric("role_2", status));
    status.updateTime = currentTime;
    status.zfsTimeout = 5000001;
    status.commitDelay = 10000000;
    EXPECT_TRUE(table.addBrokerInMetric("role_2", status));

    status.updateTime = currentTime - 181;
    status.zfsTimeout = 5000000;
    status.commitDelay = 10000000;
    EXPECT_TRUE(table.addBrokerInMetric("role_1", status));
    EXPECT_EQ(2,
              table.findErrorBrokers(
                  zombieWorkers, timeoutWorkers, deadBrokerTimeoutSec, zfsTimeout, commitDelayThresholdInSec));
    EXPECT_EQ(1, zombieWorkers.size());
    EXPECT_EQ(2, timeoutWorkers.size());
    EXPECT_EQ("default##role_1#_#127.2.2.2:1234", zombieWorkers[0]);
    EXPECT_EQ("default##role_1#_#127.2.2.2:1234", timeoutWorkers[0]);
    EXPECT_EQ("default##role_2#_#128.2.2.2:2345", timeoutWorkers[1]);
}

} // namespace admin
} // namespace swift
