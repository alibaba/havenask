#include "swift/admin/SimpleMasterSchedulerLocal.h"

#include <ctime>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <iosfwd>
#include <map>
#include <set>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "master_framework/RolePlan.h"
#include "swift/admin/AppPlanMaker.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;

namespace swift {
namespace admin {

class MockSimpleMasterSchedulerLocal : public SimpleMasterSchedulerLocal {
public:
    MockSimpleMasterSchedulerLocal()
        : SimpleMasterSchedulerLocal("/swift_one/admin_0", "/swift_one/binary/usr/local/etc/swift/swift_alog.conf") {}
    MockSimpleMasterSchedulerLocal(const std::string &workDir, const std::string &logConfFile)
        : SimpleMasterSchedulerLocal(workDir, logConfFile) {}
    MOCK_METHOD1(startProcess, pid_t(const ProcessLauncherParam &param));
    MOCK_METHOD2(stopProcess, bool(pid_t pid, int));
    MOCK_METHOD1(existProcess, bool(pid_t pid));
    MOCK_METHOD1(isHang, bool(pid_t pid));
    MOCK_METHOD2(isAlive, bool(pid_t pid, const std::string &workerDir));
};

class SimpleMasterSchedulerLocalTest : public TESTBASE {
public:
    void setUp() override{};
    void tearDown() override{};

public:
    //    void checkPidFile(const string &pidFile, const map<string, pid_t> &);
};

TEST_F(SimpleMasterSchedulerLocalTest, testInit) {
    {
        SimpleMasterSchedulerLocal scheduler("/aa/bb", "/bb");
        ASSERT_FALSE(scheduler.init("hippoRoot", NULL, ""));
        ASSERT_FALSE(scheduler.init("hippoRoot", NULL, "appid"));
    }
    {
        SimpleMasterSchedulerLocal scheduler("binary/workDir/",
                                             "linshen/test/binary/usr/local/etc/swift/swift_alog.conf");
        ASSERT_TRUE(scheduler.init("hippoRoot", NULL, "appid"));
        ASSERT_EQ("binary/workDir/", scheduler._workDir);
        ASSERT_EQ("binary", scheduler._baseDir);
        ASSERT_EQ("linshen/test/binary/usr/local/etc/swift/swift_alog.conf", scheduler._logConfFile);
        ASSERT_EQ("linshen/test/binary", scheduler._binaryPath);
    }
}

TEST_F(SimpleMasterSchedulerLocalTest, testReadWritePids) {
    {
        string tmpPidFile = GET_TEMP_DATA_PATH() + "/pids1";
        map<string, pid_t> writeWorkerPids;
        ASSERT_TRUE(SimpleMasterSchedulerLocal::writeWorkerPids(tmpPidFile, writeWorkerPids));
        map<string, pid_t> readWorkerPids = SimpleMasterSchedulerLocal::readWorkerPids(tmpPidFile);
        ASSERT_EQ(writeWorkerPids, readWorkerPids);
    }
    {
        string tmpPidFile = GET_TEMP_DATA_PATH() + "/pids2";
        map<string, pid_t> writeWorkerPids = {{"aaa", 1}};
        ASSERT_TRUE(SimpleMasterSchedulerLocal::writeWorkerPids(tmpPidFile, writeWorkerPids));
        map<string, pid_t> readWorkerPids = SimpleMasterSchedulerLocal::readWorkerPids(tmpPidFile);
        ASSERT_EQ(writeWorkerPids, readWorkerPids);
    }
    {
        string tmpPidFile = GET_TEMP_DATA_PATH() + "/pids3";
        map<string, pid_t> writeWorkerPids = {{"aaa", 1}, {"bbb", 2}};
        ASSERT_TRUE(SimpleMasterSchedulerLocal::writeWorkerPids(tmpPidFile, writeWorkerPids));
        map<string, pid_t> readWorkerPids = SimpleMasterSchedulerLocal::readWorkerPids(tmpPidFile);
        ASSERT_EQ(writeWorkerPids, readWorkerPids);
    }
}

TEST_F(SimpleMasterSchedulerLocalTest, testGetBaseDir) {
    ASSERT_EQ("", SimpleMasterSchedulerLocal::getBaseDir(""));
    ASSERT_EQ("", SimpleMasterSchedulerLocal::getBaseDir("/"));
    ASSERT_EQ("/a", SimpleMasterSchedulerLocal::getBaseDir("/a/b"));
    ASSERT_EQ("/a", SimpleMasterSchedulerLocal::getBaseDir("/a/b/"));
    ASSERT_EQ("/a/b", SimpleMasterSchedulerLocal::getBaseDir("/a/b/c"));
}

TEST_F(SimpleMasterSchedulerLocalTest, testDiffBrokersAllwayAlive) {
    string tmpfilePath = GET_TEMP_DATA_PATH() + "/testSetAppPlan";
    MockSimpleMasterSchedulerLocal scheduler(tmpfilePath, tmpfilePath + "/binary/usr/local/etc/swift/swift_alog.conf");
    EXPECT_CALL(scheduler, isAlive(_, _)).WillRepeatedly(Return(true));
    {
        map<string, pid_t> brokerPids = {{"a", 1}, {"b", 2}, {"c", 3}};
        map<string, ProcessLauncherParam> brokerLaunchMap = {{"a", {}}, {"b", {}}, {"c", {}}};
        map<string, pid_t> toKeep, toStop;
        set<string> toStart;
        scheduler.diffWorkers(brokerLaunchMap, brokerPids, toKeep, toStop, toStart);
        ASSERT_EQ(toKeep, brokerPids);
        ASSERT_TRUE(toStop.empty());
        ASSERT_TRUE(toStart.empty());
    }
    {
        map<string, pid_t> brokerPids = {{"a", 1}, {"b", 2}};
        map<string, ProcessLauncherParam> brokerLaunchMap = {{"a", {}}, {"b", {}}, {"c", {}}};
        map<string, pid_t> toKeep, toStop;
        set<string> toStart;
        scheduler.diffWorkers(brokerLaunchMap, brokerPids, toKeep, toStop, toStart);
        ASSERT_EQ(toKeep, brokerPids);
        ASSERT_TRUE(toStop.empty());
        set<string> expectStart = {"c"};
        ASSERT_EQ(expectStart, toStart);
    }
    {
        map<string, pid_t> brokerPids = {{"a", 1}, {"b", 2}};
        map<string, ProcessLauncherParam> brokerLaunchMap = {{"b", {}}, {"c", {}}};
        map<string, pid_t> toKeep, toStop;
        set<string> toStart;
        scheduler.diffWorkers(brokerLaunchMap, brokerPids, toKeep, toStop, toStart);
        map<string, pid_t> expectToKeep = {{"b", 2}};
        ASSERT_EQ(expectToKeep, toKeep);
        set<string> expectStart = {"c"};
        ASSERT_EQ(expectStart, toStart);
        map<string, pid_t> expectToStop = {{"a", 1}};
        ASSERT_EQ(expectToStop, toStop);
    }
}
TEST_F(SimpleMasterSchedulerLocalTest, testDiffBrokersAllwayFalseAlive) {
    string tmpfilePath = GET_TEMP_DATA_PATH() + "/testSetAppPlan";
    MockSimpleMasterSchedulerLocal scheduler(tmpfilePath, tmpfilePath + "/binary/usr/local/etc/swift/swift_alog.conf");
    EXPECT_CALL(scheduler, isAlive(_, _)).WillRepeatedly(Return(false));
    {
        map<string, pid_t> brokerPids = {{"a", 1}, {"b", 2}};
        map<string, ProcessLauncherParam> brokerLaunchMap = {{"a", {}}, {"b", {}}};
        map<string, pid_t> toKeep, toStop;
        set<string> toStart;
        scheduler.diffWorkers(brokerLaunchMap, brokerPids, toKeep, toStop, toStart);
        ASSERT_TRUE(toKeep.empty());
        ASSERT_TRUE(toStop.empty());
        set<string> expectStart = {"a", "b"};
        ASSERT_EQ(expectStart, toStart);
    }
}

TEST_F(SimpleMasterSchedulerLocalTest, testSetAppPlan) {
    string tmpfilePath = GET_TEMP_DATA_PATH() + "/testSetAppPlan";
    MockSimpleMasterSchedulerLocal scheduler(tmpfilePath, tmpfilePath + "/binary/usr/local/etc/swift/swift_alog.conf");
    ASSERT_TRUE(scheduler.init("app", NULL, "test_aa"));
    AppPlanMaker maker1;
    string configPath = GET_TEST_DATA_PATH() + "app_plan_maker_test/normal_conf";
    maker1.init(configPath, "0");
    AppPlan plan;
    maker1.makeBrokerRolePlan(plan);

    EXPECT_CALL(scheduler, isAlive(_, _)).WillRepeatedly(Return(true));
    // 1. first update
    EXPECT_CALL(scheduler, startProcess(_))
        .WillOnce(Return(1232))
        .WillOnce(Return(1234))
        .WillOnce(Return(1236))
        .WillOnce(Return(1238));

    EXPECT_CALL(scheduler, stopProcess(_, _)).Times(0);
    scheduler.setAppPlan(plan);

    auto roleStatus = scheduler.readWorkerPids(scheduler._pidFile);
    EXPECT_EQ(4, roleStatus.size());
    uint32_t step = 0;
    for (auto &role : roleStatus) {
        EXPECT_EQ(StringUtil::formatString("default##broker_%d_0", step), role.first);
        EXPECT_EQ(1232 + step * 2, role.second);
        step++;
    }
    // 2. update again
    EXPECT_CALL(scheduler, startProcess(_)).Times(0);
    EXPECT_CALL(scheduler, stopProcess(_, _)).Times(0);
    scheduler.setAppPlan(plan);

    roleStatus = scheduler.readWorkerPids(scheduler._pidFile);
    EXPECT_EQ(4, roleStatus.size());
    step = 0;
    for (auto &role : roleStatus) {
        EXPECT_EQ(StringUtil::formatString("default##broker_%d_0", step), role.first);
        EXPECT_EQ(1232 + step * 2, role.second);
        step++;
    }

    // 3. upgrade
    AppPlanMaker maker2;
    maker2.init(GET_TEST_DATA_PATH() + "app_plan_maker_test/normal_conf", "1");
    AppPlan targetPlan;
    maker2.makeBrokerRolePlan(targetPlan);
    plan.rolePlans.insert(targetPlan.rolePlans.begin(), targetPlan.rolePlans.end());
    EXPECT_CALL(scheduler, startProcess(_))
        .WillOnce(Return(1233))
        .WillOnce(Return(1235))
        .WillOnce(Return(1237))
        .WillOnce(Return(1239));

    EXPECT_CALL(scheduler, stopProcess(_, _)).Times(0);
    scheduler.setAppPlan(plan);

    roleStatus = scheduler.readWorkerPids(scheduler._pidFile);
    EXPECT_EQ(8, roleStatus.size());
    step = 0;
    for (auto &role : roleStatus) {
        EXPECT_EQ(StringUtil::formatString("default##broker_%d_%d", step / 2, step % 2), role.first);
        EXPECT_EQ(1232 + step, role.second);
        ++step;
    }

    // 4. stop
    plan.rolePlans.erase("default##broker_0_0");
    plan.rolePlans.erase("default##broker_1_0");
    plan.rolePlans.erase("default##broker_2_0");
    plan.rolePlans.erase("default##broker_3_0");
    EXPECT_CALL(scheduler, startProcess(_)).Times(0);
    EXPECT_CALL(scheduler, stopProcess(roleStatus["default##broker_0_0"], _)).WillOnce(Return(true));
    EXPECT_CALL(scheduler, stopProcess(roleStatus["default##broker_1_0"], _)).WillOnce(Return(true));
    EXPECT_CALL(scheduler, stopProcess(roleStatus["default##broker_2_0"], _)).WillOnce(Return(true));
    EXPECT_CALL(scheduler, stopProcess(roleStatus["default##broker_3_0"], _)).WillOnce(Return(true));

    scheduler.setAppPlan(plan);
    roleStatus = scheduler.readWorkerPids(scheduler._pidFile);
    EXPECT_EQ(4, roleStatus.size());
    step = 0;
    for (auto &role : roleStatus) {
        EXPECT_EQ(StringUtil::formatString("default##broker_%d_1", step), role.first);
        EXPECT_EQ(1233 + step * 2, role.second);
        ++step;
    }
}

}; // namespace admin
}; // namespace swift
