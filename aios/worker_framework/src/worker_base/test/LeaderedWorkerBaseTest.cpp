#ifndef WORKER_FRAMEWORK_LEADEREDWORKERBASETEST_H
#define WORKER_FRAMEWORK_LEADEREDWORKERBASETEST_H

#include "worker_framework/LeaderedWorkerBase.h"

#include "gmock/gmock.h"
#include <sys/socket.h>

#include "autil/CommandLineParameter.h"
#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "fslib/fslib.h"
#include "test/test.h"
#include "unittest/unittest.h"
#include "worker_base/test/MockLeaderElector.h"
#include "worker_base/test/MockLeaderedWorkerBase.h"
#include "worker_framework/PathUtil.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace testing;
using namespace cm_basic;
using namespace worker_framework;

namespace worker_framework {

class LeaderedWorkerBaseTest : public ::testing::Test {
protected:
    LeaderedWorkerBaseTest() {}

    virtual ~LeaderedWorkerBaseTest() {}

    virtual void SetUp() {
        _testDataPath = getTestDataPath();
        string defaultLogConfFile = _testDataPath + "/" + "worker_framework_alog.conf";
        FileSystem::remove(defaultLogConfFile);

        unsigned short port = fslib::fs::zookeeper::ZkServer::getZkServer()->port();
        ostringstream oss;
        oss << "zfs://127.0.0.1:" << port << "/LeaderedWorkerBaseTest";
        _zkPath = oss.str();
        _zk = new ZkWrapper(_zkPath, 10);
        _zk->open();
    }

    virtual void TearDown() { DELETE_AND_SET_NULL(_zk); }

protected:
    string _testDataPath;
    string _zkPath;
    ZkWrapper *_zk;

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(worker_framework.worker_base, LeaderedWorkerBaseTest);

TEST_F(LeaderedWorkerBaseTest, testInitLeaderElector) {
    MockLeaderedWorkerBase *worker1 = new MockLeaderedWorkerBase;
    // EXPECT_CALL(*worker1, becomeLeader());
    string logConfFile = _testDataPath + "/WorkerBaseTestLogger.conf";
    string commandStr = "./WorkerBase -l " + logConfFile + " -w " + _testDataPath;

    CommandLineParameter cmdLinePara(commandStr);
    EXPECT_TRUE(worker1->init(cmdLinePara.getArgc(), cmdLinePara.getArgv()));
    EXPECT_EQ(logConfFile, worker1->getLogConfFile());

    std::string retPath;
    ASSERT_TRUE(PathUtil::getCurrentPath(retPath));
    EXPECT_EQ(retPath, worker1->getCwdPath());

    EXPECT_TRUE(worker1->initLeaderElector(_zkPath));
    usleep(5 * 1000 * 1000);
    EXPECT_TRUE(worker1->isLeader());
    // ASSERT_EQ(worker1->getAddress(), worker1->getLeaderAddress());

    MockLeaderedWorkerBase *worker2 = new MockLeaderedWorkerBase;
    // EXPECT_CALL(*worker2, becomeLeader());
    EXPECT_TRUE(worker2->init(cmdLinePara.getArgc(), cmdLinePara.getArgv()));
    EXPECT_TRUE(worker1->getAddress() != worker2->getAddress());

    EXPECT_TRUE(worker2->initLeaderElector(_zkPath));
    usleep(5 * 1000 * 1000);
    EXPECT_FALSE(worker2->isLeader());
    // EXPECT_EQ(worker2->getLeaderAddress(), worker1->getAddress());

    DELETE_AND_SET_NULL(worker1);

    usleep(5 * 1000 * 1000);
    EXPECT_TRUE(worker2->isLeader());
    // EXPECT_EQ(worker2->getLeaderAddress(), worker2->getAddress());
    DELETE_AND_SET_NULL(worker2);
}

void handleSignalTest(int sig) { cout << "receive signal [" << sig << "], stop" << endl; }

TEST_F(LeaderedWorkerBaseTest, testDeadNode) {
    signal(SIGUSR1, handleSignalTest);
    setenv(LeaderElector::WF_PREEMPTIVE_LEADER.c_str(), "true", 1);

    MockLeaderedWorkerBase *worker1 = new MockLeaderedWorkerBase;
    string workDir1 = PathUtil::joinPath(_testDataPath, "worker1");
    string workDir2 = PathUtil::joinPath(_testDataPath, "worker2");

    fs::FileSystem::removeDir(workDir1);
    fs::FileSystem::removeDir(workDir2);

    string logConfFile = _testDataPath + "/WorkerBaseTestLogger.conf";
    string commandStr1 = "./WorkerBase -l " + logConfFile + " -w " + workDir1;
    string commandStr2 = "./WorkerBase -l " + logConfFile + " -w " + workDir2;

    CommandLineParameter cmdLinePara1(commandStr1);
    CommandLineParameter cmdLinePara2(commandStr2);

    EXPECT_TRUE(worker1->init(cmdLinePara1.getArgc(), cmdLinePara1.getArgv()));
    EXPECT_TRUE(worker1->initLeaderElector(_zkPath));
    while (!worker1->isLeader()) {
        usleep(5 * 1000 * 1000);
    }
    EXPECT_TRUE(worker1->isLeader());
    // worker1 become leader, dead node

    MockLeaderedWorkerBase *worker2 = new MockLeaderedWorkerBase;
    EXPECT_TRUE(worker2->init(cmdLinePara2.getArgc(), cmdLinePara2.getArgv()));
    EXPECT_TRUE(worker2->initLeaderElector(_zkPath));

    bool reached = false;
    while (!reached) {
        bool worker1Reach = worker1->isLeader();
        bool worker2Reach = worker2->isLeader();
        if (!worker1Reach && worker2Reach) {
            reached = true;
        }
        usleep(5 * 1000 * 1000);
    }
    // worker 2 become leader

    DELETE_AND_SET_NULL(worker2);
    while (worker1->isLeader()) {
        usleep(5 * 1000 * 1000);
    }
    // dead node can't work

    DELETE_AND_SET_NULL(worker1);

    unsetenv(LeaderElector::WF_PREEMPTIVE_LEADER.c_str());
}

TEST_F(LeaderedWorkerBaseTest, testNeedSlotIdInfo) {
    autil::EnvGuard guard("HIPPO_SLOT_ID", "100");
    MockLeaderedWorkerBase *worker = new MockLeaderedWorkerBase;
    EXPECT_CALL(*worker, needSlotIdInLeaderInfo).WillOnce(Return(true));
    auto leaderInfo = worker->getLeaderInfo();
    auto splitedLeaderInfo = autil::StringUtil::split(leaderInfo, "\n", false);
    ASSERT_EQ(3, splitedLeaderInfo.size());
    ASSERT_EQ("100", splitedLeaderInfo[2]);
    DELETE_AND_SET_NULL(worker);
}
}; // namespace worker_framework

#endif // WORKER_FRAMEWORK_LEADEREDWORKERBASETEST_H
