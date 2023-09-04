#ifndef WORKER_FRAMEWORK_LEADERLOCATORTEST_H
#define WORKER_FRAMEWORK_LEADERLOCATORTEST_H

#include "worker_framework/LeaderLocator.h"

#include "autil/Log.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "unittest/unittest.h"
#include "worker_base/test/MockLeaderLocator.h"
#include "worker_framework/LeaderChecker.h"

using namespace std;
namespace worker_framework {

#define LEADERLOCATOR_TEST_PATH "/leaderlocator_test"
#define LEADERLOCATOR_TEST_BASE_PATH LEADERLOCATOR_TEST_PATH "/basepath"
#define LEADERLOCATOR_TIMEOUT 1000000
#define LEADERLOCATOR_BASENAME "worker_framework_leader_elector_"

class LeaderLocatorTest : public ::testing::Test {
protected:
    LeaderLocatorTest() {}

    virtual ~LeaderLocatorTest() {}

    virtual void SetUp() {
        unsigned short port = fslib::fs::zookeeper::ZkServer::getZkServer()->port();
        std::ostringstream oss;
        oss << "127.0.0.1:" << port;
        _host = oss.str();
        _zk = new cm_basic::ZkWrapper(_host, 10000);
        ASSERT_TRUE(_zk->open());
        ASSERT_TRUE(_zk->remove(LEADERLOCATOR_TEST_PATH));

        _zk2 = new cm_basic::ZkWrapper(_host, 10000);
        ASSERT_TRUE(_zk2->open());

        _zk3 = new cm_basic::ZkWrapper(_host, 10000);
        ASSERT_TRUE(_zk3->open());
    }

    virtual void TearDown() {
        delete _zk;
        delete _zk2;
        delete _zk3;
    }

protected:
    cm_basic::ZkWrapper *_zk;
    cm_basic::ZkWrapper *_zk2;
    cm_basic::ZkWrapper *_zk3;
    std::string _host;
};

TEST_F(LeaderLocatorTest, testInit) {
    MockLeaderLocator mockLeaderLocator;

    mockLeaderLocator.init_P(_zk, LEADERLOCATOR_TEST_BASE_PATH, LEADERLOCATOR_BASENAME);
    ASSERT_TRUE(mockLeaderLocator.getZkWrapper() == _zk);
}

TEST_F(LeaderLocatorTest, testGetLine) {
    LeaderLocator locator;
    string origin = "first line\nsecond line\nthird line";
    EXPECT_EQ("first line", locator.getLine(origin, 1));
    EXPECT_EQ("second line", locator.getLine(origin, 2));
    EXPECT_EQ("third line", locator.getLine(origin, 3));
    EXPECT_EQ("", locator.getLine(origin, 4));
    EXPECT_EQ("", locator.getLine(origin, 5));
}

TEST_F(LeaderLocatorTest, testDoGetLeaderAddr) {
    LeaderLocator leaderLocator;
    leaderLocator.init(_zk, LEADERLOCATOR_TEST_BASE_PATH, LEADERLOCATOR_BASENAME);
    std::string path = std::string(LEADERLOCATOR_TEST_BASE_PATH) + "/" + LEADERLOCATOR_BASENAME;
    std::string illegalPath = std::string(LEADERLOCATOR_TEST_BASE_PATH) + "/a_" + LEADERLOCATOR_BASENAME;

    EXPECT_EQ("unknown", leaderLocator.doGetLeaderAddr());

    _zk->touch(illegalPath + "_0", "illegal_addr");
    EXPECT_EQ("unknown", leaderLocator.doGetLeaderAddr());

    _zk->touch(path + "_2", "addr2");
    EXPECT_EQ("addr2", leaderLocator.doGetLeaderAddr());

    _zk->touch(path + "_1", "addr1");
    EXPECT_EQ("addr1", leaderLocator.doGetLeaderAddr());
}

TEST_F(LeaderLocatorTest, testGetLeaderAddrOneLeaderOneFollower) {
    ASSERT_TRUE(_zk->remove(LEADERLOCATOR_TEST_PATH));

    LeaderElector leaderElector(_zk, LEADERLOCATOR_TEST_BASE_PATH);
    leaderElector.setLeaderInfo("addr1");
    leaderElector.setLeaderInfoPath(string(LEADERLOCATOR_TEST_BASE_PATH) + "/" + LEADERLOCATOR_BASENAME);
    ASSERT_FALSE(leaderElector.isLeader());
    ASSERT_TRUE(leaderElector.tryLock());
    ASSERT_TRUE(leaderElector.isLeader());

    LeaderLocator leaderLocator;
    leaderLocator.init(_zk3, LEADERLOCATOR_TEST_BASE_PATH, LEADERLOCATOR_BASENAME);
    std::string path = std::string(LEADERLOCATOR_TEST_BASE_PATH) + "/" + LEADERLOCATOR_BASENAME;

    EXPECT_EQ("addr1", leaderLocator.getLeaderAddr());

    LeaderChecker leaderElector2(_zk, LEADERLOCATOR_TEST_BASE_PATH);
    leaderElector2.setLeaderInfo("addr2");
    leaderElector2.setLeaderInfoPath(string(LEADERLOCATOR_TEST_BASE_PATH) + "/" + LEADERLOCATOR_BASENAME);

    ASSERT_FALSE(leaderElector2.isLeader());
    ASSERT_FALSE(leaderElector2.tryLock());
    ASSERT_FALSE(leaderElector2.isLeader());

    EXPECT_EQ("addr1", leaderLocator.getLeaderAddr());

    leaderElector.unlock();
    ASSERT_FALSE(leaderElector.isLeader());

    ASSERT_TRUE(leaderElector2.tryLock());
    ASSERT_TRUE(leaderElector2.isLeader());
    usleep(LEADERLOCATOR_TIMEOUT);
    ASSERT_FALSE(leaderElector.tryLock());
    EXPECT_EQ("addr2", leaderLocator.getLeaderAddr());

    leaderElector2.unlock();
    ASSERT_FALSE(leaderElector2.isLeader());
    ASSERT_FALSE(leaderElector.isLeader());

    ASSERT_TRUE(leaderElector.tryLock());
    ASSERT_TRUE(leaderElector.isLeader());
    usleep(LEADERLOCATOR_TIMEOUT);
    EXPECT_EQ("addr1", leaderLocator.getLeaderAddr());

    _zk3->close();
    EXPECT_EQ("addr1", leaderLocator.getLeaderAddr());
}

}; // namespace worker_framework

#endif // WORKER_FRAMEWORK_LEADERLOCATORTEST_H
