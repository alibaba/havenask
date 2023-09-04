#include "worker_framework/LeaderElector.h"

#include "autil/Thread.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "test/test.h"
#include "unittest/unittest.h"
#include "worker_base/test/MockLeaderElector.h"

using namespace std;
using namespace autil;
using namespace cm_basic;
namespace worker_framework {

class LeaderElectorTestBase : public ::testing::Test {
public:
    LeaderElectorTestBase() {}
    virtual ~LeaderElectorTestBase() {}
    virtual void SetUp() {
        unsigned short port = fslib::fs::zookeeper::ZkServer::getZkServer()->port();
        ostringstream oss;
        oss << "127.0.0.1:" << port;
        string host = oss.str();
        _zk = new ZkWrapper(host, 10);
        _zk->open();
    }

    void TearDown() { DELETE_AND_SET_NULL(_zk); }

protected:
    ZkWrapper *_zk;
};

class LeaderElectorTestSingleLoop : public LeaderElectorTestBase {
public:
    /* override */ void SetUp() {
        LeaderElectorTestBase::SetUp();
        _leaderElector = new NiceMockLeaderElector(_zk, "/leader", 20, 5);
        ASSERT_EQ(int64_t(20 * 1000 * 1000), _leaderElector->_leaseTimeoutUs);
        _leaderElector->_leaseTimeoutUs = 20;
    }

    /* override */ void TearDown() {
        DELETE_AND_SET_NULL(_leaderElector);
        _zk->remove("/leader");
        LeaderElectorTestBase::TearDown();
    }

public:
    // test for callback
    MOCK_METHOD0(noLongerLeader, void());

protected:
    MockLeaderElector *_leaderElector;
};

TEST_F(LeaderElectorTestSingleLoop, testWorkLoopCampaign) {
    int64_t currentTime = 100;
    ASSERT_FALSE(_leaderElector->isLeader());
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _)).Times(2);
    EXPECT_CALL(*_leaderElector, lockForLease());
    EXPECT_CALL(*_leaderElector, reletLeaderTime(currentTime + _leaderElector->_leaseTimeoutUs));
    EXPECT_CALL(*_leaderElector, unlockForLease());
    _leaderElector->workLoop(currentTime);
    ASSERT_TRUE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testWorkLoopDisConnect) {
    int64_t currentTime = 100;
    ASSERT_FALSE(_leaderElector->isLeader());
    cm_basic::ZkWrapper *zkWrapper = new ZkWrapper("", 10);
    _leaderElector->_zkWrapper = zkWrapper;
    EXPECT_CALL(*_leaderElector, checkStillLeader(currentTime));
    _leaderElector->workLoop(currentTime);
    ASSERT_FALSE(_leaderElector->isLeader());
    delete zkWrapper;
}

TEST_F(LeaderElectorTestSingleLoop, testWorkLoopOpenSuccess) {
    int64_t currentTime = 100;
    ASSERT_FALSE(_leaderElector->isLeader());
    _zk->close();
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _)).Times(2);
    EXPECT_CALL(*_leaderElector, lockForLease());
    EXPECT_CALL(*_leaderElector, reletLeaderTime(currentTime + _leaderElector->_leaseTimeoutUs));
    EXPECT_CALL(*_leaderElector, unlockForLease());
    _leaderElector->workLoop(currentTime);
    ASSERT_TRUE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testWorkLoopHoldLeader) {
    int64_t currentTime = 100;
    int64_t leaseTime = 120;
    _leaderElector->updateLeaderInfo(true, leaseTime);
    EXPECT_CALL(*_leaderElector, checkStillLeader(currentTime));
    EXPECT_CALL(*_leaderElector, lockForLease());
    EXPECT_CALL(*_leaderElector, reletLeaderTime(currentTime + _leaderElector->_leaseTimeoutUs));
    EXPECT_CALL(*_leaderElector, unlockForLease());
    _leaderElector->workLoop(currentTime);
    ASSERT_TRUE(_leaderElector->isLeader());
}

ACTION_P2(GetLeaseExpireTime, ret, expireTime) {
    arg0 = expireTime;
    return ret;
}

ACTION_P3(GetLeaseExpireTime, ret, expireTime, progressKey) {
    arg0 = expireTime;
    arg1 = progressKey;
    return ret;
}

TEST_F(LeaderElectorTestSingleLoop, testCampaignLeaderGetLeaseFail) {
    int64_t currentTime = 100;
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _)).WillOnce(GetLeaseExpireTime(false, 0));
    _leaderElector->campaignLeader(currentTime);
    EXPECT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testCampaignLeaderCurrentTimeLess) {
    int64_t currentTime = 10;
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _)).WillOnce(GetLeaseExpireTime(true, 20));
    _leaderElector->campaignLeader(currentTime);
    EXPECT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testCampaignLeaderHasNextCanCampainsTime) {
    int64_t currentTime = 100;
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _)).Times(0);
    _leaderElector->_nextCanCampaignTime = 200;
    _leaderElector->campaignLeader(currentTime);
    EXPECT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testCampaignLeaderLockForLeaseFail) {
    int64_t currentTime = 100;
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _)).WillOnce(GetLeaseExpireTime(true, 20));
    EXPECT_CALL(*_leaderElector, lockForLease()).WillOnce(Return(false));
    _leaderElector->campaignLeader(currentTime);
    EXPECT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testCampaignLeaderDoubleCheckFailed) {
    int64_t currentTime = 100;
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _))
        .WillOnce(GetLeaseExpireTime(true, 20))
        .WillOnce(GetLeaseExpireTime(false, 20));
    EXPECT_CALL(*_leaderElector, lockForLease()).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElector, unlockForLease()).WillOnce(Return(true));
    ;
    _leaderElector->campaignLeader(currentTime);
    EXPECT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testCampaignLeaderDoubleCheckWithProcessKey) {
    int64_t currentTime = 10;
    _leaderElector->_progressKey = "AAAA";
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _))
        .WillOnce(GetLeaseExpireTime(true, 20, "AAAA"))
        .WillOnce(GetLeaseExpireTime(true, 20, "AAAA"));
    EXPECT_CALL(*_leaderElector, lockForLease()).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElector, reletLeaderTime(30));
    EXPECT_CALL(*_leaderElector, unlockForLease()).WillOnce(Return(true));
    ;
    _leaderElector->campaignLeader(currentTime);
    EXPECT_TRUE(_leaderElector->isLeader());
    EXPECT_EQ(int64_t(30), _leaderElector->getLeaseExpirationTime());
}

TEST_F(LeaderElectorTestSingleLoop, testCampaignLeaderDoubleCheckSuccess) {
    int64_t currentTime = 100;
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _))
        .WillOnce(GetLeaseExpireTime(true, 20))
        .WillOnce(GetLeaseExpireTime(true, 20));
    EXPECT_CALL(*_leaderElector, lockForLease()).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElector, reletLeaderTime(120));
    EXPECT_CALL(*_leaderElector, unlockForLease());
    _leaderElector->campaignLeader(currentTime);
    EXPECT_TRUE(_leaderElector->isLeader());
    EXPECT_EQ(int64_t(120), _leaderElector->getLeaseExpirationTime());
}

TEST_F(LeaderElectorTestSingleLoop, testCampaignLeaderDoubleCheckSuccessWithPorgramKey) {
    int64_t currentTime = 100;
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _))
        .WillOnce(GetLeaseExpireTime(true, 20, "abc"))
        .WillOnce(GetLeaseExpireTime(true, 20, "abc"));
    EXPECT_CALL(*_leaderElector, lockForLease()).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElector, reletLeaderTime(120));
    EXPECT_CALL(*_leaderElector, unlockForLease());
    _leaderElector->campaignLeader(currentTime);
    EXPECT_TRUE(_leaderElector->isLeader());
    EXPECT_EQ(int64_t(120), _leaderElector->getLeaseExpirationTime());
}

TEST_F(LeaderElectorTestSingleLoop, testHoldLeaderCheckStillLeaderFail) {
    int64_t currentTime = 100;
    ASSERT_FALSE(_leaderElector->isLeader());
    EXPECT_CALL(*_leaderElector, checkStillLeader(currentTime)).WillOnce(Return(false));
    _leaderElector->holdLeader(currentTime);
    ASSERT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testHoldLeaderReleteLeaderFailed) {
    int64_t currentTime = 100;
    ASSERT_FALSE(_leaderElector->isLeader());
    EXPECT_CALL(*_leaderElector, checkStillLeader(currentTime)).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElector, lockForLease()).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElector, reletLeaderTime(120)).WillOnce(Return(false));
    EXPECT_CALL(*_leaderElector, unlockForLease());
    _leaderElector->holdLeader(currentTime);
}

TEST_F(LeaderElectorTestSingleLoop, testHoldLeaderReleteLeaderSuccess) {
    int64_t currentTime = 100;
    ASSERT_FALSE(_leaderElector->isLeader());
    EXPECT_CALL(*_leaderElector, checkStillLeader(currentTime)).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElector, lockForLease()).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElector, reletLeaderTime(120)).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElector, unlockForLease());
    _leaderElector->holdLeader(currentTime);
    ASSERT_TRUE(_leaderElector->isLeader());
    ASSERT_EQ(int64_t(120), _leaderElector->getLeaseExpirationTime());
}

TEST_F(LeaderElectorTestSingleLoop, testHoldLeaderLockForLeaseFailed) {
    int64_t currentTime = 100;
    ASSERT_FALSE(_leaderElector->isLeader());
    EXPECT_CALL(*_leaderElector, checkStillLeader(currentTime)).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElector, lockForLease()).WillOnce(Return(false));
    _leaderElector->holdLeader(currentTime);
    ASSERT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testDoCampaignGetLeaseTimeFail) {
    int64_t currentTime = 100;
    ASSERT_FALSE(_leaderElector->isLeader());
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _)).WillOnce(GetLeaseExpireTime(false, 20));
    _leaderElector->doCampaign(currentTime);
    ASSERT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testDoCampaignCurrentTimeLess) {
    int64_t currentTime = 100;
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _)).WillOnce(GetLeaseExpireTime(true, 120));
    _leaderElector->doCampaign(currentTime);
    ASSERT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testDoCampaignReletLeaderFail) {
    int64_t currentTime = 100;
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _)).WillOnce(GetLeaseExpireTime(true, 20));
    EXPECT_CALL(*_leaderElector, reletLeaderTime(120)).WillOnce(Return(false));
    _leaderElector->doCampaign(currentTime);
    ASSERT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testDoCampaignSuccess) {
    int64_t currentTime = 100;
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _)).WillOnce(GetLeaseExpireTime(true, 20));
    EXPECT_CALL(*_leaderElector, reletLeaderTime(120)).WillOnce(Return(true));
    _leaderElector->doCampaign(currentTime);
    ASSERT_TRUE(_leaderElector->isLeader());
    ASSERT_EQ(int64_t(120), _leaderElector->getLeaseExpirationTime());
}

TEST_F(LeaderElectorTestSingleLoop, testUnLock) {
    _leaderElector->updateLeaderInfo(false, 100);
    EXPECT_CALL(*_leaderElector, unlock());
    _leaderElector->unlock();
    ASSERT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testUnLockIsLeader) {
    _leaderElector->updateLeaderInfo(true, 100);
    EXPECT_CALL(*_leaderElector, unlock());
    EXPECT_CALL(*_leaderElector, lockForLease()).WillOnce(Return(false));
    _leaderElector->unlock();
    ASSERT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testUnLockLockSuccess) {
    EXPECT_CALL(*_leaderElector, reletLeaderTime(_));
    _leaderElector->reletLeaderTime(20);
    int64_t expectEndTime;
    string progressKey;
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _));
    ASSERT_TRUE(_leaderElector->getLeaderLeaseExpirationTime(expectEndTime, progressKey));
    ASSERT_EQ(int64_t(20), expectEndTime);
    _leaderElector->updateLeaderInfo(true, 100);
    EXPECT_CALL(*_leaderElector, unlock());
    EXPECT_CALL(*_leaderElector, lockForLease()).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElector, unlockForLease()).WillOnce(Return(true));
    _leaderElector->unlock();
    ASSERT_FALSE(_leaderElector->isLeader());
    EXPECT_CALL(*_leaderElector, getLeaderLeaseExpirationTime(_, _));
    ASSERT_TRUE(_leaderElector->getLeaderLeaseExpirationTime(expectEndTime, progressKey));
    ASSERT_EQ(int64_t(-1), expectEndTime);
}

TEST_F(LeaderElectorTestSingleLoop, testCheckStillLeaderNotLeader) {
    int64_t currentTime = 100;
    _leaderElector->checkStillLeader(currentTime);
    ASSERT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testCheckStillLeaderIsLeader) {
    int64_t currentTime = 100;
    _leaderElector->updateLeaderInfo(true, 120);
    EXPECT_TRUE(_leaderElector->checkStillLeader(currentTime));
    ASSERT_TRUE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testCheckStillLeaderNotLeader1) {
    int64_t currentTime = 100;
    _leaderElector->updateLeaderInfo(true, 100);
    EXPECT_FALSE(_leaderElector->checkStillLeader(currentTime));
    ASSERT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testCheckStillLeaderNotLeaderWithForbitTime) {
    int64_t currentTime = 100;
    _leaderElector->updateLeaderInfo(true, 100);
    _leaderElector->setForbidCampaignLeaderTimeMs(1);
    _leaderElector->checkStillLeader(currentTime);
    ASSERT_FALSE(_leaderElector->isLeader());
    ASSERT_EQ(1 * 1000 + 100, _leaderElector->_nextCanCampaignTime);
}

TEST_F(LeaderElectorTestSingleLoop, testGetLeaderLeaseExpirationTimeEmptyFile) {
    int64_t lastLeaderEndTime = 100;
    string progressKey;
    ASSERT_TRUE(_leaderElector->getLeaderLeaseExpirationTime(lastLeaderEndTime, progressKey));
    ASSERT_EQ(int64_t(-1), lastLeaderEndTime);
}

TEST_F(LeaderElectorTestSingleLoop, testGetLeaderLeaseExpirationTimeSuccess) {
    int64_t lastLeaderEndTime = 100;
    string progressKey;
    ASSERT_TRUE(_leaderElector->reletLeaderTime(20));
    ASSERT_TRUE(_leaderElector->getLeaderLeaseExpirationTime(lastLeaderEndTime, progressKey));
    ASSERT_EQ(int64_t(20), lastLeaderEndTime);
}

TEST_F(LeaderElectorTestSingleLoop, testSetHandler) {
    int64_t currentTime = 100;
    EXPECT_CALL(*this, noLongerLeader());
    _leaderElector->setNoLongerLeaderHandler(std::bind(&LeaderElectorTestSingleLoop::noLongerLeader, this));
    _leaderElector->updateLeaderInfo(true, 100);
    _leaderElector->checkStillLeader(currentTime);
}

TEST_F(LeaderElectorTestSingleLoop, testWriteEmptyLeaderInfo) {
    _leaderElector->_state.isLeader = true;
    ASSERT_TRUE(_leaderElector->writeLeaderInfo("", "leaderInfo"));
    ASSERT_TRUE(_leaderElector->writeLeaderInfo("leaderInfoPath", ""));
}

TEST_F(LeaderElectorTestSingleLoop, testWriteLeaderInfo) {
    _leaderElector->_state.isLeader = true;
    ASSERT_TRUE(_leaderElector->writeLeaderInfo("/leaderInfoPath", "leaderInfo"));
    string data;
    ASSERT_EQ(ZOK, _leaderElector->_zkWrapper->getData("/leaderInfoPath", data));
    ASSERT_EQ("leaderInfo", data);
}

TEST_F(LeaderElectorTestSingleLoop, testUpdateLeaderInfoNotLeader) {
    _leaderElector->setLeaderInfo("leaderInfo");
    _leaderElector->setLeaderInfoPath("leaderInfoPath1");
    _leaderElector->updateLeaderInfo(false, 100);
    ASSERT_FALSE(_leaderElector->isLeader());
    string data;
    _leaderElector->_zkWrapper->getData("/leaderInfoPath1", data);
    ASSERT_TRUE("leaderInfo" != data);
    ASSERT_FALSE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testUpdateLeaderInfo) {
    _leaderElector->setLeaderInfo("leaderInfo");
    _leaderElector->setLeaderInfoPath("/leaderInfoPath");
    _leaderElector->updateLeaderInfo(true, 100);
    string data;
    _leaderElector->_zkWrapper->getData("/leaderInfoPath", data);
    ASSERT_TRUE("leaderInfo" == data);
    ASSERT_TRUE(_leaderElector->isLeader());
}

TEST_F(LeaderElectorTestSingleLoop, testGetLeaderEndTimeFromString1) {
    {
        string leaderFileContentStr = "";
        int64_t lastLeaderEndTime;
        string progressKey;
        ASSERT_FALSE(_leaderElector->getLeaderEndTimeFromString(leaderFileContentStr, lastLeaderEndTime, progressKey));
    }
    {
        string leaderFileContentStr = "100d0#_#key";
        int64_t lastLeaderEndTime;
        string progressKey;
        ASSERT_FALSE(_leaderElector->getLeaderEndTimeFromString(leaderFileContentStr, lastLeaderEndTime, progressKey));
    }
    {
        string leaderFileContentStr = "1000#_#key";
        int64_t lastLeaderEndTime;
        string progressKey;
        ASSERT_TRUE(_leaderElector->getLeaderEndTimeFromString(leaderFileContentStr, lastLeaderEndTime, progressKey));
        EXPECT_EQ(int64_t(1000), lastLeaderEndTime);
        EXPECT_EQ(string("key"), progressKey);
    }
    {
        string leaderFileContentStr = "1000#_#key\nip:port";
        int64_t lastLeaderEndTime;
        string progressKey;
        ASSERT_TRUE(_leaderElector->getLeaderEndTimeFromString(leaderFileContentStr, lastLeaderEndTime, progressKey));
        EXPECT_EQ(int64_t(1000), lastLeaderEndTime);
        EXPECT_EQ(string("key"), progressKey);
    }
    {
        // no progress key
        string leaderFileContentStr = "1000\nip:port";
        int64_t lastLeaderEndTime;
        string progressKey;
        ASSERT_TRUE(_leaderElector->getLeaderEndTimeFromString(leaderFileContentStr, lastLeaderEndTime, progressKey));
        EXPECT_EQ(int64_t(1000), lastLeaderEndTime);
        EXPECT_EQ("", progressKey);
    }
    {
        string leaderFileContentStr = "1000";
        int64_t lastLeaderEndTime;
        string progressKey;
        ASSERT_TRUE(_leaderElector->getLeaderEndTimeFromString(leaderFileContentStr, lastLeaderEndTime, progressKey));
        EXPECT_EQ(int64_t(1000), lastLeaderEndTime);
        EXPECT_EQ(string(""), progressKey);
    }
}

TEST_F(LeaderElectorTestSingleLoop, testGenerateLeaderLeaseFileStr) {
    ASSERT_EQ("123", _leaderElector->generateLeaderLeaseFileStr(123));
    _leaderElector->_progressKey = "xx";
    ASSERT_EQ("123#_#xx", _leaderElector->generateLeaderLeaseFileStr(123));
    _leaderElector->setLeaseInfo("yy");
    ASSERT_EQ("123#_#xx\nyy", _leaderElector->generateLeaderLeaseFileStr(123));
}

class LeaderElectorTestMultiLoop : public LeaderElectorTestBase {
public:
    /* override */ void SetUp() {
        LeaderElectorTestBase::SetUp();
        _leaderElector1 = new NiceMockLeaderElector(_zk, "/leader", 5, 1);
        _leaderElector2 = new NiceMockLeaderElector(_zk, "/leader", 5, 1);
    }

    /* override */ void TearDown() {
        DELETE_AND_SET_NULL(_leaderElector1);
        DELETE_AND_SET_NULL(_leaderElector2);
        _zk->remove("/leader");
        LeaderElectorTestBase::TearDown();
    }

    static bool lockForLeaseSleep() {
        usleep(15 * 1000 * 1000);
        return false;
    }

protected:
    MockLeaderElector *_leaderElector1;
    MockLeaderElector *_leaderElector2;
};

// in this case, we make lease time out = 5s and loop interval = 1s
// comment two leader ut, not use and not stable
// TEST_F(LeaderElectorTestMultiLoop, testTwoLeaderContinueLeader) {
//     ASSERT_TRUE(_leaderElector1->start());
//     // wait leaderElector1 to be leader
//     usleep(1 * 1000 * 1000);
//     ASSERT_TRUE(_leaderElector2->start());
//     ASSERT_TRUE(_leaderElector1->isLeader());
//     usleep(1 * 1000 * 1000);
//     ASSERT_FALSE(_leaderElector2->isLeader());
//     usleep(6 * 1000 * 1000);
//     ASSERT_TRUE(_leaderElector1->isLeader());
//     ASSERT_FALSE(_leaderElector2->isLeader());
// }

// TEST_F(LeaderElectorTestMultiLoop, testTwoLeader) {
//     ASSERT_TRUE(_leaderElector1->start());
//     // wait leaderElector1 to be leader
//     usleep(1 * 1000 * 1000);
//     ASSERT_TRUE(_leaderElector2->start());
//     ASSERT_TRUE(_leaderElector1->isLeader());
//     ASSERT_FALSE(_leaderElector2->isLeader());
//     _leaderElector1->stop();
//     usleep(1500 * 1000);
//     ASSERT_FALSE(_leaderElector1->isLeader());
//     ASSERT_TRUE(_leaderElector2->isLeader());
// }

TEST_F(LeaderElectorTestMultiLoop, testTwoLeaderOneElectorTerminate) {
    // Note (guanming.fh): comment out flaky test that use local zk running unittest,
    // which is not a non-hermetic test. Furthermore, the two leaders is not used.

    // ASSERT_TRUE(_leaderElector1->start());
    // // wait leaderElector1 to be leader
    // usleep(1000 * 1000);
    // ASSERT_TRUE(_leaderElector2->start());
    // ASSERT_TRUE(_leaderElector1->isLeader());
    // usleep(1000 * 1000);
    // ASSERT_FALSE(_leaderElector2->isLeader());
    // // make leader1 stop
    // _leaderElector1->_leaderLockThreadPtr.reset();
    // usleep(1000 * 1000);
    // // leader1 still leader because lease time = 5s
    // ASSERT_TRUE(_leaderElector1->isLeader());
    // ASSERT_FALSE(_leaderElector2->isLeader());
    // usleep(5500 * 1000);
    // // leader 2 become leader because leader timeout
    // ASSERT_TRUE(_leaderElector1->start());
    // usleep(2000 * 1000);
    // ASSERT_FALSE(_leaderElector1->isLeader());
    // ASSERT_TRUE(_leaderElector2->isLeader());
}

TEST_F(LeaderElectorTestMultiLoop, DISABLED_testTwoLeaderOneElectorCheckExistDead) {
    ASSERT_TRUE(_leaderElector1->start());
    // wait leaderElector1 to be leader
    usleep(1000 * 1000);
    ASSERT_TRUE(_leaderElector2->start());
    ASSERT_TRUE(_leaderElector1->isLeader());
    usleep(1000 * 1000);
    ASSERT_FALSE(_leaderElector2->isLeader());
    // make leader1 stop
    ON_CALL(*_leaderElector1, lockForLease())
        .WillByDefault(Invoke(std::bind(&LeaderElectorTestMultiLoop::lockForLeaseSleep)));
    usleep(2000 * 1000);
    // leader1 still leader because lease time = 5s
    ASSERT_TRUE(_leaderElector1->isLeader());
    ASSERT_FALSE(_leaderElector2->isLeader());
    usleep(5500 * 1000);
    // leader 2 become leader because leader timeout
    // ASSERT_TRUE(_leaderElector1->start());
    usleep(2000 * 1000);
    ASSERT_FALSE(_leaderElector1->isLeader());
    ASSERT_TRUE(_leaderElector2->isLeader());
}

// TEST_F(LeaderElectorTestMultiLoop, testXXX) {
//     _zk->touch("/a/b/c", "aaa", true);
//     delete _zk;

//     unsigned short port = fslib::fs::zookeeper::ZkServer::getZkServer()->port();
//     ostringstream oss;
//     oss << "127.0.0.2:" << port;
//     string host = oss.str();
//     _zk = new ZkWrapper(host, 10);
//     _zk->open();

//     vector<string> data;
//     _zk->getChild("/a/b", data);
//     for (size_t i = 0 ; i < data.size(); i++) {
//         cout << "***" << data[i] << endl;
//     }
// }

}; // namespace worker_framework
