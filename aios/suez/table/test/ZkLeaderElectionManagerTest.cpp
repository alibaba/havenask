#include "suez/table/ZkLeaderElectionManager.h"

#include "aios/network/gig/multi_call/rpc/GigRpcServer.h"
#include "autil/EnvUtil.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "suez/common/GeneratorDef.h"
#include "suez/common/test/TableMetaUtil.h"
#include "suez/sdk/PathDefine.h"
#include "suez/table/TableInfoPublishWrapper.h"
#include "suez/table/TableLeaderInfoPublisher.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class ZkLeaderElectionManagerTest : public TESTBASE {
public:
    void setUp() override {
        unsigned short port = fslib::fs::zookeeper::ZkServer::getZkServer()->port();
        ostringstream oss;
        oss << "127.0.0.1:" << port;
        _zkHost = oss.str();
        _zkRoot = "zfs://" + _zkHost + "/" + GET_CLASS_NAME();
    }

    void tearDown() override { fslib::fs::zookeeper::ZkServer::getZkServer()->stop(); }
    multi_call::GigRpcServerPtr createGigRpcServer();
    void doTestUpdateLeaderInfo(bool publishZoneName, bool extraPublishRawTable);

protected:
    string _zkHost;
    string _zkRoot;
};

multi_call::GigRpcServerPtr ZkLeaderElectionManagerTest::createGigRpcServer() {
    multi_call::GigRpcServerPtr gigServer(new multi_call::GigRpcServer());
    multi_call::GrpcServerDescription desc;
    auto grpcPortStr = autil::StringUtil::toString(0);
    desc.port = grpcPortStr;
    desc.ioThreadNum = 1;
    if (!gigServer->initGrpcServer(desc)) {
        return nullptr;
    }
    return gigServer;
}

TEST_F(ZkLeaderElectionManagerTest, testSimple) {
    LeaderElectionConfig config;
    config.zkRoot = "zfs://" + _zkHost + "/" + GET_CLASS_NAME();
    config.strategyType = "table";

    ZkLeaderElectionManager manager(config);
    ASSERT_TRUE(manager.init());

    auto pid1 = TableMetaUtil::makePid("t1", 0, 1, 2);
    auto pid2 = TableMetaUtil::makePid("t2", 0, 1, 2);
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid2));

    ASSERT_TRUE(manager.registerPartition(pid1));
    ASSERT_TRUE(manager.registerPartition(pid2));

    int retryTimes = 20;
    int retrySleepTime = 1;
    while (retryTimes-- > 0) {
        auto roleType = manager.getRoleType(pid1);
        if (roleType != RT_LEADER) {
            sleep(retrySleepTime);
        } else {
            break;
        }
    }
    ASSERT_EQ(RT_LEADER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_LEADER, manager.getRoleType(pid2));

    manager.getLeaderElector(pid1)->forceFollowerTimeInMs = 2000;
    manager.releaseLeader(pid1);
    ASSERT_FALSE(manager.isLeader(pid1));
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid1));

    sleep(3);
    ASSERT_FALSE(manager.isLeader(pid1));

    manager.removePartition(pid1);
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_LEADER, manager.getRoleType(pid2));

    manager.stop();
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid2));
}

TEST_F(ZkLeaderElectionManagerTest, testCampaignLeaderByTable) {
    LeaderElectionConfig config;
    config.zkRoot = "zfs://" + _zkHost + "/" + GET_CLASS_NAME();
    config.strategyType = "table";
    config.campaignNow = false;
    ZkLeaderElectionManager manager(config);
    ASSERT_TRUE(manager.init());

    auto pid1 = TableMetaUtil::makePid("t1", 0, 1, 2);
    auto pid2 = TableMetaUtil::makePid("t2", 0, 1, 2);
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid2));

    ASSERT_TRUE(manager.registerPartition(pid1));
    ASSERT_TRUE(manager.registerPartition(pid2));
    sleep(2);
    ASSERT_FALSE(manager.getLeaderElector(pid1)->hasStarted);
    ASSERT_FALSE(manager.getLeaderElector(pid2)->hasStarted);
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid2));
    manager.allowCampaginLeader(pid1);
    int retryTimes = 20;
    int retrySleepTime = 1;
    while (retryTimes-- > 0) {
        auto roleType = manager.getRoleType(pid1);
        if (roleType != RT_LEADER) {
            sleep(retrySleepTime);
        } else {
            break;
        }
    }
    ASSERT_EQ(RT_LEADER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid2));

    manager.getLeaderElector(pid1)->forceFollowerTimeInMs = 2000;
    manager.releaseLeader(pid1);
    ASSERT_FALSE(manager.isLeader(pid1));
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid1));
    ASSERT_TRUE(manager.getLeaderElector(pid1)->hasStarted);

    sleep(3);
    ASSERT_FALSE(manager.isLeader(pid1));

    manager.removePartition(pid1);
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid2));

    manager.stop();
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid2));
}

TEST_F(ZkLeaderElectionManagerTest, testCampaignLeaderByRange) {
    LeaderElectionConfig config;
    config.zkRoot = "zfs://" + _zkHost + "/" + GET_CLASS_NAME();
    config.strategyType = "range";
    config.campaignNow = false;
    ZkLeaderElectionManager manager(config);
    ASSERT_TRUE(manager.init());

    auto pid1 = TableMetaUtil::makePid("t1", 0, 1, 2);
    auto pid2 = TableMetaUtil::makePid("t2", 0, 1, 2);
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid2));

    ASSERT_TRUE(manager.registerPartition(pid1));
    ASSERT_TRUE(manager.registerPartition(pid2));
    sleep(2);
    ASSERT_FALSE(manager.getLeaderElector(pid1)->hasStarted);
    ASSERT_FALSE(manager.getLeaderElector(pid2)->hasStarted);
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid2));
    manager.allowCampaginLeader(pid1);
    sleep(2);
    ASSERT_FALSE(manager.getLeaderElector(pid1)->hasStarted);
    ASSERT_FALSE(manager.getLeaderElector(pid2)->hasStarted);
    manager.allowCampaginLeader(pid2);

    int retryTimes = 20;
    int retrySleepTime = 1;
    while (retryTimes-- > 0) {
        auto roleType = manager.getRoleType(pid1);
        if (roleType != RT_LEADER) {
            sleep(retrySleepTime);
        } else {
            break;
        }
    }
    ASSERT_EQ(RT_LEADER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_LEADER, manager.getRoleType(pid2));
    ASSERT_TRUE(manager.getLeaderElector(pid1)->hasStarted);
    ASSERT_TRUE(manager.getLeaderElector(pid2)->hasStarted);
}

TEST_F(ZkLeaderElectionManagerTest, testCampaignLeaderLaterByWorker) {
    autil::EnvGuard env1("roleName", "role_name");
    LeaderElectionConfig config;
    config.zkRoot = "zfs://" + _zkHost + "/" + GET_CLASS_NAME();
    config.strategyType = "worker";
    config.campaignNow = false;
    ZkLeaderElectionManager manager(config);
    ASSERT_TRUE(manager.init());

    auto pid1 = TableMetaUtil::makePid("t1", 0, 1, 2);
    auto pid2 = TableMetaUtil::makePid("t2", 0, 1, 2);
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid2));

    ASSERT_TRUE(manager.registerPartition(pid1));
    ASSERT_TRUE(manager.registerPartition(pid2));
    sleep(2);
    ASSERT_FALSE(manager.getLeaderElector(pid1)->hasStarted);
    ASSERT_FALSE(manager.getLeaderElector(pid2)->hasStarted);
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_FOLLOWER, manager.getRoleType(pid2));
    manager.allowCampaginLeader(pid1);
    sleep(2);
    ASSERT_FALSE(manager.getLeaderElector(pid1)->hasStarted);
    ASSERT_FALSE(manager.getLeaderElector(pid2)->hasStarted);
    manager.allowCampaginLeader(pid2);
    int retryTimes = 20;
    int retrySleepTime = 1;
    while (retryTimes-- > 0) {
        auto roleType = manager.getRoleType(pid1);
        if (roleType != RT_LEADER) {
            sleep(retrySleepTime);
        } else {
            break;
        }
    }
    ASSERT_EQ(RT_LEADER, manager.getRoleType(pid1));
    ASSERT_EQ(RT_LEADER, manager.getRoleType(pid2));
    ASSERT_TRUE(manager.getLeaderElector(pid1)->hasStarted);
    ASSERT_TRUE(manager.getLeaderElector(pid2)->hasStarted);
}

void ZkLeaderElectionManagerTest::doTestUpdateLeaderInfo(bool publishZoneName, bool extraPublishRawTable) {
    autil::EnvGuard env1("roleName", "searcher_0");
    LeaderElectionConfig config;
    config.zkRoot = "zfs://" + _zkHost + "/" + GET_CLASS_NAME();
    config.strategyType = "worker";
    config.needZoneName = publishZoneName;
    config.extraPublishRawTable = extraPublishRawTable;

    ZkLeaderElectionManager manager(config);
    ASSERT_TRUE(manager.init());
    ASSERT_EQ(PathDefine::join(config.zkRoot, "leader_info"), manager._leaderInfoRoot);
    manager.setLeaderInfo("ip:port");

    auto pid1 = TableMetaUtil::makePid("t1", 0, 0, 32767);
    auto pid2 = TableMetaUtil::makePid("t2", 0, 0, 32767);
    auto pid3 = TableMetaUtil::makePid("t3", 0, 0, 65535);

    ASSERT_TRUE(manager.registerPartition(pid1));
    ASSERT_TRUE(manager.registerPartition(pid2));
    ASSERT_TRUE(manager.registerPartition(pid3));
    ASSERT_EQ(1, manager._leaderElectors.size());

    int retryTimes = 20;
    int retrySleepTime = 1;
    while (retryTimes-- > 0) {
        auto roleType = manager.getRoleType(pid1);
        if (roleType != RT_LEADER) {
            sleep(retrySleepTime);
        } else {
            break;
        }
    }
    ASSERT_EQ(RT_LEADER, manager.getRoleType(pid1));

    pid1.index = 1;
    pid1.partCount = 2;
    auto gigServer = createGigRpcServer();
    ASSERT_TRUE(gigServer);
    auto publishWrapper = std::make_shared<TableInfoPublishWrapper>(gigServer.get());
    auto publisher = std::make_unique<TableLeaderInfoPublisher>(pid1, "zone_name", publishWrapper.get(), &manager);
    EXPECT_FALSE(publisher->signaturePublished());
    ASSERT_TRUE(publisher->publish());

    auto publishInfos = gigServer->getPublishInfos();
    if (!extraPublishRawTable) {
        ASSERT_EQ(1u, publishInfos.size());
    } else {
        ASSERT_EQ(2u, publishInfos.size());
        auto &info = publishInfos[1];
        EXPECT_EQ("t1", info.bizName);
        EXPECT_EQ(2, info.partCount);
        EXPECT_EQ(1, info.partId);
        EXPECT_EQ(0, info.version);
        EXPECT_EQ(1u, info.tags.size());
        EXPECT_EQ(1u, info.tags.size());
        EXPECT_EQ(manager.getLeaderTimestamp(pid1), info.tags[GeneratorDef::LEADER_TAG]);
    }
    auto &info = publishInfos[0];
    EXPECT_EQ(publishZoneName ? "zone_name.t1.write" : "t1.write", info.bizName);
    EXPECT_EQ(2, info.partCount);
    EXPECT_EQ(1, info.partId);
    EXPECT_EQ(0, info.version);
    EXPECT_EQ(1u, info.tags.size());
    EXPECT_EQ(1u, info.tags.size());
    EXPECT_EQ(manager.getLeaderTimestamp(pid1), info.tags[GeneratorDef::LEADER_TAG]);

    EXPECT_TRUE(publisher->signaturePublished());

    manager.getLeaderElector(pid1)->becomeLeaderTimestamp = 10;
    ASSERT_TRUE(publisher->publish());
    auto publishInfosNew = gigServer->getPublishInfos();
    ASSERT_EQ(publishInfos.size(), publishInfosNew.size());
    auto &infoNew = publishInfosNew[0];
    EXPECT_EQ(infoNew.bizName, info.bizName);
    EXPECT_EQ(infoNew.partCount, info.partCount);
    EXPECT_EQ(infoNew.partId, info.partId);
    EXPECT_EQ(infoNew.version, info.version);
    EXPECT_EQ(10, infoNew.tags[GeneratorDef::LEADER_TAG]);
}

TEST_F(ZkLeaderElectionManagerTest, testUpdateLeaderInfo) {
    doTestUpdateLeaderInfo(false, false);
    doTestUpdateLeaderInfo(true, false);
    doTestUpdateLeaderInfo(false, true);
    doTestUpdateLeaderInfo(true, true);
}

TEST_F(ZkLeaderElectionManagerTest, testRefCount) {
    LeaderElectionConfig config;
    config.zkRoot = "zfs://" + _zkHost + "/" + GET_CLASS_NAME();
    config.strategyType = "range";

    ZkLeaderElectionManager manager(config);
    ASSERT_TRUE(manager.init());

    auto pid1 = TableMetaUtil::makePid("t1", 0, 0, 32767);
    auto pid2 = TableMetaUtil::makePid("t2", 0, 0, 32767);
    auto pid3 = TableMetaUtil::makePid("t3", 0, 0, 65535);

    ASSERT_TRUE(manager.registerPartition(pid1));
    ASSERT_EQ(1, manager._leaderElectors.size());
    LeaderElectorWrapper *leaderElector = manager.getLeaderElector(pid1);
    ASSERT_TRUE(leaderElector != nullptr);
    ASSERT_EQ(1, leaderElector->getRefCount());

    ASSERT_TRUE(manager.registerPartition(pid2));
    ASSERT_EQ(1, manager._leaderElectors.size());
    leaderElector = manager.getLeaderElector(pid2);
    ASSERT_TRUE(leaderElector != nullptr);
    ASSERT_EQ(2, leaderElector->getRefCount());

    ASSERT_TRUE(manager.registerPartition(pid3));
    ASSERT_EQ(2, manager._leaderElectors.size());
    leaderElector = manager.getLeaderElector(pid3);
    ASSERT_TRUE(leaderElector != nullptr);
    ASSERT_EQ(1, leaderElector->getRefCount());

    // only table startegy type remove partition
    manager.removePartition(pid1);
    ASSERT_EQ(2, manager._leaderElectors.size());
    leaderElector = manager.getLeaderElector(pid1);
    ASSERT_TRUE(leaderElector != nullptr);
    ASSERT_EQ(1, leaderElector->getRefCount());

    manager.removePartition(pid2);
    ASSERT_EQ(1, manager._leaderElectors.size());
    leaderElector = manager.getLeaderElector(pid2);
    ASSERT_TRUE(leaderElector == nullptr);
}

} // namespace suez
