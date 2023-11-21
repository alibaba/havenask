#include "suez/table/SyncVersion.h"

#include <memory>

#include "MockLeaderElectionManager.h"
#include "MockVersionPublisher.h"
#include "MockVersionSynchronizer.h"
#include "TableTestUtil.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/framework/Version.h"
#include "suez/common/TablePathDefine.h"
#include "suez/common/test/TableMetaUtil.h"
#include "suez/table/VersionManager.h"
#include "unittest/unittest.h"

namespace suez {

class SyncVersionTest : public TESTBASE {
public:
    void setUp() override {
        _versionMgr = std::make_unique<VersionManager>();
        _leaderElectionMgr = std::make_unique<NiceMockLeaderElectionManager>();
        _versionPublisher = std::make_unique<NiceMockVersionPublisher>();
        _versionSync = std::make_unique<NiceMockVersionSynchronizer>();

        _pid = TableMetaUtil::makePid("table", 1);
        _table = TableTestUtil::make(_pid);
    }

protected:
    PartitionId _pid;
    TablePtr _table;
    std::unique_ptr<VersionManager> _versionMgr;
    std::unique_ptr<NiceMockLeaderElectionManager> _leaderElectionMgr;
    std::unique_ptr<NiceMockVersionPublisher> _versionPublisher;
    std::unique_ptr<NiceMockVersionSynchronizer> _versionSync;
};

TEST_F(SyncVersionTest, testSyncForLeader) {
    auto indexRoot = GET_TEMP_DATA_PATH();
    auto partitionRoot = TablePathDefine::constructIndexPath(indexRoot, _pid);
    auto ec = fslib::fs::FileSystem::mkDir(partitionRoot, true);
    ASSERT_EQ(fslib::EC_OK, ec);
    TargetPartitionMeta target;
    target.setRawIndexRoot(indexRoot);
    target.setConfigPath(GET_TEST_DATA_PATH() + "/table_test/empty_config");
    std::string expectedConfigPath = target.getConfigPath();
    SyncVersion sync(_table,
                     target,
                     _leaderElectionMgr.get(),
                     _versionMgr.get(),
                     _versionPublisher.get(),
                     _versionSync.get(),
                     SyncVersion::SE_COMMIT);
    EXPECT_CALL(*_leaderElectionMgr, getRoleType(_)).WillRepeatedly(Return(RT_LEADER));
    EXPECT_CALL(*_versionSync, persistVersion(_, _, _, expectedConfigPath, _)).Times(0);
    EXPECT_CALL(*_versionPublisher, publish(_, _, _)).Times(0);
    EXPECT_CALL(*_leaderElectionMgr, removePartition(_)).Times(0);
    sync.run();

    TableVersion localVersion(1);
    localVersion.setIsLeaderVersion(true);
    ASSERT_TRUE(_versionMgr->updateLocalVersion(_pid, localVersion));
    EXPECT_CALL(*_versionSync, persistVersion(_pid, _, _, expectedConfigPath, _)).WillOnce(Return(true));
    EXPECT_CALL(*_versionPublisher, publish(_, _pid, _)).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElectionMgr, removePartition(_)).Times(0);
    sync.run();
    {
        PartitionVersion pv;
        ASSERT_TRUE(_versionMgr->getVersion(_pid, pv));
        ASSERT_EQ(pv.localVersion, pv.leaderVersion);
        ASSERT_EQ(pv.localVersion, pv.publishedVersion);
    }

    TableVersion sealedVersion(2);
    sealedVersion.setFinished(true);
    sealedVersion.setIsLeaderVersion(true);
    ASSERT_TRUE(_versionMgr->updateLocalVersion(_pid, sealedVersion));
    EXPECT_CALL(*_versionSync, persistVersion(_pid, _, _, _, _)).WillOnce(Return(false));
    EXPECT_CALL(*_versionPublisher, publish(_, _pid, _)).Times(0);
    EXPECT_CALL(*_leaderElectionMgr, removePartition(_)).Times(0);
    sync.run();
    {
        PartitionVersion pv;
        ASSERT_TRUE(_versionMgr->getVersion(_pid, pv));
        ASSERT_TRUE(pv.localVersion.isFinished());
        ASSERT_FALSE(pv.leaderVersion.isFinished());
        ASSERT_NE(pv.localVersion, pv.leaderVersion);
        ASSERT_NE(pv.localVersion, pv.publishedVersion);
        ASSERT_EQ(pv.leaderVersion, pv.publishedVersion);
    }

    EXPECT_CALL(*_versionSync, persistVersion(_pid, _, _, _, _)).WillOnce(Return(true));
    EXPECT_CALL(*_versionPublisher, publish(_, _pid, _)).WillOnce(Return(false));
    EXPECT_CALL(*_leaderElectionMgr, removePartition(_)).Times(0);
    sync.run();
    {
        PartitionVersion pv;
        ASSERT_TRUE(_versionMgr->getVersion(_pid, pv));
        ASSERT_TRUE(pv.localVersion.isFinished());
        ASSERT_TRUE(pv.leaderVersion.isFinished());
        ASSERT_FALSE(pv.publishedVersion.isFinished());
        ASSERT_EQ(pv.localVersion, pv.leaderVersion);
        ASSERT_NE(pv.localVersion, pv.publishedVersion);
    }

    EXPECT_CALL(*_versionSync, persistVersion(_pid, _, _, _, _)).Times(0);
    EXPECT_CALL(*_versionPublisher, publish(_, _pid, _)).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElectionMgr, seal(_)).Times(1);
    sync.run();
    {
        PartitionVersion pv;
        ASSERT_TRUE(_versionMgr->getVersion(_pid, pv));
        ASSERT_TRUE(pv.localVersion.isFinished());
        ASSERT_EQ(pv.localVersion, pv.leaderVersion);
        ASSERT_EQ(pv.localVersion, pv.publishedVersion);
    }
}

TEST_F(SyncVersionTest, testSyncForFollower) {
    TargetPartitionMeta target;
    target.setRawIndexRoot("/index/root");
    target.setConfigPath(GET_TEST_DATA_PATH() + "/table_test/empty_config");
    SyncVersion sync(_table,
                     target,
                     _leaderElectionMgr.get(),
                     _versionMgr.get(),
                     _versionPublisher.get(),
                     _versionSync.get(),
                     SyncVersion::SE_COMMIT);
    EXPECT_CALL(*_leaderElectionMgr, getRoleType(_)).WillRepeatedly(Return(RT_FOLLOWER));

    {
        TableVersion leaderVersion(1);
        EXPECT_CALL(*_versionSync, syncFromPersist(_pid, _, _, _, _))
            .WillOnce(DoAll(SetArgReferee<4>(leaderVersion), Return(true)));
        EXPECT_CALL(*_versionPublisher, getPublishedVersion(_, _, _, _)).Times(0);
        EXPECT_CALL(*_leaderElectionMgr, removePartition(_pid)).Times(0);
        sync.run();
        PartitionVersion pv;
        ASSERT_TRUE(_versionMgr->getVersion(_pid, pv));
        ASSERT_EQ(leaderVersion, pv.leaderVersion);
        ASSERT_NE(pv.leaderVersion, pv.publishedVersion);
    }

    {
        TableVersion leaderVersion(2);
        leaderVersion.setFinished(true);
        EXPECT_CALL(*_versionSync, syncFromPersist(_pid, _, _, _, _))
            .WillOnce(DoAll(SetArgReferee<4>(leaderVersion), Return(true)));
        EXPECT_CALL(*_versionPublisher, getPublishedVersion(_, _, _, _)).WillOnce(Return(false));
        EXPECT_CALL(*_leaderElectionMgr, removePartition(_pid)).Times(0);
        sync.run();
        PartitionVersion pv;
        ASSERT_TRUE(_versionMgr->getVersion(_pid, pv));
        ASSERT_EQ(leaderVersion, pv.leaderVersion);
        ASSERT_NE(pv.leaderVersion, pv.publishedVersion);
    }

    {
        TableVersion leaderVersion(2);
        leaderVersion.setFinished(true);
        EXPECT_CALL(*_versionSync, syncFromPersist(_pid, _, _, _, _)).Times(0);
        EXPECT_CALL(*_versionPublisher, getPublishedVersion(_, _, _, _))
            .WillOnce(DoAll(SetArgReferee<3>(leaderVersion), Return(true)));
        EXPECT_CALL(*_leaderElectionMgr, removePartition(_pid)).Times(0);
        EXPECT_CALL(*_leaderElectionMgr, seal(_pid)).Times(1);
        sync.run();
        PartitionVersion pv;
        ASSERT_TRUE(_versionMgr->getVersion(_pid, pv));
        ASSERT_EQ(leaderVersion, pv.leaderVersion);
        ASSERT_EQ(pv.leaderVersion, pv.publishedVersion);
        ASSERT_TRUE(pv.publishedVersion.isFinished());
    }

    {
        EXPECT_CALL(*_versionSync, syncFromPersist(_pid, _, _, _, _)).Times(0);
        EXPECT_CALL(*_versionPublisher, getPublishedVersion(_, _, _, _)).Times(0);
        EXPECT_CALL(*_leaderElectionMgr, removePartition(_pid)).Times(0);
        EXPECT_CALL(*_leaderElectionMgr, seal(_pid)).Times(1);
        sync.run();
    }
}

TEST_F(SyncVersionTest, testUpdateVersionList) {
    auto indexRoot = GET_TEMP_DATA_PATH();
    auto partitionRoot = TablePathDefine::constructIndexPath(indexRoot, _pid);
    auto ec = fslib::fs::FileSystem::mkDir(partitionRoot, true);
    ASSERT_EQ(fslib::EC_OK, ec);
    ASSERT_EQ(fslib::EC_OK,
              fslib::fs::FileSystem::writeFile(partitionRoot + "/../realtime_info.json",
                                               "{\"app_name\":\"appName\",\"data_table\":\"dataTable\"}"));

    TargetPartitionMeta target;
    target.setRawIndexRoot(indexRoot);
    target.setConfigPath(GET_TEST_DATA_PATH() + "/table_test/empty_config");
    for (int32_t i = 3; i < 5; ++i) {
        indexlibv2::framework::Version version(i);
        auto versionFile = fslib::fs::FileSystem::joinFilePath(partitionRoot, "version." + std::to_string(i));
        ec = fslib::fs::FileSystem::writeFile(versionFile, FastToJsonString(version));
        ASSERT_EQ(fslib::EC_OK, ec);
    }
    SyncVersion sync(_table,
                     target,
                     _leaderElectionMgr.get(),
                     _versionMgr.get(),
                     _versionPublisher.get(),
                     _versionSync.get(),
                     SyncVersion::SE_COMMIT);
    std::vector<TableVersion> versionList, newVersionList;
    for (int i = 0; i < 5; ++i) {
        TableVersion version(i);
        versionList.push_back(version);
    }
    for (int i = 3; i < 5; ++i) {
        TableVersion version(i);
        newVersionList.push_back(version);
    }
    TableVersion localVersion(6);
    localVersion.setIsLeaderVersion(true);
    newVersionList.push_back(localVersion);

    EXPECT_CALL(*_leaderElectionMgr, getRoleType(_)).WillRepeatedly(Return(RT_LEADER));
    EXPECT_CALL(*_versionSync, getVersionList(_, "appName", "dataTable", _))
        .WillOnce(DoAll(SetArgReferee<3>(versionList), Return(true)));
    EXPECT_CALL(*_versionSync, updateVersionList(_pid, "appName", "dataTable", newVersionList)).WillOnce(Return(true));

    ASSERT_TRUE(_versionMgr->updateLocalVersion(_pid, localVersion));
    EXPECT_CALL(*_versionSync, persistVersion(_pid, "appName", "dataTable", _, _)).WillOnce(Return(true));
    EXPECT_CALL(*_versionPublisher, publish(_, _pid, _)).WillOnce(Return(true));
    EXPECT_CALL(*_leaderElectionMgr, removePartition(_)).Times(0);
    sync.run();
}

} // namespace suez
