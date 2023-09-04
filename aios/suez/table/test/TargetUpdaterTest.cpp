#include "suez/table/TargetUpdater.h"

#include "MockLeaderElectionManager.h"
#include "MockVersionSynchronizer.h"
#include "fslib/fs/FileSystem.h"
#include "suez/common/TablePathDefine.h"
#include "suez/common/test/TableMetaUtil.h"
#include "suez/table/VersionManager.h"
#include "unittest/unittest.h"

namespace suez {

class TargetUpdaterTest : public TESTBASE {
public:
    void setUp() override {
        _leaderMgr = std::make_unique<NiceMockLeaderElectionManager>();
        _versionMgr = std::make_unique<VersionManager>();
        _versionSync = std::make_unique<NiceMockVersionSynchronizer>();
    }

protected:
    std::unique_ptr<NiceMockLeaderElectionManager> _leaderMgr;
    std::unique_ptr<VersionManager> _versionMgr;
    std::unique_ptr<NiceMockVersionSynchronizer> _versionSync;
};

TEST_F(TargetUpdaterTest, testNoUpdate) {
    EXPECT_CALL(*_leaderMgr, getRoleType(_)).Times(0);
    EXPECT_CALL(*_versionSync, syncFromPersist(_, _, _)).Times(0);
    TargetUpdater updater(_leaderMgr.get(), _versionMgr.get(), _versionSync.get());
    HeartbeatTarget target;
    auto targetBefore = target;
    ASSERT_EQ(UR_REACH_TARGET, updater.maybeUpdate(target));
    ASSERT_EQ(targetBefore.getTableMetas(), target.getTableMetas());

    auto tableMetas = TableMetaUtil::makeTableMetas(
        {"t1", "t2"}, {"1_/t1/config_/t1/index_indexlib_1", "1_/t2/config_/t2/index_indexlib_1"});
    target.setTableMetas(tableMetas);
    targetBefore = target;
    ASSERT_EQ(UR_REACH_TARGET, updater.maybeUpdate(target));
    ASSERT_EQ(targetBefore.getTableMetas(), target.getTableMetas());
}

TEST_F(TargetUpdaterTest, testUpdate) {
    HeartbeatTarget target;
    auto pid1 = TableMetaUtil::makePidFromStr("t1");
    auto pid2 = TableMetaUtil::makePidFromStr("t2");
    auto meta1 = TableMetaUtil::makeTargetFromStr("1_/t1/config_/t1/index_indexlib_1");
    auto meta2 = TableMetaUtil::makeTargetFromStr("1_/t2/config_/t2/index_indexlib_1_rw");
    TableMetas tableMetas;
    tableMetas[pid1] = meta1;
    tableMetas[pid2] = meta2;
    target.setTableMetas(tableMetas);
    ASSERT_EQ(RT_FOLLOWER, target._tableMetas[pid1].getRoleType());
    ASSERT_EQ(RT_FOLLOWER, target._tableMetas[pid2].getRoleType());
    ASSERT_EQ(1, target._tableMetas[pid1].getIncVersion());
    ASSERT_EQ(1, target._tableMetas[pid2].getIncVersion());

    EXPECT_CALL(*_leaderMgr, getRoleType(pid2)).WillOnce(Return(RT_LEADER));
    TableVersion version(2);
    EXPECT_CALL(*_versionSync, syncFromPersist(pid2, _, _)).WillOnce(DoAll(SetArgReferee<2>(version), Return(true)));

    TargetUpdater updater(_leaderMgr.get(), _versionMgr.get(), _versionSync.get());
    ASSERT_EQ(UR_REACH_TARGET, updater.maybeUpdate(target));

    ASSERT_EQ(RT_FOLLOWER, target._tableMetas[pid1].getRoleType());
    ASSERT_EQ(1, target._tableMetas[pid1].getIncVersion());

    ASSERT_EQ(RT_LEADER, target._tableMetas[pid2].getRoleType());
    ASSERT_EQ(2, target._tableMetas[pid2].getIncVersion());
    TableVersion leaderVersion;
    ASSERT_TRUE(_versionMgr->getLeaderVersion(pid2, leaderVersion));
    ASSERT_EQ(2, leaderVersion.getVersionId());
}

TEST_F(TargetUpdaterTest, testNeedRollback) {
    HeartbeatTarget target;

    auto pid1 = TableMetaUtil::makePidFromStr("t1");
    auto pid2 = TableMetaUtil::makePidFromStr("t2");
    auto indexRoot = GET_TEMP_DATA_PATH();
    auto partitionRoot = TablePathDefine::constructIndexPath(indexRoot, pid2);
    auto ec = fslib::fs::FileSystem::mkDir(partitionRoot, true);
    for (int i = 1; i <= 3; ++i) {
        auto versionFile = fslib::fs::FileSystem::joinFilePath(partitionRoot, "version." + std::to_string(i));
        ec = fslib::fs::FileSystem::writeFile(versionFile, "");
        ASSERT_EQ(fslib::EC_OK, ec);
    }

    auto meta1 = TableMetaUtil::makeTargetFromStr("1_/t1/config_/t1/index_indexlib_1");
    auto meta2 = TableMetaUtil::makeTargetFromStr("1_/t2/config_/t2/index_indexlib_1_rw");
    meta2.setRawIndexRoot(indexRoot);
    meta2.setBranchId(10);
    meta2.setRollbackTimestamp(220);
    TableMetas tableMetas;
    tableMetas[pid1] = meta1;
    tableMetas[pid2] = meta2;
    target.setTableMetas(tableMetas);
    ASSERT_EQ(1, target._tableMetas[pid1].getIncVersion());
    ASSERT_EQ(1, target._tableMetas[pid2].getIncVersion());

    EXPECT_CALL(*_leaderMgr, getRoleType(pid2)).WillOnce(Return(RT_LEADER));
    auto createTableVersion = [](versionid_t versionId, int64_t timestamp) {
        TableVersion version(versionId);
        version.setBranchId(1);
        indexlibv2::framework::VersionMeta meta;
        meta.TEST_Set(versionId, {}, timestamp);
        version.setVersionMeta(meta);
        return version;
    };
    TableVersion version1 = createTableVersion(1, 100);
    TableVersion version2 = createTableVersion(2, 200);
    TableVersion version3 = createTableVersion(3, 300);

    std::vector<TableVersion> versionChain({version1, version2, version3});
    EXPECT_CALL(*_versionSync, syncFromPersist(pid2, _, _)).WillOnce(DoAll(SetArgReferee<2>(version3), Return(true)));
    EXPECT_CALL(*_versionSync, getVersionList(pid2, _))
        .WillOnce(DoAll(SetArgReferee<1>(versionChain), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(versionChain), Return(true)));

    TargetUpdater updater(_leaderMgr.get(), _versionMgr.get(), _versionSync.get());
    ASSERT_EQ(UR_REACH_TARGET, updater.maybeUpdate(target));
    ASSERT_EQ(1, target._tableMetas[pid1].getIncVersion());
    ASSERT_EQ(2, target._tableMetas[pid2].getIncVersion());
}

} // namespace suez
