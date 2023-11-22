#include "suez/table/RollbackVersionSelector.h"

#include "MockVersionSynchronizer.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/framework/Version.h"
#include "suez/common/TablePathDefine.h"
#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"
using std::string;
namespace suez {

class RollbackVersionSelectorTest : public TESTBASE {
public:
    void setUp() override {}

protected:
};

TEST_F(RollbackVersionSelectorTest, testSelect) {
    auto pid = TableMetaUtil::makePid("table", 1, 0, 32767);
    std::string configPath = GET_TEST_DATA_PATH() + "/table_test/empty_config";
    auto indexRoot = GET_TEMP_DATA_PATH();
    auto partitionRoot = TablePathDefine::constructIndexPath(indexRoot, pid);
    auto ec = fslib::fs::FileSystem::mkDir(partitionRoot, true);
    ASSERT_EQ(fslib::EC_OK, ec);
    std::set<int32_t> indexVersions{0, 1, 3, 4, 5};
    std::vector<TableVersion> syncedVersions;
    std::set<int32_t> publishVersions{2, 3, 4, 5};
    for (int32_t i = 0; i < 5; ++i) {
        indexlibv2::framework::Version version(i);
        version.SetCommitTime((i + 1) * 50);

        if (indexVersions.find(i) != indexVersions.end()) {
            // write index version
            auto versionFile = fslib::fs::FileSystem::joinFilePath(partitionRoot, "version." + std::to_string(i));
            ec = fslib::fs::FileSystem::writeFile(versionFile, FastToJsonString(version));
            ASSERT_EQ(fslib::EC_OK, ec);
        }

        if (publishVersions.find(i) != publishVersions.end()) {
            indexlibv2::framework::VersionMeta versionMeta(version, 100, 100);
            TableVersion tableVersion(i, versionMeta, true);
            syncedVersions.push_back(tableVersion);
        }
    }

    MockVersionSynchronizer mockVersionSynchronizer;
    {
        // version5
        indexlibv2::framework::Version version(5);
        version.SetCommitTime(245);
        auto versionFile = fslib::fs::FileSystem::joinFilePath(partitionRoot, "version.5");
        ec = fslib::fs::FileSystem::writeFile(versionFile, FastToJsonString(version));
        ASSERT_EQ(fslib::EC_OK, ec);
        indexlibv2::framework::VersionMeta versionMeta(version, 100, 100);
        TableVersion tableVersion(5, versionMeta, true);
        syncedVersions.push_back(tableVersion);
    }
    {
        // version6
        indexlibv2::framework::Version version(6);
        version.SetCommitTime(250);
        auto versionFile = fslib::fs::FileSystem::joinFilePath(partitionRoot, "version.6");
        ec = fslib::fs::FileSystem::writeFile(versionFile, FastToJsonString(version));
        ASSERT_EQ(fslib::EC_OK, ec);
        indexlibv2::framework::VersionMeta versionMeta(version, 100, 100);
        TableVersion tableVersion(6, versionMeta, true);
        syncedVersions.push_back(tableVersion);
    }
    EXPECT_CALL(mockVersionSynchronizer, getVersionList(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgReferee<3>(syncedVersions), Return(true)));

    // index version                      published version      locator  branchid
    // version 0                                                 50       0
    // version 1                                                 100      0
    //                                    version2               150      0
    // version 3                          version3               200      0
    // version 4                          version4               250      0
    // version 5                          version5               245      1
    // version 6                          version6               250      1
    {
        //通过时间回滚，没有合适的版
        RollbackVersionSelector versionSelector(&mockVersionSynchronizer, configPath, indexRoot, pid);
        auto targetVersion = versionSelector.select(/*timestamp = */ 100, /*raw target version = */ 0);
        ASSERT_EQ(-1, targetVersion);
    }
    {
        //通过version回滚，但是索引不存在
        RollbackVersionSelector versionSelector(&mockVersionSynchronizer, configPath, indexRoot, pid);
        auto targetVersion = versionSelector.select(/*timestamp = */ 0, /*raw target version = */ 2);
        ASSERT_EQ(-1, targetVersion);
    }
    {
        // publish版本存在但是索引版本不存在
        RollbackVersionSelector versionSelector(&mockVersionSynchronizer, configPath, indexRoot, pid);
        auto targetVersion = versionSelector.select(/*timestamp = */ 150, /*raw target version = */ 0);
        ASSERT_EQ(-1, targetVersion);
    }
    {
        //正常选择版本
        RollbackVersionSelector versionSelector(&mockVersionSynchronizer, configPath, indexRoot, pid);
        auto targetVersion = versionSelector.select(/*timestamp = */ 200, /*raw target version = */ 0);
        ASSERT_EQ(3, targetVersion);
    }
    {
        //正常找版本
        RollbackVersionSelector versionSelector(&mockVersionSynchronizer, configPath, indexRoot, pid);
        auto targetVersion = versionSelector.select(/*timestamp = */ 245, /*raw target version = */ 0);
        ASSERT_EQ(5, targetVersion);
    }
    {
        //忽略version4
        RollbackVersionSelector versionSelector(&mockVersionSynchronizer, configPath, indexRoot, pid);
        auto targetVersion = versionSelector.select(/*timestamp = */ 250, /*raw target version = */ 0);
        ASSERT_EQ(6, targetVersion);
    }
}

} // namespace suez
