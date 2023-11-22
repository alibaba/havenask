#include "suez/table/BsVersionSynchronizer.h"

#include "autil/EnvUtil.h"
#include "build_service/common/RemoteVersionCommitter.h"
#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using build_service::common::RemoteVersionCommitter;

namespace suez {

class BsVersionSynchronizerTest : public TESTBASE {
public:
    void setUp() override {}
    void tearDown() override {}

protected:
};

TEST_F(BsVersionSynchronizerTest, testCreate) {
    std::string configStr = R"({"zk_root":"BS"})";
    autil::EnvGuard envGuard("version_sync_config", configStr);
    auto synchronizerBase = VersionSynchronizer::create();
    ASSERT_TRUE(synchronizerBase);
    auto synchronizer = dynamic_cast<BsVersionSynchronizer *>(synchronizerBase);
    ASSERT_TRUE(synchronizer);
    delete synchronizerBase;
}

class MockRemoteVersionCommitter : public RemoteVersionCommitter {
public:
    MockRemoteVersionCommitter() : RemoteVersionCommitter() {}
    MOCK_METHOD(Status, Init, (const RemoteVersionCommitter::InitParam &), (override));
    MOCK_METHOD(Status, Commit, (const indexlibv2::framework::VersionMeta &), (override));
    MOCK_METHOD(Status,
                GetCommittedVersions,
                (uint32_t count, std::vector<indexlibv2::versionid_t> &versions),
                (override));
};

class FakeBsVersionSynchronizer : public BsVersionSynchronizer {
public:
    FakeBsVersionSynchronizer(const VersionSynchronizerConfig &config) : BsVersionSynchronizer(config) {
        _committer.reset(new MockRemoteVersionCommitter);
    }
    std::shared_ptr<build_service::common::RemoteVersionCommitter> createCommitter() const override {
        return _committer;
    }

public:
    std::shared_ptr<MockRemoteVersionCommitter> _committer;
};

TEST_F(BsVersionSynchronizerTest, testPersistVersion) {
    const string configPath = GET_TEST_DATA_PATH() + "/table_test/tablet_test/rt_build/config/0/";
    int32_t generationId = 12345;
    auto checkFunc = [&](const RemoteVersionCommitter::InitParam &initParam) -> Status {
        if (initParam.rangeFrom != 0 || initParam.rangeTo != 128 || initParam.generationId != generationId ||
            initParam.dataTable != "orc" || initParam.clusterName != "orc" || initParam.configPath != configPath) {
            return Status::Corruption("invalid atgs");
        }
        return Status::OK();
    };
    indexlibv2::framework::VersionMeta versionMeta;
    versionMeta.TEST_Set(10, {}, 100);
    auto checkVersionMeta = [versionMeta](const indexlibv2::framework::VersionMeta &args) -> Status {
        if (args == versionMeta) {
            return Status::OK();
        }
        return Status::Corruption("version meta differ");
    };
    FakeBsVersionSynchronizer synchronizer({});
    EXPECT_CALL(*synchronizer._committer, Init(_)).WillOnce(Invoke(bind(checkFunc, std::placeholders::_1)));
    EXPECT_CALL(*synchronizer._committer, Commit(_)).WillOnce(Invoke(bind(checkVersionMeta, std::placeholders::_1)));

    auto pid = TableMetaUtil::makePid("orc", /*fullVersion*/ generationId, /*rangeFrom*/ 0, /*rangeTo*/ 128);
    TableVersion tableVersion(10, versionMeta);
    ASSERT_TRUE(synchronizer.init());
    ASSERT_TRUE(synchronizer.persistVersion(pid, "", "", configPath, tableVersion));
}

TEST_F(BsVersionSynchronizerTest, testUselessFunction) {
    BsVersionSynchronizer synchronizer({});
    ASSERT_TRUE(synchronizer.init());
    auto pid = TableMetaUtil::makePid("orc", /*fullVersion*/ 12345, /*rangeFrom*/ 0, /*rangeTo*/ 128);
    synchronizer.remove(pid);
    TableVersion tableVersion;
    std::vector<TableVersion> versions;
    ASSERT_TRUE(synchronizer.getVersionList(pid, "", "", versions));
    ASSERT_TRUE(synchronizer.updateVersionList(pid, "", "", versions));
}

TEST_F(BsVersionSynchronizerTest, testSyncFromPersist) {
    auto checkFunction =
        [this](const std::string &syncFromBsAdminOption, bool isLeader, bool expectedSupportSyncFromPersist) {
            std::cerr << "test mode " << syncFromBsAdminOption << " leader " << isLeader << " expected "
                      << expectedSupportSyncFromPersist << std::endl;
            PartitionMeta target;
            target.setRoleType(isLeader ? RT_LEADER : RT_FOLLOWER);
            VersionSynchronizerConfig config;
            config.syncFromBsAdmin = syncFromBsAdminOption;
            const string configPath = GET_TEST_DATA_PATH() + "/table_test/tablet_test/rt_build/config/0/";

            int32_t generationId = 12345;
            std::vector<versionid_t> committedVersions;
            auto commitFunc = [&](const indexlibv2::framework::VersionMeta &versionMeta) -> Status {
                committedVersions.push_back(versionMeta.GetVersionId());
                return Status::OK();
            };
            indexlibv2::framework::VersionMeta versionMeta;
            versionMeta.TEST_Set(10, {}, 100);
            FakeBsVersionSynchronizer synchronizer(config);
            ON_CALL(*synchronizer._committer, Init(_)).WillByDefault(Return(Status::OK()));
            ON_CALL(*synchronizer._committer, Commit(_)).WillByDefault(Invoke(bind(commitFunc, std::placeholders::_1)));

            auto pid = TableMetaUtil::makePid("orc", /*fullVersion*/ generationId, /*rangeFrom*/ 0, /*rangeTo*/ 128);
            TableVersion tableVersion(10, versionMeta);
            ASSERT_TRUE(synchronizer.init());
            ASSERT_TRUE(synchronizer.persistVersion(pid, "", "", configPath, tableVersion));
            ON_CALL(*synchronizer._committer, GetCommittedVersions(1, _))
                .WillByDefault(DoAll(SetArgReferee<1>(committedVersions), Return(Status::OK())));
            if (expectedSupportSyncFromPersist) {
                TableVersion version;
                ASSERT_TRUE(synchronizer.supportSyncFromPersist(target));
                ASSERT_TRUE(synchronizer.syncFromPersist(pid, "", "", configPath, version));
                ASSERT_EQ(10, version.getVersionId());
            } else {
                ASSERT_FALSE(synchronizer.supportSyncFromPersist(target));
            }
        };
    checkFunction("leader_only", true, true);
    checkFunction("leader_only", false, false);

    checkFunction("all", true, true);
    checkFunction("all", false, true);

    checkFunction("", true, false);
    checkFunction("", false, false);
}

} // namespace suez
