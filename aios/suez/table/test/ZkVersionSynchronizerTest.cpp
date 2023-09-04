#include "suez/table/ZkVersionSynchronizer.h"

#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class ZkVersionSynchronizerTest : public TESTBASE {
public:
    void setUp() override {
        unsigned short port = fslib::fs::zookeeper::ZkServer::getZkServer()->port();
        ostringstream oss;
        oss << "127.0.0.1:" << port;
        _zkHost = oss.str();
    }

    void tearDown() override { fslib::fs::zookeeper::ZkServer::getZkServer()->stop(); }

protected:
    string _zkHost;
};

TEST_F(ZkVersionSynchronizerTest, testSimple) {
    VersionSynchronizerConfig config;
    config.zkRoot = "zfs://" + _zkHost + "/" + GET_CLASS_NAME();

    ZkVersionSynchronizer synchronizer(config);
    ASSERT_TRUE(synchronizer.init());

    auto pid = TableMetaUtil::makePid("t1", 0, 1, 2);
    indexlibv2::framework::VersionMeta meta;
    meta.TEST_Set(1, {}, {});
    TableVersion version(1, meta, "testSimple");
    version.setBranchId(1);
    ASSERT_TRUE(synchronizer.persistVersion(pid, "", version));

    TableVersion version2;
    // using the same ZkState, zkState->read will returns EC_OK instead of EC_UPDATE
    ASSERT_TRUE(synchronizer.syncFromPersist(pid, "", version2));
    ASSERT_EQ(INVALID_VERSION, version2.getVersionId());

    synchronizer._states.clear();
    ASSERT_TRUE(synchronizer.syncFromPersist(pid, "", version2));
    ASSERT_TRUE(version == version2);
}

TEST_F(ZkVersionSynchronizerTest, testAutoReconnect) {
    VersionSynchronizerConfig config;
    config.zkRoot = "zfs://" + _zkHost + "/" + GET_CLASS_NAME();

    ZkVersionSynchronizer synchronizer(config);
    ASSERT_TRUE(synchronizer.init());

    auto pid = TableMetaUtil::makePid("t1", 0, 1, 2);
    indexlibv2::framework::VersionMeta meta;
    meta.TEST_Set(1, {}, {});
    TableVersion version(1, meta, "testSimple");
    version.setBranchId(1);
    ASSERT_TRUE(synchronizer.persistVersion(pid, "", version));
    ASSERT_TRUE(synchronizer.updateVersionList(pid, {version}));

    ASSERT_TRUE(synchronizer._zkWrapper.get() != nullptr);
    ASSERT_EQ(2, synchronizer._states.size());
    auto zk1 = synchronizer._zkWrapper;
    auto state1 = synchronizer._states[make_pair(pid, ZkVersionSynchronizer::VST_VERSION)];
    auto state2 = synchronizer._states[make_pair(pid, ZkVersionSynchronizer::VST_VERSION_LIST)];
    synchronizer._zkWrapper->close();

    ASSERT_TRUE(synchronizer.persistVersion(pid, "", version));
    ASSERT_EQ(1, synchronizer._states.size());
    ASSERT_NE(zk1.get(), synchronizer._zkWrapper.get());
    ASSERT_NE(state1.get(), synchronizer._states[make_pair(pid, ZkVersionSynchronizer::VST_VERSION)].get());

    ASSERT_TRUE(synchronizer.updateVersionList(pid, {version}));
    ASSERT_EQ(2, synchronizer._states.size());
    ASSERT_NE(state2.get(), synchronizer._states[make_pair(pid, ZkVersionSynchronizer::VST_VERSION_LIST)].get());
}

TEST_F(ZkVersionSynchronizerTest, testVersionList) {
    VersionSynchronizerConfig config;
    config.zkRoot = "zfs://" + _zkHost + "/" + GET_CLASS_NAME();

    ZkVersionSynchronizer synchronizer(config);
    ASSERT_TRUE(synchronizer.init());

    auto pid = TableMetaUtil::makePid("t1", 0, 1, 2);
    indexlibv2::framework::VersionMeta meta;
    meta.TEST_Set(1, {}, {});

    std::vector<TableVersion> readedVersions;
    ASSERT_TRUE(synchronizer.getVersionList(pid, readedVersions));
    ASSERT_EQ(0, readedVersions.size());

    std::vector<TableVersion> versions;
    TableVersion version(1, meta, "testSimple");
    version.setBranchId(1);
    versions.push_back(version);

    ASSERT_TRUE(synchronizer.updateVersionList(pid, versions));

    ASSERT_TRUE(synchronizer.getVersionList(pid, readedVersions));
    ASSERT_EQ(versions, readedVersions);
}

} // namespace suez
