#include "suez/table/VersionManager.h"

#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class VersionManagerTest : public TESTBASE {};

TEST_F(VersionManagerTest, testSimple) {
    VersionManager vm;

    auto pid = TableMetaUtil::makePid("t1", 0, 1, 2);
    TableVersion version;
    ASSERT_FALSE(vm.getLocalVersion(pid, version));
    ASSERT_FALSE(vm.getLeaderVersion(pid, version));
    ASSERT_FALSE(vm.getPublishedVersion(pid, version));
    indexlibv2::framework::VersionMeta meta;
    meta.TEST_Set(4, {}, {});

    version.setVersionId(4);
    version.setGenerator("leader_1");

    ASSERT_TRUE(vm.updateLocalVersion(pid, version));
    ASSERT_TRUE(vm.updateLeaderVersion(pid, version));
    ASSERT_TRUE(vm.updatePublishedVersion(pid, version));

    {
        TableVersion version2;
        ASSERT_TRUE(vm.getLocalVersion(pid, version2));
        ASSERT_TRUE(version2 == version);
        version2.setVersionId(3);
        ASSERT_FALSE(vm.updateLocalVersion(pid, version2));
    }
    {
        TableVersion version2;
        ASSERT_TRUE(vm.getLeaderVersion(pid, version2));
        ASSERT_TRUE(version2 == version);
        version2.setVersionId(3);
        ASSERT_FALSE(vm.updateLeaderVersion(pid, version2));
    }
    {
        TableVersion version2;
        ASSERT_TRUE(vm.getPublishedVersion(pid, version2));
        ASSERT_TRUE(version2 == version);
        version2.setVersionId(3);
        ASSERT_FALSE(vm.updatePublishedVersion(pid, version2));
    }

    vm.remove(pid);
    ASSERT_FALSE(vm.getLeaderVersion(pid, version));
    ASSERT_FALSE(vm.getLocalVersion(pid, version));
    ASSERT_FALSE(vm.getPublishedVersion(pid, version));
}

} // namespace suez
