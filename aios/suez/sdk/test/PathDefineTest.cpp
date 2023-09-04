#include "suez/sdk/PathDefine.h"

#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class PathDefineTest : public TESTBASE {};

TEST_F(PathDefineTest, testJoin) {
    EXPECT_EQ("path/file", PathDefine::join("path", "file"));
    EXPECT_EQ("path/file", PathDefine::join("path/", "file"));
    EXPECT_EQ("path/file/", PathDefine::join("path/", "file/"));
}

TEST_F(PathDefineTest, testgetFsType) {
    EXPECT_EQ("dcache", PathDefine::getFsType("dcache://hdfs/et2prod2/path/to/file"));
    EXPECT_EQ("LOCAL", PathDefine::getFsType("/home/admin/hippo/worker/slave/path/to/file"));
}

TEST_F(PathDefineTest, testgetTableConfigFileName) {
    EXPECT_EQ("clusters/table_name_cluster.json", PathDefine::getTableConfigFileName("table_name"));
}

TEST_F(PathDefineTest, testgetLeaderElectionRoot) {
    EXPECT_EQ("zfs://root/leader_election", PathDefine::getLeaderElectionRoot("zfs://root/"));
}

TEST_F(PathDefineTest, testgetVersionStateRoot) {
    EXPECT_EQ("zfs://root/versions", PathDefine::getVersionStateRoot("zfs://root/"));
}

} // namespace suez
