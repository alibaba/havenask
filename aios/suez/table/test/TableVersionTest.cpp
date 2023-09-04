#include "suez/table/TableVersion.h"

#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class TableVersionTest : public TESTBASE {};

TEST_F(TableVersionTest, testConstructor) {
    TableVersion version1;
    ASSERT_EQ(indexlibv2::INVALID_VERSIONID, version1.getVersionId());
    ASSERT_EQ(indexlibv2::INVALID_VERSIONID, version1.getVersionMeta().GetVersionId());
    ASSERT_EQ(0, version1.getBranchId());
    ASSERT_FALSE(version1.isFinished());
    ASSERT_EQ("", version1.getGenerator());

    indexlibv2::framework::VersionMeta meta;
    meta.TEST_Set(1, {0, 1}, {});
    TableVersion version2(1, meta, false);
    ASSERT_EQ(1, version2.getVersionId());
    ASSERT_EQ(1, version2.getVersionMeta().GetVersionId());
    ASSERT_EQ(2u, version2.getVersionMeta().GetSegments().size());
    ASSERT_FALSE(version2.isFinished());
    ASSERT_FALSE(version2.getGenerator().empty());

    TableVersion version3(1, meta, true);
    ASSERT_EQ(1, version3.getVersionId());
    ASSERT_TRUE(version3.isFinished());
    ASSERT_FALSE(version3.getGenerator().empty());
}

TEST_F(TableVersionTest, testOperator) {
    TableVersion v1;
    ASSERT_TRUE(v1 == v1);

    TableVersion v2;
    ASSERT_TRUE(v1 == v2);

    v1.setVersionId(1);
    ASSERT_FALSE(v1 == v2);

    TableVersion v3;
    v3.setBranchId(100);
    ASSERT_FALSE(v3 == v2);

    v3.setBranchId(0);
    ASSERT_TRUE(v3 == v2);
}

TEST_F(TableVersionTest, testSetEpochId) {
    TableVersion v;
    v.setBranchId(100);
    ASSERT_EQ(100, v.getBranchId());
}

TEST_F(TableVersionTest, testJsonize) {
    {
        TableVersion v1;
        auto str = FastToJsonString(v1);
        TableVersion v2;
        try {
            FastFromJsonString(v2, str);
            ASSERT_TRUE(v1 == v2);
        } catch (...) { ASSERT_TRUE(false) << "parse from " << str << " failed"; }
    }

    {
        indexlibv2::framework::VersionMeta meta;
        meta.TEST_Set(1, {1, 2, 3}, {});
        TableVersion v1(1, meta, true);
        v1.setBranchId(100);
        TableVersion v2;
        try {
            FastFromJsonString(v2, FastToJsonString(v1));
            ASSERT_TRUE(v1 == v2);
        } catch (...) { ASSERT_TRUE(false); }
    }
}

} // namespace suez
