#include "indexlib/framework/VersionLine.h"

#include "indexlib/base/Constant.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class VersionLineTest : public TESTBASE
{
public:
    VersionLineTest() = default;
    ~VersionLineTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void VersionLineTest::setUp() {}

void VersionLineTest::tearDown() {}

TEST_F(VersionLineTest, testUsage)
{
    VersionLine line1;
    line1.AddCurrentVersion(VersionCoord(1, "fence_a"));
    line1.AddCurrentVersion(VersionCoord(2, "fence_a"));
    line1.AddCurrentVersion(VersionCoord(3, "fence_a"));

    VersionLine line2 = line1;
    line2.AddCurrentVersion(VersionCoord(6, "fence_b"));

    VersionLine line3 = line2;
    line3.AddCurrentVersion(VersionCoord(8, "fence_c"));

    line1.AddCurrentVersion(VersionCoord(4, "fence_a"));
    line1.AddCurrentVersion(VersionCoord(5, "fence_a"));

    ASSERT_TRUE(line1.CanFastFowardFrom(VersionCoord(INVALID_VERSIONID, ""), /*isDirty*/ false));
    ASSERT_TRUE(line1.CanFastFowardFrom(VersionCoord(1, "fence_a"), /*isDirty*/ false));
    ASSERT_TRUE(line1.CanFastFowardFrom(VersionCoord(2, "fence_a"), /*isDirty*/ false));
    ASSERT_TRUE(line1.CanFastFowardFrom(VersionCoord(3, "fence_a"), /*isDirty*/ false));
    ASSERT_TRUE(line1.CanFastFowardFrom(VersionCoord(4, "fence_a"), /*isDirty*/ false));
    ASSERT_TRUE(line1.CanFastFowardFrom(VersionCoord(5, "fence_a"), /*isDirty*/ false));
    ASSERT_FALSE(line1.CanFastFowardFrom(VersionCoord(10, "fence_a"), /*isDirty*/ false));
    ASSERT_FALSE(line1.CanFastFowardFrom(VersionCoord(6, "fence_b"), /*isDirty*/ false));

    line2.AddCurrentVersion(VersionCoord(7, "fence_b"));

    ASSERT_TRUE(line2.CanFastFowardFrom(VersionCoord(INVALID_VERSIONID, ""), /*isDirty*/ false));
    ASSERT_TRUE(line2.CanFastFowardFrom(VersionCoord(1, "fence_a"), /*isDirty*/ false));
    ASSERT_TRUE(line2.CanFastFowardFrom(VersionCoord(2, "fence_a"), /*isDirty*/ false));
    ASSERT_TRUE(line2.CanFastFowardFrom(VersionCoord(3, "fence_a"), /*isDirty*/ false));
    ASSERT_FALSE(line2.CanFastFowardFrom(VersionCoord(4, "fence_a"), /*isDirty*/ false));
    ASSERT_FALSE(line2.CanFastFowardFrom(VersionCoord(5, "fence_a"), /*isDirty*/ false));
    ASSERT_TRUE(line2.CanFastFowardFrom(VersionCoord(6, "fence_b"), /*isDirty*/ false));

    line3.AddCurrentVersion(VersionCoord(9, "fence_c"));
    ASSERT_TRUE(line3.CanFastFowardFrom(VersionCoord(1, "fence_a"), /*isDirty*/ false));
    ASSERT_TRUE(line3.CanFastFowardFrom(VersionCoord(2, "fence_a"), /*isDirty*/ false));
    ASSERT_TRUE(line3.CanFastFowardFrom(VersionCoord(3, "fence_a"), /*isDirty*/ false));
    ASSERT_FALSE(line3.CanFastFowardFrom(VersionCoord(4, "fence_a"), /*isDirty*/ false));
    ASSERT_FALSE(line3.CanFastFowardFrom(VersionCoord(5, "fence_a"), /*isDirty*/ false));
    ASSERT_TRUE(line3.CanFastFowardFrom(VersionCoord(6, "fence_b"), /*isDirty*/ false));
    ASSERT_FALSE(line3.CanFastFowardFrom(VersionCoord(7, "fence_b"), /*isDirty*/ false));
    ASSERT_TRUE(line3.CanFastFowardFrom(VersionCoord(8, "fence_c"), /*isDirty*/ false));
    ASSERT_TRUE(line3.CanFastFowardFrom(VersionCoord(9, "fence_c"), /*isDirty*/ false));

    std::vector<std::pair<std::string, versionid_t>> expectedKeys1({{"fence_a", 5}});
    ASSERT_EQ(expectedKeys1, line1._keyVersions);

    std::vector<std::pair<std::string, versionid_t>> expectedKeys2({{"fence_a", 3}, {"fence_b", 7}});
    ASSERT_EQ(expectedKeys2, line2._keyVersions);

    std::vector<std::pair<std::string, versionid_t>> expectedKeys3({{"fence_a", 3}, {"fence_b", 6}, {"fence_c", 9}});
    ASSERT_EQ(expectedKeys3, line3._keyVersions);

    ASSERT_EQ(4, line1.GetParentVersion().GetVersionId());
}

TEST_F(VersionLineTest, testJsonize)
{
    VersionLine line1;
    line1.AddCurrentVersion(VersionCoord(1, "fence_a"));
    line1.AddCurrentVersion(VersionCoord(2, "fence_a"));
    line1.AddCurrentVersion(VersionCoord(3, "fence_a"));

    VersionLine line2;
    FromJsonString(line2, ToJsonString(line1));
    ASSERT_EQ(line1, line2);
}

TEST_F(VersionLineTest, testKeyNodeLimit)
{
    VersionLine line1;
    line1.AddCurrentVersion(VersionCoord(2, "fence_b"));
    line1.AddCurrentVersion(VersionCoord(3, "fence_c"));
    for (int i = 0; i < 20; ++i) {
        line1.AddCurrentVersion(VersionCoord(i, "fence_" + std::to_string(i)));
    }
    ASSERT_EQ(10, line1._keyVersions.size());
}

} // namespace indexlibv2::framework
