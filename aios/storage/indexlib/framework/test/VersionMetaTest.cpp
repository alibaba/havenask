#include "indexlib/framework/VersionMeta.h"

#include "indexlib/framework/Version.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class VersionMetaTest : public TESTBASE
{
public:
    VersionMetaTest() = default;
    ~VersionMetaTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void VersionMetaTest::setUp() {}

void VersionMetaTest::tearDown() {}

TEST_F(VersionMetaTest, testConstruct)
{
    Version version(1 | Version::PUBLIC_VERSION_ID_MASK);
    std::vector<base::Progress> progress;
    progress.push_back({0, 100, {2023, 0}});
    progress.push_back({101, 200, {2000, 0}});
    Locator locator;
    locator.SetMultiProgress({progress});
    version.SetLocator(locator);
    version.SetCommitTime(1234);
    version.Finalize();

    VersionMeta meta(version, 100, 200);
    ASSERT_EQ(1 | Version::PUBLIC_VERSION_ID_MASK, meta.GetVersionId());
    ASSERT_EQ(1234, meta.GetCommitTime());
    ASSERT_EQ(100, meta.GetDocCount());
    ASSERT_EQ(200, meta.GetIndexSize());
    ASSERT_EQ(2000, meta.GetMinLocatorTs());
    ASSERT_EQ(2023, meta.GetMaxLocatorTs());
    VersionLine versionLine;
    versionLine.AddCurrentVersion(1 | Version::PUBLIC_VERSION_ID_MASK);
    ASSERT_EQ(meta.GetVersionLine(), versionLine);
}

TEST_F(VersionMetaTest, testJsonize)
{
    Version version(1 | Version::PUBLIC_VERSION_ID_MASK);
    Locator locator(0, 2023);
    version.SetLocator(locator);
    version.SetCommitTime(1234);
    version.Finalize();
    VersionMeta meta(version, 100, 200);
    VersionMeta meta2;
    FromJsonString(meta2, ToJsonString(meta));
    ASSERT_EQ(meta, meta2);
}
} // namespace indexlibv2::framework
