#include "indexlib/table/common/CommonVersionImporter.h"

#include "unittest/unittest.h"

using namespace std;
using namespace indexlibv2::framework;

namespace indexlibv2 { namespace table {

class CommonVersionImporterTest : public TESTBASE
{
public:
    CommonVersionImporterTest();
    ~CommonVersionImporterTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.framework, CommonVersionImporterTest);

CommonVersionImporterTest::CommonVersionImporterTest() {}

CommonVersionImporterTest::~CommonVersionImporterTest() {}

TEST_F(CommonVersionImporterTest, testCheck)
{
    // default import strategy : NOT_SUPPORT
    framework::ImportOptions options;
    options.SetImportStrategy(CommonVersionImporter::NOT_SUPPORT);
    CommonVersionImporter importer;
    {
        std::vector<Version> validVersions;
        Version baseVersion(/*versionId=*/0, /*timestamp=*/100);
        baseVersion.AddSegment(0);
        auto status = importer.Check({}, &baseVersion, options, &validVersions);
        ASSERT_TRUE(status.IsExist());
    }
    {
        std::vector<Version> validVersions;
        Version baseVersion(/*versionId=*/0, /*timestamp=*/100);
        baseVersion.AddSegment(0);
        Version version1(/*versionId=*/0, /*timestamp=*/50);
        version1.SetSchemaId(1);
        version1.AddSegment(2);
        version1.AddSegment(4);
        Locator locator1(/*src=*/0, /*minOffset=*/50);
        locator1.ShrinkToRange(0, 32767);
        version1.SetLocator(locator1);
        auto status = importer.Check({version1}, &baseVersion, options, &validVersions);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(validVersions.size(), 1u);
    }
    {
        std::vector<Version> validVersions;
        Version baseVersion(/*versionId=*/0, /*timestamp=*/100);
        Locator baseLocator(/*src=*/1, /*minOffset=*/100);
        baseVersion.AddSegment(0);
        baseVersion.SetLocator(baseLocator);
        Version version1(/*versionId=*/0, /*timestamp=*/50);
        version1.SetSchemaId(1);
        version1.AddSegment(2);
        version1.AddSegment(4);
        Locator locator1(/*src=*/1, /*minOffset=*/50);
        locator1.ShrinkToRange(0, 32767);
        version1.SetLocator(locator1);
        auto status = importer.Check({version1}, &baseVersion, options, &validVersions);
        ASSERT_TRUE(status.IsExist());
    }
    {
        std::vector<Version> validVersions;
        Version baseVersion(/*versionId=*/0, /*timestamp=*/100);
        Locator baseLocator(/*src=*/1, /*minOffset=*/100);
        baseVersion.AddSegment(0);
        baseVersion.SetLocator(baseLocator);
        Version version1(/*versionId=*/1, /*timestamp=*/50);
        version1.AddSegment(2);
        version1.AddSegment(4);
        Locator locator1(/*src=*/1, /*minOffset=*/50);
        locator1.ShrinkToRange(0, 32767);
        version1.SetLocator(locator1);

        Version version2(/*versionId=*/2, /*timestamp=*/150);
        version2.AddSegment(1);
        version2.AddSegment(3);
        Locator locator2(/*src=*/1, /*minOffset=*/150);
        locator2.ShrinkToRange(32768, 65535);
        version2.SetLocator(locator2);

        auto status = importer.Check({version1, version2}, &baseVersion, options, &validVersions);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(validVersions.size(), 1u);
        ASSERT_EQ(validVersions[0].GetVersionId(), 2);
    }
    {
        // // test base version is null
        std::vector<Version> validVersions;
        auto status = importer.Check({}, nullptr, options, &validVersions);
        ASSERT_FALSE(status.IsOK());
        ASSERT_TRUE(status.IsInternalError());
    }
    {
        // test import versions with diffent src
        std::vector<Version> validVersions;
        Version version1(/*versionId*/ 1, /*timestamp*/ 50);
        version1.SetLocator(Locator(/*src*/ 0, /*minOffset*/ 50));
        version1.AddSegment(0);
        Version version2(/*versionId*/ 2, /*timestamp*/ 60);
        version2.SetLocator(Locator(/*src*/ 1, /*minOffset*/ 60));
        version2.AddSegment(1);

        Version baseVersion(/*versionId=*/0, /*timestamp=*/100);
        auto status = importer.Check({version1, version2}, &baseVersion, options, &validVersions);
        ASSERT_TRUE(status.IsInvalidArgs());
    }
    options.SetImportStrategy(CommonVersionImporter::KEEP_SEGMENT_IGNORE_LOCATOR);
    {
        // test diffent src, base version locator faster than import versions
        std::vector<Version> validVersions;
        Version version1(/*versionId*/ 1, /*timestamp*/ 50);
        version1.SetLocator(Locator(/*src*/ 1, /*minOffset*/ 50));
        version1.AddSegment(0);
        Version version2(/*versionId*/ 2, /*timestamp*/ 60);
        version2.SetLocator(Locator(/*src*/ 1, /*minOffset*/ 60));
        version2.AddSegment(1);

        Version baseVersion(/*versionId=*/0, /*timestamp=*/100);
        baseVersion.SetLocator(Locator(/*src*/ 2, /*minOffset*/ 100));
        auto status = importer.Check({version1, version2}, &baseVersion, options, &validVersions);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(2u, validVersions.size());
        ASSERT_EQ(1, validVersions[0].GetVersionId());
        ASSERT_EQ(2, validVersions[1].GetVersionId());
    }
    options.SetImportStrategy(CommonVersionImporter::KEEP_SEGMENT_OVERWRITE_LOCATOR);
    {
        // test diffent src, base version locator faster than import versions
        std::vector<Version> validVersions;
        Version version1(/*versionId*/ 1, /*timestamp*/ 50);
        version1.SetLocator(Locator(/*src*/ 1, /*minOffset*/ 50));
        version1.AddSegment(0);

        Version version2(/*versionId*/ 2, /*timestamp*/ 60);
        version2.SetLocator(Locator(/*src*/ 1, /*minOffset*/ 60));
        version2.AddSegment(1);

        Version baseVersion(/*versionId=*/0, /*timestamp=*/100);
        baseVersion.SetLocator(Locator(/*src*/ 2, /*minOffset*/ 100));
        auto status = importer.Check({version1, version2}, &baseVersion, options, &validVersions);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(2u, validVersions.size());
        ASSERT_EQ(1, validVersions[0].GetVersionId());
        ASSERT_EQ(2, validVersions[1].GetVersionId());
    }
}

}} // namespace indexlibv2::table
