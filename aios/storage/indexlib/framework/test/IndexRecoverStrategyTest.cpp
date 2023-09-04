#include "indexlib/framework/IndexRecoverStrategy.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/framework/VersionCommitter.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class IndexRecoverStrategyTest : public TESTBASE
{
public:
    void setUp() override
    {
        auto rootDir = GET_TEMP_DATA_PATH();
        _directory = indexlib::file_system::Directory::GetPhysicalDirectory(rootDir);
    }
    void tearDown() override {}

private:
    void MakeSegments(const std::vector<segmentid_t>& segIds)
    {
        for (auto id : segIds) {
            std::string segmentName = std::string("segment_") + std::to_string(id) + "_level_0";
            auto segDir = _directory->MakeDirectory(segmentName);
            ASSERT_TRUE(segDir);
        }
    }
    void MakeVersion(const std::vector<segmentid_t> segIds, versionid_t versionId)
    {
        Version version;
        version.SetVersionId(versionId);
        for (auto id : segIds) {
            version.AddSegment(id);
        }
        auto versionFileName = std::string("version.") + std::to_string(versionId);
        _directory->Store(versionFileName, version.ToString());
    }

private:
    indexlib::file_system::DirectoryPtr _directory;
};

TEST_F(IndexRecoverStrategyTest, testRecoverFromEmptyDir)
{
    auto [status, version] = IndexRecoverStrategy::RecoverLatestVersion(_directory);
    ASSERT_TRUE(status.IsOK());
    ASSERT_FALSE(version.IsValid());
}

TEST_F(IndexRecoverStrategyTest, testRecover)
{
    MakeSegments({1, 2, 3, 4, 5});
    MakeVersion({1}, 0);
    MakeVersion({1, 3}, 1);
    ASSERT_TRUE(_directory->IsExist("segment_4_level_0"));
    ASSERT_TRUE(_directory->IsExist("segment_5_level_0"));
    auto [status, version] = IndexRecoverStrategy::RecoverLatestVersion(_directory);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(1, version.GetVersionId());
    ASSERT_EQ(3, version.GetLastSegmentId());
    ASSERT_FALSE(_directory->IsExist("segment_4_level_0"));
    ASSERT_FALSE(_directory->IsExist("segment_5_level_0"));
}

TEST_F(IndexRecoverStrategyTest, testRecoverDeleteAll)
{
    MakeSegments({0, 5});
    ASSERT_TRUE(_directory->IsExist("segment_0_level_0"));
    ASSERT_TRUE(_directory->IsExist("segment_5_level_0"));
    auto [status, version] = IndexRecoverStrategy::RecoverLatestVersion(_directory);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(INVALID_VERSIONID, version.GetVersionId());
    ASSERT_EQ(INVALID_SEGMENTID, version.GetLastSegmentId());
    ASSERT_FALSE(_directory->IsExist("segment_0_level_0"));
    ASSERT_FALSE(_directory->IsExist("segment_5_level_0"));
}

TEST_F(IndexRecoverStrategyTest, testRecoverDeleteNone)
{
    MakeSegments({0, 5});
    MakeVersion({5}, 1);
    ASSERT_TRUE(_directory->IsExist("segment_0_level_0"));
    ASSERT_TRUE(_directory->IsExist("segment_5_level_0"));
    auto [status, version] = IndexRecoverStrategy::RecoverLatestVersion(_directory);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(1, version.GetVersionId());
    ASSERT_EQ(5, version.GetLastSegmentId());
    ASSERT_TRUE(_directory->IsExist("segment_0_level_0"));
    ASSERT_TRUE(_directory->IsExist("segment_5_level_0"));
}

TEST_F(IndexRecoverStrategyTest, testRecoverFromNonExistDir)
{
    auto dir = indexlib::file_system::Directory::GetPhysicalDirectory("1234567");
    auto [status, version] = IndexRecoverStrategy::RecoverLatestVersion(dir);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(INVALID_VERSIONID, version.GetVersionId());
}

} // namespace indexlibv2::framework
