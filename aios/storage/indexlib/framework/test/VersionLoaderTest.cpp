#include "indexlib/framework/VersionLoader.h"

#include <string>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/framework/Version.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::framework {

class VersionLoaderTest : public TESTBASE
{
public:
    VersionLoaderTest() {}
    ~VersionLoaderTest() {}

    void setUp() override
    {
        indexlib::file_system::FileSystemOptions fsOptions;
        std::string rootPath = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("online", rootPath, fsOptions).GetOrThrow();
        _rootDir = indexlib::file_system::Directory::Get(fs);
    }
    void tearDown() override {}

private:
    void CreateDirectory(const std::string& dir) { _rootDir->MakeDirectory(dir); }
    void CreateFile(const std::string& fileName, const std::string& content = std::string(""));

private:
    std::shared_ptr<indexlib::file_system::Directory> _rootDir;
};

void VersionLoaderTest::CreateFile(const std::string& fileName, const std::string& content)
{
    _rootDir->Store(fileName, content);
}

TEST_F(VersionLoaderTest, TestListSegment)
{
    CreateDirectory("segment_0");
    CreateDirectory("segment_0~");
    CreateDirectory("segment_0_bak");
    CreateDirectory("segment_0_leel_0");
    CreateDirectory("segment_0level_0");
    CreateDirectory("segment_0_level0");
    CreateDirectory("segment_0_level_0");
    CreateDirectory("segment_0_level_1_");
    CreateDirectory("segment_1");
    CreateDirectory("segment_1_level_11");
    CreateDirectory("segment_11");
    CreateDirectory("segment_2");
    CreateDirectory("segment_0.bak");
    CreateDirectory("_segment_2");
    CreateDirectory("segment_8");
    CreateDirectory("segment_1000_level_0");
    CreateFile("segment_12_level_12"); // ...

    fslib::FileList fileList;
    ASSERT_TRUE(VersionLoader::ListSegment(_rootDir, &fileList).IsOK());
    ASSERT_EQ(4, fileList.size());
    ASSERT_EQ(std::string("segment_0_level_0"), fileList[0]);
    ASSERT_EQ(std::string("segment_1_level_11"), fileList[1]);
    ASSERT_EQ(std::string("segment_12_level_12"), fileList[2]);
    ASSERT_EQ(std::string("segment_1000_level_0"), fileList[3]);
}

TEST_F(VersionLoaderTest, TestListVersion)
{
    CreateFile("version.1");
    CreateFile("_version.1");
    CreateFile("version.1_bak");
    CreateFile("version.1.bak");
    CreateFile("version_.1");
    CreateFile("version._1.bak");
    CreateFile("version.1_.bak");
    CreateFile("version.1._bak");
    CreateFile("version.1~");
    CreateFile("version.11");
    CreateFile("version.111");
    CreateFile("version.31");

    fslib::FileList fileList;
    ASSERT_TRUE(VersionLoader::ListVersion(_rootDir, &fileList).IsOK());
    ASSERT_EQ(4, fileList.size());
    ASSERT_EQ(std::string("version.1"), fileList[0]);
    ASSERT_EQ(std::string("version.11"), fileList[1]);
    ASSERT_EQ(std::string("version.31"), fileList[2]);
    ASSERT_EQ(std::string("version.111"), fileList[3]);
}

TEST_F(VersionLoaderTest, TestGetVersion)
{
    Version version;
    fslib::FileList fileList;
    ASSERT_TRUE(VersionLoader::GetVersion(_rootDir, INVALID_VERSIONID, &version).IsOK());

    // no version exist
    CreateFile("_version.1");
    CreateFile("version.1_bak");
    CreateFile("version.1.bak");
    CreateFile("version_.1");
    CreateFile("version._1.bak");
    CreateFile("version.1_.bak");
    CreateFile("version.1._bak");
    CreateFile("version.1~");
    ASSERT_TRUE(VersionLoader::GetVersion(_rootDir, INVALID_VERSIONID, &version).IsOK());

    // not specify vid, version exist, but invalid
    const std::string invalidVersionContent("invalid_vesion_content");
    CreateFile("version.1", invalidVersionContent);
    ASSERT_TRUE(VersionLoader::GetVersion(_rootDir, INVALID_VERSIONID, &version).IsConfigError());

    // not specify vid, version exist, and valid
    const std::string validVersionContent1 =
        R"(
    {
        "segments": [1,2],
        "versionid": 10
    })";

    const std::string validVersionContent2 =
        R"(
    {
        "segments": [1,2],
        "versionid": 100
    })";

    CreateFile("version.2", validVersionContent1);
    CreateFile("version.3", validVersionContent1);
    CreateFile("version.4", validVersionContent2);
    ASSERT_TRUE(VersionLoader::GetVersion(_rootDir, INVALID_VERSIONID, &version).IsOK());
    ASSERT_EQ(2, version.GetSegmentCount());
    ASSERT_EQ(2, version.GetLastSegmentId());
    ASSERT_EQ(100, version.GetVersionId());

    // specify vid, but version not exist
    ASSERT_TRUE(VersionLoader::GetVersion(_rootDir, 5 /* version not exist */, &version).IsInternalError());

    // specify vid, version exist, but invalid
    CreateFile("version.5", invalidVersionContent);
    ASSERT_TRUE(VersionLoader::GetVersion(_rootDir, 5, &version).IsConfigError());

    // specify vid, version exist, and valid
    Version version2;
    CreateFile("version.6", validVersionContent2);
    ASSERT_TRUE(VersionLoader::GetVersion(_rootDir, 6, &version2).IsOK());
    ASSERT_EQ(2, version2.GetSegmentCount());
    ASSERT_EQ(2, version2.GetLastSegmentId());
    ASSERT_EQ(100, version2.GetVersionId());
}

} // namespace indexlibv2::framework
