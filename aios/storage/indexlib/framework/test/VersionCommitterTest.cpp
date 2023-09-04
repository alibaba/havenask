#include "indexlib/framework/VersionCommitter.h"

#include <string>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class VersionCommitterTest : public TESTBASE
{
public:
    VersionCommitterTest() {}
    ~VersionCommitterTest() {}

    void setUp() override {}
    void tearDown() override {}

private:
    bool CheckFileExist(const std::string& fileName, const indexlib::file_system::DirectoryPtr& dir)
    {
        // check logical
        bool exist = dir->IsExist(fileName);
        if (!exist) {
            return false;
        }
        return CheckFileExist(fileName, dir->GetOutputPath());
    }

    bool CheckFileExist(const std::string& fileName, const std::string& path)
    {
        bool exist = false;
        auto physicalFile = indexlib::util::PathUtil::JoinPath(path, fileName);
        auto ec = indexlib::file_system::FslibWrapper::IsExist(physicalFile, exist);
        if (ec == indexlib::file_system::ErrorCode::FSEC_OK && exist) {
            return true;
        }
        return false;
    }

    bool CheckVersionExist(const std::string& versionFileName, const std::string& globalRoot,
                           const indexlib::file_system::DirectoryPtr& fenceDir)
    {
        return CheckFileExist(versionFileName, fenceDir) && CheckFileExist(versionFileName, globalRoot);
    }
};

namespace {

std::shared_ptr<indexlib::file_system::Directory>
CreateFenceRoot(const std::string& rootDir, const std::string& fenceName,
                const indexlib::file_system::FileSystemOptions& fsOptions)
{
    std::string fence = indexlib::util::PathUtil::JoinPath(rootDir, fenceName);
    auto fs = indexlib::file_system::FileSystemCreator::Create(/*name*/ "", fence, fsOptions).GetOrThrow();
    return indexlib::file_system::Directory::Get(fs);
}

} // namespace

TEST_F(VersionCommitterTest, testCommitWithoutPublish)
{
    const std::string rootDir = GET_TEMP_DATA_PATH();
    indexlib::file_system::FileSystemOptions fsOptions;
    auto masterDir = CreateFenceRoot(rootDir, "master", fsOptions);

    // empty version
    Version version;
    Fence fence(rootDir, "master", masterDir->GetFileSystem());
    version.SetVersionId(0);
    version.SetFenceName("d1");
    version.AddSegment(1);
    auto status = VersionCommitter::Commit(version, fence, {});
    ASSERT_TRUE(status.IsOK());
    const std::string versionFile0("version.0");
    ASSERT_TRUE(CheckFileExist(versionFile0, masterDir));
    ASSERT_FALSE(CheckFileExist(versionFile0, rootDir));
}

TEST_F(VersionCommitterTest, testNormal)
{
    const std::string rootDir = GET_TEMP_DATA_PATH();
    indexlib::file_system::FileSystemOptions fsOptions;
    auto masterDir = CreateFenceRoot(rootDir, "master", fsOptions);
    auto zombieDir = CreateFenceRoot(rootDir, "zombie", fsOptions);

    // empty version
    Version version;
    Fence fence(rootDir, "master", masterDir->GetFileSystem());
    auto status = VersionCommitter::CommitAndPublish(version, fence, {});
    ASSERT_TRUE(status.IsInvalidArgs());
    version.SetVersionId(0);
    version.SetFenceName("d1");
    version.AddSegment(1);
    status = VersionCommitter::CommitAndPublish(version, fence, {});
    ASSERT_TRUE(status.IsOK());

    const std::string versionFile0("version.0");
    ASSERT_TRUE(CheckVersionExist(versionFile0, rootDir, masterDir));

    const std::string versionFile1("version.1");
    version.IncVersionId();
    version.AddSegment(2);
    status = VersionCommitter::CommitAndPublish(version, fence, {});
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(CheckVersionExist(versionFile0, rootDir, masterDir));
    ASSERT_TRUE(CheckVersionExist(versionFile1, rootDir, masterDir));

    const std::string versionFile2("version.2");
    version.IncVersionId();
    version.AddSegment(3);
    status = VersionCommitter::CommitAndPublish(version, fence, {});
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(CheckVersionExist(versionFile0, rootDir, masterDir));
    ASSERT_TRUE(CheckVersionExist(versionFile1, rootDir, masterDir));
    ASSERT_TRUE(CheckVersionExist(versionFile2, rootDir, masterDir));

    const std::string versionFile3("version.3");
    version.IncVersionId();
    version.AddSegment(4);
    Fence fence2(rootDir, "zombie", zombieDir->GetFileSystem());
    status = VersionCommitter::CommitAndPublish(version, fence2, {});
    ASSERT_TRUE(status.IsOK());
    ASSERT_FALSE(CheckVersionExist(versionFile0, rootDir, zombieDir));
    ASSERT_FALSE(CheckVersionExist(versionFile1, rootDir, zombieDir));
    ASSERT_FALSE(CheckVersionExist(versionFile2, rootDir, zombieDir));
    ASSERT_TRUE(CheckVersionExist(versionFile3, rootDir, zombieDir));
}

TEST_F(VersionCommitterTest, testAbnormal)
{
    const std::string rootDir = GET_TEMP_DATA_PATH();
    indexlib::file_system::FileSystemOptions fsOptions;
    auto masterDir = CreateFenceRoot(rootDir, "master", fsOptions);
    auto zombieDir = CreateFenceRoot(rootDir, "zombie", fsOptions);
    // master will create version.0/version.1/version.2
    uint32_t commitRetryCount = 3;
    Version version;
    Fence fence(rootDir, "master", masterDir->GetFileSystem());
    for (int32_t i = 0; i < commitRetryCount; ++i) {
        std::string versionFile("version.");
        versionFile += std::to_string(i);
        version.SetVersionId(i);
        version.AddSegment(i);
        Status status = VersionCommitter::CommitAndPublish(version, fence, {});
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(CheckVersionExist(versionFile, rootDir, masterDir));
    }

    const std::string versionFile0("version.0");
    const std::string versionFile1("version.1");
    const std::string versionFile2("version.2");
    const std::string versionFile3("version.3");
    version.SetVersionId(0);
    // zombie will try commit version from 0 -> 2 (retry =3), failed.
    // But version.0/version.1/version.2 exist in zombie fs
    Fence fence2(rootDir, "zombie", zombieDir->GetFileSystem());
    for (int i = 0; i < 3; ++i) {
        Status status = VersionCommitter::CommitAndPublish(version, fence2, {});
        ASSERT_TRUE(status.IsExist());
        version.IncVersionId();
    }
    ASSERT_EQ(version.GetVersionId(), 3);
    ASSERT_TRUE(CheckVersionExist(versionFile0, rootDir, zombieDir));
    ASSERT_TRUE(CheckVersionExist(versionFile1, rootDir, zombieDir));
    ASSERT_TRUE(CheckVersionExist(versionFile2, rootDir, zombieDir));
    ASSERT_FALSE(CheckVersionExist(versionFile3, rootDir, zombieDir));
}
} // namespace indexlibv2::framework
