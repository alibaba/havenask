#include "indexlib/framework/DeployIndexUtil.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/VersionCommitter.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class DeployIndexUtilTest : public TESTBASE
{
};

TEST_F(DeployIndexUtilTest, testSimple)
{
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fs);

    for (int segmentId = 0; segmentId < 2; ++segmentId) {
        auto segDirName = std::string("segment_") + std::to_string(segmentId) + "_level_0";
        auto segDir = rootDir->MakeDirectory(segDirName);
        assert(segDir);
        segDir->Store(/*name*/ "data", /*fileContent*/ "");
        SegmentInfo segInfo;
        ASSERT_TRUE(segInfo.Store(segDir->GetIDirectory()).IsOK());
    }
    Version version;
    version.AddSegment(0);
    version.AddSegment(1);
    version.SetVersionId(0);
    auto status = VersionCommitter::Commit(version, rootDir, {});
    ASSERT_TRUE(status.IsOK());

    auto [status2, indexSize] = DeployIndexUtil::GetIndexSize(testRoot, /*targetVersion*/ 0);
    ASSERT_TRUE(status2.IsOK());

    auto [ec, dirSize] = indexlib::file_system::FslibWrapper::GetDirSize(testRoot);
    ASSERT_EQ(indexlib::file_system::FSEC_OK, ec);
    int64_t entryTableSize = indexlib::file_system::FslibWrapper::GetFileLength(
                                 indexlib::util::PathUtil::JoinPath(testRoot, "entry_table.0"))
                                 .GetOrThrow();
    ASSERT_EQ(static_cast<int64_t>(dirSize) - entryTableSize, indexSize);
}

} // namespace indexlibv2::framework
