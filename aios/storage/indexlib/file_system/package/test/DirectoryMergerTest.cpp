#include "indexlib/file_system/package/DirectoryMerger.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <initializer_list>
#include <iosfwd>
#include <memory>

#include "autil/CommonMacros.h"
#include "autil/LambdaWorkItem.h"
#include "autil/StringUtil.h"
#include "autil/ThreadPool.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace file_system {

class DirectoryMergerTest : public INDEXLIB_TESTBASE
{
public:
    DirectoryMergerTest();
    ~DirectoryMergerTest();

    DECLARE_CLASS_NAME(DirectoryMergerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    std::string _rootDir;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, DirectoryMergerTest);

INDEXLIB_UNIT_TEST_CASE(DirectoryMergerTest, TestSimpleProcess);
//////////////////////////////////////////////////////////////////////

DirectoryMergerTest::DirectoryMergerTest() {}
DirectoryMergerTest::~DirectoryMergerTest() {}

void DirectoryMergerTest::CaseSetUp() { _rootDir = util::PathUtil::NormalizeDir(GET_TEMP_DATA_PATH()) + "/"; }

void DirectoryMergerTest::CaseTearDown() {}

void DirectoryMergerTest::TestSimpleProcess()
{
    for (int instanceId = 0; instanceId < 3; ++instanceId) {
        FileSystemOptions fsOptions;
        fsOptions.outputStorage = FSST_PACKAGE_DISK;
        auto fs =
            FileSystemCreator::CreateForWrite("i" + StringUtil::toString(instanceId), _rootDir, fsOptions).GetOrThrow();

        autil::ThreadPool threadPool(/*threadNum=*/3, /*queueSize=*/100, true);
        for (int i = 0; i < 3; ++i) {
            auto workItem = new autil::LambdaWorkItem([&, i]() {
                string description = "i" + StringUtil::toString(instanceId) + "t" + StringUtil::toString(i);
                std::shared_ptr<Directory> dir = Directory::Get(fs);
                for (auto segDirName : {"seg_1", "seg_2"}) {
                    std::shared_ptr<Directory> segDir = dir->MakeDirectory(segDirName, DirectoryOption::Package());
                    string attrPath = "attr/name-" + description;
                    std::shared_ptr<Directory> attrDir = segDir->MakeDirectory(attrPath);
                    attrDir->Store("data", description + "-data");
                    attrDir->Store("offset", description + "-offset");
                }
            });
            if (ThreadPool::ERROR_NONE != threadPool.pushWorkItem(workItem)) {
                ThreadPool::dropItemIgnoreException(workItem);
            }
        }
        threadPool.start("");
        threadPool.waitFinish();
        fs->CommitPackage().GetOrThrow();
    }

    FSResult<bool> ret = DirectoryMerger::MergePackageFiles(_rootDir + "seg_1");
    ASSERT_EQ(FSEC_OK, ret.ec);
    ASSERT_TRUE(ret.result);
    ASSERT_EQ(FSEC_OK, DirectoryMerger::MergePackageFiles(_rootDir + "seg_2").ec);

    FileList fileList;
    FslibWrapper::ListDirE(_rootDir + "seg_1", fileList);
    EXPECT_THAT(fileList, testing::UnorderedElementsAre("package_file.__data__0", "package_file.__data__1",
                                                        "package_file.__data__2", "package_file.__meta__"));

    auto fs = FileSystemCreator::CreateForRead("", GET_TEMP_DATA_PATH()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fs->MountDir(_rootDir, "seg_1", "seg_1", FSMT_READ_WRITE, true));
    ASSERT_EQ(FSEC_OK, fs->MountDir(_rootDir, "seg_2", "seg_2", FSMT_READ_WRITE, true));
    ASSERT_EQ(FSEC_OK, fs->MountSegment("seg_1"));
    ASSERT_EQ(FSEC_OK, fs->MountSegment("seg_2"));
    std::shared_ptr<Directory> dir = Directory::Get(fs);
    for (int instanceId = 0; instanceId < 3; ++instanceId) {
        for (int threadId = 0; threadId < 3; ++threadId) {
            string description = "i" + StringUtil::toString(instanceId) + "t" + StringUtil::toString(threadId);
            ASSERT_EQ(description + "-data",
                      dir->CreateFileReader("seg_1/attr/name-" + description + "/data", FSOT_MEM)->TEST_Load());
            ASSERT_EQ(description + "-data",
                      dir->CreateFileReader("seg_2/attr/name-" + description + "/data", FSOT_MEM)->TEST_Load());
            ASSERT_EQ(description + "-offset",
                      dir->CreateFileReader("seg_1/attr/name-" + description + "/offset", FSOT_MEM)->TEST_Load());
            ASSERT_EQ(description + "-offset",
                      dir->CreateFileReader("seg_2/attr/name-" + description + "/offset", FSOT_MEM)->TEST_Load());
        }
    }
}
}} // namespace indexlib::file_system
