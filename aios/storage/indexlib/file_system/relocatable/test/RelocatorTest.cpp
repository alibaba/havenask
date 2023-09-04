#include "indexlib/file_system/relocatable/Relocator.h"

#include "gtest/gtest.h"
#include <cstddef>
#include <memory>

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/relocatable/RelocatableFolder.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib::file_system {

class RelocatorTest : public TESTBASE
{
private:
    std::string _rootDir;
    std::string _folderRoot;
    std::string _targetDir;

public:
    void setUp() override
    {
        _rootDir = GET_TEMP_DATA_PATH();
        _folderRoot = util::PathUtil::JoinPath(_rootDir, "work/test0");
        _targetDir = util::PathUtil::JoinPath(_rootDir, "target");
    }
    void tearDown() override {}

    void MakeSimpleData()
    {
        auto [createStatus, folder] = Relocator::CreateFolder(_folderRoot, /*metricProvider*/ nullptr);
        ASSERT_TRUE(createStatus.IsOK());
        ASSERT_TRUE(folder);
        auto [status, writer] = folder->CreateFileWriter(/*fileName*/ "testfile", WriterOption::Buffer());
        ASSERT_TRUE(status.IsOK());
        std::string content = "helloworld";
        auto [writeStatus, writeLen] = writer->Write(content.data(), content.size()).StatusWith();
        ASSERT_TRUE(writeStatus.IsOK());
        ASSERT_TRUE(writer->Close().Status().IsOK());
        auto testDir = folder->MakeRelocatableFolder("testdir");
        ASSERT_TRUE(testDir);
        std::tie(status, writer) = testDir->CreateFileWriter("testfile1", WriterOption::Buffer());
        std::tie(writeStatus, writeLen) = writer->Write(content.data(), content.size()).StatusWith();
        ASSERT_TRUE(writeStatus.IsOK());
        ASSERT_TRUE(writer->Close().Status().IsOK());
        ASSERT_TRUE(Relocator::SealFolder(folder).IsOK());

        ASSERT_TRUE(FslibWrapper::IsExist(util::PathUtil::JoinPath(_folderRoot, "testfile")).GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(util::PathUtil::JoinPath(_folderRoot, "testdir/testfile1")).GetOrThrow());
    }
    void RelocateAndCheck()
    {
        Relocator relocator(_targetDir);
        ASSERT_TRUE(relocator.AddSource(_folderRoot).IsOK());
        ASSERT_TRUE(relocator.Relocate().IsOK());

        ASSERT_FALSE(FslibWrapper::IsExist(util::PathUtil::JoinPath(_folderRoot, "testfile")).GetOrThrow());
        ASSERT_FALSE(FslibWrapper::IsExist(util::PathUtil::JoinPath(_folderRoot, "testdir/testfile1")).GetOrThrow());

        ASSERT_TRUE(FslibWrapper::IsExist(util::PathUtil::JoinPath(_targetDir, "testfile")).GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(util::PathUtil::JoinPath(_targetDir, "testdir/testfile1")).GetOrThrow());

        std::string content = "helloworld";
        std::string readedContent;
        FslibWrapper::AtomicLoad(util::PathUtil::JoinPath(_targetDir, "testfile"), readedContent).GetOrThrow();
        ASSERT_EQ(content, readedContent);

        FslibWrapper::AtomicLoad(util::PathUtil::JoinPath(_targetDir, "testdir/testfile1"), readedContent).GetOrThrow();
        ASSERT_EQ(content, readedContent);
    }
};

TEST_F(RelocatorTest, testSimple)
{
    MakeSimpleData();
    RelocateAndCheck();
}

TEST_F(RelocatorTest, testFailover)
{
    MakeSimpleData();
    FslibWrapper::Rename(util::PathUtil::JoinPath(_folderRoot, "testdir/testfile1"),
                         util::PathUtil::JoinPath(_targetDir, "testdir/testfile1"))
        .GetOrThrow();
    RelocateAndCheck();
}

TEST_F(RelocatorTest, testCorruptionFolder)
{
    MakeSimpleData();
    FslibWrapper::DeleteFile(util::PathUtil::JoinPath(_folderRoot, "testdir/testfile1"), DeleteOption()).GetOrThrow();
    Relocator relocator(_targetDir);
    ASSERT_TRUE(relocator.AddSource(_folderRoot).IsOK());
    ASSERT_FALSE(relocator.Relocate().IsOK());
}

} // namespace indexlib::file_system
