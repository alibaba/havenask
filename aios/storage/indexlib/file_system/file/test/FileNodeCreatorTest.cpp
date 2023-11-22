#include "indexlib/file_system/file/FileNodeCreator.h"

#include "indexlib/file_system/DiskStorage.h"
#include "indexlib/file_system/package/PackageOpenMeta.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class FileNodeCreatorTest : public INDEXLIB_TESTBASE
{
public:
    FileNodeCreatorTest();
    ~FileNodeCreatorTest();

    DECLARE_CLASS_NAME(FileNodeCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestOpenFileNode();

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, FileNodeCreatorTest);

INDEXLIB_UNIT_TEST_CASE(FileNodeCreatorTest, TestOpenFileNode);

namespace {

class MockFileNodeCreator : public FileNodeCreator
{
public:
    MockFileNodeCreator() {}
    using FileNodeCreator::CreateFileNode;
    MOCK_METHOD(bool, Init, (const LoadConfig& loadConfig, const util::BlockMemoryQuotaControllerPtr& memController),
                (override));
    MOCK_METHOD(std::shared_ptr<FileNode>, CreateFileNode,
                (FSOpenType type, bool readOnly, const std::string& linkRoot), (override));
    MOCK_METHOD(bool, Match, (const std::string& filePath, const std::string& lifecycle), (const, override));
    MOCK_METHOD(bool, MatchType, (FSOpenType type), (const, override));
    MOCK_METHOD(size_t, EstimateFileLockMemoryUse, (size_t fileLength), (const, noexcept, override));
};

class MockFileNode : public FileNode
{
public:
    MOCK_METHOD(FSFileType, GetType, (), (const, noexcept, override));
    MOCK_METHOD(size_t, GetLength, (), (const, noexcept, override));
    MOCK_METHOD(void*, GetBaseAddress, (), (const, noexcept, override));
    MOCK_METHOD(FSResult<size_t>, Read, (void* buffer, size_t length, size_t offset, ReadOption option),
                (noexcept, override));
    MOCK_METHOD(util::ByteSliceList*, ReadToByteSliceList, (size_t length, size_t offset, ReadOption option),
                (noexcept, override));
    MOCK_METHOD(FSResult<size_t>, Write, (const void* buffer, size_t length), (noexcept, override));
    MOCK_METHOD(FSResult<void>, Close, (), (noexcept, override));
    MOCK_METHOD(ErrorCode, DoOpen, (const std::string& path, FSOpenType openType, int64_t fileLength),
                (noexcept, override));
    MOCK_METHOD(ErrorCode, DoOpen, (const PackageOpenMeta& packageOpenMeta, FSOpenType openType), (noexcept, override));
    MOCK_METHOD(bool, ReadOnly, (), (const, noexcept, override));
};
}; // namespace

FileNodeCreatorTest::FileNodeCreatorTest() {}

FileNodeCreatorTest::~FileNodeCreatorTest() {}

void FileNodeCreatorTest::CaseSetUp() {}

void FileNodeCreatorTest::CaseTearDown() {}

void FileNodeCreatorTest::TestOpenFileNode()
{
    // MockFileNode* mockFileNode = new MockFileNode();
    // std::shared_ptr<FileNode> fileNode(mockFileNode);

    // {
    //     // test has packageFileMountTable, not in packageFile
    //     DiskStorage storage("", util::BlockMemoryQuotaControllerPtr());
    //     MockFileNodeCreator fileNodeCreator;
    //     string path = "abc";
    //     EXPECT_CALL(*mockFileNode, DoOpen(path, _)).Times(1);
    //     storage.OpenFileNode(fileNode, path, FSOT_MEM);
    // }

    // {
    //     // test has packageFileMountTable, in packageFile
    //     string path = "abc";
    //     PackageOpenMeta packageOpenMeta;
    //     packageOpenMeta.SetLogicalFilePath(path);
    //     packageOpenMeta.innerFileMeta._filePath = "123";
    //     packageOpenMeta.innerFileMeta._isDir = false;
    //     packageOpenMeta.innerFileMeta.Init(0, 4, 0);

    //     DiskStorage storage("", util::BlockMemoryQuotaControllerPtr());
    //     storage._packageFileMountTable._table.insert(make_pair(path, packageOpenMeta.innerFileMeta));
    //     storage._packageFileMountTable._physicalFileSize.insert(make_pair("123", 4));

    //     MockFileNodeCreator fileNodeCreator;
    //     EXPECT_CALL(*mockFileNode, DoOpen(packageOpenMeta, _)).Times(1);
    //     storage.OpenFileNode(fileNode, path, FSOT_MEM);
    // }

    // {
    //     auto testOpenWithFileMeta = [&fileNode, &mockFileNode](FSOpenType type)
    //     {
    //         // test open file with file meta
    //         string path = "abc";
    //         DiskStorage storage("", util::BlockMemoryQuotaControllerPtr());
    //         storage._pathMetaContainer.reset(new PathMetaContainer);
    //         storage._pathMetaContainer->AddFileInfo(path, 1, 2, false);

    //         EXPECT_CALL(*mockFileNode, DoOpen(_, testing::An<const FileMeta&>(), _)).Times(1);
    //         storage.OpenFileNode(fileNode, path, type);
    //     };
    //     testOpenWithFileMeta(FSOT_MEM);
    //     testOpenWithFileMeta(FSOT_BUFFERED);
    //     testOpenWithFileMeta(FSOT_MMAP);
    // }
}
}} // namespace indexlib::file_system
