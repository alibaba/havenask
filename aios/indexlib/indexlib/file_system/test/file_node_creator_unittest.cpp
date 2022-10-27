#include "indexlib/file_system/test/file_node_creator_unittest.h"
#include "indexlib/file_system/disk_storage.h"
#include "indexlib/file_system/package_open_meta.h"

using namespace std;
using namespace fslib;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileNodeCreatorTest);

namespace 
{

class MockFileNodeCreator : public FileNodeCreator
{
public:
    MockFileNodeCreator()
    {}

    MOCK_METHOD2(Init, bool(const LoadConfig& loadConfig,
                            const util::BlockMemoryQuotaControllerPtr& memController));
    MOCK_METHOD3(CreateFileNode, FileNodePtr(
                    const std::string& filePath, FSOpenType type, bool readOnly));
    MOCK_CONST_METHOD2(Match, bool(const std::string& filePath, const std::string& lifecycle));
    MOCK_CONST_METHOD1(MatchType, bool(FSOpenType type));
    MOCK_CONST_METHOD1(EstimateFileLockMemoryUse, size_t(size_t fileLength));
};

class MockFileNode : public FileNode
{
public:
    MOCK_CONST_METHOD0(GetType, FSFileType());
    MOCK_CONST_METHOD0(GetLength, size_t());
    MOCK_CONST_METHOD0(GetBaseAddress, void*());
    MOCK_METHOD4(Read, size_t(void* buffer, size_t length, size_t offset, ReadOption option));
    MOCK_METHOD3(Read, util::ByteSliceList*(size_t length, size_t offset, ReadOption option));
    MOCK_METHOD2(Write, size_t(const void* buffer, size_t length));
    MOCK_METHOD0(Close, void());
    MOCK_METHOD2(DoOpen, void(const std::string& path, FSOpenType openType));
    MOCK_METHOD2(DoOpen, void(const PackageOpenMeta& packageOpenMeta, FSOpenType openType));
    MOCK_METHOD3(DoOpen, void(const std::string& path, const fslib::FileMeta& fileMeta, FSOpenType openType));
    MOCK_CONST_METHOD0(ReadOnly, bool());
};
};

FileNodeCreatorTest::FileNodeCreatorTest()
{
}

FileNodeCreatorTest::~FileNodeCreatorTest()
{
}

void FileNodeCreatorTest::CaseSetUp()
{
}

void FileNodeCreatorTest::CaseTearDown()
{
}

void FileNodeCreatorTest::TestOpenFileNode()
{
    MockFileNode* mockFileNode = new MockFileNode();
    FileNodePtr fileNode(mockFileNode);

    {
        // test has packageFileMountTable, not in packageFile
        DiskStorage storage("", util::BlockMemoryQuotaControllerPtr());
        MockFileNodeCreator fileNodeCreator;
        string path = "abc";
        EXPECT_CALL(*mockFileNode, DoOpen(path, _)).Times(1);
        storage.OpenFileNode(fileNode, path, FSOT_IN_MEM);
    }

    {
        // test has packageFileMountTable, in packageFile
        string path = "abc";
        PackageOpenMeta packageOpenMeta;
        packageOpenMeta.SetLogicalFilePath(path);
        packageOpenMeta.innerFileMeta.mFilePath = "123";
        packageOpenMeta.innerFileMeta.mIsDir = false;
        packageOpenMeta.innerFileMeta.Init(0, 4, 0);

        DiskStorage storage("", util::BlockMemoryQuotaControllerPtr());
        storage.mPackageFileMountTable.mTable.insert(make_pair(path, packageOpenMeta.innerFileMeta));
        storage.mPackageFileMountTable.mPhysicalFileSize.insert(make_pair("123", 4));

        MockFileNodeCreator fileNodeCreator;
        EXPECT_CALL(*mockFileNode, DoOpen(packageOpenMeta, _)).Times(1);
        storage.OpenFileNode(fileNode, path, FSOT_IN_MEM);
    }

    {
        auto testOpenWithFileMeta = [&fileNode, &mockFileNode](FSOpenType type)
        {
            // test open file with file meta
            string path = "abc";
            DiskStorage storage("", util::BlockMemoryQuotaControllerPtr());
            storage.mPathMetaContainer.reset(new PathMetaContainer);
            storage.mPathMetaContainer->AddFileInfo(path, 1, 2, false);

            EXPECT_CALL(*mockFileNode, DoOpen(_, testing::An<const FileMeta&>(), _)).Times(1);
            storage.OpenFileNode(fileNode, path, type);
        };
        testOpenWithFileMeta(FSOT_IN_MEM);
        testOpenWithFileMeta(FSOT_BUFFERED);
        testOpenWithFileMeta(FSOT_MMAP);
    }
}

IE_NAMESPACE_END(file_system);

