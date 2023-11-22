#include "indexlib/file_system/file/FileNodeCache.h"

#include "gtest/gtest.h"
#include <cstddef>
#include <memory>
#include <string>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/test/FakeFileNode.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class FileNodeCacheTest : public INDEXLIB_TESTBASE
{
public:
    FileNodeCacheTest();
    ~FileNodeCacheTest();

    DECLARE_CLASS_NAME(FileNodeCacheTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNormal();
    void TestListDir();
    void TestRemoveFile();
    void TestRemoveDirectory();
    void TestRemoveDirectoryWithoutDir();
    void TestClean();
    void TestCleanFiles();
    void TestGetUseCount();
    void TestInsertDuplicatedFileWithDifferentOpenType();

private:
    std::shared_ptr<FileNode> CreateFileNode(const std::string& path = "", FSFileType type = FSFT_MEM,
                                             bool isDirty = false);

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, FileNodeCacheTest);

INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestNormal);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestListDir);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestRemoveFile);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestRemoveDirectory);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestRemoveDirectoryWithoutDir);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestClean);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestCleanFiles);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestGetUseCount);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestInsertDuplicatedFileWithDifferentOpenType);
//////////////////////////////////////////////////////////////////////

FileNodeCacheTest::FileNodeCacheTest() {}

FileNodeCacheTest::~FileNodeCacheTest() {}

void FileNodeCacheTest::CaseSetUp() {}

void FileNodeCacheTest::CaseTearDown() {}

void FileNodeCacheTest::TestNormal()
{
    StorageMetrics metrics;
    FileNodeCache fileNodeCache(&metrics);

    string filePath1 = "/a/b/f1.txt";
    string filePath2 = "/a/b/f2.txt";
    std::shared_ptr<FileNode> fileNode1(CreateFileNode(filePath1));
    std::shared_ptr<FileNode> fileNode2(CreateFileNode(filePath2));

    fileNodeCache.Insert(fileNode1);
    fileNodeCache.Insert(fileNode2);
    ASSERT_TRUE(fileNodeCache.IsExist(filePath1));
    ASSERT_EQ(fileNode1, fileNodeCache.Find(filePath1));

    ASSERT_EQ(FSEC_ERROR, fileNodeCache.RemoveFile(filePath1));
    ASSERT_TRUE(fileNodeCache.IsExist(filePath1));

    fileNode1.reset();
    ASSERT_EQ(FSEC_OK, fileNodeCache.RemoveFile(filePath1));
    ASSERT_FALSE(fileNodeCache.IsExist(filePath1));
    ASSERT_FALSE(fileNodeCache.Find(filePath1));
    ASSERT_TRUE(fileNodeCache.IsExist(filePath2));

    std::shared_ptr<FileNode> fileNode3(CreateFileNode(filePath2));
    fileNode2.reset();
    fileNodeCache.Insert(fileNode3);
    ASSERT_EQ(fileNode3, fileNodeCache.Find(filePath2));
}

void FileNodeCacheTest::TestListDir()
{
    StorageMetrics metrics;
    FileNodeCache fileNodeCache(&metrics);

    fslib::FileList fileList;
    fileNodeCache.ListDir("/a", fileList);
    ASSERT_EQ((size_t)0, fileList.size());

    fileNodeCache.Insert(CreateFileNode("/a/bbbbbb/fx.txt"));
    fileNodeCache.Insert(CreateFileNode("/a/bbbbbb"));

    fileList.clear();
    fileNodeCache.ListDir("/a", fileList);
    ASSERT_EQ((size_t)1, fileList.size());
    ASSERT_EQ(fileList[0], "bbbbbb");

    fileNodeCache.Insert(CreateFileNode("/a/f2.txt"));
    fileList.clear();
    fileNodeCache.ListDir("/a", fileList);
    ASSERT_EQ((size_t)2, fileList.size());
    ASSERT_EQ(fileList[0], "bbbbbb");
    ASSERT_EQ(fileList[1], "f2.txt");

    fileNodeCache.Insert(CreateFileNode("/a/f1.txt"));
    fileList.clear();
    fileNodeCache.ListDir("/a", fileList);
    ASSERT_EQ((size_t)3, fileList.size());
    ASSERT_EQ(fileList[0], "bbbbbb");
    ASSERT_EQ(fileList[1], "f1.txt");
    ASSERT_EQ(fileList[2], "f2.txt");

    fileList.clear();
    fileNodeCache.ListDir("/a/bbbbbb", fileList);
    ASSERT_EQ((size_t)1, fileList.size());
    ASSERT_EQ(fileList[0], "fx.txt");
}

void FileNodeCacheTest::TestRemoveFile()
{
    StorageMetrics metrics;
    FileNodeCache fileNodeCache(&metrics);

    fileNodeCache.Insert(CreateFileNode("", FSFT_DIRECTORY));
    fileNodeCache.Insert(CreateFileNode("/a", FSFT_DIRECTORY));
    fileNodeCache.Insert(CreateFileNode("/a/bb", FSFT_DIRECTORY));
    fileNodeCache.Insert(CreateFileNode("/a/bb/c", FSFT_DIRECTORY));

    fileNodeCache.Insert(CreateFileNode("/a/bb/f1.txt"));
    fileNodeCache.Insert(CreateFileNode("/a/bb/c/dirty.txt", FSFT_MEM, true));
    std::shared_ptr<FileNode> fileNode4 = CreateFileNode("/a/bbb.txt");
    fileNodeCache.Insert(fileNode4);
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb/f1.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb/c/dirty.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bbb.txt"));

    // directory
    ASSERT_EQ(FSEC_ISDIR, fileNodeCache.RemoveFile("/a/bb"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb"));

    // use count > 1
    ASSERT_EQ(FSEC_ERROR, fileNodeCache.RemoveFile("/a/bbb.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bbb.txt"));

    // not exist and /a/b is prefix of /a/bbb.txt
    fileNode4.reset();
    ASSERT_EQ(FSEC_NOENT, fileNodeCache.RemoveFile("/a/b"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bbb.txt"));

    // normal
    ASSERT_EQ(FSEC_OK, fileNodeCache.RemoveFile("/a/bbb.txt"));
    ASSERT_FALSE(fileNodeCache.IsExist("/a/bbb.txt"));
    ASSERT_EQ(1, metrics.GetFileCacheMetrics().removeCount.getValue());

    // dirty
    ASSERT_EQ(FSEC_OK, fileNodeCache.RemoveFile("/a/bb/c/dirty.txt"));
    ASSERT_FALSE(fileNodeCache.IsExist("/a/bb/c/dirty.txt"));
    ASSERT_EQ(2, metrics.GetFileCacheMetrics().removeCount.getValue());
}

void FileNodeCacheTest::TestRemoveDirectory()
{
    StorageMetrics metrics;
    FileNodeCache fileNodeCache(&metrics);

    fileNodeCache.Insert(CreateFileNode("", FSFT_DIRECTORY));
    fileNodeCache.Insert(CreateFileNode("/a", FSFT_DIRECTORY));
    fileNodeCache.Insert(CreateFileNode("/a/bb", FSFT_DIRECTORY));
    fileNodeCache.Insert(CreateFileNode("/a/bb/c", FSFT_DIRECTORY));

    std::shared_ptr<FileNode> fileNode1 = CreateFileNode("/a/bb/f1.txt");
    fileNodeCache.Insert(fileNode1);
    std::shared_ptr<FileNode> fileNodeDirty = CreateFileNode("/a/bb/c/dirty.txt", FSFT_MEM, true);
    fileNodeCache.Insert(fileNodeDirty);
    std::shared_ptr<FileNode> fileNode4 = CreateFileNode("/a/bbb.txt");
    fileNodeCache.Insert(fileNode4);

    // remove file
    ASSERT_EQ(FSEC_NOTDIR, fileNodeCache.RemoveDirectory("/a/bb/f1.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb/f1.txt"));

    // not exist and /a/b is prefix of /a/bb
    ASSERT_EQ(FSEC_OK, fileNodeCache.RemoveDirectory("/a/b"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb"));

    // file /a/bb/f1.txt use count > 1
    ASSERT_EQ(FSEC_ERROR, fileNodeCache.RemoveDirectory("/a/bb"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb/f1.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb"));

    // normal
    fileNode1.reset();
    fileNodeDirty.reset();
    ASSERT_EQ(FSEC_OK, fileNodeCache.RemoveDirectory("/a/bb"));
    ASSERT_FALSE(fileNodeCache.IsExist("/a/bb/f1.txt"));
    ASSERT_FALSE(fileNodeCache.IsExist("/a/bb/c/dirty.txt"));
    ASSERT_FALSE(fileNodeCache.IsExist("/a/bb/c"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bbb.txt"));
    ASSERT_EQ(2, metrics.GetFileCacheMetrics().removeCount.getValue());

    // long name
    fileNodeCache.Insert(CreateFileNode("/root/LongNameDir", FSFT_DIRECTORY));
    fileNodeCache.Insert(CreateFileNode("/root/s"));
    ASSERT_EQ(FSEC_OK, fileNodeCache.RemoveDirectory("/root/LongNameDir"));
    ASSERT_TRUE(fileNodeCache.IsExist("/root/s"));
    ASSERT_FALSE(fileNodeCache.IsExist("/root/LongNameDir"));
}

void FileNodeCacheTest::TestRemoveDirectoryWithoutDir()
{
    StorageMetrics metrics;
    FileNodeCache fileNodeCache(&metrics);

    fileNodeCache.Insert(CreateFileNode("/a/b"));
    fileNodeCache.Insert(CreateFileNode("/a/ba"));
    fileNodeCache.Insert(CreateFileNode("/a/bb/f2.txt"));
    fileNodeCache.Insert(CreateFileNode("/a/bb/f3.txt"));
    fileNodeCache.Insert(CreateFileNode("/a/bc"));

    ASSERT_EQ(FSEC_OK, fileNodeCache.RemoveDirectory("/a/bb"));

    ASSERT_FALSE(fileNodeCache.IsExist("/a/bb/f2.txt"));
    ASSERT_FALSE(fileNodeCache.IsExist("/a/bb/f3.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/b"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/ba"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bc"));
    ASSERT_EQ(2, metrics.GetFileCacheMetrics().removeCount.getValue());
}

void FileNodeCacheTest::TestClean()
{
    StorageMetrics metrics;
    FileNodeCache fileNodeCache(&metrics);

    std::shared_ptr<FileNode> fileNode;
    fileNode = CreateFileNode("/a/b/f1.txt", FSFT_MEM, false);
    fileNodeCache.Insert(fileNode);
    fileNode = CreateFileNode("/a/b/f2.txt", FSFT_MEM, true);
    fileNodeCache.Insert(fileNode);
    fileNode = CreateFileNode("/a/f1.txt", FSFT_MEM, true);
    fileNodeCache.Insert(fileNode);
    fileNode = CreateFileNode("/a/f2.txt", FSFT_MEM, false);
    fileNodeCache.Insert(fileNode);
    fileNode.reset();

    ASSERT_TRUE(fileNodeCache.IsExist("/a/b/f1.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/b/f2.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/f1.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/f2.txt"));

    // not clean with dirty
    fileNodeCache.Clean();
    ASSERT_FALSE(fileNodeCache.IsExist("/a/b/f1.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/b/f2.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/f1.txt"));
    ASSERT_FALSE(fileNodeCache.IsExist("/a/f2.txt"));
    fileNode = fileNodeCache.Find("/a/b/f2.txt");
    ASSERT_TRUE(fileNode->IsDirty());
    fileNode->SetDirty(false);
    fileNode = fileNodeCache.Find("/a/f1.txt");
    ASSERT_TRUE(fileNode->IsDirty());
    fileNode->SetDirty(false);
    fileNode.reset();
    ASSERT_EQ(2, metrics.GetFileCacheMetrics().removeCount.getValue());

    // not clean with use_count
    fileNode = fileNodeCache.Find("/a/b/f2.txt");
    fileNodeCache.Clean();
    ASSERT_TRUE(fileNodeCache.IsExist("/a/b/f2.txt"));
    ASSERT_FALSE(fileNodeCache.IsExist("/a/f1.txt"));
    ASSERT_EQ(3, metrics.GetFileCacheMetrics().removeCount.getValue());
}

void FileNodeCacheTest::TestCleanFiles()
{
    StorageMetrics metrics;
    FileNodeCache fileNodeCache(&metrics);

    FileList fileList;
    std::shared_ptr<FileNode> fileNode;
    fileNode = CreateFileNode("/a/b/f1.txt", FSFT_MEM, false);
    fileNodeCache.Insert(fileNode);
    fileList.push_back("/a/b/f1.txt");

    fileNode = CreateFileNode("/a/b/f2.txt", FSFT_MEM, true);
    fileNodeCache.Insert(fileNode);
    fileList.push_back("/a/b/f2.txt");

    fileNode = CreateFileNode("/a/f1.txt", FSFT_MEM, true);
    fileNodeCache.Insert(fileNode);
    fileList.push_back("/a/f1.txt");

    fileNode = CreateFileNode("/a/f2.txt", FSFT_MEM, false);
    fileNodeCache.Insert(fileNode);
    fileList.push_back("/a/f2.txt");

    fileNodeCache.CleanFiles(fileList);
    ASSERT_EQ(1, metrics.GetFileCacheMetrics().removeCount.getValue());
}

void FileNodeCacheTest::TestInsertDuplicatedFileWithDifferentOpenType()
{
    StorageMetrics metrics;
    FileNodeCache fileNodeCache(&metrics);

    std::string filePath = "/seg_1/f1";

    std::shared_ptr<FileNode> fileNode1 = CreateFileNode(filePath, FSFT_MEM, false);
    fileNodeCache.Insert(fileNode1);
    ASSERT_TRUE(fileNodeCache.IsExist(filePath));
    ASSERT_EQ(fileNode1, fileNodeCache.Find(filePath));

    std::shared_ptr<FileNode> fileNode2 = CreateFileNode(filePath, FSFT_BLOCK, false);
    fileNodeCache.Insert(fileNode2);
    ASSERT_TRUE(fileNodeCache.IsExist(filePath));
    ASSERT_EQ(fileNode2, fileNodeCache.Find(filePath));

    std::shared_ptr<FileNode> fileNode3 = CreateFileNode(filePath, FSFT_MEM, false);
    fileNodeCache.Insert(fileNode3);
    ASSERT_TRUE(fileNodeCache.IsExist(filePath));
    ASSERT_EQ(fileNode3, fileNodeCache.Find(filePath));

    ASSERT_EQ(2, fileNodeCache._toDelFileNodeVec.size());
    ASSERT_EQ(1, fileNodeCache.GetFileCount());
    ASSERT_EQ(2, fileNodeCache.GetUseCount("/seg_1/f1"));

    ASSERT_EQ(2, metrics.GetFileCount(FSMG_LOCAL, FSFT_MEM));
    ASSERT_EQ(1, metrics.GetFileCount(FSMG_LOCAL, FSFT_BLOCK));
    ASSERT_EQ(1, metrics.GetFileCount(FSMG_LOCAL, FSFT_BLOCK));

    fileNode1.reset();
    fileNode2.reset();
    fileNodeCache.Clean();

    ASSERT_EQ(0, fileNodeCache._toDelFileNodeVec.size());
    ASSERT_EQ(1, fileNodeCache.GetFileCount());
    ASSERT_EQ(2, fileNodeCache.GetUseCount("/seg_1/f1"));

    ASSERT_EQ(1, metrics.GetFileCount(FSMG_LOCAL, FSFT_MEM));
    ASSERT_EQ(0, metrics.GetFileCount(FSMG_LOCAL, FSFT_BLOCK));
}

std::shared_ptr<FileNode> FileNodeCacheTest::CreateFileNode(const string& path, FSFileType type, bool isDirty)
{
    FakeFileNodePtr fileNode(new FakeFileNode(type));
    EXPECT_EQ(FSEC_OK, fileNode->Open(path, path, FSOT_UNKNOWN, -1));
    fileNode->SetDirty(isDirty);
    return fileNode;
}

void FileNodeCacheTest::TestGetUseCount()
{
    StorageMetrics metrics;
    FileNodeCache fileNodeCache(&metrics);

    std::shared_ptr<FileNode> fileNode;
    fileNode = CreateFileNode("/a/b/f1.txt", FSFT_MEM, false);
    fileNodeCache.Insert(fileNode);
    ASSERT_EQ(2L, fileNodeCache.GetUseCount("/a/b/f1.txt"));
    fileNode.reset();
    ASSERT_EQ(1L, fileNodeCache.GetUseCount("/a/b/f1.txt"));
    ASSERT_EQ(0L, fileNodeCache.GetUseCount("not_exist"));
}
}} // namespace indexlib::file_system
