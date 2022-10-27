#include <fslib/fslib.h>
#include "indexlib/file_system/test/file_node_cache_unittest.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/file_node_cache.h"
#include "indexlib/file_system/test/fake_file_node.h"

using namespace std;
using namespace fslib;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileNodeCacheTest);

FileNodeCacheTest::FileNodeCacheTest()
{
}

FileNodeCacheTest::~FileNodeCacheTest()
{
}

void FileNodeCacheTest::CaseSetUp()
{
}

void FileNodeCacheTest::CaseTearDown()
{
}

void FileNodeCacheTest::TestNormal()
{
    StorageMetrics metrics;
    FileNodeCache fileNodeCache(&metrics);

    string filePath1 = "/a/b/f1.txt";
    string filePath2 = "/a/b/f2.txt";
    FileNodePtr fileNode1(CreateFileNode(filePath1));
    FileNodePtr fileNode2(CreateFileNode(filePath2));

    fileNodeCache.Insert(fileNode1);
    fileNodeCache.Insert(fileNode2);
    ASSERT_TRUE(fileNodeCache.IsExist(filePath1));
    ASSERT_EQ(fileNode1, fileNodeCache.Find(filePath1));

    ASSERT_FALSE(fileNodeCache.RemoveFile(filePath1));
    ASSERT_TRUE(fileNodeCache.IsExist(filePath1));

    fileNode1.reset();
    ASSERT_TRUE(fileNodeCache.RemoveFile(filePath1));
    ASSERT_FALSE(fileNodeCache.IsExist(filePath1));
    ASSERT_FALSE(fileNodeCache.Find(filePath1));
    ASSERT_TRUE(fileNodeCache.IsExist(filePath2));

    FileNodePtr fileNode3(CreateFileNode(filePath2));
    fileNode2.reset();
    fileNodeCache.Insert(fileNode3);
    ASSERT_EQ(fileNode3, fileNodeCache.Find(filePath2));
}

void FileNodeCacheTest::TestListFile()
{
    StorageMetrics metrics;
    FileNodeCache fileNodeCache(&metrics);

    fslib::FileList fileList;
    fileNodeCache.ListFile("/a/", fileList);
    ASSERT_EQ((size_t)0, fileList.size());

    fileNodeCache.Insert(CreateFileNode("/a/bbbbbb/fx.txt"));
    fileNodeCache.Insert(CreateFileNode("/a/bbbbbb"));

    fileList.clear();
    fileNodeCache.ListFile("/a/", fileList);
    ASSERT_EQ((size_t)1, fileList.size());
    ASSERT_EQ(fileList[0], "bbbbbb");

    fileNodeCache.Insert(CreateFileNode("/a/f2.txt"));
    fileList.clear();
    fileNodeCache.ListFile("/a/", fileList);
    ASSERT_EQ((size_t)2, fileList.size());
    ASSERT_EQ(fileList[0], "bbbbbb");
    ASSERT_EQ(fileList[1], "f2.txt");

    fileNodeCache.Insert(CreateFileNode("/a/f1.txt"));
    fileList.clear();
    fileNodeCache.ListFile("/a/", fileList);
    ASSERT_EQ((size_t)3, fileList.size());
    ASSERT_EQ(fileList[0], "bbbbbb");
    ASSERT_EQ(fileList[1], "f1.txt");
    ASSERT_EQ(fileList[2], "f2.txt");

    fileList.clear();
    fileNodeCache.ListFile("/a/bbbbbb/", fileList);
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
    fileNodeCache.Insert(CreateFileNode("/a/bb/c/dirty.txt",
                    FSFT_IN_MEM, true));
    FileNodePtr fileNode4 = CreateFileNode("/a/bbb.txt");
    fileNodeCache.Insert(fileNode4);
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb/f1.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb/c/dirty.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bbb.txt"));

    // directory
    ASSERT_FALSE(fileNodeCache.RemoveFile("/a/bb"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb"));

    // use count > 1
    ASSERT_FALSE(fileNodeCache.RemoveFile("/a/bbb.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bbb.txt"));

    // not exist and /a/b is prefix of /a/bbb.txt
    fileNode4.reset();
    ASSERT_FALSE(fileNodeCache.RemoveFile("/a/b"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bbb.txt"));

    // normal
    ASSERT_TRUE(fileNodeCache.RemoveFile("/a/bbb.txt"));
    ASSERT_FALSE(fileNodeCache.IsExist("/a/bbb.txt"));
    ASSERT_EQ(1, metrics.GetFileCacheMetrics().removeCount.getValue());

    // dirty
    ASSERT_TRUE(fileNodeCache.RemoveFile("/a/bb/c/dirty.txt"));
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

    FileNodePtr fileNode1 = CreateFileNode("/a/bb/f1.txt");
    fileNodeCache.Insert(fileNode1);
    FileNodePtr fileNodeDirty = CreateFileNode("/a/bb/c/dirty.txt",
            FSFT_IN_MEM, true);
    fileNodeCache.Insert(fileNodeDirty);
    FileNodePtr fileNode4 = CreateFileNode("/a/bbb.txt");
    fileNodeCache.Insert(fileNode4);

    // remove file
    ASSERT_FALSE(fileNodeCache.RemoveDirectory("/a/bb/f1.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb/f1.txt"));

    // not exist and /a/b is prefix of /a/bb
    ASSERT_TRUE(fileNodeCache.RemoveDirectory("/a/b"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb"));

    // file /a/bb/f1.txt use count > 1
    ASSERT_FALSE(fileNodeCache.RemoveDirectory("/a/bb/"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb/f1.txt"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bb"));

    // normal
    fileNode1.reset();
    fileNodeDirty.reset();
    ASSERT_TRUE(fileNodeCache.RemoveDirectory("/a/bb/"));
    ASSERT_FALSE(fileNodeCache.IsExist("/a/bb/f1.txt"));
    ASSERT_FALSE(fileNodeCache.IsExist("/a/bb/c/dirty.txt"));
    ASSERT_FALSE(fileNodeCache.IsExist("/a/bb/c"));
    ASSERT_TRUE(fileNodeCache.IsExist("/a/bbb.txt"));
    ASSERT_EQ(2, metrics.GetFileCacheMetrics().removeCount.getValue());

    // long name
    fileNodeCache.Insert(CreateFileNode("/root/LongNameDir", FSFT_DIRECTORY));
    fileNodeCache.Insert(CreateFileNode("/root/s"));
    ASSERT_TRUE(fileNodeCache.RemoveDirectory("/root/LongNameDir"));
    ASSERT_TRUE(fileNodeCache.IsExist("/root/s"));
    ASSERT_FALSE(fileNodeCache.IsExist("/root/LongNameDir"));
}

void FileNodeCacheTest::TestClean()
{
    StorageMetrics metrics;
    FileNodeCache fileNodeCache(&metrics);

    FileNodePtr fileNode;
    fileNode = CreateFileNode("/a/b/f1.txt", FSFT_IN_MEM, false);
    fileNodeCache.Insert(fileNode);
    fileNode = CreateFileNode("/a/b/f2.txt", FSFT_IN_MEM, true);
    fileNodeCache.Insert(fileNode);
    fileNode = CreateFileNode("/a/f1.txt", FSFT_IN_MEM, true);
    fileNodeCache.Insert(fileNode);
    fileNode = CreateFileNode("/a/f2.txt", FSFT_IN_MEM, false);
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
    FileNodePtr fileNode;
    fileNode = CreateFileNode("/a/b/f1.txt", FSFT_IN_MEM, false);
    fileNodeCache.Insert(fileNode);
    fileList.push_back("/a/b/f1.txt");
    
    fileNode = CreateFileNode("/a/b/f2.txt", FSFT_IN_MEM, true);
    fileNodeCache.Insert(fileNode);
    fileList.push_back("/a/b/f2.txt");
    
    fileNode = CreateFileNode("/a/f1.txt", FSFT_IN_MEM, true);
    fileNodeCache.Insert(fileNode);
    fileList.push_back("/a/f1.txt");
    
    fileNode = CreateFileNode("/a/f2.txt", FSFT_IN_MEM, false);
    fileNodeCache.Insert(fileNode);
    fileList.push_back("/a/f2.txt");
    
    fileNodeCache.CleanFiles(fileList);
    ASSERT_EQ(1, metrics.GetFileCacheMetrics().removeCount.getValue());
}

FileNodePtr FileNodeCacheTest::CreateFileNode(
        const string& path, FSFileType type, bool isDirty)
{
    FakeFileNodePtr fileNode(new FakeFileNode(type));
    fileNode->Open(path, FSOT_UNKNOWN);
    fileNode->SetDirty(isDirty);

    return fileNode;
}

void FileNodeCacheTest::TestGetUseCount()
{
    StorageMetrics metrics;
    FileNodeCache fileNodeCache(&metrics);

    FileNodePtr fileNode;
    fileNode = CreateFileNode("/a/b/f1.txt", FSFT_IN_MEM, false);
    fileNodeCache.Insert(fileNode);
    ASSERT_EQ(2L, fileNodeCache.GetUseCount("/a/b/f1.txt"));
    fileNode.reset();
    ASSERT_EQ(1L, fileNodeCache.GetUseCount("/a/b/f1.txt"));
    ASSERT_EQ(0L, fileNodeCache.GetUseCount("not_exist"));
}

IE_NAMESPACE_END(file_system);

