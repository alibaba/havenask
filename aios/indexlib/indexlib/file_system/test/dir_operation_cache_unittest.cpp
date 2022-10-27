#include "indexlib/file_system/test/dir_operation_cache_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;

IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, DirOperationCacheTest);

DirOperationCacheTest::DirOperationCacheTest()
{
}

DirOperationCacheTest::~DirOperationCacheTest()
{
}

void DirOperationCacheTest::CaseSetUp()
{
}

void DirOperationCacheTest::CaseTearDown()
{
}

void DirOperationCacheTest::TestGetParent()
{
    EXPECT_EQ("hdfs://a////b", DirOperationCache::GetParent("hdfs://a////b/c"));
    EXPECT_EQ("hdfs://a/b", DirOperationCache::GetParent("hdfs://a/b/c"));
    EXPECT_EQ("hdfs://a/b/c", DirOperationCache::GetParent("hdfs://a/b/c/"));
}

void DirOperationCacheTest::TestSimpleProcess()
{
    DirOperationCache dirOperationCache;
    string path1 = GET_TEST_DATA_PATH() + "/a/b/c";
    string path2 = GET_TEST_DATA_PATH() + "/a/b/d";
    string dir = GET_TEST_DATA_PATH() + "/a/b";
    string dir2 = GET_TEST_DATA_PATH() + "/a/d";
    dirOperationCache.MkParentDirIfNecessary(path1);
    dirOperationCache.Mkdir(dir2);
    FileSystemWrapper::IsExist(dir);
    EXPECT_TRUE(dirOperationCache.Get(dir + '/'));
    EXPECT_TRUE(dirOperationCache.Get(dir2 + '/'));
    FileSystemWrapper::SetError(FileSystemWrapper::MAKE_DIR_ERROR, dir);
    dirOperationCache.Mkdir(dir);
    dirOperationCache.Mkdir(dir + '/');
    dirOperationCache.MkParentDirIfNecessary(path1);
    dirOperationCache.MkParentDirIfNecessary(path2);
    FileSystemWrapper::ClearError();
}

void DirOperationCacheTest::TestNoNeedMkParentDir()
{
    DirOperationCache dirOperationCache;
    dirOperationCache.MkParentDirIfNecessary("hdfs://parent/abc");
    dirOperationCache.MkParentDirIfNecessary("pangu://abc/def");
    EXPECT_TRUE(dirOperationCache.Get("hdfs://parent/"));
    EXPECT_TRUE(dirOperationCache.Get("pangu://abc/"));
}

void DirOperationCacheTest::TestMakeDirWithFlushCache()
{
    PathMetaContainerPtr flushCache(new PathMetaContainer);
    DirOperationCache dirOperationCache(flushCache);
    dirOperationCache.MkParentDirIfNecessary("hdfs://parent/abc");
    dirOperationCache.MkParentDirIfNecessary("pangu://abc/def");

    ASSERT_FALSE(flushCache->IsExist("hdfs://parent/abc"));
    ASSERT_FALSE(flushCache->IsExist("pangu://abc/def"));

    string localDir = GET_TEST_DATA_PATH() + "/dir/local_dir";
    dirOperationCache.MkParentDirIfNecessary(localDir);
    ASSERT_TRUE(flushCache->IsExist(GET_TEST_DATA_PATH()));
    ASSERT_TRUE(flushCache->IsExist(GET_TEST_DATA_PATH() + "/dir"));

    ASSERT_FALSE(flushCache->IsExist(localDir));
    dirOperationCache.Mkdir(localDir);
    ASSERT_TRUE(flushCache->IsExist(GET_TEST_DATA_PATH()));
    ASSERT_TRUE(flushCache->IsExist(GET_TEST_DATA_PATH() + "/dir"));
    ASSERT_TRUE(flushCache->IsExist(localDir));
}

IE_NAMESPACE_END(file_system);

