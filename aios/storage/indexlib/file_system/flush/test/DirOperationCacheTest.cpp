#include "indexlib/file_system/flush/DirOperationCache.h"

#include "gtest/gtest.h"
#include <iosfwd>

#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class DirOperationCacheTest : public INDEXLIB_TESTBASE
{
public:
    DirOperationCacheTest();
    ~DirOperationCacheTest();

    DECLARE_CLASS_NAME(DirOperationCacheTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestGetParent();
    void TestNoNeedMkParentDir();
    void TestMakeDirWithFlushCache();

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, DirOperationCacheTest);

INDEXLIB_UNIT_TEST_CASE(DirOperationCacheTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DirOperationCacheTest, TestGetParent);
INDEXLIB_UNIT_TEST_CASE(DirOperationCacheTest, TestNoNeedMkParentDir);
INDEXLIB_UNIT_TEST_CASE(DirOperationCacheTest, TestMakeDirWithFlushCache);

//////////////////////////////////////////////////////////////////////

DirOperationCacheTest::DirOperationCacheTest() {}

DirOperationCacheTest::~DirOperationCacheTest() {}

void DirOperationCacheTest::CaseSetUp() {}

void DirOperationCacheTest::CaseTearDown() {}

void DirOperationCacheTest::TestGetParent()
{
    EXPECT_EQ("hdfs://a////b", DirOperationCache::GetParent("hdfs://a////b/c"));
    EXPECT_EQ("hdfs://a/b", DirOperationCache::GetParent("hdfs://a/b/c"));
    EXPECT_EQ("hdfs://a/b/c", DirOperationCache::GetParent("hdfs://a/b/c/"));
}

void DirOperationCacheTest::TestSimpleProcess()
{
    DirOperationCache dirOperationCache;
    string path1 = GET_TEMP_DATA_PATH() + "/a/b/c";
    string path2 = GET_TEMP_DATA_PATH() + "/a/b/d";
    string dir = GET_TEMP_DATA_PATH() + "/a/b";
    string dir2 = GET_TEMP_DATA_PATH() + "/a/d";
    dirOperationCache.MkParentDirIfNecessary(path1);
    dirOperationCache.Mkdir(dir2);
    ASSERT_TRUE(FslibWrapper::IsExist(dir).GetOrThrow());
    EXPECT_TRUE(dirOperationCache.Get(dir + '/'));
    EXPECT_TRUE(dirOperationCache.Get(dir2 + '/'));
    // FslibWrapper::SetError(FslibWrapper::MAKE_DIR_ERROR, dir);
    dirOperationCache.Mkdir(dir);
    dirOperationCache.Mkdir(dir + '/');
    dirOperationCache.MkParentDirIfNecessary(path1);
    dirOperationCache.MkParentDirIfNecessary(path2);
    // FslibWrapper::ClearError();
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
    // TODO
    // InMemFlushPathCachePtr flushCache(new InMemFlushPathCache);
    // DirOperationCache dirOperationCache(flushCache);
    // dirOperationCache.MkParentDirIfNecessary("hdfs://parent/abc");
    // dirOperationCache.MkParentDirIfNecessary("pangu://abc/def");

    // ASSERT_FALSE(flushCache->IsExist("hdfs://parent/abc"));
    // ASSERT_FALSE(flushCache->IsExist("pangu://abc/def"));

    // string localDir = GET_TEMP_DATA_PATH() + "/dir/local_dir";
    // dirOperationCache.MkParentDirIfNecessary(localDir);
    // ASSERT_TRUE(flushCache->IsExist(GET_TEMP_DATA_PATH()));
    // ASSERT_TRUE(flushCache->IsExist(GET_TEMP_DATA_PATH() + "/dir"));

    // ASSERT_FALSE(flushCache->IsExist(localDir));
    // dirOperationCache.Mkdir(localDir);
    // ASSERT_TRUE(flushCache->IsExist(GET_TEMP_DATA_PATH()));
    // ASSERT_TRUE(flushCache->IsExist(GET_TEMP_DATA_PATH() + "/dir"));
    // ASSERT_TRUE(flushCache->IsExist(localDir));
}
}} // namespace indexlib::file_system
