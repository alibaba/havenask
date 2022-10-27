#include "indexlib/file_system/test/session_file_cache_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SessionFileCacheTest);

SessionFileCacheTest::SessionFileCacheTest()
{
}

SessionFileCacheTest::~SessionFileCacheTest()
{
}

void SessionFileCacheTest::CaseSetUp()
{
}

void SessionFileCacheTest::CaseTearDown()
{
}

void SessionFileCacheTest::TestSimpleProcess()
{
    string filePath = "hdfs://10.101.193.199?username=xxx&paswword=xxx/indexlib/test";
    SessionFileCache cache;
    {
        FileCacheGuard guard(&cache, filePath, -1);
        ASSERT_EQ(0, cache.GetSessionFileCount(pthread_self(), filePath));
    }
    ASSERT_EQ(1, cache.GetSessionFileCount(pthread_self(), filePath));
    {
        FileCacheGuard guard(&cache, filePath, -1);
        ASSERT_EQ(0, cache.GetSessionFileCount(pthread_self(), filePath));
        FileCacheGuard guard1(&cache, filePath, -1);
        ASSERT_EQ(0, cache.GetSessionFileCount(pthread_self(), filePath));
    }
    ASSERT_EQ(2, cache.GetSessionFileCount(pthread_self(), filePath));
    {
        FileCacheGuard guard(&cache, filePath, -1);
        ASSERT_EQ(1, cache.GetSessionFileCount(pthread_self(), filePath));
        FileCacheGuard guard1(&cache, filePath, -1);
        ASSERT_EQ(0, cache.GetSessionFileCount(pthread_self(), filePath));
    }
}

IE_NAMESPACE_END(file_system);

