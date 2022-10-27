#include "indexlib/file_system/test/file_block_cache_unittest.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/util/cache//block_cache.h"
#include "indexlib/util/cache/sharded_cache.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileBlockCacheTest);

FileBlockCacheTest::FileBlockCacheTest()
{
}

FileBlockCacheTest::~FileBlockCacheTest()
{
}

void FileBlockCacheTest::CaseSetUp()
{
}

void FileBlockCacheTest::CaseTearDown()
{
}

bool FileBlockCacheTest::CheckInitWithConfigStr(const string& paramStr,
        int64_t expectCacheSize, bool useClockCache, int expectedShardBits)
{
    FileBlockCache fileBlockCache;
    util::MemoryQuotaControllerPtr memoryQuotaController =
        util::MemoryQuotaControllerCreator::CreateMemoryQuotaController();
    if (!fileBlockCache.Init(paramStr, memoryQuotaController))
    {
        return false;
    }
    if (expectCacheSize > 0 && (size_t)expectCacheSize != fileBlockCache.GetMemoryUse())
    {
        return false;
    }
    auto cache = fileBlockCache.GetBlockCache()->mCache.get();
    auto shardedCache = dynamic_cast<util::ShardedCache*>(cache);
    assert(shardedCache);
    if (expectedShardBits != shardedCache->num_shard_bits_)
    {
        return false;
    }
    string cacheName(fileBlockCache.GetBlockCache()->TEST_GetCacheName());
    return cacheName == "LRUCache";
}

void FileBlockCacheTest::TestInitWithConfigStr()
{
    ASSERT_FALSE(CheckInitWithConfigStr("cache_size"));
    ASSERT_FALSE(CheckInitWithConfigStr("cache_size=10=1"));
    ASSERT_FALSE(CheckInitWithConfigStr("cache_size=-1"));
    ASSERT_FALSE(CheckInitWithConfigStr("block_size=a"));
    ASSERT_FALSE(CheckInitWithConfigStr("cache_size=-1;block_size=10"));
    ASSERT_FALSE(CheckInitWithConfigStr("bad_name=10"));
    ASSERT_FALSE(CheckInitWithConfigStr("cache_size=2;block_size=0"));
    ASSERT_FALSE(CheckInitWithConfigStr("cache_size=2;num_shard_bits=abcde"));

    ASSERT_TRUE(CheckInitWithConfigStr("", 1UL * 1024 * 1024, false));
    ASSERT_TRUE(CheckInitWithConfigStr("cache_size=0;", 4096 << 6));
    ASSERT_TRUE(CheckInitWithConfigStr("cache_size=2;", 2UL * 1024 * 1024));
    ASSERT_TRUE(CheckInitWithConfigStr("cache_size=2;block_size=4", 2UL * 1024 * 1024));
    ASSERT_TRUE(CheckInitWithConfigStr("cache_size=2;block_size=2097152", 2097152 << 6));
    ASSERT_TRUE(CheckInitWithConfigStr("cache_size=2;block_size=2097152", 2097152 << 6));
}

void FileBlockCacheTest::TestMemoryQuotaController()
{
    FileBlockCache fileBlockCache;
    util::MemoryQuotaControllerPtr memoryQuotaController =
        util::MemoryQuotaControllerCreator::CreateMemoryQuotaController(3 * 1024 * 1024);
    ASSERT_TRUE(fileBlockCache.Init("cache_size=2", memoryQuotaController));
    ASSERT_EQ(2UL * 1024 * 1024, fileBlockCache.GetMemoryUse());
    ASSERT_EQ(1 * 1024 * 1024, memoryQuotaController->GetFreeQuota());
}

IE_NAMESPACE_END(file_system);

