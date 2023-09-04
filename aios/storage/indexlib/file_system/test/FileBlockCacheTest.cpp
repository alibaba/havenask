#include "indexlib/file_system/FileBlockCache.h"

#include "gtest/gtest.h"
#include <assert.h>
#include <cstddef>
#include <memory>

#include "autil/cache/cache.h"
#include "autil/cache/sharded_cache.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/cache/Block.h"
#include "indexlib/util/cache/BlockAllocator.h"
#include "indexlib/util/cache/BlockCache.h"
#include "indexlib/util/cache/MemoryBlockCache.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace file_system {

class FileBlockCacheTest : public INDEXLIB_TESTBASE
{
public:
    FileBlockCacheTest();
    ~FileBlockCacheTest();

    DECLARE_CLASS_NAME(FileBlockCacheTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInitWithConfigStr();
    void TestMemoryQuotaController();

    void TestDisableBlockCache();

private:
    bool CheckInitWithConfigStr(const std::string& paramStr, int64_t expectCacheSize = -1, int expectedShardBits = 6);

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, FileBlockCacheTest);

INDEXLIB_UNIT_TEST_CASE(FileBlockCacheTest, TestInitWithConfigStr);
INDEXLIB_UNIT_TEST_CASE(FileBlockCacheTest, TestMemoryQuotaController);
INDEXLIB_UNIT_TEST_CASE(FileBlockCacheTest, TestDisableBlockCache);

//////////////////////////////////////////////////////////////////////

FileBlockCacheTest::FileBlockCacheTest() {}

FileBlockCacheTest::~FileBlockCacheTest() {}

void FileBlockCacheTest::CaseSetUp() {}

void FileBlockCacheTest::CaseTearDown() {}

bool FileBlockCacheTest::CheckInitWithConfigStr(const string& paramStr, int64_t expectCacheSize, int expectedShardBits)
{
    FileBlockCache fileBlockCache;
    util::MemoryQuotaControllerPtr memoryQuotaController =
        util::MemoryQuotaControllerCreator::CreateMemoryQuotaController();
    if (!fileBlockCache.Init(paramStr, memoryQuotaController)) {
        return false;
    }
    if (expectCacheSize > 0 && (size_t)expectCacheSize != fileBlockCache.GetResourceInfo().maxMemoryUse) {
        return false;
    }
    auto cache = std::dynamic_pointer_cast<MemoryBlockCache>(fileBlockCache.GetBlockCache())->_cache.get();
    auto shardedCache = dynamic_cast<autil::ShardedCache*>(cache);
    assert(shardedCache);
    if (expectedShardBits != shardedCache->num_shard_bits_) {
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
    ASSERT_FALSE(CheckInitWithConfigStr("cache_size=2;block_size=0"));
    ASSERT_FALSE(CheckInitWithConfigStr("cache_size=2;num_shard_bits=abcde"));

    ASSERT_TRUE(CheckInitWithConfigStr("", 1UL * 1024 * 1024));
    ASSERT_TRUE(CheckInitWithConfigStr("cache_size=2;", 2UL * 1024 * 1024));
    ASSERT_TRUE(CheckInitWithConfigStr("cache_size=2;block_size=4", 2UL * 1024 * 1024));
    ASSERT_TRUE(CheckInitWithConfigStr("cache_size=2;block_size=2097152", 2097152 << 6));
    ASSERT_TRUE(CheckInitWithConfigStr("cache_size=2;block_size=2097152", 2097152 << 6));
    ASSERT_TRUE(CheckInitWithConfigStr("use_clock_cache=0", 1UL * 1024 * 1024));

    ASSERT_TRUE(CheckInitWithConfigStr("cache_type=lru", 1UL * 1024 * 1024));
    ASSERT_TRUE(CheckInitWithConfigStr("cache_size=2;cache_type=lru;num_shard_bits=1", 2UL * 1024 * 1024, 1));

    // test detail params
    ASSERT_TRUE(CheckInitWithConfigStr("cache_size=2;cache_type=lru;num_shard_bits=1;detail=2", 2UL * 1024 * 1024, 1));
    ASSERT_TRUE(CheckInitWithConfigStr("bad_name=10"));
}

void FileBlockCacheTest::TestMemoryQuotaController()
{
    FileBlockCache fileBlockCache;
    util::MemoryQuotaControllerPtr memoryQuotaController =
        util::MemoryQuotaControllerCreator::CreateMemoryQuotaController(3 * 1024 * 1024);
    ASSERT_TRUE(fileBlockCache.Init("cache_size=2", memoryQuotaController));
    ASSERT_EQ(2UL * 1024 * 1024, fileBlockCache.GetResourceInfo().maxMemoryUse);
    ASSERT_EQ(1 * 1024 * 1024, memoryQuotaController->GetFreeQuota());
}

void FileBlockCacheTest::TestDisableBlockCache()
{
    FileBlockCache fileBlockCache;
    util::MemoryQuotaControllerPtr memoryQuotaController =
        util::MemoryQuotaControllerCreator::CreateMemoryQuotaController();
    ASSERT_TRUE(fileBlockCache.Init("cache_size=0", memoryQuotaController));
    const auto& cache = fileBlockCache.GetBlockCache();

    // Get & Put
    const auto& allocator = cache->GetBlockAllocator();
    auto block = allocator->AllocBlock();
    autil::CacheBase::Handle* handle = nullptr;
    ASSERT_TRUE(cache->Put(block, &handle, autil::CacheBase::Priority::LOW));
    ASSERT_TRUE(handle);
    cache->ReleaseHandle(handle);
    handle = nullptr;

    util::blockid_t blockId;
    auto ret = cache->Get(blockId, &handle);
    ASSERT_FALSE(ret);
    ASSERT_EQ(nullptr, handle);

    ASSERT_TRUE(allocator.get());
    auto addr = allocator->Allocate();
    ASSERT_TRUE(addr);
    allocator->Deallocate(addr);
    ASSERT_EQ(0u, cache->GetBlockCount());
    ASSERT_EQ(0u, cache->GetMaxBlockCount());
    ASSERT_EQ(0u, cache->GetResourceInfo().maxMemoryUse);

    ASSERT_EQ(0u, fileBlockCache.GetResourceInfo().maxMemoryUse);
    ASSERT_EQ(0u, fileBlockCache.GetResourceInfo().memoryUse);

    ASSERT_NO_THROW(fileBlockCache.ReportMetrics());
}
}} // namespace indexlib::file_system
