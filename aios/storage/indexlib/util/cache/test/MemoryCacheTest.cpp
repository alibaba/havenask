#include "autil/Log.h"
#include "indexlib/util/cache/BlockAllocator.h"
#include "indexlib/util/cache/BlockCache.h"
#include "indexlib/util/cache/BlockCacheCreator.h"
#include "indexlib/util/testutil/unittest.h"
using namespace std;

namespace indexlib { namespace util {

class MemoryCacheTest : public INDEXLIB_TESTBASE
{
public:
    MemoryCacheTest();
    ~MemoryCacheTest();

    DECLARE_CLASS_NAME(MemoryCacheTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestBottomPriorityInsertionPerformance();
    double TestBottomPriorityInsertionHelper(size_t lruSize, bool useBottom);

private:
    AUTIL_LOG_DECLARE();
    void loadFileToCache(BlockCachePtr& blockCache, size_t ifile, size_t iblock, autil::CacheBase::Priority priority,
                         size_t& hits, size_t& miss);
};

INDEXLIB_UNIT_TEST_CASE(MemoryCacheTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MemoryCacheTest, TestBottomPriorityInsertionPerformance);
AUTIL_LOG_SETUP(indexlib.util, MemoryCacheTest);

MemoryCacheTest::MemoryCacheTest() {}

MemoryCacheTest::~MemoryCacheTest() {}

void MemoryCacheTest::CaseSetUp() {}

void MemoryCacheTest::CaseTearDown() {}

void MemoryCacheTest::TestSimpleProcess()
{
    size_t blockSize = 64;
    BlockCacheOption option = BlockCacheOption::LRU(16 * 1024 * blockSize, blockSize, 4);
    option.cacheParams["lru_high_priority_ratio"] = "0.7";
    BlockCachePtr blockCache(BlockCacheCreator::Create(option));

    size_t nQuery = 1024 * 1024;
    size_t nFile = 32;
    size_t nBlockInFile = 1024;
    // absolutly random Lookup, if miss then write back to cache

    size_t hits = 0;
    size_t miss = 0;

    for (size_t i = 0; i < nQuery; ++i) {
        autil::CacheBase::Handle* handle = nullptr;
        blockid_t blockId = {random() % nFile, random() % nBlockInFile};
        Block* block = blockCache->Get(blockId, &handle);
        if (handle == nullptr) {
            block = blockCache->GetBlockAllocator()->AllocBlock();
            block->id = blockId;
            memset(block->data, (char)blockId.inFileIdx, blockSize);
            autil::CacheBase::Priority priority =
                (i & 1) ? autil::CacheBase::Priority::LOW : autil::CacheBase::Priority::HIGH;
            ASSERT_TRUE(blockCache->Put(block, &handle, priority));
            ++miss;
        } else {
            ASSERT_EQ((char)blockId.inFileIdx, *((char*)block->data));
            ++hits;
        }
        blockCache->ReleaseHandle(handle);
    }
    AUTIL_LOG(ERROR, "final miss ratio [%s]", std::to_string(1.0 * miss / (hits + miss)).c_str());
}

void MemoryCacheTest::loadFileToCache(BlockCachePtr& blockCache, size_t ifile, size_t iblock,
                                      autil::CacheBase::Priority priority, size_t& hits, size_t& miss)
{
    size_t blockSize = 4 * 1024;
    autil::CacheBase::Handle* handle = nullptr;
    blockid_t blockId = {ifile, iblock};
    Block* block = blockCache->Get(blockId, &handle);
    if (handle == nullptr) {
        // file miss, then generate one
        block = blockCache->GetBlockAllocator()->AllocBlock();
        block->id = blockId;
        memset(block->data, (char)blockId.inFileIdx, blockSize);
        ASSERT_TRUE(blockCache->Put(block, &handle, priority));
        miss++;
    } else {
        ASSERT_EQ((char)blockId.inFileIdx, *((char*)block->data));
        hits++;
    }
    blockCache->ReleaseHandle(handle);
}

double MemoryCacheTest::TestBottomPriorityInsertionHelper(size_t lruSize, bool useBottom)
{
    size_t blockSize = 4 * 1024;
    BlockCacheOption option = BlockCacheOption::LRU(lruSize, blockSize, 4);
    option.cacheParams["lru_high_priority_ratio"] = "0.0";
    option.cacheParams["lru_low_priority_ratio"] = "0.5";
    // lru_bottom_priority_ratio = 0.5
    BlockCachePtr blockCache(BlockCacheCreator::Create(option));

    size_t nBlockInFile = 1024;
    size_t hits = 0;
    size_t miss = 0;

    // load file1
    for (size_t ifile = 0; ifile < 8; ifile++) {
        for (size_t iblock = 0; iblock < nBlockInFile; iblock++) {
            loadFileToCache(blockCache, ifile, iblock, autil::CacheBase::Priority::LOW, hits, miss);
        }
    }

    // load file2
    for (size_t ifile = 8; ifile < 16; ifile++) {
        for (size_t iblock = 0; iblock < nBlockInFile; iblock++) {
            loadFileToCache(blockCache, ifile, iblock,
                            useBottom ? autil::CacheBase::Priority::BOTTOM : autil::CacheBase::Priority::LOW, hits,
                            miss);
        }
    }

    hits = 0;
    miss = 0;
    // load file1 again, calculate hit rate here.
    for (size_t ifile = 0; ifile < 8; ifile++) {
        for (size_t iblock = 0; iblock < nBlockInFile; iblock++) {
            loadFileToCache(blockCache, ifile, iblock, autil::CacheBase::Priority::LOW, hits, miss);
        }
    }
    return 1.0 * miss / (hits + miss);
}

// TESTing bottom insertion performance about SCAN problem flushing the cache
// steps:
// 1. read file1 32MB with LOW priority
// 2. read file2 32MB with BOTTOM priority
// 3. read file1 again and calculate cache miss
void MemoryCacheTest::TestBottomPriorityInsertionPerformance()
{
    for (size_t lruSize = 20; lruSize <= 80; lruSize += 5) {
        double missRate1 = TestBottomPriorityInsertionHelper(lruSize * 1024 * 1024, true);
        double missRate2 = TestBottomPriorityInsertionHelper(lruSize * 1024 * 1024, false);
        AUTIL_LOG(INFO, "cache [%lu] MB, miss ratio [%lf/%lf], use BOTTOM [true/false]", lruSize, missRate1, missRate2);
    }
}

}} // namespace indexlib::util
