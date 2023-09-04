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

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MemoryCacheTest, TestSimpleProcess);
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

}} // namespace indexlib::util
