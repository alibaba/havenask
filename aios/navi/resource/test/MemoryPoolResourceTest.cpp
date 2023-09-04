#include "navi/resource/MemoryPoolR.h"

#include <iosfwd>
#include <memory>
#include <stdlib.h>
#include <vector>

#include "autil/EnvUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;

namespace navi {
class MemoryPoolTest : public TESTBASE {
private:
    typedef std::shared_ptr<autil::mem_pool::Pool> PoolPtr;
public:
    MemoryPoolTest() : _disablePoolModeAsan("naviPoolModeAsan", "0") {}
    void setUp() {}
    void tearDown(){}
private:
    EnvGuard _disablePoolModeAsan;
};

TEST_F(MemoryPoolTest, testInitParam) {
    {
        MemoryPoolR poolRes;
        ASSERT_EQ(16 * 1024 * 1024, poolRes._poolReleaseThreshold);
        ASSERT_EQ(2 * 1024 * 1024, poolRes._poolChunkSize);
        
        ASSERT_TRUE(poolRes.init(nullptr));
        ASSERT_FALSE(poolRes._useAsanPool);
        ASSERT_EQ(16 * 1024 * 1024, poolRes._poolReleaseThreshold);
        ASSERT_EQ(2 * 1024 * 1024, poolRes._poolChunkSize);
    }
    {
        EnvGuard _a("naviPoolRecycleSizeLimit", "8");
        EnvGuard _b("naviPoolTrunkSize", "1");
        MemoryPoolR poolRes;
        ASSERT_EQ(8 * 1024 * 1024, poolRes._poolReleaseThreshold);
        ASSERT_EQ(1 * 1024 * 1024, poolRes._poolChunkSize);
        
        ASSERT_TRUE(poolRes.init(nullptr));        
        ASSERT_EQ(8 * 1024 * 1024, poolRes._poolReleaseThreshold);
        ASSERT_EQ(1 * 1024 * 1024, poolRes._poolChunkSize);
    }
    {
        EnvGuard _a("naviPoolRecycleSizeLimit", "2");
        EnvGuard _b("naviPoolTrunkSize", "4");
        MemoryPoolR poolRes;
        ASSERT_EQ(2 * 1024 * 1024, poolRes._poolReleaseThreshold);
        ASSERT_EQ(4 * 1024 * 1024, poolRes._poolChunkSize);
        
        ASSERT_TRUE(poolRes.init(nullptr));        
        ASSERT_EQ(4 * 1024 * 1024, poolRes._poolReleaseThreshold);
        ASSERT_EQ(4 * 1024 * 1024, poolRes._poolChunkSize);
    }
    {
        NaviLoggerProvider provider("INFO");
        EnvGuard _b("naviPoolTrunkSize", "2");
        MemoryPoolR poolRes;
        ASSERT_EQ(16 * 1024 * 1024, poolRes._poolReleaseThreshold);
        ASSERT_EQ(2 * 1024 * 1024, poolRes._poolChunkSize);
        
        ASSERT_TRUE(poolRes.init(nullptr));
        ASSERT_EQ(16 * 1024 * 1024, poolRes._poolReleaseThreshold);
        ASSERT_EQ(2 * 1024 * 1024, poolRes._poolChunkSize);
    }
}

TEST_F(MemoryPoolTest, testTrimPool) {
    size_t poolReleaseThreshold = 512;
    MemoryPoolR poolRes;
    poolRes._poolChunkSize = poolReleaseThreshold;
    poolRes._poolReleaseThreshold = poolReleaseThreshold;
    poolRes._poolCacheSizeLimit = 10000000u;

    ASSERT_EQ(0, poolRes._poolQueue.Size());

    auto *pool1 = poolRes.getPool();
    ASSERT_GE(poolReleaseThreshold, pool1->getTotalBytes());
    auto *pool2 = poolRes.getPool();
    ASSERT_GE(poolReleaseThreshold, pool2->getTotalBytes());

    auto *pool3 = poolRes.getPool();
    pool3->allocate(poolReleaseThreshold + 1);
    ASSERT_LE(poolReleaseThreshold, pool3->getTotalBytes());

    poolRes.putPool(pool1); // keep
    ASSERT_EQ(1, poolRes._poolQueue.Size());
    poolRes.putPool(pool2); // keep
    ASSERT_EQ(2, poolRes._poolQueue.Size());
    poolRes.putPool(pool3); // drop
    ASSERT_EQ(2, poolRes._poolQueue.Size());
    for (size_t i = 0; i < poolRes._poolQueue.Size(); ++i) {
        autil::mem_pool::Pool *pool = nullptr;
        ASSERT_TRUE(poolRes._poolQueue.Pop(&pool));
        ASSERT_GE(poolReleaseThreshold, pool->getTotalBytes());
        delete pool;
    }
}

TEST_F(MemoryPoolTest, testLimitPool) {
    MemoryPoolR poolRes;
    poolRes._poolCacheSizeLimit = 10;
    vector<PoolPtr> poolPtrVec;
    for (int i = 0; i < 100; i++) {
        auto *pool = new autil::mem_pool::Pool;
        pool->allocate(100);
        poolRes.putPool(pool);
    }
    ASSERT_EQ(10, poolRes._poolQueue.Size());
}

TEST_F(MemoryPoolTest, testAutoScale) {
    NaviLoggerProvider _("INFO");
    {
        // test init
        MemoryPoolR poolRes;
        poolRes.initLogger(poolRes._logger, "NaviMemScale");
        ASSERT_EQ(MemoryPoolR::DEFAULT_POOL_RELEASE_RATE, poolRes._poolAutoScaleReleaseRate);
        poolRes.poolCacheSizeAutoScale();
        ASSERT_EQ(MemoryPoolR::DEFAULT_POOL_CACHE_AUTOSCALE_KEEP_COUNT, poolRes._poolCacheSizeLimit);
    }
    {
        MemoryPoolR poolRes;
        poolRes.initLogger(poolRes._logger, "NaviMemScale");
        poolRes._poolCacheSizeUB = 1000ul;
        poolRes._poolCacheSizeLB = 300ul;
        poolRes._poolAutoScaleReleaseRate = 5;
        poolRes.poolCacheSizeAutoScale();
        ASSERT_EQ(std::max(MemoryPoolR::DEFAULT_POOL_CACHE_AUTOSCALE_KEEP_COUNT, 1000ul - 300ul / 3),
                  poolRes._poolCacheSizeLimit);
    }
    {
        MemoryPoolR poolRes;
        poolRes.initLogger(poolRes._logger, "NaviMemScale");
        poolRes._poolCacheSizeUB = 1000ul;
        poolRes._poolCacheSizeLB = 1000ul;
        poolRes._poolAutoScaleReleaseRate = 5;
        poolRes.poolCacheSizeAutoScale();
        ASSERT_EQ(std::max(MemoryPoolR::DEFAULT_POOL_CACHE_AUTOSCALE_KEEP_COUNT, 1000ul - 1000ul / 3),
                  poolRes._poolCacheSizeLimit);
    }
}

}
