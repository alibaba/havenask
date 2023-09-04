#include "indexlib/file_system/load_config/CacheLoadStrategy.h"

#include "gtest/gtest.h"
#include <iosfwd>
#include <stdint.h>

#include "autil/legacy/jsonizable.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class CacheLoadStrategyTest : public TESTBASE
{
};

TEST_F(CacheLoadStrategyTest, TestCheck)
{
    {
        // allow cache size be 0
        string jsonStr = "{                           \
                              \"cache_size\" : 0,   \
                              \"block_size\" : 8192,  \
                              \"direct_io\" : true    \
                          }";
        CacheLoadStrategy loadStrategy;
        FromJsonString(loadStrategy, jsonStr);
        ASSERT_NO_THROW(loadStrategy.Check());
    }

    {
        // block_size should be multiple of 4096 (4K)
        string jsonStr = "{                           \
                              \"cache_size\" : 0,   \
                              \"block_size\" : 8191,  \
                              \"direct_io\" : true    \
                          }";
        CacheLoadStrategy loadStrategy;
        FromJsonString(loadStrategy, jsonStr);
        ASSERT_ANY_THROW(loadStrategy.Check());
    }

    {
        // block_size should be multiple of 4096 (4K)
        string jsonStr = "{                           \
                              \"cache_size\" : 0,   \
                              \"block_size\" : 2048,  \
                              \"direct_io\" : true    \
                          }";
        CacheLoadStrategy loadStrategy;
        FromJsonString(loadStrategy, jsonStr);
        ASSERT_ANY_THROW(loadStrategy.Check());
    }

    {
        // block size not allowed to be zero
        string jsonStr = "{                           \
                              \"cache_size\" : 0,   \
                              \"block_size\" : 0,  \
                              \"direct_io\" : true    \
                          }";
        CacheLoadStrategy loadStrategy;
        FromJsonString(loadStrategy, jsonStr);
        ASSERT_ANY_THROW(loadStrategy.Check());
    }
}

TEST_F(CacheLoadStrategyTest, TestParse)
{
    {
        CacheLoadStrategy loadStrategy;
        string loadStrategyStr = ToJsonString(loadStrategy);
        CacheLoadStrategy resultLoadStrategy;
        FromJsonString(resultLoadStrategy, loadStrategyStr);

        ASSERT_EQ(loadStrategy, resultLoadStrategy);
    }
    {
        string jsonStr = "{                           \
                              \"cache_size\" : 512,   \
                              \"block_size\" : 8192,  \
                              \"io_batch_size\" : 128,\
                              \"direct_io\" : true    \
                          }";
        CacheLoadStrategy loadStrategy;
        FromJsonString(loadStrategy, jsonStr);
        ASSERT_EQ((uint32_t)(512 * 1024 * 1024), loadStrategy.GetBlockCacheOption().memorySize);
        ASSERT_EQ((uint32_t)8192, loadStrategy.GetBlockCacheOption().blockSize);
        ASSERT_EQ((uint32_t)128, loadStrategy.GetBlockCacheOption().ioBatchSize);
        ASSERT_TRUE(loadStrategy.UseDirectIO());
        ASSERT_TRUE(!loadStrategy.UseGlobalCache());
        ASSERT_TRUE(!loadStrategy.CacheDecompressFile());
    }
    {
        string jsonStr = "{                           \
                              \"direct_io\" : true,   \
                              \"global_cache\" : true,  \
                              \"cache_decompress_file\" : true \
                          }";
        CacheLoadStrategy loadStrategy;
        FromJsonString(loadStrategy, jsonStr);
        ASSERT_TRUE(loadStrategy.UseDirectIO());
        ASSERT_TRUE(loadStrategy.UseGlobalCache());
        ASSERT_TRUE(loadStrategy.CacheDecompressFile());
    }
}

TEST_F(CacheLoadStrategyTest, TestDefaultValue)
{
    // constructor default value
    CacheLoadStrategy loadStrategy;
    ASSERT_EQ((uint32_t)(1 * 1024 * 1024), loadStrategy.GetBlockCacheOption().memorySize);
    ASSERT_EQ((uint32_t)4096, loadStrategy.GetBlockCacheOption().blockSize);
    ASSERT_EQ("lru", loadStrategy.GetBlockCacheOption().cacheType);
    ASSERT_TRUE(!loadStrategy.UseDirectIO());
    ASSERT_TRUE(!loadStrategy.UseGlobalCache());

    // jsonize default value
    FromJsonString(loadStrategy, "{}");
    ASSERT_EQ((uint32_t)(1 * 1024 * 1024), loadStrategy.GetBlockCacheOption().memorySize);
    ASSERT_EQ((uint32_t)4096, loadStrategy.GetBlockCacheOption().blockSize);
    ASSERT_EQ("lru", loadStrategy.GetBlockCacheOption().cacheType);
    ASSERT_TRUE(!loadStrategy.UseDirectIO());
    ASSERT_TRUE(!loadStrategy.UseGlobalCache());
}

TEST_F(CacheLoadStrategyTest, TestDadi)
{
    string jsonStr = R"({
        "memory_size_in_mb" : 512,
        "block_size" : 8192,
        "direct_io" : true,
        "cache_type" : "dadi",
        "disk_size_in_gb" : 10,
        "cache_params" : {
            "nothing" : "32"
        }
    })";
    CacheLoadStrategy loadStrategy;
    FromJsonString(loadStrategy, jsonStr);
    auto& blockCacheOption = loadStrategy.GetBlockCacheOption();
    ASSERT_EQ("dadi", blockCacheOption.cacheType);
    ASSERT_EQ(512 * 1024 * 1024, blockCacheOption.memorySize);
    ASSERT_EQ(8192, blockCacheOption.blockSize);
    ASSERT_EQ(10UL * 1024 * 1024 * 1024, blockCacheOption.diskSize);
    util::KeyValueMap kv = {{"nothing", "32"}};
    ASSERT_EQ(kv, blockCacheOption.cacheParams);
}
}} // namespace indexlib::file_system
