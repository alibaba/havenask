#include "indexlib/file_system/load_config/test/cache_load_strategy_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, CacheLoadStrategyTest);

CacheLoadStrategyTest::CacheLoadStrategyTest()
{
}

CacheLoadStrategyTest::~CacheLoadStrategyTest()
{
}

void CacheLoadStrategyTest::CaseSetUp()
{
}

void CacheLoadStrategyTest::CaseTearDown()
{
}

void CacheLoadStrategyTest::TestCheck()
{
    {
        string jsonStr = "{                           \
                              \"cache_size\" : 0,   \
                              \"block_size\" : 8192,  \
                              \"direct_io\" : true    \
                          }";
        CacheLoadStrategy loadStrategy;
        FromJsonString(loadStrategy, jsonStr);
        ASSERT_ANY_THROW(loadStrategy.Check());
    }

    {
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

void CacheLoadStrategyTest::TestParse()
{
    {
        CacheLoadStrategy loadStrategy;
        string loadStrategyStr = ToJsonString(loadStrategy);
        CacheLoadStrategy resultLoadStrategy;
        FromJsonString(resultLoadStrategy, loadStrategyStr);
    
        INDEXLIB_TEST_EQUAL(loadStrategy, resultLoadStrategy);
    }
    {
        string jsonStr = "{                           \
                              \"cache_size\" : 512,   \
                              \"block_size\" : 8192,  \
                              \"direct_io\" : true    \
                          }";
        CacheLoadStrategy loadStrategy;
        FromJsonString(loadStrategy, jsonStr);
        INDEXLIB_TEST_EQUAL((uint32_t)(512 * 1024 * 1024),
                            loadStrategy.GetCacheSize());
        INDEXLIB_TEST_EQUAL((uint32_t)8192, loadStrategy.GetBlockSize());
        INDEXLIB_TEST_TRUE(loadStrategy.UseDirectIO());
        INDEXLIB_TEST_TRUE(!loadStrategy.UseGlobalCache());
        INDEXLIB_TEST_TRUE(!loadStrategy.CacheDecompressFile());
    }
    {
        string jsonStr = "{                           \
                              \"direct_io\" : true,   \
                              \"global_cache\" : true,  \
                              \"cache_decompress_file\" : true \
                          }";
        CacheLoadStrategy loadStrategy;
        FromJsonString(loadStrategy, jsonStr);
        INDEXLIB_TEST_TRUE(loadStrategy.UseDirectIO());
        INDEXLIB_TEST_TRUE(loadStrategy.UseGlobalCache());
        INDEXLIB_TEST_TRUE(loadStrategy.CacheDecompressFile());
    }

}

void CacheLoadStrategyTest::TestDefaultValue()
{
    // constructor default value
    CacheLoadStrategy loadStrategy;
    INDEXLIB_TEST_EQUAL((uint32_t)(1 * 1024 * 1024),
                        loadStrategy.GetCacheSize());
    INDEXLIB_TEST_EQUAL((uint32_t)4096, loadStrategy.GetBlockSize());
    INDEXLIB_TEST_TRUE(!loadStrategy.UseDirectIO());
    INDEXLIB_TEST_TRUE(!loadStrategy.UseGlobalCache());
    // jsonize default value
    FromJsonString(loadStrategy, "{}");
    INDEXLIB_TEST_EQUAL((uint32_t)(1 * 1024 * 1024),
                        loadStrategy.GetCacheSize());
    INDEXLIB_TEST_EQUAL((uint32_t)4096, loadStrategy.GetBlockSize());
    INDEXLIB_TEST_TRUE(!loadStrategy.UseDirectIO());
    INDEXLIB_TEST_TRUE(!loadStrategy.UseGlobalCache());
}

IE_NAMESPACE_END(file_system);

