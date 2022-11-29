/**
 * @file   knn_dynamic_config_test.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Fri Jan 18 13:43:27 2019
 *
 * @brief
 *
 *
 */
#include <fslib/fslib.h>
#include <unittest/unittest.h>
#include <indexlib/file_system/indexlib_file_system.h>
#include <indexlib/file_system/indexlib_file_system_impl.h>
#include <indexlib/file_system/directory_creator.h>
#include "aitheta_indexer/plugins/aitheta/util/knn_config.h"
#include "aitheta_indexer/test/test.h"
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "indexlib/file_system/file_system_options.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(aitheta_plugin);

class KnnConfigTest : public ::testing::Test {
 protected:
    KnnConfigTest() {}

    virtual ~KnnConfigTest() {}

    virtual void SetUp() { _testDir = string(TEST_DATA_PATH) + "/knn_config_test/"; }

    virtual void TearDown() {}

    void TestRangeConfigs(KnnStrategy strategy, uint32_t baseLevel, bool asDefault = false) {
        ASSERT_EQ(asDefault, strategy.useAsDefault);
        ASSERT_EQ(3, strategy.rangeParamsList.size());
        for (size_t i = 0; i < strategy.rangeParamsList.size(); ++i) {
            ASSERT_EQ(40000 * (i + 1), strategy.rangeParamsList[i].upperLimit);
            ASSERT_EQ("hnsw_compress", strategy.rangeParamsList[i].params["builder.type"]);
            ASSERT_EQ(StringUtil::toString(baseLevel * (i + 1)),
                      strategy.rangeParamsList[i].params["builder.max_level"]);
            ASSERT_EQ("hnsw", strategy.rangeParamsList[i].params["searcher.type"]);
            ASSERT_EQ(StringUtil::toString(2 * baseLevel * (i + 1)),
                      strategy.rangeParamsList[i].params["searcher.max_level"]);
        }
    }

    string _testDir;

 protected:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(aitheta_plugin, KnnConfigTest);

TEST_F(KnnConfigTest, testKnnRangeParams) {
    string path = _testDir + "/knn_range_config.json";
    string content;
    ASSERT_TRUE(EC_OK == FileSystem::readFile(path, content));

    KnnRangeParams config;
    try {
        FromJsonString(config, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
        ASSERT_TRUE(false);
    }
    ASSERT_EQ(true, config.IsAvailable());
    ASSERT_EQ(40000, config.upperLimit);
    ASSERT_EQ("hnsw_compress", config.params["builder.type"]);
    ASSERT_EQ("1", config.params["builder.max_level"]);
    ASSERT_EQ("hnsw", config.params["searcher.type"]);
    ASSERT_EQ("2", config.params["searcher.max_level"]);
}

TEST_F(KnnConfigTest, testKnnRangeParamsMerge) {
    string path = _testDir + "/knn_range_config.json";
    string content;
    ASSERT_TRUE(EC_OK == FileSystem::readFile(path, content));

    KnnRangeParams config;
    try {
        FromJsonString(config, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
        ASSERT_TRUE(false);
    }
    KnnRangeParams config1;
    config1.params["builder.type"] = "eeee";
    config1.params["builder.type1"] = "oooo";
    config1.params["builder.type2"] = "aaaa";
    config1.params["searcher.type"] = "eeee";
    config1.params["searcher.type1"] = "oooo";
    config1.params["searcher.type2"] = "aaaa";

    ASSERT_EQ(true, config.IsAvailable());
    ASSERT_EQ(40000, config.upperLimit);
    ASSERT_EQ("hnsw_compress", config.params["builder.type"]);
    ASSERT_EQ("1", config.params["builder.max_level"]);
    ASSERT_EQ("hnsw", config.params["searcher.type"]);
    ASSERT_EQ("2", config.params["searcher.max_level"]);

    config.Merge(config1);
    ASSERT_EQ(true, config.IsAvailable());
    ASSERT_EQ(40000, config.upperLimit);
    ASSERT_EQ("hnsw_compress", config.params["builder.type"]);
    ASSERT_EQ("oooo", config.params["builder.type1"]);
    ASSERT_EQ("aaaa", config.params["builder.type2"]);
    ASSERT_EQ("1", config.params["builder.max_level"]);
    ASSERT_EQ("hnsw", config.params["searcher.type"]);
    ASSERT_EQ("2", config.params["searcher.max_level"]);
    ASSERT_EQ("oooo", config.params["searcher.type1"]);
    ASSERT_EQ("aaaa", config.params["searcher.type2"]);
}

TEST_F(KnnConfigTest, testKnnRangeParamsRangeEq0) {
    string path = _testDir + "/knn_range_config_0_range.json";
    string content;
    ASSERT_TRUE(EC_OK == FileSystem::readFile(path, content));

    KnnRangeParams config;
    try {
        FromJsonString(config, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
        ASSERT_TRUE(false);
    }
    ASSERT_EQ(false, config.IsAvailable());
    ASSERT_EQ(0, config.upperLimit);
    ASSERT_EQ("hnsw_compress", config.params["builder.type"]);
    ASSERT_EQ("1", config.params["builder.max_level"]);
    ASSERT_EQ("hnsw", config.params["searcher.type"]);
    ASSERT_EQ("2", config.params["searcher.max_level"]);
}

TEST_F(KnnConfigTest, testKnnStrategy) {
    string path = _testDir + "/knn_range_configs.json";
    string content;
    ASSERT_TRUE(EC_OK == FileSystem::readFile(path, content));

    KnnStrategy configs;
    try {
        FromJsonString(configs, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
        ASSERT_TRUE(false);
    }

    ASSERT_EQ(true, configs.IsAvailable());
    TestRangeConfigs(configs, 1);
}

TEST_F(KnnConfigTest, testKnnStrategyUserAsDefault) {
    string path = _testDir + "/knn_range_configs_default.json";
    string content;
    ASSERT_TRUE(EC_OK == FileSystem::readFile(path, content));

    KnnStrategy configs;
    try {
        FromJsonString(configs, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
        ASSERT_TRUE(false);
    }

    ASSERT_EQ(true, configs.IsAvailable());
    TestRangeConfigs(configs, 1, true);
}

TEST_F(KnnConfigTest, testKnnStrategyBadRangeList) {
    string path = _testDir + "/knn_range_configs_bad_range_list.json";
    string content;
    ASSERT_TRUE(EC_OK == FileSystem::readFile(path, content));

    KnnStrategy configs;
    try {
        FromJsonString(configs, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
        ASSERT_TRUE(false);
    }

    ASSERT_EQ(false, configs.IsAvailable());
}

TEST_F(KnnConfigTest, testKnnStrategyOneIsBad) {
    string path = _testDir + "/knn_range_configs_bad_one.json";
    string content;
    ASSERT_TRUE(EC_OK == FileSystem::readFile(path, content));

    KnnStrategy configs;
    try {
        FromJsonString(configs, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
        ASSERT_TRUE(false);
    }

    ASSERT_EQ(false, configs.IsAvailable());
}

TEST_F(KnnConfigTest, testKnnStrategies) {
    string path = _testDir + "/knn_dynamic_config.json";
    string content;
    ASSERT_TRUE(EC_OK == FileSystem::readFile(path, content));

    KnnStrategies configs;
    try {
        FromJsonString(configs, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
        ASSERT_TRUE(false);
    }

    ASSERT_EQ(true, configs.IsAvailable());
    KnnStrategy strategy = configs.strategy2RangeParams["blance"];
    TestRangeConfigs(strategy, 1, true);
    strategy = configs.strategy2RangeParams["low_rt"];
    TestRangeConfigs(strategy, 10);
}

TEST_F(KnnConfigTest, testKnnStrategiesToJson) {
    string path = _testDir + "/knn_dynamic_config.json";
    string content;
    ASSERT_TRUE(EC_OK == FileSystem::readFile(path, content));

    KnnStrategies configs;
    try {
        FromJsonString(configs, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
        ASSERT_TRUE(false);
    }

    string content1 = ToJsonString(configs);
    KnnStrategies configs1;
    try {
        FromJsonString(configs1, content1);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content1.c_str());
        ASSERT_TRUE(false);
    }

    ASSERT_EQ(true, configs1.IsAvailable());
    KnnStrategy strategy = configs1.strategy2RangeParams["blance"];
    TestRangeConfigs(strategy, 1, true);
    strategy = configs.strategy2RangeParams["low_rt"];
    TestRangeConfigs(strategy, 10);
}

TEST_F(KnnConfigTest, testKnnConfigBad) {
    string path = _testDir + "/knn_dynamic_config_bad.json";
    string content;
    ASSERT_TRUE(EC_OK == FileSystem::readFile(path, content));

    KnnStrategies configs;
    try {
        FromJsonString(configs, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
        ASSERT_TRUE(false);
    }

    ASSERT_EQ(false, configs.IsAvailable());
}

TEST_F(KnnConfigTest, testKnnStrategiesNoDefault) {
    string path = _testDir + "/knn_dynamic_config_no_default.json";
    string content;
    ASSERT_TRUE(EC_OK == FileSystem::readFile(path, content));

    KnnStrategies configs;
    try {
        FromJsonString(configs, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
        ASSERT_TRUE(false);
    }

    ASSERT_EQ(false, configs.IsAvailable());
    KnnStrategy strategy = configs.strategy2RangeParams["blance"];
    TestRangeConfigs(strategy, 1);
    strategy = configs.strategy2RangeParams["low_rt"];
    TestRangeConfigs(strategy, 10);
}

TEST_F(KnnConfigTest, testKnnConfig) {
    string path = _testDir + "/knn_dynamic_configs.json";
    string content;
    ASSERT_TRUE(EC_OK == FileSystem::readFile(path, content));

    KnnConfig configs;
    try {
        FromJsonString(configs, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
        ASSERT_TRUE(false);
    }

    ASSERT_EQ(true, configs.IsAvailable());
    KnnStrategy strategy = configs.type2Strategies["hc"].strategy2RangeParams["blance"];
    TestRangeConfigs(strategy, 1, true);
    strategy = configs.type2Strategies["hc"].strategy2RangeParams[DEFAULT_STRATEGY];
    TestRangeConfigs(strategy, 1, true);
    strategy = configs.type2Strategies["hc"].strategy2RangeParams["low_rt"];
    TestRangeConfigs(strategy, 10);
    strategy = configs.type2Strategies["graph"].strategy2RangeParams["blance"];
    TestRangeConfigs(strategy, 5);
    strategy = configs.type2Strategies["graph"].strategy2RangeParams["low_rt"];
    TestRangeConfigs(strategy, 10, true);
    strategy = configs.type2Strategies["graph"].strategy2RangeParams[DEFAULT_STRATEGY];
    TestRangeConfigs(strategy, 10, true);
}

TEST_F(KnnConfigTest, testKnnConfigToJson) {
    string path = _testDir + "/knn_dynamic_configs.json";
    string content;
    ASSERT_TRUE(EC_OK == FileSystem::readFile(path, content));

    KnnConfig configs;
    try {
        FromJsonString(configs, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
        ASSERT_TRUE(false);
    }

    string content1 = ToJsonString(configs);
    KnnConfig configs1;
    try {
        FromJsonString(configs1, content1);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content1.c_str());
        ASSERT_TRUE(false);
    }

    ASSERT_EQ(true, configs1.IsAvailable());
    KnnStrategy strategy = configs1.type2Strategies["hc"].strategy2RangeParams["blance"];
    TestRangeConfigs(strategy, 1, true);
    strategy = configs1.type2Strategies["hc"].strategy2RangeParams[DEFAULT_STRATEGY];
    TestRangeConfigs(strategy, 1, true);
    strategy = configs1.type2Strategies["hc"].strategy2RangeParams["low_rt"];
    TestRangeConfigs(strategy, 10);
    strategy = configs1.type2Strategies["graph"].strategy2RangeParams["blance"];
    TestRangeConfigs(strategy, 5);
    strategy = configs1.type2Strategies["graph"].strategy2RangeParams["low_rt"];
    TestRangeConfigs(strategy, 10, true);
    strategy = configs1.type2Strategies["graph"].strategy2RangeParams[DEFAULT_STRATEGY];
    TestRangeConfigs(strategy, 10, true);
}

TEST_F(KnnConfigTest, testDumpAndLoad) {
    IndexlibFileSystemPtr fileSystem(new IndexlibFileSystemImpl(TEST_DATA_PATH, ""));
    FileSystemOptions options;
    options.needFlush = true;
    options.enableAsyncFlush = false;
    options.useRootLink = false;

    util::PartitionMemoryQuotaControllerPtr controller;
    util::MemoryQuotaControllerPtr mqc(new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
    controller.reset(new util::PartitionMemoryQuotaController(mqc));

    options.memoryQuotaController = controller;
    fileSystem->Init(options);

    auto dir = DirectoryCreator::Create(fileSystem, _testDir);

    string content = R"({
    "dynamic_configs": [
        {
            "type": "hc",
            "strategies": [
                {
                    "name": "v0",
                    "dynamic_param_list": [
                        {
                            "upper_limit": 50000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "10"
                            }
                        },
                        {
                            "upper_limit": 100000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "20"
                            }
                        }
                    ]
                },
                {
                    "name": "mainse",
                    "as_default": true,
                    "dynamic_param_list": [
                        {
                            "upper_limit": 50000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100"
                            }
                        }
                    ]
                }
            ]
        }
    ]
})";
    KnnConfig dumpConfig;
    FromJsonString(dumpConfig, content);
    ASSERT_TRUE(dumpConfig.Dump(dir));

    KnnConfig loadConfig0;
    ASSERT_TRUE(loadConfig0.Load(dir));
    ASSERT_EQ(1u, loadConfig0.type2Strategies.size());
    ASSERT_EQ("v0", loadConfig0.type2Strategies["hc"].strategy2RangeParams["v0"].name);
    ASSERT_EQ(false, loadConfig0.type2Strategies["hc"].strategy2RangeParams["v0"].useAsDefault);
    ASSERT_EQ(2u, loadConfig0.type2Strategies["hc"].strategy2RangeParams["v0"].rangeParamsList.size());
    ASSERT_EQ("mainse", loadConfig0.type2Strategies["hc"].strategy2RangeParams["mainse"].name);
    ASSERT_EQ(true, loadConfig0.type2Strategies["hc"].strategy2RangeParams["mainse"].useAsDefault);
    ASSERT_EQ(1u, loadConfig0.type2Strategies["hc"].strategy2RangeParams["mainse"].rangeParamsList.size());
}

IE_NAMESPACE_END(aitheta_plugin);
