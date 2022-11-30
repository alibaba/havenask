/**
 * @file   knn_params_selector_test.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Tue Jan 22 13:38:57 2019
 *
 * @brief
 *
 *
 */
#include <string>
#include <map>
#include <unittest/unittest.h>
#include <fslib/fslib.h>

#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/lr_knn_params_selector.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/pq_knn_params_selector.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/hc_knn_params_selector.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/graph_knn_params_selector.h"
#include "aitheta_indexer/test/test.h"
#include "aitheta_indexer/plugins/aitheta/common_define.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace aitheta;

IE_NAMESPACE_BEGIN(aitheta_plugin);

class KnnParamsSelectorTest : public ::testing::Test {
 public:
    KnnParamsSelectorTest() {}
    virtual ~KnnParamsSelectorTest() {}

    virtual void SetUp() { _testDir = string(TEST_DATA_PATH) + "/knn_config_selector_test/"; }

    virtual void TearDown() {}

 protected:
    IKnnParamsSelectorPtr MakeSelector(const string &name, const string &configFile) {
        string path = _testDir + configFile;
        string content;
        FileSystem::readFile(path, content);

        KnnStrategies strategies;
        try {
            FromJsonString(strategies, content);
        } catch (const autil::legacy::ExceptionBase &e) {
            IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
            return IKnnParamsSelectorPtr();
        }

        if (name == INDEX_TYPE_LR) {
            return IKnnParamsSelectorPtr(new LRKnnParamsSelector(strategies));
        } else if (name == INDEX_TYPE_PQ) {
            return IKnnParamsSelectorPtr(new PQKnnParamsSelector(strategies));
        } else if (name == INDEX_TYPE_HC) {
            return IKnnParamsSelectorPtr(new HCKnnParamsSelector(strategies));
        } else if (name == INDEX_TYPE_GRAPH) {
            return IKnnParamsSelectorPtr(new GraphKnnParamsSelector(strategies));
        } else {
            return IKnnParamsSelectorPtr();
        }
    }
    IKnnParamsSelectorPtr MakeSelectorFromStr(const string &name, const string &content) {
        KnnConfig config;
        try {
            FromJsonString(config, content);
        } catch (const autil::legacy::ExceptionBase &e) {
            IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), content.c_str());
            return IKnnParamsSelectorPtr();
        }

        if (name == INDEX_TYPE_LR) {
            return IKnnParamsSelectorPtr(new LRKnnParamsSelector(config.type2Strategies[INDEX_TYPE_LR]));
        } else if (name == INDEX_TYPE_PQ) {
            return IKnnParamsSelectorPtr(new PQKnnParamsSelector(config.type2Strategies[INDEX_TYPE_PQ]));
        } else if (name == INDEX_TYPE_HC) {
            return IKnnParamsSelectorPtr(new HCKnnParamsSelector(config.type2Strategies[INDEX_TYPE_HC]));
        } else if (name == INDEX_TYPE_GRAPH) {
            return IKnnParamsSelectorPtr(new GraphKnnParamsSelector(config.type2Strategies[INDEX_TYPE_GRAPH]));
        } else {
            return IKnnParamsSelectorPtr();
        }
    }
 protected:
    string _testDir;

 protected:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(aitheta_plugin, KnnParamsSelectorTest);

TEST_F(KnnParamsSelectorTest, testPQBuilderParamsNotUseDynamic) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_PQ, "knn_pq_strategy.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {{DIMENSION, "72"},
                                      {DOCUMENT_NUM, "9000"},
                                      {INDEX_TYPE, INDEX_TYPE_PQ},
                                      {BUILD_METRIC_TYPE, L2},
                                      {SEARCH_METRIC_TYPE, L2},
                                      {USE_LINEAR_THRESHOLD, "1"},
                                      {proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM, "8192"},
                                      {proxima::PARAM_PQ_BUILDER_TRAIN_PRODUCT_CENTROID_NUM, "256"},
                                      {proxima::PARAM_PQ_BUILDER_TRAIN_FRAGMENT_NUM, "8"},
                                      {proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA, "1000000000"},
                                      {proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO, "1"},
                                      {proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM, "2000"}};

    aitheta::IndexParams params;
    bool ret = selector->InitKnnBuilderParams(parameters, params, true);
    EXPECT_EQ(8, params.getInt64(proxima::PARAM_PQ_BUILDER_TRAIN_FRAGMENT_NUM));
    EXPECT_EQ(256, params.getInt64(proxima::PARAM_PQ_BUILDER_TRAIN_PRODUCT_CENTROID_NUM));
    EXPECT_EQ(8192, params.getInt64(proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM));
    EXPECT_EQ(1000000000, params.getInt64(proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA));
    EXPECT_EQ(9000, params.getInt64(proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT));
}

TEST_F(KnnParamsSelectorTest, testPQBuilderParamsUseConfigDynamic) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_PQ, "knn_pq_strategy.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {
        {DIMENSION, "72"},         {DOCUMENT_NUM, "9000"},        {INDEX_TYPE, INDEX_TYPE_PQ},
        {BUILD_METRIC_TYPE, L2},   {SEARCH_METRIC_TYPE, L2},      {USE_LINEAR_THRESHOLD, "1"},
        {USE_DYNAMIC_PARAMS, "1"}, {STRATEGY_NAME, "high_ratio"},
    };

    aitheta::IndexParams params;
    bool ret = selector->InitKnnBuilderParams(parameters, params, true);
    EXPECT_EQ(16, params.getInt64(proxima::PARAM_PQ_BUILDER_TRAIN_FRAGMENT_NUM));
    EXPECT_EQ(256, params.getInt64(proxima::PARAM_PQ_BUILDER_TRAIN_PRODUCT_CENTROID_NUM));
    EXPECT_EQ(40, params.getInt64(proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM));
    EXPECT_EQ(1024000, params.getInt64(proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA));
    EXPECT_EQ(1600, params.getInt64(proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT));
}

TEST_F(KnnParamsSelectorTest, testPQBuilderParamsUseConfigDynamic1) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_PQ, "knn_pq_strategy.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {{DIMENSION, "72"},         {DOCUMENT_NUM, "100"},    {INDEX_TYPE, INDEX_TYPE_PQ},
                                      {BUILD_METRIC_TYPE, L2},   {SEARCH_METRIC_TYPE, L2}, {USE_LINEAR_THRESHOLD, "1"},
                                      {USE_DYNAMIC_PARAMS, "1"}, {STRATEGY_NAME, "low_rt"}};

    aitheta::IndexParams params;
    bool ret = selector->InitKnnBuilderParams(parameters, params, true);
    EXPECT_EQ(24, params.getInt64(proxima::PARAM_PQ_BUILDER_TRAIN_FRAGMENT_NUM));
    EXPECT_EQ(100, params.getInt64(proxima::PARAM_PQ_BUILDER_TRAIN_PRODUCT_CENTROID_NUM));
    EXPECT_EQ(60, params.getInt64(proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM));
    EXPECT_EQ(153600, params.getInt64(proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA));
    EXPECT_EQ(100, params.getInt64(proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT));
}

TEST_F(KnnParamsSelectorTest, testPQBuilderParamsUseParamsDynamic) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_PQ, "knn_pq_strategy.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {
        {DIMENSION, "72"},
        {DOCUMENT_NUM, "100"},
        {INDEX_TYPE, INDEX_TYPE_PQ},
        {BUILD_METRIC_TYPE, L2},
        {SEARCH_METRIC_TYPE, L2},
        {USE_LINEAR_THRESHOLD, "1"},
        {USE_DYNAMIC_PARAMS, "1"},
        {STRATEGY_NAME, "low_rt"},
        {proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, "40"},
        {DYNAMIC_STRATEGY,
         "{\"name\":\"blance\",\"as_default\":true,\"dynamic_param_list\":[{\"upper_limit\":5000,\"params\":{\"proxima."
         "pq.builder.train_fragment_number\":\"8\",\"proxima.pq.builder.train_coarse_centroid_number\":\"20\","
         "\"proxima.general.builder.memory_quota\":\"51200\",\"proxima.general.builder.train_sample_count\":\"800\","
         "\"proxima.pq.builder.train_fragment_number\":\"8\",\"proxima.pq.builder.train_coarse_"
         "centroid_number\":\"100\",\"proxima.general.builder.memory_quota\":\"51200\",\"proxima.general.builder.train_"
         "sample_count\":\"1600\"}},{\"upper_limit\":10000,\"build_params\":{\"proxima.pq.builder.train_fragment_"
         "number\":\"32\",\"proxima.pq.builder.train_coarse_centroid_number\":\"80\",\"proxima.general.builder.memory_"
         "quota\":\"2048000\",\"proxima.general.builder.train_sample_count\":\"1600\"},\"search_params\":{\"proxima.pq."
         "builder.train_fragment_number\":\"36\",\"proxima.pq.builder.train_coarse_centroid_number\":\"400\",\"proxima."
         "general.builder.memory_quota\":\"2048000\",\"proxima.general.builder.train_sample_count\":\"\"}}]}"},
        {proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA, "1000000000"},
    };

    aitheta::IndexParams params;
    bool ret = selector->InitKnnBuilderParams(parameters, params, true);
    EXPECT_EQ(24, params.getInt64(proxima::PARAM_PQ_BUILDER_TRAIN_FRAGMENT_NUM));
    EXPECT_EQ(100, params.getInt64(proxima::PARAM_PQ_BUILDER_TRAIN_PRODUCT_CENTROID_NUM));
    EXPECT_EQ(60, params.getInt64(proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM));
    EXPECT_EQ(1000000000, params.getInt64(proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA));
    EXPECT_EQ(40, params.getInt64(proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT));
}

TEST_F(KnnParamsSelectorTest, testPQSearcherParamsNotUseDynamic) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_PQ, "knn_pq_strategy.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {{DIMENSION, "72"},
                                      {DOCUMENT_NUM, "9000"},
                                      {INDEX_TYPE, INDEX_TYPE_PQ},
                                      {BUILD_METRIC_TYPE, L2},
                                      {SEARCH_METRIC_TYPE, L2},
                                      {USE_LINEAR_THRESHOLD, "1"},
                                      {proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO, "1"},
                                      {proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM, "2000"}};
    aitheta::IndexParams params;
    bool ret = selector->InitKnnSearcherParams(parameters, params, true);
    EXPECT_EQ(1, params.getFloat(proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO));
    EXPECT_EQ(2000, params.getInt64(proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM));
    EXPECT_EQ(IndexDistance::kMethodFloatSquaredEuclidean,
              params.getInt64(proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD));
}

TEST_F(KnnParamsSelectorTest, testPQSearcherParamsUseParamsDynamic) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_PQ, "knn_pq_strategy.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {
        {DIMENSION, "72"},
        {DOCUMENT_NUM, "9000"},
        {INDEX_TYPE, INDEX_TYPE_PQ},
        {USE_LINEAR_THRESHOLD, "1"},
        {USE_DYNAMIC_PARAMS, "1"},
        {proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM, "2000"},
        {DYNAMIC_STRATEGY,
         "{\"dynamic_param_list\":[{\"upper_limit\":5000,\"params\":{\"proxima.pq.builder.train_fragment_"
         "number\":\"32\",\"proxima.pq.builder.train_coarse_centroid_number\":\"80\",\"proxima.general.builder.memory_"
         "quota\":\"2048000\",\"proxima.general.builder.train_sample_count\":\"1600\",\"search_"
         "metric_type\":\"INNER_PRODUCT\",\"proxima.pq.searcher.coarse_scan_ratio\":\"0.40\",\"proxima.pq.searcher."
         "product_scan_number\":\"200\"}},{\"upper_limit\":10000,\"build_params\":{\"proxima.pq.builder.train_fragment_"
         "number\":\"32\",\"proxima.pq.builder.train_coarse_centroid_number\":\"80\",\"proxima.general.builder.memory_"
         "quota\":\"2048000\",\"proxima.general.builder.train_sample_count\":\"1600\"},\"search_params\":{\"search_"
         "metric_type\":\"INNER_PRODUCT\",\"proxima.pq.searcher.coarse_scan_ratio\":\"0.20\",\"proxima.pq.searcher."
         "product_scan_number\":\"200\"}}]}"}};

    aitheta::IndexParams params;
    bool ret = selector->InitKnnSearcherParams(parameters, params, true);
    EXPECT_FLOAT_EQ(0.01, params.getFloat(proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO));
    EXPECT_EQ(2000, params.getInt64(proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM));
    EXPECT_EQ(IndexDistance::kMethodUnknown, params.getInt64(proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD));
}

TEST_F(KnnParamsSelectorTest, testHCBuilderParamsNotUseDynamic) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_HC, "empty_config.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {{DIMENSION, "72"},
                                      {INDEX_TYPE, INDEX_TYPE_HC},
                                      {BUILD_METRIC_TYPE, L2},
                                      {SEARCH_METRIC_TYPE, L2},
                                      {DOCUMENT_NUM, "1000"},
                                      {USE_DYNAMIC_PARAMS, "1"},
                                      {DYNAMIC_STRATEGY, "low_rt"},
                                      {proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, "32"},
                                      {proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1", "2000"},
                                      {proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2", "200"},
                                      {proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM, "400000"},
                                      {proxima::PARAM_HC_COMMON_LEVEL_CNT, "2"},
                                      {proxima::PARAM_HC_COMMON_MAX_DOC_CNT, "100000000"},
                                      {proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, "10000000"},
                                      {proxima::PARAM_HC_BUILDER_MEMORY_QUOTA, "2000000000"},
                                      {PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO, "0.1"}};

    aitheta::IndexParams params;
    bool ret = selector->InitKnnBuilderParams(parameters, params, true);
    EXPECT_EQ(32, params.getInt64(proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE));
    EXPECT_EQ(2000, params.getInt64(proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1"));
    EXPECT_EQ(200, params.getInt64(proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2"));
    EXPECT_EQ(400000, params.getInt64(proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM));
    EXPECT_EQ(2, params.getInt64(proxima::PARAM_HC_COMMON_LEVEL_CNT));
    EXPECT_EQ(1001, params.getInt64(proxima::PARAM_HC_COMMON_MAX_DOC_CNT));
    EXPECT_EQ(1000, params.getInt64(proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT));
    EXPECT_EQ(2000000000, params.getInt64(proxima::PARAM_HC_BUILDER_MEMORY_QUOTA));
    EXPECT_EQ("hc_builder_temp", params.getString(proxima::PARAM_HC_BUILDER_BASIC_PATH));
    EXPECT_EQ(true, params.getBool(proxima::PARAM_HC_COMMON_COMBO_FILE));
}

TEST_F(KnnParamsSelectorTest, testHCBuilderParamsUseParamsDynamic) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_HC, "knn_hc_strategy.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {{DIMENSION, "72"},         {INDEX_TYPE, INDEX_TYPE_HC},
                                      {BUILD_METRIC_TYPE, L2},   {SEARCH_METRIC_TYPE, L2},
                                      {DOCUMENT_NUM, "2000"},    {USE_DYNAMIC_PARAMS, "1"},
                                      {STRATEGY_NAME, "low_rt"}, {proxima::PARAM_HC_BUILDER_MEMORY_QUOTA, "20000"}};

    aitheta::IndexParams params;
    bool ret = selector->InitKnnBuilderParams(parameters, params, true);
    EXPECT_EQ(32, params.getInt64(proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE));
    EXPECT_EQ(2000, params.getInt64(proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1"));
    EXPECT_EQ(200, params.getInt64(proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2"));
    EXPECT_EQ(400000, params.getInt64(proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM));
    EXPECT_EQ(2, params.getInt64(proxima::PARAM_HC_COMMON_LEVEL_CNT));
    EXPECT_EQ(2001, params.getInt64(proxima::PARAM_HC_COMMON_MAX_DOC_CNT));
    EXPECT_EQ(1000, params.getInt64(proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT));
    EXPECT_EQ(2000000000, params.getInt64(proxima::PARAM_HC_BUILDER_MEMORY_QUOTA));
    EXPECT_EQ("hc_builder_temp", params.getString(proxima::PARAM_HC_BUILDER_BASIC_PATH));
    EXPECT_EQ(true, params.getBool(proxima::PARAM_HC_COMMON_COMBO_FILE));
}

TEST_F(KnnParamsSelectorTest, testHCSearcherParamsNotUseDynamic) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_HC, "empty_config.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {{DIMENSION, "72"},
                                      {INDEX_TYPE, INDEX_TYPE_HC},
                                      {BUILD_METRIC_TYPE, L2},
                                      {SEARCH_METRIC_TYPE, L2},
                                      {DOCUMENT_NUM, "1000"},
                                      {USE_DYNAMIC_PARAMS, "1"},
                                      {DYNAMIC_STRATEGY, "low_rt"},
                                      {proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, "32"},
                                      {proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "1", "2000"},
                                      {proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "2", "200"},
                                      {proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM, "400000"},
                                      {proxima::PARAM_HC_COMMON_LEVEL_CNT, "2"},
                                      {proxima::PARAM_HC_COMMON_MAX_DOC_CNT, "100000000"},
                                      {proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, "10000000"},
                                      {proxima::PARAM_HC_BUILDER_MEMORY_QUOTA, "2000000000"},
                                      {proxima::PARAM_HC_SEARCHER_TOPK, "30"},
                                      {proxima::PARAM_HC_SEARCHER_MAX_SCAN_NUM, "400"},
                                      {proxima::PARAM_HC_COMMON_COMBO_FILE, "0"},
                                      {PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO, "0.1"}};

    aitheta::IndexParams params;
    bool ret = selector->InitKnnSearcherParams(parameters, params, true);
    EXPECT_EQ(32, params.getInt64(proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE));
    EXPECT_EQ(2000, params.getInt64(proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "1"));
    EXPECT_EQ(200, params.getInt64(proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "2"));
    EXPECT_EQ(400, params.getInt64(proxima::PARAM_HC_SEARCHER_MAX_SCAN_NUM));
    EXPECT_EQ(30, params.getInt64(proxima::PARAM_HC_SEARCHER_TOPK));
    EXPECT_EQ(IndexDistance::kMethodFloatSquaredEuclidean,
              params.getInt64(proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD));
    EXPECT_EQ(true, params.getBool(proxima::PARAM_HC_COMMON_COMBO_FILE));
}

TEST_F(KnnParamsSelectorTest, testHCSearcherParamsUseParamsDynamic) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_HC, "knn_hc_strategy.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {
        {DIMENSION, "72"},
        {INDEX_TYPE, INDEX_TYPE_HC},
        {BUILD_METRIC_TYPE, INNER_PRODUCT},
        {SEARCH_METRIC_TYPE, INNER_PRODUCT},
        {DOCUMENT_NUM, "2000"},
        {USE_DYNAMIC_PARAMS, "1"},
        {STRATEGY_NAME, "low_rt"},
        {proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "1", "2000"},
        {proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "2", "200"},
        {proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM, "400000"},
        {PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO, "0.1"},
    };
    aitheta::IndexParams params;
    bool ret = selector->InitKnnSearcherParams(parameters, params, true);
    EXPECT_EQ(32, params.getInt64(proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE));
    EXPECT_EQ(100, params.getInt64(proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "1"));
    EXPECT_EQ(20, params.getInt64(proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "2"));
    EXPECT_EQ(300, params.getInt64(proxima::PARAM_HC_SEARCHER_MAX_SCAN_NUM));
    EXPECT_EQ(10, params.getInt64(proxima::PARAM_HC_SEARCHER_TOPK));
    EXPECT_EQ(IndexDistance::kMethodFloatInnerProduct, params.getInt64(proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD));
    EXPECT_EQ(true, params.getBool(proxima::PARAM_HC_COMMON_COMBO_FILE));
}

TEST_F(KnnParamsSelectorTest, testGraphBuilderParamsNotUseDynamic) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_GRAPH, "empty_config.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {{DIMENSION, "72"},
                                      {INDEX_TYPE, INDEX_TYPE_GRAPH},
                                      {BUILD_METRIC_TYPE, L2},
                                      {SEARCH_METRIC_TYPE, L2},
                                      {DOCUMENT_NUM, "1000"},
                                      {USE_DYNAMIC_PARAMS, "1"},
                                      {DYNAMIC_STRATEGY, "low_rt"},
                                      {proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, "hnsw_compress"},
                                      {proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, "2000"},
                                      {proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT, "20000"},
                                      {proxima::PARAM_GRAPH_BUILDER_MEMORY_QUOTA, "400000"},
                                      {proxima::PARAM_GRAPH_COMMON_WITH_QUANTIZER, "true"},
                                      {proxima::PARAM_HNSW_BUILDER_EFCONSTRUCTION, "100000"},
                                      {proxima::PARAM_HNSW_BUILDER_MAX_LEVEL, "10"},
                                      {proxima::PARAM_HNSW_BUILDER_SCALING_FACTOR, "10"},
                                      {proxima::PARAM_HNSW_BUILDER_UPPER_NEIGHBOR_CNT, "30000"},
                                      {proxima::PARAM_HNSW_SEARCHER_EF, "100"},
                                      {proxima::PARAM_GRAPH_COMMON_COMBO_FILE, "0"}};

    aitheta::IndexParams params;
    bool ret = selector->InitKnnBuilderParams(parameters, params, true);
    EXPECT_EQ(1000, params.getInt64(proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM));
    EXPECT_EQ(1000, params.getInt64(proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT));
    EXPECT_EQ(400000, params.getInt64(proxima::PARAM_GRAPH_BUILDER_MEMORY_QUOTA));
    EXPECT_EQ(true, params.getBool(proxima::PARAM_GRAPH_COMMON_WITH_QUANTIZER));
    EXPECT_EQ(1000, params.getInt64(proxima::PARAM_HNSW_BUILDER_EFCONSTRUCTION));
    EXPECT_EQ(10, params.getInt64(proxima::PARAM_HNSW_BUILDER_MAX_LEVEL));
    EXPECT_EQ(10, params.getInt64(proxima::PARAM_HNSW_BUILDER_SCALING_FACTOR));
    EXPECT_EQ(1000, params.getInt64(proxima::PARAM_HNSW_BUILDER_UPPER_NEIGHBOR_CNT));
    EXPECT_EQ(100, params.getInt64(proxima::PARAM_HNSW_SEARCHER_EF));
    EXPECT_EQ(true, params.getBool(proxima::PARAM_GRAPH_COMMON_COMBO_FILE));
    EXPECT_EQ("hnsw_compress", params.getString(proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE));
}

TEST_F(KnnParamsSelectorTest, testGraphSearcherParamsNotUseDynamic) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_GRAPH, "empty_config.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {{DIMENSION, "72"},
                                      {INDEX_TYPE, INDEX_TYPE_GRAPH},
                                      {BUILD_METRIC_TYPE, L2},
                                      {SEARCH_METRIC_TYPE, L2},
                                      {DOCUMENT_NUM, "1000"},
                                      {USE_DYNAMIC_PARAMS, "1"},
                                      {DYNAMIC_STRATEGY, "low_rt"},
                                      {proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, "hnsw_compress"},
                                      {proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, "2000"},
                                      {proxima::PARAM_HNSW_SEARCHER_EF, "100"},
                                      {proxima::PARAM_GRAPH_COMMON_COMBO_FILE, "false"}};

    aitheta::IndexParams params;
    bool ret = selector->InitKnnSearcherParams(parameters, params, true);
    EXPECT_EQ(1000, params.getInt64(proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM));
    EXPECT_EQ(100, params.getInt64(proxima::PARAM_HNSW_SEARCHER_EF));
    EXPECT_EQ(false, params.getBool(proxima::PARAM_GRAPH_COMMON_COMBO_FILE));
    EXPECT_EQ("hnsw_compress", params.getString(proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE));
}

TEST_F(KnnParamsSelectorTest, testMipsLR) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_LR, "empty_config.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {{DIMENSION, "72"},
                                      {INDEX_TYPE, INDEX_TYPE_LR},
                                      {BUILD_METRIC_TYPE, L2},
                                      {SEARCH_METRIC_TYPE, INNER_PRODUCT},
									  {MIPS_ENABLE, "true"}};

    EXPECT_FALSE(selector->EnableMipsParams(parameters));
    MipsParams mipsParams;
    bool ret = selector->InitMipsParams(parameters, mipsParams);
    EXPECT_FALSE(mipsParams.enable);
}

TEST_F(KnnParamsSelectorTest, testMipsGRAPH) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_GRAPH, "knn_hc_strategy.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = { { DIMENSION, "72" },
                                       { INDEX_TYPE, INDEX_TYPE_GRAPH },
                                       { BUILD_METRIC_TYPE, L2 },
                                       { SEARCH_METRIC_TYPE, INNER_PRODUCT },
                                       { MIPS_ENABLE, "true" },
                                       { DOCUMENT_NUM, "1000" },
                                       { USE_DYNAMIC_PARAMS, "1" },
                                       { DYNAMIC_STRATEGY, "low_rt" },
                                       { proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, "hnsw_compress" },
                                       { proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, "2000" },
                                       { proxima::PARAM_HNSW_SEARCHER_EF, "100" },
                                       { proxima::PARAM_GRAPH_COMMON_COMBO_FILE, "false" } };

    EXPECT_TRUE(selector->EnableMipsParams(parameters));
    MipsParams mipsParams;
    bool ret = selector->InitMipsParams(parameters, mipsParams);
    EXPECT_TRUE(mipsParams.enable);
    EXPECT_EQ(4U, mipsParams.mval);
    EXPECT_FLOAT_EQ(0.381966F, mipsParams.uval);
    EXPECT_EQ(0.0F, mipsParams.norm);
}

TEST_F(KnnParamsSelectorTest, testMipsGRAPH2) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_GRAPH, "knn_hc_strategy.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = { { DIMENSION, "72" },
                                       { INDEX_TYPE, INDEX_TYPE_GRAPH },
                                       { BUILD_METRIC_TYPE, L2 },
                                       { SEARCH_METRIC_TYPE, INNER_PRODUCT },
                                       { MIPS_ENABLE, "true" },
                                       { MIPS_PARAM_MVAL, "8" },
                                       { MIPS_PARAM_UVAL, "0.2" },
                                       { MIPS_PARAM_NORM, "1" },
                                       { DOCUMENT_NUM, "1000" },
                                       { USE_DYNAMIC_PARAMS, "1" },
                                       { DYNAMIC_STRATEGY, "low_rt" },
                                       { proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, "hnsw_compress" },
                                       { proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, "2000" },
                                       { proxima::PARAM_HNSW_SEARCHER_EF, "100" },
                                       { proxima::PARAM_GRAPH_COMMON_COMBO_FILE, "false" } };

    EXPECT_TRUE(selector->EnableMipsParams(parameters));
    MipsParams mipsParams;
    bool ret = selector->InitMipsParams(parameters, mipsParams);
    EXPECT_TRUE(mipsParams.enable);
    EXPECT_EQ(8U, mipsParams.mval);
    EXPECT_EQ(0.2F, mipsParams.uval);
    EXPECT_EQ(1.0F, mipsParams.norm);
}

TEST_F(KnnParamsSelectorTest, testMipsL2) {
    IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_GRAPH, "knn_hc_strategy.json");
    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = { { DIMENSION, "72" },
                                       { INDEX_TYPE, INDEX_TYPE_GRAPH },
                                       { BUILD_METRIC_TYPE, L2 },
                                       { SEARCH_METRIC_TYPE, L2 },
                                       { MIPS_ENABLE, "true" },
                                       { MIPS_PARAM_MVAL, "8" },
                                       { MIPS_PARAM_UVAL, "0.2" },
                                       { MIPS_PARAM_NORM, "1" },
                                       { DOCUMENT_NUM, "1000" },
                                       { USE_DYNAMIC_PARAMS, "1" },
                                       { DYNAMIC_STRATEGY, "low_rt" },
                                       { proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, "hnsw_compress" },
                                       { proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, "2000" },
                                       { proxima::PARAM_HNSW_SEARCHER_EF, "100" },
                                       { proxima::PARAM_GRAPH_COMMON_COMBO_FILE, "false" } };

    EXPECT_FALSE(selector->EnableMipsParams(parameters));
    MipsParams mipsParams;
    bool ret = selector->InitMipsParams(parameters, mipsParams);
    EXPECT_FALSE(mipsParams.enable);
}

TEST_F(KnnParamsSelectorTest, testInMeta) {
    {
        IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_LR, "knn_hc_strategy.json");
        ASSERT_TRUE(selector != nullptr);
        util::KeyValueMap parameters = {
            {DIMENSION, "72"}, {INDEX_TYPE, INDEX_TYPE_LR}, {BUILD_METRIC_TYPE, INNER_PRODUCT}, {MIPS_ENABLE, "true"}};
        IndexMeta meta;
        EXPECT_TRUE(selector->InitMeta(parameters, meta));
        EXPECT_EQ(meta.method(), IndexDistance::kMethodFloatInnerProduct);
    }
    {
        IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_LR, "knn_hc_strategy.json");
        ASSERT_TRUE(selector != nullptr);
        util::KeyValueMap parameters = {
            {DIMENSION, "72"}, {INDEX_TYPE, INDEX_TYPE_LR}, {SEARCH_METRIC_TYPE, INNER_PRODUCT}, {MIPS_ENABLE, "true"}};
        IndexMeta meta;
        EXPECT_TRUE(selector->InitMeta(parameters, meta));
        EXPECT_EQ(meta.method(), IndexDistance::kMethodFloatInnerProduct);
    }
    {
        IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_GRAPH, "knn_hc_strategy.json");
        ASSERT_TRUE(selector != nullptr);
        util::KeyValueMap parameters = {{DIMENSION, "72"},
                                          {INDEX_TYPE, INDEX_TYPE_GRAPH},
                                          {BUILD_METRIC_TYPE, L2},
                                          {SEARCH_METRIC_TYPE, INNER_PRODUCT},
                                          {MIPS_ENABLE, "true"},
                                          {MIPS_PARAM_MVAL, "8"},
                                          {MIPS_PARAM_UVAL, "0.2"},
                                          {MIPS_PARAM_NORM, "1"},
                                          {DOCUMENT_NUM, "1000"},
                                          {USE_DYNAMIC_PARAMS, "1"},
                                          {DYNAMIC_STRATEGY, "low_rt"},
                                          {proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, "hnsw_compress"},
                                          {proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, "2000"},
                                          {proxima::PARAM_HNSW_SEARCHER_EF, "100"},
                                          {proxima::PARAM_GRAPH_COMMON_COMBO_FILE, "false"}};
        IndexMeta meta;
        EXPECT_TRUE(selector->InitMeta(parameters, meta));
        EXPECT_EQ(meta.method(), IndexDistance::kMethodFloatSquaredEuclidean);
    }
    {
        IKnnParamsSelectorPtr selector = MakeSelector(INDEX_TYPE_GRAPH, "knn_hc_strategy.json");
        ASSERT_TRUE(selector != nullptr);
        util::KeyValueMap parameters = {{DIMENSION, "72"},
                                          {INDEX_TYPE, INDEX_TYPE_GRAPH},
                                          {BUILD_METRIC_TYPE, L2},
                                          {SEARCH_METRIC_TYPE, L2},
                                          {DOCUMENT_NUM, "1000"},
                                          {USE_DYNAMIC_PARAMS, "1"},
                                          {DYNAMIC_STRATEGY, "low_rt"},
                                          {proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, "hnsw_compress"},
                                          {proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, "2000"},
                                          {proxima::PARAM_HNSW_SEARCHER_EF, "100"},
                                          {proxima::PARAM_GRAPH_COMMON_COMBO_FILE, "false"}};
        IndexMeta meta;
        EXPECT_TRUE(selector->InitMeta(parameters, meta));
        EXPECT_EQ(meta.method(), IndexDistance::kMethodFloatSquaredEuclidean);
    }
}

TEST_F(KnnParamsSelectorTest, testHCSearcherParamsUseParamsDynamicFromStr) {
    IKnnParamsSelectorPtr selector = MakeSelectorFromStr(INDEX_TYPE_HC, kDefaultKnnConfig);

    ASSERT_TRUE(selector != nullptr);
    util::KeyValueMap parameters = {
        {DIMENSION, "72"},
        {INDEX_TYPE, INDEX_TYPE_HC},
        {BUILD_METRIC_TYPE, INNER_PRODUCT},
        {SEARCH_METRIC_TYPE, INNER_PRODUCT},
        {DOCUMENT_NUM, "200001"},
        {USE_DYNAMIC_PARAMS, "1"},
        {proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "1", "30"},
        {proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "2", "100"},
        {proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM, "100000"},
    };
    aitheta::IndexParams params;
    bool ret = selector->InitKnnSearcherParams(parameters, params, true);

    EXPECT_EQ(16, params.getInt64(proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE));
    EXPECT_EQ(100, params.getInt64(proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "1"));
    EXPECT_EQ(4000, params.getInt64(proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "2"));
    EXPECT_EQ(25000, params.getInt64(proxima::PARAM_HC_SEARCHER_MAX_SCAN_NUM));
    EXPECT_EQ(IndexDistance::kMethodFloatInnerProduct, params.getInt64(proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD));
    EXPECT_EQ(true, params.getBool(proxima::PARAM_HC_COMMON_COMBO_FILE));
}

IE_NAMESPACE_END(aitheta_plugin);
