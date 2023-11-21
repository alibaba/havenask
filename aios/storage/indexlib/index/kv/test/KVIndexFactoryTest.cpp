#include "indexlib/index/kv/KVIndexFactory.h"

#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/MemIndexerParameter.h"
#include "indexlib/index/kv/FixedLenKVMemIndexer.h"
#include "indexlib/index/kv/VarLenKVMemIndexer.h"
#include "indexlib/index/kv/test/KVIndexConfigBuilder.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::index {

class KVIndexFactoryTest : public TESTBASE
{
};

TEST_F(KVIndexFactoryTest, testCreateFixedLenMemIndexer)
{
    KVIndexFactory factory;
    auto [_, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("f1:long;f2:string", "f2", "f1");
    ASSERT_TRUE(indexConfig);
    MemIndexerParameter parameter;
    auto indexer = factory.CreateMemIndexer(indexConfig, parameter);
    ASSERT_FALSE(indexer);
    parameter.maxMemoryUseInBytes = 1024 * 1024;
    indexer = factory.CreateMemIndexer(indexConfig, parameter);
    ASSERT_TRUE(indexer);
    auto typedIndexer = std::dynamic_pointer_cast<FixedLenKVMemIndexer>(indexer);
    ASSERT_TRUE(typedIndexer);
    ASSERT_EQ(parameter.maxMemoryUseInBytes / 2, typedIndexer->_maxMemoryUse);
}

TEST_F(KVIndexFactoryTest, testCreateVarLenMemIndexer)
{
    KVIndexFactory factory;
    auto [_, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("f1:long;f2:string", "f2", "f1;f2");
    ASSERT_TRUE(indexConfig);
    MemIndexerParameter parameter;
    config::SortDescription sortDescription("f1", config::sp_desc);
    parameter.sortDescriptions.push_back(sortDescription);
    auto indexer = factory.CreateMemIndexer(indexConfig, parameter);
    ASSERT_FALSE(indexer);

    parameter.maxMemoryUseInBytes = 1024 * 1024;
    indexer = factory.CreateMemIndexer(indexConfig, parameter);
    ASSERT_TRUE(indexer);
    auto typedIndexer = std::dynamic_pointer_cast<VarLenKVMemIndexer>(indexer);
    ASSERT_TRUE(typedIndexer);
    EXPECT_EQ(parameter.maxMemoryUseInBytes / 2, typedIndexer->_maxMemoryUse);
    EXPECT_EQ(VarLenKVMemIndexer::DEFAULT_KEY_VALUE_MEM_RATIO, typedIndexer->_keyValueSizeRatio);
    EXPECT_EQ(VarLenKVMemIndexer::DEFAULT_VALUE_COMPRESSION_RATIO, typedIndexer->_valueCompressRatio);
}

TEST_F(KVIndexFactoryTest, testCreateVarLenMemIndexerWithLastSegmentMetrics)
{
    KVIndexFactory factory;
    auto [_, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("f1:long;f2:string", "f2", "f1;f2");
    ASSERT_TRUE(indexConfig);
    MemIndexerParameter parameter;
    config::SortDescription sortDescription("f1", config::sp_desc);
    parameter.sortDescriptions.push_back(sortDescription);
    parameter.maxMemoryUseInBytes = 1024 * 1024;

    {
        string segmentMetricsStr = R"({
        "segment_stat_metrics_@_0" : {
            "key_count" : 1000,
            "kv_hash_mem_use" : 100,
            "kv_hash_occupancy_pct" : 60,
            "kv_key_value_mem_ratio" : 0.1,
            "kv_segment_mem_use" : 1000,
            "kv_value_mem_use" : 900,
            "kv_writer_typeid" : "N8indexlib5index18HashTableVarWriterE"
        }})";
        indexlib::framework::SegmentMetrics segmentMetrics;
        segmentMetrics.FromString(segmentMetricsStr);
        parameter.lastSegmentMetrics = &segmentMetrics;
        auto indexer = factory.CreateMemIndexer(indexConfig, parameter);
        ASSERT_TRUE(indexer);
        auto typedIndexer = std::dynamic_pointer_cast<VarLenKVMemIndexer>(indexer);
        ASSERT_TRUE(typedIndexer);
        EXPECT_EQ(parameter.maxMemoryUseInBytes / 2, typedIndexer->_maxMemoryUse);
        EXPECT_FLOAT_EQ(0.1, typedIndexer->_keyValueSizeRatio);
        EXPECT_EQ(VarLenKVMemIndexer::DEFAULT_VALUE_COMPRESSION_RATIO, typedIndexer->_valueCompressRatio);
    }
    {
        string segmentMetricsStr = R"({
        "segment_stat_metrics_@_0" : {
            "key_count" : 1000,
            "kv_hash_mem_use" : 100,
            "kv_hash_occupancy_pct" : 60,
            "kv_key_value_mem_ratio" : 0.0001,
            "kv_segment_mem_use" : 1000,
            "kv_value_mem_use" : 900,
            "kv_writer_typeid" : "N8indexlib5index18HashTableVarWriterE"
        }})";
        indexlib::framework::SegmentMetrics segmentMetrics;
        segmentMetrics.FromString(segmentMetricsStr);
        parameter.lastSegmentMetrics = &segmentMetrics;
        auto indexer = factory.CreateMemIndexer(indexConfig, parameter);
        ASSERT_TRUE(indexer);
        auto typedIndexer = std::dynamic_pointer_cast<VarLenKVMemIndexer>(indexer);
        ASSERT_TRUE(typedIndexer);
        EXPECT_EQ(parameter.maxMemoryUseInBytes / 2, typedIndexer->_maxMemoryUse);
        EXPECT_FLOAT_EQ(VarLenKVMemIndexer::MIN_KEY_VALUE_MEM_RATIO, typedIndexer->_keyValueSizeRatio);
        EXPECT_EQ(VarLenKVMemIndexer::DEFAULT_VALUE_COMPRESSION_RATIO, typedIndexer->_valueCompressRatio);
    }
    {
        string segmentMetricsStr = R"({
        "segment_stat_metrics_@_0" : {
            "key_count" : 1000,
            "kv_hash_mem_use" : 100,
            "kv_hash_occupancy_pct" : 60,
            "kv_key_value_mem_ratio" : 1.0,
            "kv_segment_mem_use" : 1000,
            "kv_value_mem_use" : 900,
            "kv_writer_typeid" : "N8indexlib5index18HashTableVarWriterE"
        }})";
        indexlib::framework::SegmentMetrics segmentMetrics;
        segmentMetrics.FromString(segmentMetricsStr);
        parameter.lastSegmentMetrics = &segmentMetrics;
        auto indexer = factory.CreateMemIndexer(indexConfig, parameter);
        ASSERT_TRUE(indexer);
        auto typedIndexer = std::dynamic_pointer_cast<VarLenKVMemIndexer>(indexer);
        ASSERT_TRUE(typedIndexer);
        EXPECT_EQ(parameter.maxMemoryUseInBytes / 2, typedIndexer->_maxMemoryUse);
        EXPECT_FLOAT_EQ(VarLenKVMemIndexer::MAX_KEY_VALUE_MEM_RATIO, typedIndexer->_keyValueSizeRatio);
        EXPECT_EQ(VarLenKVMemIndexer::DEFAULT_VALUE_COMPRESSION_RATIO, typedIndexer->_valueCompressRatio);
    }
}

} // namespace indexlibv2::index
