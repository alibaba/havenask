#include "indexlib/index/kv/test/hash_table_var_writer_unittest.h"

#include "indexlib/common/hash_table/special_value.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/hash_table_var_creator.h"
#include "indexlib/index/kv/hash_table_var_writer.h"
#include "indexlib/index/kv/kv_factory.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::test;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, HashTableVarWriterTest);

HashTableVarWriterTest::HashTableVarWriterTest() {}

HashTableVarWriterTest::~HashTableVarWriterTest() {}

void HashTableVarWriterTest::CaseSetUp() {}

void HashTableVarWriterTest::CaseTearDown() {}

void HashTableVarWriterTest::TestCalculateMemoryRatio()
{
    typedef HashTableVarWriter Writer;

    ASSERT_DOUBLE_EQ(0.01, Writer::CalculateMemoryRatio(std::shared_ptr<framework::SegmentGroupMetrics>(), 0));

    indexlib::framework::SegmentMetrics metrics;
    string groupName = "group";

    metrics.Set<size_t>(groupName, KV_HASH_MEM_USE, 0);
    metrics.Set<size_t>(groupName, KV_VALUE_MEM_USE, 0);
    metrics.Set<double>(groupName, KV_KEY_VALUE_MEM_RATIO, 0);
    std::shared_ptr<framework::SegmentGroupMetrics> groupMetrics = metrics.GetSegmentGroupMetrics(groupName);

    ASSERT_DOUBLE_EQ(0.01, Writer::CalculateMemoryRatio(groupMetrics, 0));

    metrics.Set<size_t>(groupName, KV_HASH_MEM_USE, 0);
    metrics.Set<size_t>(groupName, KV_VALUE_MEM_USE, 1);
    ASSERT_DOUBLE_EQ(0.0001, Writer::CalculateMemoryRatio(groupMetrics, 0));

    metrics.Set<size_t>(groupName, KV_HASH_MEM_USE, 100);
    metrics.Set<size_t>(groupName, KV_VALUE_MEM_USE, 0);
    metrics.Set<double>(groupName, KV_KEY_VALUE_MEM_RATIO, 1);
    ASSERT_DOUBLE_EQ(0.90, Writer::CalculateMemoryRatio(groupMetrics, 0));

    metrics.Set<size_t>(groupName, KV_HASH_MEM_USE, 8);
    metrics.Set<size_t>(groupName, KV_VALUE_MEM_USE, 192);
    metrics.Set<double>(groupName, KV_KEY_VALUE_MEM_RATIO, 0);
    ASSERT_DOUBLE_EQ(0.04 * 0.7, Writer::CalculateMemoryRatio(groupMetrics, 0));

    metrics.Set<size_t>(groupName, KV_HASH_MEM_USE, 8);
    metrics.Set<size_t>(groupName, KV_VALUE_MEM_USE, 192);
    metrics.Set<double>(groupName, KV_KEY_VALUE_MEM_RATIO, 1);
    ASSERT_DOUBLE_EQ(0.04 * 0.7 + 0.3, Writer::CalculateMemoryRatio(groupMetrics, 0));
}

void HashTableVarWriterTest::TestValueThresholdWithShortenOffset()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema("key:int64;value:int8:true", "key", "value");
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
    const KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
    const KVIndexPreference::HashDictParam& dictParam = kvIndexPreference.GetHashDictParam();

    std::shared_ptr<framework::SegmentGroupMetrics> groupMetrics;
    IndexPartitionOptions options;
    size_t maxMemoryUse = (size_t)8 * 1024 * 1024 * 1024; // 8GB

    {
        // test short offset
        auto kvWriter = KVFactory::CreateWriter(kvIndexConfig);
        kvWriter->Init(kvIndexConfig, GET_SEGMENT_DIRECTORY(), options.IsOnline(), maxMemoryUse, groupMetrics);

        auto varWriter = DYNAMIC_POINTER_CAST(HashTableVarWriter, kvWriter);
        ASSERT_TRUE(varWriter.get() != NULL);
        ASSERT_LT((int64_t)varWriter->mValueMemoryUseThreshold, DEFAULT_MAX_VALUE_SIZE_FOR_SHORT_OFFSET);
    }

    KVIndexPreference::HashDictParam newDictParam(dictParam.GetHashType(), dictParam.GetOccupancyPct(),
                                                  dictParam.UsePreciseCountWhenMerge(),
                                                  dictParam.HasEnableCompactHashKey(), false);
    kvIndexConfig->GetIndexPreference().SetHashDictParam(newDictParam);

    {
        auto kvWriter = KVFactory::CreateWriter(kvIndexConfig);
        kvWriter->Init(kvIndexConfig, GET_SEGMENT_DIRECTORY(), options.IsOnline(), maxMemoryUse, groupMetrics);

        auto varWriter = DYNAMIC_POINTER_CAST(HashTableVarWriter, kvWriter);
        ASSERT_TRUE(varWriter.get() != NULL);
        ASSERT_GT((int64_t)varWriter->mValueMemoryUseThreshold, DEFAULT_MAX_VALUE_SIZE_FOR_SHORT_OFFSET);
    }
}
}} // namespace indexlib::index
