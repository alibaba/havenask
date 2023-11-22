#include "indexlib/partition/segment/test/compress_ratio_calculator_unittest.h"

#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/partition_state_machine.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, CompressRatioCalculatorTest);

CompressRatioCalculatorTest::CompressRatioCalculatorTest() {}

CompressRatioCalculatorTest::~CompressRatioCalculatorTest() {}

void CompressRatioCalculatorTest::CaseSetUp() {}

void CompressRatioCalculatorTest::CaseTearDown() {}

void CompressRatioCalculatorTest::TestGetRatioForKVTable()
{
    // not compress
    {
        // no shard
        IndexPartitionSchemaPtr schema = PrepareData(tt_kv, 1, false);
        PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM(), schema);
        CompressRatioCalculator cal;
        cal.Init(partitionData, schema);
        ASSERT_TRUE(cal.GetCompressRatio(index::KV_VALUE_FILE_NAME) == 1.0);
    }

    {
        // shard count = 8
        IndexPartitionSchemaPtr schema = PrepareData(tt_kv, 8, false);
        PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM(), schema);
        CompressRatioCalculator cal;
        cal.Init(partitionData, schema);
        ASSERT_TRUE(cal.GetCompressRatio(index::KV_VALUE_FILE_NAME) == 1.0);
    }

    // compress
    {
        // no shard
        IndexPartitionSchemaPtr schema = PrepareData(tt_kv, 1, true);
        RESET_FILE_SYSTEM();
        PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM(), schema);
        CompressRatioCalculator cal;
        cal.Init(partitionData, schema);
        ASSERT_NE(cal.GetCompressRatio(index::KV_VALUE_FILE_NAME), 1.0);
    }

    {
        // shard count = 8
        IndexPartitionSchemaPtr schema = PrepareData(tt_kv, 8, true);
        RESET_FILE_SYSTEM();
        PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM(), schema);
        CompressRatioCalculator cal;
        cal.Init(partitionData, schema);
        ASSERT_NE(cal.GetCompressRatio(index::KV_VALUE_FILE_NAME), 1.0);
    }
}

void CompressRatioCalculatorTest::TestGetRatioForKKVTable()
{
    // no compress
    {
        // no shard
        IndexPartitionSchemaPtr schema = PrepareData(tt_kkv, 1, false);
        PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM(), schema);
        CompressRatioCalculator cal;
        cal.Init(partitionData, schema);
        ASSERT_TRUE(cal.GetCompressRatio(index::SUFFIX_KEY_FILE_NAME) == 1.0);
        ASSERT_TRUE(cal.GetCompressRatio(index::KKV_VALUE_FILE_NAME) == 1.0);
    }

    {
        // shard count = 8
        IndexPartitionSchemaPtr schema = PrepareData(tt_kkv, 8, false);
        PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM(), schema);
        CompressRatioCalculator cal;
        cal.Init(partitionData, schema);
        ASSERT_TRUE(cal.GetCompressRatio(index::SUFFIX_KEY_FILE_NAME) == 1.0);
        ASSERT_TRUE(cal.GetCompressRatio(index::KKV_VALUE_FILE_NAME) == 1.0);
    }

    // compress
    {
        // no shard
        IndexPartitionSchemaPtr schema = PrepareData(tt_kkv, 1, true);
        RESET_FILE_SYSTEM();
        PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM(), schema);
        CompressRatioCalculator cal;
        cal.Init(partitionData, schema);
        ASSERT_NE(cal.GetCompressRatio(index::SUFFIX_KEY_FILE_NAME), 1.0);
        ASSERT_NE(cal.GetCompressRatio(index::KKV_VALUE_FILE_NAME), 1.0);
    }

    {
        // shard count = 8
        IndexPartitionSchemaPtr schema = PrepareData(tt_kkv, 8, true);
        RESET_FILE_SYSTEM();
        PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM(), schema);
        CompressRatioCalculator cal;
        cal.Init(partitionData, schema);
        ASSERT_NE(cal.GetCompressRatio(index::SUFFIX_KEY_FILE_NAME), 1.0);
        ASSERT_NE(cal.GetCompressRatio(index::KKV_VALUE_FILE_NAME), 1.0);
    }
}

IndexPartitionSchemaPtr CompressRatioCalculatorTest::PrepareData(TableType tableType, size_t shardingCount,
                                                                 bool compress)
{
    tearDown();
    setUp();

    IndexPartitionSchemaPtr schema;
    string field = "pkey:string;skey:int32;value:uint32;";
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey2,skey=3,value=3,ts=102;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102;"
                       "cmd=add,pkey=pkey2,skey=4,value=4,ts=102;"
                       "cmd=add,pkey=pkey3,skey=5,value=5,ts=102;"
                       "cmd=delete,pkey=pkey4,ts=102;"
                       "cmd=add,pkey=pkey3,skey=6,value=6,ts=102;"
                       "cmd=delete,pkey=pkey5,ts=102;"
                       "cmd=add,pkey=pkey6,skey=7,value=7,ts=102;"
                       "cmd=add,pkey=pkey6,skey=8,value=8,ts=102;"
                       "cmd=add,pkey=pkey6,skey=9,value=9,ts=102;";

    IndexPartitionOptions options;
    options.GetBuildConfig(true).maxDocCount = 2;
    options.GetBuildConfig(false).maxDocCount = 2;
    options.GetBuildConfig(true).buildTotalMemory = 22;
    options.GetBuildConfig(false).buildTotalMemory = 40;
    options.GetBuildConfig(false).shardingColumnNum = shardingCount;
    options.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    options.GetBuildConfig(false).levelNum = 2;

    if (tableType == tt_kv) {
        schema = SchemaMaker::MakeKVSchema(field, "pkey", "skey;value");
        if (compress) {
            KVIndexConfigPtr kvConfig =
                DYNAMIC_POINTER_CAST(KVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
            KVIndexPreference::ValueParam valueParam(false, "zstd");
            kvConfig->GetIndexPreference().SetValueParam(valueParam);
        }
    } else if (tableType == tt_kkv) {
        schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
        if (compress) {
            KKVIndexConfigPtr kkvConfig =
                DYNAMIC_POINTER_CAST(KKVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
            KKVIndexPreference::SuffixKeyParam suffixParam(false, "zstd");
            KKVIndexPreference::ValueParam valueParam(false, "zstd");
            kkvConfig->GetIndexPreference().SetSkeyParam(suffixParam);
            kkvConfig->GetIndexPreference().SetValueParam(valueParam);
        }
    }
    assert(schema);
    PartitionStateMachine psm;
    psm.Init(schema, options, GET_TEMP_DATA_PATH());
    psm.Transfer(BUILD_FULL, docString, "", "");
    return schema;
}
}} // namespace indexlib::partition
