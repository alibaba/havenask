#include "indexlib/index/kv/test/kv_merger_unittest.h"

#include "indexlib/config/test/region_schema_maker.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/kv_merger.h"
#include "indexlib/index_base/index_meta/segment_metrics_util.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/partition_state_machine.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KvMergerTest);

KvMergerTest::KvMergerTest() {}

KvMergerTest::~KvMergerTest() {}

void KvMergerTest::CaseSetUp() {}

void KvMergerTest::CaseTearDown() {}

void KvMergerTest::TestInit()
{
    {
        string field = "key1:string;key2:int32;value:int64;";
        // multi region with ttl
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value", "");
        maker.AddRegion("region2", "key2", "value", "", 10);
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        KVIndexConfigPtr kvConfig = kvMerger.mKVIndexConfig;
        ASSERT_TRUE(kvConfig->TTLEnabled());
        config::KVIndexPreference::HashDictParam param = kvConfig->GetIndexPreference().GetHashDictParam();
        ASSERT_FALSE(param.HasEnableCompactHashKey());
        ASSERT_FALSE(param.HasEnableShortenOffset());
    }
    {
        string field = "key1:string;key2:int32;value:int64;";
        // multi region without ttl
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value", "");
        maker.AddRegion("region2", "key2", "value", "");
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        KVIndexConfigPtr kvConfig = kvMerger.mKVIndexConfig;
        ASSERT_FALSE(kvConfig->TTLEnabled());
        config::KVIndexPreference::HashDictParam param = kvConfig->GetIndexPreference().GetHashDictParam();
        ASSERT_FALSE(param.HasEnableCompactHashKey());
        ASSERT_FALSE(param.HasEnableShortenOffset());
    }
    {
        // single region with ttl
        string field = "key1:int32;value:int64;";
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value", "", 66);
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        KVIndexConfigPtr kvConfig = kvMerger.mKVIndexConfig;
        ASSERT_TRUE(kvConfig->TTLEnabled());
    }
    {
        // single region without ttl
        string field = "key1:string;value:int64;";
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value", "");
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        KVIndexConfigPtr kvConfig = kvMerger.mKVIndexConfig;
        ASSERT_FALSE(kvConfig->TTLEnabled());
    }
}

void KvMergerTest::TestNeedCompactBucket()
{
    SegmentTopologyInfo info;
    info.isBottomLevel = true;
    info.columnIdx = 0;
    indexlib::framework::SegmentMetricsVec metricsVec;
    string groupName = index_base::SegmentMetricsUtil::GetColumnGroupName(info.columnIdx);

    for (int i = 0; i < 5; i++) {
        std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics());
        metrics->Set<size_t>(groupName, KV_KEY_DELETE_COUNT, 0);
        metricsVec.push_back(metrics);
    }
    {
        // old version, mEnableCompactPackAttribute = false
        string field = "key1:int64;value:int8;";
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value", "");
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        RewriteConfig(kvMerger.mKVIndexConfig, true, -1, true);
        ASSERT_FALSE(kvMerger.NeedCompactBucket(kvMerger.mKVIndexConfig, metricsVec, info));
    }
    {
        // enable ttl
        string field = "key1:int64;value:int8;";
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value", "");
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        KVIndexConfigPtr kvConfig = kvMerger.mKVIndexConfig;
        RewriteConfig(kvMerger.mKVIndexConfig, false, 10, true);
        ASSERT_FALSE(kvMerger.NeedCompactBucket(kvConfig, metricsVec, info));
    }
    {
        // useCompactBucket is false
        string field = "key1:int64;value:int8;";
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value", "");
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        KVIndexConfigPtr kvConfig = kvMerger.mKVIndexConfig;
        RewriteConfig(kvMerger.mKVIndexConfig, false, -1, false);
    }
    {
        // value length > 8
        string field = "key1:int64;value:int64;value1:int32";
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value;value1", "");
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        KVIndexConfigPtr kvConfig = kvMerger.mKVIndexConfig;
        RewriteConfig(kvMerger.mKVIndexConfig, false, -1, true);
        ASSERT_FALSE(kvMerger.NeedCompactBucket(kvConfig, metricsVec, info));
    }
    {
        // value length == -1
        string field = "key1:int64;value:string";
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value", "");
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        KVIndexConfigPtr kvConfig = kvMerger.mKVIndexConfig;
        RewriteConfig(kvMerger.mKVIndexConfig, false, -1, true);
        ASSERT_FALSE(kvMerger.NeedCompactBucket(kvConfig, metricsVec, info));
    }
    {
        // key length <= value length
        string field = "key1:int16;value:int32";
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value", "");
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        KVIndexConfigPtr kvConfig = kvMerger.mKVIndexConfig;
        RewriteConfig(kvMerger.mKVIndexConfig, false, -1, true);
        ASSERT_FALSE(kvMerger.NeedCompactBucket(kvConfig, metricsVec, info));
    }
    {
        // merge is bottom level
        string field = "key1:int64;value:int32";
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value", "");
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        KVIndexConfigPtr kvConfig = kvMerger.mKVIndexConfig;
        RewriteConfig(kvMerger.mKVIndexConfig, false, -1, true);
        ASSERT_TRUE(kvMerger.NeedCompactBucket(kvConfig, metricsVec, info));
    }
    {
        // merge is not bottom level, but delete count sum = 0
        info.isBottomLevel = false;
        string field = "key1:int64;value:int32";
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value", "");
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        KVIndexConfigPtr kvConfig = kvMerger.mKVIndexConfig;
        RewriteConfig(kvMerger.mKVIndexConfig, false, -1, true);
        ASSERT_TRUE(kvMerger.NeedCompactBucket(kvConfig, metricsVec, info));

        info.isBottomLevel = true;
    }
    {
        // merge is not bottom level, but delete count sum != 0
        info.isBottomLevel = false;
        metricsVec[0]->Set<size_t>(groupName, KV_KEY_DELETE_COUNT, 1);
        string field = "key1:int64;value:int32";
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value", "");
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        KVIndexConfigPtr kvConfig = kvMerger.mKVIndexConfig;
        RewriteConfig(kvMerger.mKVIndexConfig, false, -1, true);
        ASSERT_FALSE(kvMerger.NeedCompactBucket(kvConfig, metricsVec, info));

        info.isBottomLevel = true;
        metricsVec[0]->Set<size_t>(groupName, KV_KEY_DELETE_COUNT, 0);
    }
    {
        // merge is not bottom level, has old segment whitch segment metrics do not have delete count
        info.isBottomLevel = false;
        std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics());
        metricsVec.push_back(metrics);

        string field = "key1:int64;value:int32";
        RegionSchemaMaker maker(field, "kv");
        maker.AddRegion("region1", "key1", "value", "");
        IndexPartitionSchemaPtr schema = maker.GetSchema();
        index_base::PartitionDataPtr partData;
        KVMerger kvMerger;
        kvMerger.Init(partData, schema, 100);
        KVIndexConfigPtr kvConfig = kvMerger.mKVIndexConfig;
        RewriteConfig(kvMerger.mKVIndexConfig, false, -1, true);
        ASSERT_FALSE(kvMerger.NeedCompactBucket(kvConfig, metricsVec, info));

        info.isBottomLevel = true;
        metricsVec.pop_back();
    }
}

void KvMergerTest::RewriteConfig(KVIndexConfigPtr kvConfig, bool isLegacy, int32_t ttl, bool useCompactValue)
{
    kvConfig->GetValueConfig()->EnableCompactFormat(!isLegacy);
    if (ttl != -1) {
        kvConfig->SetTTL(ttl);
    }
    if (useCompactValue) {
        KVIndexPreference& kvIndexPreference = kvConfig->GetIndexPreference();
        KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
        valueParam.EnableFixLenAutoInline();
        kvIndexPreference.SetValueParam(valueParam);
    }
}
}} // namespace indexlib::index
