#include "indexlib/partition/segment/test/kv_segment_writer_unittest.h"

#include "future_lite/CoroInterface.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/document_rewriter/pack_attribute_rewriter.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/kv/hash_table_fix_segment_reader.h"
#include "indexlib/index/kv/kv_read_options.h"
#include "indexlib/index/kv/kv_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/PrimeNumberTable.h"

using namespace std;
using namespace autil;

using namespace indexlib::index;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(segment, KVSegmentWriterTest);

KVSegmentWriterTest::KVSegmentWriterTest() {}

KVSegmentWriterTest::~KVSegmentWriterTest() {}

void KVSegmentWriterTest::CaseSetUp()
{
    string fields = "single_int8:int8;key:uint64;";
    mSchema = SchemaMaker::MakeKVSchema(fields, "key", "single_int8");
    mSchema->SetEnableTTL(true);
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    mKvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    mKvConfig->SetTTL(10);
    mOptions.GetBuildConfig(true).buildTotalMemory = 22;
    mOptions.GetBuildConfig(false).buildTotalMemory = 40;
    mFormatter.Init(mKvConfig->GetValueConfig()->CreatePackAttributeConfig());
    mQuotaControl.reset(new util::QuotaControl(1024 * 1024 * 1024));
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void KVSegmentWriterTest::CaseTearDown() {}

void KVSegmentWriterTest::BuildDocs(KVSegmentWriter& writer, const string& docStr)
{
    auto docs = test::DocumentCreator::CreateKVDocuments(mSchema, docStr);
    for (size_t i = 0; i < docs.size(); i++) {
        writer.AddDocument(docs[i]);
    }
}

void KVSegmentWriterTest::TestUserTimestamp()
{
    string docStr = "cmd=add,single_int8=-8,key=5,ts=1000000,ha_reserved_timestamp=2000000;";

    IndexPartitionOptions options;
    options.GetBuildConfig(false).useUserTimestamp = true;
    options.GetBuildConfig(false).enablePackageFile = false;
    KVSegmentWriter segWriter(mSchema, options);
    BuildingSegmentData segmentData(options.GetBuildConfig());
    segmentData.SetSegmentId(0);
    segmentData.SetSegmentDirName("test_segment");
    MAKE_SEGMENT_DIRECTORY(0);
    segmentData.Init(GET_SEGMENT_DIRECTORY(), false);
    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    segWriter.Init(segmentData, metrics, mQuotaControl);
    BuildDocs(segWriter, docStr);
    HashTableFixSegmentReader reader;
    reader.Open(mKvConfig, segmentData);
    autil::StringView value;
    uint64_t ts;
    bool isDeleted;
    keytype_t key = (keytype_t)5;

    KVIndexOptions indexOptions;
    indexOptions.ttl = mKvConfig->GetTTL();
    indexOptions.fixedValueLen = mKvConfig->GetValueConfig()->GetFixedLength();
    ASSERT_TRUE(future_lite::interface::syncAwait(reader.Get(&indexOptions, key, value, ts, isDeleted)));
    ASSERT_EQ(2, ts);
    // std::cout << "value: " << (int)*value.data() << std::endl;
}

void KVSegmentWriterTest::TestGetTotalMemoryUse()
{
    string fields = "multi_int8:int8:true;key:uint64;";
    mSchema = SchemaMaker::MakeKVSchema(fields, "key", "multi_int8");

    string docStr = "cmd=add,multi_int8=-8 10,key=5,ts=1000000,ha_reserved_timestamp=2000000;";

    IndexPartitionOptions options;
    options.GetBuildConfig(false).useUserTimestamp = true;
    options.GetBuildConfig(false).enablePackageFile = false;
    KVSegmentWriter segWriter(mSchema, options);
    BuildingSegmentData segmentData(options.GetBuildConfig());
    segmentData.SetSegmentId(0);
    segmentData.SetSegmentDirName("test_segment");
    MAKE_SEGMENT_DIRECTORY(0);
    segmentData.Init(GET_SEGMENT_DIRECTORY(), false);
    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    segWriter.Init(segmentData, metrics, mQuotaControl);

    ASSERT_EQ(0, segWriter.GetTotalMemoryUse());
    BuildDocs(segWriter, docStr);
    ASSERT_GT(segWriter.GetTotalMemoryUse(), 0);
}

void KVSegmentWriterTest::TestSwapMmapFile()
{
    string field = "key:uint64;uint32:uint32;mint8:int8:true;mstr:string:true";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "key;uint32;mint8;mstr");
    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_", "_KV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    mOptions.GetOnlineConfig().useSwapMmapFile = true;
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    config::KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    IndexFormatVersion indexFormatVersion(INDEX_FORMAT_VERSION);
    SchemaRewriter::RewriteForKV(schema, mOptions, indexFormatVersion);
    mOptions.SetIsOnline(true);
    PartitionStateMachine psm;
    auto spath = GET_TEMP_DATA_PATH();
    psm.Init(schema, mOptions, spath);
    ASSERT_TRUE(kvConfig->GetUseSwapMmapFile());
    string docString = "cmd=add,key=10,uint32=11,ts=101000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docString, "", ""));

    auto kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    autil::mem_pool::Pool pool;
    KVReadOptions kvOptions;
    KVMetricsCollector collector;
    kvOptions.metricsCollector = &collector;
    kvOptions.fieldName = "uint32";
    kvOptions.pool = &pool;

    uint32_t value;
    ASSERT_FALSE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("2"), value, kvOptions)));

    collector.Reset();
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("10"), value, kvOptions)));
    ASSERT_EQ(11, value);
}

void KVSegmentWriterTest::TestNeedDumpWhenNoSpace()
{
    string field = "key:uint64;uint32:uint32;mint8:int8:true;mstr:string:true";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "key;uint32;mint8;mstr");
    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_", "_KV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    mOptions.GetOnlineConfig().useSwapMmapFile = true;
    mOptions.GetOnlineConfig().maxSwapMmapFileSize = 1;
    IndexFormatVersion indexFormatVersion(INDEX_FORMAT_VERSION);
    SchemaRewriter::RewriteForKV(schema, mOptions, indexFormatVersion);
    mOptions.SetIsOnline(true);
    PartitionStateMachine psm;
    psm.Init(schema, mOptions, GET_TEMP_DATA_PATH());
    string longValue(800 * 1000, 'a');
    string docString = "cmd=add,key=10,uint32=11,mstr=" + longValue + ",ts=101000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    OnlinePartitionPtr onlinePartition = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    InMemorySegmentPtr inMemorySegment1 = onlinePartition->GetPartitionData()->GetInMemorySegment();
    segmentid_t id1 = inMemorySegment1->GetSegmentData().GetSegmentId();

    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    InMemorySegmentPtr inMemorySegment2 = onlinePartition->GetPartitionData()->GetInMemorySegment();
    segmentid_t id2 = inMemorySegment2->GetSegmentData().GetSegmentId();
    ASSERT_FALSE(id1 == id2);
    ASSERT_TRUE(id1 + 1 == id2);

    longValue += longValue;
    docString = "cmd=add,key=11,uint32=11,mstr=" + longValue + ",ts=101000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "11", ""));
}

void KVSegmentWriterTest::TestMmapFileDump()
{
    string field = "key:uint64;uint32:uint32;mint8:int8:true;mstr:string:true";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "key;uint32;mint8;mstr");
    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_", "_KV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    mOptions.GetOnlineConfig().useSwapMmapFile = true;
    mOptions.GetOnlineConfig().maxSwapMmapFileSize = 1;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    // mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    IndexFormatVersion indexFormatVersion(INDEX_FORMAT_VERSION);
    SchemaRewriter::RewriteForKV(schema, mOptions, indexFormatVersion);
    mOptions.SetIsOnline(true);
    PartitionStateMachine psm;
    psm.Init(schema, mOptions, GET_TEMP_DATA_PATH());
    string longValue(800 * 1000, 'a');
    string docString = "cmd=add,key=10,uint32=11,mstr=" + longValue + ",ts=101000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docString, "", ""));
    RESET_FILE_SYSTEM();
    string path = "rt_index_partition/segment_1073741824_level_0/index/key/value";
    ASSERT_TRUE(GET_PARTITION_DIRECTORY()->IsExist(path));

    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    assert(onlinePart);
    DirectoryPtr rootDir = onlinePart->GetRootDirectory();
    assert(rootDir);

    FileStat fileStat;
    ASSERT_EQ(FSEC_OK, rootDir->GetFileSystem()->TEST_GetFileStat(path, fileStat));
    ASSERT_EQ(FSFT_MMAP, fileStat.fileType);
}
}} // namespace indexlib::partition
