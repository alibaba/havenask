#include "indexlib/index/kv/test/kv_reader_unittest.h"

#include "future_lite/CoroInterface.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/test/region_schema_maker.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/kv/hash_table_fix_segment_reader.h"
#include "indexlib/index/kv/kv_common.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/index/kv/kv_reader_impl.h"
#include "indexlib/index/kv/kv_segment_reader.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_state_machine.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace future_lite::interface;

using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::common;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KVReaderTest);

namespace {
class FakeSegmentReader final : public KVSegmentReaderImplBase
{
public:
    FakeSegmentReader() : mKey(0), mValueTs(0), mIsDeleted(true) {}
    void Open(const KVIndexConfigPtr& kvIndexConfig, const index_base::SegmentData& segData) override {}

    FL_LAZY(bool)
    Get(const KVIndexOptions* options, keytype_t key, autil::StringView& value, uint64_t& ts, bool& isDeleted,
        autil::mem_pool::Pool* pool, KVMetricsCollector* collector = nullptr) const override
    {
        if (key != mKey) {
            FL_CORETURN false;
        }
        value = mValue;
        ts = mValueTs;
        isDeleted = mIsDeleted;
        FL_CORETURN true;
    }

    void SetKeyValue(const std::string& key, const std::string& value, const std::string& isDeleted,
                     const std::string& valueTs)
    {
        mKey = autil::StringUtil::numberFromString<uint64_t>(key);
        mStrValue = value;
        mValue = autil::StringView(mStrValue.c_str(), mStrValue.size());
        if (isDeleted == "true") {
            mIsDeleted = true;
        } else {
            mIsDeleted = false;
        }
        mValueTs = autil::StringUtil::numberFromString<uint64_t>(valueTs);
    }
    bool doCollectAllCode() override { return true; }

private:
    uint64_t mKey;
    uint64_t mValueTs;
    bool mIsDeleted;
    autil::StringView mValue;
    std::string mStrValue;
};

}; // namespace

KVReaderTest::KVReaderTest() {}

KVReaderTest::~KVReaderTest() {}

void KVReaderTest::CaseSetUp()
{
    mOptions.GetBuildConfig(true).buildTotalMemory = 22;
    mOptions.GetBuildConfig(false).buildTotalMemory = 40;
    setenv("DISABLE_CODEGEN", "true", 1);
}

void KVReaderTest::CaseTearDown() { unsetenv("DISABLE_CODEGEN"); }

void KVReaderTest::TestBlockCache()
{
    string field = "key:uint64;uint32:uint32;mint8:int8:true;mstr:string:true";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "key;uint32;mint8;mstr");
    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_", "_KV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key=10,uint32=10,mint8=1 2,mstr=a cd,ts=101000000;"
                       "cmd=add,key=0,uint32=0,mint8=,mstr=,ts=102000000;"
                       "cmd=add,key=5,uint32=5,mint8=3,mstr=a,ts=102000000";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    auto kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    autil::mem_pool::Pool pool;
    KVReadOptions kvOptions;
    KVMetricsCollector collector;
    kvOptions.metricsCollector = &collector;
    kvOptions.fieldName = "mstr";
    kvOptions.pool = &pool;
    future_lite::executors::SimpleExecutor ex(1);
    MultiString value;
    ASSERT_FALSE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("2"), value, kvOptions), &ex));

    collector.Reset();
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("10"), value, kvOptions), &ex));
    ASSERT_EQ(2, value.size());
    ASSERT_EQ(1, value[0].size());
    ASSERT_EQ('a', value[0][0]);
    ASSERT_EQ(2, value[1].size());
    ASSERT_EQ('c', value[1][0]);
    ASSERT_EQ('d', value[1][1]);
    ASSERT_LT(0L, collector.GetSSTableLatency());
    ASSERT_LE(0L, collector.GetMemTableLatency());
    // FIXME: refactor block cache counter
    EXPECT_EQ(2L, collector.GetBlockCacheHitCount());
    EXPECT_EQ(1L, collector.GetBlockCacheMissCount());
    ASSERT_LT(0L, collector.GetBlockCacheReadLatency());
    ASSERT_EQ(0L, collector.GetSearchCacheMissCount());
    ASSERT_EQ(0L, collector.GetSearchCacheHitCount());
    ASSERT_EQ(1L, collector.GetSSTableCount());
    ASSERT_EQ(0L, collector.GetMemTableCount());

    docString = "cmd=add,key=10,uint32=11,ts=202000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    collector.Reset();
    kvOptions.fieldName = "uint32";
    uint32_t value32;
    kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("10"), value32, kvOptions), &ex));
    ASSERT_EQ(11, value32);
    ASSERT_EQ(0L, collector.GetSSTableCount());
    ASSERT_EQ(1L, collector.GetMemTableCount());

    // accumulate metrics collector
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("10"), value32, kvOptions), &ex));
    ASSERT_EQ(11, value32);
    ASSERT_EQ(0L, collector.GetSSTableCount());
    ASSERT_EQ(2L, collector.GetMemTableCount());
}

void KVReaderTest::TestSearchCache()
{
    future_lite::executors::SimpleExecutor ex(1);
    string field = "key:string;value:uint64;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    string docString = "cmd=add,key=abc,value=1,ts=101000000;"
                       "cmd=add,key=def,value=2,ts=102000000";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
    SearchCachePtr searchCache(new SearchCache(40960, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
    resource.searchCache.reset(new SearchCachePartitionWrapper(searchCache, ""));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    auto kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    uint64_t value;
    mem_pool::Pool pool;
    KVReadOptions kvOptions;
    KVMetricsCollector collector;
    kvOptions.metricsCollector = &collector;
    kvOptions.pool = &pool;
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("abc"), value, kvOptions), &ex));
    ASSERT_EQ(1ul, value);
    ASSERT_EQ(1L, collector.GetSearchCacheMissCount());
    ASSERT_EQ(0L, collector.GetSearchCacheHitCount());

    collector.Reset();
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("abc"), value, kvOptions), &ex));
    ASSERT_EQ(1ul, value);
    ASSERT_LE(0L, collector.GetSSTableLatency());
    ASSERT_LE(0L, collector.GetMemTableLatency());
    // FIXME: refactor
    ASSERT_EQ(0L, collector.GetBlockCacheHitCount());
    ASSERT_EQ(0L, collector.GetBlockCacheMissCount());
    ASSERT_EQ(1L, collector.GetSearchCacheHitCount());
    ASSERT_EQ(0L, collector.GetSearchCacheMissCount());
    ASSERT_EQ(1L, collector.GetSSTableCount());
    ASSERT_EQ(0L, collector.GetMemTableCount());
    ASSERT_EQ(1L, collector.GetSearchCacheResultCount());

    docString = "cmd=add,key=abc,value=3,ts=202000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    collector.Reset();
    kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("abc"), value, kvOptions), &ex));
    ASSERT_EQ(3, value);
    ASSERT_EQ(0L, collector.GetSearchCacheHitCount());
    ASSERT_EQ(0L, collector.GetSearchCacheMissCount());
    ASSERT_EQ(0L, collector.GetSSTableCount());
    ASSERT_EQ(1L, collector.GetMemTableCount());

    // accumulate metrics collector
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("abc"), value, kvOptions), &ex));
    ASSERT_EQ(3, value);
    ASSERT_EQ(0L, collector.GetSearchCacheHitCount());
    ASSERT_EQ(2L, collector.GetMemTableCount());
}

void KVReaderTest::TestSearchCacheIncReclaimAllRtBug()
{
    // EXPECT failed
    // TODO: fix it
    string field = "key:string;value:uint64;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    schema->SetEnableTTL(true);
    string fullDoc = "cmd=add,key=abc,value=1,ts=101000000;"
                     "cmd=add,key=def,value=2,ts=102000000";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
    SearchCachePtr searchCache(new SearchCache(40960, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
    resource.searchCache.reset(new SearchCachePartitionWrapper(searchCache, ""));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));

    auto kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    uint64_t value;
    mem_pool::Pool pool;
    KVReadOptions kvOptions;
    KVMetricsCollector collector;
    future_lite::executors::SimpleExecutor ex(1);
    kvOptions.metricsCollector = &collector;
    kvOptions.pool = &pool;
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("abc"), value, kvOptions), &ex));
    ASSERT_EQ(1ul, value);
    ASSERT_EQ(1L, collector.GetSearchCacheMissCount());
    ASSERT_EQ(0L, collector.GetSearchCacheHitCount());

    collector.Reset();
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("abc"), value, kvOptions), &ex));
    ASSERT_EQ(1ul, value);
    // FIXME: refactor
    ASSERT_EQ(1L, collector.GetSearchCacheHitCount());
    ASSERT_EQ(0L, collector.GetSearchCacheMissCount());
    // ASSERT_EQ(1L, collector.GetSSTableCount());
    // ASSERT_EQ(0L, collector.GetMemTableCount());
    ASSERT_EQ(1L, collector.GetSearchCacheResultCount());

    string rtDocStr = "cmd=add,key=efg,value=3,ts=202000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocStr, "", ""));
    collector.Reset();
    kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("abc"), value, kvOptions), &ex));
    ASSERT_EQ(1, value);
    ASSERT_EQ(1L, collector.GetSearchCacheHitCount());
    ASSERT_EQ(0L, collector.GetSearchCacheMissCount());

    collector.Reset();
    string incDocStr = "cmd=add,key=ghi,value=3,ts=302000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr, "", ""));
    kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    // accumulate metrics collector
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("abc"), value, kvOptions), &ex));
    ASSERT_EQ(1, value);
    ASSERT_EQ(1L, collector.GetSearchCacheHitCount());
    ASSERT_EQ(0L, collector.GetSearchCacheMissCount());
    EXPECT_EQ(1L, collector.GetSearchCacheResultCount());

    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("abc"), value, kvOptions), &ex));
    ASSERT_EQ(1, value);
    EXPECT_EQ(2L, collector.GetSearchCacheHitCount());
    EXPECT_EQ(0L, collector.GetSearchCacheMissCount());
    EXPECT_EQ(1L, collector.GetSearchCacheResultCount());
}

void KVReaderTest::TestCuckoo()
{
    string field = "key:string;value:uint64;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KVIndexPreference::HashDictParam keyParam("cuckoo", 90);
    kvConfig->GetIndexPreference().SetHashDictParam(keyParam);

    string docString = "cmd=add,key=abc,value=1,ts=101000000;"
                       "cmd=add,key=def,value=2,ts=102000000";
    PartitionStateMachine psm;
    mOptions.SetEnablePackageFile(false);
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    KVReaderPtr kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    KVReaderImpl* readerImpl = (KVReaderImpl*)kvReader.get();
    // typedef CuckooHashTableTraits<keytype_t, Timestamp0Value<uint64_t>, false> OfflineHashTableTraits;
    // typedef DenseHashTableTraits<keytype_t, TimestampValue<uint64_t>, false> OnlineHashTableTraits;
    // typedef HashTableFixSegmentReader<OfflineHashTableTraits> OfflineSegmentReader;
    // typedef HashTableFixSegmentReader<OnlineHashTableTraits> OnlineSegmentReader;
    // typedef KVReaderImpl<OfflineSegmentReader, OnlineSegmentReader, false, false> ReaderImpl;
    // ReaderImpl* readerImpl = dynamic_cast<ReaderImpl*>(readerImplBase);
    ASSERT_TRUE(readerImpl);

    uint64_t value;
    mem_pool::Pool pool;
    KVReadOptions kvOptions;
    future_lite::executors::SimpleExecutor ex(1);
    kvOptions.pool = &pool;
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("abc"), value, kvOptions), &ex));
    ASSERT_EQ(1ul, value);

    docString = "cmd=add,key=hij,value=4,ts=202000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docString, "", ""));

    docString = "cmd=add,key=abc,value=3,ts=203000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));

    kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("abc"), value, kvOptions), &ex));
    ASSERT_EQ(3ul, value);
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("def"), value, kvOptions), &ex));
    ASSERT_EQ(2ul, value);
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("hij"), value, kvOptions), &ex));
    ASSERT_EQ(4ul, value);
}

string KVReaderTest::MakeFixValue(const string& fixValueStr, size_t fixLen)
{
    vector<uint32_t> values;
    StringUtil::fromString(fixValueStr, values, "#");
    values.resize(fixLen, 0);
    return string((const char*)values.data(), sizeof(uint32_t) * fixLen);
}

// str:"key,value,isdeleted,ts;key,value,isDeleted,ts"
template <typename Reader>
void KVReaderTest::PrepareSegmentReader(const std::string& offlineReaderStr, const std::string& onlineReaderStr,
                                        Reader& reader, size_t fixLen)
{
    vector<vector<string>> values;
    StringUtil::fromString(offlineReaderStr, values, ",", ";");
    vector<KVSegmentReader> readers;
    for (size_t i = 0; i < values.size(); i++) {
        FakeSegmentReader* segReader = new FakeSegmentReader();
        assert(values[i].size() == 4);
        segReader->SetKeyValue(values[i][0], MakeFixValue(values[i][1], fixLen), values[i][2], values[i][3]);
        KVSegmentReader kvSegmentReader;
        kvSegmentReader.mReader.reset((KVSegmentReaderImplBase*)segReader);
        readers.push_back(kvSegmentReader);
    }
    reader.mOfflineSegmentReaders.push_back(readers);

    values.clear();
    readers.clear();
    StringUtil::fromString(onlineReaderStr, values, ",", ";");
    for (size_t i = 0; i < values.size(); i++) {
        FakeSegmentReader* segReader = new FakeSegmentReader();
        assert(values[i].size() == 4);
        segReader->SetKeyValue(values[i][0], MakeFixValue(values[i][1], fixLen), values[i][2], values[i][3]);
        KVSegmentReader kvSegmentReader;
        kvSegmentReader.mReader.reset((KVSegmentReaderImplBase*)segReader);
        reader.mOnlineSegmentReaders.push_back(kvSegmentReader);
    }
    //    reader.mOnlineSegmentReaders.push_back(readers);
}

template <typename ValueType>
void KVReaderTest::CheckValue(const KVReader* reader, uint64_t key, const string& resultStr,
                              const KVReadOptions& options)
{
    ValueType value;
    future_lite::executors::SimpleExecutor ex(1);
    ASSERT_TRUE(future_lite::interface::syncAwait(reader->GetAsync(key, value, options), &ex));
    vector<uint32_t> expectValues;
    StringUtil::fromString(resultStr, expectValues, "#");

    ASSERT_EQ((uint32_t)expectValues.size(), value.size());
    for (size_t i = 0; i < expectValues.size(); i++) {
        ASSERT_EQ(expectValues[i], value[i]);
    }
}

void KVReaderTest::TestGetFixLenValue()
{
    typedef KVReaderImpl KVReaderTyped;
    std::unique_ptr<KVReaderTyped> readerImpl(new KVReaderTyped);
    readerImpl->mHasTTL = false;
    readerImpl->mHasSearchCache = false;
    PrepareSegmentReader("1,10#20#30#20,false,1", "2,20#30#40#10,false,2", *readerImpl, 4);
    // KVReader reader;
    // reader.mReader.reset(readerImpl.release());

    string field = "key:uint64;uint32:uint32:true:false::4";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "uint32");
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    readerImpl->InitInnerMeta(kvConfig);

    Pool pool;
    KVReadOptions options;
    options.pool = &pool;
    CheckValue<MultiUInt32>(readerImpl.get(), 1, "10#20#30#20", options);
    CheckValue<MultiUInt32>(readerImpl.get(), 2, "20#30#40#10", options);
}

void KVReaderTest::TestGetWithRegionId()
{
    string field = "key1:string;key2:int32;"
                   "value1:int32;value2:uint32;";
    RegionSchemaMaker maker(field, "kv");
    maker.AddRegion("region1", "key1", "value1", "");
    maker.AddRegion("region2", "key2", "value2", "");
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    auto innerCheck = [&schema, this](regionid_t regionId, function<void(KVReaderImpl&, KVReadOptions&)> validater) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema(regionId);
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        KVIndexOptions indexOptions;
        indexOptions.kvConfig = kvConfig;

        typedef KVReaderImpl KVReaderTyped;
        std::unique_ptr<KVReaderTyped> readerImpl(new KVReaderTyped);
        readerImpl->mHasTTL = false;
        readerImpl->mHasSearchCache = false;

        PrepareSegmentReader(to_string(indexOptions.GetLookupKeyHash(7)) + ",10,false,1",
                             to_string(indexOptions.GetLookupKeyHash(12)) + ",20,false,2", *readerImpl, 4);
        // KVReader reader;
        // reader.mReader.reset(readerImpl.release());
        readerImpl->InitInnerMeta(kvConfig);
        readerImpl->mKVIndexOptions = indexOptions;
        mem_pool::Pool pool;
        KVReadOptions kvOptions;
        kvOptions.pool = &pool;
        validater(*readerImpl, kvOptions);
    };
    auto validater1 = [](KVReaderImpl& reader, KVReadOptions& kvOptions) {
        int32_t value;
        future_lite::executors::SimpleExecutor ex(1);
        future_lite::interface::syncAwait(reader.GetAsync(7, value, kvOptions), &ex);
        ASSERT_EQ(10, value);
        future_lite::interface::syncAwait(reader.GetAsync(12, value, kvOptions), &ex);
        ASSERT_EQ(20, value);
    };
    innerCheck(0, validater1);
    auto validater2 = [](KVReaderImpl& reader, KVReadOptions& kvOptions) {
        uint32_t value;
        future_lite::executors::SimpleExecutor ex(1);
        future_lite::interface::syncAwait(reader.GetAsync(7, value, kvOptions), &ex);
        ASSERT_EQ(10, value);
        future_lite::interface::syncAwait(reader.GetAsync(12, value, kvOptions), &ex);
        ASSERT_EQ(20, value);
    };
    innerCheck(1, validater2);
}

void KVReaderTest::TestBatchGet()
{
    typedef KVReaderImpl KVReaderTyped;
    std::unique_ptr<KVReaderTyped> readerImpl(new KVReaderTyped);
    readerImpl->mHasTTL = false;
    readerImpl->mHasSearchCache = false;
    PrepareSegmentReader("1,10,false,1", "2,20,false,2", *readerImpl, 4);
    string field = "key:uint64;uint32:uint32";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "uint32");
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    readerImpl->InitInnerMeta(kvConfig);
    ASSERT_EQ(indexlibv2::index::KVVT_TYPED, readerImpl->GetValueType());

    Pool pool;
    KVReadOptions options;
    options.pool = &pool;
    future_lite::executors::SimpleExecutor ex(1);
    vector<uint64_t> keys = {1, 2};
    vector<StringView> values;
    auto res = future_lite::coro::syncAwait(readerImpl->BatchGetAsync(keys, values, tsc_default, options), &ex);
#if FUTURE_LITE_USE_COROUTINES
    ASSERT_FALSE(res[0].hasError());
    ASSERT_FALSE(res[1].hasError());
    ASSERT_TRUE(res[0].value());
    ASSERT_TRUE(res[1].value());
    ASSERT_EQ(10, *(uint32_t*)values[0].data());
    ASSERT_EQ(20, *(uint32_t*)values[1].data());
#else
    ASSERT_TRUE(res[0]);
    ASSERT_TRUE(res[1]);
    ASSERT_EQ(10, *(uint32_t*)values[0].data());
    ASSERT_EQ(20, *(uint32_t*)values[1].data());
#endif
}

}} // namespace indexlib::index
