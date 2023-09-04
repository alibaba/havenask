
#include "indexlib/index/kkv/test/kkv_reader_unittest.h"

#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/kkv/codegen_kkv_reader.h"
#include "indexlib/index/kkv/multi_region_kkv_reader.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/fake_partition_data_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/region_schema_maker.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KKVReaderTest);

KKVReaderTest::KKVReaderTest() {}

KKVReaderTest::~KKVReaderTest() {}

void KKVReaderTest::CaseSetUp()
{
    mOptions.GetBuildConfig(false).buildTotalMemory = 20; // 20MB
    mOptions.GetBuildConfig(true).buildTotalMemory = 20;  // 20MB
}

void KKVReaderTest::CaseTearDown() {}

void KKVReaderTest::TestBlockCache()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KKV_PKEY_", "_KKV_SKEY_", "_KKV_VALUE_"],
               "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KKVIndexPreference::HashDictParam param(PrefixKeyTableType2Str(PKT_CUCKOO));
    kkvConfig->GetIndexPreference().SetHashDictParam(param);

    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    auto kkvReader = psm.GetIndexPartition()->GetReader()->GetKKVReader();
    future_lite::executors::SimpleExecutor ex(1);
    KVMetricsCollector collector;
    PackAttributeFormatterPtr packAttrFormatter(new PackAttributeFormatter);
    auto valueConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, psm.GetIndexPartition()
                                                                ->GetSchema()
                                                                ->GetRegionSchema(DEFAULT_REGIONID)
                                                                ->GetIndexSchema()
                                                                ->GetPrimaryKeyIndexConfig())
                           ->GetValueConfig();
    ASSERT_TRUE(packAttrFormatter->Init(valueConfig->CreatePackAttributeConfig()));
    auto* valueRef = packAttrFormatter->GetAttributeReferenceTyped<uint32_t>("value");
    StringView rawValue;
    uint32_t value;

    {
        auto kkvDocIter = future_lite::interface::syncAwait(
            kkvReader->LookupAsync(StringView("pkey1"), vector<StringView>({StringView("1")}), 0, tsc_default, &mPool,
                                   &collector),
            &ex);

        ASSERT_TRUE(kkvDocIter->IsValid());
        kkvDocIter->GetCurrentValue(rawValue);
        valueRef->GetValue(rawValue.data(), value);
        ASSERT_EQ(1, kkvDocIter->GetCurrentSkey());
        ASSERT_EQ(1, value);
        kkvDocIter->MoveToNext();
        ASSERT_FALSE(kkvDocIter->IsValid());
        kkvDocIter->Finish();
        EXPECT_LT(0L, collector.GetSSTableLatency());
        EXPECT_LE(0L, collector.GetMemTableLatency());
        // FIXME: refactor
        EXPECT_EQ(3L, collector.GetBlockCacheHitCount());
        EXPECT_EQ(2L, collector.GetBlockCacheMissCount());
        EXPECT_LT(0L, collector.GetBlockCacheReadLatency());
        EXPECT_EQ(0L, collector.GetSearchCacheMissCount());
        EXPECT_EQ(0L, collector.GetSearchCacheHitCount());
        EXPECT_EQ(1L, collector.GetSSTableCount());
        EXPECT_EQ(0L, collector.GetMemTableCount());
        IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, kkvDocIter);
    }

    collector.Reset();
    {
        auto kkvDocIter = future_lite::interface::syncAwait(
            kkvReader->LookupAsync(StringView("pkey1"), 0, tsc_default, &mPool, &collector), &ex);

        ASSERT_TRUE(kkvDocIter->IsValid());
        EXPECT_EQ(1, kkvDocIter->GetCurrentSkey());
        kkvDocIter->MoveToNext();
        ASSERT_TRUE(kkvDocIter->IsValid());
        EXPECT_EQ(2, kkvDocIter->GetCurrentSkey());
        kkvDocIter->MoveToNext();
        ASSERT_FALSE(kkvDocIter->IsValid());
        kkvDocIter->Finish();
        EXPECT_EQ(2L, collector.GetSSTableCount());
        EXPECT_EQ(0L, collector.GetMemTableCount());
        IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, kkvDocIter);
    }

    {
        // accumulate metrics collector
        auto kkvDocIter = future_lite::interface::syncAwait(
            kkvReader->LookupAsync(StringView("pkey1"), 0, tsc_default, &mPool, &collector), &ex);

        ASSERT_TRUE(kkvDocIter->IsValid());
        EXPECT_EQ(1, kkvDocIter->GetCurrentSkey());
        kkvDocIter->MoveToNext();
        ASSERT_TRUE(kkvDocIter->IsValid());
        EXPECT_EQ(2, kkvDocIter->GetCurrentSkey());
        kkvDocIter->MoveToNext();
        ASSERT_FALSE(kkvDocIter->IsValid());
        kkvDocIter->Finish();
        EXPECT_EQ(4L, collector.GetSSTableCount());
        EXPECT_EQ(0L, collector.GetMemTableCount());
        IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, kkvDocIter);
    }
}

void KKVReaderTest::TestSearchCache()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KKVIndexPreference::HashDictParam param(PrefixKeyTableType2Str(PKT_CUCKOO));
    kkvConfig->GetIndexPreference().SetHashDictParam(param);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
    SearchCachePtr searchCache(new SearchCache(40960, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
    resource.searchCache.reset(new SearchCachePartitionWrapper(searchCache, ""));
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pkey1", "skey=1,value=1;skey=2,value=2"));

    auto kkvReader = psm.GetIndexPartition()->GetReader()->GetKKVReader();
    future_lite::executors::SimpleExecutor ex(1);
    KVMetricsCollector collector;

    PackAttributeFormatterPtr packAttrFormatter(new PackAttributeFormatter);
    ValueConfigPtr valueConfig = kkvConfig->GetValueConfig();
    ASSERT_TRUE(packAttrFormatter->Init(valueConfig->CreatePackAttributeConfig()));
    auto* valueRef = packAttrFormatter->GetAttributeReferenceTyped<uint32_t>("value");
    StringView rawValue;
    uint32_t value;
    {
        auto kkvIterator = future_lite::interface::syncAwait(
            kkvReader->LookupAsync(StringView("pkey1"), vector<StringView>({StringView("1")}), 0, tsc_default, &mPool,
                                   &collector),
            &ex);

        ASSERT_TRUE(kkvIterator->IsValid());
        kkvIterator->GetCurrentValue(rawValue);
        valueRef->GetValue(rawValue.data(), value);
        ASSERT_EQ(1, kkvIterator->GetCurrentSkey());
        ASSERT_EQ(1, value);
        kkvIterator->MoveToNext();
        ASSERT_FALSE(kkvIterator->IsValid());
        kkvIterator->Finish();
        EXPECT_EQ(1L, collector.GetSearchCacheMissCount());
        EXPECT_EQ(0L, collector.GetSearchCacheHitCount());
        EXPECT_LT(0L, collector.GetSSTableLatency());
        EXPECT_LE(0L, collector.GetMemTableLatency());
        // FIXME: refactor
        EXPECT_EQ(0L, collector.GetBlockCacheHitCount());
        EXPECT_EQ(0L, collector.GetBlockCacheMissCount());
        EXPECT_EQ(0L, collector.GetBlockCacheReadLatency());
        EXPECT_EQ(1L, collector.GetSSTableCount());
        EXPECT_EQ(0L, collector.GetMemTableCount());
        EXPECT_EQ(0L, collector.GetSearchCacheResultCount());
        IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, kkvIterator);
    }

    docString = "cmd=add,pkey=pkey1,skey=3,value=3,ts=201;"
                "cmd=add,pkey=pkey1,skey=4,value=4,ts=202";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    kkvReader = psm.GetIndexPartition()->GetReader()->GetKKVReader();
    collector.Reset();
    {
        auto kkvIterator = future_lite::interface::syncAwait(
            kkvReader->LookupAsync(StringView("pkey1"), 0, tsc_default, &mPool, &collector), &ex);
        ASSERT_TRUE(kkvIterator->IsValid());
        ASSERT_EQ(3, kkvIterator->GetCurrentSkey());
        kkvIterator->MoveToNext();
        ASSERT_TRUE(kkvIterator->IsValid());
        ASSERT_EQ(4, kkvIterator->GetCurrentSkey());
        kkvIterator->MoveToNext();
        ASSERT_TRUE(kkvIterator->IsValid());
        ASSERT_EQ(1, kkvIterator->GetCurrentSkey());
        kkvIterator->MoveToNext();
        ASSERT_TRUE(kkvIterator->IsValid());
        ASSERT_EQ(2, kkvIterator->GetCurrentSkey());
        kkvIterator->MoveToNext();
        ASSERT_FALSE(kkvIterator->IsValid());
        kkvIterator->Finish();
        ASSERT_EQ(0L, collector.GetSearchCacheMissCount());
        ASSERT_EQ(1L, collector.GetSearchCacheHitCount());
        ASSERT_EQ(2L, collector.GetSSTableCount());
        ASSERT_EQ(2L, collector.GetMemTableCount());
        ASSERT_EQ(2L, collector.GetSearchCacheResultCount());
        IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, kkvIterator);
    }

    // accumulate metrics collector
    {
        auto kkvIterator = future_lite::interface::syncAwait(
            kkvReader->LookupAsync(StringView("pkey1"), 0, tsc_default, &mPool, &collector), &ex);

        ASSERT_TRUE(kkvIterator->IsValid());
        ASSERT_EQ(3, kkvIterator->GetCurrentSkey());
        kkvIterator->MoveToNext();
        ASSERT_TRUE(kkvIterator->IsValid());
        ASSERT_EQ(4, kkvIterator->GetCurrentSkey());
        kkvIterator->MoveToNext();
        ASSERT_TRUE(kkvIterator->IsValid());
        ASSERT_EQ(1, kkvIterator->GetCurrentSkey());
        kkvIterator->MoveToNext();
        ASSERT_TRUE(kkvIterator->IsValid());
        ASSERT_EQ(2, kkvIterator->GetCurrentSkey());
        kkvIterator->MoveToNext();
        ASSERT_FALSE(kkvIterator->IsValid());
        kkvIterator->Finish();
        EXPECT_EQ(0L, collector.GetSearchCacheMissCount());
        EXPECT_EQ(2L, collector.GetSearchCacheHitCount());
        EXPECT_EQ(4L, collector.GetSSTableCount());
        EXPECT_EQ(4L, collector.GetMemTableCount());
        EXPECT_EQ(4L, collector.GetSearchCacheResultCount());
        IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, kkvIterator);
    }
}

void KKVReaderTest::TestMultiInMemSegments()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    IndexPartitionOptions options;
    FakePartitionDataCreator pdc;
    pdc.Init(schema, options, GET_TEMP_DATA_PATH());

    string fullDocs = "cmd=add,pkey=0,skey=1,value=1;"
                      "cmd=add,pkey=0,skey=2,value=2;";
    pdc.AddFullDocs(fullDocs);

    string builtRtDocs = "cmd=add,pkey=0,skey=3,value=3,ts=0;"
                         "cmd=add,pkey=0,skey=4,value=4,ts=0;";
    pdc.AddBuiltSegment(builtRtDocs);

    string inMemSegment1 = "cmd=add,pkey=0,skey=5,value=5,ts=0;"
                           "cmd=add,pkey=0,skey=6,value=6,ts=0;"
                           "cmd=add,pkey=0,skey=8,value=6,ts=0;";
    pdc.AddInMemSegment(inMemSegment1);

    string inMemSegment2 = "cmd=add,pkey=0,skey=7,value=7,ts=0;"
                           "cmd=add,pkey=0,skey=8,value=8,ts=0;"
                           "cmd=add,pkey=0,skey=9,value=9,ts=0;"
                           "cmd=add,pkey=0,skey=10,value=9,ts=0;";
    pdc.AddInMemSegment(inMemSegment2);

    string buildingDocs = "cmd=add,pkey=0,skey=10,value=10,ts=0;"
                          "cmd=add,pkey=0,skey=11,value=11,ts=0;";
    pdc.AddBuildingData(buildingDocs);

    auto kkvConfig = DYNAMIC_POINTER_CAST(
        KKVIndexConfig, pdc.GetIndexPartition()->GetSchema()->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    PartitionDataPtr partData = pdc.CreatePartitionData(false);

    KKVReaderPtr reader(new CodegenKKVReader);
    reader->Open(kkvConfig, partData, SearchCachePartitionWrapperPtr(), 0);
    CheckResult(reader, "0", "10,10;11,11;7,7;8,8;9,9;5,5;6,6;3,3;4,4;1,1;2,2");
}

void KKVReaderTest::TestBug35656866()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    IndexPartitionOptions options;
    FakePartitionDataCreator pdc;
    pdc.Init(schema, options, GET_TEMP_DATA_PATH());

    stringstream builtDocs;
    stringstream builtResults;
    for (size_t i = 0; i < 68; i++) {
        builtDocs << "cmd=add,pkey=0,skey=" << i << ",value=10,ts=0;";
        builtResults << i << ",10;";
    }
    pdc.AddBuiltSegment(builtDocs.str());

    stringstream buildingDocs;
    stringstream expectResults;
    for (size_t i = 0; i < 68; i++) {
        buildingDocs << "cmd=add,pkey=0,skey=" << i + 68 << ",value=10,ts=0;";
        expectResults << i + 68 << ",10;";
    }
    expectResults << builtResults.str();
    pdc.AddBuildingData(buildingDocs.str());
    auto kkvConfig = DYNAMIC_POINTER_CAST(
        KKVIndexConfig, pdc.GetIndexPartition()->GetSchema()->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    PartitionDataPtr partData = pdc.CreatePartitionData(false);

    KKVReaderPtr reader(new CodegenKKVReader);
    reader->Open(kkvConfig, partData, SearchCachePartitionWrapperPtr(), 0);
    CheckResult(reader, "0", expectResults.str());
}

void KKVReaderTest::TestTableTypeWithCuckoo()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KKVIndexPreference::HashDictParam param(PrefixKeyTableType2Str(PKT_CUCKOO), 90);
    kkvConfig->GetIndexPreference().SetHashDictParam(param);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102";
    typedef ClosedHashPrefixKeyTable<SKeyListInfo, PKT_DENSE> TableD;
    typedef ClosedHashPrefixKeyTable<OnDiskPKeyOffset, PKT_CUCKOO> TableC;
    TableD* denseTable;
    TableC* cuckooTable;

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pkey1", "skey=1,value=1;skey=2,value=2"));
    auto kkvReader = psm.GetIndexPartition()->GetReader()->GetKKVReader();
    auto ReaderImpl = dynamic_cast<KKVReaderImpl<int32_t>*>(((CodegenKKVReader*)kkvReader.get())->mReader.get());
    ASSERT_TRUE(ReaderImpl->mBuildingSegReaders.empty());
    int32_t i = ReaderImpl->mColumnIdToSegmentReaders.size() - 1;
    ASSERT_GE(i, 0);
    int32_t j = 0;
    for (; i >= 0; --i) {
        j = ReaderImpl->mColumnIdToSegmentReaders[i].size() - 1;
        ASSERT_GE(j, 0);
        for (; j >= 0; --j) {
            ASSERT_EQ(PKT_CUCKOO, ReaderImpl->mColumnIdToSegmentReaders[i][j]->mPKeyTable->GetTableType());
            cuckooTable = dynamic_cast<TableC*>(ReaderImpl->mColumnIdToSegmentReaders[i][j]->mPKeyTable.get());
            ASSERT_TRUE(cuckooTable);
            ASSERT_EQ(90, cuckooTable->GetOccupancyPct());
        }
    }

    KVMetricsCollector collector;

    ValueConfigPtr valueConfig = kkvConfig->GetValueConfig();
    PackAttributeFormatterPtr packAttrFormatter(new PackAttributeFormatter);
    ASSERT_TRUE(packAttrFormatter->Init(valueConfig->CreatePackAttributeConfig()));
    auto* valueRef = packAttrFormatter->GetAttributeReferenceTyped<uint32_t>("value");
    StringView rawValue;
    uint32_t value;
    future_lite::executors::SimpleExecutor ex(1);
    {
        auto kkvIterator = future_lite::interface::syncAwait(
            kkvReader->LookupAsync(StringView("pkey1"), vector<StringView>({StringView("1")}), 0, tsc_default, &mPool,
                                   &collector),
            &ex);

        ASSERT_TRUE(kkvIterator->IsValid());
        kkvIterator->GetCurrentValue(rawValue);
        valueRef->GetValue(rawValue.data(), value);
        ASSERT_EQ(1, kkvIterator->GetCurrentSkey());
        ASSERT_EQ(1, value);
        kkvIterator->MoveToNext();
        ASSERT_FALSE(kkvIterator->IsValid());
        kkvIterator->Finish();
        IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, kkvIterator);
    }

    docString = "cmd=add,pkey=pkey1,skey=3,value=3,ts=201;"
                "cmd=add,pkey=pkey1,skey=4,value=4,ts=202";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docString, "", ""));
    kkvReader = psm.GetIndexPartition()->GetReader()->GetKKVReader();
    ReaderImpl = dynamic_cast<KKVReaderImpl<int32_t>*>(((CodegenKKVReader*)kkvReader.get())->mReader.get());

    ASSERT_EQ(PKT_DENSE, ReaderImpl->mBuildingSegReaders[0]->mPKeyTable->GetTableType());
    denseTable = dynamic_cast<TableD*>(ReaderImpl->mBuildingSegReaders[0]->mPKeyTable.get());
    ASSERT_TRUE(denseTable);
    ASSERT_EQ(80, denseTable->GetOccupancyPct());

    i = ReaderImpl->mColumnIdToSegmentReaders.size() - 1;
    ASSERT_GE(i, 0);
    for (; i >= 0; --i) {
        j = ReaderImpl->mColumnIdToSegmentReaders[i].size() - 1;
        ASSERT_GE(j, 0);
        for (; j >= 0; --j) {
            ASSERT_EQ(PKT_CUCKOO, ReaderImpl->mColumnIdToSegmentReaders[i][j]->mPKeyTable->GetTableType());
            cuckooTable = dynamic_cast<TableC*>(ReaderImpl->mColumnIdToSegmentReaders[i][j]->mPKeyTable.get());
            ASSERT_TRUE(cuckooTable);
            ASSERT_EQ(90, cuckooTable->GetOccupancyPct());
        }
    }

    collector.Reset();
    {
        auto kkvIterator = future_lite::interface::syncAwait(
            kkvReader->LookupAsync(StringView("pkey1"), 0, tsc_default, &mPool, &collector), &ex);

        ASSERT_TRUE(kkvIterator->IsValid());
        ASSERT_EQ(3, kkvIterator->GetCurrentSkey());
        kkvIterator->MoveToNext();
        ASSERT_TRUE(kkvIterator->IsValid());
        ASSERT_EQ(4, kkvIterator->GetCurrentSkey());
        kkvIterator->MoveToNext();
        ASSERT_TRUE(kkvIterator->IsValid());
        ASSERT_EQ(1, kkvIterator->GetCurrentSkey());
        kkvIterator->MoveToNext();
        ASSERT_TRUE(kkvIterator->IsValid());
        ASSERT_EQ(2, kkvIterator->GetCurrentSkey());
        kkvIterator->MoveToNext();
        ASSERT_FALSE(kkvIterator->IsValid());
        kkvIterator->Finish();
        IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, kkvIterator);
    }
}

void KKVReaderTest::TestMultiRegion()
{
    string field = "uid:string;friendid:int32;value:uint32;";
    RegionSchemaMaker maker(field, "kkv");
    maker.AddRegion("region1", "uid", "friendid", "value;friendid;", "");
    maker.AddRegion("region2", "friendid", "uid", "value;uid;", "");
    IndexPartitionSchemaPtr schema = maker.GetSchema();
    KKVIndexConfigPtr firstKKVConfig =
        DYNAMIC_POINTER_CAST(config::KKVIndexConfig, schema->GetIndexSchema(0)->GetPrimaryKeyIndexConfig());

    auto suffixInfo = firstKKVConfig->GetSuffixFieldInfo();
    suffixInfo.countLimits = 5;
    firstKKVConfig->SetSuffixFieldInfo(suffixInfo);

    KKVIndexConfigPtr secondKKVConfig =
        DYNAMIC_POINTER_CAST(config::KKVIndexConfig, schema->GetIndexSchema(1)->GetPrimaryKeyIndexConfig());

    suffixInfo = secondKKVConfig->GetSuffixFieldInfo();
    suffixInfo.countLimits = KKVIndexFieldInfo::INVALID_COUNT_LIMITS;
    suffixInfo.sortParams = StringToSortParams("-value");
    suffixInfo.enableKeepSortSequence = true;
    suffixInfo.countLimits = 10;
    secondKKVConfig->SetSuffixFieldInfo(suffixInfo);

    IndexPartitionOptions options;
    FakePartitionDataCreator pdc;
    pdc.Init(schema, options, GET_TEMP_DATA_PATH());

    string fullDocs = "cmd=add,uid=superman,friendid=0,value=1,region_name=region1;"
                      "cmd=add,uid=batman,friendid=1,value=2,region_name=region2;"
                      "cmd=add,uid=superman,friendid=1,value=3,region_name=region1;"
                      "cmd=add,uid=ironman,friendid=3,value=4,region_name=region1;"
                      "cmd=add,uid=woman,friendid=3,value=5,region_name=region2;";
    pdc.AddFullDocs(fullDocs);

    PartitionDataPtr partData = pdc.CreatePartitionData(false);
    MultiRegionKKVReader mrReader;
    mrReader.Open(schema, partData, SearchCachePartitionWrapperPtr(), 0);
    ASSERT_EQ(2u, mrReader.mReaderVec.size());

    auto reader1 = mrReader.GetReader("region1");
    auto reader2 = mrReader.GetReader("region2");

    ASSERT_EQ(ft_int32, reader1->mKKVIndexOptions.kkvConfig->GetSuffixFieldConfig()->GetFieldType());
    ASSERT_EQ(ft_string, reader2->mKKVIndexOptions.kkvConfig->GetSuffixFieldConfig()->GetFieldType());

    ASSERT_EQ(5u, reader1->mKKVIndexOptions.GetSKeyCountLimits());
    ASSERT_EQ(10u, reader2->mKKVIndexOptions.GetSKeyCountLimits());
}

void KKVReaderTest::TestCompatibleIndex()
{
    // index was generated by KKVReaderTest::TestMultiRegion(old binary),
    //   with all sort/truncate settings disabled
    string srcIndexRoot = util::PathUtil::JoinPath(GET_PRIVATE_TEST_DATA_PATH(), "kkv_old_index");
    string indexRoot = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "index");
    ASSERT_EQ(FSEC_OK, file_system::FslibWrapper::Copy(srcIndexRoot, indexRoot).Code());
    string field = "uid:string;friendid:int32;value:uint32;";
    RegionSchemaMaker maker(field, "kkv");
    maker.AddRegion("region1", "uid", "friendid", "value;friendid;", "");
    maker.AddRegion("region2", "friendid", "uid", "value;uid;", "");
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    IndexPartitionOptions options;
    PartitionStateMachine psm;

    ASSERT_TRUE(psm.Init(schema, options, indexRoot));
    string incDocs = "cmd=add,uid=ironman,friendid=13,value=14,region_name=region1;"
                     "cmd=add,uid=woman,friendid=13,value=15,region_name=region2;"
                     "cmd=add,uid=loki,friendid=16,value=17,region_name=region1;"
                     "cmd=add,uid=thor,friendid=1,value=18,region_name=region2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "", ""));
    string rtDocs = "cmd=add,uid=ironman,friendid=23,value=24,region_name=region1;"
                    "cmd=add,uid=woman,friendid=1,value=25,region_name=region2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "", ""));

    ASSERT_TRUE(
        psm.Transfer(QUERY, "", "region1@ironman", "value=24,friendid=23;value=14,friendid=13;value=4,friendid=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@loki", "value=17,friendid=16"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@superman", "value=3,friendid=1;value=1,friendid=0"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@1", "value=25,uid=woman;value=18,uid=thor;value=2,uid=batman"));
}

void KKVReaderTest::CheckResult(const KKVReaderPtr& kkvReader, const string& key, const string& valueResult)
{
    vector<vector<uint64_t>> resultInfos;
    StringUtil::fromString(valueResult, resultInfos, ",", ";");

    future_lite::executors::SimpleExecutor ex(1);
    Pool pool;
    StringView pkeyStr(key.data(), key.size());
    auto iter = future_lite::interface::syncAwait(kkvReader->LookupAsync(pkeyStr, 0, tsc_default, &pool, nullptr), &ex);
    size_t cursor = 0;
    while (iter->IsValid()) {
        assert(resultInfos[cursor].size() == 2);
        ASSERT_EQ(resultInfos[cursor][0], iter->GetCurrentSkey());

        StringView value;
        iter->GetCurrentValue(value);
        ASSERT_EQ(resultInfos[cursor][1], *(uint32_t*)value.data());

        ++cursor;
        iter->MoveToNext();
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, iter);
}

void KKVReaderTest::TestBatchLookup()
{
    /*  TODO: test when barch lookup is available
    string field = "pkey:string;skey:int32;value:uint32;";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig,
            indexSchema->GetPrimaryKeyIndexConfig());
    KKVIndexPreference::HashDictParam param(PrefixKeyTableType2Str(PKT_CUCKOO));
    kkvConfig->GetIndexPreference().SetHashDictParam(param);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
    SearchCachePtr searchCache(new SearchCache(40960,
                    resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
    resource.searchCache.reset(new SearchCachePartitionWrapper(searchCache, ""));
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102;"
                       "cmd=add,pkey=pkey3,skey=3,value=3,ts=103";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString,
                             "pkey1", "skey=1,value=1;skey=2,value=2"));

    KKVReadOptions kkvReaderOption;
    auto kkvReader = psm.GetIndexPartition()->GetReader()->GetKKVReader();
    KVMetricsCollector collector;
    vector<KKVIterator*> kkvIterator = kkvReader->BatchLookup(
            {StringView("pkey1"), StringView("pkey2")},
            {{StringView("1")}, {StringView("2")}}, kkvReaderOption);
    PackAttributeFormatterPtr packAttrFormatter(new PackAttributeFormatter);
    ValueConfigPtr valueConfig = kkvConfig->GetValueConfig();
    ASSERT_TRUE(packAttrFormatter->Init(valueConfig->CreatePackAttributeConfig()));
    auto* valueRef = packAttrFormatter->GetAttributeReferenceTyped<uint32_t>("value");
    StringView rawValue;
    uint32_t value;
    ASSERT_TRUE(kkvIterator[0]);
    ASSERT_TRUE(kkvIterator[0]->IsValid());
    kkvIterator[0]->GetCurrentValue(rawValue);
    valueRef->GetValue(rawValue.data(), value);
    ASSERT_EQ(1, kkvIterator[0]->GetCurrentSkey());
    ASSERT_EQ(1, value);
    kkvIterator[0]->MoveToNext();
    ASSERT_FALSE(kkvIterator[0]->IsValid());
    kkvIterator[0]->Finish();

    ASSERT_TRUE(kkvIterator[1]);
    ASSERT_FALSE(kkvIterator[1]->IsValid());
    */
}
}} // namespace indexlib::index
