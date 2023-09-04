#include "indexlib/index/kkv/test/kkv_data_collector_unittest.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/test/region_schema_maker.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KKVDataCollectorTest);

KKVDataCollectorTest::KKVDataCollectorTest() : mPool(NULL) {}

KKVDataCollectorTest::~KKVDataCollectorTest() {}

void KKVDataCollectorTest::CaseSetUp()
{
    DELETE_AND_SET_NULL(mPool);
    mPool = new autil::mem_pool::Pool();
}

void KKVDataCollectorTest::CaseTearDown() { DELETE_AND_SET_NULL(mPool); }

void KKVDataCollectorTest::TestMultiRegion()
{
    string field = "uid:string;friendid:int32;value:uint32;";
    string field1 = "uid:string;friendid:int32;value:string;";
    string field2 = "uid:string;friendid:int32;value:uint32;";
    RegionSchemaMaker maker(field, "kkv");
    maker.AddRegion("region1", "uid", "friendid", "value;friendid;", field1);
    maker.AddRegion("region2", "friendid", "uid", "value;uid;", field2);
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

    KKVIndexConfigPtr kkvIndexConfig = CreateKKVIndexConfigForMultiRegionData(schema);
    { // test offline build will not trigger sort or truncate
        tearDown();
        setUp();
        file_system::DirectoryPtr directory = MAKE_SEGMENT_DIRECTORY(0);
        auto indexDir = directory->MakeDirectory(INDEX_DIR_NAME);
        auto kkvDir = indexDir->MakeDirectory(kkvIndexConfig->GetIndexName());

        KKVDataCollector<uint64_t> dataCollector(schema, kkvIndexConfig, true);
        dataCollector.Init(kkvDir, mPool, 1024, DCP_BUILD_DUMP);
        ASSERT_EQ(2u, dataCollector.mCollectProcessors.size());

        auto typedCProcessor0 = DYNAMIC_POINTER_CAST(CollectInfoProcessor, dataCollector.mCollectProcessors[0]);
        auto typedCProcessor1 = DYNAMIC_POINTER_CAST(CollectInfoProcessor, dataCollector.mCollectProcessors[1]);
        ASSERT_TRUE(typedCProcessor0.operator bool());
        ASSERT_TRUE(typedCProcessor1.operator bool());
        dataCollector.Close();
        KKVIndexFormat indexFormat;
        indexFormat.Load(kkvDir);
        ASSERT_TRUE(indexFormat.StoreTs());
        ASSERT_FALSE(indexFormat.KeepSortSequence());
    }
    { // test online build will always trigger truncate
        tearDown();
        setUp();
        file_system::DirectoryPtr directory = MAKE_SEGMENT_DIRECTORY(0);
        auto indexDir = directory->MakeDirectory(INDEX_DIR_NAME);
        auto kkvDir = indexDir->MakeDirectory(kkvIndexConfig->GetIndexName());
        KKVDataCollector<uint64_t> dataCollector(schema, kkvIndexConfig, false);
        dataCollector.Init(kkvDir, mPool, 1024, DCP_RT_BUILD_DUMP);
        ASSERT_EQ(2u, dataCollector.mCollectProcessors.size());

        auto typedCProcessor0 = DYNAMIC_POINTER_CAST(TruncateCollectInfoProcessor, dataCollector.mCollectProcessors[0]);
        auto typedCProcessor1 = DYNAMIC_POINTER_CAST(TruncateCollectInfoProcessor, dataCollector.mCollectProcessors[1]);
        ASSERT_TRUE(typedCProcessor0.operator bool());
        ASSERT_TRUE(typedCProcessor1.operator bool());

        ASSERT_EQ(5u, typedCProcessor0->mSKeyCountLimits);
        ASSERT_EQ(10u, typedCProcessor1->mSKeyCountLimits);
        ASSERT_EQ(ft_int32, typedCProcessor0->mSkeyFieldType);
        ASSERT_EQ(ft_uint64, typedCProcessor1->mSkeyFieldType);
        dataCollector.Close();
        KKVIndexFormat indexFormat;
        indexFormat.Load(kkvDir);
        ASSERT_FALSE(indexFormat.StoreTs());
        ASSERT_FALSE(indexFormat.KeepSortSequence());
    }
    { // test merge bottom level will trigger truncate or sort
        tearDown();
        setUp();
        file_system::DirectoryPtr directory = MAKE_SEGMENT_DIRECTORY(0);
        auto indexDir = directory->MakeDirectory(INDEX_DIR_NAME);
        auto kkvDir = indexDir->MakeDirectory(kkvIndexConfig->GetIndexName());
        KKVDataCollector<uint64_t> dataCollector(schema, kkvIndexConfig, true);
        dataCollector.Init(kkvDir, mPool, 1024, DCP_MERGE_BOTTOMLEVEL);
        ASSERT_EQ(2u, dataCollector.mCollectProcessors.size());
        auto typedCProcessor0 = DYNAMIC_POINTER_CAST(TruncateCollectInfoProcessor, dataCollector.mCollectProcessors[0]);
        auto typedCProcessor1 = DYNAMIC_POINTER_CAST(SortCollectInfoProcessor, dataCollector.mCollectProcessors[1]);
        ASSERT_TRUE(typedCProcessor0.operator bool());
        ASSERT_TRUE(typedCProcessor1.operator bool());
        ASSERT_EQ(5u, typedCProcessor0->mSKeyCountLimits);
        ASSERT_EQ(10u, typedCProcessor1->mSKeyCountLimits);
        ASSERT_EQ(ft_int32, typedCProcessor0->mSkeyFieldType);
        ASSERT_EQ(ft_uint64, typedCProcessor1->mSkeyFieldType);
        dataCollector.Close();
        KKVIndexFormat indexFormat;
        indexFormat.Load(kkvDir);
        ASSERT_TRUE(indexFormat.StoreTs());
        ASSERT_TRUE(indexFormat.KeepSortSequence());
    }
    { // test inc merge  will trigger truncate only
        tearDown();
        setUp();
        file_system::DirectoryPtr directory = MAKE_SEGMENT_DIRECTORY(0);
        auto indexDir = directory->MakeDirectory(INDEX_DIR_NAME);
        auto kkvDir = indexDir->MakeDirectory(kkvIndexConfig->GetIndexName());
        KKVDataCollector<uint64_t> dataCollector(schema, kkvIndexConfig, true);
        dataCollector.Init(kkvDir, mPool, 1024, DCP_MERGE);
        ASSERT_EQ(2u, dataCollector.mCollectProcessors.size());
        auto typedCProcessor0 = DYNAMIC_POINTER_CAST(TruncateCollectInfoProcessor, dataCollector.mCollectProcessors[0]);
        auto typedCProcessor1 = DYNAMIC_POINTER_CAST(TruncateCollectInfoProcessor, dataCollector.mCollectProcessors[1]);
        ASSERT_TRUE(typedCProcessor0.operator bool());
        ASSERT_TRUE(typedCProcessor1.operator bool());
        ASSERT_EQ(5u, typedCProcessor0->mSKeyCountLimits);
        ASSERT_EQ(10u, typedCProcessor1->mSKeyCountLimits);
        ASSERT_EQ(ft_int32, typedCProcessor0->mSkeyFieldType);
        ASSERT_EQ(ft_uint64, typedCProcessor1->mSkeyFieldType);
        dataCollector.Close();
        KKVIndexFormat indexFormat;
        indexFormat.Load(kkvDir);
        ASSERT_TRUE(indexFormat.StoreTs());
        ASSERT_FALSE(indexFormat.KeepSortSequence());
    }
}
}} // namespace indexlib::index
