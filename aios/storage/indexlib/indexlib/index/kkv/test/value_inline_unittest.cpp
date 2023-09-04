#include "indexlib/index/kkv/test/value_inline_unittest.h"

#include "future_lite/CoroInterface.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/kkv/kkv_reader.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::util;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, ValueInlineTest);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(ValueInlineTestMode, ValueInlineTest, testing::Values(true, false));

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(ValueInlineTestSimple, ValueInlineTest, testing::Values(true));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ValueInlineTestMode, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ValueInlineTestMode, TestSimpleProcessWithTs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ValueInlineTestMode, TestFullIncRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ValueInlineTestSimple, TestMultiChunk);

ValueInlineTest::ValueInlineTest() {}

ValueInlineTest::~ValueInlineTest() {}

void ValueInlineTest::CaseSetUp()
{
    bool fixedValueLen = GET_CASE_PARAM();
    string field;
    if (fixedValueLen) {
        field = "pkey:uint64;skey:uint32;value:uint32;";
    } else {
        field = "pkey:uint64;skey:uint32;value:string;";
    }

    mSchema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    auto kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    kkvConfig->GetIndexPreference().TEST_SetValueInline(true);
}

void ValueInlineTest::CaseTearDown() {}

void ValueInlineTest::TestSimpleProcess()
{
    string docString = "cmd=add,pkey=1,skey=1,value=1,ts=1000000;";
    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    options.GetOfflineConfig().fullIndexStoreKKVTs = false;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    auto partData = psm.GetIndexPartition()->GetPartitionData();
    auto iter = partData->Begin();
    ASSERT_TRUE(iter != partData->End());
    for (; iter != partData->End(); ++iter) {
        auto indexDir = iter->GetIndexDirectory("pkey", true);
        ASSERT_TRUE(indexDir->IsExist(SUFFIX_KEY_FILE_NAME));
        ASSERT_FALSE(indexDir->IsExist(KKV_VALUE_FILE_NAME));
    }
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000"));
}

void ValueInlineTest::TestSimpleProcessWithTs()
{
    std::cout << "env:";
    std::cout << getenv("IS_INDEXLIB_TEST_MODE") << std::endl;

    string docString = "cmd=add,pkey=1,skey=1,value=1,ts=1000000;";
    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    options.GetOfflineConfig().fullIndexStoreKKVTs = true;
    std::cout << "mem use:" << options.GetOfflineConfig().buildConfig.buildTotalMemory << std::endl;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    auto partData = psm.GetIndexPartition()->GetPartitionData();
    auto iter = partData->Begin();
    ASSERT_TRUE(iter != partData->End());
    for (; iter != partData->End(); ++iter) {
        auto indexDir = iter->GetIndexDirectory("pkey", true);
        ASSERT_TRUE(indexDir->IsExist(SUFFIX_KEY_FILE_NAME));
        ASSERT_FALSE(indexDir->IsExist(KKV_VALUE_FILE_NAME));
    }
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000"));
}

void ValueInlineTest::TestFullIncRt()
{
    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    options.GetOfflineConfig().fullIndexStoreKKVTs = true;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));

    string fullDocs = "cmd=add,pkey=1,skey=1,value=1,ts=1000000;"
                      "cmd=add,pkey=1,skey=2,value=2,ts=1000000;"
                      "cmd=add,pkey=100,skey=1,value=1,ts=1000000;"
                      "cmd=add,pkey=100,skey=2,value=2,ts=1000000;";

    string incDocs = "cmd=add,pkey=1,skey=11,value=11,ts=1000001;"
                     "cmd=add,pkey=100,skey=11,value=11,ts=1000001;";

    string rtDocs = "cmd=add,pkey=1,skey=21,value=21,ts=1000002;"
                    "cmd=add,pkey=100,skey=21,value=21,ts=1000002;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "1", "skey=1,value=1;skey=2,value=2"));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "1", "skey=1,value=1;skey=2,value=2;skey=11,value=11"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "1", "skey=1,value=1;skey=2,value=2;skey=11,value=11;skey=21,value=21"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "100", "skey=1,value=1;skey=2,value=2;skey=11,value=11;skey=21,value=21"));
}

void ValueInlineTest::TestMultiChunk()
{
    string jsonStr = R"({ "load_config" :
        [
             { "file_patterns" : ["_KKV_PKEY_"],
               "load_strategy" : "mmap"
             },
             { "file_patterns" : ["_KKV_SKEY_", "_KKV_VALUE_"],
               "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }
             }
        ]
    })";

    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    options.GetOfflineConfig().fullIndexStoreKKVTs = true;
    options.GetOnlineConfig().loadConfigList =
        file_system::LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));

    auto& globalFileBlockCache = psm.GetIndexPartitionResource().fileBlockCacheContainer;
    globalFileBlockCache.reset(new file_system::FileBlockCacheContainer());
    globalFileBlockCache->Init("cache_size=20;", util::MemoryQuotaControllerCreator::CreateMemoryQuotaController());

    auto genDocString = [](const string& pkey, size_t docCount) {
        string docString;
        for (size_t i = 0; i < docCount; ++i) {
            string docIdStr = std::to_string(i);
            docString += "cmd=add,pkey=" + pkey + ",skey=" + docIdStr + ",value=" + docIdStr + ",ts=101;";
        }
        return docString;
    };
    const uint64_t skeyCount = 400;
    string fullDocs = genDocString("1", skeyCount) + genDocString("2", skeyCount) + genDocString("3", skeyCount * 2);
    ;
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));
    auto kkvReader = psm.GetIndexPartition()->GetReader()->GetKKVReader();
    KVMetricsCollector collector;

    future_lite::executors::SimpleExecutor ex(1);
    auto kkvDocIter = future_lite::interface::syncAwait(
        kkvReader->LookupAsync(autil::StringView("1"), 0, tsc_default, &mPool, &collector), &ex);

    uint64_t idx = 0;
    while (kkvDocIter->IsValid()) {
        auto skey = kkvDocIter->GetCurrentSkey();
        ASSERT_EQ(idx, skey);
        kkvDocIter->MoveToNext();
        ++idx;
    }
    ASSERT_EQ(skeyCount, idx);
    kkvDocIter->Finish();
    IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, kkvDocIter);

    // TODO: do not hardcode
    // FIXME: refactor
    EXPECT_EQ(6, collector.GetBlockCacheHitCount()); // 2 chunk meta, 2 chunk(cross block) * 2
    EXPECT_EQ(1, collector.GetBlockCacheIOCount());
    EXPECT_EQ(3 * 4096, collector.GetBlockCacheIODataSize()); // 3 blocks
    EXPECT_EQ(2, collector.GetSKeyChunkCountInBlocks());      // 2 chunks
    EXPECT_EQ(5200, collector.GetSKeyDataSizeInBlocks());     // (4 + 4 + 4 + 1) * 400
    EXPECT_EQ(0, collector.GetValueDataSizeInBlocks());
}
}} // namespace indexlib::index
