#include "indexlib/partition/test/kv_table_intetest.h"

#include <errno.h>

#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/codegen/code_factory.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/config/impl/merge_config_impl.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/DeployIndexMeta.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/fslib/MultiPathDataFlushController.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/kv/kv_factory.h"
#include "indexlib/index/kv/kv_format_options.h"
#include "indexlib/index/kv/kv_reader.h"
#include "indexlib/index/kv/kv_segment_iterator.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/merger/multi_partition_merger.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/slow_dump_segment_container.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/cache/BlockCache.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace autil;

using namespace indexlib::test;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::merger;
using namespace indexlib::index_base;
using namespace indexlib::codegen;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, KVTableInteTest);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(KVTableInteTestMode, KVTableInteTest, testing::Values(false, true));

INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestBuildReadWithDiffType);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestNumberHash);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestSearchWithTimestamp);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestSearchWithTimestampForDefalutTTL);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestSearchWithMultiSegment);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestSearchKVWithDelete);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestAddSameDocs);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestAddKVWithoutValue);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestBuildShardingSegment);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestBuildLegacyKvDoc);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeTwoSegmentsSimple);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeSingleSegment);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeSameKey);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeDeleteKey);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeOptimizeSingleSegmentWithSharding);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestNoFullBuildData);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMultiPartitionMerge);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeWithSeparateMode);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeWithEmptyColumn);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestOptimizeMergeMultipleLevel);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeWithTTL);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeWithUserTs);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeWithDefaultTTL);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeWithTTLWhenAllDead);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeUntilBottomLevel);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestShardBasedMergeWithBadDocs);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestHashTableOneField);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestAddTrigerDump);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestReopenIncWithoutRt);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestReopenIncOlderThanAllRt);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestReopenIncCoverPartRtBuiltSegment);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestReopenIncCoverPartRtBuildingSegment);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestReopenIncCoverAllRt);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestReopenIncKeyAndRtKeyInSameSecond);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestForceReopenIncWithoutRt);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestForceReopenIncOlderThanAllRt);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestForceReopenIncCoverPartRtBuiltSegment);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestForceReopenIncCoverPartRtBuildingSegment);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestForceReopenIncCoverAllRt);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestReopenIncCoverBuiltAndBuildingSegment);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestForceReopenIncKeyAndRtKeyInSameSecond);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestOpenWithoutEnoughMemory);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestReopenWithoutEnoughMemory);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestReopenIncCoverBuiltSegmentWhenReaderHold);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestKeyIsExitError);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestAsyncDumpWithIncReopen);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestRtLoadBuiltRtSegmentWithIncCoverCore);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestRtIndexSizeWhenUseMultiValue);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestProhibitInMemDumpBug_13765192);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestBugFix14177981);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestBuildFixLengthWithLegacyFormat);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestBuildFixLengthString);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestOffsetLimitTriggerForceDump);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestShortOffsetWithDeleteDoc);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestKVFormatOption);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestAdaptiveReadWithShortOffsetReader);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeWithShortOffset);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMultiFloatField);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeWithCompressAndShortOffsetIsDisable);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestPartitionInfo);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestFixLenValueAutoInline);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestCompactValueWithDenseHash);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestCompactValueWithCuckooHash);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestCompactValueWithDelete);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMergeNoBottomWithNoDelete);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestSingleFloatCompress);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestCompressFloatValueAutoInline);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestDumpingSearchCache);
INDEXLIB_UNIT_TEST_CASE(KVTableInteTest, TestMetric);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestHashTableOneStringField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestHashTableOneMultiField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestHashTableMultiField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestHashTableVarIndexMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestHashTableVarValueUseBlockCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestHashTableVarKeyUseBlockCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestHashTableFixUseBlockCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestHashTableFixVarValue);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestFlushRealtimeIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestCleanRealtimeIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestReleaseMemoryForTmpValueFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestCompactHashKeyForFixHashTable);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestCompactHashKeyForVarHashTable);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestShortOffset);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestLegacyFormat);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestCompressedFixedLengthFloatField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, testMergeMultiPartWithTTL);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KVTableInteTestMode, TestCompatableError);

KVTableInteTest::KVTableInteTest() {}

KVTableInteTest::~KVTableInteTest() {}

void KVTableInteTest::CaseSetUp()
{
    mImpactValue = (GET_TEST_NAME().size() % 2 == 0);
    mPlainFormat = (autil::HashAlgorithm::hashString64(GET_TEST_NAME().c_str()) % 2 == 0) && !mImpactValue;
    MultiPathDataFlushController::GetDataFlushController("")->Reset();

    mOptions = IndexPartitionOptions();
    mOptions.GetBuildConfig(true).buildTotalMemory = 22;
    mOptions.GetBuildConfig(false).buildTotalMemory = 40;
    mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    mOptions.GetBuildConfig(false).levelNum = 2;
    mOptions.GetBuildConfig(false).keepVersionCount = 100;
    mOptions.GetMergeConfig().mImpl->enablePackageFile = true;

    string field = "key:int32;value:uint64;";
    mSchema = CreateSchema(field, "key", "value");
}

void KVTableInteTest::CaseTearDown() {}

void KVTableInteTest::TestCompatableError()
{
    string indexPath = GET_PRIVATE_TEST_DATA_PATH() + "/compatable_error_index/partition_0_4095";
    IndexPartitionSchemaPtr schema;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, indexPath));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    // auto partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(psm.GetFileSystem());
    // schema = psm.GetSchema();
    // auto iter = partitionData->Begin();
    // for(; iter != partitionData->End(); iter++) {
    //     index::KVSegmentIterator segIter;
    //     KVFormatOptionsPtr kvOptions(new KVFormatOptions);
    //     segIter.Open(schema, kvOptions, *iter);
    //     keytype_t key;
    //     autil::StringView value;
    //     uint32_t timestamp;
    //     bool isDeleted;
    //     regionid_t regionId;
    //     while(segIter.IsValid()) {
    //         segIter.Get(key, value, timestamp, isDeleted, regionId);
    //         std::cout << "key:" << key << " value:" << value.toString() << std::endl;
    //         segIter.MoveToNext();
    //     }
    // }
    // ASSERT_TRUE(psm.Transfer(QUERY, "", "key2", "value=2"));
}

void KVTableInteTest::TestRtLoadBuiltRtSegmentWithIncCoverCore()
{
    IndexPartitionOptions options;
    options.GetOnlineConfig().loadRemainFlushRealtimeIndex = false;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetBuildConfig(false).buildTotalMemory = 50; // 50MB
    options.GetBuildConfig(true).buildTotalMemory = 50;  // 50MB
    string field = "key:string;value:int8";
    mSchema = CreateSchema(field, "key", "value");
    string docString = "cmd=add,key=key1,value=1,ts=101000000;"
                       "cmd=add,key=key2,value=2,ts=102000000;"
                       "cmd=add,key=key,value=3,ts=103000000;"
                       "cmd=add,key=key,value=31,ts=104000000";
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docString, "key1", "value=1"));
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, docString, "", ""));
    }
    {
        PartitionStateMachine psm1;
        ASSERT_TRUE(psm1.Init(mSchema, options, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm1.Transfer(BUILD_RT, "cmd=add,key=key,value=32,ts=105000000", "key1", "value=1"));
        ASSERT_TRUE(psm1.Transfer(BUILD_RT, "", "key", "value=32"));
    }
}

void KVTableInteTest::InnerTestBuildSearch(string fieldType, bool hasTTL, const IndexPartitionOptions& options)
{
    tearDown();
    setUp();
    IndexPartitionOptions optionsInUse = options;
    string field = "key:string;value:" + fieldType;
    mSchema = CreateSchema(field, "key", "value");
    mSchema->SetEnableTTL(hasTTL);
    optionsInUse.GetBuildConfig(true).buildTotalMemory = 22;
    optionsInUse.GetBuildConfig(false).buildTotalMemory = 22;

    string docString = "cmd=add,key=key1,value=1,ts=101000000;"
                       "cmd=add,key=key2,value=2,ts=102000000;"
                       "cmd=add,key=key,value=3,ts=103000000;"
                       "cmd=add,key=key,value=31,ts=104000000";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, optionsInUse, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "key1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "key2", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "key2", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "key", "value=31"));

    string rtDocString = "cmd=add,key=key3,value=3,ts=105000000;"
                         "cmd=add,key=key4,value=4,ts=106000000;"
                         "cmd=add,key=key,value=32,ts=107000000";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "key3", "value=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "key4", "value=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "key", "value=32"));
}

void KVTableInteTest::TestBuildReadWithDiffType()
{
    InnerTestBuildSearch("int8", true);
    InnerTestBuildSearch("int8", false);
    InnerTestBuildSearch("int16", true);
    InnerTestBuildSearch("int16", false);
    InnerTestBuildSearch("int32", true);
    InnerTestBuildSearch("int32", false);
    InnerTestBuildSearch("int64", true);
    InnerTestBuildSearch("int64", false);
    InnerTestBuildSearch("uint8", true);
    InnerTestBuildSearch("uint8", false);
    InnerTestBuildSearch("uint16", true);
    InnerTestBuildSearch("uint16", false);
    InnerTestBuildSearch("uint32", true);
    InnerTestBuildSearch("uint32", false);
    InnerTestBuildSearch("uint64", true);
    InnerTestBuildSearch("uint64", false);
    InnerTestBuildSearch("double", true);
    InnerTestBuildSearch("float", false);
}

void KVTableInteTest::TestNumberHash()
{
    string docString = "cmd=add,key=10,value=1,ts=101000000;"
                       "cmd=add,key=-10,value=2,ts=102000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "10", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "-10", "value=2"));
}

void KVTableInteTest::TestMetric()
{
    string docString = "cmd=add,key=10,value=1,ts=101000000;"
                       "cmd=add,key=-10,value=2,ts=102000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docString, "10", "value=1"));
    auto partition = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    ASSERT_TRUE(partition);
    ASSERT_NE(0, partition->mResourceCalculator->GetCurrentIndexSize());
}

void KVTableInteTest::TestSearchWithTimestamp()
{
    ASSERT_FALSE(mSchema->TTLEnabled());
    mSchema->SetEnableTTL(true);

    string docString = "cmd=add,key=1,value=1,ts=10000000;"
                       "cmd=add,key=2,value=2,ts=30000000";

    mOptions.GetOnlineConfig().ttl = 20;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    // test full build, search with ts
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#30", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#31", ""));

    // test rt cover inc segment, search with ts
    docString = "cmd=add,key=1,value=1,ts=40000000;"
                "cmd=add,key=4,value=4,ts=50000000";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#31", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#60", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#61", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2#50", "value=2"));
}

void KVTableInteTest::TestAsyncDumpWithIncReopen()
{
    ASSERT_FALSE(mSchema->TTLEnabled());
    mSchema->SetDefaultTTL(20);
    DumpSegmentContainerPtr dumpContainer(new SlowDumpSegmentContainer(500));
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
    PartitionStateMachine psm(false, IndexPartitionResource(), dumpContainer);
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

    string rtDocs = "cmd=add,key=1,value=1,ts=10000000;"
                    "cmd=add,key=2,value=2,ts=30000000";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "1", "value=1;"));

    ASSERT_EQ(1u, dumpContainer->GetDumpItemSize());
    rtDocs = "cmd=add,key=3,value=3,ts=30000000;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs, "3", "value=3;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "", "2", "value=2;"));
    string incDocs = "cmd=add,key=1,value=11,ts=11000000;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocs, "1", "value=11;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "", "3", "value=3;"));
    rtDocs = "cmd=add,key=4,value=4,ts=30000000;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs, "4", "value=4;"));
    ASSERT_EQ(0u, dumpContainer->GetDumpItemSize());
}

void KVTableInteTest::TestSearchWithTimestampForDefalutTTL()
{
    ASSERT_FALSE(mSchema->TTLEnabled());
    mSchema->SetDefaultTTL(20);

    string docString = "cmd=add,key=1,value=1,ts=10000000;"
                       "cmd=add,key=2,value=2,ts=30000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    const IndexPartitionSchemaPtr& runtimeSchema = psm.GetIndexPartition()->GetSchema();
    const IndexSchemaPtr& indexSchema = runtimeSchema->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(runtimeSchema->TTLEnabled());
    ASSERT_EQ(20L, kvConfig->GetTTL());

    // test full build, search with ts
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#30", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#31", ""));

    // test rt cover inc segment, search with ts
    docString = "cmd=add,key=1,value=1,ts=40000000;"
                "cmd=add,key=4,value=4,ts=50000000";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#31", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#60", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#61", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2#50", "value=2"));
}

void KVTableInteTest::TestSearchWithMultiSegment()
{
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;

    InnerTestBuildSearch("int8", false, mOptions);
    InnerTestBuildSearch("int8", true, mOptions);
}

void KVTableInteTest::TestSearchKVWithDelete()
{
    mSchema->SetEnableTTL(true);
    mOptions.GetBuildConfig(true).buildTotalMemory = 22;
    mOptions.GetBuildConfig(false).buildTotalMemory = 22;
    mOptions.GetBuildConfig(true).maxDocCount = 3;
    mOptions.GetBuildConfig(false).maxDocCount = 3;
    mOptions.GetOnlineConfig().ttl = 20;

    string docString = "cmd=delete,key=1,ts=10000000;"
                       "cmd=delete,key=2,ts=10000000;"
                       "cmd=delete,key=3,ts=10000000;"
                       "cmd=add,key=2,value=2,ts=10000000;"
                       "cmd=add,key=3,value=3,ts=10000000;"
                       "cmd=delete,key=3,ts=10000000;";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));

    string rtDocString = "cmd=delete,key=4,ts=10000001;"
                         "cmd=delete,key=5,ts=10000001;"
                         "cmd=delete,key=6,ts=10000001;"
                         "cmd=add,key=5,value=5,ts=10000001;"
                         "cmd=add,key=6,value=6,ts=10000001;"
                         "cmd=delete,key=6,ts=10000001;"
                         "cmd=add,key=3,value=33,ts=10000001;"
                         "cmd=delete,key=2,value=33,ts=10000001;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "5", "value=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "6", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=33"));
}

void KVTableInteTest::TestAddSameDocs()
{
    mSchema->SetEnableTTL(true);
    mOptions.GetBuildConfig(true).buildTotalMemory = 22;
    mOptions.GetBuildConfig(false).buildTotalMemory = 22;
    mOptions.GetBuildConfig(true).maxDocCount = 4;
    mOptions.GetBuildConfig(false).maxDocCount = 4;
    mOptions.GetOnlineConfig().ttl = 20;

    string docString = "cmd=add,key=1,value=1,ts=10000000;"
                       "cmd=add,key=1,value=11,ts=10000000;"
                       "cmd=add,key=2,value=2,ts=10000000;"
                       "cmd=add,key=3,value=3,ts=10000000;"
                       "cmd=add,key=2,value=22,ts=10000000;";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=11"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=22"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=3"));

    string rtDocString = "cmd=add,key=4,value=4,ts=10000001;"
                         "cmd=add,key=4,value=44,ts=10000001;"
                         "cmd=add,key=2,value=222,ts=10000001;"
                         "cmd=add,key=5,value=5,ts=10000001;"
                         "cmd=add,key=5,value=55,ts=10000001;"
                         "cmd=add,key=3,value=33,ts=10000001;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=44"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "5", "value=55"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=222"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=33"));
}

void KVTableInteTest::TestAddKVWithoutValue()
{
    ASSERT_FALSE(mSchema->TTLEnabled());
    mSchema->SetEnableTTL(true);

    string docString = "cmd=add,key=1,ts=10000000;"
                       "cmd=add,key=2,ts=30000000";

    mOptions.GetOnlineConfig().ttl = 20;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    // test full build, search with ts
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=0"));
}

void KVTableInteTest::TestBuildShardingSegment()
{
    IndexPartitionOptions options = mOptions;
    options.GetBuildConfig(false).shardingColumnNum = 2;

    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    IndexBuilder indexBuilder(GET_TEMP_DATA_PATH(), options, mSchema, quotaControl,
                              BuilderBranchHinter::Option::Test());
    indexBuilder.Init();

    string docString = "cmd=add,key=1,value=1,ts=10000000;"
                       "cmd=add,key=4,value=11,ts=10000000;"
                       "cmd=add,key=2,value=2,ts=10000000;"
                       "cmd=add,key=3,value=3,ts=10000000;"
                       "cmd=add,key=5,value=22,ts=10000000;";
    auto docs = CreateKVDocuments(mSchema, docString);
    for (size_t i = 0; i < docs.size(); i++) {
        indexBuilder.Build(docs[i]);
    }
    indexBuilder.EndIndex();
    indexBuilder.TEST_BranchFSMoveToMainRoad();

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    OnDiskPartitionDataPtr onDiskPartData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM(), mSchema);
    DirectoryPtr seg0 = onDiskPartData->GetSegmentData(0).GetDirectory();
    ASSERT_TRUE(seg0);

    DirectoryPtr sharding0Dir = seg0->GetDirectory("column_0", true);
    ASSERT_TRUE(sharding0Dir->IsExist("index/key/key"));

    DirectoryPtr sharding1Dir = seg0->GetDirectory("column_1", true);
    ASSERT_TRUE(sharding1Dir->IsExist("index/key/key"));

    // check segment info
    SegmentInfo segInfo;
    segInfo.Load(seg0);
    ASSERT_EQ((uint32_t)2, segInfo.shardCount);
    ASSERT_EQ((uint64_t)5, segInfo.docCount);
    ASSERT_EQ((int64_t)10000000, segInfo.timestamp);

    OnlinePartition onlinePartition;
    ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, onlinePartition.Open(GET_TEMP_DATA_PATH(), "", mSchema, mOptions));
}

void KVTableInteTest::TestBuildLegacyKvDoc() {}
// {
//     mOptions.GetBuildConfig(true).maxDocCount = 1;
//     mOptions.GetBuildConfig(false).maxDocCount = 1;
//     mOptions.GetBuildConfig(false).ttl = 10;

//     string field = "key:int32;value:uint64:true;";
//     IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
//     schema->SetEnableTTL(true);

//     // discard doc: TTL + TS < NOW
//     string docString = "cmd=add,key=10,value=1 2,ts=50000000;"
//                        "cmd=add,key=-10,value=2 3 4,ts=60000000";
//     PartitionStateMachine psm;
//     psm.SetCurrentTimestamp(70000000);
//     psm.SetPsmOption("legacyKvDocFormat", "true");
//     ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
//     ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
//     CheckMergeResult(psm, 2, "1", "1");
//     ASSERT_TRUE(psm.Transfer(QUERY, "", "10", ""));
//     ASSERT_TRUE(psm.Transfer(QUERY, "", "-10", "value=2 3 4"));
// }

void KVTableInteTest::TestMergeTwoSegmentsSimple()
{
    PartitionStateMachine psm;
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key=1,value=1,ts=101;"
                       "cmd=add,key=2,value=2,ts=102;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "1", "value=1"));

    CheckMergeResult(psm, 1, "2", "2");

    // check deploy_index && deploy_meta
    auto checkSegmentDeployIndex = [this](const string& segDir) {
        DeployIndexMeta deployIndex;
        ASSERT_TRUE(deployIndex.Load(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), segDir + "/deploy_index")).OK());
        for (auto dpFile : deployIndex.deployFileMetas) {
            if (!dpFile.isDirectory()) {
                ASSERT_TRUE(dpFile.fileLength > 0);
            }
        }
    };
    checkSegmentDeployIndex("segment_0_level_0");
    checkSegmentDeployIndex("segment_1_level_0");
    checkSegmentDeployIndex("segment_2_level_1");
    auto checkDeployMeta = [this](const string& fileName) {
        DeployIndexMeta deployMeta;
        ASSERT_TRUE(deployMeta.Load(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), fileName)).OK());
        for (auto dpFile : deployMeta.deployFileMetas) {
            if (!dpFile.isDirectory() && dpFile.filePath != fileName) {
                ASSERT_TRUE(dpFile.fileLength > 0);
            }
        }
    };
    checkDeployMeta("deploy_meta.0");
    checkDeployMeta("deploy_meta.1");

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));
}

void KVTableInteTest::TestMergeSingleSegment()
{
    PartitionStateMachine psm;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key=1,value=1,ts=101;"
                       "cmd=add,key=2,value=2,ts=102;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "1", "value=1"));
    CheckMergeResult(psm, 1, "2", "2");

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));
}

void KVTableInteTest::TestMergeOptimizeSingleSegmentWithSharding()
{
    PartitionStateMachine psm;

    BuildConfig& buildConfig = mOptions.GetBuildConfig(false);

    buildConfig.shardingColumnNum = 2;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key=1,value=1,ts=101;"
                       "cmd=add,key=2,value=2,ts=102;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    CheckMergeResult(psm, 1, "1;1", "1;1", {{}, {1, 2}});

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));

    string rtDosStr = "cmd=add,key=3,value=3,ts=1000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDosStr, "3", "value=3"));
}

void KVTableInteTest::TestMergeSameKey()
{
    PartitionStateMachine psm;
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key=1,value=1,ts=101;"
                       "cmd=add,key=1,value=2,ts=102;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    CheckMergeResult(psm, 1, "1", "1");
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=2"));
}

void KVTableInteTest::TestMergeDeleteKey()
{
    PartitionStateMachine psm;
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key=1,value=1,ts=101;"
                       "cmd=delete,key=1,ts=102;"
                       "cmd=add,key=2,value=2,ts=103";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    CheckMergeResult(psm, 1, "1;", "1;");
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "")); // bottomlevel merge remove it
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));
}

void KVTableInteTest::TestNoFullBuildData()
{
    PartitionStateMachine psm;

    BuildConfig& buildConfig = mOptions.GetBuildConfig(false);
    buildConfig.shardingColumnNum = 2;
    buildConfig.keepVersionCount = 100;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));

    CheckMergeResult(psm, 0, "", "", {{}, {-1, -1}});
    string docString = "cmd=add,key=1,value=1,ts=101;"
                       "cmd=add,key=2,value=2,ts=102;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, docString, "", ""));
    // uncomment after reopen support kv table
    // CheckMergeResult(psm, 1, "1:1", "1:1", {{}, {1, 2}});
    // ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    // ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));
}

void KVTableInteTest::TestMultiPartitionMerge()
{
    MakeOnePartitionData("part1", "cmd=add,key=1,value=1,ts=104;");
    MakeOnePartitionData("part2", "cmd=add,key=2,value=2,ts=102;");
    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part2");

    GET_CHECK_DIRECTORY(false)->MakeDirectory("mergePart");
    string mergePartPath = GET_TEMP_DATA_PATH() + "mergePart";

    mOptions.SetIsOnline(false);
    MultiPartitionMerger multiPartMerger(mOptions, NULL, "", CommonBranchHinterOption::Test());
    multiPartMerger.Merge(mergeSrcs, mergePartPath);

    PartitionStateMachine psm;
    psm.Init(mSchema, mOptions, mergePartPath);

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));

    // bug fix: new version timestamp should large than old one
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
}

void KVTableInteTest::TestMergeWithSeparateMode()
{
    RESET_FILE_SYSTEM("ut", false, FileSystemOptions::Offline());
    string rootDir = GET_TEMP_DATA_PATH();
    mOptions.GetBuildConfig(false).shardingColumnNum = 2;
    {
        // prepare index
        PartitionStateMachine psm;

        ASSERT_TRUE(psm.Init(mSchema, mOptions, rootDir));
        string docString = "cmd=add,key=1,value=1,ts=101;"
                           "cmd=add,key=2,value=2,ts=102;";
        ASSERT_TRUE(psm.Transfer(PE_BUILD_FULL, docString, "", ""));
    }
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    // merge
    Version version;
    VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, INVALID_VERSIONID);
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(false, false);
    string mergeMetaDir = rootDir + "/merge_meta_dir";
    {
        IndexPartitionMerger merger(segDir, mSchema, mOptions, DumpStrategyPtr(), NULL, plugin::PluginManagerPtr(),
                                    CommonBranchHinterOption::Test());
        MergeMetaPtr mergeMeta = merger.CreateMergeMeta(false, 2, 0);
        mergeMeta->Store(mergeMetaDir);
    }
    {
        IndexPartitionMerger merger(segDir, mSchema, mOptions, DumpStrategyPtr(), NULL, plugin::PluginManagerPtr(),
                                    CommonBranchHinterOption::Test());
        MergeMetaPtr mergeMeta(new IndexMergeMeta());
        mergeMeta->Load(mergeMetaDir);
        merger.DoMerge(false, mergeMeta, 0);
        merger.DoMerge(false, mergeMeta, 1);
    }
    {
        IndexPartitionMerger merger(segDir, mSchema, mOptions, DumpStrategyPtr(), NULL, plugin::PluginManagerPtr(),
                                    CommonBranchHinterOption::Test());
        MergeMetaPtr mergeMeta(new IndexMergeMeta());
        mergeMeta->LoadBasicInfo(mergeMetaDir);
        merger.EndMerge(mergeMeta);
    }
    // check result
    PartitionStateMachine psm;
    psm.Init(mSchema, mOptions, rootDir);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));

    CheckMergeResult(psm, 1, "1;1", "1;1", {{}, {1, 2}});
}

void KVTableInteTest::TestMergeWithEmptyColumn()
{
    PartitionStateMachine psm;

    BuildConfig& buildConfig = mOptions.GetBuildConfig(false);
    buildConfig.shardingColumnNum = 2;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key=1,value=1,ts=101;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    CheckMergeResult(psm, 1, "0;1", "0;1", {{}, {1, 2}});

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
}

void KVTableInteTest::TestKeyIsExitError()
{
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;

    string field = "key:uint64;uint32:uint32;mint8:int8:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "key;uint32;mint8");

    {
        string docString = "cmd=add,key=10,uint32=10,mint8=1 2,ts=101000000;"
                           "cmd=add,key=0,uint32=0,mint8=,ts=102000000;";
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

        IndexPartitionPtr indexPart = psm.GetIndexPartition();
        DirectoryPtr rootDir = indexPart->GetRootDirectory();
        IFileSystemPtr fileSystem = rootDir->GetFileSystem();

        string rtString = "cmd=add,key=0,uint32=1,mint8=2 3,ts=104000000;"
                          "cmd=add,key=5,uint32=5,mint8=3,ts=105000000;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtString, "", ""));
        fileSystem->Sync(true).GetOrThrow();
    }

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "5", "uint32=5"));

        string incString1 = "cmd=add,key=0,uint32=1,mint8=2 3,ts=104000000;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incString1, "", ""));

        string incString2 = "cmd=add,key=0,uint32=1,mint8=2 3,ts=104000000;"
                            "cmd=add,key=5,uint32=5,mint8=3,ts=105000000;"
                            "cmd=add,key=5,uint32=10,mint8=3,ts=106000000;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incString2, "", ""));
    }
}

void KVTableInteTest::TestFlushRealtimeIndex()
{
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().prohibitInMemDump = true;
    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "mmap" },
         { "file_patterns" : ["_KV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true, "cache_decompress_file" : true }}]
    })";

    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    mOptions.GetBuildConfig(false).maxDocCount = 1;
    mOptions.GetBuildConfig(true).maxDocCount = 10;

    string field = "key:uint64;uint32:uint32;mint8:int8:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "key;uint32;mint8");
    if (GET_CASE_PARAM()) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        KVIndexPreference::ValueParam valueParam(false, "zstd");
        valueParam.EnableValueImpact(kvConfig->GetIndexPreference().GetValueParam().IsValueImpact());
        valueParam.EnablePlainFormat(kvConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
        kvConfig->GetIndexPreference().SetValueParam(valueParam);
        mOptions.SetEnablePackageFile(true);
    }

    string docString = "cmd=add,key=10,uint32=10,mint8=1 2,ts=101000000;"
                       "cmd=add,key=0,uint32=0,mint8=,ts=102000000;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", "key=10,uint32=10,mint8=1 2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "0", "key=0,uint32=0,mint8="));

    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    DirectoryPtr rootDir = indexPart->GetRootDirectory();
    IFileSystemPtr fileSystem = rootDir->GetFileSystem();

    StorageMetrics localMetrics = fileSystem->GetFileSystemMetrics().GetInputStorageMetrics();

    string rtString = "cmd=add,key=0,uint32=1,mint8=2 3,ts=104000000;"
                      "cmd=add,key=5,uint32=5,mint8=3,ts=105000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtString, "", ""));
    fileSystem->Sync(true).GetOrThrow();

    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    fileSystem->CleanCache();

    StorageMetrics localMetrics1 = fileSystem->GetFileSystemMetrics().GetInputStorageMetrics();
    ASSERT_GT(localMetrics1.GetTotalFileLength(), localMetrics.GetTotalFileLength());
    ASSERT_GT(localMetrics1.GetTotalFileCount(), localMetrics.GetTotalFileCount());

    ASSERT_TRUE(psm.Transfer(QUERY, "", "0", "key=0,uint32=1,mint8=2 3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "5", "key=5,uint32=5,mint8=3"));

    ASSERT_TRUE(indexPart->GetReader()->GetPartitionVersion()->GetLastLinkRtSegmentId() != INVALID_SEGMENTID);

    string rtSegPath = rootDir->GetRootLinkPath() + "/rt_index_partition/segment_1073741824_level_0";
    string keyPath = rtSegPath + "/index/key/key";
    string valuePath = rtSegPath + "/index/key/value";

    FileStat fileStat;
    ASSERT_EQ(FSEC_OK, fileSystem->TEST_GetFileStat(keyPath, fileStat));
    ASSERT_TRUE(fileStat.inCache);
    ASSERT_EQ(FSOT_LOAD_CONFIG, fileStat.openType);
    ASSERT_NE(FSFT_MEM, fileStat.fileType);

    ASSERT_EQ(FSEC_OK, fileSystem->TEST_GetFileStat(valuePath, fileStat));
    ASSERT_EQ(FSOT_LOAD_CONFIG, fileStat.openType);
    ASSERT_NE(FSFT_MEM, fileStat.fileType);

    ASSERT_TRUE(psm.CreateOnlinePartition(-1));
}

void KVTableInteTest::CheckMergeResult(const PartitionStateMachine& psm, versionid_t versionId, const string& docCount,
                                       const string& keyCount, const vector<vector<segmentid_t>> levelInfo)
{
    const IndexPartitionPtr& indexPartition = psm.GetIndexPartition();
    assert(indexPartition);
    const IndexPartitionReaderPtr& reader = indexPartition->GetReader();
    Version version = reader->GetVersion();
    ASSERT_EQ(versionId, version.GetVersionId());
    PartitionDataPtr partData = reader->GetPartitionData();
    vector<size_t> docCountVec;
    vector<size_t> keyCountVec;
    autil::StringUtil::fromString(docCount, docCountVec, ";");
    autil::StringUtil::fromString(keyCount, keyCountVec, ";");
    assert(keyCountVec.size() == docCountVec.size());
    size_t cnt = 0;
    for (auto iter = partData->Begin(); iter != partData->End(); ++iter) {
        ASSERT_EQ(docCountVec[cnt], iter->GetSegmentInfo()->docCount);
        // const file_system::DirectoryPtr& indexDir = iter->GetIndexDirectory(true);
        // SegmentMetrics segmentMetrics;
        // segmentMetrics.Load(indexDir);
        // uint64_t key_count = 0;
        // string groupName = SegmentWriter::GetMetricsGroupName(
        //         iter->GetSegmentInfo()->shardId);
        // ASSERT_TRUE(segmentMetrics.Get(groupName, "key_count", key_count));
        // ASSERT_EQ(keyCountVec[cnt], key_count);
        ASSERT_TRUE(iter->GetSegmentInfo()->mergedSegment);
        ++cnt;
    }
    ASSERT_EQ(keyCountVec.size(), cnt);
    if (!levelInfo.empty()) {
        const indexlibv2::framework::LevelInfo& actualLevelInfo = version.GetLevelInfo();
        ASSERT_EQ(levelInfo.size(), actualLevelInfo.GetLevelCount());
        for (size_t i = 0; i < levelInfo.size(); ++i) {
            const indexlibv2::framework::LevelMeta& levelMeta = actualLevelInfo.levelMetas[i];
            ASSERT_EQ(levelInfo[i], levelMeta.segments);
        }
    }
}

void KVTableInteTest::MakeOnePartitionData(const string& partRootDir, const string& docStrs)
{
    GET_PARTITION_DIRECTORY()->MakeDirectory(partRootDir);
    string rootDir = GET_TEMP_DATA_PATH() + partRootDir;
    PartitionStateMachine psm;

    psm.Init(mSchema, mOptions, rootDir);
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrs, "", ""));
}

void KVTableInteTest::TestOptimizeMergeMultipleLevel()
{
    BuildConfig& buildConfig = mOptions.GetBuildConfig(false);
    buildConfig.shardingColumnNum = 2;
    buildConfig.keepVersionCount = 100;

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));

        string fullDoc = "cmd=add,key=1,value=1,ts=101;"
                         "cmd=add,key=2,value=2,ts=102;";

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));

        CheckMergeResult(psm, 1, "1;1", "1;1", {{}, {1, 2}});
        string incDoc = "cmd=add,key=1,value=3,ts=101;"
                        "cmd=add,key=2,value=4,ts=102;";
        // refactor after kv merge support reopen
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDoc, "", ""));
    }
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=4"));
}

void KVTableInteTest::testMergeMultiPartWithTTL()
{
    mSchema->SetDefaultTTL(0);
    MakeOnePartitionData("part1", "cmd=add,key=1,value=1,ts=104;");
    MakeOnePartitionData("part2", "cmd=add,key=2,value=2,ts=102;");
    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part2");

    GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEMP_DATA_PATH() + "mergePart";

    mOptions.SetIsOnline(false);
    MultiPartitionMerger multiPartMerger(mOptions, NULL, "", CommonBranchHinterOption::Test());
    vector<DirectoryPtr> mergeSrcDirs = multiPartMerger.CreateMergeSrcDirs(mergeSrcs, INVALID_VERSIONID, nullptr);
    auto partMerger = multiPartMerger.CreatePartitionMerger(mergeSrcDirs, mergePartPath);
    partMerger->Merge(true, 103);

    PartitionStateMachine psm;
    psm.Init(mSchema, mOptions, mergePartPath);

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    // bug fix: new version timestamp should large than old one
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
}

void KVTableInteTest::TestMergeWithTTL()
{
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;
    mOptions.GetBuildConfig(false).ttl = 10;

    string field = "key:int32;value:uint64:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    schema->SetEnableTTL(true);

    // discard doc: TTL + TS < NOW
    string docString = "cmd=add,key=10,value=1 2,ts=50000000;"
                       "cmd=add,key=-10,value=2 3 4,ts=60000000";
    PartitionStateMachine psm;
    psm.SetCurrentTimestamp(70000000);
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    CheckMergeResult(psm, 2, "1", "1");
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "-10", "value=2 3 4"));
}

void KVTableInteTest::TestMergeWithUserTs()
{
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;
    mOptions.GetBuildConfig(false).ttl = 10;
    mOptions.GetBuildConfig(false).useUserTimestamp = true;

    string field = "key:int32;value:uint64:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    schema->SetEnableTTL(true);

    // discard doc: TTL + TS < NOW
    string docString = "cmd=add,key=10,value=1 2,ts=60000000,ha_reserved_timestamp=50000000;"
                       "cmd=add,key=-10,value=2 3 4,ts=50000000,ha_reserved_timestamp=60000000";
    PartitionStateMachine psm;
    psm.SetCurrentTimestamp(70000000);
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    CheckMergeResult(psm, 1, "1", "1");
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "-10", "value=2 3 4"));
}

void KVTableInteTest::TestMergeWithDefaultTTL()
{
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;

    string field = "key:int32;value:uint64:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    schema->SetDefaultTTL(10);

    // discard doc: TTL + TS < NOW
    string docString = "cmd=add,key=10,value=1 2,ts=50000000;"
                       "cmd=add,key=-10,value=2 3 4,ts=60000000";
    PartitionStateMachine psm;
    psm.SetCurrentTimestamp(70000000);
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    CheckMergeResult(psm, 2, "1", "1");
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "-10", "value=2 3 4"));
}

void KVTableInteTest::TestMergeWithTTLWhenAllDead()
{
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;
    mOptions.GetBuildConfig(false).ttl = 10;

    string field = "key:int32;value:uint64:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    schema->SetEnableTTL(true);

    // discard doc: TTL + TS < NOW
    string docString = "cmd=add,key=10,value=1 2,ts=50000000;"
                       "cmd=add,key=-10,value=2 3 4,ts=60000000";
    PartitionStateMachine psm;
    psm.SetCurrentTimestamp(80000000);
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    CheckMergeResult(psm, 1, "", "");
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "-10", ""));
}

void KVTableInteTest::TestMergeUntilBottomLevel()
{
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;

    string field = "key:int32;value:uint64:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");

    string docString = "cmd=add,key=10,value=1 2,ts=50000000;"
                       "cmd=add,key=20,value=4 5,ts=51000000;"
                       "cmd=delete,key=10,ts=60000000";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "20", "value=4 5"));
    CheckMergeResult(psm, 1, "1", "1");
}

void KVTableInteTest::TestShardBasedMergeWithBadDocs()
{
    // fix bug: xxxx://invalid/issue/38787282
    string field = "key:string;value:uint64;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    schema->SetEnableTTL(true);
    ASSERT_TRUE(schema);

    mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    mOptions.GetBuildConfig(false).shardingColumnNum = 2;
    mOptions.GetBuildConfig(false).levelNum = 3;
    // cmd invaild, shard write failed
    string docString = "cmd=delete_sub,key=1,value=1,ts=101000000;"
                       "cmd=delete_sub,key=2,value=2,ts=102000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "1", ""));
}

void KVTableInteTest::TestHashTableOneField()
{
    string field = "key:string;value:uint64;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    schema->SetEnableTTL(true);
    ASSERT_TRUE(schema);

    string docString = "cmd=add,key=abc,value=1,ts=101000000;"
                       "cmd=add,key=def,value=2,ts=102000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "abc", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "def", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "abcf", ""));

    auto kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    uint64_t value;
    KVReadOptions options;
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("abc"), value, options)));
    ASSERT_EQ(1ul, value);
    ASSERT_FALSE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("abcf"), value, options)));
}

void KVTableInteTest::TestHashTableOneStringField()
{
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;

    string field = "key:int64;value:string:false;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    ASSERT_TRUE(schema);

    if (GET_CASE_PARAM()) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        KVIndexPreference::HashDictParam keyParam("dense", 90);
        kvConfig->GetIndexPreference().SetHashDictParam(keyParam);
        KVIndexPreference::ValueParam valueParam(false, "lz4hc", 512);
        valueParam.EnableValueImpact(kvConfig->GetIndexPreference().GetValueParam().IsValueImpact());
        valueParam.EnablePlainFormat(kvConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
        kvConfig->GetIndexPreference().SetValueParam(valueParam);
        mOptions.SetEnablePackageFile(true);
    }

    string docString = "cmd=add,key=1,value=v1,ts=101000000;"
                       "cmd=add,key=-1,value=v-1,ts=102000000;"
                       "cmd=add,key=0,value=v0,ts=103000000;"
                       "cmd=delete,key=0,ts=104000000;"
                       "cmd=delete,key=2,ts=105000000;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=v1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "-1", "value=v-1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "0", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));

    auto kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    MultiChar value;
    autil::mem_pool::Pool pool;
    KVReadOptions options;
    options.pool = &pool;
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("1"), value, options)));
    ASSERT_EQ(2, value.size());
    ASSERT_EQ('v', value[0]);
    ASSERT_EQ('1', value[1]);
    ASSERT_FALSE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("2"), value, options)));
}

void KVTableInteTest::TestHashTableOneMultiField()
{
    string field = "key:int32;value:uint64:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    schema->SetEnableTTL(true);

    if (GET_CASE_PARAM()) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        KVIndexPreference::HashDictParam keyParam("cuckoo", 90);
        kvConfig->GetIndexPreference().SetHashDictParam(keyParam);
        KVIndexPreference::ValueParam valueParam(false, "lz4");
        valueParam.EnableValueImpact(kvConfig->GetIndexPreference().GetValueParam().IsValueImpact());
        valueParam.EnablePlainFormat(kvConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
        kvConfig->GetIndexPreference().SetValueParam(valueParam);
        mOptions.SetEnablePackageFile(true);
    }

    string docString = "cmd=add,key=10,value=1 2,ts=101000000;"
                       "cmd=add,key=-10,value=2,ts=102000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", "value=1 2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "-10", "value=2"));

    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,key=1,value=3,ts=103000000;", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=3"));

    auto kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    autil::mem_pool::Pool pool;
    KVReadOptions options;
    options.pool = &pool;

    MultiUInt64 value;
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("10"), value, options)));
    ASSERT_EQ(2, value.size());
    ASSERT_EQ(1, value[0]);
    ASSERT_EQ(2, value[1]);
    ASSERT_FALSE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("2"), value, options)));
}

void KVTableInteTest::TestHashTableMultiField()
{
    string field = "key:uint64;uint32:uint32;mint8:int8:true;mstr:string:true";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "key;uint32;mint8;mstr");

    if (GET_CASE_PARAM()) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        KVIndexPreference::ValueParam valueParam(false, "zstd");
        valueParam.EnableValueImpact(kvConfig->GetIndexPreference().GetValueParam().IsValueImpact());
        valueParam.EnablePlainFormat(kvConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
        kvConfig->GetIndexPreference().SetValueParam(valueParam);
        mOptions.SetEnablePackageFile(true);
    }

    string docString = "cmd=add,key=10,uint32=10,mint8=1 2,mstr=a cd,ts=101000000;"
                       "cmd=add,key=0,uint32=0,mint8=,mstr=,ts=102000000;"
                       "cmd=add,key=5,uint32=5,mint8=3,mstr=a,ts=102000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", "key=10,uint32=10,mint8=1 2,mstr=a cd"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "0", "key=0,uint32=0,mint8=,mstr="));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "5", "key=5,uint32=5,mint8=3,mstr=a"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "-10", ""));

    auto kvReader = psm.GetIndexPartition()->GetReader()->GetKVReader();
    autil::mem_pool::Pool pool;
    KVReadOptions options;
    options.fieldName = "mstr";
    options.pool = &pool;

    MultiString value;
    ASSERT_TRUE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("10"), value, options)));
    ASSERT_EQ(2, value.size());
    ASSERT_EQ(1, value[0].size());
    ASSERT_EQ('a', value[0][0]);
    ASSERT_EQ(2, value[1].size());
    ASSERT_EQ('c', value[1][0]);
    ASSERT_EQ('d', value[1][1]);
    ASSERT_FALSE(future_lite::interface::syncAwait(kvReader->GetAsync(StringView("2"), value, options)));
}

void KVTableInteTest::TestHashTableVarIndexMerge()
{
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;

    string field = "key:int32;value:uint64:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");

    if (GET_CASE_PARAM()) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        KVIndexPreference::ValueParam valueParam(false, "zstd");
        valueParam.EnableValueImpact(kvConfig->GetIndexPreference().GetValueParam().IsValueImpact());
        valueParam.EnablePlainFormat(kvConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
        kvConfig->GetIndexPreference().SetValueParam(valueParam);
        mOptions.SetEnablePackageFile(true);
    }

    string docString = "cmd=add,key=10,value=1 2,ts=101000000;"
                       "cmd=add,key=-10,value=2,ts=102000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    CheckMergeResult(psm, 1, "2", "2");
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", "value=1 2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "-10", "value=2"));
}

void KVTableInteTest::TestHashTableVarValueUseBlockCache()
{
    const char* disableCodegenSo = getenv("DISABLE_CODEGEN");
    setenv("DISABLE_CODEGEN", "true", 1);
    auto factory = indexlib::util::Singleton<CodeFactory>::GetInstance();
    factory->reinit();
    InnerTestHashTableVarValueUseBlockCache(false);
    InnerTestHashTableVarValueUseBlockCache(true);
    if (!disableCodegenSo) {
        unsetenv("DISABLE_CODEGEN");
    } else {
        setenv("DISABLE_CODEGEN", disableCodegenSo, 1);
    }
    factory->reinit();
}

void KVTableInteTest::InnerTestHashTableVarValueUseBlockCache(bool cacheDecompressFile)
{
    tearDown();
    setUp();

    string field = "key:int32;value:uint64:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    if (GET_CASE_PARAM()) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        KVIndexPreference::ValueParam valueParam(false, "lz4");
        valueParam.EnableValueImpact(kvConfig->GetIndexPreference().GetValueParam().IsValueImpact());
        valueParam.EnablePlainFormat(kvConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
        kvConfig->GetIndexPreference().SetValueParam(valueParam);
        mOptions.SetEnablePackageFile(true);
    }

    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "mmap" },
         { "file_patterns" : ["_KV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    if (cacheDecompressFile) {
        jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "mmap" },
         { "file_patterns" : ["_KV_VALUE_"], "load_strategy" : "cache",
           "load_strategy_param" : { "global_cache" : true, "cache_decompress_file" : true}}]
        })";
    }

    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;

    string docString = "cmd=add,key=10,value=1 2,ts=101000000;"
                       "cmd=add,key=-10,value=2,ts=102000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    CheckMergeResult(psm, 1, "2", "2");

    // NOTE: lagacy interface, use BlockCache->GetTotalHitCount instaed,
    //  but currently totalAccessCount and totalHitCount are not tested here.(see below comments)
    // file_system::BlockCacheMetrics blockCacheMetrics;
    // psm.GetIndexPartitionResource().fileBlockCache->FillBlockCacheMetrics(blockCacheMetrics);
    // auto blockCache = psm.GetIndexPartitionResource().fileBlockCache->GetBlockCache();

    // NOTE: exact cache hit count is variable
    // ASSERT_EQ(0, blockCacheMetrics.totalAccessCount);
    // ASSERT_EQ(0, blockCacheMetrics.totalHitCount);

    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", "value=1 2"));
    // psm.GetIndexPartitionResource().fileBlockCache->FillBlockCacheMetrics(blockCacheMetrics);
    // NOTE: depend to implementation of HashTableVarSegmentReader<T>::GetValue
    // merge make here only one segment.
    // three read per query, and all read will refer to same block
    // 1st read len(value len only 1 bytes),
    // 2nd will read 0 bytes(so do noththing actually)
    // 3rd is read value
    // only first read access block cache when compress.

    // ASSERT_EQ(GET_CASE_PARAM() ? 1UL : 2UL, blockCacheMetrics.totalAccessCount);
    // ASSERT_EQ(GET_CASE_PARAM() ? 0UL : 1UL, blockCacheMetrics.totalHitCount);

    ASSERT_TRUE(psm.Transfer(QUERY, "", "-10", "value=2"));
    // psm.GetIndexPartitionResource().fileBlockCache->FillBlockCacheMetrics(blockCacheMetrics);
    // ASSERT_EQ(GET_CASE_PARAM() ? 2UL : 4UL, blockCacheMetrics.totalAccessCount);
    // ASSERT_EQ(GET_CASE_PARAM() ? 1UL : 3UL, blockCacheMetrics.totalHitCount);
}

void KVTableInteTest::TestHashTableVarKeyUseBlockCache()
{
    string field = "key:uint64;value:uint64:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    if (GET_CASE_PARAM()) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        KVIndexPreference::ValueParam valueParam(false, "lz4");
        valueParam.EnableValueImpact(kvConfig->GetIndexPreference().GetValueParam().IsValueImpact());
        valueParam.EnablePlainFormat(kvConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
        kvConfig->GetIndexPreference().SetValueParam(valueParam);
        mOptions.SetEnablePackageFile(true);
    }

    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }},
         { "file_patterns" : ["_KV_VALUE_"], "load_strategy" : "mmap" }]
    })";
    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);

    // 18446744073709551359 is _EmptyKey in SpecialKeyBucket
    // 18446744073709551103 is _DeleteKey in SpecialKeyBucket
    string docString = "cmd=add,key=10,value=1 2,ts=101000000;"
                       "cmd=add,key=18446744073709551359,value=2,ts=102000000;"
                       "cmd=add,key=18446744073709551103,value=,ts=103000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    CheckMergeResult(psm, 1, "3", "3");

    auto blockCache =
        psm.GetIndexPartitionResource().fileBlockCacheContainer->GetAvailableFileCache("")->GetBlockCache();

    // 1 for read header to init hash table file reader when open key
    ASSERT_EQ(1, blockCache->GetTotalMissCount());
    ASSERT_EQ(0, blockCache->GetTotalHitCount());

    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", "value=1 2"));

    ASSERT_EQ(1, blockCache->GetTotalMissCount());
    ASSERT_EQ(1, blockCache->GetTotalHitCount());

    ASSERT_TRUE(psm.Transfer(QUERY, "", "18446744073709551359", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "18446744073709551103", "value="));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", ""));
}

void KVTableInteTest::InnerTestHashTableFixVarValue(bool cacheLoad)
{
    tearDown();
    setUp();

    string field = "key:uint64;value:uint64:true:false::4;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    if (GET_CASE_PARAM()) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        KVIndexPreference::ValueParam valueParam(false, "zstd");
        valueParam.EnableValueImpact(kvConfig->GetIndexPreference().GetValueParam().IsValueImpact());
        valueParam.EnablePlainFormat(kvConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
        kvConfig->GetIndexPreference().SetValueParam(valueParam);
        mOptions.SetEnablePackageFile(true);
    }

    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    if (cacheLoad) {
        jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_", "_KV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    }

    mOptions.GetBuildConfig(false).shardingColumnNum = 2;
    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    mOptions.GetOnlineConfig().ttl = 20;

    string docString = "cmd=add,key=1,value=1,ts=10000000;"
                       "cmd=add,key=2,value=2 3 4 6,ts=30000000";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1 0 0 0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2 3 4 6"));
}

void KVTableInteTest::TestHashTableFixVarValue()
{
    InnerTestHashTableFixVarValue(true);
    InnerTestHashTableFixVarValue(false);
}

void KVTableInteTest::TestHashTableFixUseBlockCache()
{
    string field = "key:uint64;value:uint64;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    ASSERT_FALSE(schema->TTLEnabled());
    schema->SetEnableTTL(true);

    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    mOptions.GetOnlineConfig().ttl = 20;

    string docString = "cmd=add,key=1,value=1,ts=10000000;"
                       "cmd=add,key=3,value=2,ts=30000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    FileBlockCacheContainerPtr& globalFileBlockCache = psm.GetIndexPartitionResource().fileBlockCacheContainer;
    ASSERT_STREQ("LRUCache", globalFileBlockCache->GetAvailableFileCache("")->GetBlockCache()->TEST_GetCacheName());

    auto blockCache =
        psm.GetIndexPartitionResource().fileBlockCacheContainer->GetAvailableFileCache("")->GetBlockCache();

    // 1 for read header to init hash table file reader when open key
    ASSERT_EQ(1, blockCache->GetTotalHitCount() + blockCache->GetTotalMissCount());

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));

    ASSERT_EQ(1, blockCache->GetTotalMissCount());
    ASSERT_EQ(1, blockCache->GetTotalHitCount());
}

void KVTableInteTest::TestAddTrigerDump()
{
    mOptions.GetBuildConfig(true).buildTotalMemory = 20;
    mOptions.GetBuildConfig(false).buildTotalMemory = 20;

    string field = "key:int32;value:string:false;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value;key");

    string docString = "cmd=add,key=1,ts=1,value=" + string(5 * 1024 * 1024, '1') + ";" +
                       "cmd=add,key=2,ts=1,value=" + string(5 * 1024 * 1024, '2') + ";";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    // CheckMergeResult(psm, 1, "2", "2");
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "key=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "key=2"));
}

void KVTableInteTest::TestShortOffsetWithDeleteDoc()
{
    mOptions.GetBuildConfig(false).buildTotalMemory = 20;
    string field = "key:int32;value:string:false;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value;key");

    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KVIndexPreference& kvIndexPreference = kvConfig->GetIndexPreference();
    const KVIndexPreference::HashDictParam& dictParam = kvIndexPreference.GetHashDictParam();
    KVIndexPreference::HashDictParam newDictParam(dictParam.GetHashType(), dictParam.GetOccupancyPct(),
                                                  dictParam.UsePreciseCountWhenMerge(), false, true);
    kvConfig->GetIndexPreference().SetHashDictParam(newDictParam);
    kvConfig->SetTTL(100);

    string docString = "cmd=add,key=2,value=hello,ts=10000000;"
                       "cmd=add,key=3,value=world,ts=10000000;"
                       "cmd=delete,key=3,ts=10000000;";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=hello"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
}

void KVTableInteTest::InnerTestMergeWithShortOffset(const string& hashType, bool enableTTL, bool enableShortOffset)
{
    tearDown();
    setUp();
    IndexPartitionOptions options = mOptions;
    options.GetBuildConfig(false).buildTotalMemory = 20;
    options.SetEnablePackageFile(false);
    string field = "key:int32;value:string:false;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value;key");

    schema->SetEnableTTL(enableTTL);
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KVIndexPreference& kvIndexPreference = kvConfig->GetIndexPreference();
    const KVIndexPreference::HashDictParam& dictParam = kvIndexPreference.GetHashDictParam();
    KVIndexPreference::HashDictParam newDictParam(hashType, dictParam.GetOccupancyPct(),
                                                  dictParam.UsePreciseCountWhenMerge(), false, enableShortOffset);
    newDictParam.SetMaxValueSizeForShortOffset((int64_t)1024);
    kvConfig->GetIndexPreference().SetHashDictParam(newDictParam);
    if (enableTTL) {
        kvConfig->SetTTL(10000);
    }
    string longSuffix(400, '0');
    string docString = string("cmd=add,key=2,value=hello") + longSuffix + string(",ts=10000000;") +
                       string("cmd=add,key=3,value=world") + longSuffix + string(",ts=10000000;") +
                       string("cmd=add,key=4,value=kitty") + longSuffix + string(",ts=10000000;");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", string("value=hello") + longSuffix));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", string("value=world") + longSuffix));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", string("value=kitty") + longSuffix));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr partDir = GET_PARTITION_DIRECTORY();
    string mergedSegDir = enableShortOffset ? string("segment_2_level_1") : string("segment_1_level_1");

    ASSERT_TRUE(partDir->IsDir(mergedSegDir));
    string filePath = mergedSegDir + "/index/key/format_option";
    string formatStr;
    ASSERT_NO_THROW(GET_PARTITION_DIRECTORY()->Load(filePath, formatStr));

    KVFormatOptions kvOptions;
    kvOptions.FromString(formatStr);
    ASSERT_FALSE(kvOptions.IsShortOffset());
}

void KVTableInteTest::TestMergeWithShortOffset()
{
    InnerTestMergeWithShortOffset("dense", true, true);
    InnerTestMergeWithShortOffset("dense", true, false);
    InnerTestMergeWithShortOffset("dense", false, true);
    InnerTestMergeWithShortOffset("dense", false, false);
    InnerTestMergeWithShortOffset("cuckoo", true, true);
    InnerTestMergeWithShortOffset("cuckoo", true, false);
    InnerTestMergeWithShortOffset("cuckoo", false, true);
    InnerTestMergeWithShortOffset("cuckoo", false, false);
    InnerTestMergeWithShortOffsetTriggerHashCompress("dense", true);
    InnerTestMergeWithShortOffsetTriggerHashCompress("dense", false);
    InnerTestMergeWithShortOffsetTriggerHashCompress("cuckoo", true);
    InnerTestMergeWithShortOffsetTriggerHashCompress("cuckoo", false);
}

void KVTableInteTest::InnerTestMergeWithShortOffsetTriggerHashCompress(const string& hashType, bool enableTTL)
{
    tearDown();
    setUp();
    IndexPartitionOptions options = mOptions;
    options.GetBuildConfig(false).buildTotalMemory = 20;
    options.SetEnablePackageFile(false);
    string field = "key:int32;value:string:false;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value;key");
    schema->SetEnableTTL(enableTTL);
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KVIndexPreference& kvIndexPreference = kvConfig->GetIndexPreference();
    const KVIndexPreference::HashDictParam& dictParam = kvIndexPreference.GetHashDictParam();
    KVIndexPreference::HashDictParam newDictParam(hashType, dictParam.GetOccupancyPct(),
                                                  dictParam.UsePreciseCountWhenMerge(), false, true);
    newDictParam.SetMaxValueSizeForShortOffset((int64_t)1024);
    kvConfig->GetIndexPreference().SetHashDictParam(newDictParam);
    if (enableTTL) {
        kvConfig->SetTTL(10000);
    }
    string shortSuffix(10, '0');
    string docString = string("cmd=add,key=2,value=hello") + shortSuffix + string(",ts=10000000;") +
                       string("cmd=add,key=3,value=world") + shortSuffix + string(",ts=10000000;") +
                       string("cmd=add,key=4,value=kitty") + shortSuffix + string(",ts=10000000;");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", string("value=hello") + shortSuffix));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", string("value=world") + shortSuffix));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", string("value=kitty") + shortSuffix));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr partDir = GET_PARTITION_DIRECTORY();
    {
        string mergedSegDir = string("segment_1_level_1");
        ASSERT_TRUE(partDir->IsDir(mergedSegDir));
        string filePath = mergedSegDir + "/index/key/format_option";
        string formatStr;
        ASSERT_NO_THROW(GET_PARTITION_DIRECTORY()->Load(filePath, formatStr));
        KVFormatOptions kvOptions;
        kvOptions.FromString(formatStr);
        ASSERT_TRUE(kvOptions.IsShortOffset());
    }
    string longSuffix = string(400, '1');
    string incDoc = string("cmd=add,key=21,value=hello") + longSuffix + string(",ts=10000001;");
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    incDoc = string("cmd=add,key=22,value=world") + longSuffix + string(",ts=10000002;");
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    incDoc = string("cmd=add,key=23,value=kitty") + longSuffix + string(",ts=10000003;");
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    {
        string mergedSegDir = string("segment_5_level_1");
        ASSERT_TRUE(partDir->IsDir(mergedSegDir));
        string filePath = mergedSegDir + "/index/key/format_option";
        string formatStr;
        ASSERT_NO_THROW(GET_PARTITION_DIRECTORY()->Load(filePath, formatStr));
        KVFormatOptions kvOptions;
        kvOptions.FromString(formatStr);
        ASSERT_FALSE(kvOptions.IsShortOffset());
    }

    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", string("value=hello") + shortSuffix));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", string("value=world") + shortSuffix));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", string("value=kitty") + shortSuffix));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "21", string("value=hello") + longSuffix));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "22", string("value=world") + longSuffix));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "23", string("value=kitty") + longSuffix));
}

void KVTableInteTest::InnerTestBuildForceDump(bool enableCompactHashKey, bool enableShortOffset,
                                              size_t maxValueFileSize, string hashType, size_t totalDocSize,
                                              size_t segmentNum, string dirName)
{
    mOptions.GetBuildConfig(false).buildTotalMemory = 2 * 1024;
    mOptions.SetEnablePackageFile(false);
    string field = "key:int32;value:string:false;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value;key");

    // mock config
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KVIndexPreference& kvIndexPreference = kvConfig->GetIndexPreference();
    const KVIndexPreference::HashDictParam& dictParam = kvIndexPreference.GetHashDictParam();
    KVIndexPreference::HashDictParam newDictParam(hashType, dictParam.GetOccupancyPct(),
                                                  dictParam.UsePreciseCountWhenMerge(), enableCompactHashKey,
                                                  enableShortOffset);
    newDictParam.SetMaxValueSizeForShortOffset(maxValueFileSize);
    kvConfig->GetIndexPreference().SetHashDictParam(newDictParam);

    // prepare doc
    size_t bufLen = maxValueFileSize / 1000;
    bufLen = bufLen > VarNumAttributeFormatter::VAR_NUM_ATTRIBUTE_MAX_COUNT - 20
                 ? VarNumAttributeFormatter::VAR_NUM_ATTRIBUTE_MAX_COUNT - 20
                 : bufLen;
    char* buffer = new char[bufLen];
    // size_t totalDocSize = segmentNum * maxValueFileSize;
    stringstream fullDocBuffer;
    memset(buffer, 'a', bufLen);
    string seperator = ";";
    int32_t i = 1;
    while (fullDocBuffer.str().size() < totalDocSize) {
        string docValue = "cmd=add,key=" + StringUtil::toString(i);
        docValue += ",ts=1,value=";
        fullDocBuffer << docValue;
        fullDocBuffer << string(buffer, bufLen) << seperator;
        i++;
    }
    string fullDoc(fullDocBuffer.str().c_str(), fullDocBuffer.str().size());
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/" + dirName));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));

        DirectoryPtr partDir = GET_CHECK_DIRECTORY();
        for (size_t i = 0; i < segmentNum; i++) {
            string segDirName = dirName + "/segment_" + StringUtil::toString(i) + "_level_0";
            ASSERT_TRUE(partDir->IsDir(segDirName)) << segDirName;
            if (enableShortOffset) {
                string filePath = segDirName + "/index/key/format_option";
                string formatStr;
                ASSERT_NO_THROW(partDir->Load(filePath, formatStr));

                KVFormatOptions kvOptions;
                kvOptions.FromString(formatStr);
                if (schema->GetRegionCount() == 1) {
                    ASSERT_TRUE(kvOptions.IsShortOffset());
                } else {
                    ASSERT_FALSE(kvOptions.IsShortOffset());
                }
            }
            // file_system::FslibWrapper::DeleteDirE(partDir->GetLogicalPath() + "/" + segDirName);
        }
        ASSERT_FALSE(partDir->IsExist(dirName + "/segment_" + StringUtil::toString(segmentNum) + "_level_0"));
    }
    delete[] buffer;
}

void KVTableInteTest::TestOffsetLimitTriggerForceDump()
{
    // 10mb for each segment
    size_t maxValueFileSize = 500 * 1024;
    // short offset
    // totalDocSize > one segment size
    InnerTestBuildForceDump(false, true, maxValueFileSize, "dense", maxValueFileSize * 3, 3, "dir1");
    // totalDocSize <= one segment size
    InnerTestBuildForceDump(false, true, maxValueFileSize, "dense", maxValueFileSize, 1, "dir2");
    // totalDocSize > one segment size
    InnerTestBuildForceDump(false, true, maxValueFileSize, "cuckoo", maxValueFileSize * 3, 3, "dir3");
    // totalDocSize <= one segment size
    InnerTestBuildForceDump(false, true, maxValueFileSize, "cuckoo", maxValueFileSize, 1, "dir4");

    // normal offset
    InnerTestBuildForceDump(false, false, maxValueFileSize, "dense", maxValueFileSize * 2, 1, "dir5");
    InnerTestBuildForceDump(false, false, maxValueFileSize, "cuckoo", maxValueFileSize * 2, 1, "dir7");
}

void KVTableInteTest::TestReopenIncWithoutRt()
{
    DoTestReopenIncWithoutRt(false, false);
    DoTestReopenIncWithoutRt(false, true);
}
void KVTableInteTest::TestReopenIncOlderThanAllRt()
{
    DoTestReopenIncOlderThanAllRt(false, false);
    DoTestReopenIncOlderThanAllRt(false, true);
}
void KVTableInteTest::TestReopenIncCoverPartRtBuiltSegment()
{
    DoTestReopenIncCoverPartRtBuiltSegment(false, false);
    DoTestReopenIncCoverPartRtBuiltSegment(false, true);
}
void KVTableInteTest::TestReopenIncCoverPartRtBuildingSegment()
{
    DoTestReopenIncCoverPartRtBuildingSegment(false, false);
    DoTestReopenIncCoverPartRtBuildingSegment(false, true);
}
void KVTableInteTest::TestReopenIncCoverAllRt()
{
    DoTestReopenIncCoverAllRt(false, false);
    DoTestReopenIncCoverAllRt(false, true);
}
void KVTableInteTest::TestReopenIncKeyAndRtKeyInSameSecond()
{
    DoTestReopenIncKeyAndRtKeyInSameSecond(false, false);
    DoTestReopenIncKeyAndRtKeyInSameSecond(false, true);
}
void KVTableInteTest::TestForceReopenIncWithoutRt()
{
    DoTestReopenIncWithoutRt(true, false);
    DoTestReopenIncWithoutRt(true, true);
}
void KVTableInteTest::TestForceReopenIncOlderThanAllRt()
{
    DoTestReopenIncOlderThanAllRt(true, false);
    DoTestReopenIncOlderThanAllRt(true, true);
}
void KVTableInteTest::TestForceReopenIncCoverPartRtBuiltSegment()
{
    DoTestReopenIncCoverPartRtBuiltSegment(true, false);
    DoTestReopenIncCoverPartRtBuiltSegment(true, true);
}
void KVTableInteTest::TestForceReopenIncCoverPartRtBuildingSegment()
{
    DoTestReopenIncCoverPartRtBuildingSegment(true, false);
    DoTestReopenIncCoverPartRtBuildingSegment(true, true);
}
void KVTableInteTest::TestForceReopenIncCoverAllRt()
{
    DoTestReopenIncCoverAllRt(true, false);
    DoTestReopenIncCoverAllRt(true, true);
}
void KVTableInteTest::TestForceReopenIncKeyAndRtKeyInSameSecond()
{
    DoTestReopenIncKeyAndRtKeyInSameSecond(true, false);
    DoTestReopenIncKeyAndRtKeyInSameSecond(true, true);
}

void KVTableInteTest::TestReopenIncCoverBuiltAndBuildingSegment()
{
    DoTestReopenIncCoverBuiltAndBuildingSegment(true);
    DoTestReopenIncCoverBuiltAndBuildingSegment(false);
}

void KVTableInteTest::TestReopenIncCoverBuiltSegmentWhenReaderHold()
{
    mOptions.GetBuildConfig(true).maxDocCount = 2;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key=0,value=0,ts=100000000;"
                       "cmd=add,key=1,value=1,ts=101000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "1", "value=1"));

    // first rt segment be in-memory & hold by reader
    string rtDoc1 = "cmd=add,key=2,value=2,ts=102000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc1, "", ""));
    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    IndexPartitionReaderPtr reader = indexPart->GetReader();

    // first rt segment transfer to built segment
    string rtDoc2 = "cmd=add,key=3,value=3,ts=103000000;"
                    "cmd=add,key=4,value=4,ts=104000000;"
                    "cmd=add,key=5,value=5,ts=105000000;"
                    "cmd=add,key=6,value=6,ts=106000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc2, "", ""));

    // inc reclaim cover first rt segment
    string incDoc = "cmd=add,key=2,value=2,ts=102000000;"
                    "cmd=add,key=3,value=3,ts=103000000;"
                    "cmd=add,key=4,value=4,ts=104000000;"
                    "cmd=add,key=5,value=5,ts=105000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));

    reader.reset();
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
}

void KVTableInteTest::TestOpenWithoutEnoughMemory()
{
    string jsonStr = "                                                \
    {                                                                   \
    \"load_config\" :                                                   \
    [{                                                                  \
        \"file_patterns\" : [\".*\"],                                   \
        \"load_strategy\" : \"mmap\",                                   \
        \"load_strategy_param\" : {                                     \
            \"lock\" : true                                             \
        }                                                               \
    }]}";
    FromJsonString(mOptions.GetOnlineConfig().loadConfigList, jsonStr);
    {
        PartitionStateMachine psm;

        ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
        string docString = "cmd=add,key=0,value=0,ts=100000000;"
                           "cmd=add,key=1,value=1,ts=101000000;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "1", "value=1"));
    }
    {
        IndexPartitionResource resource =
            IndexPartitionResource::Create(IndexPartitionResource::IR_ONLINE, 4 * 1024 * 1024);
        OnlinePartition partition(resource);
        ASSERT_EQ(IndexPartition::OS_LACK_OF_MEMORY, partition.Open(GET_TEMP_DATA_PATH(), "", mSchema, mOptions));
    }
    {
        IndexPartitionResource resource =
            IndexPartitionResource::Create(IndexPartitionResource::IR_ONLINE, 100 * 1024 * 1024);
        OnlinePartition partition(resource);
        ASSERT_EQ(IndexPartition::OS_OK, partition.Open(GET_TEMP_DATA_PATH(), "", mSchema, mOptions));
    }
}

void KVTableInteTest::TestReopenWithoutEnoughMemory()
{
    string jsonStr = "                                                \
    {                                                                   \
    \"load_config\" :                                                   \
    [{                                                                  \
        \"file_patterns\" : [\".*\"],                                   \
        \"load_strategy\" : \"mmap\",                                   \
        \"load_strategy_param\" : {                                     \
            \"lock\" : true                                             \
        }                                                               \
    }]}";
    FromJsonString(mOptions.GetOnlineConfig().loadConfigList, jsonStr);
    {
        PartitionStateMachine psm;

        ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));
    }
    IndexPartitionResource resource =
        IndexPartitionResource::Create(IndexPartitionResource::IR_ONLINE, 10 * 1024 * 1024);
    resource.memoryQuotaController->Allocate(6 * 1024 * 1024); // allocate 6MB, 4MB for InMemorySegment
    OnlinePartition partition(resource);
    ASSERT_EQ(IndexPartition::OS_OK, partition.Open(GET_TEMP_DATA_PATH(), "", mSchema, mOptions));
    // build inc index
    {
        string docString = "cmd=add,key=0,value=0,ts=100000000;"
                           "cmd=add,key=1,value=1,ts=101000000;";
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, docString, "", ""));
    }
    ASSERT_EQ(IndexPartition::OS_FAIL, partition.ReOpen(false));
    resource.memoryQuotaController->Free(6 * 1024 * 1024); // free 6MB
    ASSERT_EQ(IndexPartition::OS_OK, partition.ReOpen(false));
}

void KVTableInteTest::DoTestReopenIncWithoutRt(bool isForce, bool flushRt)
{
    tearDown();
    setUp();

    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = flushRt;

    PartitionStateMachine psm;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key=0,value=0,ts=100000000;"
                       "cmd=add,key=1,value=1,ts=101000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "1", "value=1"));

    string incDoc = "cmd=add,key=2,value=2,ts=102000000;"
                    "cmd=add,key=1,value=3,ts=103000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(isForce ? PE_REOPEN_FORCE : PE_REOPEN, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "0", "value=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));
}

void KVTableInteTest::DoTestReopenIncCoverBuiltAndBuildingSegment(bool flushRt)
{
    tearDown();
    setUp();

    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = flushRt;
    mOptions.GetBuildConfig(true).maxDocCount = 2;

    PartitionStateMachine psm;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key=0,value=0,ts=100000000;"
                       "cmd=add,key=1,value=1,ts=101000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "1", "value=1"));

    string rtDoc = "cmd=add,key=2,value=2,ts=102000000;"
                   "cmd=add,key=3,value=3,ts=103000000;"
                   "cmd=add,key=4,value=4,ts=104000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));

    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    IndexPartitionReaderPtr reader = indexPart->GetReader();

    string incDoc = "cmd=add,key=2,value=2,ts=102000000;"
                    "cmd=add,key=3,value=3,ts=103000000;"
                    "cmd=add,key=4,value=4,ts=104000000;"
                    "cmd=add,key=5,value=5,ts=105000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    reader.reset();
}

void KVTableInteTest::DoTestReopenIncOlderThanAllRt(bool isForce, bool flushRt)
{
    tearDown();
    setUp();

    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = flushRt;

    PartitionStateMachine psm;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=2,value=2,ts=102000000;", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,key=1,value=3,ts=103000000;", "", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=3"));

    // trigger switch reader for flush rt index
    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    DirectoryPtr rootDir = indexPart->GetRootDirectory();
    IFileSystemPtr fileSystem = rootDir->GetFileSystem();
    fileSystem->Sync(true).GetOrThrow();
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));

    string incDoc = "cmd=add,key=1,value=1,ts=101000000;"
                    "cmd=add,key=3,value=3,ts=101000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(isForce ? PE_REOPEN_FORCE : PE_REOPEN, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=3"));
}

void KVTableInteTest::DoTestReopenIncCoverPartRtBuiltSegment(bool isForce, bool flushRt)
{
    tearDown();
    setUp();

    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = flushRt;

    PartitionStateMachine psm;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string rtDoc = "cmd=add,key=1,value=1,ts=101000000;"
                   "cmd=add,key=2,value=2,ts=102000000;"
                   "cmd=add,key=10,value=10,ts=110000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));

    // trigger switch reader for flush rt index
    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    DirectoryPtr rootDir = indexPart->GetRootDirectory();
    IFileSystemPtr fileSystem = rootDir->GetFileSystem();
    fileSystem->Sync(true).GetOrThrow();
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", "value=10"));

    string incDoc = "cmd=add,key=1,value=5,ts=105000000;"
                    "cmd=add,key=3,value=3,ts=103000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(isForce ? PE_REOPEN_FORCE : PE_REOPEN, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", "value=10"));
}

void KVTableInteTest::DoTestReopenIncCoverPartRtBuildingSegment(bool isForce, bool flushRt)
{
    tearDown();
    setUp();

    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = flushRt;

    PartitionStateMachine psm;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string rtDoc = "cmd=add,key=1,value=1,ts=101000000;"
                   "cmd=add,key=2,value=2,ts=102000000;"
                   "cmd=add,key=10,value=10,ts=110000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", "value=10"));

    string incDoc = "cmd=add,key=1,value=5,ts=105000000;"
                    "cmd=add,key=3,value=3,ts=103000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(isForce ? PE_REOPEN_FORCE : PE_REOPEN, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", "value=10"));
}

void KVTableInteTest::DoTestReopenIncCoverAllRt(bool isForce, bool flushRt)
{
    tearDown();
    setUp();

    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = flushRt;

    PartitionStateMachine psm;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=1,value=1,ts=101000000;", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,key=2,value=2,ts=102000000;", "2", "value=2"));

    string incDoc = "cmd=add,key=1,value=5,ts=105000000;"
                    "cmd=add,key=3,value=3,ts=103000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(isForce ? PE_REOPEN_FORCE : PE_REOPEN, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,key=4,value=4,ts=106000000;", "4", "value=4"));
}

void KVTableInteTest::DoTestReopenIncKeyAndRtKeyInSameSecond(bool isForce, bool flushRt)
{
    autil::EnvGuard envGuard("BRANCH_NAME_POLICY", CommonBranchHinterOption::BNP_LEGACY);
    tearDown();
    setUp();
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = flushRt;

    PartitionStateMachine psm;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    {
        // rt doc is new
        string rtDoc = "cmd=add,key=1,value=9,ts=101000009;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "1", "value=9"));

        string incDoc = "cmd=add,key=1,value=1,ts=101000001;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(isForce ? PE_REOPEN_FORCE : PE_REOPEN, "", "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=9"));
    }
    {
        // inc doc is new
        string rtDoc = "cmd=add,key=2,value=2,ts=102000001;"
                       "cmd=add,key=100,value=100,ts=103000001;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "2", "value=2"));

        // result is temporarily out of date
        string incDoc = "cmd=add,key=2,value=9,ts=102000009;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(isForce ? PE_REOPEN_FORCE : PE_REOPEN, "", "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));
        // when the rt doc came,  the result correct again
        ASSERT_TRUE(psm.Transfer(BUILD_RT, incDoc, "2", "value=9"));
    }
    {
        // inc doc is same as rt doc
        string rtDoc = "cmd=add,key=3,value=3,ts=103000003;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "3", "value=3"));

        string incDoc = "cmd=add,key=3,value=3,ts=103000003;";
        // key 3 from rt
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(isForce ? PE_REOPEN_FORCE : PE_REOPEN, "", "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=3"));

        string incDoc2 = "cmd=add,key=4,value=4,ts=104000004;";
        // key 3 from inc
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDoc2, "", ""));
        ASSERT_TRUE(psm.Transfer(isForce ? PE_REOPEN_FORCE : PE_REOPEN, "", "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=3"));
    }
}

void KVTableInteTest::TestReleaseMemoryForTmpValueFile()
{
    GET_PARTITION_DIRECTORY()->MakeDirectory("rt_index_partition");
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;

    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "mmap" },
         { "file_patterns" : ["_KV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true, "cache_decompress_file" : true }}]
    })";

    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    mOptions.GetBuildConfig(false).maxDocCount = 1;
    mOptions.GetBuildConfig(true).maxDocCount = 10;

    string field = "key:uint64;uint32:uint32;mint8:int8:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "key;uint32;mint8");
    if (GET_CASE_PARAM()) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        KVIndexPreference::ValueParam valueParam(false, "zstd");
        valueParam.EnableValueImpact(kvConfig->GetIndexPreference().GetValueParam().IsValueImpact());
        valueParam.EnablePlainFormat(kvConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
        kvConfig->GetIndexPreference().SetValueParam(valueParam);
        mOptions.SetEnablePackageFile(true);
    }

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));

    string docString = "cmd=add,key=10,uint32=10,mint8=1 2,ts=101000000;"
                       "cmd=add,key=0,uint32=0,mint8=,ts=102000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    string rtString = "cmd=add,key=0,uint32=1,mint8=2 3,ts=104000000;"
                      "cmd=add,key=5,uint32=5,mint8=3,ts=105000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtString, "", ""));

    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    assert(onlinePart);
    string filePath = "rt_index_partition/segment_1073741824_level_0/index/key/value.tmp";
    ASSERT_TRUE(onlinePart->GetRootDirectory()->IsExist(filePath));

    // reopen trigger clean old reader
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    ASSERT_FALSE(onlinePart->GetRootDirectory()->IsExist(filePath));
}

void KVTableInteTest::TestRtIndexSizeWhenUseMultiValue()
{
    string field = "key:int32;value:uint64:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    schema->SetEnableTTL(true);

    string docString = "cmd=add,key=10,value=1 2,ts=101000000;"
                       "cmd=add,key=-10,value=2,ts=102000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,key=1,value=3,ts=103000000;", "", ""));

    auto partition = psm.GetIndexPartition();
    auto rootDirectoy = partition->GetRootDirectory();
    auto partWriter = partition->GetWriter();

    PartitionResourceCalculator calculator(mOptions.GetOnlineConfig());
    calculator.Init(rootDirectoy, partWriter, InMemorySegmentContainerPtr(), plugin::PluginManagerPtr());
    ASSERT_LT(calculator.GetRtIndexMemoryUse(), mOptions.GetBuildConfig(true).buildTotalMemory * 1024 * 1024 * 0.1);
}

void KVTableInteTest::TestProhibitInMemDumpBug_13765192()
{
    MultiPathDataFlushController::GetInstance()->InitFromString("quota_size=2560,quota_interval=500,flush_unit=2560");

    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().useSwapMmapFile = false;
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
    mOptions.GetOnlineConfig().prohibitInMemDump = true;

    string field = "key:int32;value:uint64:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");

    stringstream ss;
    for (size_t i = 0; i < 100; i++) {
        ss << "cmd=add,key=" << i << ",value=" << i << " " << i + 1 << ",ts=1000000;";
    }
    string rtDocStr = ss.str();
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocStr, "", ""));

    while (true) {
        ASSERT_TRUE(psm.Transfer(QUERY, "", "7", "value=7 8"));
        auto partReader = psm.GetIndexPartition()->GetReader();
        if (partReader->GetSwitchRtSegments().size() > 0) {
            break;
        }
    }
}

void KVTableInteTest::TestBugFix14177981()
{
    string field = "key:string;value:uint64;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");
    schema->SetEnableTTL(true);
    ASSERT_TRUE(schema);

    IndexPartitionOptions options;
    options.GetBuildConfig().maxDocCount = 1;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;

    string docString = "cmd=add,key=abc,value=1,ts=101000000;"
                       "cmd=add,key=def,value=2,ts=102000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    string rtDocs = "cmd=add,key=abc,value=2,ts=200000000;"
                    "cmd=add,key=abc,value=3,ts=210000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "", ""));
    // sleep(5);

    DirectoryPtr rootDir = GET_CHECK_DIRECTORY();
    size_t fileLen = rootDir->GetFileLength("rt_index_partition/segment_1073741825_level_0/index/key/key");
    size_t defaultLen = 16 * 1024 * 1024;
    ASSERT_GT(fileLen, defaultLen);
}

void KVTableInteTest::TestBuildFixLengthWithLegacyFormat()
{
    InnerTestBuildFixLengthWithLegacyFormat(false);
    InnerTestBuildFixLengthWithLegacyFormat(true);
}

void KVTableInteTest::InnerTestBuildFixLengthWithLegacyFormat(bool cacheLoad)
{
    tearDown();
    setUp();

    string field = "key:uint64;value1:uint64;value2:uint32";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value1;value2");

    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    if (cacheLoad) {
        jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_", "_KV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    }

    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    mOptions.GetOnlineConfig().ttl = 20;

    MakeEmptyFullIndexWithIndexFormatVersion(schema, mOptions, "2.1.0", GET_TEMP_DATA_PATH());
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key=1,value1=1,value2=2,ts=10000000;"
                       "cmd=add,key=2,value1=2,value2=3,ts=30000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docString, "", ""));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    size_t valueFileLen = GET_PARTITION_DIRECTORY()->GetFileLength("segment_1_level_1/index/key/value");
    size_t fixLen = sizeof(uint64_t) + sizeof(uint32_t);
    size_t countLen = MultiValueFormatter::getEncodedCountLength(fixLen);
    size_t expectLen = 2 * (fixLen + countLen);
    ASSERT_EQ(expectLen, valueFileLen);

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value1=1,value2=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value1=2,value2=3"));

    string rtDocStr = "cmd=add,key=3,value1=3,value2=4,ts=40000000;"
                      "cmd=add,key=4,value1=4,value2=5,ts=50000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value1=3,value2=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value1=4,value2=5"));
}

void KVTableInteTest::TestBuildFixLengthString()
{
    string field = "key:uint64;value:string:false:false::5";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value");

    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";

    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    mOptions.GetOnlineConfig().ttl = 20;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key=1,value=abcdef,ts=10000000;"
                       "cmd=add,key=2,value=123456,ts=30000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    size_t valueFileLen = GET_PARTITION_DIRECTORY()->GetFileLength("segment_1_level_1/index/key/value");
    size_t expectLen = 2 * 5;
    ASSERT_EQ(expectLen, valueFileLen);

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=abcde"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=12345"));
}

void KVTableInteTest::TestCompactHashKeyForFixHashTable()
{
    InnerTestCompactHashKey(false, "uint64", "dense", false);
    InnerTestCompactHashKey(false, "string", "dense", false);
    InnerTestCompactHashKey(false, "uint32", "dense", true);
    InnerTestCompactHashKey(false, "int16", "dense", true);

    InnerTestCompactHashKey(false, "uint64", "cuckoo", false);
    InnerTestCompactHashKey(false, "string", "cuckoo", false);
    InnerTestCompactHashKey(false, "uint32", "cuckoo", true);
    InnerTestCompactHashKey(false, "int16", "cuckoo", true);
}

void KVTableInteTest::TestCompactHashKeyForVarHashTable()
{
    InnerTestCompactHashKey(true, "uint64", "dense", false);
    InnerTestCompactHashKey(true, "string", "dense", false);
    InnerTestCompactHashKey(true, "uint32", "dense", true);
    InnerTestCompactHashKey(true, "int16", "dense", true);

    InnerTestCompactHashKey(true, "uint64", "cuckoo", false);
    InnerTestCompactHashKey(true, "string", "cuckoo", false);
    InnerTestCompactHashKey(true, "uint32", "cuckoo", true);
    InnerTestCompactHashKey(true, "int16", "cuckoo", true);
}

void KVTableInteTest::TestKVFormatOption()
{
    InnerTestKVFormatOption(true);
    InnerTestKVFormatOption(false);
}

void KVTableInteTest::InnerTestKVFormatOption(bool isShortOffset)
{
    tearDown();
    setUp();

    string field = "key:string;value:int8:true";
    mSchema = CreateSchema(field, "key", "value");
    const KVIndexConfigPtr& kvIndexConfig =
        DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    const KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
    const KVIndexPreference::HashDictParam& dictParam = kvIndexPreference.GetHashDictParam();

    KVIndexPreference::HashDictParam newDictParam(dictParam.GetHashType(), dictParam.GetOccupancyPct(),
                                                  dictParam.UsePreciseCountWhenMerge(),
                                                  dictParam.HasEnableCompactHashKey(), isShortOffset);
    kvIndexConfig->GetIndexPreference().SetHashDictParam(newDictParam);

    string docString = "cmd=add,key=key1,value=1 2,ts=101000000;"
                       "cmd=add,key=key2,value=2 3,ts=102000000;"
                       "cmd=add,key=key,value=3 4,ts=103000000;"
                       "cmd=add,key=key,value=3 5,ts=104000000";
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetBuildConfig(false).enablePackageFile = false;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    string filePath = "segment_0_level_0/index/key/format_option";
    string formatStr;
    GET_CHECK_DIRECTORY()->Load(filePath, formatStr);

    bool expectShortOffset = isShortOffset && (mSchema->GetRegionCount() == 1);
    KVFormatOptions kvOptions;
    kvOptions.FromString(formatStr);
    ASSERT_EQ(expectShortOffset, kvOptions.IsShortOffset());
}

void KVTableInteTest::TestFixLenValueAutoInline()
{
    string field = "key:int32;value:int8";
    mSchema = CreateSchema(field, "key", "key;value");
    const KVIndexConfigPtr& kvIndexConfig =
        DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
    KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
    valueParam.EnableFixLenAutoInline();
    kvIndexPreference.SetValueParam(valueParam);

    // build u64 offset
    string docString = "cmd=add,key=1,value=1,ts=101000000;"
                       "cmd=add,key=2,value=2,ts=102000000;"
                       "cmd=add,key=3,value=3,ts=103000000;"
                       "cmd=add,key=4,value=4,ts=104000000;"
                       "cmd=delete,key=2,ts=105000000";
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetBuildConfig(false).keepVersionCount = 20;
    options.GetBuildConfig(false).buildTotalMemory = 32;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4"));
    string incDoc = "cmd=delete,key=4,ts=106000000;"
                    "cmd=add,key=5,value=5,ts=107000000";

    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "5", "value=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, incDoc, "4", ""));

    string rtDoc = "cmd=delete,key=3,ts=108000000;"
                   "cmd=add,key=6,value=6,ts=109000000";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "6", "value=6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));

    string incDoc1 = "cmd=delete,key=3,ts=108000000;";
    string incDoc2 = "cmd=add,key=6,value=6,ts=109000000";

    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc1, "3", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc2, "6", "value=6"));
}

void KVTableInteTest::TestCompressFloatValueAutoInline()
{
    // fp16
    {
        string field = "key:int32;value:float:false:false:fp16";
        mSchema = CreateSchema(field, "key", "key;value");
        const KVIndexConfigPtr& kvIndexConfig =
            DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
        KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
        valueParam.EnableFixLenAutoInline();
        kvIndexPreference.SetValueParam(valueParam);

        // build u64 offset
        string docString = "cmd=add,key=1,value=1.1,ts=101000000;"
                           "cmd=add,key=2,value=2.1,ts=102000000;"
                           "cmd=add,key=3,value=3.1,ts=103000000;"
                           "cmd=add,key=4,value=4.1,ts=104000000;"
                           "cmd=delete,key=2,ts=105000000";
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(false).buildTotalMemory = 32;
        options.GetBuildConfig(false).keepVersionCount = 20;
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH() + "/fp16"));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4.09766"));
        string incDoc = "cmd=delete,key=4,ts=106000000;"
                        "cmd=add,key=5,value=5.1,ts=107000000";

        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "5", "value=5.09766"));
        ASSERT_TRUE(psm.Transfer(QUERY, incDoc, "4", ""));

        string rtDoc = "cmd=delete,key=3,ts=108000000;"
                       "cmd=add,key=6,value=6.1,ts=109000000";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "6", "value=6.09766"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));

        string incDoc1 = "cmd=delete,key=3,ts=108000000;";
        string incDoc2 = "cmd=add,key=6,value=6.1,ts=109000000";

        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc1, "3", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc2, "6", "value=6.09766"));
    }
    tearDown();
    setUp();
    // int8
    {
        string field = "key:int32;value:float:false:false:int8#7";
        mSchema = CreateSchema(field, "key", "key;value");
        const KVIndexConfigPtr& kvIndexConfig =
            DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
        KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
        valueParam.EnableFixLenAutoInline();
        kvIndexPreference.SetValueParam(valueParam);

        // build u64 offset
        string docString = "cmd=add,key=1,value=1.1,ts=101000000;"
                           "cmd=add,key=2,value=2.1,ts=102000000;"
                           "cmd=add,key=3,value=3.1,ts=103000000;"
                           "cmd=add,key=4,value=4.1,ts=104000000;"
                           "cmd=delete,key=2,ts=105000000";
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(false).keepVersionCount = 20;
        options.GetBuildConfig(false).buildTotalMemory = 32;
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH() + +"/int8"));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4.07874"));
        string incDoc = "cmd=delete,key=4,ts=106000000;"
                        "cmd=add,key=5,value=5.1,ts=107000000";

        EXPECT_TRUE(psm.Transfer(BUILD_INC, incDoc, "5", "value=5.12598"));
        ASSERT_TRUE(psm.Transfer(QUERY, incDoc, "4", ""));

        string rtDoc = "cmd=delete,key=3,ts=108000000;"
                       "cmd=add,key=6,value=6.1,ts=109000000";
        EXPECT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "6", "value=6.11811"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));

        string incDoc1 = "cmd=delete,key=3,ts=108000000;";
        string incDoc2 = "cmd=add,key=6,value=6.1,ts=109000000";

        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc1, "3", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc2, "6", "value=6.11811"));
    }
}

void KVTableInteTest::TestCompactValueWithDelete()
{
    string field = "key:int32;value:int16";
    mSchema = CreateSchema(field, "key", "value");
    const KVIndexConfigPtr& kvIndexConfig =
        DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
    KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
    valueParam.EnableFixLenAutoInline();
    kvIndexPreference.SetValueParam(valueParam);

    // build u64 offset
    string docString = prepareDoc(0, 1000, {}, 101000000);
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    options.GetBuildConfig(false).shardingColumnNum = 2;
    options.GetBuildConfig(false).levelNum = 3;
    options.GetBuildConfig(false).keepVersionCount = 100;
    options.GetBuildConfig(false).buildTotalMemory = 50; // 50MB
    options.GetBuildConfig(true).buildTotalMemory = 50;  // 50MB
    MergeConfig& mergeConfig = options.GetMergeConfig();
    mergeConfig.mergeStrategyStr = "shard_based";
    mergeConfig.mergeStrategyParameter.strategyConditions = "space_amplification=1.8";
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    docString = prepareDoc(1000, 500, {1, 2, 3, 1001, 1002, 1003}, 102000000);
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "5", "value=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "6", "value=6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1001", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1002", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1003", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1004", "value=1004"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1005", "value=1005"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1006", "value=1006"));
    docString = prepareDoc(0, 4, {1004, 1005, 1006}, 103000000);
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "5", "value=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "6", "value=6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1001", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1002", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1003", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1004", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1005", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1006", ""));
    CheckLevelInfo({{}, {1, {-1, 5}}, {0, {4, 2}}});
}

void KVTableInteTest::TestMergeNoBottomWithNoDelete()
{
    string field = "key:int32;value:int16";
    mSchema = CreateSchema(field, "key", "value");
    const KVIndexConfigPtr& kvIndexConfig =
        DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
    KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
    valueParam.EnableFixLenAutoInline();
    kvIndexPreference.SetValueParam(valueParam);

    // build u64 offset
    string docString = prepareDoc(0, 1000, {}, 101000000);
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    options.GetBuildConfig(false).shardingColumnNum = 2;
    options.GetBuildConfig(false).levelNum = 3;
    options.GetBuildConfig(false).keepVersionCount = 100;
    options.GetBuildConfig(false).buildTotalMemory = 50; // 50MB
    options.GetBuildConfig(true).buildTotalMemory = 50;  // 50MB
    MergeConfig& mergeConfig = options.GetMergeConfig();
    mergeConfig.mergeStrategyStr = "shard_based";
    mergeConfig.mergeStrategyParameter.strategyConditions = "space_amplification=1.8";
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    docString = prepareDoc(1000, 500, {2}, 102000000);
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1004", "value=1004"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1005", "value=1005"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1006", "value=1006"));
    CheckLevelInfo({{}, {1, {-1, 5}}, {0, {4, 2}}});
}

void KVTableInteTest::CheckLevelInfo(const vector<LevelMetaInfo>& levelInfos)
{
    Version version;
    VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSIONID);
    indexlibv2::framework::LevelInfo info = version.GetLevelInfo();

    ASSERT_EQ(levelInfos.size(), info.GetLevelCount());
    for (size_t i = 0; i < levelInfos.size(); ++i) {
        ASSERT_EQ(levelInfos[i].first, info.levelMetas[i].cursor);
        ASSERT_EQ(levelInfos[i].second, info.levelMetas[i].segments);
    }
}

string KVTableInteTest::prepareDoc(int64_t baseKey, int64_t docCount, vector<int64_t> deleteKeys, int64_t timestamp)
{
    stringstream ss;
    for (int64_t i = 0; i < docCount; ++i) {
        ss << "cmd=add,key=" << baseKey + i << ",value=" << baseKey + i << ",ts=" << timestamp << ";";
    }
    for (auto key : deleteKeys) {
        ss << "cmd=delete,key=" << key << ",ts=" << timestamp << ";";
    }
    return ss.str();
}

void KVTableInteTest::TestCompactValueWithDenseHash()
{
    {
        // single attribute
        tearDown();
        setUp();
        string field = "key:int32;value:int16";
        mSchema = CreateSchema(field, "key", "value");
        const KVIndexConfigPtr& kvIndexConfig =
            DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
        KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
        valueParam.EnableFixLenAutoInline();
        kvIndexPreference.SetValueParam(valueParam);
        // build u64 offset
        string docString = "cmd=add,key=1,value=1,ts=101000000;"
                           "cmd=add,key=2,value=2,ts=102000000;"
                           "cmd=add,key=3,value=3,ts=103000000;"
                           "cmd=add,key=4,value=4,ts=104000000;"
                           "cmd=delete,key=2,ts=105000000";
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(false).buildTotalMemory = 32;

        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4"));

        string incDoc = "cmd=delete,key=4,ts=106000000;"
                        "cmd=add,key=5,value=5,ts=107000000";

        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "5", "value=5"));
        ASSERT_TRUE(psm.Transfer(QUERY, incDoc, "4", ""));

        string rtDoc = "cmd=delete,key=3,ts=108000000;"
                       "cmd=add,key=6,value=6,ts=109000000";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "6", "value=6"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    }
    {
        // single attribute, string fixed length
        tearDown();
        setUp();
        string field = "key:int32;value:string:false:::6";
        mSchema = CreateSchema(field, "key", "value");
        const KVIndexConfigPtr& kvIndexConfig =
            DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
        KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
        valueParam.EnableFixLenAutoInline();
        kvIndexPreference.SetValueParam(valueParam);

        // build u64 offset
        string docString = "cmd=add,key=1,value=aaaaa1,ts=101000000;"
                           "cmd=add,key=2,value=aaaaa2,ts=102000000;"
                           "cmd=add,key=3,value=aaaaa3,ts=103000000;"
                           "cmd=add,key=4,value=aaaaa4,ts=104000000;"
                           "cmd=delete,key=2,ts=105000000";
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(false).buildTotalMemory = 32;

        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=aaaaa4"));

        string incDoc = "cmd=delete,key=4,ts=106000000;"
                        "cmd=add,key=5,value=aaaaa5,ts=107000000";

        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "5", "value=aaaaa5"));
        ASSERT_TRUE(psm.Transfer(QUERY, incDoc, "4", ""));

        string rtDoc = "cmd=delete,key=3,ts=108000000;"
                       "cmd=add,key=6,value=aaaaa6,ts=109000000";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "6", "value=aaaaa6"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    }
    {
        // multi attribute fixed length
        tearDown();
        setUp();
        string field = "key:int32;value:int16;value2:int8";
        mSchema = CreateSchema(field, "key", "value;value2");
        const KVIndexConfigPtr& kvIndexConfig =
            DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
        KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
        valueParam.EnableFixLenAutoInline();
        kvIndexPreference.SetValueParam(valueParam);

        // build u64 offset
        string docString = "cmd=add,key=1,value=1,value2=1,ts=101000000;"
                           "cmd=add,key=2,value=2,value2=2,ts=102000000;"
                           "cmd=add,key=3,value=3,value2=3,ts=103000000;"
                           "cmd=add,key=4,value=4,value2=4,ts=104000000;"
                           "cmd=delete,key=2,ts=105000000";
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(false).buildTotalMemory = 32;
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4,value2=4"));

        string incDoc = "cmd=delete,key=4,ts=106000000;"
                        "cmd=add,key=5,value=5,value2=5,ts=107000000";

        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "5", "value=5,value2=5"));
        ASSERT_TRUE(psm.Transfer(QUERY, incDoc, "4", ""));

        string rtDoc = "cmd=delete,key=3,ts=108000000;"
                       "cmd=add,key=6,value=6,value2=6,ts=109000000";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "6", "value=6,value2=6"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    }
    {
        // single attribute, multi value, fixed length
        tearDown();
        setUp();
        string field = "key:int32;value:int16:true:::3";
        mSchema = CreateSchema(field, "key", "value");
        const KVIndexConfigPtr& kvIndexConfig =
            DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
        KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
        valueParam.EnableFixLenAutoInline();
        kvIndexPreference.SetValueParam(valueParam);

        // build u64 offset
        string docString = "cmd=add,key=1,value=1 2 3,ts=101000000;"
                           "cmd=add,key=2,value=2 3 4,ts=102000000;"
                           "cmd=add,key=3,value=3 4 5,ts=103000000;"
                           "cmd=add,key=4,value=4 5 6,ts=104000000;"
                           "cmd=delete,key=2,ts=105000000";
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(false).buildTotalMemory = 32;
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4 5 6"));

        string incDoc = "cmd=delete,key=4,ts=106000000;"
                        "cmd=add,key=5,value=5 6 7,ts=107000000";

        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "5", "value=5 6 7"));
        ASSERT_TRUE(psm.Transfer(QUERY, incDoc, "4", ""));

        string rtDoc = "cmd=delete,key=3,ts=108000000;"
                       "cmd=add,key=6,value=6 7 8,ts=109000000";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "6", "value=6 7 8"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    }
}

void KVTableInteTest::TestCompactValueWithCuckooHash()
{
    {
        // single attribute
        tearDown();
        setUp();
        string field = "key:int32;value:int16";
        mSchema = CreateSchema(field, "key", "value");
        const KVIndexConfigPtr& kvIndexConfig =
            DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
        KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
        valueParam.EnableFixLenAutoInline();
        kvIndexPreference.SetValueParam(valueParam);
        KVIndexPreference::HashDictParam dictParam("cuckoo");
        kvIndexPreference.SetHashDictParam(dictParam);
        // build u64 offset
        string docString = "cmd=add,key=1,value=1,ts=101000000;"
                           "cmd=add,key=2,value=2,ts=102000000;"
                           "cmd=add,key=3,value=3,ts=103000000;"
                           "cmd=add,key=4,value=4,ts=104000000;"
                           "cmd=delete,key=2,ts=105000000";
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(false).buildTotalMemory = 32;

        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4"));

        string incDoc = "cmd=delete,key=4,ts=106000000;"
                        "cmd=add,key=5,value=5,ts=107000000";

        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "5", "value=5"));
        ASSERT_TRUE(psm.Transfer(QUERY, incDoc, "4", ""));

        string rtDoc = "cmd=delete,key=3,ts=108000000;"
                       "cmd=add,key=6,value=6,ts=109000000";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "6", "value=6"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    }
    {
        // single attribute, string fixed length
        tearDown();
        setUp();
        string field = "key:int32;value:string:false:::6";
        mSchema = CreateSchema(field, "key", "value");
        const KVIndexConfigPtr& kvIndexConfig =
            DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
        KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
        valueParam.EnableFixLenAutoInline();
        kvIndexPreference.SetValueParam(valueParam);
        KVIndexPreference::HashDictParam dictParam("cuckoo");
        kvIndexPreference.SetHashDictParam(dictParam);
        // build u64 offset
        string docString = "cmd=add,key=1,value=aaaaa1,ts=101000000;"
                           "cmd=add,key=2,value=aaaaa2,ts=102000000;"
                           "cmd=add,key=3,value=aaaaa3,ts=103000000;"
                           "cmd=add,key=4,value=aaaaa4,ts=104000000;"
                           "cmd=delete,key=2,ts=105000000";
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(false).buildTotalMemory = 32;
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=aaaaa4"));

        string incDoc = "cmd=delete,key=4,ts=106000000;"
                        "cmd=add,key=5,value=aaaaa5,ts=107000000";

        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "5", "value=aaaaa5"));
        ASSERT_TRUE(psm.Transfer(QUERY, incDoc, "4", ""));

        string rtDoc = "cmd=delete,key=3,ts=108000000;"
                       "cmd=add,key=6,value=aaaaa6,ts=109000000";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "6", "value=aaaaa6"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    }
    {
        // multi attribute fixed length
        tearDown();
        setUp();
        string field = "key:int32;value:int16;value2:int8";
        mSchema = CreateSchema(field, "key", "value;value2");
        const KVIndexConfigPtr& kvIndexConfig =
            DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
        KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
        valueParam.EnableFixLenAutoInline();
        kvIndexPreference.SetValueParam(valueParam);
        KVIndexPreference::HashDictParam dictParam("cuckoo");
        kvIndexPreference.SetHashDictParam(dictParam);
        // build u64 offset
        string docString = "cmd=add,key=1,value=1,value2=1,ts=101000000;"
                           "cmd=add,key=2,value=2,value2=2,ts=102000000;"
                           "cmd=add,key=3,value=3,value2=3,ts=103000000;"
                           "cmd=add,key=4,value=4,value2=4,ts=104000000;"
                           "cmd=delete,key=2,ts=105000000";
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(false).buildTotalMemory = 32;
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4,value2=4"));

        string incDoc = "cmd=delete,key=4,ts=106000000;"
                        "cmd=add,key=5,value=5,value2=5,ts=107000000";

        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "5", "value=5,value2=5"));
        ASSERT_TRUE(psm.Transfer(QUERY, incDoc, "4", ""));

        string rtDoc = "cmd=delete,key=3,ts=108000000;"
                       "cmd=add,key=6,value=6,value2=6,ts=109000000";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "6", "value=6,value2=6"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    }
    {
        // single attribute, multi value, fixed length
        tearDown();
        setUp();
        string field = "key:int32;value:int16:true:::3";
        mSchema = CreateSchema(field, "key", "value");
        const KVIndexConfigPtr& kvIndexConfig =
            DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
        KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
        valueParam.EnableFixLenAutoInline();
        kvIndexPreference.SetValueParam(valueParam);
        KVIndexPreference::HashDictParam dictParam("cuckoo");
        kvIndexPreference.SetHashDictParam(dictParam);
        // build u64 offset
        string docString = "cmd=add,key=1,value=1 2 3,ts=101000000;"
                           "cmd=add,key=2,value=2 3 4,ts=102000000;"
                           "cmd=add,key=3,value=3 4 5,ts=103000000;"
                           "cmd=add,key=4,value=4 5 6,ts=104000000;"
                           "cmd=delete,key=2,ts=105000000";
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(false).buildTotalMemory = 32;
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4 5 6"));

        string incDoc = "cmd=delete,key=4,ts=106000000;"
                        "cmd=add,key=5,value=5 6 7,ts=107000000";

        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "5", "value=5 6 7"));
        ASSERT_TRUE(psm.Transfer(QUERY, incDoc, "4", ""));

        string rtDoc = "cmd=delete,key=3,ts=108000000;"
                       "cmd=add,key=6,value=6 7 8,ts=109000000";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "6", "value=6 7 8"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    }
}

void KVTableInteTest::TestAdaptiveReadWithShortOffsetReader()
{
    string field = "key:string;value:int8:true";
    mSchema = CreateSchema(field, "key", "value");
    const KVIndexConfigPtr& kvIndexConfig =
        DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    const KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
    KVIndexPreference::HashDictParam dictParam = kvIndexPreference.GetHashDictParam();

    KVIndexPreference::HashDictParam newDictParam(dictParam.GetHashType(), dictParam.GetOccupancyPct(),
                                                  dictParam.UsePreciseCountWhenMerge(),
                                                  dictParam.HasEnableCompactHashKey(), false);
    kvIndexConfig->GetIndexPreference().SetHashDictParam(newDictParam);

    {
        // build u64 offset
        string docString = "cmd=add,key=key1,value=1 2,ts=101000000;"
                           "cmd=add,key=key2,value=2 3,ts=102000000;"
                           "cmd=add,key=key3,value=3 4,ts=103000000;"
                           "cmd=add,key=key4,value=3 5,ts=104000000";
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        options.GetBuildConfig(false).enablePackageFile = false;

        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "key4", "value=3 5"));
    }

    DirectoryPtr editDirectory = GET_CHECK_DIRECTORY(false);
    editDirectory->RemoveFile("schema.json");
    kvIndexConfig->GetIndexPreference().SetHashDictParam(dictParam);
    SchemaAdapter::StoreSchema(editDirectory, mSchema);
    editDirectory->RemoveFile("deploy_meta.0");
    editDirectory->RemoveFile("entry_table.0");

    // build u32 offset
    string docString = "cmd=add,key=key5,value=5 2,ts=201000000;"
                       "cmd=add,key=key6,value=6 3,ts=202000000;"
                       "cmd=add,key=key7,value=7 4,ts=203000000;"
                       "cmd=add,key=key8,value=8 5,ts=204000000";
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetBuildConfig(false).enablePackageFile = false;

    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString, "", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "key5", "value=5 2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "key4", "value=3 5"));
}

void KVTableInteTest::InnerTestCompactHashKey(bool isVarHashTable, const string& keyType, const string& dictType,
                                              bool expectCompact)
{
    tearDown();
    setUp();

    string field = "key:" + keyType + ";value:uint64";
    string valueStr = isVarHashTable ? "value;key" : "value";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", valueStr);
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KVIndexPreference::HashDictParam dictParam(dictType, 50, true, false, false);
    kvConfig->GetIndexPreference().SetHashDictParam(dictParam);

    string docString = "cmd=add,key=1,value=1,ts=10000000;"
                       "cmd=add,key=2,value=2,ts=30000000";
    size_t keyFileSize = 0;
    string keyFilePath = "segment_1_level_1/index/key/key";
    {
        // build not compact hash key
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/normal"));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH() + "/normal", -1, "normal",
                                                           FSMT_READ_ONLY, nullptr));
        DirectoryPtr dir = GET_PARTITION_DIRECTORY()->GetDirectory("normal", true);
        assert(dir);
        keyFileSize = dir->GetFileLength(keyFilePath);
    }

    KVIndexPreference::HashDictParam compactDictParam(dictType, 50, true, true, false);
    kvConfig->GetIndexPreference().SetHashDictParam(compactDictParam);
    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    if (GET_CASE_PARAM()) {
        jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "mmap" }]
    })";
    }

    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    size_t compactKeyFileSize = GET_PARTITION_DIRECTORY()->GetFileLength(keyFilePath);
    if (expectCompact && schema->GetRegionCount() == 1) {
        ASSERT_LT(compactKeyFileSize, keyFileSize);
    } else {
        ASSERT_EQ(compactKeyFileSize, keyFileSize);
    }
    string incDocString = "cmd=delete,key=1,ts=40000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "1", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));

    string rtDocString = "cmd=delete,key=2,ts=50000000;"
                         "cmd=add,key=1,value=3,ts=60000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=3"));
}

void KVTableInteTest::MakeEmptyFullIndexWithIndexFormatVersion(const IndexPartitionSchemaPtr& schema,
                                                               const IndexPartitionOptions& options,
                                                               const string& formatVersionStr, const string& rootPath)
{
    // make empty index with legacy index format
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, rootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));

    string filePath = PathUtil::JoinPath(rootPath, INDEX_FORMAT_VERSION_FILE_NAME);
    IndexFormatVersion version(formatVersionStr);
    FslibWrapper::DeleteFileE(filePath, DeleteOption::NoFence(true));
    version.Store(filePath, FenceContext::NoFence());
}

void KVTableInteTest::TestShortOffset()
{
    // dense
    InnerTestShortOffset("uint32", "dense", false, false, true);
    InnerTestShortOffset("uint32", "dense", false, false, false);

    InnerTestShortOffset("uint64", "dense", true, true, true);
    InnerTestShortOffset("uint64", "dense", true, true, false);

    InnerTestShortOffset("uint32", "dense", true, false, true);
    InnerTestShortOffset("uint32", "dense", true, false, false);

    InnerTestShortOffset("uint64", "dense", false, true, true);
    InnerTestShortOffset("uint64", "dense", false, true, false);

    // cuckoo
    InnerTestShortOffset("uint64", "cuckoo", false, false, true);
    InnerTestShortOffset("uint64", "cuckoo", false, false, false);

    InnerTestShortOffset("uint32", "cuckoo", true, true, true);
    InnerTestShortOffset("uint32", "cuckoo", true, true, false);

    InnerTestShortOffset("uint64", "cuckoo", true, false, true);
    InnerTestShortOffset("uint64", "cuckoo", true, false, false);

    InnerTestShortOffset("uint32", "cuckoo", false, true, true);
    InnerTestShortOffset("uint32", "cuckoo", false, true, false);
}

void KVTableInteTest::InnerTestShortOffset(const string& keyType, const string& dictType, bool enableTTL,
                                           bool useCompress, bool useShortOffset)
{
    tearDown();
    setUp();

    string field = "key:" + keyType + ";value:uint64";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value;key");
    schema->SetEnableTTL(enableTTL);

    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    KVIndexPreference::HashDictParam dictParam(dictType, 50, true, true, useShortOffset);
    kvConfig->GetIndexPreference().SetHashDictParam(dictParam);
    if (useCompress) {
        KVIndexPreference::ValueParam valueParam(false, "lz4");
        valueParam.EnableValueImpact(kvConfig->GetIndexPreference().GetValueParam().IsValueImpact());
        valueParam.EnablePlainFormat(kvConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
        kvConfig->GetIndexPreference().SetValueParam(valueParam);
    }

    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    if (GET_CASE_PARAM()) {
        jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "mmap" }]
    })";
    }

    string docString = "cmd=add,key=1,value=1,ts=10000000;"
                       "cmd=add,key=2,value=2,ts=30000000;";
    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "key=1,value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "key=2,value=2"));

    string incDocString = "cmd=delete,key=1,ts=40000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "1", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "key=2,value=2"));

    string rtDocString = "cmd=delete,key=2,ts=50000000;"
                         "cmd=add,key=1,value=3,ts=60000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "key=1,value=3"));

    rtDocString = "cmd=delete,key=1,ts=70000000;"
                  "cmd=add,key=3,value=4,ts=80000000;"
                  "cmd=add,key=4,value=5,ts=90000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "1", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "key=3,value=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "key=4,value=5"));
}

void KVTableInteTest::TestLegacyFormat()
{
    // dense
    InnerTestLegacyFormat("uint64", "dense", false, false);
}

void KVTableInteTest::InnerTestLegacyFormat(const string& keyType, const string& dictType, bool enableTTL,
                                            bool useCompress)
{
    string field = "key:" + keyType + ";value:uint64";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value;key");
    schema->SetEnableTTL(enableTTL);

    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    KVIndexPreference::HashDictParam dictParam(dictType, 50, true, false, false);
    kvConfig->GetIndexPreference().SetHashDictParam(dictParam);
    if (useCompress) {
        KVIndexPreference::ValueParam valueParam(false, "lz4");
        valueParam.EnableValueImpact(kvConfig->GetIndexPreference().GetValueParam().IsValueImpact());
        valueParam.EnablePlainFormat(kvConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
        kvConfig->GetIndexPreference().SetValueParam(valueParam);
    }

    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    if (GET_CASE_PARAM()) {
        jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "mmap" }]
    })";
    }

    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    mOptions.SetEnablePackageFile(false);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));

    uint64_t specialKey = (uint64_t)0xFFFFFFFFFFFFFEFFUL;
    string specialKeyStr = StringUtil::toString(specialKey);
    string docString = "cmd=add,key=1,value=1,ts=10000000;"
                       "cmd=add,key=2,value=2,ts=30000000;"
                       "cmd=add,key=" +
                       specialKeyStr + ",value=10,ts=30000000;";

    ASSERT_TRUE(psm.Transfer(PE_BUILD_FULL, docString, "", ""));
    DirectoryPtr rootDir = GET_CHECK_DIRECTORY(false);
    rootDir->RemoveFile("segment_0_level_0/index/key/format_option");
    // ignore check deploy_index
    rootDir->RemoveFile("segment_0_level_0/deploy_index");
    rootDir->RemoveFile("segment_0_level_0/segment_file_list");
    rootDir->RemoveFile("entry_table.0");
    rootDir->RemoveFile("deploy_meta.0");
    rootDir->Store("segment_0_level_0/deploy_index", DeployIndexMeta().ToString());

    partition::OnlinePartitionPtr onlinePartition(new partition::OnlinePartition());
    IndexPartition::OpenStatus rs =
        onlinePartition->Open(GET_TEMP_DATA_PATH(), "", schema, mOptions, INVALID_VERSIONID);
    ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, rs);
}

void KVTableInteTest::TestMultiFloatField()
{
    string field = "nid:uint64;musicid:uint64;score:float:true;nick:string;age:uint8";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "musicid", "musicid;nid;score");

    string docString = "cmd=add,musicid=1,nid=1,score=1.1 2.1;"
                       "cmd=add,musicid=2,nid=2,score=2.1 -6.1;"
                       "cmd=add,musicid=3,nid=3,score=-1.1 0.12371;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "musicid=1,nid=1,score=1.1 2.1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "musicid=2,nid=2,score=2.1 -6.1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "musicid=3,nid=3,score=-1.1 0.12371"));
}

void KVTableInteTest::TestSingleFloatCompress()
{
    IndexPartitionSchemaPtr schema;
    auto setEnableInline = [&schema]() {
        const KVIndexConfigPtr& kvIndexConfig =
            DYNAMIC_POINTER_CAST(KVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
        KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
        valueParam.EnableFixLenAutoInline();
        kvIndexPreference.SetValueParam(valueParam);
    };
    // fp 16 + single field
    {
        string field = "nid:uint64;price:float:false:false:fp16;amount:uint64";
        schema = CreateSchema(field, "nid", "price");
        setEnableInline();
        schema->SetEnableTTL(false);
        string docString = "cmd=add,nid=1,price=10.1,amount=100;"
                           "cmd=add,nid=2,price=-10.2,amount=101;"
                           "cmd=add,nid=3,amount=102;";
        string incDoc = "cmd=add,nid=4,price=10.1,amount=100;"
                        "cmd=add,nid=1,price=10.2,amount=300;";

        IndexPartitionOptions options = mOptions;
        options.GetBuildConfig().ttl = 60;
        options.GetBuildConfig(false).enablePackageFile = false;
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH() + "/fp16"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "price=10.0938"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=-10.1953"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=0"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "price=10.0938"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=-10.1953"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=0"));

        // test build rt doc
        ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,nid=1,price=11.4,ts=105000000", "1", "price=11.3984"));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "price=11.3984"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=-10.1953"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "price=10.0938"));
    }

    // int8 + single field
    {
        string field = "nid:uint64;price:float:false:false:int8#20;amount:uint64";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "nid", "price");
        setEnableInline();
        schema->SetEnableTTL(true);
        string docString = "cmd=add,nid=1,price=10.1,amount=100;"
                           "cmd=add,nid=2,price=-10.2,amount=101;"
                           "cmd=add,nid=3,amount=102;";
        string incDoc = "cmd=add,nid=4,price=10.1,amount=100;"
                        "cmd=add,nid=1,price=10.2,amount=300;";

        IndexPartitionOptions options = mOptions;
        options.GetBuildConfig().ttl = 60;
        options.GetBuildConfig(false).enablePackageFile = false;
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH() + "/int8"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "price=10.0787"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=-10.2362"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=0"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "price=10.0787"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=-10.2362"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=0"));

        // test build rt doc
        ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,nid=1,price=11.4,ts=105000000", "1", "price=11.3386"));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "price=11.3386"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=-10.2362"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "price=10.0787"));
    }

    // fp16 + multi field
    {
        string field = "nid:uint64;price:float:false:false:fp16;amount:uint16";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "nid", "price;amount");
        setEnableInline();
        schema->SetEnableTTL(true);
        string docString = "cmd=add,nid=1,price=10.1,amount=100;"
                           "cmd=add,nid=2,price=10.2,amount=101;"
                           "cmd=add,nid=3;";

        string incDoc = "cmd=add,nid=4,price=10.1,amount=100;"
                        "cmd=add,nid=1,price=10.2,amount=300;";

        IndexPartitionOptions options = mOptions;
        options.GetBuildConfig().ttl = 600;
        options.GetBuildConfig(false).enablePackageFile = false;
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH() + "/fp16_2"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "price=10.0938, amount=100"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=10.1953, amount=101"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=0, amount=0"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "price=10.0938, amount=100"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=10.1953, amount=101"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=0, amount=0"));
        // test build rt doc
        ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,nid=1,price=11.4,amout=300,ts=105000000", "1", "price=11.3984"));
        ASSERT_TRUE(psm.Transfer(BUILD_RT, incDoc, "", ""));
        // ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "price=11.3984, amount=300"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=10.1953, amount=101"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=0, amount=0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "price=10.0938, amount=100"));
    }

    // int8 + multi field
    {
        string field = "nid:uint64;price:float:false:false:int8#20;amount:uint64";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "nid", "price;amount");
        setEnableInline();
        schema->SetEnableTTL(true);
        string docString = "cmd=add,nid=1,price=10.1,amount=100;"
                           "cmd=add,nid=2,price=10.2,amount=101;"
                           "cmd=add,nid=3,price=10.3,amount=102;";
        string incDoc = "cmd=add,nid=4,price=10.1,amount=100;"
                        "cmd=add,nid=1,price=10.2,amount=300;";

        IndexPartitionOptions options = mOptions;
        options.GetBuildConfig().ttl = 60;
        options.GetBuildConfig(false).enablePackageFile = false;
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH() + "/int8_2"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "price=10.0787,amount=100"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=10.2362,amount=101"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=10.2362,amount=102"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "price=10.0787, amount=100"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=10.2362, amount=101"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=10.2362, amount=102"));

        // test build rt doc
        ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,nid=1,price=11.4,amount=300", "1", "price=11.3386"));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=10.2362, amount=101"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=10.2362, amount=102"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "price=10.0787, amount=100"));
    }
}

void KVTableInteTest::TestCompressedFixedLengthFloatField()
{
    // compress multi float(blockfp) & single float(fp16)
    {
        string field = "nid:uint64;score:float:true:false:block_fp:4;"
                       "price:float:false:false:fp16";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "nid", "score;price");
        // value len = (9 + 2)*3 = 33
        string docString = "cmd=add,nid=1,score=1.1 2.1 2.3 3.4,price=10.1;"
                           "cmd=add,nid=2,score=2.1 -6.1,price=10.2;"
                           "cmd=add,nid=3,score=-1.1 0.12371 -2.3 4.0 5.1,price=10.3;";

        IndexPartitionOptions options = mOptions;
        options.GetBuildConfig(false).enablePackageFile = false;
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH() + "/block_fp"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        string valueFile = GET_TEMP_DATA_PATH() + "/block_fp/segment_0_level_0/index/nid/value";
        ASSERT_EQ(33, FslibWrapper::GetFileLength(valueFile).GetOrThrow());
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "score=1.09998 2.09998 2.30005 3.40002,price=10.0938"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "score=2.1001 -6.1001 0 0,price=10.1953"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "score=-1.1001 0.123779 -2.30005 4,price=10.2969"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "score=1.09998 2.09998 2.30005 3.40002,price=10.0938"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "score=2.1001 -6.1001 0 0,price=10.1953"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "score=-1.1001 0.123779 -2.30005 4,price=10.2969"));

        // test build rt doc
        ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,nid=1,price=11.4,ts=105000000", "1", "price=11.3984"));
    }

    // compress multi float(fp16) & single float(int8) & single float(no compress)
    {
        string field = "nid:uint64;score:float:true:false:fp16:4;"
                       "price:float:false:false:int8#20;discount:float";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "nid", "score;price;discount");
        // value len = (8+1+4)*3 = 39
        string docString = "cmd=add,nid=1,score=1.1 2.1 2.3 3.4,price=10.1,discount=0.9;"
                           "cmd=add,nid=2,score=2.1 -6.1,price=10.2,discount=0.8;"
                           "cmd=add,nid=3,score=-1.1 0.12371 -2.3 4.0 5.1,price=10.3,discount=0.7;";

        PartitionStateMachine psm;
        IndexPartitionOptions options = mOptions;
        options.GetBuildConfig(false).enablePackageFile = false;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH() + "/fp16"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        string valueFile = GET_TEMP_DATA_PATH() + "/fp16/segment_0_level_0/index/nid/value";
        ASSERT_EQ(39, FslibWrapper::GetFileLength(valueFile).GetOrThrow());
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "score=1.09961 2.09961 2.29883 3.39844,price=10.0787,discount=0.9"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "score=2.09961 -6.09766 0 0,price=10.2362,discount=0.8"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "score=-1.09961 0.123657 -2.29883 4,price=10.2362,discount=0.7"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "score=1.09961 2.09961 2.29883 3.39844,price=10.0787,discount=0.9"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "score=2.09961 -6.09766 0 0,price=10.2362,discount=0.8"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "score=-1.09961 0.123657 -2.29883 4,price=10.2362,discount=0.7"));

        ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,nid=1,price=11.4,ts=105000000", "1", "price=11.3386"));
    }

    // single compress field : int8, with search cache
    {
        string field = "nid:uint64;score:float:true:false:int8#127:4";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "nid", "score");

        string docString = "cmd=add,nid=1,score=1 2 2 3;"
                           "cmd=add,nid=2,score=2 -6;"
                           "cmd=add,nid=3,score=-1 0 -2 4 5;";

        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/int8"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "score=1 2 2 3"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "score=2 -6 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "score=-1 0 -2 4"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "score=1 2 2 3"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "score=2 -6 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "score=-1 0 -2 4"));
    }

    // combine fields : fix length
    {
        string field = "nid:uint64;price:int32;score:float:true:false:block_fp:4";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "nid", "score;price");

        string docString = "cmd=add,nid=1,price=2,score=1.1 2.1 2.3 3.4;"
                           "cmd=add,nid=2,price=4,score=2.1 -6.1;"
                           "cmd=add,nid=3,price=8,score=-1.1 0.12371 -2.3 4.0 5.1;";

        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/block_fp_fix"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "price=2,score=1.09998 2.09998 2.30005 3.40002"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=4,score=2.1001 -6.1001 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=8,score=-1.1001 0.123779 -2.30005 4"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "price=2,score=1.09998 2.09998 2.30005 3.40002"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "price=4,score=2.1001 -6.1001 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "price=8,score=-1.1001 0.123779 -2.30005 4"));
    }

    // combine fields : var length
    {
        string field = "nid:uint64;name:string;score:float:true:false:fp16:4";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "nid", "score;name");

        string docString = "cmd=add,nid=1,name=abc,score=1.1 2.1 2.3 3.4;"
                           "cmd=add,nid=2,name=1234,score=2.1 -6.1;"
                           "cmd=add,nid=3,name=helpme,score=-1.1 0.12371 -2.3 4.0 5.1;";

        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/fp16_var"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "name=abc,score=1.09961 2.09961 2.29883 3.39844"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "name=1234,score=2.09961 -6.09766 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "name=helpme,score=-1.09961 0.123657 -2.29883 4"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "name=abc,score=1.09961 2.09961 2.29883 3.39844"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "name=1234,score=2.09961 -6.09766 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "name=helpme,score=-1.09961 0.123657 -2.29883 4"));
    }
}

void KVTableInteTest::TestMergeWithCompressAndShortOffsetIsDisable()
{
    InnerTestMergeWithCompressAndShortOffsetIsDisable("dense", "snappy");
    InnerTestMergeWithCompressAndShortOffsetIsDisable("dense", "lz4");
    InnerTestMergeWithCompressAndShortOffsetIsDisable("dense", "zlib");
    InnerTestMergeWithCompressAndShortOffsetIsDisable("dense", "lz4hc");
    InnerTestMergeWithCompressAndShortOffsetIsDisable("dense", "zstd");
    InnerTestMergeWithCompressAndShortOffsetIsDisable("cuckoo", "snappy");
    InnerTestMergeWithCompressAndShortOffsetIsDisable("cuckoo", "lz4");
    InnerTestMergeWithCompressAndShortOffsetIsDisable("cuckoo", "zlib");
    InnerTestMergeWithCompressAndShortOffsetIsDisable("cuckoo", "lz4hc");
    InnerTestMergeWithCompressAndShortOffsetIsDisable("cuckoo", "zstd");
}

void KVTableInteTest::InnerTestMergeWithCompressAndShortOffsetIsDisable(const string& hashType,
                                                                        const string& compressType)
{
    tearDown();
    setUp();
    IndexPartitionOptions options = mOptions;
    options.GetBuildConfig(false).buildTotalMemory = 20;
    options.SetEnablePackageFile(false);
    string field = "key:int32;value:string:false;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "value;key");

    schema->SetEnableTTL(false);
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KVIndexPreference& kvIndexPreference = kvConfig->GetIndexPreference();
    const KVIndexPreference::HashDictParam& dictParam = kvIndexPreference.GetHashDictParam();
    KVIndexPreference::HashDictParam newDictParam(hashType, dictParam.GetOccupancyPct(),
                                                  dictParam.UsePreciseCountWhenMerge(), false, true);

    int64_t maxValueSize = (int64_t)4096;
    newDictParam.SetMaxValueSizeForShortOffset(maxValueSize);
    kvConfig->GetIndexPreference().SetHashDictParam(newDictParam);
    // set compress
    KVIndexPreference::ValueParam valueParam(false, compressType);
    valueParam.EnableValueImpact(kvConfig->GetIndexPreference().GetValueParam().IsValueImpact());
    valueParam.EnablePlainFormat(kvConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
    kvConfig->GetIndexPreference().SetValueParam(valueParam);
    // total value file length = 3000 * 3 > 4096
    // compressed value file length = 523 < 4096
    // expect merge will not enable short offset,
    // since actual value len is greater than MaxValueSizeForShortOffset
    string longSuffix(3000, '0');
    string docString = string("cmd=add,key=2,value=hello") + longSuffix + string(";") +
                       string("cmd=add,key=3,value=world") + longSuffix + string(";") +
                       string("cmd=add,key=4,value=kitty") + longSuffix + string(";");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", string("value=hello") + longSuffix));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", string("value=world") + longSuffix));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", string("value=kitty") + longSuffix));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr partDir = GET_PARTITION_DIRECTORY();
    string mergedSegDir = string("segment_3_level_1");

    ASSERT_TRUE(partDir->IsDir(mergedSegDir));
    string filePath = mergedSegDir + "/index/key/format_option";
    string formatStr;
    ASSERT_NO_THROW(GET_PARTITION_DIRECTORY()->Load(filePath, formatStr));
    KVFormatOptions kvOptions;
    kvOptions.FromString(formatStr);
    ASSERT_FALSE(kvOptions.IsShortOffset());

    string valuePath = mergedSegDir + "/index/key/value";
    ASSERT_TRUE(partDir->GetFileLength(valuePath) < (size_t)maxValueSize);
}

IndexPartitionSchemaPtr KVTableInteTest::CreateSchema(const string& fields, const string& key, const string& values,
                                                      int64_t ttl)
{
    std::string valueFormat = "";
    if (mImpactValue) {
        valueFormat = "impact";
    } else if (random() % 2 == 1) {
        valueFormat = "plain";
    }
    return SchemaMaker::MakeKVSchema(fields, key, values, ttl, valueFormat);
}

vector<document::DocumentPtr> KVTableInteTest::CreateKVDocuments(const IndexPartitionSchemaPtr& schema,
                                                                 const string& docStrs)
{
    vector<document::DocumentPtr> ret;
    auto kvDocs = DocumentCreator::CreateKVDocuments(schema, docStrs, DEFAULT_REGIONNAME);
    for (auto doc : kvDocs) {
        ret.push_back(doc);
    }
    return ret;
}

void KVTableInteTest::TestPartitionInfo()
{
    string field = "key:string;value:int8";
    mSchema = CreateSchema(field, "key", "value");

    string docString = "cmd=add,key=key1,value=1,ts=101000000;"
                       "cmd=add,key=key2,value=2,ts=102000000;"
                       "cmd=add,key=key3,value=3,ts=103000000;"
                       "cmd=add,key=key4,value=3,ts=103000000;"
                       "cmd=add,key=key5,value=3,ts=103000000;"
                       "cmd=add,key=key,value=31,ts=104000000"
                       "cmd=add,key=key,value=33,ts=104000000";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "key1", "value=1"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "key2", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "key2", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "key", "value=33"));

    auto partInfo = psm.GetIndexPartition()->GetReader()->GetPartitionInfo();
    ASSERT_TRUE(partInfo.get());
    ASSERT_TRUE(partInfo->IsKeyValueTable());
    EXPECT_EQ(6u, partInfo->GetTotalDocCount());

    string incDocString = "cmd=add,key=key13,value=3,ts=105000000;"
                          "cmd=add,key=key14,value=4,ts=106000000;"
                          "cmd=add,key=key,value=32,ts=107000000";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "key3", "value=3"));

    string rtDocString = "cmd=add,key=key23,value=3,ts=115000000;"
                         "cmd=add,key=key24,value=4,ts=116000000;"
                         "cmd=add,key=key,value=32,ts=117000000";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));

    partInfo = psm.GetIndexPartition()->GetReader()->GetPartitionInfo();
    ASSERT_TRUE(partInfo.get());
    EXPECT_EQ(11u, partInfo->GetTotalDocCount());

    const auto& partMetrics = partInfo->GetPartitionMetrics();
    EXPECT_EQ(8u, partMetrics.incDocCount);
    EXPECT_EQ(0u, partMetrics.delDocCount);
}

void KVTableInteTest::TestDumpingSearchCache()
{
    ASSERT_FALSE(mSchema->TTLEnabled());
    auto slowDumpContainer = new SlowDumpSegmentContainer(1000 * 1000);
    DumpSegmentContainerPtr dumpContainer;
    dumpContainer.reset(slowDumpContainer);
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
    PartitionStateMachine psm(false, IndexPartitionResource(), dumpContainer);
    EXPECT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));

    partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
    util::SearchCachePtr searchCache(
        new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
    resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

    string fullDocs = "cmd=add,key=1,value=1,ts=10000000;";
    EXPECT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "1", "value=1"));
    slowDumpContainer->EnableSlowSleep();

    string rtDocs = "cmd=add,key=1,value=2,ts=20000000";
    EXPECT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "1", "value=2;"));
    EXPECT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "", "1", "value=2;"));
    EXPECT_EQ(1u, dumpContainer->GetDumpItemSize());

    string rtDocs2 = "cmd=add,key=2,value=3,ts=30000000";
    EXPECT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "1", "value=2;"));
    EXPECT_EQ(1u, dumpContainer->GetDumpItemSize());
    slowDumpContainer->DisableSlowSleep();
}

void KVTableInteTest::TestCleanRealtimeIndex()
{
    // config
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().prohibitInMemDump = true;
    mOptions.GetOnlineConfig().enableAsyncFlushIndex = true;
    string jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "mmap" },
         { "file_patterns" : ["_KV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true, "cache_decompress_file" : true }}]
    })";
    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    mOptions.GetBuildConfig(false).maxDocCount = 1;
    mOptions.GetBuildConfig(true).maxDocCount = 1;

    string field = "key:uint64;uint32:uint32;mint8:int8:true;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "key", "key;uint32;mint8");
    if (GET_CASE_PARAM()) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        KVIndexPreference::ValueParam valueParam(false, "zstd");
        valueParam.EnableValueImpact(kvConfig->GetIndexPreference().GetValueParam().IsValueImpact());
        valueParam.EnablePlainFormat(kvConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
        kvConfig->GetIndexPreference().SetValueParam(valueParam);
        mOptions.SetEnablePackageFile(true);
    }

    // prepare
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));

    // build
    string rtString1 = "cmd=add,key=10,uint32=10,mint8=1 2,ts=101000000;"
                       "cmd=add,key=0,uint32=0,mint8=,ts=102000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtString1, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", "key=10,uint32=10,mint8=1 2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "0", "key=0,uint32=0,mint8="));
    IFileSystemPtr fileSystem = psm.GetIndexPartition()->GetFileSystem();
    string rtString2 = "cmd=add,key=0,uint32=1,mint8=2 3,ts=104000000;"
                       "cmd=add,key=5,uint32=5,mint8=3,ts=105000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtString2, "", ""));
    fileSystem->Sync(true).GetOrThrow();
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    fileSystem->CleanCache();
    ASSERT_TRUE(psm.Transfer(QUERY, "", "0", "key=0,uint32=1,mint8=2 3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "5", "key=5,uint32=5,mint8=3"));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, rtString1, "", ""));
}

}} // namespace indexlib::partition
