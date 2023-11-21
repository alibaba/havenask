#include "indexlib/partition/test/multi_region_kv_intetest.h"

#include "indexlib/config/test/region_schema_maker.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/fslib/DataFlushController.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/fslib/MultiPathDataFlushController.h"
#include "indexlib/index/kv/kv_factory.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::index;
using namespace indexlib::document;
using namespace indexlib::codegen;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, MultiRegionKVInteTest);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(MultiRegionKVInteTestMode, MultiRegionKVInteTest, testing::Values(false, true));

INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestBuildReadWithDiffType);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestNumberHash);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestSearchWithTimestamp);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestSearchWithTimestampForDefalutTTL);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestSearchWithMultiSegment);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestSearchKVWithDelete);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestAddSameDocs);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestAddKVWithoutValue);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestBuildShardingSegment);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestBuildLegacyKvDoc);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMergeTwoSegmentsSimple);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMergeSingleSegment);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMergeSameKey);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMergeDeleteKey);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMergeOptimizeSingleSegmentWithSharding);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestNoFullBuildData);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMultiPartitionMerge);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMergeWithSeparateMode);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMergeWithEmptyColumn);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestOptimizeMergeMultipleLevel);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMergeWithTTL);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMergeWithUserTs);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMergeWithDefaultTTL);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMergeWithTTLWhenAllDead);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMergeUntilBottomLevel);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestHashTableOneField);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestAddTrigerDump);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestReopenIncWithoutRt);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestReopenIncOlderThanAllRt);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestReopenIncCoverPartRtBuiltSegment);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestReopenIncCoverPartRtBuildingSegment);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestReopenIncCoverAllRt);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestReopenIncKeyAndRtKeyInSameSecond);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestForceReopenIncWithoutRt);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestForceReopenIncOlderThanAllRt);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestForceReopenIncCoverPartRtBuiltSegment);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestForceReopenIncCoverPartRtBuildingSegment);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestForceReopenIncCoverAllRt);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestReopenIncCoverBuiltAndBuildingSegment);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestForceReopenIncKeyAndRtKeyInSameSecond);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestOpenWithoutEnoughMemory);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestReopenWithoutEnoughMemory);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestReopenIncCoverBuiltSegmentWhenReaderHold);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestKeyIsExitError);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestAsyncDumpWithIncReopen);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestRtLoadBuiltRtSegmentWithIncCoverCore);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestRtIndexSizeWhenUseMultiValue);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestProhibitInMemDumpBug_13765192);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestBugFix14177981);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestBuildFixLengthWithLegacyFormat);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestShortOffsetWithDeleteDoc);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestKVFormatOption);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestAdaptiveReadWithShortOffsetReader);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestDumpingSearchCache);

// not support compact format
// INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestBuildFixLengthString);

// not support short offset
// INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestOffsetLimitTriggerForceDump);
// INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMergeWithShortOffset);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestHashTableOneStringField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestHashTableOneMultiField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestHashTableMultiField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestHashTableVarIndexMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestHashTableVarValueUseBlockCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestHashTableVarKeyUseBlockCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestHashTableFixUseBlockCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestHashTableFixVarValue);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestFlushRealtimeIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestReleaseMemoryForTmpValueFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestCompactHashKeyForFixHashTable);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestCompactHashKeyForVarHashTable);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestShortOffset);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKVInteTestMode, TestLegacyFormat);

INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestAddAndDel);
// INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestSetGlobalRegionPreference);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestDifferentTTL);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestNoTTL);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestTTL);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMultiField);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestSetOneRegionFieldSchema);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestSetTwoRegionFieldSchema);
INDEXLIB_UNIT_TEST_CASE(MultiRegionKVInteTest, TestMultiRegionWithFixLengthAttribute);

MultiRegionKVInteTest::MultiRegionKVInteTest() {}

MultiRegionKVInteTest::~MultiRegionKVInteTest() {}

void MultiRegionKVInteTest::CaseSetUp()
{
    MultiPathDataFlushController::GetDataFlushController("")->Reset();

    mOptions = IndexPartitionOptions();
    mOptions.GetBuildConfig(true).buildTotalMemory = 22;
    mOptions.GetBuildConfig(false).buildTotalMemory = 40;
    mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    mOptions.GetBuildConfig(false).levelNum = 2;
    mOptions.GetBuildConfig(false).keepVersionCount = 100;

    string field = "key:int32;value:uint64;";
    mSchema = CreateSchema(field, "key", "value");
}

void MultiRegionKVInteTest::CaseTearDown() {}

void MultiRegionKVInteTest::TestSimpleProcess()
{
    string field = "key1:string;key2:int32;iValue:uint32;"
                   "sValue:string;miValue:int32:true";

    RegionSchemaMaker maker(field, "kv");
    maker.AddRegion("region1", "key1", "key2;iValue;sValue;miValue", "", INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "key2", "iValue", "", INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    string docString = "cmd=add,key1=a,key2=1,iValue=1,miValue=1 2,sValue=abc,region_name=region1;"
                       "cmd=add,key1=b,key2=2,iValue=2,miValue=1 2,sValue=def,region_name=region1;"
                       "cmd=add,key1=b,key2=2,iValue=2,miValue=3 2,sValue=abcef,region_name=region2;"
                       "cmd=add,key1=a,key2=3,iValue=3,miValue=4 2,sValue=cef,region_name=region2;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=1,iValue=1,miValue=1 2,sValue=abc"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@b", "key2=2,iValue=2,miValue=1 2,sValue=def"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2", "iValue=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@3", "iValue=3"));

    string rtDocString = "cmd=add,key1=a,key2=3,iValue=3,miValue=1 2 3,sValue=ali,region_name=region1,ts=1000000;"
                         "cmd=add,key1=b,key2=4,iValue=5,miValue=2 3,sValue=alixing,region_name=region1,ts=1000000;"
                         "cmd=add,key1=a,key2=2,iValue=6,miValue=4,sValue=efg,region_name=region2,ts=1000000;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=3,iValue=3,miValue=1 2 3,sValue=ali"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@b", "key2=4,iValue=5,miValue=2 3,sValue=alixing"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2", "iValue=6"));
}

void MultiRegionKVInteTest::TestAddAndDel()
{
    string field = "key1:string;key2:int32;iValue:uint32;"
                   "sValue:string;miValue:int32:true";

    RegionSchemaMaker maker(field, "kv");
    maker.AddRegion("region1", "key1", "key2;iValue;sValue;miValue", "", INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "key2", "iValue", "", INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    // full build
    {
        string docString =
            // region1
            "cmd=add,key1=a,key2=1,iValue=1,miValue=1,sValue=abc1,region_name=region1,ts=101;"
            "cmd=add,key1=b,key2=2,iValue=2,miValue=2,sValue=abc2,region_name=region1,ts=102;"
            "cmd=add,key1=c,key2=3,iValue=3,miValue=3,sValue=abc3,region_name=region1,ts=103;"
            "cmd=add,key1=d,key2=4,iValue=4,miValue=4,sValue=abc4,region_name=region1,ts=104;"

            "cmd=add,key1=b,key2=2,iValue=5,miValue=5,sValue=abc5,region_name=region1,ts=105;"
            "cmd=delete,key1=c,region_name=region1,ts=106;"
            // region2
            "cmd=add,key2=1,iValue=6,region_name=region2,ts=107;"
            "cmd=add,key2=2,iValue=7,region_name=region2,ts=108;"
            "cmd=add,key2=3,iValue=8,region_name=region2,ts=109;"
            "cmd=add,key2=4,iValue=9,region_name=region2,ts=110;"
            "cmd=add,key2=5,iValue=10,region_name=region2,ts=111;"
            "cmd=add,key2=6,iValue=11,region_name=region2,ts=112;"

            "cmd=delete,key2=4,region_name=region2,ts=113;"
            "cmd=add,key2=5,iValue=12,region_name=region2,ts=114;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=1,iValue=1,miValue=1,sValue=abc1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@b", "key2=2,iValue=5,miValue=5,sValue=abc5"));
        ASSERT_FALSE(psm.Transfer(QUERY, "", "region1@c", "key2=3,iValue=3,miValue=3,sValue=abc3"));

        ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@1", "iValue=6"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@5", "iValue=12"));
        ASSERT_FALSE(psm.Transfer(QUERY, "", "region2@4", "iValue=9"));
    }
    // rt build
    {
        string docString =
            // region1:add delete dup
            "cmd=add,key1=e,key2=7,iValue=13,miValue=6,sValue=abc6,region_name=region1,ts=115;"
            "cmd=delete,key1=a,region_name=region1,ts=116;"
            "cmd=add,key1=d,key2=8,iValue=14,miValue=7,sValue=abc7,region_name=region1,ts=117;"
            // region2:add delete dup
            "cmd=add,key2=7,iValue=15,region_name=region2,ts=118;"
            "cmd=delete,key2=1,region_name=region2,ts=119;"
            "cmd=add,key2=2,iValue=16,region_name=region2,ts=120;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));

        ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@e", "key2=7,iValue=13,miValue=6,sValue=abc6"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@d", "key2=8,iValue=14,miValue=7,sValue=abc7"));
        ASSERT_FALSE(psm.Transfer(QUERY, "", "region1@a", "key2=1,iValue=1,miValue=1,sValue=abc1"));

        ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@7", "iValue=15"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2", "iValue=16"));
        ASSERT_FALSE(psm.Transfer(QUERY, "", "region2@1", "iValue=6"));
    }
    // inc build
    {
        string docString =
            // region1:add del dup
            "cmd=add,key1=f,key2=9,iValue=15,miValue=8,sValue=abc8,region_name=region1,ts=121;"
            "cmd=delete,key1=c,region_name=region1,ts=122;"
            "cmd=add,key1=b,key2=10,iValue=16,miValue=9,sValue=abc9,region_name=region1,ts=123;"

            // region2:add del dup
            "cmd=add,key2=8,iValue=17,region_name=region2,ts=124;"
            "cmd=delete,key2=2,region_name=region2,ts=125;"
            "cmd=add,key2=3,iValue=18,region_name=region2,ts=126;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, docString, "", ""));

        ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@f", "key2=9,iValue=15,miValue=8,sValue=abc8"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@b", "key2=10,iValue=16,miValue=9,sValue=abc9"));
        ASSERT_FALSE(psm.Transfer(QUERY, "", "region1@c", "key2=3,iValue=3,miValue=3,sValue=abc3"));

        ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@8", "iValue=17"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@3", "iValue=18"));
        ASSERT_FALSE(psm.Transfer(QUERY, "", "region2@2", "iValue=7"));
    }
}

void MultiRegionKVInteTest::TestSetGlobalRegionPreference()
{
    InnerTestSetGlobalRegionPreference("");
    InnerTestSetGlobalRegionPreference("lz4");
    InnerTestSetGlobalRegionPreference("zstd");
    InnerTestSetGlobalRegionPreference("snappy");
}

void MultiRegionKVInteTest::InnerTestSetGlobalRegionPreference(const string& valueCompressType)
{
    tearDown();
    setUp();

    string field = "key1:string;key2:int32;iValue:uint32;"
                   "sValue:string;miValue:int32:true";

    RegionSchemaMaker maker(field, "kv");
    maker.AddRegion("region1", "key1", "key2;iValue;sValue;miValue", "", INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "key2", "iValue", "", INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();
    MakeGlobalRegionIndexPreference(schema, "dense", valueCompressType);

    IndexPartitionOptions options;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key1=a,key2=1,iValue=1,miValue=1 2,sValue=abc,region_name=region1,ts=0;"
                       "cmd=add,key1=b,key2=2,iValue=2,miValue=1 2,sValue=def,region_name=region1,ts=0;"
                       "cmd=add,key1=b,key2=2,iValue=2,miValue=3 2,sValue=abcef,region_name=region2,ts=0;"
                       "cmd=add,key1=a,key2=3,iValue=3,miValue=4 2,sValue=cef,region_name=region2,ts=0;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=1,iValue=1,miValue=1 2,sValue=abc"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@b", "key2=2,iValue=2,miValue=1 2,sValue=def"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2", "iValue=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@3", "iValue=3"));

    DirectoryPtr segmentDir = GET_CHECK_DIRECTORY()->GetDirectory("segment_1_level_1", true);
    CheckCompress(segmentDir, "index/key1", valueCompressType);

    string rtDocString = "cmd=add,key1=a,key2=3,iValue=3,miValue=1 2 3,sValue=ali,region_name=region1,ts=1000000;"
                         "cmd=add,key1=b,key2=4,iValue=5,miValue=2 3,sValue=alixing,region_name=region1,ts=1000000;"
                         "cmd=add,key1=a,key2=2,iValue=6,miValue=4,sValue=efg,region_name=region2,ts=1000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=3,iValue=3,miValue=1 2 3,sValue=ali"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@b", "key2=4,iValue=5,miValue=2 3,sValue=alixing"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2", "iValue=6"));
    string rtDocString1 = "cmd=delete,key2=2,region_name=region2,ts=2000000;"
                          "cmd=add,key2=3,iValue=7,region_name=region2,ts=2000000;"
                          "cmd=add,key1=c,key2=8,iValue=5,miValue=2 3,sValue=aliren,region_name=region1,ts=2000000;"
                          "cmd=delete,key1=a,region_name=region1,ts=2000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString1, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@3", "iValue=7"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@c", "key2=8,iValue=5,miValue=2 3,sValue=aliren"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", ""));
    string rtPath = "rt_index_partition/segment_1073741824_level_0/index/key1/";

    // rt not compress
    CheckCompress(GET_CHECK_DIRECTORY(), rtPath, "");
}

void MultiRegionKVInteTest::TestDifferentTTL()
{
    string field = "key1:string;key2:int32;iValue:uint32;"
                   "sValue:string;miValue:int32:true";

    // region1 : ttl = 20; region2 : no ttl
    RegionSchemaMaker maker(field, "kv");
    maker.AddRegion("region1", "key1", "key2;iValue;sValue;miValue", "", 20, mImpactValue);
    maker.AddRegion("region2", "key2", "iValue", "", INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key1=a,key2=1,iValue=1,miValue=1 2,sValue=abc,region_name=region1,ts=0;"
                       "cmd=add,key2=2,iValue=2,region_name=region2,ts=0;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a#50", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a#10", "key2=1,iValue=1,miValue=1 2,sValue=abc"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2#50", "iValue=2"));

    string rtDocString = "cmd=delete,key2=2,region_name=region2,ts=2000000;"
                         "cmd=add,key2=3,iValue=7,region_name=region2,ts=3000000;"
                         "cmd=add,key1=c,key2=8,iValue=5,miValue=2 3,sValue=aliren,region_name=region1,ts=4000000;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@c#20", "key2=8,iValue=5,miValue=2 3,sValue=aliren"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@c#50", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@3#50", "iValue=7"));

    string incDocString = "cmd=delete,key2=2,region_name=region2,ts=2000000;"
                          "cmd=add,key2=3,iValue=7,region_name=region2,ts=3000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@c#20", "key2=8,iValue=5,miValue=2 3,sValue=aliren"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=1,iValue=1,miValue=1 2,sValue=abc"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@c#50", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@3#50", "iValue=7"));

    // ttl reclaim all docs in region1
    psm.SetCurrentTimestamp(25000000);
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@3", "iValue=7"));
}

void MultiRegionKVInteTest::TestTTL()
{
    string field = "key1:string;key2:int32;iValue:uint32;"
                   "sValue:string;miValue:int32:true";

    RegionSchemaMaker maker(field, "kv");
    maker.AddRegion("region1", "key1", "key2;iValue;sValue;miValue", "", 20, mImpactValue);
    maker.AddRegion("region2", "key2", "iValue", "", 100, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    IndexPartitionOptions options;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key1=a,key2=1,iValue=1,miValue=1 2,sValue=abc,region_name=region1,ts=0;"
                       "cmd=add,key2=2,iValue=2,region_name=region2,ts=0;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a#50", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a#10", "key2=1,iValue=1,miValue=1 2,sValue=abc"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2#50", "iValue=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2#150", ""));

    // reclaim all docs
    psm.SetCurrentTimestamp(200000000);
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2", ""));
}

void MultiRegionKVInteTest::TestNoTTL()
{
    string field = "key1:string;key2:int32;iValue:uint32;"
                   "sValue:string;miValue:int32:true";

    RegionSchemaMaker maker(field, "kv");
    maker.AddRegion("region1", "key1", "key2;iValue;sValue;miValue", "", INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "key2", "iValue", "", INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    IndexPartitionOptions options;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key1=a,key2=1,iValue=1,miValue=1 2,sValue=abc,region_name=region1,ts=0;"
                       "cmd=add,key2=2,iValue=2,region_name=region2,ts=0;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a#50", "key2=1,iValue=1,miValue=1 2,sValue=abc"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2#50", "iValue=2"));

    string rtDocString = "cmd=delete,key2=2,region_name=region2,ts=2000000;"
                         "cmd=add,key2=3,iValue=7,region_name=region2,ts=3000000;"
                         "cmd=add,key1=c,key2=8,iValue=5,miValue=2 3,sValue=aliren,region_name=region1,ts=4000000;"
                         "cmd=delete,key1=a,region_name=region1,ts=5000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a#50", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@c#50", "key2=8,iValue=5,miValue=2 3,sValue=aliren"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2#50", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@3#50", "iValue=7"));

    string incDocString = "cmd=delete,key2=2,region_name=region2,ts=2000000;"
                          "cmd=add,key2=3,iValue=7,region_name=region2,ts=3000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a#50", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@c#50", "key2=8,iValue=5,miValue=2 3,sValue=aliren"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2#50", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@3#50", "iValue=7"));
}

void MultiRegionKVInteTest::TestMultiField()
{
    string field = "nid:uint64;musicid:uint64;score:float:true;nick:string;age:uint8";

    RegionSchemaMaker maker(field, "kv");
    maker.AddRegion("region1", "nid", "nid;nick;age", "", INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "musicid", "musicid;score;nid", "", INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    IndexPartitionOptions options;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    string docString = "cmd=add,musicid=1,nid=1,score=1.1 2.1,region_name=region2;"
                       "cmd=add,musicid=2,nid=2,score=2.1 -6.1,region_name=region2;"
                       "cmd=add,musicid=3,nid=3,score=-1.1 0.12371,region_name=region2;"
                       "cmd=add,nid=4,nick=test1,age=19,score=1.1,region_name=region1;"
                       "cmd=add,nid=5,nick=test2,age=20,region_name=region1;"
                       "cmd=add,nid=6,nick=test3,age=21,region_name=region1;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@1", "musicid=1,nid=1,score=1.1 2.1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2", "musicid=2,nid=2,score=2.1 -6.1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@3", "musicid=3,nid=3,score=-1.1 0.12371"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@4", "nid=4,nick=test1,age=19"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@5", "nid=5,nick=test2,age=20"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@6", "nid=6,nick=test3,age=21"));
}

void MultiRegionKVInteTest::TestSetTwoRegionFieldSchema()
{
    string field1 = "key1:string;key2:int32;iValue:uint32;"
                    "sValue:string;miValue:int32:true";
    string field2 = "key1:int64;key2:string;iValue:uint32;otherValue:float";

    RegionSchemaMaker maker("", "kv");
    maker.AddRegion("region1", "key1", "key2;iValue;sValue;miValue", field1, INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "key2", "key1;key2;iValue;otherValue", field2, INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    string docString = "cmd=add,key1=a,key2=1,iValue=1,miValue=1 2,sValue=abc,region_name=region1;"
                       "cmd=add,key1=b,key2=2,iValue=2,miValue=1 2,sValue=def,region_name=region1;"
                       "cmd=add,key1=100000,key2=a,iValue=21,miValue=3 2,otherValue=11.1,region_name=region2;"
                       "cmd=add,key1=200000,key2=b,iValue=31,miValue=4 2,otherValue=11.2,region_name=region2;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=1,iValue=1,miValue=1 2,sValue=abc"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@b", "key2=2,iValue=2,miValue=1 2,sValue=def"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@a", "key1=100000,key2=a,iValue=21,otherValue=11.1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@b", "key1=200000,key2=b,iValue=31,otherValue=11.2"));

    string rtDocString =
        "cmd=add,key1=a,key2=3,iValue=3,miValue=1 2 3,sValue=ali,region_name=region1,ts=1000000;"
        "cmd=add,key1=b,key2=4,iValue=5,miValue=2 3,sValue=alixing,region_name=region1,ts=1000000;"
        "cmd=add,key1=300000,key2=c,iValue=41,miValue=4,otherValue=11.3,region_name=region2,ts=1000000;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=3,iValue=3,miValue=1 2 3,sValue=ali"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@b", "key2=4,iValue=5,miValue=2 3,sValue=alixing"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@c", "key1=300000,key2=c,iValue=41,otherValue=11.3"));
}

void MultiRegionKVInteTest::TestSetOneRegionFieldSchema()
{
    string field1 = "key1:string;key2:int32;iValue:uint32;"
                    "sValue:string;miValue:int32:true";
    string field2 = "key1:int32;key2:string;iValue:uint32;"
                    "sValue:string;otherValue:int32:true";

    RegionSchemaMaker maker(field1, "kv");
    maker.AddRegion("region1", "key1", "key2;iValue;sValue;miValue", "", INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "key2", "key1;otherValue", field2, INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    string docString = "cmd=add,key1=a,key2=1,iValue=1,miValue=1 2,sValue=abc,region_name=region1;"
                       "cmd=add,key1=b,key2=2,iValue=2,miValue=1 2,sValue=def,region_name=region1;"
                       "cmd=add,key1=1,key2=a,iValue=2,otherValue=3 2,sValue=abcef,region_name=region2;"
                       "cmd=add,key1=2,key2=b,iValue=3,otherValue=4 2,sValue=cef,region_name=region2;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=1,iValue=1,miValue=1 2,sValue=abc"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@b", "key2=2,iValue=2,miValue=1 2,sValue=def"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@a", "key1=1,otherValue=3 2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@b", "key1=2,otherValue=4 2"));

    string rtDocString = "cmd=add,key1=a,key2=3,iValue=3,miValue=1 2 3,sValue=ali,region_name=region1,ts=1000000;"
                         "cmd=add,key1=b,key2=4,iValue=5,miValue=2 3,sValue=alixing,region_name=region1,ts=1000000;"
                         "cmd=add,key1=2,key2=a,iValue=6,otherValue=4,sValue=efg,region_name=region2,ts=1000000;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=3,iValue=3,miValue=1 2 3,sValue=ali"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@b", "key2=4,iValue=5,miValue=2 3,sValue=alixing"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@a", "key1=2,otherValue=4"));
}

void MultiRegionKVInteTest::TestMultiRegionWithFixLengthAttribute()
{
    string field1 = "key1:string;key2:int32;iValue:uint64;";
    string field2 = "key1:string;key2:int32;iValue:uint32;"
                    "sValue:string;miValue:int32:true";
    string field3 = "key1:int32;key2:string;iValue:uint32;"
                    "sValue:string;otherValue:int32:true";

    RegionSchemaMaker maker("", "kv");
    maker.AddRegion("region0", "key1", "key2;iValue", field1, INVALID_TTL, mImpactValue);
    maker.AddRegion("region1", "key1", "key2;iValue;sValue;miValue", field2, INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "key2", "key1;otherValue", field3, INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 1;
    options.GetBuildConfig(false).keepVersionCount = 3;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetBuildConfig(false).buildTotalMemory = 32;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    string docString = "cmd=add,key1=a,key2=1,iValue=2,region_name=region0;"
                       "cmd=add,key1=a,key2=1,iValue=1,miValue=1 2,sValue=abc,region_name=region1;"
                       "cmd=add,key1=b,key2=2,iValue=2,miValue=1 2,sValue=def,region_name=region1;"
                       "cmd=add,key1=1,key2=a,iValue=2,otherValue=3 2,sValue=abcef,region_name=region2;"
                       "cmd=add,key1=2,key2=b,iValue=3,otherValue=4 2,sValue=cef,region_name=region2;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    size_t fileLen =
        FslibWrapper::GetFileLength(GET_TEMP_DATA_PATH() + "/segment_0_level_0/index/key1/value").GetOrThrow();
    // 4 + 8
    ASSERT_EQ(12, fileLen);

    ASSERT_TRUE(psm.Transfer(QUERY, "", "region0@a", "key2=1,iValue=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=1,iValue=1,miValue=1 2,sValue=abc"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@b", "key2=2,iValue=2,miValue=1 2,sValue=def"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@a", "key1=1,otherValue=3 2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@b", "key1=2,otherValue=4 2"));

    string rtDocString = "cmd=add,key1=a,key2=2,iValue=3,region_name=region0,ts=1000000;"
                         "cmd=add,key1=a,key2=3,iValue=3,miValue=1 2 3,sValue=ali,region_name=region1,ts=1000000;"
                         "cmd=add,key1=b,key2=4,iValue=5,miValue=2 3,sValue=alixing,region_name=region1,ts=1000000;"
                         "cmd=add,key1=2,key2=a,iValue=6,otherValue=4,sValue=efg,region_name=region2,ts=1000000;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region0@a", "key2=2,iValue=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=3,iValue=3,miValue=1 2 3,sValue=ali"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@b", "key2=4,iValue=5,miValue=2 3,sValue=alixing"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@a", "key1=2,otherValue=4"));
}

IndexPartitionSchemaPtr MultiRegionKVInteTest::CreateSchema(const string& fields, const string& key,
                                                            const string& values, int64_t ttl)
{
    string newRegionFields = "new_region_key:string;new_region_value:string;";
    string newFields;
    if (*fields.rbegin() == ';') {
        newFields = fields + newRegionFields;
    } else {
        newFields = fields + ";" + newRegionFields;
    }
    RegionSchemaMaker maker(newFields, "kv");
    maker.AddRegion(DEFAULT_REGIONNAME, key, values, "", ttl, mImpactValue);
    maker.AddRegion("new_region", "new_region_key", "new_region_value", "", ttl, mImpactValue);
    return maker.GetSchema();
}

vector<DocumentPtr> MultiRegionKVInteTest::CreateKVDocuments(const IndexPartitionSchemaPtr& schema,
                                                             const string& docStrs)
{
    vector<document::DocumentPtr> ret;
    auto kvDocs = DocumentCreator::CreateKVDocuments(schema, docStrs, DEFAULT_REGIONNAME);
    for (auto doc : kvDocs) {
        ret.push_back(doc);
    }
    return ret;
}

void MultiRegionKVInteTest::MakeGlobalRegionIndexPreference(const IndexPartitionSchemaPtr& schema,
                                                            const string& hashDictType, const string& valueCompressType)
{
    KVIndexPreference preference;
    KVIndexPreference::HashDictParam dictParam(hashDictType);
    KVIndexPreference::ValueParam valueParam(false, valueCompressType);
    valueParam.EnableValueImpact(mImpactValue);
    preference.SetHashDictParam(dictParam);
    preference.SetValueParam(valueParam);

    autil::legacy::Jsonizable::JsonWrapper wrapper;
    preference.Jsonize(wrapper);
    schema->SetGlobalRegionIndexPreference(wrapper.GetMap());
}

void MultiRegionKVInteTest::CheckCompress(DirectoryPtr dir, const string& filePath, const string& valueCompressType)
{
    string valueCompressFile = string("value") + COMPRESS_FILE_INFO_SUFFIX;
    string valueCompressFileInfo = PathUtil::JoinPath(filePath, valueCompressFile);

    if (valueCompressType.empty()) {
        ASSERT_FALSE(dir->IsExist(valueCompressFileInfo));
    } else {
        ASSERT_TRUE(dir->IsExist(valueCompressFileInfo));
        string infoStr;
        dir->Load(valueCompressFileInfo, infoStr);
        file_system::CompressFileInfo fileInfo;
        fileInfo.FromString(infoStr);
        ASSERT_EQ(valueCompressType, fileInfo.compressorName);
    }
}
}} // namespace indexlib::partition
