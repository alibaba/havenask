#include "indexlib/partition/test/multi_region_kkv_intetest.h"

#include "indexlib/config/SortParam.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/fslib/DataFlushController.h"
#include "indexlib/file_system/fslib/MultiPathDataFlushController.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/region_schema_maker.h"
#include "indexlib/test/slow_dump_segment_container.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::codegen;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, MultiRegionKKVInteTest);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
    MultiRegionKKVInteTestMode, MultiRegionKKVInteTest,
    testing::Values(tuple<PKeyTableType, bool, bool, bool, bool, bool>(PKT_DENSE, false, true, false, true, false),
                    tuple<PKeyTableType, bool, bool, bool, bool, bool>(PKT_CUCKOO, false, true, true, true, false),
                    tuple<PKeyTableType, bool, bool, bool, bool, bool>(PKT_SEPARATE_CHAIN, true, false, true, false,
                                                                       false),
                    tuple<PKeyTableType, bool, bool, bool, bool, bool>(PKT_DENSE, false, true, true, true, true)));

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
    MultiRegionKKVInteTestSimple, MultiRegionKKVInteTest,
    testing::Values(tuple<PKeyTableType, bool, bool, bool, bool, bool>(PKT_DENSE, false, true, false, true, false)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestSearchPkey);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestSearchPkeyAndSkeys);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestPkeyWithDifferentType);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestSkeyWithDifferentType);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestDeleteDocs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestDeletePKeys);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestSearchWithTimestamp);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestSearchSkeysWithOptimization);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestBuildSegmentInLevel0);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestBuildShardingSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestMergeTwoSegmentSimple);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestMergeDuplicateSKey);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestMergeDuplicateSKey_13227243);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestMergeDeleteSKey);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestMergeDeletePKey);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestMergeShardingSimple);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestMergeWithCountLimit);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestMergeWithOneColumnHasNoKey);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestFullBuild);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestIncBuildNoMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestIncBuildOptimizeMerge_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestIncBuildOptimizeMerge_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestIncBuildOptimizeMerge_3);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestIncBuildOptimizeMerge_4);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestRtBuild_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestRtBuild_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestShardBasedMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestMergeClearTable);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestMultiPartMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestNoNeedOptimizeMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestNoNeedShardBasedMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestFullMergeWithSKeyTruncate_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestFullMergeWithSKeyTruncate_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestFullMergeWithSKeyTruncate_3);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestLongValue);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestReopen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestSearchWithBlockCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestFlushRealtimeIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestOptimizeMergeWithoutTs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestOptimizeMergeWithUserTs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestFlushRealtimeIndexWithRemainRtIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestRtWithSKeyTruncate);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestIncCoverAllRtIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestBuildWithProtectionThreshold);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestForceDumpByValueFull);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestForceDumpBySKeyFull);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestSearchPkeyAndSkeysSeekBySKey);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestSimple, TestSearchPkeyAndSkeysPerf);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestSimple, TestSearchPkeyAndSkeysInCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestSimple, TestBuildWithKeepSortSequence);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestSimple, TestIncCoverAllRtIndexWithAsyncDump);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestSimple, TestMergeMultiTimesLongCaseTest);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestSimple, TestProhibitInMemDumpBug_13765192);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestSimple, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestSimple, TestMergeTruncWithMultiRegion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestSimple, TestMergeTruncWithDiffenertFieldSchema);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestSimple, TestMergeSortWithMultiRegion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestSimple, TestMergeSortWithDiferentFieldSchema);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestSimple, TestTTL);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestSimple, TestSetGlobalRegionPreference);

// INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiRegionKKVInteTestMode, TestBuildShardingSegmentWithLegacyDoc);

MultiRegionKKVInteTest::MultiRegionKKVInteTest() {}

MultiRegionKKVInteTest::~MultiRegionKKVInteTest() {}

void MultiRegionKKVInteTest::CaseSetUp()
{
    MultiPathDataFlushController::GetDataFlushController("")->Reset();
    PKeyTableType tableType = std::get<0>(GET_CASE_PARAM());
    bool useStrHash = std::get<1>(GET_CASE_PARAM());
    bool fileCompress = std::get<2>(GET_CASE_PARAM());
    bool flushRtIndex = std::get<3>(GET_CASE_PARAM());
    bool usePackageFile = std::get<4>(GET_CASE_PARAM());
    mImpactValue = usePackageFile;

    string field = "pkey:string;skey:int32;value:uint32;";
    if (useStrHash) {
        field = "pkey:string;skey:string;value:uint32;";
    }

    RegionSchemaMaker maker(field, "kkv");
    maker.AddRegion("region1", "pkey", "skey", "value;skey;", "", INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "skey", "pkey", "value;pkey;", "", INVALID_TTL, mImpactValue);
    mSchema = maker.GetSchema();
    ASSERT_TRUE(mSchema.get());
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KKVIndexPreference::HashDictParam param(PrefixKeyTableType2Str(tableType));
    kkvConfig->GetIndexPreference().SetHashDictParam(param);
    auto suffixInfo = kkvConfig->GetSuffixFieldInfo();
    suffixInfo.skipListThreshold = 2;
    kkvConfig->SetSuffixFieldInfo(suffixInfo);
    if (fileCompress) {
        KKVIndexPreference::SuffixKeyParam suffixParam(false, GetCompressType());
        KKVIndexPreference::ValueParam valueParam(false, GetCompressType());
        valueParam.EnableValueImpact(mImpactValue);
        kkvConfig->GetIndexPreference().SetSkeyParam(suffixParam);
        kkvConfig->GetIndexPreference().SetValueParam(valueParam);
    }

    mOptions = IndexPartitionOptions();
    mOptions.GetBuildConfig(false).buildTotalMemory = 20; // 20MB
    mOptions.GetBuildConfig(true).buildTotalMemory = 20;  // 20MB
    mOptions.GetBuildConfig(false).keepVersionCount = 10;
    mOptions.GetBuildConfig(true).keepVersionCount = 10;
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = flushRtIndex;
    mOptions.GetOnlineConfig().useSwapMmapFile = flushRtIndex;
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = fileCompress && usePackageFile;
    mOptions.GetOfflineConfig().fullIndexStoreKKVTs = true;
    mOptions.SetEnablePackageFile(usePackageFile);

    // random load config : mmap, cache, cache decompress file
    mOptions.GetOnlineConfig().loadConfigList =
        LoadConfigListCreator::CreateLoadConfigListFromJsonString(GetLoadConfigJsonStr());
    mResultInfos = ExpectResultInfos();

    if (mOptions.GetOnlineConfig().enableAsyncDumpSegment) {
        mDumpSegmentContainer.reset(new SlowDumpSegmentContainer(500));
    } else {
        mDumpSegmentContainer.reset(new DumpSegmentContainer);
    }
}

void MultiRegionKKVInteTest::CaseTearDown() {}

void MultiRegionKKVInteTest::TestSimpleProcess()
{
    string field = "key1:string;key2:int32;iValue:uint32;"
                   "sValue:string;miValue:int32:true";

    RegionSchemaMaker maker(field, "kkv");
    maker.AddRegion("region1", "key1", "key2", "key2;iValue;miValue", "", INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "key2", "key1", "key1;sValue;iValue", "", INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));

    string docString = "cmd=add,key1=a,key2=1,iValue=1,miValue=1 2,sValue=abc,region_name=region1;"
                       "cmd=add,key1=a,key2=2,iValue=2,miValue=1 2,sValue=def,region_name=region1;"
                       "cmd=add,key1=b,key2=2,iValue=2,miValue=3 2,sValue=abcef,region_name=region2;"
                       "cmd=add,key1=a,key2=2,iValue=3,miValue=4 2,sValue=cef,region_name=region2;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=1,iValue=1,miValue=1 2;key2=2,iValue=2,miValue=1 2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2", "key1=b,iValue=2,sValue=abcef;key1=a,iValue=3,sValue=cef"));

    string rtDocString = "cmd=add,key1=a,key2=3,iValue=3,miValue=1 2 3,sValue=ali,region_name=region1,ts=1000000;"
                         "cmd=add,key1=b,key2=4,iValue=5,miValue=2 3,sValue=alixing,region_name=region1,ts=1000000;"
                         "cmd=add,key1=a,key2=2,iValue=6,miValue=4,sValue=efg,region_name=region2,ts=1000000;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a",
                             "key2=1,iValue=1,miValue=1 2;key2=2,iValue=2,miValue=1 2;"
                             "key2=3,iValue=3,miValue=1 2 3"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@b", "key2=4,iValue=5,miValue=2 3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2", "key1=b,iValue=2,sValue=abcef;key1=a,iValue=6,sValue=efg"));
}

void MultiRegionKKVInteTest::TestMergeTruncWithMultiRegion()
{
    string field = "key1:string;key2:int32;iValue1:uint32;"
                   "sValue:string;iValue2:int32";

    RegionSchemaMaker maker(field, "kkv");
    maker.AddRegion("region1", "key1", "key2", "key2;iValue1;iValue2", "", INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "key2", "key1", "key1;sValue;iValue1", "", INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    auto AddKKVTruncateConfig = [&schema](regionid_t regionId, const string& skeyField, const string& truncDesc,
                                          int countLimit) {
        const SingleFieldIndexConfigPtr& pkConfig = schema->GetIndexSchema(regionId)->GetPrimaryKeyIndexConfig();
        KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, pkConfig);
        KKVIndexFieldInfo suffixFieldInfo;
        suffixFieldInfo.fieldName = skeyField;
        suffixFieldInfo.keyType = KKVKeyType::SUFFIX;
        suffixFieldInfo.countLimits = countLimit;
        suffixFieldInfo.sortParams = StringToSortParams(truncDesc);
        kkvConfig->SetSuffixFieldInfo(suffixFieldInfo);
    };

    AddKKVTruncateConfig(0, "key2", "+iValue2", 3);
    AddKKVTruncateConfig(1, "key1", "-iValue1", 2);

    PartitionStateMachine psm;
    IndexPartitionOptions options = mOptions;
    options.GetBuildConfig(false).maxDocCount = 3;

    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    // region1, pkey = a, {(skey, value)} =  (1, 5), (2, 4), (3, 3), (4, 2), (5, 1)
    // region2, pkey = 2, {(skey, value)} =  (b, -2), (a, -1), (c, 0), (e, 2), (f, 1), (g, 3)
    string docString = "cmd=add,key1=a,key2=1,iValue1=1,iValue2=5,sValue=abc,region_name=region1;"
                       "cmd=add,key2=2,key1=b,iValue1=-2,iValue2=3,sValue=abcef,region_name=region2;"
                       "cmd=add,key1=a,key2=2,iValue1=2,iValue2=4,sValue=def,region_name=region1;"
                       "cmd=add,key2=2,key1=a,iValue1=-1,iValue2=4,sValue=cef,region_name=region2;"
                       "cmd=add,key1=a,key2=3,iValue1=2,iValue2=3,sValue=def,region_name=region1;"
                       "cmd=add,key2=2,key1=c,iValue1=0,iValue2=3,sValue=abcef,region_name=region2;"
                       "cmd=add,key1=a,key2=4,iValue1=2,iValue2=2,sValue=def,region_name=region1;"
                       "cmd=add,key2=2,key1=e,iValue1=2,iValue2=3,sValue=abcef,region_name=region2;"
                       "cmd=add,key1=a,key2=5,iValue1=2,iValue2=1,sValue=def,region_name=region1;"
                       "cmd=add,key2=2,key1=f,iValue1=1,iValue2=4,sValue=cef,region_name=region2;"
                       "cmd=add,key2=2,key1=g,iValue1=3,iValue2=4,sValue=cef,region_name=region2;";

    // after truncate, expected:
    // region1, pkey = a, {(skey, value)} =  (3, 3), (4, 2), (5, 1)
    // region2, pkey = 2, {(skey, value)} =  (e, 2), (g, 3)
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    psm.SetPsmOption("resultCheckType", "UNORDERED");
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a",
                             "key2=3,iValue1=2,iValue2=3;key2=4,iValue1=2,iValue2=2;key2=5,iValue1=2,iValue2=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2", "key1=e,iValue1=2,sValue=abcef;key1=g,iValue1=3,sValue=cef"));
}

void MultiRegionKKVInteTest::TestMergeTruncWithDiffenertFieldSchema()
{
    string field1 = "key1:string;key2:int32;iValue1:uint32;"
                    "sValue:string;iValue2:int8";
    string field2 = "key1:string;key2:int32;iValue1:uint32;"
                    "sValue:uint8;iValue2:uint32";

    RegionSchemaMaker maker("", "kkv");
    maker.AddRegion("region1", "key1", "key2", "key2;iValue1;iValue2", field1, INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "key2", "key1", "key1;sValue;iValue2", field2, INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    auto AddKKVTruncateConfig = [&schema](regionid_t regionId, const string& skeyField, const string& truncDesc,
                                          int countLimit) {
        const SingleFieldIndexConfigPtr& pkConfig = schema->GetIndexSchema(regionId)->GetPrimaryKeyIndexConfig();
        KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, pkConfig);
        KKVIndexFieldInfo suffixFieldInfo;
        suffixFieldInfo.fieldName = skeyField;
        suffixFieldInfo.keyType = KKVKeyType::SUFFIX;
        suffixFieldInfo.countLimits = countLimit;
        suffixFieldInfo.sortParams = StringToSortParams(truncDesc);
        kkvConfig->SetSuffixFieldInfo(suffixFieldInfo);
    };

    AddKKVTruncateConfig(0, "key2", "+iValue2", 3);
    AddKKVTruncateConfig(1, "key1", "-iValue2", 4);

    PartitionStateMachine psm;
    IndexPartitionOptions options = mOptions;
    options.GetBuildConfig(false).maxDocCount = 3;

    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    // region1, pkey = a, {(skey, value)} =  (1, 5), (2, 4), (3, 3), (4, 2), (5, 1)
    // region2, pkey = 2, {(skey, value)} =  (k1, 1000), (k2, 1001), (k3, 1002), (k4, 1003), (k5, 1004), (k6, 1005)
    string docString = "cmd=add,key1=a,key2=1,iValue1=1,iValue2=5,sValue=abc,region_name=region1;"
                       "cmd=add,key1=a,key2=2,iValue1=2,iValue2=4,sValue=def,region_name=region1;"
                       "cmd=add,key1=a,key2=3,iValue1=2,iValue2=3,sValue=def,region_name=region1;"
                       "cmd=add,key1=a,key2=4,iValue1=2,iValue2=2,sValue=def,region_name=region1;"
                       "cmd=add,key1=a,key2=5,iValue1=2,iValue2=1,sValue=def,region_name=region1;"
                       "cmd=add,key2=2,key1=k1,iValue1=-1,iValue2=1000,sValue=1,region_name=region2;"
                       "cmd=add,key2=2,key1=k2,iValue1=-2,iValue2=1001,sValue=2,region_name=region2;"
                       "cmd=add,key2=2,key1=k3,iValue1=0,iValue2=1002,sValue=3,region_name=region2;"
                       "cmd=add,key2=2,key1=k4,iValue1=1,iValue2=1003,sValue=4,region_name=region2;"
                       "cmd=add,key2=2,key1=k5,iValue1=2,iValue2=1004,sValue=5,region_name=region2;"
                       "cmd=add,key2=2,key1=k6,iValue1=3,iValue2=1005,sValue=3000,region_name=region2;";
    // after truncate, expected:
    // region1, pkey = a, {(skey, value)} =  (3, 3), (4, 2), (5, 1)
    // region2, pkey = 2, {(skey, value)} =  (k3, 1002), (k4, 1003), (k5, 1004), (k6, 1005)
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    psm.SetPsmOption("resultCheckType", "UNORDERED");
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a",
                             "key2=3,iValue1=2,iValue2=3;key2=4,iValue1=2,iValue2=2;key2=5,iValue1=2,iValue2=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2",
                             "key1=k3,iValue2=1002,sValue=3;"
                             "key1=k4,iValue2=1003,sValue=4;"
                             "key1=k5,iValue2=1004,sValue=5;"
                             "key1=k6,iValue2=1005,sValue=0"));
}

void MultiRegionKKVInteTest::TestMergeSortWithMultiRegion()
{
    string field = "key1:string;key2:int32;iValue1:uint32;"
                   "sValue:string;iValue2:int32";
    RegionSchemaMaker maker(field, "kkv");
    maker.AddRegion("region1", "key1", "key2", "key2;iValue1;iValue2", "", INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "key2", "key1", "key1;sValue;iValue1", "", INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    auto AddKKVSortConfig = [&schema](regionid_t regionId, const string& skeyField, const string& sortParams,
                                      int countLimit) {
        const SingleFieldIndexConfigPtr& pkConfig = schema->GetIndexSchema(regionId)->GetPrimaryKeyIndexConfig();
        KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, pkConfig);
        KKVIndexFieldInfo suffixFieldInfo;
        suffixFieldInfo.fieldName = skeyField;
        suffixFieldInfo.keyType = KKVKeyType::SUFFIX;
        suffixFieldInfo.enableKeepSortSequence = true;
        suffixFieldInfo.countLimits = countLimit;
        suffixFieldInfo.sortParams = StringToSortParams(sortParams);
        kkvConfig->SetSuffixFieldInfo(suffixFieldInfo);
    };

    AddKKVSortConfig(0, "key2", "+iValue2", 2);
    AddKKVSortConfig(1, "key1", "-iValue1", 3);

    PartitionStateMachine psm;
    IndexPartitionOptions options = mOptions;
    options.GetBuildConfig(false).maxDocCount = 3;

    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    // region1, pkey = a, {(skey, value)} =  (1, 5), (2, 4), (3, 3), (4, 2), (5, 1)
    // region2, pkey = 2, {(skey, value)} =  (b, -2), (a, -1), (c, 0), (e, 2), (f, 1), (g, 3)
    string docString = "cmd=add,key1=a,key2=1,iValue1=1,iValue2=5,sValue=abc,region_name=region1;"
                       "cmd=add,key2=2,key1=b,iValue1=-2,iValue2=3,sValue=abcef,region_name=region2;"
                       "cmd=add,key1=a,key2=2,iValue1=2,iValue2=4,sValue=def,region_name=region1;"
                       "cmd=add,key2=2,key1=a,iValue1=-1,iValue2=4,sValue=cef,region_name=region2;"
                       "cmd=add,key1=a,key2=3,iValue1=2,iValue2=3,sValue=def,region_name=region1;"
                       "cmd=add,key2=2,key1=c,iValue1=0,iValue2=3,sValue=abcef,region_name=region2;"
                       "cmd=add,key1=a,key2=4,iValue1=2,iValue2=2,sValue=def,region_name=region1;"
                       "cmd=add,key2=2,key1=e,iValue1=2,iValue2=3,sValue=abcef,region_name=region2;"
                       "cmd=add,key1=a,key2=5,iValue1=2,iValue2=1,sValue=def,region_name=region1;"
                       "cmd=add,key2=2,key1=f,iValue1=1,iValue2=4,sValue=cef,region_name=region2;"
                       "cmd=add,key2=2,key1=g,iValue1=3,iValue2=4,sValue=cef,region_name=region2;";

    // after truncate, expected:
    // region1, pkey = a, {(skey, value)} =  (5, 1), (4, 2)
    // region2, pkey = 2, {(skey, value)} =  (g, 3), (e, 2), (f, 1)

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    psm.SetPsmOption("resultCheckType", "ORDERED");
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=5,iValue1=2,iValue2=1;key2=4,iValue1=2,iValue2=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2",
                             "key1=g,iValue1=3,sValue=cef;key1=e,iValue1=2,"
                             "sValue=abcef;key1=f,iValue1=1,sValue=cef"));

    string incDocStr = "cmd=add,key1=a,key2=1,iValue1=1,iValue2=5,sValue=abc,region_name=region1;"
                       "cmd=add,key2=2,key1=b,iValue1=-2,iValue2=3,sValue=abcef,region_name=region2;";
    // not support inc build with keep sort sequence
    ASSERT_FALSE(psm.Transfer(BUILD_INC, incDocStr, "", ""));
}

void MultiRegionKKVInteTest::TestMergeSortWithDiferentFieldSchema()
{
    string field1 = "key1:string;key2:int32;iValue1:uint32;"
                    "sValue:string;iValue2:uint8";
    string field2 = "key1:string;key2:int32;iValue1:uint32;"
                    "otherValue:string;iValue2:int32";

    RegionSchemaMaker maker("", "kkv");
    maker.AddRegion("region1", "key1", "key2", "key2;iValue1;iValue2", field1, INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", "key2", "key1", "key1;otherValue;iValue2", field2, INVALID_TTL, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    auto AddKKVSortConfig = [&schema](regionid_t regionId, const string& skeyField, const string& sortParams,
                                      int countLimit) {
        const SingleFieldIndexConfigPtr& pkConfig = schema->GetIndexSchema(regionId)->GetPrimaryKeyIndexConfig();
        KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, pkConfig);
        KKVIndexFieldInfo suffixFieldInfo;
        suffixFieldInfo.fieldName = skeyField;
        suffixFieldInfo.keyType = KKVKeyType::SUFFIX;
        suffixFieldInfo.enableKeepSortSequence = true;
        suffixFieldInfo.countLimits = countLimit;
        suffixFieldInfo.sortParams = StringToSortParams(sortParams);
        kkvConfig->SetSuffixFieldInfo(suffixFieldInfo);
    };

    AddKKVSortConfig(0, "key2", "+iValue2", 2);
    AddKKVSortConfig(1, "key1", "+iValue2", 3);

    PartitionStateMachine psm;
    IndexPartitionOptions options = mOptions;
    options.GetBuildConfig(false).maxDocCount = 3;

    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    // region1, pkey = a, {(skey, value)} =  (1, 5), (2, 4), (3, 3), (4, 2), (5, 1)
    // region2, pkey = 2, {(skey, value)} =  (b, -2), (a, 1000), (c, 1001), (e, 999), (f, -1), (g, 1003)
    string docString = "cmd=add,key1=a,key2=1,iValue1=1,iValue2=5,sValue=abc,region_name=region1;"
                       "cmd=add,key1=a,key2=2,iValue1=2,iValue2=4,sValue=def,region_name=region1;"
                       "cmd=add,key1=a,key2=3,iValue1=2,iValue2=3,sValue=def,region_name=region1;"
                       "cmd=add,key1=a,key2=4,iValue1=2,iValue2=2,sValue=def,region_name=region1;"
                       "cmd=add,key1=a,key2=5,iValue1=2,iValue2=1,sValue=def,region_name=region1;"

                       "cmd=add,key2=2,key1=a,iValue2=1000,iValue1=4,otherValue=cef,region_name=region2;"
                       "cmd=add,key2=2,key1=b,iValue2=-2,iValue1=3,otherValue=abcef,region_name=region2;"
                       "cmd=add,key2=2,key1=c,iValue2=1001,iValue1=3,otherValue=abcef,region_name=region2;"
                       "cmd=add,key2=2,key1=e,iValue2=999,iValue1=3,otherValue=abcef,region_name=region2;"
                       "cmd=add,key2=2,key1=f,iValue2=-1,iValue1=4,otherValue=cef,region_name=region2;"
                       "cmd=add,key2=2,key1=g,iValue2=1003,iValue1=4,otherValue=cef,region_name=region2;";

    // after truncate, expected:
    // region1, pkey = a, {(skey, value)} =  (5, 1), (4, 2)
    // region2, pkey = 2, {(skey, value)} =  (b, -2), (f, -1), (e, 999)

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    psm.SetPsmOption("resultCheckType", "ORDERED");
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", "key2=5,iValue1=2,iValue2=1;key2=4,iValue1=2,iValue2=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2",
                             "key1=b,iValue2=-2,otherValue=abcef;"
                             "key1=f,iValue2=-1,otherValue=cef;"
                             "key1=e,iValue2=999,otherValue=abcef"));

    string incDocStr = "cmd=add,key1=a,key2=1,iValue1=1,iValue2=5,sValue=abc,region_name=region1;"
                       "cmd=add,key2=2,key1=b,iValue1=-2,iValue2=3,otherValue=abcef,region_name=region2;";
    // not support inc build with keep sort sequence
    ASSERT_FALSE(psm.Transfer(BUILD_INC, incDocStr, "", ""));
}

void MultiRegionKKVInteTest::TestSetGlobalRegionPreference()
{
    InnerTestSetGlobalRegionPreference("", "");
    InnerTestSetGlobalRegionPreference("", "lz4");
    InnerTestSetGlobalRegionPreference("zstd", "");
    InnerTestSetGlobalRegionPreference("lz4", "snappy");
}

void MultiRegionKKVInteTest::InnerTestSetGlobalRegionPreference(const string& skeyCompressType,
                                                                const string valueCompressType)
{
    tearDown();
    setUp();

    string field = "key1:string;key2:int32;iValue1:uint32;"
                   "sValue:string;iValue2:int32";
    RegionSchemaMaker maker(field, "kkv");
    maker.AddRegion("region1", "key1", "key2", "key2;iValue1;iValue2", "", 20, mImpactValue);
    maker.AddRegion("region2", "key2", "key1", "key1;sValue;iValue1", "", 100, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    MakeGlobalRegionIndexPreference(schema, "separate_chain", skeyCompressType, valueCompressType);

    PartitionStateMachine psm;
    IndexPartitionOptions options = mOptions;
    options.GetOfflineConfig().fullIndexStoreKKVTs = true;

    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key1=a,key2=1,iValue1=1,iValue2=5,sValue=abc,region_name=region1,ts=30000000;"
                       "cmd=add,key1=a,key2=3,iValue1=2,iValue2=3,sValue=def,region_name=region1,ts=30000000;"
                       "cmd=add,key1=a,key2=2,iValue1=2,iValue2=4,sValue=def,region_name=region1,ts=10000000;"
                       "cmd=add,key2=2,key1=a,iValue1=-1,iValue2=4,sValue=cef,region_name=region2,ts=40000000;"
                       "cmd=add,key2=2,key1=b,iValue1=-2,iValue2=3,sValue=abcef,region_name=region2,ts=80000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a#50", "key2=1,iValue1=1,iValue2=5;key2=3,iValue1=2,iValue2=3"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2#120", "key1=a,sValue=cef,iValue1=0;key1=b,sValue=abcef,iValue1=0"));
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr segmentDir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_1_level_1", true);
    CheckCompress(segmentDir->GetDirectory("index/key1", true), skeyCompressType, valueCompressType);
}

void MultiRegionKKVInteTest::TestTTL()
{
    string field = "key1:string;key2:int32;iValue1:uint32;"
                   "sValue:string;iValue2:int32";
    RegionSchemaMaker maker(field, "kkv");
    maker.AddRegion("region1", "key1", "key2", "key2;iValue1;iValue2", "", 20, mImpactValue);
    maker.AddRegion("region2", "key2", "key1", "key1;sValue;iValue1", "", 100, mImpactValue);
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    PartitionStateMachine psm;
    IndexPartitionOptions options = mOptions;
    options.GetBuildConfig(false).maxDocCount = 3;
    options.GetOfflineConfig().fullIndexStoreKKVTs = true;

    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    string docString = "cmd=add,key1=a,key2=1,iValue1=1,iValue2=5,sValue=abc,region_name=region1,ts=30000000;"
                       "cmd=add,key1=a,key2=3,iValue1=2,iValue2=3,sValue=def,region_name=region1,ts=30000000;"
                       "cmd=add,key1=a,key2=2,iValue1=2,iValue2=4,sValue=def,region_name=region1,ts=10000000;"
                       "cmd=add,key2=2,key1=a,iValue1=-1,iValue2=4,sValue=cef,region_name=region2,ts=40000000;"
                       "cmd=add,key2=2,key1=b,iValue1=-2,iValue2=3,sValue=abcef,region_name=region2,ts=80000000;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a#50", "key2=1,iValue1=1,iValue2=5;key2=3,iValue1=2,iValue2=3"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2#120", "key1=a,sValue=cef,iValue1=0;key1=b,sValue=abcef,iValue1=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2#150", "key1=b,sValue=abcef,iValue1=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2#200", ""));

    // make all doc reclaimed by ttl
    psm.SetCurrentTimestamp(200000000);
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@a", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@2", ""));
}

IndexPartitionSchemaPtr MultiRegionKKVInteTest::CreateSchema(const string& fields, const string& pkey,
                                                             const string& skey, const string& values)
{
    RegionSchemaMaker maker(fields, "kkv");
    maker.AddRegion("region1", pkey, skey, values, "", INVALID_TTL, mImpactValue);
    maker.AddRegion("region2", skey, pkey, values, "", INVALID_TTL, mImpactValue);
    return maker.GetSchema();
}

vector<DocumentPtr> MultiRegionKKVInteTest::CreateKKVDocuments(const IndexPartitionSchemaPtr& schema,
                                                               const string& docStrs)
{
    vector<DocumentPtr> ret;
    auto kvDocs = DocumentCreator::CreateKVDocuments(schema, docStrs, "region1");
    for (auto doc : kvDocs) {
        ret.push_back(doc);
    }
    return ret;
}

void MultiRegionKKVInteTest::MakeGlobalRegionIndexPreference(const IndexPartitionSchemaPtr& schema,
                                                             const string& hashDictType, const string& skeyCompressType,
                                                             const string& valueCompressType)
{
    KKVIndexPreference preference;
    KKVIndexPreference::HashDictParam dictParam(hashDictType);
    KKVIndexPreference::SuffixKeyParam skeyParam(false, skeyCompressType);
    KKVIndexPreference::ValueParam valueParam(false, valueCompressType);
    valueParam.EnableValueImpact(mImpactValue);

    preference.SetHashDictParam(dictParam);
    preference.SetSkeyParam(skeyParam);
    preference.SetValueParam(valueParam);

    autil::legacy::Jsonizable::JsonWrapper wrapper;
    preference.Jsonize(wrapper);
    schema->SetGlobalRegionIndexPreference(wrapper.GetMap());
}

void MultiRegionKKVInteTest::CheckCompress(DirectoryPtr dir, const string& skeyCompressType,
                                           const string& valueCompressType)
{
    string skeyCompressFile = string("skey") + COMPRESS_FILE_INFO_SUFFIX;
    string valueCompressFile = string("value") + COMPRESS_FILE_INFO_SUFFIX;

    if (skeyCompressType.empty()) {
        ASSERT_FALSE(dir->IsExist(skeyCompressFile));
    } else {
        ASSERT_TRUE(dir->IsExist(skeyCompressFile));
        string infoStr;
        dir->Load(skeyCompressFile, infoStr);
        CompressFileInfo fileInfo;
        fileInfo.FromString(infoStr);
        ASSERT_EQ(skeyCompressType, fileInfo.compressorName);
    }

    if (valueCompressType.empty()) {
        ASSERT_FALSE(dir->IsExist(valueCompressFile));
    } else {
        ASSERT_TRUE(dir->IsExist(valueCompressFile));
        string infoStr;
        dir->Load(valueCompressFile, infoStr);
        CompressFileInfo fileInfo;
        fileInfo.FromString(infoStr);
        ASSERT_EQ(valueCompressType, fileInfo.compressorName);
    }
}
}} // namespace indexlib::partition
