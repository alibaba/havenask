#include "indexlib/partition/test/kkv_table_intetest.h"

#include "autil/StringUtil.h"
#include "future_lite/CoroInterface.h"
#include "indexlib/common/chunk/chunk_define.h"
#include "indexlib/config/impl/merge_config_impl.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/fslib/DataFlushController.h"
#include "indexlib/file_system/fslib/MultiPathDataFlushController.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/index/kkv/kkv_iterator.h"
#include "indexlib/index/kkv/kkv_reader.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/index/test/deploy_index_checker.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/multi_partition_merger.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/partition_writer.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/region_schema_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/slow_dump_segment_container.h"
#include "indexlib/util/cache/SearchCache.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::merger;
using namespace indexlib::codegen;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, KKVTableTest);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
    KKVTableTestMode, KKVTableTest,
    testing::Values(tuple<PKeyTableType, bool, bool, bool, bool, bool>(PKT_DENSE, false, true, false, true, false),
                    tuple<PKeyTableType, bool, bool, bool, bool, bool>(PKT_CUCKOO, false, true, true, true, false),
                    tuple<PKeyTableType, bool, bool, bool, bool, bool>(PKT_SEPARATE_CHAIN, true, false, true, false,
                                                                       false),
                    tuple<PKeyTableType, bool, bool, bool, bool, bool>(PKT_DENSE, false, true, true, true, true)));

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
    KKVTableTestSimple, KKVTableTest,
    testing::Values(tuple<PKeyTableType, bool, bool, bool, bool, bool>(PKT_DENSE, false, true, false, true, false)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestSearchPkey);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestSearchOnlyRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestSearchPkeyAndSkeys);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestPkeyWithDifferentType);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestSkeyWithDifferentType);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestDeleteDocs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestDeletePKeys);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestSearchWithTimestamp);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestSearchSkeysWithOptimization);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestBuildSegmentInLevel0);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestBuildShardingSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestMergeTwoSegmentSimple);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestMergeDuplicateSKey);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestMergeDuplicateSKey_13227243);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestMergeDeleteSKey);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestMergeDeletePKey);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestMergeShardingSimple);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestMergeWithCountLimit);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestMergeWithOneColumnHasNoKey);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestFullBuild);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestIncBuildNoMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestIncBuildOptimizeMerge_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestIncBuildOptimizeMerge_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestIncBuildOptimizeMerge_3);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestIncBuildOptimizeMerge_4);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestRtBuild_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestRtBuild_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestShardBasedMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestShardBasedMergeWithBadDocs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestMergeClearTable);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestMultiPartMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestNoNeedOptimizeMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestNoNeedShardBasedMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestFullMergeWithSKeyTruncate_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestFullMergeWithSKeyTruncate_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestFullMergeWithSKeyTruncate_3);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestLongValue);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestReopen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestSearchWithBlockCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestFlushRealtimeIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestBuildShardingSegmentWithLegacyDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestOptimizeMergeWithoutTs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestOptimizeMergeWithUserTs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestFlushRealtimeIndexWithRemainRtIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestRtWithSKeyTruncate);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestIncCoverAllRtIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestBuildWithProtectionThreshold);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestForceDumpByValueFull);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestForceDumpBySKeyFull);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestSearchPkeyAndSkeysSeekBySKey);
// TODO(qingran): this ut is wrong in master, psm get wrong secondary root
// INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestFlushRealtimeIndexForRemoteLoadConfig);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestMode, TestDocTTL);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestSimple, TestSearchPkeyAndSkeysPerf);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestSimple, TestSearchPkeyAndSkeysInCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestSimple, TestBuildWithKeepSortSequence);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestSimple, TestIncCoverAllRtIndexWithAsyncDump);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestSimple, TestMergeMultiTimesLongCaseTest);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestSimple, TestProhibitInMemDumpBug_13765192);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestSimple, TestCompressedFixedLengthFloatField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestSimple, TestSingleFloatCompress);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestSimple, TestFixedLengthMultiValueField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestSimple, TestRtDeleteDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVTableTestSimple, TestCacheSomeSKeyExpired);

bool ExpectResultInfos::ExpectResultInfoComp::operator()(const ExpectResultInfos::SkeyInfo& lft,
                                                         const ExpectResultInfos::SkeyInfo& rht)
{
    for (size_t i = 0; i < mSortParams.size(); i++) {
        const SortParam& param = mSortParams[i];
        const string& sortField = param.GetSortField();
        bool isDesc = (param.GetSortPattern() == indexlibv2::config::sp_desc);
        if (sortField == KKVIndexConfig::DEFAULT_SKEY_TS_TRUNC_FIELD_NAME) {
            if (lft.ts < rht.ts) {
                return !isDesc;
            }
            if (rht.ts < lft.ts) {
                return isDesc;
            }
        } else if (sortField == "value") {
            if (lft.value < rht.value) {
                return !isDesc;
            }
            if (rht.value < lft.value) {
                return isDesc;
            }
        } else {
            assert(false);
        }
    }
    return lft.skey < rht.skey;
}

KKVTableTest::KKVTableTest() {}

KKVTableTest::~KKVTableTest() {}

void KKVTableTest::CaseSetUp()
{
    MultiPathDataFlushController::GetDataFlushController("")->Reset();

    PKeyTableType tableType = std::get<0>(GET_CASE_PARAM());
    bool useStrHash = std::get<1>(GET_CASE_PARAM());
    bool fileCompress = std::get<2>(GET_CASE_PARAM());
    bool flushRtIndex = std::get<3>(GET_CASE_PARAM());
    bool usePackageFile = std::get<4>(GET_CASE_PARAM());
    mImpactValue = usePackageFile;
    bool ttlFromDoc = std::get<5>(GET_CASE_PARAM());
    string field = "pkey:string;skey:int32;value:uint32;";
    if (useStrHash) {
        field = "pkey:string;skey:string;value:uint32;";
    }

    mSchema = CreateSchema(field, "pkey", "skey", "value;skey;");
    ASSERT_TRUE(mSchema.get());
    if (ttlFromDoc) {
        mSchema->GetRegionSchema(DEFAULT_REGIONID)->TEST_SetTTLFromDoc(true);
    }

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
        kkvConfig->GetIndexPreference().SetSkeyParam(suffixParam);
        kkvConfig->GetIndexPreference().SetValueParam(valueParam);
    }

    KKVIndexPreference indexPreference = kkvConfig->GetIndexPreference();
    auto& valueParam = indexPreference.GetValueParam();
    if (mImpactValue) {
        valueParam.EnableValueImpact(true);
    } else if (random() % 2 == 1) {
        valueParam.EnablePlainFormat(true);
    }
    kkvConfig->SetIndexPreference(indexPreference);

    mOptions = IndexPartitionOptions();
    mOptions.GetBuildConfig(false).buildTotalMemory = 20; // 20MB
    mOptions.GetBuildConfig(true).buildTotalMemory = 20;  // 20MB
    mOptions.GetBuildConfig(false).keepVersionCount = 10;
    mOptions.GetBuildConfig(true).keepVersionCount = 10;
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = flushRtIndex;
    mOptions.GetOnlineConfig().useSwapMmapFile = flushRtIndex;
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = fileCompress && usePackageFile;
    mOptions.GetOfflineConfig().fullIndexStoreKKVTs = true;
    mOptions.GetBuildConfig(false).enablePackageFile = usePackageFile;
    mOptions.GetMergeConfig().SetEnablePackageFile(usePackageFile);

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

void KKVTableTest::CaseTearDown() {}

void KKVTableTest::TestIncCoverAllRtIndex()
{
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102";

    PartitionStateMachine psm(false, partition::IndexPartitionResource());
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pkey1", "skey=1,value=1;skey=2,value=2"));

    string rtDocString = "cmd=add,pkey=pkey1,skey=4,value=4,ts=104;"
                         "cmd=add,pkey=pkey1,skey=2,value=12,ts=105;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1", "skey=2,value=12;skey=4,value=4;skey=1,value=1"));

    ASSERT_TRUE(psm.Transfer(BUILD_INC, rtDocString, "pkey1", "skey=2,value=12;skey=4,value=4;skey=1,value=1"));

    rtDocString = "cmd=add,pkey=pkey1,skey=4,value=5,ts=106;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1", "skey=2,value=12;skey=4,value=5;skey=1,value=1"));
}

void KKVTableTest::TestIncCoverAllRtIndexWithAsyncDump()
{
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102";
    DumpSegmentContainerPtr dumpContainer(new SlowDumpSegmentContainer(500));
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
    PartitionStateMachine psm(false, partition::IndexPartitionResource(), dumpContainer);

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pkey1", "skey=1,value=1;skey=2,value=2"));

    string rtDocString = "cmd=add,pkey=pkey1,skey=4,value=4,ts=104;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "pkey1", "skey=2,value=2;skey=4,value=4;skey=1,value=1"));
    rtDocString = "cmd=add,pkey=pkey1,skey=2,value=12,ts=105;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1", "skey=2,value=12;skey=4,value=4;skey=1,value=1"));
    ASSERT_EQ(1u, dumpContainer->GetDumpItemSize());
    ASSERT_TRUE(psm.Transfer(BUILD_INC, rtDocString, "pkey1", "skey=2,value=12;skey=1,value=1"));
    ASSERT_EQ(0u, dumpContainer->GetDumpItemSize());
    rtDocString = "cmd=add,pkey=pkey1,skey=4,value=5,ts=106;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1", "skey=2,value=12;skey=4,value=5;skey=1,value=1"));
}

void KKVTableTest::TestSearchPkey()
{
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102";

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pkey1", "skey=1,value=1;skey=2,value=2"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2", ""));

    string rtDocString = "cmd=add,pkey=pkey1,skey=4,value=4,ts=104;"
                         "cmd=add,pkey=pkey1,skey=2,value=12,ts=105;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1", "skey=2,value=12;skey=4,value=4;skey=1,value=1"));

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "", "", ""));
    string newRtDocString = "cmd=add,pkey=pkey1,skey=4,value=24,ts=106;"
                            "cmd=add,pkey=pkey1,skey=2,value=22,ts=107;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, newRtDocString, "pkey1", "skey=2,value=22;skey=4,value=24;skey=1,value=1;"));
    std::cout << "case finish" << std::endl;
    // codegen::CodeFactory::resetCode();
}

void KKVTableTest::TestSearchOnlyRt()
{
    string docString = "";

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    string rtDocString = "cmd=add,pkey=pkey1,skey=4,value=4,ts=104;"
                         "cmd=add,pkey=pkey1,skey=2,value=12,ts=105;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1", "skey=2,value=12;skey=4,value=4"));

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "", "", ""));
    string newRtDocString = "cmd=add,pkey=pkey1,skey=4,value=24,ts=106;"
                            "cmd=add,pkey=pkey1,skey=2,value=22,ts=107;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, newRtDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1", "skey=2,value=22;skey=4,value=24;"));
}

void KKVTableTest::TestOptimizeMergeWithoutTs()
{
    string docString = "cmd=add,pkey=1,skey=101,value=1,ts=10000000;"
                       "cmd=add,pkey=1,skey=102,value=2,ts=30000000";

    mOptions.GetOnlineConfig().ttl = 20;
    mOptions.GetOfflineConfig().fullIndexStoreKKVTs = false;

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#50", "skey=101,value=1;skey=102,value=2;"));
}

void KKVTableTest::TestOptimizeMergeWithUserTs()
{
    string docString = "cmd=add,pkey=1,skey=101,value=1,ts=30000000,ha_reserved_timestamp=10000000;"
                       "cmd=add,pkey=1,skey=102,value=2,ts=10000000,ha_reserved_timestamp=30000000";
    mOptions.GetOnlineConfig().ttl = 20;
    mOptions.GetOfflineConfig().fullIndexStoreKKVTs = false;
    mOptions.GetOfflineConfig().buildConfig.useUserTimestamp = true;

    PartitionStateMachine psmUTS;
    ASSERT_TRUE(psmUTS.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psmUTS.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_FALSE(psmUTS.Transfer(QUERY, "", "1#50", "skey=101,value=1;skey=102,value=2;"));
    ASSERT_TRUE(psmUTS.Transfer(QUERY, "", "1#50", "skey=102,value=2;"));
}

void KKVTableTest::TestDeletePKeys()
{
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102;"
                       "cmd=add,pkey=pkey2,skey=3,value=3,ts=102;"
                       "cmd=add,pkey=pkey2,skey=4,value=4,ts=102;"
                       "cmd=add,pkey=pkey3,skey=5,value=5,ts=102;"
                       "cmd=add,pkey=pkey3,skey=6,value=6,ts=102;"
                       "cmd=delete,pkey=pkey1,ts=102;"
                       "cmd=delete,pkey=pkey2,ts=102;"
                       "cmd=add,pkey=pkey2,skey=7,value=7,ts=102;";

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pkey1", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2", "skey=7,value=7"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey3", "skey=5,value=5;skey=6,value=6"));

    docString = "cmd=add,pkey=pkey4,skey=8,value=8,ts=103;"
                "cmd=add,pkey=pkey4,skey=9,value=9,ts=103;"
                "cmd=add,pkey=pkey5,skey=10,value=10,ts=103;"
                "cmd=add,pkey=pkey5,skey=11,value=11,ts=103;"
                "cmd=delete,pkey=pkey4,ts=103;"
                "cmd=delete,pkey=pkey5,ts=103;"
                "cmd=add,pkey=pkey4,skey=12,value=12,ts=103;"
                "cmd=delete,pkey=pkey3,ts=103;"
                "cmd=delete,pkey=pkey2,ts=103;"
                "cmd=add,pkey=pkey1,skey=1,value=1,ts=103;"
                "cmd=add,pkey=pkey2,skey=2,value=2,ts=103;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "pkey3", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey4", "skey=12,value=12"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey5", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1", "skey=1,value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2", "skey=2,value=2"));
}

void KKVTableTest::TestDeleteDocs()
{
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102;"
                       "cmd=add,pkey=pkey2,skey=3,value=3,ts=102;"
                       "cmd=add,pkey=pkey2,skey=4,value=4,ts=102;"
                       "cmd=delete,pkey=pkey2,skey=4,value=2,ts=102;"
                       "cmd=delete,pkey=pkey2,skey=5,value=2,ts=102;"
                       "cmd=delete,pkey=pkey1,ts=102;"
                       "cmd=delete,pkey=pkey3,ts=102;"
                       "cmd=delete,pkey=pkey3,skey=5,ts=102;";

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pkey1", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2", "skey=3,value=3"));

    docString = "cmd=add,pkey=pkey3,skey=5,value=5,ts=103;"
                "cmd=add,pkey=pkey3,skey=6,value=6,ts=103;"
                "cmd=add,pkey=pkey4,skey=7,value=7,ts=103;"
                "cmd=add,pkey=pkey4,skey=8,value=8,ts=103;"
                "cmd=delete,pkey=pkey4,ts=103;"
                "cmd=delete,pkey=pkey3,skey=6,ts=103;"
                "cmd=delete,pkey=pkey4,skey=7,ts=103;"
                "cmd=add,pkey=pkey4,skey=9,value=9,ts=103;"
                "cmd=delete,pkey=pkey5,skey=9,value=9,ts=103";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "pkey3", "skey=5,value=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey4", "skey=9,value=9"));
}
void KKVTableTest::TestRtDeleteDoc()
{
    string docString = "cmd=add,pkey=pkey2,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;";

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    docString = "cmd=delete,pkey=pkey1,skey=1,ts=102;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "pkey1", ""));
}

void KKVTableTest::TestDocTTL()
{
    DoTestDocTTL(false);
    DoTestDocTTL(true);
}

void KKVTableTest::DoTestDocTTL(bool valueInline)
{
    TearDown();
    SetUp();
    mSchema->GetRegionSchema(DEFAULT_REGIONID)->TEST_SetTTLFromDoc(true);
    constexpr uint32_t defaultTTL = 50000000;
    mSchema->SetDefaultTTL(defaultTTL, DEFAULT_REGIONID, "ttl");
    ASSERT_TRUE(mSchema->TTLEnabled());

    auto kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    kkvConfig->GetIndexPreference().TEST_SetValueInline(valueInline);

    ASSERT_EQ(std::string("ttl"), mSchema->GetTTLFieldName());
    int64_t currentTs = autil::TimeUtility::currentTimeInSeconds();
    string docString = string("cmd=add,pkey=pkey1,skey=1,value=1,ts=") + std::to_string(currentTs * 1000000) +
                       ",ttl=1000000;" +
                       "cmd=add,pkey=pkey1,skey=2,value=20,ts=" + std::to_string(currentTs * 1000000) + ",ttl=1000000;";

    mOptions.GetBuildConfig(false).levelNum = 3;
    MergeConfig& mergeConfig = mOptions.GetMergeConfig();
    mergeConfig.mergeStrategyStr = "shard_based";
    mergeConfig.mergeStrategyParameter.strategyConditions = "space_amplification=100.0";

    {
        PartitionStateMachine psm(true, partition::IndexPartitionResource(), mDumpSegmentContainer);
        ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

        docString =
            string("cmd=add,pkey=pkey1,skey=2,value=2,ts=") + std::to_string((currentTs + 1) * 1000000) + ",ttl=100;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString, "", ""));

        auto pkConfig = psm.GetIndexPartition()->GetSchema()->GetIndexSchema()->GetPrimaryKeyIndexConfig();
        auto kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, pkConfig);
        ASSERT_EQ(defaultTTL, kvConfig->GetTTL());

        string query = string("pkey1#") + std::to_string((currentTs + 200));
        string result = string("skey=1,value=1,ts=") + std::to_string(currentTs * 1000000);
        ASSERT_TRUE(psm.Transfer(QUERY, "", query, result));
        query = string("pkey1#") + std::to_string((currentTs + 1000002));
        ASSERT_TRUE(psm.Transfer(QUERY, "", query, ""));

        query = string("pkey1#") + std::to_string((currentTs));
        result = string("skey=1,value=1,ts=") + std::to_string(currentTs * 1000000);
        result += ";skey=2,value=2,ts=" + std::to_string((currentTs + 1) * 1000000);
        ASSERT_TRUE(psm.Transfer(QUERY, "", query, result));

        // expire doc
        docString =
            string("cmd=add,pkey=pkey1,skey=2,value=2,ts=") + std::to_string((currentTs - 10) * 1000000) + ",ttl=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString, "", ""));

        query = string("pkey1#") + std::to_string((currentTs));
        result = string("skey=1,value=1,ts=") + std::to_string(currentTs * 1000000);
        ASSERT_TRUE(psm.Transfer(QUERY, "", query, result));
    }
    // 1. cache, TODO: add test
    // 2. value_inline TODO: add test
    // 3. merge delete TODO: add test

    {
        mOptions.GetMergeConfig().mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
        PartitionStateMachine psm(true, partition::IndexPartitionResource(), mDumpSegmentContainer);
        psm.SetCurrentTimestamp((currentTs + 1000002ll) * 1000000);
        ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));

        psm.Transfer(BUILD_INC, "", "", "");
        Version version;
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        ASSERT_EQ(1u, version.GetSegmentCount());
        auto segId = version[0];
        auto segData = psm.GetIndexPartition()->GetPartitionData()->GetSegmentData(segId);
        ASSERT_EQ(0u, segData.GetSegmentInfo()->docCount);
    }
}

void KKVTableTest::TestCacheSomeSKeyExpired()
{
    mSchema->GetRegionSchema(DEFAULT_REGIONID)->TEST_SetTTLFromDoc(true);
    constexpr uint32_t defaultTTL = 50000000;
    mSchema->SetDefaultTTL(defaultTTL, DEFAULT_REGIONID, "ttl");
    ASSERT_TRUE(mSchema->TTLEnabled());

    ASSERT_EQ(std::string("ttl"), mSchema->GetTTLFieldName());
    int64_t currentTs = autil::TimeUtility::currentTimeInSeconds();
    string docString =
        string("cmd=add,pkey=pkey1,skey=1,value=10,ts=") + std::to_string(currentTs * 1000000) + ",ttl=1000000;" +
        "cmd=add,pkey=pkey1,skey=2,value=20,ts=" + std::to_string(currentTs * 1000000) + ",ttl=10000;" +
        +"cmd=add,pkey=pkey1,skey=3,value=30,ts=" + std::to_string(currentTs * 1000000) + ",ttl=1000000;";

    PartitionStateMachine psm(true, partition::IndexPartitionResource(), mDumpSegmentContainer);
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
    util::SearchCachePtr searchCache(
        new util::SearchCache(40960, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
    resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, /*partitionName*/ ""));

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    string query = string("pkey1#") + std::to_string(currentTs + 100);
    ASSERT_TRUE(psm.Transfer(QUERY, "", query, "skey=1,value=10;skey=2,value=20;skey=3,value=30"));
    // pkey in cache after search

    query = string("pkey1#") + std::to_string(currentTs + 50000);
    ASSERT_TRUE(psm.Transfer(QUERY, "", query, "skey=1,value=10;skey=3,value=30"));
}

void KKVTableTest::TestSearchPkeyAndSkeys()
{
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102";

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pkey1:1", "skey=1,value=1;"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2:1", ""));

    string rtDocString = "cmd=add,pkey=pkey1,skey=4,value=4,ts=104;"
                         "cmd=add,pkey=pkey1,skey=2,value=12,ts=105;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1:2,4", "skey=2,value=12;skey=4,value=4;"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "", "", ""));
    string newRtDocString = "cmd=add,pkey=pkey1,skey=5,value=5,ts=106;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, newRtDocString, "pkey1:2,4", "skey=2,value=12;skey=4,value=4;"));
}

void KKVTableTest::TestSearchPkeyAndSkeysSeekBySKey()
{
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102";

    PartitionStateMachine psm(false, partition::IndexPartitionResource());
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pkey1:1", "skey=1,value=1;"));

    string rtDocString = "cmd=add,pkey=pkey1,skey=2,value=4,ts=104;"
                         "cmd=add,pkey=pkey1,skey=3,value=12,ts=105;"
                         "cmd=add,pkey=pkey1,skey=4,value=12,ts=105;"
                         "cmd=add,pkey=pkey1,skey=4,value=12,ts=105;"
                         "cmd=add,pkey=pkey1,skey=5,value=12,ts=105;"
                         "cmd=add,pkey=pkey1,skey=6,value=12,ts=105;"
                         "cmd=add,pkey=pkey1,skey=7,value=12,ts=105;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "", ""));

    rtDocString = "cmd=add,pkey=pkey1,skey=8,value=5,ts=106;"
                  "cmd=add,pkey=pkey1,skey=9,value=5,ts=106;"
                  "cmd=add,pkey=pkey1,skey=10,value=5,ts=106;"
                  "cmd=add,pkey=pkey1,skey=11,value=5,ts=106;"
                  "cmd=add,pkey=pkey1,skey=12,value=5,ts=106;"
                  "cmd=add,pkey=pkey1,skey=13,value=5,ts=106;"
                  "cmd=add,pkey=pkey1,skey=14,value=5,ts=106;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "", ""));

    rtDocString = "cmd=add,pkey=pkey1,skey=15,value=6,ts=107;"
                  "cmd=add,pkey=pkey1,skey=16,value=6,ts=107;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));

    ASSERT_TRUE(
        psm.Transfer(QUERY, "", "pkey1:12,7,1,16", "skey=1,value=1;skey=7,value=12;skey=12,value=5;skey=16,value=6"));
}

void KKVTableTest::TestSearchPkeyAndSkeysInCache()
{
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = false;
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
    util::SearchCachePtr searchCache(
        new util::SearchCache(40960, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
    resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    string rtDocString = "cmd=add,pkey=pkey1,skey=4,value=4,ts=104;"
                         "cmd=add,pkey=pkey1,skey=2,value=12,ts=105;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "", ""));
    string newRtDocString = "cmd=add,pkey=pkey1,skey=5,value=5,ts=106;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, newRtDocString, "", ""));

    autil::mem_pool::Pool pool;
    auto kkvReader = psm.GetIndexPartition()->GetReader()->GetKKVReader();
    KVMetricsCollector collector;
    KKVIterator* kkvIterator = future_lite::interface::syncAwait(
        kkvReader->LookupAsync(StringView("pkey1"), vector<autil::StringView>({StringView("2"), StringView("4")}), 0,
                               tsc_default, &pool, &collector));
    ASSERT_TRUE(kkvIterator);
    ASSERT_TRUE(kkvIterator->IsValid());
    while (kkvIterator->IsValid()) {
        kkvIterator->MoveToNext();
    }
    kkvIterator->Finish();
    ASSERT_EQ(0L, collector.GetSearchCacheHitCount());
    IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, kkvIterator);
    kkvIterator = future_lite::interface::syncAwait(
        kkvReader->LookupAsync(StringView("pkey1"), vector<autil::StringView>({StringView("2"), StringView("4")}), 0,
                               tsc_default, &pool, &collector));
    ASSERT_TRUE(kkvIterator);
    ASSERT_TRUE(kkvIterator->IsValid());
    uint64_t pkey = kkvIterator->mIterator->GetPKeyHash();
    FieldType skeyDictFieldType = kkvIterator->TEST_GetSKeyDictFieldType();

    vector<int32_t> expectedSkeys {2, 4};
    int i = 0;
    while (kkvIterator->IsValid()) {
        EXPECT_EQ(expectedSkeys[i++], kkvIterator->GetCurrentSkey());
        kkvIterator->MoveToNext();
    }
    kkvIterator->Finish();
    ASSERT_EQ(1L, collector.GetSearchCacheHitCount());
    IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, kkvIterator);

    // kkvIterator = syncAwait(kkvReader->LookupAsync(
    //                             StringView("pkey1"), {}, 0, tsc_default, &pool, &collector));
    // ASSERT_TRUE(kkvIterator);
    // ASSERT_TRUE(kkvIterator->IsValid());
    // while (kkvIterator->IsValid())
    // {
    //     kkvIterator->MoveToNext();
    // }
    // kkvIterator->Finish();

    // ASSERT_EQ(1L, collector.GetSearchCacheHitCount());
    // IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, kkvIterator);

    switch (skeyDictFieldType) {
#define MACRO(type)                                                                                                    \
    case type: {                                                                                                       \
        std::vector<uint64_t> requiredSkeys = {2, 4};                                                                  \
        SKeySearchContext<FieldTypeTraits<type>::AttrItemType> skeyContext(&pool);                                     \
        skeyContext.Init(requiredSkeys);                                                                               \
        uint64_t cacheKey =                                                                                            \
            SearchCacheContext::GetCacheKey<FieldTypeTraits<type>::AttrItemType>(pkey, &skeyContext, &pool);           \
        auto cacheGuard = resource.searchCache->Get(cacheKey, DEFAULT_REGIONID);                                       \
        ASSERT_TRUE(cacheGuard.get());                                                                                 \
        requiredSkeys.push_back(9);                                                                                    \
        SKeySearchContext<FieldTypeTraits<type>::AttrItemType> newSkeyContext(&pool);                                  \
        newSkeyContext.Init(requiredSkeys);                                                                            \
        cacheKey = SearchCacheContext::GetCacheKey<FieldTypeTraits<type>::AttrItemType>(pkey, &newSkeyContext, &pool); \
        cacheGuard = resource.searchCache->Get(cacheKey, DEFAULT_REGIONID);                                            \
        ASSERT_FALSE(cacheGuard.get());                                                                                \
        break;                                                                                                         \
    }

        NUMBER_FIELD_MACRO_HELPER(MACRO);
#undef MACRO
    default:
        assert(false);
    }

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2,4", "skey=2,value=12;skey=4,value=4;"));
}

void KKVTableTest::TestSearchPkeyAndSkeysPerf()
{
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = false;
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pkey1:1", "skey=1,value=1;"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2:1", ""));

    string rtDocString = "cmd=add,pkey=pkey1,skey=4,value=4,ts=104;"
                         "cmd=add,pkey=pkey1,skey=2,value=12,ts=105;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1:2,4", "skey=2,value=12;skey=4,value=4;"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "", "", ""));
    string newRtDocString = "cmd=add,pkey=pkey1,skey=5,value=5,ts=106;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, newRtDocString, "pkey1:2,4", "skey=2,value=12;skey=4,value=4;"));

    KVMetricsCollector collector;
    autil::mem_pool::Pool mPool;
    auto kkvReader = psm.GetIndexPartition()->GetReader()->GetKKVReader();
    auto kkvIter = future_lite::interface::syncAwait(
        kkvReader->LookupAsync(StringView("pkey1"), std::vector<StringView>({StringView("2"), StringView("5")}), 0,
                               tsc_default, &mPool, &collector));

    FieldType skeyDictFieldType = kkvIter->TEST_GetSKeyDictFieldType();
    switch (skeyDictFieldType) {
#define MACRO(type)                                                                                                    \
    case type: {                                                                                                       \
        kkvIter->MoveToNext();                                                                                         \
        kkvIter->MoveToNext();                                                                                         \
        break;                                                                                                         \
    }

        NUMBER_FIELD_MACRO_HELPER(MACRO);
#undef MACRO
    default:
        assert(false);
    }

    IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, kkvIter);
}

void KKVTableTest::TestPkeyWithDifferentType()
{
    vector<string> typeStrings {"int8",  "uint8",  "int16", "uint16", "int32", "uint32",
                                "int64", "uint64", "float", "double", "string"};
    for (size_t i = 0; i < typeStrings.size(); ++i) {
        DoTestPKeyAndSkeyWithDifferentType(typeStrings[i], "string");
    }
}

void KKVTableTest::TestSkeyWithDifferentType()
{
    vector<string> typeStrings {"int8",  "uint8",  "int16", "uint16", "int32", "uint32",
                                "int64", "uint64", "float", "double", "string"};
    for (size_t i = 0; i < typeStrings.size(); ++i) {
        DoTestPKeyAndSkeyWithDifferentType("string", typeStrings[i]);
    }
}

void KKVTableTest::TestSearchWithTimestamp()
{
    string docString = "cmd=add,pkey=1,skey=101,value=1,ts=10000000;"
                       "cmd=add,pkey=1,skey=102,value=2,ts=30000000";

    mOptions.GetOnlineConfig().ttl = 20;
    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#10", "skey=101,value=1;skey=102,value=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#20", "skey=101,value=1;skey=102,value=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#40", "skey=102,value=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#50", "skey=102,value=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#60", ""));
}

void KKVTableTest::TestSearchSkeysWithOptimization()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "pkey", "skey", "value;skey;");
    string docString = "cmd=add,pkey=pkey1,skey=1,value=0,ts=101;"
                       "cmd=add,pkey=pkey1,skey=-1,value=0,ts=101;"
                       "cmd=add,pkey=pkey1,skey=3,value=0,ts=102;";

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "pkey1:-3,-4", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "pkey1:-1,0", "skey=-1;"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "pkey1:8", ""));
}

void KKVTableTest::DoTestPKeyAndSkeyWithDifferentType(const string& pkeyType, const string& skeyType)
{
    tearDown();
    setUp();
    string field = string("pkey:") + pkeyType + ";" + string("skey:") + skeyType + ";" + "value:uint32;";

    IndexPartitionSchemaPtr schema = CreateSchema(field, "pkey", "skey", "value;skey;");
    ASSERT_TRUE(schema.get());

    string docString = "cmd=add,pkey=1,skey=101,value=1,ts=101;"
                       "cmd=add,pkey=2,skey=102,value=2,ts=102";
    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

    ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=101,value=1;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "skey=102,value=2;"));
}

void KKVTableTest::TestBuildSegmentInLevel0()
{
    mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_sequence;
    mOptions.GetBuildConfig(false).levelNum = 1;

    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;";
    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    string incDocString = "cmd=add,pkey=pkey1,skey=4,value=4,ts=104;";
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocString, "", ""));

    Version version;
    VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);

    indexlibv2::framework::LevelInfo info = version.GetLevelInfo();
    ASSERT_EQ(1u, info.GetLevelCount());

    const vector<segmentid_t>& segments = info.levelMetas[0].segments;
    vector<segmentid_t> expected = {0, 1};
    ASSERT_EQ(expected, segments);
}

void KKVTableTest::TestBuildShardingSegment()
{
    mOptions.GetBuildConfig(false).shardingColumnNum = 2;
    mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    mOptions.GetBuildConfig(false).levelNum = 2;
    // mOptions.GetBuildConfig(false).enablePackageFile = true;

    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    IndexBuilder indexBuilder(GET_TEMP_DATA_PATH(), mOptions, mSchema, quotaControl,
                              BuilderBranchHinter::Option::Test());
    indexBuilder.Init();

    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102;"
                       "cmd=add,pkey=pkey2,skey=3,value=3,ts=102;"
                       "cmd=add,pkey=pkey2,skey=4,value=4,ts=102;"
                       "cmd=add,pkey=pkey3,skey=5,value=5,ts=102;"
                       "cmd=add,pkey=pkey3,skey=6,value=6,ts=102;"
                       "cmd=delete,pkey=pkey4,ts=102;"
                       "cmd=delete,pkey=pkey5,ts=102;"
                       "cmd=add,pkey=pkey6,skey=7,value=8,ts=102;"
                       "cmd=add,pkey=pkey6,skey=7,value=7,ts=102;"; // duplicate pkey+skey
    vector<DocumentPtr> docs;
    docs = CreateKKVDocuments(mSchema, docString);
    for (size_t i = 0; i < docs.size(); i++) {
        indexBuilder.Build(docs[i]);
    }
    indexBuilder.EndIndex();
    indexBuilder.TEST_BranchFSMoveToMainRoad();
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    OnDiskPartitionDataPtr onDiskPartData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM(), mSchema);
    DirectoryPtr seg0 = onDiskPartData->GetSegmentData(0).GetDirectory();
    ASSERT_TRUE(seg0.get());

    DirectoryPtr sharding0Dir = seg0->GetDirectory("column_0/index/pkey", true);
    ASSERT_TRUE(sharding0Dir->IsExist(PREFIX_KEY_FILE_NAME));
    ASSERT_TRUE(sharding0Dir->IsExist(SUFFIX_KEY_FILE_NAME));
    ASSERT_TRUE(sharding0Dir->IsExist(KKV_VALUE_FILE_NAME));

    DirectoryPtr sharding1Dir = seg0->GetDirectory("column_1/index/pkey", true);
    ASSERT_TRUE(sharding1Dir->IsExist(PREFIX_KEY_FILE_NAME));
    ASSERT_TRUE(sharding1Dir->IsExist(SUFFIX_KEY_FILE_NAME));
    ASSERT_TRUE(sharding1Dir->IsExist(KKV_VALUE_FILE_NAME));

    // check segment info
    SegmentInfo segInfo;
    segInfo.Load(seg0);
    ASSERT_EQ((uint32_t)2, segInfo.shardCount);
    ASSERT_EQ((uint64_t)9, segInfo.docCount);
    ASSERT_EQ((int64_t)102, segInfo.timestamp);

    OnlinePartition onlinePartition;
    IndexPartitionOptions options;
    ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, onlinePartition.Open(GET_TEMP_DATA_PATH(), "", mSchema, options));
}

void KKVTableTest::TestBuildShardingSegmentWithLegacyDoc() {}
// {
//     mOptions.GetBuildConfig(false).shardingColumnNum = 2;
//     mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
//     mOptions.GetBuildConfig(false).levelNum = 2;
//     QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
//     IndexBuilder indexBuilder(GET_TEMP_DATA_PATH(), mOptions, mSchema, quotaControl);
//     indexBuilder.Init();

//     string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
//                        "cmd=add,pkey=pkey1,skey=2,value=2,ts=102;"
//                        "cmd=add,pkey=pkey2,skey=3,value=3,ts=102;"
//                        "cmd=add,pkey=pkey2,skey=4,value=4,ts=102;"
//                        "cmd=add,pkey=pkey3,skey=5,value=5,ts=102;"
//                        "cmd=add,pkey=pkey3,skey=6,value=6,ts=102;"
//                        "cmd=delete,pkey=pkey4,ts=102;"
//                        "cmd=delete,pkey=pkey5,ts=102;"
//                        "cmd=add,pkey=pkey6,skey=7,value=8,ts=102;"
//                        "cmd=add,pkey=pkey6,skey=7,value=7,ts=102;"; // duplicate pkey+skey
//     vector<DocumentPtr> docs = CreateKKVDocuments(mSchema, docString, true);
//     for (size_t i = 0; i < docs.size(); i++) {
//         indexBuilder.Build(docs[i]);
//     }
//     indexBuilder.EndIndex();

//     GET_FILE_SYSTEM()->TEST_MountLastVersion();
//     OnDiskPartitionDataPtr onDiskPartData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM(),
//     mSchema); DirectoryPtr seg0 = onDiskPartData->GetSegmentData(0).GetDirectory(); ASSERT_TRUE(seg0.get());

//     DirectoryPtr sharding0Dir = seg0->GetDirectory("column_0/index/pkey", true);
//     ASSERT_TRUE(sharding0Dir->IsExist(PREFIX_KEY_FILE_NAME));
//     ASSERT_TRUE(sharding0Dir->IsExist(SUFFIX_KEY_FILE_NAME));
//     ASSERT_TRUE(sharding0Dir->IsExist(KKV_VALUE_FILE_NAME));

//     DirectoryPtr sharding1Dir = seg0->GetDirectory("column_1/index/pkey", true);
//     ASSERT_TRUE(sharding1Dir->IsExist(PREFIX_KEY_FILE_NAME));
//     ASSERT_TRUE(sharding1Dir->IsExist(SUFFIX_KEY_FILE_NAME));
//     ASSERT_TRUE(sharding1Dir->IsExist(KKV_VALUE_FILE_NAME));

//     // check segment info
//     SegmentInfo segInfo;
//     segInfo.Load(seg0);
//     ASSERT_EQ((uint32_t)2, segInfo.shardCount);
//     ASSERT_EQ((uint64_t)9, segInfo.docCount);
//     ASSERT_EQ((int64_t)102, segInfo.timestamp);

//     OnlinePartition onlinePartition;
//     IndexPartitionOptions options;
//     ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, onlinePartition.Open(GET_TEMP_DATA_PATH(), "", mSchema,
//     options));
// }

void KKVTableTest::TestMergeTwoSegmentSimple()
{
    mOptions.GetBuildConfig(true).maxDocCount = 2;
    mOptions.GetBuildConfig(false).maxDocCount = 2;

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey2,skey=2,value=2,ts=102;"
                       "cmd=add,pkey=pkey2,skey=3,value=3,ts=103;"
                       "cmd=add,pkey=pkey1,skey=1,value=4,ts=104;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pkey1", "skey=1,value=4;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2", "skey=2,value=2;skey=3,value=3;"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_2_level_1"));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr dir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_2_level_1", true);
    CheckKeyCount(dir, 0, 2, 3);
}

void KKVTableTest::TestMergeDuplicateSKey()
{
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=1,value=2,ts=102;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pkey1", "skey=1,value=2;"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_2_level_1"));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr dir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_2_level_1", true);
    CheckKeyCount(dir, 0, 1, 1);
}

void KKVTableTest::TestMergeDuplicateSKey_13227243()
{
    mOptions.GetBuildConfig(true).maxDocCount = 3;
    mOptions.GetBuildConfig(false).maxDocCount = 3;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102;"
                       "cmd=add,pkey=pkey1,skey=3,value=3,ts=103;"
                       "cmd=add,pkey=pkey1,skey=-1,value=4,ts=104;"
                       "cmd=add,pkey=pkey1,skey=2,value=5,ts=104;"
                       "cmd=add,pkey=pkey1,skey=3,value=6,ts=106;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_2_level_1"));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr dir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_2_level_1", true);
    CheckKeyCount(dir, 0, 1, 4);
}

void KKVTableTest::TestMergeDeleteSKey()
{
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=delete,pkey=pkey1,skey=1,ts=102;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pkey1", ""));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_2_level_1"));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr dir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_2_level_1", true);
    CheckKeyCount(dir, 0, 0, 0); // bottomlevel remove delete
}

void KKVTableTest::TestMergeDeletePKey()
{
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102;"
                       "cmd=add,pkey=pkey2,skey=3,value=3,ts=103;"
                       "cmd=delete,pkey=pkey1,ts=104;"
                       "cmd=add,pkey=pkey1,skey=4,value=4,ts=105;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pkey1", "skey=4,value=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2", "skey=3,value=3"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_5_level_1"));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr dir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_5_level_1", true);
    CheckKeyCount(dir, 0, 2, 2);
}

void KKVTableTest::TestMergeShardingSimple()
{
    mOptions.GetBuildConfig(true).maxDocCount = 2;
    mOptions.GetBuildConfig(false).maxDocCount = 2;
    mOptions.GetBuildConfig(false).shardingColumnNum = 2;
    mOptions.GetMergeConfig().mergeThreadCount = 1;

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey2,skey=3,value=3,ts=102;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102;"
                       "cmd=add,pkey=pkey2,skey=4,value=4,ts=102;"
                       "cmd=add,pkey=pkey3,skey=5,value=5,ts=102;"
                       "cmd=delete,pkey=pkey4,ts=102;"
                       "cmd=add,pkey=pkey3,skey=6,value=6,ts=102;"
                       "cmd=delete,pkey=pkey5,ts=102;"
                       "cmd=add,pkey=pkey6,skey=7,value=8,ts=102;"
                       "cmd=add,pkey=pkey6,skey=7,value=7,ts=102;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pkey1", "skey=1,value=1;skey=2,value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2", "skey=3,value=3;skey=4,value=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey3", "skey=5,value=5;skey=6,value=6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey6", "skey=7,value=7"));

    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_5_level_1"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_6_level_1"));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr dir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_5_level_1", true);
    CheckKeyCount(dir, 0, 1, 2);

    dir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_6_level_1", true);
    CheckKeyCount(dir, 1, 3, 5);

    string rtDocStr = "cmd=add,pkey=pkey7,skey=8,value=9,ts=1000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocStr, "pkey7", "skey=8,value=9"));
}

void KKVTableTest::TestMergeWithCountLimit()
{
    KKVIndexFieldInfo suffixFieldInfo;
    suffixFieldInfo.fieldName = "skey";
    suffixFieldInfo.keyType = KKVKeyType::SUFFIX;
    suffixFieldInfo.countLimits = 4;
    suffixFieldInfo.sortParams = StringToSortParams("-$TIME_STAMP");

    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    kkvConfig->SetSuffixFieldInfo(suffixFieldInfo);

    string docString = "cmd=add,pkey=pkey1,skey=1,value=6,ts=1000000;"
                       "cmd=add,pkey=pkey1,skey=2,value=5,ts=2000000;"
                       "cmd=add,pkey=pkey1,skey=3,value=4,ts=3000000;"
                       "cmd=add,pkey=pkey1,skey=4,value=1,ts=4000000;"
                       "cmd=add,pkey=pkey1,skey=5,value=2,ts=5000000;"
                       "cmd=add,pkey=pkey1,skey=6,value=3,ts=6000000;";

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

    mOptions.GetBuildConfig(false).maxDocCount = 2;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(
        psm.Transfer(BUILD_FULL, docString, "pkey1", "skey=6,value=3;skey=5,value=2;skey=4,value=1;skey=3,value=4"));

    suffixFieldInfo.sortParams = StringToSortParams("-value");
    kkvConfig->SetSuffixFieldInfo(suffixFieldInfo);
    kkvConfig->SetSuffixFieldInfo(suffixFieldInfo);

    PartitionStateMachine psm1;
    ASSERT_TRUE(psm1.Init(mSchema, mOptions, GET_TEMP_DATA_PATH() + "/1"));
    ASSERT_TRUE(
        psm1.Transfer(BUILD_FULL, docString, "pkey1", "skey=1,value=6;skey=2,value=5;skey=3,value=4;skey=6,value=3"));
}

void KKVTableTest::TestMergeWithOneColumnHasNoKey()
{
    PartitionStateMachine psm;
    mOptions.GetBuildConfig(false).shardingColumnNum = 2;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pkey1", "skey=1,value=1;skey=2,value=2"));
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr dir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_1_level_1", true);
    CheckKeyCount(dir, 0, 1, 2);
    dir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_2_level_1", true);
    CheckKeyCount(dir, 1, 0, 0);
}

void KKVTableTest::TestFullBuild()
{
    InnerTestBuild(30, 0, 0, 1, 10);

    // simple 2 sharding
    InnerTestBuild(100, 0, 0, 2, 10);

    // sharding count (8) > MAX_DIST_PKEY_COUNT (5), test empty sharding
    InnerTestBuild(50, 0, 0, 8, 10);
}

void KKVTableTest::TestIncBuildNoMerge() { InnerTestBuild(30, 10, 0, 1, 10, false); }

void KKVTableTest::TestIncBuildOptimizeMerge_1()
{
    // no full docs with diff sharding count
    InnerTestBuild(0, 15, 0, 1, 5);
}

void KKVTableTest::TestIncBuildOptimizeMerge_2() { InnerTestBuild(0, 15, 0, 2, 5); }

void KKVTableTest::TestIncBuildOptimizeMerge_3() { InnerTestBuild(10, 15, 0, 1, 5); }

void KKVTableTest::TestIncBuildOptimizeMerge_4() { InnerTestBuild(10, 15, 0, 2, 5); }

void KKVTableTest::TestRtBuild_1()
{
    // no full, only rt
    std::cout << "==1==" << std::endl;
    InnerTestBuild(0, 0, 15, 1, 5);
    std::cout << "==2==" << std::endl;
    InnerTestBuild(0, 0, 15, 2, 5);
}

void KKVTableTest::TestRtBuild_2()
{
    // // has full & rt
    std::cout << "==3==" << std::endl;
    InnerTestBuild(10, 0, 15, 1, 5);
    // InnerTestBuild(80, 0, 20, 1, 10);
    std::cout << "==4==" << std::endl;
    // InnerTestBuild(10, 0, 15, 2, 10);
}

void KKVTableTest::TestShardBasedMerge()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    mSchema = CreateSchema(field, "pkey", "skey", "value;skey;");
    ASSERT_TRUE(mSchema.get());

    mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    mOptions.GetBuildConfig(false).shardingColumnNum = 2;
    mOptions.GetBuildConfig(false).levelNum = 3;
    mOptions.GetBuildConfig(false).keepVersionCount = 100;

    // segment size : get all file size in segment directory, not precise for packageFile
    mOptions.SetEnablePackageFile(false);

    MergeConfig& mergeConfig = mOptions.GetMergeConfig();
    mergeConfig.mergeStrategyStr = "shard_based";
    mergeConfig.mergeStrategyParameter.strategyConditions = "space_amplification=1.4";

    BuildDoc(1000);
    CheckLevelInfo({{}, {0, {-1, -1}}, {0, {1, 2}}});
    BuildDoc(500);
    CheckLevelInfo({{}, {1, {-1, 5}}, {0, {4, 2}}});
}

void KKVTableTest::TestShardBasedMergeWithBadDocs()
{
    // fix bug: xxxx://invalid/issue/38787282
    string field = "pkey:string;skey:int32;value:uint32;";
    mSchema = CreateSchema(field, "pkey", "skey", "value;skey;");
    ASSERT_TRUE(mSchema.get());

    mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    mOptions.GetBuildConfig(false).shardingColumnNum = 2;
    mOptions.GetBuildConfig(false).levelNum = 3;
    mOptions.GetBuildConfig(false).keepVersionCount = 100;

    // segment size : get all file size in segment directory, not precise for packageFile
    mOptions.SetEnablePackageFile(false);

    MergeConfig& mergeConfig = mOptions.GetMergeConfig();
    mergeConfig.mergeStrategyStr = "shard_based";
    mergeConfig.mergeStrategyParameter.strategyConditions = "space_amplification=1.4";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=pkey1,value=1,ts=101;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pkey1", ""));
}

void KKVTableTest::CheckKeyCount(const DirectoryPtr& segDir, uint32_t shardId, size_t expectPkeyCount,
                                 size_t expectSkeyCount)
{
    indexlib::framework::SegmentMetrics segmentMetrics;
    segmentMetrics.Load(segDir);

    string groupName = SegmentWriter::GetMetricsGroupName(shardId);
    size_t pkeyCount, skeyCount;

    segmentMetrics.Get(groupName, KKV_PKEY_COUNT, pkeyCount);
    segmentMetrics.Get(groupName, KKV_SKEY_COUNT, skeyCount);

    ASSERT_EQ(expectPkeyCount, pkeyCount);
    ASSERT_EQ(expectSkeyCount, skeyCount);
}

void KKVTableTest::InnerTestFullBuildWithTruncate(size_t fullDocCount, size_t shardingCount, size_t maxDocCount,
                                                  const std::string& sortParamsStr)
{
    tearDown();
    setUp();

    KKVIndexFieldInfo suffixFieldInfo;
    suffixFieldInfo.fieldName = "skey";
    suffixFieldInfo.keyType = KKVKeyType::SUFFIX;
    suffixFieldInfo.countLimits = SKEY_COUNT_LIMITS;
    suffixFieldInfo.sortParams = StringToSortParams(sortParamsStr);

    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    kkvConfig->SetSuffixFieldInfo(suffixFieldInfo);

    mOptions.GetBuildConfig(false).maxDocCount = maxDocCount;
    mOptions.GetBuildConfig(true).maxDocCount = maxDocCount;
    mOptions.GetBuildConfig(false).shardingColumnNum = shardingCount;
    mOptions.GetMergeConfig().mergeThreadCount = shardingCount;
    mOptions.GetOnlineConfig().ttl = KKV_TEST_TTL;
    mOptions.GetBuildConfig(false).ttl = KKV_TEST_TTL;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ExpectResultInfos resultInfos;
    string fullDocStr = MakeDocs(fullDocCount, resultInfos);
    // cout << fullDocStr << endl;

    // full merge tigger will ttl reclaim
    int32_t curTs = 20;
    psm.SetCurrentTimestamp(curTs * 1000000);
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocStr, "", ""));

    int32_t deadLineTs = curTs - KKV_TEST_TTL;
    resultInfos.RemoveObseleteItems(deadLineTs);
    resultInfos.TruncateItems(SKEY_COUNT_LIMITS, sortParamsStr);
    CheckResult(psm, resultInfos, curTs);
}

void KKVTableTest::InnerTestRtBuildWithTruncate(size_t rtDocCount, const std::string& sortParamsStr)
{
    tearDown();
    setUp();

    size_t maxDocCount = (uint32_t)-1;
    size_t skeyCountLimits = SKEY_COUNT_LIMITS;

    KKVIndexFieldInfo suffixFieldInfo;
    suffixFieldInfo.fieldName = "skey";
    suffixFieldInfo.keyType = KKVKeyType::SUFFIX;
    suffixFieldInfo.countLimits = skeyCountLimits;
    suffixFieldInfo.sortParams = StringToSortParams(sortParamsStr);

    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    kkvConfig->SetSuffixFieldInfo(suffixFieldInfo);

    mOptions.GetBuildConfig(false).maxDocCount = maxDocCount;
    mOptions.GetBuildConfig(true).maxDocCount = maxDocCount;
    mOptions.GetBuildConfig(false).ttl = INVALID_TTL;
    mOptions.GetBuildConfig(true).ttl = INVALID_TTL;
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = false;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));

    ExpectResultInfos resultInfos;
    string rtDocStr = MakeDocs(rtDocCount, resultInfos);
    // cout << rtDocStr << endl;

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocStr, "", ""));
    resultInfos.TruncateItems(skeyCountLimits, sortParamsStr);
    CheckResult(psm, resultInfos, 20, INVALID_TTL);
}

void KKVTableTest::InnerTestBuild(size_t fullDocCount, size_t incDocCount, size_t rtDocCount, size_t shardingCount,
                                  size_t maxDocCount, bool mergeInc)
{
    tearDown();
    setUp();
    mOptions.GetBuildConfig(false).maxDocCount = maxDocCount;
    mOptions.GetBuildConfig(true).maxDocCount = maxDocCount;
    mOptions.GetBuildConfig(false).shardingColumnNum = shardingCount;
    mOptions.GetMergeConfig().mergeThreadCount = shardingCount;
    mOptions.GetOnlineConfig().ttl = KKV_TEST_TTL;
    mOptions.GetBuildConfig(false).ttl = KKV_TEST_TTL;

    PartitionStateMachine psm(false, partition::IndexPartitionResource());
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ExpectResultInfos resultInfos;
    string fullDocStr = MakeDocs(fullDocCount, resultInfos);

    // full merge tigger will ttl reclaim
    int32_t curTs = 20;
    psm.SetCurrentTimestamp(curTs * 1000000);
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocStr, "", ""));

    int32_t deadLineTs = curTs - KKV_TEST_TTL;
    resultInfos.RemoveObseleteItems(deadLineTs);
    CheckResult(psm, resultInfos, curTs);
    // inc merge will not trigger ttl reclaim
    psm.SetMergeTTL(numeric_limits<int32_t>::max());
    if (incDocCount > 0) {
        string incDocStr = MakeDocs(incDocCount, resultInfos);
        if (mergeInc) {
            // std::cout << "inc build: " << incDocStr << std::endl;
            ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocStr, "", ""));
        } else {
            // std::cout << "inc build no merge: " << incDocStr << std::endl;
            ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr, "", ""));
        }
        CheckResult(psm, resultInfos, curTs);
    }
    if (rtDocCount > 0) {
        string rtDocStr = MakeDocs(rtDocCount, resultInfos);
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocStr, "", ""));
        CheckResult(psm, resultInfos, curTs);
    }
}

void KKVTableTest::Transfer(PsmEvent event, size_t docCount, const vector<string>& mergeInfo, int64_t ttl,
                            bool pkeyUnique)
{
    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

    string docString = MakeDocs(docCount, mResultInfos, 1, pkeyUnique);
    if (mergeInfo.empty()) {
        mOptions.GetMergeConfig().mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
        mOptions.GetMergeConfig().mergeStrategyParameter.strategyConditions.clear();
    } else {
        mOptions.GetMergeConfig().mergeStrategyStr = mergeInfo[0];
        if (mergeInfo.size() == 2) {
            mOptions.GetMergeConfig().mergeStrategyParameter.strategyConditions = mergeInfo[1];
        }
    }
    mOptions.GetBuildConfig(false).ttl = ttl;
    mOptions.GetOnlineConfig().ttl = ttl;
    psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH());
    int32_t curTs = mResultInfos.GetMaxTs() + 1;
    psm.SetCurrentTimestamp(curTs * 1000000);
    ASSERT_TRUE(psm.Transfer(event, docString, "", ""));
    auto part = psm.GetIndexPartition();
    auto partData = part->GetPartitionData();
    size_t indexSize = 0;
    OnDiskSegmentSizeCalculator calculator;
    index_base::SegmentDirectoryPtr segmentDirector(partData->GetSegmentDirectory()->Clone());
    for (auto& segmentData : segmentDirector->GetSegmentDatas()) {
        indexSize += calculator.GetSegmentSize(segmentData, mSchema);
    }

    mResultInfos.mIndexSize = indexSize;
    int64_t deadLineTs = curTs - ttl;
    if (event & PE_MERGE_MASK) {
        mResultInfos.RemoveObseleteItems(deadLineTs);
    }

    CheckResult(psm, mResultInfos, curTs, ttl, mergeInfo.empty());
}

void KKVTableTest::CheckLocator(int64_t locator, int64_t ts)
{
    Version version;
    VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
    indexlibv2::framework::Locator versionLocator;
    versionLocator.Deserialize(version.GetLocator().ToString());
    ASSERT_EQ(locator, versionLocator.GetOffset().first);
    ASSERT_EQ(ts * 1000000, version.GetTimestamp());
}

void KKVTableTest::TestMergeClearTable()
{
    mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    mOptions.GetBuildConfig(false).shardingColumnNum = 2;
    mOptions.GetBuildConfig(false).maxDocCount = 1;
    mOptions.GetBuildConfig(false).levelNum = 2;
    mOptions.GetBuildConfig(false).keepVersionCount = 100;

    Transfer(BUILD_FULL, 2, {}, 0); // clear table
    CheckLevelInfo({{}, {0, {-1, -1}}});
    CheckLocator(2, 2);

    Transfer(BUILD_INC, 1, {}, DEFAULT_TIME_TO_LIVE);

    CheckLevelInfo({{}, {0, {3, 4}}});
    CheckLocator(3, 3);

    // clear table with shard_based merge strategy
    Transfer(BUILD_INC, 0, {"shard_based", "space_amplification=1.3"}, 0);
    CheckLevelInfo({{}, {0, {-1, -1}}});

    Transfer(BUILD_INC, 1, {"shard_based", "space_amplification=1.3"}, DEFAULT_TIME_TO_LIVE);
    CheckLevelInfo({{}, {0, {6, 7}}});
    CheckLocator(4, 4);
}

void KKVTableTest::TestMultiPartMerge()
{
    InnerTestMultiPartMerge(1); // no shard
    // InnerTestMultiPartMerge(4);  // shard
}

void KKVTableTest::TestFullMergeWithSKeyTruncate_1() { InnerTestFullBuildWithTruncate(200, 1, 20, "-$TIME_STAMP"); }

void KKVTableTest::TestFullMergeWithSKeyTruncate_2() { InnerTestFullBuildWithTruncate(100, 4, 20, "+value"); }

void KKVTableTest::TestFullMergeWithSKeyTruncate_3()
{
    InnerTestFullBuildWithTruncate(100, 4, 20, "+value;+$TIME_STAMP");
}

void KKVTableTest::TestRtWithSKeyTruncate()
{
    InnerTestRtBuildWithTruncate(10, "-$TIME_STAMP");
    InnerTestRtBuildWithTruncate(100, "+value");
    InnerTestRtBuildWithTruncate(200, "+value;+$TIME_STAMP");
}

void KKVTableTest::TestNoNeedOptimizeMerge()
{
    mOptions.GetBuildConfig(false).shardingColumnNum = 2;
    mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    mOptions.GetBuildConfig(false).levelNum = 3;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    CheckLevelInfo({{}, {0, {-1, -1}}, {0, {1, 2}}});

    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    CheckLevelInfo({{}, {0, {-1, -1}}, {0, {1, 2}}});
}

void KKVTableTest::TestNoNeedShardBasedMerge()
{
    mOptions.GetBuildConfig(false).shardingColumnNum = 2;
    mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    mOptions.GetBuildConfig(false).levelNum = 3;

    MergeConfig& mergeConfig = mOptions.GetMergeConfig();
    mergeConfig.mergeStrategyStr = "shard_based";
    mergeConfig.mergeStrategyParameter.strategyConditions = "space_amplification=1.4";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    CheckLevelInfo({{}, {0, {-1, -1}}, {0, {1, 2}}});

    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    CheckLevelInfo({{}, {0, {-1, -1}}, {0, {1, 2}}});
}

void KKVTableTest::TestMergeMultiTimesLongCaseTest()
{
    /* useless ong case
    InnerTestMergeMultiTimes(1, false); // no shard
    InnerTestMergeMultiTimes(4, false); // shard

    InnerTestMergeMultiTimes(1, true); // pkeyunique, test if ttl could reclaim expired docs
    InnerTestMergeMultiTimes(4, true);
    */
}

void KKVTableTest::TestLongValue()
{
    string field = "pkey:string;skey:int32;value:string;";
    mSchema = CreateSchema(field, "pkey", "skey", "value;skey;");
    ASSERT_TRUE(mSchema.get());

    mOptions = IndexPartitionOptions();
    mOptions.GetBuildConfig(false).buildTotalMemory = 20; // 20MB
    mOptions.GetBuildConfig(true).buildTotalMemory = 20;  // 20MB
    mOptions.GetBuildConfig(false).keepVersionCount = 10;
    mOptions.GetBuildConfig(true).keepVersionCount = 10;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string valueStr(1024 * 1024 * 8, 'b');
    stringstream ss;
    ss << "cmd=add,pkey=pkey1,skey=1,value=" << valueStr << ",ts=101;";
    string docString = ss.str();
    string expectValue = "skey=1,value=" + valueStr;
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pkey1", expectValue));
}

void KKVTableTest::TestReopen()
{
    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string rtDocString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=100000000;"
                         "cmd=add,pkey=pkey1,skey=2,value=2,ts=200000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1", "skey=1;skey=2;"));

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "", "", ""));
    string incDocString = "cmd=add,pkey=pkey1,skey=3,value=3,ts=130000000;"
                          "cmd=add,pkey=pkey1,skey=4,value=4,ts=140000000;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "pkey1", "skey=2;skey=3;skey=4;"));
}

void KKVTableTest::TestSearchWithBlockCache()
{
    string jsonStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["_KKV_PKEY_"], "load_strategy" : "mmap" },
         { "file_patterns" : ["_KKV_SKEY_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : false, "cache_decompress_file" : true }},
         { "file_patterns" : ["_KKV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    InnerTestSearchWithBlockCache(jsonStr);

    jsonStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["_KKV_PKEY_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }},
         { "file_patterns" : ["_KKV_SKEY_"], "load_strategy" : "mmap" },
         { "file_patterns" : ["_KKV_VALUE_"], "load_strategy" : "mmap" }]
    })";
    // only PKT_DENSE support block cache
    if (GET_PARAM_VALUE(0) == PKT_DENSE) {
        InnerTestSearchWithBlockCache(jsonStr);
    }
}

void KKVTableTest::TestFlushRealtimeIndex()
{
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().useSwapMmapFile = true;
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = false;

    mOptions.GetBuildConfig(true).maxDocCount = 10;

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string rtDocString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=100000000;"
                         "cmd=add,pkey=pkey1,skey=2,value=2,ts=200000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1", "skey=1;skey=2;"));

    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    DirectoryPtr rootDir = indexPart->GetRootDirectory();
    IFileSystemPtr fileSystem = rootDir->GetFileSystem();
    fileSystem->CleanCache();
    StorageMetrics localMetrics = fileSystem->GetFileSystemMetrics().GetInputStorageMetrics();

    // maybe open with switched reader
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "", "", ""));
    fileSystem->Sync(true).GetOrThrow();

    // make sure switch reader for rt segment
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    fileSystem->CleanCache();

    StorageMetrics localMetrics1 = fileSystem->GetFileSystemMetrics().GetInputStorageMetrics();
    ASSERT_GT(localMetrics1.GetTotalFileLength(), localMetrics.GetTotalFileLength());
    ASSERT_GT(localMetrics1.GetTotalFileCount(), localMetrics.GetTotalFileCount());

    // background thread such as OnlinePartition::ExecuteCleanResourceTask will inc in_mem file length
    for (auto i = 5000; i >= 0; --i) {
        if (0 == fileSystem->GetFileSystemMetrics().GetOutputStorageMetrics().GetInMemFileLength(FSMG_LOCAL)) {
            break;
        }
        ASSERT_TRUE(i != 0) << "IN_MEM File Length still > 0 after retry";
        usleep(1000);
    }

    ASSERT_TRUE(indexPart->GetReader()->GetPartitionVersion()->GetLastLinkRtSegmentId() != INVALID_SEGMENTID);

    string rtSegPath = rootDir->GetRootLinkPath() + "/rt_index_partition/segment_1073741824_level_0";
    string skeyPath = rtSegPath + "/index/pkey/skey";
    string valuePath = rtSegPath + "/index/pkey/value";

    FileStat fileStat;
    ASSERT_EQ(FSEC_OK, fileSystem->TEST_GetFileStat(skeyPath, fileStat));
    ASSERT_TRUE(fileStat.inCache);
    ASSERT_EQ(FSOT_LOAD_CONFIG, fileStat.openType);
    ASSERT_NE(FSFT_MEM, fileStat.fileType);

    ASSERT_EQ(FSEC_OK, fileSystem->TEST_GetFileStat(valuePath, fileStat));
    ASSERT_EQ(FSOT_LOAD_CONFIG, fileStat.openType);
    ASSERT_NE(FSFT_MEM, fileStat.fileType);
}

void KKVTableTest::TestFixedLengthMultiValueField()
{
    InnerTestFixedLengthMultiValueField("uint8");
    InnerTestFixedLengthMultiValueField("int8");

    InnerTestFixedLengthMultiValueField("uint16");
    InnerTestFixedLengthMultiValueField("int16");

    InnerTestFixedLengthMultiValueField("uint32");
    InnerTestFixedLengthMultiValueField("int32");

    InnerTestFixedLengthMultiValueField("uint64");
    InnerTestFixedLengthMultiValueField("int64");

    InnerTestFixedLengthMultiValueField("float");
    InnerTestFixedLengthMultiValueField("double");
}

void KKVTableTest::InnerTestFixedLengthMultiValueField(const string& fieldTypeStr)
{
    tearDown();
    setUp();

    string field = "pkey:string;nid:uint64;score:" + fieldTypeStr + ":true:false::4";
    IndexPartitionSchemaPtr schema = CreateSchema(field, "pkey", "nid", "score");

    string docString = "cmd=add,pkey=pkey1,nid=1,score=1 2 2 3;"
                       "cmd=add,pkey=pkey1,nid=2,score=2 6;"
                       "cmd=add,pkey=pkey1,nid=3,score=1 0 2 4 5;";

    string rootPath = GET_TEMP_DATA_PATH() + "/fix_length";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, rootPath));
    partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
    util::SearchCachePtr searchCache(
        new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
    resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1 2 2 3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=2 6 0 0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=1 0 2 4"));

    auto fs = FileSystemCreator::Create("test", rootPath).GetOrThrow();
    fs->TEST_MountLastVersion();
    DirectoryPtr segmentDir = Directory::Get(fs)->GetDirectory("segment_1_level_1", true);
    size_t expectLen = sizeof(common::ChunkMeta) + // chunk meta + 4 count * sizeof(Type) * 3 doc
                       SizeOfFieldType(FieldConfig::StrToFieldType(fieldTypeStr)) * 4 * 3;
    ASSERT_EQ(expectLen, segmentDir->GetFileLength("index/pkey/value"));
}

void KKVTableTest::TestCompressedFixedLengthFloatField()
{
    // TODO: add single float, compress and no compress
    // single compress field : block_fp
    {
        string field = "pkey:string;nid:uint64;score:float:true:false:block_fp:4";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "pkey", "nid", "score");

        string docString = "cmd=add,pkey=pkey1,nid=1,score=1.1 2.1 2.3 3.4;"
                           "cmd=add,pkey=pkey1,nid=2,score=2.1 -6.1;"
                           "cmd=add,pkey=pkey1,nid=3,score=-1.1 0.12371 -2.3 4.0 5.1;";

        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/block_fp"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.09998 2.09998 2.30005 3.40002"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=2.1001 -6.1001 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=-1.1001 0.123779 -2.30005 4"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.09998 2.09998 2.30005 3.40002"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=2.1001 -6.1001 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=-1.1001 0.123779 -2.30005 4"));
    }

    // single compress field : fp16, with search cache
    {
        string field = "pkey:string;nid:uint64;score:float:true:false:fp16:4";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "pkey", "nid", "score");

        string docString = "cmd=add,pkey=pkey1,nid=1,score=1.1 2.1 2.3 3.4;"
                           "cmd=add,pkey=pkey1,nid=2,score=2.1 -6.1;"
                           "cmd=add,pkey=pkey1,nid=3,score=-1.1 0.12371 -2.3 4.0 5.1;";

        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/fp16"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.09961 2.09961 2.29883 3.39844"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=2.09961 -6.09766 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=-1.09961 0.123657 -2.29883 4"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.09961 2.09961 2.29883 3.39844"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=2.09961 -6.09766 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=-1.09961 0.123657 -2.29883 4"));
    }

    // single compress field : int8, with search cache
    {
        string field = "pkey:string;nid:uint64;score:float:true:false:int8#127:4";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "pkey", "nid", "score");

        string docString = "cmd=add,pkey=pkey1,nid=1,score=1 2 2 3;"
                           "cmd=add,pkey=pkey1,nid=2,score=2 -6;"
                           "cmd=add,pkey=pkey1,nid=3,score=-1 0 -2 4 5;";

        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/int8"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1 2 2 3"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=2 -6 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=-1 0 -2 4"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1 2 2 3"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=2 -6 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=-1 0 -2 4"));
    }

    // combine fields : fix length
    {
        string field = "pkey:string;nid:uint64;price:int32;score:float:true:false:int8#127:4";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "pkey", "nid", "score;price");

        string docString = "cmd=add,pkey=pkey1,nid=1,price=2,score=1 2 2 3;"
                           "cmd=add,pkey=pkey1,nid=2,price=4,score=2 -6;"
                           "cmd=add,pkey=pkey1,nid=3,price=8,score=-1 0 -2 4 5;";

        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/block_fp_fix"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "price=2,score=1 2 2 3"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "price=4,score=2 -6 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "price=8,score=-1 0 -2 4"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "price=2,score=1 2 2 3"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "price=4,score=2 -6 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "price=8,score=-1 0 -2 4"));
    }

    // combine fields : var length
    {
        string field = "pkey:string;nid:uint64;name:string;score:float:true:false:fp16:4";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "pkey", "nid", "score;name");

        string docString = "cmd=add,pkey=pkey1,nid=1,name=abc,score=1.1 2.1 2.3 3.4;"
                           "cmd=add,pkey=pkey1,nid=2,name=1234,score=2.1 -6.1;"
                           "cmd=add,pkey=pkey1,nid=3,name=helpme,score=-1.1 0.12371 -2.3 4.0 5.1;";

        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/fp16_var"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "name=abc,score=1.09961 2.09961 2.29883 3.39844"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "name=1234,score=2.09961 -6.09766 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "name=helpme,score=-1.09961 0.123657 -2.29883 4"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "name=abc,score=1.09961 2.09961 2.29883 3.39844"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "name=1234,score=2.09961 -6.09766 0 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "name=helpme,score=-1.09961 0.123657 -2.29883 4"));
    }
}

void KKVTableTest::TestSingleFloatCompress()
{
    // fp16 + single field
    {
        string field = "pkey:string;nid:uint64;score:float:false:false:fp16";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "pkey", "nid", "score");
        auto kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        kkvConfig->GetIndexPreference().TEST_SetValueInline(true);

        string docString = "cmd=add,pkey=pkey1,nid=1,score=1.1;"
                           "cmd=add,pkey=pkey1,nid=2;"
                           "cmd=add,pkey=pkey1,nid=3,score=-1.1;";
        string incDoc = "cmd=add,pkey=pkey2,nid=1,score=-10.3;"
                        "cmd=add,pkey=pkey3,nid=1,score=0;";

        PartitionStateMachine psm;
        mOptions.GetOfflineConfig().fullIndexStoreKKVTs = false;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/fp16"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.09961"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=-1.09961"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.09961"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=-1.09961"));

        // rt build
        ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,pkey=pkey1,nid=1,score=11.4", "pkey1",
                                 "nid=1,score=11.3984;nid=2,score=0;nid=3,score=-1.09961"));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.09961"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2:1", "score=-10.2969"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey3:1", "score=0"));
    }
    // int8 + single field
    {
        string field = "pkey:string;nid:uint64;score:float:false:false:int8#3";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "pkey", "nid", "score");
        auto kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        kkvConfig->GetIndexPreference().TEST_SetValueInline(true);

        string docString = "cmd=add,pkey=pkey1,nid=1,score=1.1;"
                           "cmd=add,pkey=pkey1,nid=2,score=2.1;"
                           "cmd=add,pkey=pkey1,nid=3;"
                           "cmd=add,pkey=pkey2,nid=100,score=-10.1;";
        string incDoc = "cmd=add,pkey=pkey2,nid=1,score=-10.3;"
                        "cmd=add,pkey=pkey3,nid=1,score=0;";

        PartitionStateMachine psm;
        mOptions.GetOfflineConfig().fullIndexStoreKKVTs = false;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/int8"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.11024"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=2.10236"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2:100", "score=-3"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.11024"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=2.10236"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2:100", "score=-3"));
        // rt build
        ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,pkey=pkey1,nid=3,score=1.1", "pkey1",
                                 "nid=1,score=1.11024;nid=3,score=1.11024;nid=2,score=2.10236"));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.11024"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2:1", "score=-3"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey3:1", "score=0"));
    }

    // fp16 + multi field
    {
        string field = "pkey:string;nid:uint64;score:float:false:false:fp16;comment:string;";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "pkey", "nid", "score;comment;");
        auto kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        kkvConfig->GetIndexPreference().TEST_SetValueInline(true);

        string docString = "cmd=add,pkey=pkey1,nid=1,score=1.1,comment=hello;"
                           "cmd=add,pkey=pkey1,nid=2,score=-2.1;"
                           "cmd=add,pkey=pkey1,nid=3;";
        string incDoc = "cmd=add,pkey=pkey2,nid=1,score=-10.3;"
                        "cmd=add,pkey=pkey3,nid=1,score=0;";

        PartitionStateMachine psm;
        mOptions.GetOfflineConfig().fullIndexStoreKKVTs = false;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/fp16_2"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.09961,nid=1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=-2.09961,nid=2"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=0,nid=3"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.09961"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=-2.09961"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=0"));

        // rt build
        ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,pkey=pkey1,nid=1,score=11.4", "pkey1",
                                 "nid=1,score=11.3984;nid=2,score=-2.09961;nid=3,score=0"));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.09961"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2:1", "score=-10.2969"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey3:1", "score=0"));
    }
    // int8 + multi field
    {
        string field = "pkey:string;nid:uint64;score:float:false:false:int8#3;comment:string;";
        IndexPartitionSchemaPtr schema = CreateSchema(field, "pkey", "nid", "score;comment");
        auto kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        kkvConfig->GetIndexPreference().TEST_SetValueInline(true);

        string docString = "cmd=add,pkey=pkey1,nid=1,score=1.1,comment=hello;"
                           "cmd=add,pkey=pkey1,nid=2,score=2.1;"
                           "cmd=add,pkey=pkey1,nid=3;"
                           "cmd=add,pkey=pkey2,nid=100,score=-10.1;";
        string incDoc = "cmd=add,pkey=pkey2,nid=1,score=-10.3;"
                        "cmd=add,pkey=pkey3,nid=1,score=0;";
        PartitionStateMachine psm;
        mOptions.GetOfflineConfig().fullIndexStoreKKVTs = false;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH() + "/int8_2"));
        partition::IndexPartitionResource& resource = psm.GetIndexPartitionResource();
        util::SearchCachePtr searchCache(
            new util::SearchCache(4096, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
        resource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.11024"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=2.10236"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2:100", "score=-3"));

        // search cache
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.11024"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:2", "score=2.10236"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:3", "score=0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2:100", "score=-3"));
        // rt build
        ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,pkey=pkey1,nid=3,score=1.1", "pkey1",
                                 "nid=1,score=1.11024;nid=3,score=1.11024;nid=2,score=2.10236"));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1:1", "score=1.11024"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2:1", "score=-3"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey3:1", "score=0"));
    }
}

void KKVTableTest::TestFlushRealtimeIndexForRemoteLoadConfig()
{
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().useSwapMmapFile = true;
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = false;
    mOptions.GetOnlineConfig().loadConfigList.SetLoadMode(LoadConfig::LoadMode::REMOTE_ONLY);

    mOptions.GetBuildConfig(true).maxDocCount = 10;

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);
    string rootPath = GET_TEMP_DATA_PATH() + "/local/";
    string secondRootPath = GET_TEMP_DATA_PATH() + "/remote/";
    ASSERT_TRUE(psm.Init(mSchema, mOptions, rootPath, "psm", secondRootPath));
    string rtDocString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=100000000;"
                         "cmd=add,pkey=pkey1,skey=2,value=2,ts=200000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1", "skey=1;skey=2;"));

    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    DirectoryPtr rootDir = indexPart->GetRootDirectory();
    IFileSystemPtr fileSystem = rootDir->GetFileSystem();
    fileSystem->CleanCache();
    StorageMetrics localMetrics = fileSystem->GetFileSystemMetrics().GetInputStorageMetrics();

    // maybe open with switched reader
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "", "", ""));
    fileSystem->Sync(true).GetOrThrow();

    // make sure switch reader for rt segment
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    fileSystem->CleanCache();

    StorageMetrics localMetrics1 = fileSystem->GetFileSystemMetrics().GetInputStorageMetrics();
    ASSERT_GT(localMetrics1.GetTotalFileLength(), localMetrics.GetTotalFileLength());
    ASSERT_GT(localMetrics1.GetTotalFileCount(), localMetrics.GetTotalFileCount());

    // background thread such as OnlinePartition::ExecuteCleanResourceTask will inc in_mem file length
    for (auto i = 5000; i >= 0; --i) {
        if (0 == fileSystem->GetFileSystemMetrics().GetOutputStorageMetrics().GetInMemFileLength(FSMG_LOCAL)) {
            break;
        }
        ASSERT_TRUE(i != 0) << "IN_MEM File Length still > 0 after retry";
        usleep(1000);
    }

    ASSERT_TRUE(indexPart->GetReader()->GetPartitionVersion()->GetLastLinkRtSegmentId() != INVALID_SEGMENTID);

    string rtSegPath = rootPath + rootDir->GetRootLinkPath() + "/rt_index_partition/segment_1073741824_level_0";
    string skeyPath = rtSegPath + "/index/pkey/skey";
    string valuePath = rtSegPath + "/index/pkey/value";

    FileStat fileStat;
    ASSERT_EQ(FSEC_OK, fileSystem->TEST_GetFileStat(skeyPath, fileStat));
    ASSERT_TRUE(fileStat.inCache);
    ASSERT_EQ(FSOT_LOAD_CONFIG, fileStat.openType);
    ASSERT_NE(FSFT_MEM, fileStat.fileType);

    ASSERT_EQ(FSEC_OK, fileSystem->TEST_GetFileStat(valuePath, fileStat));
    ASSERT_EQ(FSOT_LOAD_CONFIG, fileStat.openType);
    ASSERT_NE(FSFT_MEM, fileStat.fileType);
}

void KKVTableTest::InnerTestSearchWithBlockCache(const string& blockStrategyStr)
{
    tearDown();
    setUp();

    mOptions.GetOnlineConfig().loadConfigList =
        LoadConfigListCreator::CreateLoadConfigListFromJsonString(blockStrategyStr);

    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102";
    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));

    auto blockCache =
        psm.GetIndexPartitionResource().fileBlockCacheContainer->GetAvailableFileCache("")->GetBlockCache();
    ASSERT_EQ(0, blockCache->GetTotalHitCount());
    ASSERT_EQ(0, blockCache->GetTotalMissCount());
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1", "skey=1,value=1;skey=2,value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1", "skey=1,value=1;skey=2,value=2"));
    ASSERT_LT(0, blockCache->GetTotalHitCount());
    ASSERT_LE(0, blockCache->GetTotalMissCount());
}

void KKVTableTest::InnerTestMergeMultiTimes(uint32_t shardingColumnNum, bool pKeyUnique)
{
    IE_LOG(ERROR, "shardingColumnNum[%u], pKeyUnique[%d]", shardingColumnNum, pKeyUnique);
    tearDown();
    setUp();
    mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    mOptions.GetBuildConfig(false).shardingColumnNum = shardingColumnNum;
    mOptions.GetBuildConfig(false).levelNum = 4;
    mOptions.GetBuildConfig(false).keepVersionCount = 100;
    mOptions.GetBuildConfig(false).buildTotalMemory = 50; // 50MB
    mOptions.GetBuildConfig(true).buildTotalMemory = 50;  // 50MB
    mOptions.GetBuildConfig(false).keepVersionCount = 1;
    mOptions.GetOnlineConfig().useSwapMmapFile = false;
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = false;
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
    mOptions.SetEnablePackageFile(false);

    int levelNum = mOptions.GetBuildConfig(false).levelNum = 4;

    int32_t docCount = 500;

    Transfer(BUILD_FULL, docCount, {}, DEFAULT_TIME_TO_LIVE, pKeyUnique);

    // use calculator index size
    int64_t fullSize = mResultInfos.GetIndexSize();
    IE_LOG(ERROR, "FULL SIZE [%ld]", fullSize);
    int32_t sa = 100;
    for (int i = 0; i < 10; ++i) {
        int32_t incDocCount = random() % 50;
        int32_t flag = random();

        vector<string> mergeInfos;
        IE_LOG(ERROR, "DOC COUNT [%d]", incDocCount);
        if (flag & 1) {
            // optimize
            mergeInfos = {};
        } else {
            // shard_based
            sa = (random() % ((levelNum - 1) * 100));
            if (sa < 100) {
                sa += 100;
            }
            mergeInfos = {"shard_based", string("space_amplification=") + StringUtil::toString(sa / 100.0)};
            IE_LOG(ERROR, "space_amplification [%s]", StringUtil::toString(sa / 100.0).c_str());
        }
        IE_LOG(ERROR, "Num: %d", i);
        Transfer(BUILD_INC, incDocCount, mergeInfos, docCount, pKeyUnique);
        if (mOptions.GetBuildConfig(false).enablePackageFile) {
            // package meta file & gap in package data file will make indexSize bigger
            return;
        }

        int64_t indexSize = mResultInfos.GetIndexSize();
        IE_LOG(ERROR, "INDEX SIZE [%ld]", indexSize);
        if (flag & 1) {
            ASSERT_LE((double)indexSize, fullSize * 1.35);
        } else {
            ASSERT_LE((double)indexSize, fullSize * ((sa + 20) / 100.0));
        }
    }
}

void KKVTableTest::InnerTestMultiPartMerge(uint32_t shardCount)
{
    tearDown();
    setUp();
    cout << "start KKVTableTest::InnerTestMultiPartMerge" << endl;
    mOptions.GetBuildConfig(false).maxDocCount = 2;
    mOptions.GetBuildConfig(true).maxDocCount = 2;
    mOptions.GetBuildConfig(false).shardingColumnNum = shardCount;

    ExpectResultInfos resultInfos;
    vector<string> srcPaths;
    for (size_t i = 1; i <= 4; i++) {
        PartitionStateMachine psm;
        string subPath = GET_TEMP_DATA_PATH() + "/" + StringUtil::toString(i);
        srcPaths.push_back(subPath);
        ASSERT_TRUE(psm.Init(mSchema, mOptions, subPath));
        string fullDocStr = MakeDocs(10, resultInfos, i);
        ASSERT_TRUE(psm.Transfer(PE_BUILD_FULL, fullDocStr, "", ""));
    }
    cout << "KKVTableTest::InnerTestMultiPartMerge prepare data finish" << endl;

    string targetPath = GET_TEMP_DATA_PATH() + "/target";
    MultiPartitionMerger multiPartMerger(mOptions, NULL, "", CommonBranchHinterOption::Test());
    multiPartMerger.Merge(srcPaths, targetPath);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, targetPath));
    CheckResult(psm, resultInfos, 0);

    DeployIndexChecker::CheckDeployIndexMeta(targetPath, 0, INVALID_VERSION);
}

string KKVTableTest::MakeDocs(size_t docCount, ExpectResultInfos& resultInfo, uint32_t pkeyTimesNum, bool pKeyUnique)
{
    int32_t curTs = resultInfo.GetMaxTs();
    stringstream ss;
    for (size_t i = 0; i < docCount; i++) {
        ++curTs;
        int32_t flag = random() % 20;
        int32_t pkey = 0;
        if (pKeyUnique) {
            pkey = curTs;
            flag = numeric_limits<int32_t>::max();
        } else {
            pkey = (random() % MAX_DIST_PKEY_COUNT) + 1;
        }
        pkey *= pkeyTimesNum; // for multi partition build
        int32_t skey = random() % MAX_SKEY_COUNT_PER_PKEY;
        switch (flag) {
        case 0: // del pkey 5%
        {
            ss << "cmd=delete,pkey=" << pkey << ",ts=" << curTs * 1000000 << ",locator=" << curTs << ";";
            resultInfo.DelPkey(StringUtil::toString(pkey), curTs);
            break;
        }
        case 1:
        case 2:
        case 3: // del skey 15%
        {
            ss << "cmd=delete,pkey=" << pkey << ",skey=" << skey << ",ts=" << curTs * 1000000 << ",locator=" << curTs
               << ";";
            resultInfo.DelSkey(StringUtil::toString(pkey), StringUtil::toString(skey), curTs);
            break;
        }
        default: // add skey 80%
        {
            ss << "cmd=add,pkey=" << pkey << ",skey=" << skey << ",value=" << curTs << ",ts=" << curTs * 1000000
               << ",locator=" << curTs << ";";
            resultInfo.AddSkey(StringUtil::toString(pkey), StringUtil::toString(skey), curTs, curTs);
        }
        }
    }
    return ss.str();
}

void KKVTableTest::CheckResult(PartitionStateMachine& psm, const ExpectResultInfos& resultInfo, int32_t curTs,
                               int64_t ttl, bool isOptimize)
{
    const ExpectResultInfos::PKeySKeyInfoMap& infoMap = resultInfo.GetInfoMap();
    ExpectResultInfos::PKeySKeyInfoMap::const_iterator iter = infoMap.begin();
    for (; iter != infoMap.end(); iter++) {
        const string& pkey = iter->first;
        const ExpectResultInfos::SKeyInfoVec& skeyInfos = iter->second;

        stringstream ssForTs;
        stringstream ss;
        for (size_t i = 0; i < skeyInfos.size(); i++) {
            ss << "skey=" << skeyInfos[i].skey << ","
               << "value=" << skeyInfos[i].value << ","
               << "ts=" << skeyInfos[i].ts * 1000000 << ";";

            if (ttl == INVALID_TTL || skeyInfos[i].ts >= (curTs - ttl)) {
                ssForTs << "skey=" << skeyInfos[i].skey << ","
                        << "value=" << skeyInfos[i].value << ","
                        << "ts=" << skeyInfos[i].ts * 1000000 << ";";
            }
        }

        if (isOptimize || ttl == INVALID_TTL) {
            // normal check
            bool ret = psm.Transfer(QUERY, "", pkey, ss.str());
            if (!ret) {
                cerr << "optimize:" << isOptimize << ", ttl:" << ttl << ", pkey:" << pkey << ", ss:" << ss.str()
                     << endl;
            }
            ASSERT_TRUE(ret);
        }

        if (ttl != INVALID_TTL) {
            // ttl check
            string pkeyWithTs = pkey + "#" + StringUtil::toString(curTs);
            ASSERT_TRUE(psm.Transfer(QUERY, "", pkeyWithTs, ssForTs.str()))
                << "pkeyWithTs: " << pkeyWithTs << ", ssForTs: " << ssForTs.str();
        }
    }
}

void KKVTableTest::BuildDoc(uint32_t docCount)
{
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString;
    for (size_t i = 0; i < docCount; ++i) {
        string docIdStr = StringUtil::toString(i);
        docString += "cmd=add,pkey=pkey" + docIdStr + ",skey=1,value=1,ts=101;";
    }
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docString, "", ""));
}

void KKVTableTest::CheckLevelInfo(const vector<LevelMetaInfo>& levelInfos)
{
    Version version;
    VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
    indexlibv2::framework::LevelInfo info = version.GetLevelInfo();

    ASSERT_EQ(levelInfos.size(), info.GetLevelCount());
    for (size_t i = 0; i < levelInfos.size(); ++i) {
        ASSERT_EQ(levelInfos[i].first, info.levelMetas[i].cursor);
        ASSERT_EQ(levelInfos[i].second, info.levelMetas[i].segments);
    }
}

string KKVTableTest::GetCompressType() const
{
    size_t idx = random() % 4;
    switch (idx) {
    case 0:
        return string("lz4");
    case 1:
        return string("lz4hc");
    case 2:
        return string("zstd");
    case 3:
        return string("zlib");
    default:
        assert(false);
    }
    return string("");
}

string KKVTableTest::GetLoadConfigJsonStr() const
{
    string mmapStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["_KKV_PKEY_"], "load_strategy" : "mmap" },
         { "file_patterns" : ["_KKV_SKEY_"], "load_strategy" : "mmap" },
         { "file_patterns" : ["_KKV_VALUE_"], "load_strategy" : "mmap"}]
    })";

    string cacheStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["_KKV_PKEY_"], "load_strategy" : "mmap" },
         { "file_patterns" : ["_KKV_SKEY_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : false }},
         { "file_patterns" : ["_KKV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";

    string cacheDecompressStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["_KKV_PKEY_"], "load_strategy" : "mmap" },
         { "file_patterns" : ["_KKV_SKEY_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : false, "cache_decompress_file" : true }},
         { "file_patterns" : ["_KKV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true, "cache_decompress_file" : true }}]
    })";

    string testName = GET_TEST_NAME();
    size_t idx = random() % 3;
    switch (idx) {
    case 0: {
        cout << "## [" << testName << "] mmap load" << endl;
        return mmapStr;
    }
    case 1: {
        cout << "## [" << testName << "] cache load" << endl;
        return cacheStr;
    }
    case 2: {
        cout << "## [" << testName << "] cache load decompress file" << endl;
        return cacheDecompressStr;
    }
    default:
        assert(false);
    }
    return string("");
}

void KKVTableTest::TestFlushRealtimeIndexWithRemainRtIndex()
{
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = false;
    mOptions.GetBuildConfig(true).maxDocCount = 2;

    DirectoryPtr partDir = GET_CHECK_DIRECTORY(false);
    {
        PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);

        ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
        string rtDocString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=100000000;"
                             "cmd=add,pkey=pkey1,skey=2,value=2,ts=200000000;"
                             "cmd=add,pkey=pkey1,skey=3,value=3,ts=300000000;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1", "skey=1;skey=2;skey=3"));
    }

    {
        ASSERT_TRUE(partDir->IsExist("rt_index_partition/segment_1073741824_level_0"));
        ASSERT_TRUE(partDir->IsExist("rt_index_partition/segment_1073741825_level_0"));

        // test rt directory recover logic : to be delete by recover
        partDir->MakeDirectory("rt_index_partition/segment_1073741826_level_0");
        partDir->Store("rt_index_partition/segment_1073741826_level_0/deploy_index", "123");

        PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);
        ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1", "skey=1;skey=2;skey=3;"));

        string rtDocString = "cmd=add,pkey=pkey1,skey=4,value=4,ts=400000000;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1", "skey=1;skey=2;skey=3;skey=4"));
    }

    {
        ASSERT_TRUE(partDir->IsExist("rt_index_partition/segment_1073741824_level_0"));
        ASSERT_TRUE(partDir->IsExist("rt_index_partition/segment_1073741825_level_0"));
        ASSERT_FALSE(partDir->IsExist("rt_index_partition/segment_1073741826_level_0"));

        PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);
        // offline build
        ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
        string incDocString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=100000000;"
                              "cmd=add,pkey=pkey1,skey=2,value=2,ts=250000000;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "", ""));
    }

    {
        PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);
        ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1", "skey=1;skey=2;skey=3;skey=4"));

        // test online reclaim rt segment
        ASSERT_FALSE(partDir->IsExist("rt_index_partition/segment_1073741824_level_0"));
        ASSERT_TRUE(partDir->IsExist("rt_index_partition/segment_1073741825_level_0"));
        ASSERT_FALSE(partDir->IsExist("rt_index_partition/segment_1073741826_level_0"));
    }
}

void KKVTableTest::TestBuildWithProtectionThreshold()
{
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    KKVIndexFieldInfo suffixInfo = kkvConfig->GetSuffixFieldInfo();
    suffixInfo.protectionThreshold = 5;
    kkvConfig->SetSuffixFieldInfo(suffixInfo);

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    string rtDocString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=100000000;"
                         "cmd=add,pkey=pkey1,skey=2,value=2,ts=200000000;"
                         "cmd=add,pkey=pkey1,skey=3,value=3,ts=300000000;"
                         "cmd=add,pkey=pkey1,skey=4,value=4,ts=400000000;"
                         "cmd=add,pkey=pkey1,skey=5,value=5,ts=500000000;"
                         "cmd=add,pkey=pkey1,skey=6,value=6,ts=600000000;"
                         "cmd=add,pkey=pkey1,skey=7,value=7,ts=700000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1", "skey=1;skey=2;skey=3;skey=4;skey=5"));
}

void KKVTableTest::TestBuildWithKeepSortSequence()
{
    string fullDocs = "cmd=add,pkey=1,skey=1,value=10,ts=10000000;"
                      "cmd=add,pkey=1,skey=2,value=5,ts=20000000;"
                      "cmd=add,pkey=1,skey=3,value=12,ts=30000000;";

    {
        // sort by ts
        OnlinePartitionPtr onlinePart = MakeSortKKVPartition("-$TIME_STAMP", fullDocs);
        KKVReaderPtr kkvReader = onlinePart->GetReader()->GetKKVReader();
        ASSERT_FALSE(kkvReader->GetSortParams().empty());

        CheckKKVReader(kkvReader, "1", "", "3,2,1");
        CheckKKVReader(kkvReader, "1", "1", "1");
        CheckKKVReader(kkvReader, "1", "2,3", "3,2");
    }

    {
        // sort by skey
        OnlinePartitionPtr onlinePart = MakeSortKKVPartition("-skey", fullDocs);
        KKVReaderPtr kkvReader = onlinePart->GetReader()->GetKKVReader();
        ASSERT_FALSE(kkvReader->GetSortParams().empty());

        CheckKKVReader(kkvReader, "1", "", "3,2,1");
        CheckKKVReader(kkvReader, "1", "1", "1");
        CheckKKVReader(kkvReader, "1", "2,3", "3,2");
    }

    {
        // sort by value
        OnlinePartitionPtr onlinePart = MakeSortKKVPartition("+value", fullDocs);
        KKVReaderPtr kkvReader = onlinePart->GetReader()->GetKKVReader();
        ASSERT_FALSE(kkvReader->GetSortParams().empty());

        CheckKKVReader(kkvReader, "1", "", "2,1,3");
        CheckKKVReader(kkvReader, "1", "1", "1");
        CheckKKVReader(kkvReader, "1", "2,3", "2,3");
    }
}

void KKVTableTest::TestForceDumpBySKeyFull()
{
    string field = "pkey:string;skey:int32;value:string;";
    mSchema = CreateSchema(field, "pkey", "skey", "value;skey;");
    ASSERT_TRUE(mSchema.get());

    mOptions = IndexPartitionOptions();
    mOptions.GetBuildConfig(false).buildTotalMemory = 20;
    mOptions.GetBuildConfig(false).keepVersionCount = 10;
    mOptions.GetBuildConfig(false).shardingColumnNum = 2;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));

    stringstream ss;
    ss << "cmd=delete,pkey=pkey0,skey=0,ts=1;";
    vector<DocumentPtr> docs = CreateKKVDocuments(mSchema, ss.str());

    util::QuotaControlPtr memoryQuotaControl(new util::QuotaControl(20 * 1024 * 1024));
    IndexBuilder builder(GET_TEMP_DATA_PATH(), mOptions, mSchema, memoryQuotaControl,
                         BuilderBranchHinter::Option::Test());
    builder.Init();
    for (size_t i = 0; i < 20 * 1024; i++) {
        builder.Build(docs[0]);
    }
    builder.EndIndex();
}

void KKVTableTest::TestProhibitInMemDumpBug_13765192()
{
    MultiPathDataFlushController::GetInstance()->InitFromString("quota_size=128,quota_interval=500,flush_unit=128");

    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().useSwapMmapFile = false;
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
    mOptions.GetOnlineConfig().prohibitInMemDump = true;
    mDumpSegmentContainer.reset(new DumpSegmentContainer);

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), mDumpSegmentContainer);
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    stringstream ss;
    for (size_t i = 0; i < 100; i++) {
        ss << "cmd=add,pkey=pkey" << i << ",skey=" << i + 1 << ",value=" << i + 2 << ",ts=1000;";
    }

    string rtDocStr = ss.str();
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocStr, "pkey7", "skey=8,value=9"));
    while (true) {
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey7", "skey=8,value=9"));
        auto partReader = psm.GetIndexPartition()->GetReader();
        if (partReader->GetSwitchRtSegments().size() > 0) {
            break;
        }
    }
}

void KKVTableTest::TestForceDumpByValueFull()
{
    string field = "pkey:string;skey:int32;value:string;";
    mSchema = CreateSchema(field, "pkey", "skey", "value;skey;");
    ASSERT_TRUE(mSchema.get());

    mOptions = IndexPartitionOptions();
    mOptions.GetBuildConfig(false).buildTotalMemory = 30; // 30MB
    mOptions.GetBuildConfig(true).buildTotalMemory = 20;  // 20MB
    mOptions.GetBuildConfig(false).keepVersionCount = 10;
    mOptions.GetBuildConfig(true).keepVersionCount = 10;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));

    string valueStr(1024 * 1024 * 8, 'a');
    string valueStr2(1024 * 1024 * 14, 'b');
    string valueStr3(MAX_KKV_VALUE_LEN, 'c'); // test doc over length

    stringstream ss;
    ss << "cmd=add,pkey=pkey0,skey=0,value=" << valueStr << ",ts=1;"
       << "cmd=add,pkey=pkey0,skey=1,value=" << valueStr2 << ",ts=1;"
       << "cmd=add,pkey=pkey0,skey=2,value=" << valueStr3 << ",ts=1;";
    string docString = ss.str();
    string expectValue = "skey=0,value=" + valueStr +
                         ";"
                         "skey=1,value=" +
                         valueStr2;
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pkey0", expectValue));

    stringstream ss1;
    ss1 << "cmd=add,pkey=pkey1,skey=1,value=" << valueStr << ",ts=101;";
    string docString1 = ss1.str();
    string expectValue1 = "skey=1,value=" + valueStr;
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString1, "pkey1", expectValue1));

    stringstream ss2;
    ss2 << "cmd=add,pkey=pkey2,skey=1,value=" << valueStr2 << ",ts=101;";
    string docString2 = ss2.str();
    string expectValue2 = "skey=1,value=" + valueStr2;
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString2, "pkey2", expectValue2));

    stringstream ss3;
    ss3 << "cmd=add,pkey=pkey3,skey=1,value=" << valueStr3 << ",ts=101;";
    string docString3 = ss3.str();
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString3, "pkey3", ""));
}

OnlinePartitionPtr KKVTableTest::MakeSortKKVPartition(const string& sortFieldInfoStr, string& docStr)
{
    tearDown();
    setUp();

    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KKVIndexFieldInfo suffixFieldInfo = kkvConfig->GetSuffixFieldInfo();
    suffixFieldInfo.sortParams = StringToSortParams(sortFieldInfoStr);
    suffixFieldInfo.enableKeepSortSequence = true;
    kkvConfig->SetSuffixFieldInfo(suffixFieldInfo);

    IndexPartitionOptions options = mOptions;
    options.GetBuildConfig(false).shardingColumnNum = 2;
    options.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    options.GetBuildConfig(false).levelNum = 2;

    PartitionStateMachine psm;
    psm.Init(mSchema, options, GET_TEMP_DATA_PATH());
    psm.Transfer(BUILD_FULL, docStr, "", "");

    OnlinePartitionPtr partition(new OnlinePartition);
    partition->Open(GET_TEMP_DATA_PATH(), "", mSchema, IndexPartitionOptions());
    return partition;
}

void KKVTableTest::CheckKKVReader(const KKVReaderPtr& kkvReader, const string& pkey, const string& skeyInfos,
                                  const string& expectResult)
{
    StringView pkeyStr(pkey);
    vector<string> skeyVec;
    StringUtil::fromString(skeyInfos, skeyVec, ",");

    Pool pool;
    KKVIterator* iter = NULL;
    if (skeyVec.empty()) {
        iter = future_lite::interface::syncAwait(kkvReader->LookupAsync(pkeyStr, 0, tsc_default, &pool));
    } else {
        vector<StringView> skeyStrVec;
        for (size_t i = 0; i < skeyVec.size(); i++) {
            skeyStrVec.push_back(StringView(skeyVec[i]));
        }
        iter = future_lite::interface::syncAwait(kkvReader->LookupAsync(pkeyStr, skeyStrVec, 0, tsc_default, &pool));
    }
    ASSERT_TRUE(iter);

    vector<uint64_t> resultInfos;
    StringUtil::fromString(expectResult, resultInfos, ",");
    for (size_t i = 0; i < resultInfos.size(); i++) {
        ASSERT_TRUE(iter->IsValid());
        ASSERT_EQ(resultInfos[i], iter->GetCurrentSkey());
        iter->MoveToNext();
    }
    ASSERT_FALSE(iter->IsValid());
    POOL_COMPATIBLE_DELETE_CLASS(&pool, iter);
}

IndexPartitionSchemaPtr KKVTableTest::CreateSchema(const string& fields, const string& pkey, const string& skey,
                                                   const string& values)
{
    return SchemaMaker::MakeKKVSchema(fields, pkey, skey, values, mImpactValue);
}

vector<DocumentPtr> KKVTableTest::CreateKKVDocuments(const IndexPartitionSchemaPtr& schema, const string& docStrs)
{
    vector<DocumentPtr> ret;
    auto docs = DocumentCreator::CreateKVDocuments(schema, docStrs);
    for (auto doc : docs) {
        ret.push_back(doc);
    }
    return ret;
}
}} // namespace indexlib::partition
