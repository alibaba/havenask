#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>
#include "indexlib/partition/test/online_partition_intetest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/slow_dump_segment_container.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/config/virtual_attribute_config_creator.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/impl/primary_key_index_config_impl.h"
#include "indexlib/config/date_index_config.h"
#include "indexlib/config/impl/date_index_config_impl.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/partition/memory_stat_reporter.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_iterator_typed.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/package_file_mount_table.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/util/env_util.h"
#include "indexlib/util/fp16_encoder.h"
#include <error.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnlinePartitionInteTest);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(OnlinePartitionInteTestMode, 
                                     OnlinePartitionInteTest,
                                     testing::Values(
                                             std::tr1::tuple<bool, bool>(true, true),
                                             std::tr1::tuple<bool, bool>(false, false)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestOpenFullIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestLoadIndexWithMultiThreadLoadPatch);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestSubSchemaLoadIndexWithMultiThreadLoadPatch);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestOpenFullIndexWithBitmapIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestOpenSpecificIncIndex);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtAddDocWithBuildingSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtAddDocWithBuiltAndBuildingSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtAddDocHavingSamePkWithIncDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtAddDocWithSamePkInBuildingSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtAddDocWithSamePkInBuiltSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtAddDocWithDocByTsFiltered);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtAddDocWithDocByTsFilteredByIncLocator);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestAddIncDocTsBetweenTwoRtDocsWithSamePk);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtRemoveRtDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtRemoveIncDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtRemoveArrivingIncDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtRemoveCurrentAndArrivingIncDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtRemoveWithDocByTsFiltered);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtRemoveRepeatedly);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtUpdateDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtUpdateRtDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtUpdateIncDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtUpdateArrivingIncDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtUpdateCurrentAndArrivingIncDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtUpdateWithDocByTsFiltered);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtUpdateWithUnupdatableFieldDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtUpdateDocWithoutAttributeSchema);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndexWithIOException);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenSpecificIncIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndexMultiTimes);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndexWithRtNotReclaimed);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndexWithRtAllReclaimed);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndexWithRtPartlyReclaimed);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndexWithSamePkRtDocsNotReclaimed);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndexWithSamePkRtDocsPartlyReclaimed);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndexWithRtReclaimedBySameDocAndSameTs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndexWithRtReclaimedTwice);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndexWhenRtIndexTimestampEqualToInc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndexWithRtUpdateInc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncTwiceWithNoRtAfterFirstInc);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncWithAddAddDelCombo);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncWithAddDelAddCombo);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndexWithTrim);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenIncIndexWithMultiNewVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenWithRollbackVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenExecutorMemControl);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestPartitionInfoWithOpenFullIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestPartitionInfoWithRtIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestPartitionInfoWithRtReopen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestPartitionInfoWithIncReopen);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestPartitionMetrics);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestPartitionMemoryMetrics);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReaderContainerMetrics);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestAddVirtualAttributes);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestCleanVirtualAttributes);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestForceReOpenCleanVirtualAttributes);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRtBuildFromEmptyVersionAndIncReopen);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestCleanReadersAndInMemoryIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestCleanOnDiskIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestCleanOnDiskIndexMultiPath);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestCleanOnDiskIndexSecondaryPath);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestCleanOnDiskIndexWithVersionHeldByReader);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRecycleUpdateVarAttributeMemory);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestAutoRecycleUpdateVarAttributeMem);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRecycleSliceWithReaderAccess);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestUpdateCompressValueLongCaseTest);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestOptimizedReopenForIncNewerThanRtLongCaseTest);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestOptimizedReopenForAllModifiedOnDiskDocUnchanged);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestOptimizedReopenForAllModifiedOnDiskDocChanged);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestOptimizedReopenForAllModifiedRtDocBeenCovered);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestOptimizedReopenForAllModifiedRtDocNotBeenCovered);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestGetDocIdAfterUpdateDocumentLongCaseTest);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestBuildRtDocsFilteredByIncTimestampLongCaseTest);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestIncCoverRtWithoutPk);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestOptimizedReopenForDeleteDocLongCaseTest);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestCombinedPkSegmentCleanSliceFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestEnableOpenLoadSpeedLimit); 
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestPreloadPatchFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestForceReopenWithSpecificVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenForCompressOperationBlocks);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenForCombineHashPk);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestIncExandMaxReopenMemUse);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestSegmentDirNameCompatibility);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenForNumberHashPk);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenForMurMurHashPk);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestReopenForReaderHashId);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestLatestNeedSkipIncTimestamp);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestAsyncDumpWithRead);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestAsyncDumpWithIncReopen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestBuildThreadReopenNewSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestRedoWithMultiDumpingSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestSharedRealtimeQuotaControl);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestSyncRealtimeQuota);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestIncCoverPartialRtWithVersionTsLTStopTs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestMultiThreadInitAttributeReader);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestAttributeFloatCompress);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestAttributeFloatPackCompress);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionInteTestMode, TestAttributeFloatCompressUpdateField);

OnlinePartitionInteTest::OnlinePartitionInteTest()
    : mFlushRtIndex(false)
{
}

OnlinePartitionInteTest::~OnlinePartitionInteTest()
{
}

void OnlinePartitionInteTest::CaseSetUp()
{
    mRootPath = GET_TEST_DATA_PATH();
    mRootDirectory = GET_PARTITION_DIRECTORY();
    MakeSchema(false);
    mOptions = IndexPartitionOptions();
    mOptions.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReaderParam = 
        "combine_segments=true,max_doc_count=100";
    mOptions.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReader =
        std::tr1::get<0>(GET_CASE_PARAM());

    mOptions.GetOfflineConfig().buildConfig.speedUpPrimaryKeyReaderParam = 
        "combine_segments=true,max_doc_count=100";
    mOptions.GetOfflineConfig().buildConfig.speedUpPrimaryKeyReader =
        std::tr1::get<0>(GET_CASE_PARAM());
    mOptions.SetEnablePackageFile(std::tr1::get<1>(GET_CASE_PARAM()));
    mBuildIncEvent = BUILD_INC_NO_MERGE;

    mFlushRtIndex = GET_PARAM_VALUE(0);
}

void OnlinePartitionInteTest::CaseTearDown()
{
}

void OnlinePartitionInteTest::TestIncExandMaxReopenMemUse()
{
    string jsonStr = "                                                \
    {                                                                   \
    \"load_config\" :                                                   \
    [                                                                   \
    {                                                                   \
        \"file_patterns\" : [\"_ATTRIBUTE_\"],                          \
        \"load_strategy\" : \"mmap\",                                   \
        \"load_strategy_param\" : {                                     \
            \"advise_random\" : true,                                   \
            \"lock\" : true,                                            \
            \"slice\" : 4096,                                           \
            \"interval\" : 1000                                         \
        }                                                               \
    }                                                                   \
    ]                                                                   \
    }                                                                   \
    ";
    
    IndexPartitionOptions options;
    FromJsonString(options.GetOnlineConfig().loadConfigList, jsonStr);
    options.GetOnlineConfig().SetMaxReopenMemoryUse(1);
    options.GetOnlineConfig().buildConfig.maxDocCount = 1;
    options.GetOfflineConfig().buildConfig.maxDocCount = 1;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    {
        TearDown();
        SetUp();
    
        string docString = "cmd=add,pk=1,updatable_multi_long=0;";
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootPath));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                        "pk:1", "docid=0,updatable_multi_long=0;"));
        string multiLongValue = PrepareLongMultiValue(1024 * 1024, 1);
        string updateDoc = "cmd=add,pk=2,updatable_multi_long="
            + multiLongValue + ",ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, updateDoc, "", ""));
        IndexPartitionPtr indexPartition = psm.GetIndexPartition();
        IndexPartition::OpenStatus status = indexPartition->ReOpen(false);
        ASSERT_EQ(IndexPartition::OS_FORCE_REOPEN, status);
    }

    {
        //test patch too mutch
        TearDown();
        SetUp();
        string docString = "cmd=add,pk=1,updatable_multi_long=0;";
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootPath));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                        "pk:1", "docid=0,updatable_multi_long=0;"));
        string multiLongValue = PrepareLongMultiValue(1024 * 1024, 1);
        string updateDoc = "cmd=update_field,pk=1,updatable_multi_long="
            + multiLongValue + ",ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, updateDoc, "", ""));
        IndexPartitionPtr indexPartition = psm.GetIndexPartition();
        IndexPartition::OpenStatus status = indexPartition->ReOpen(false);
        ASSERT_EQ(IndexPartition::OS_FORCE_REOPEN, status);
    }
}

void OnlinePartitionInteTest::TestRecycleSliceWithReaderAccess()
{
    string docString = "cmd=add,pk=1,updatable_multi_long=0;";
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOnlineConfig().maxCurReaderReclaimableMem = 64;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "pk:1", "docid=0,updatable_multi_long=0;"));

    // doc size : (65535 * 8 + 2)
    // the 128th doc will trigger move data
    for (size_t i = 0; i < 130; i++)
    {
        string multiLongValue = PrepareLongMultiValue(65535, i);
        string updateDoc = "cmd=update_field,pk=1,updatable_multi_long="
                           + multiLongValue + ",ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, updateDoc, "", ""));
    }

    INDEXLIB_TEST_TRUE(psm.Transfer(PE_METRICS, "", "", "ReclaimedSliceCount:0"));
    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    //keep old reader to test slice not dropped
    IndexPartitionReaderPtr oldReader = indexPartition->GetReader();

    //check current reader reclaimable bytes: 64M
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_METRICS, "", "", "CurReaderReclaimableBytes:67108864"));
    // wasted 130 docs
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_METRICS, "", "", "VarAttributeWastedBytes:68157426"));

    //dump reader replace
    psm.Transfer(PE_REOPEN, "", "", "");
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_METRICS, "", "", "CurReaderReclaimableBytes:0"));
    // wasted 2 docs
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_METRICS, "", "", "VarAttributeWastedBytes:1048566"));

    psm.Transfer(PE_REOPEN, "", "", "");
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_METRICS, "", "", "ReclaimedSliceCount:0"));
    //test old reader reset, slice release
    oldReader.reset();
    psm.Transfer(PE_REOPEN, "", "", "");
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_METRICS, "", "", "ReclaimedSliceCount:1"));
}

util::BytesAlignedSliceArrayPtr OnlinePartitionInteTest::GetSliceFileArray(
        const IndexPartitionPtr& indexPartition, const string& fileName)
{
    OnlinePartitionPtr onlinePartition = DYNAMIC_POINTER_CAST(
            OnlinePartition, indexPartition);
    IndexlibFileSystemPtr fileSystem = onlinePartition->GetFileSystem();
    SliceFileReaderPtr sliceFile = 
        fileSystem->GetSliceFile(fileName)->CreateSliceFileReader();
    return sliceFile->GetBytesAlignedSliceArray();
}

void OnlinePartitionInteTest::TestReopenForCombineHashPk()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "uint32", SFP_PK_INDEX);
    IndexConfigPtr indexConfig = provider.GetIndexConfig();
    PrimaryKeyIndexConfigPtr primaryKeyIndexConfig = 
        DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, indexConfig);
    primaryKeyIndexConfig->SetPrimaryKeyIndexType(pk_hash_table);
    provider.Build("1,2,3,4,5#6,7,8,9#10,11", SFP_OFFLINE);
    OnlinePartition onlinePartition;
    IndexPartitionOptions options;
    options.SetIsOnline(true);
    BuildConfig& buildConfig = options.GetBuildConfig();
    buildConfig.speedUpPrimaryKeyReader = true;
    buildConfig.speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=9";
    onlinePartition.Open(GET_TEST_DATA_PATH(), "", provider.GetSchema(), options);
    provider.Build("12,13,14", SFP_OFFLINE);
    ASSERT_EQ(IndexPartition::OS_OK, onlinePartition.ReOpen(false));
}

void OnlinePartitionInteTest::TestReopenForNumberHashPk()
{
    DoTestReopenIncIndexWithSamePkRtDocsNotReclaimed(pk_number_hash);
    DoTestRtUpdateCurrentAndArrivingIncDoc(pk_number_hash);
}

void OnlinePartitionInteTest::TestReopenForMurMurHashPk()
{
    DoTestReopenIncIndexWithSamePkRtDocsNotReclaimed(pk_murmur_hash);
    DoTestRtUpdateCurrentAndArrivingIncDoc(pk_murmur_hash);
}

void OnlinePartitionInteTest::TestAutoRecycleUpdateVarAttributeMem()
{
    string docString = "cmd=add,pk=1,updatable_multi_long=0;"
                       "cmd=add,pk=2,updatable_multi_long=121";
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOnlineConfig().maxCurReaderReclaimableMem = 64;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "pk:1", "docid=0,updatable_multi_long=0;"));

    //update doc 2
    string updateDoc = "cmd=update_field,pk=2,updatable_multi_long=122,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, updateDoc, "pk:2",
                                    "updatable_multi_long=122"));

    //update doc 1 trigger move
    for (size_t i = 0; i < 129; i++)
    {
        string multiLongValue = PrepareLongMultiValue(65535, i);
        string updateDoc = "cmd=update_field,pk=1,updatable_multi_long="
                           + multiLongValue + ",ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, updateDoc, "", ""));
    }
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:2", "updatable_multi_long=122"));
    sleep(3);
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_METRICS, "", "", "ReclaimedSliceCount:1"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:2", "updatable_multi_long=122"));
}

void OnlinePartitionInteTest::TestRecycleUpdateVarAttributeMemory()
{
    string docString = "cmd=add,pk=1,updatable_multi_long=0;"
                       "cmd=add,pk=2,updatable_multi_long=121";
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOnlineConfig().maxCurReaderReclaimableMem = 64;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "pk:1", "docid=0,updatable_multi_long=0;"));

    //update doc 2
    string updateDoc = "cmd=update_field,pk=2,updatable_multi_long=122,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, updateDoc, "pk:2",
                                    "updatable_multi_long=122"));

    //update doc 1 trigger move
    for (size_t i = 0; i < 129; i++)
    {
        string multiLongValue = PrepareLongMultiValue(65535, i);
        string updateDoc = "cmd=update_field,pk=1,updatable_multi_long="
                           + multiLongValue + ",ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, updateDoc, "", ""));
    }
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_METRICS, "", "", "ReclaimedSliceCount:0"));
    //doc 2 is right
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:2", "updatable_multi_long=122"));
    //reader replace
    psm.Transfer(PE_REOPEN, "", "", "");
    //check read ok
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:2", "updatable_multi_long=122"));
    //release old reader
    psm.Transfer(PE_REOPEN, "", "", "");
    //test slice recycle, doc 2 is right
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_METRICS, "", "", "ReclaimedSliceCount:1"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:2", "updatable_multi_long=122"));
}

void OnlinePartitionInteTest::TestReopenForReaderHashId()
{
    SCOPED_TRACE("Failed");
    string docString = "cmd=add,pk=1,string1=hello,long1=2,multi_long=3";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    uint64_t readerHashId = indexPart->GetReaderHashId();
    ASSERT_NE((uint64_t)0, readerHashId);

    string rtDocs = "cmd=add,pk=1,string1=hello,long1=2,multi_long=3,ts=2;"
                    "cmd=add,pk=1,string1=hello,long1=2,multi_long=3,ts=3";
    
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
    ASSERT_NE((uint64_t)0, indexPart->GetReaderHashId());
    ASSERT_NE(readerHashId, indexPart->GetReaderHashId());
    readerHashId = indexPart->GetReaderHashId();

    string incDocs = "cmd=add,pk=1,string1=hello,long1=2,multi_long=3,ts=2";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));
    ASSERT_NE((uint64_t)0, indexPart->GetReaderHashId());
    ASSERT_NE(readerHashId, indexPart->GetReaderHashId());
}


void OnlinePartitionInteTest::TestAttributeFloatCompress()
{
    // fp16
    {
        string field = "pkey:string;nid:uint64;score:float:false:false:fp16";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, 
            "pk:primarykey64:pkey",
            "score", "");
        
        string docString = "cmd=add,pkey=pkey1,nid=1,score=1.1;"
                           "cmd=add,pkey=pkey2,nid=2,score=2.1;"
                           "cmd=add,pkey=pkey3,nid=3,score=0;";
        
        string rtDoc = "cmd=add,pkey=pkey5,nid=1,score=4.2;"
                       "cmd=update_field,pkey=pkey1,nid=1,score=11.4;";

        PartitionStateMachine psm;
        string rootPath = GET_TEST_DATA_PATH() + "/fp16";
        INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, rootPath));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:pkey1", "score=1.09961"));
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:pkey2", "score=2.09961"));
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:pkey3", "score=0"));

        EXPECT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:pkey1", "score=11.3984"));
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:pkey5", "score=4.19922"));

        EXPECT_TRUE(psm.Transfer(BUILD_INC, "cmd=update_field,pkey=pkey3,nid=3,score=-1.1", "", ""));
        EXPECT_TRUE(psm.Transfer(BUILD_INC, "cmd=add, pkey=pkey4,nid=3,score=-1.1", "", ""));
        EXPECT_TRUE(psm.Transfer(QUERY, "","pk:pkey3", "score=-1.09961"));
        EXPECT_TRUE(psm.Transfer(QUERY, "","pk:pkey4", "score=-1.09961"));

        const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
        autil::mem_pool::Pool pool;
        const AttributeReaderPtr attrReader = indexPart->GetReader()->GetAttributeReader("score");
        AttributeIteratorTyped<float>* attrIter = 
            static_cast<AttributeIteratorTyped<float>*>(attrReader->CreateIterator(&pool));
        float value;
        attrIter->Seek(0, value);
        ASSERT_FLOAT_EQ(11.398438, value);
        const string segDir = "segment_5_level_0";
        const file_system::DirectoryPtr& segmentDir =
            indexPart->GetRootDirectory()->GetDirectory(segDir, true);
        ASSERT_EQ(8, segmentDir->GetFileLength("attribute/score/data"));
    }

    // int8
    {
        string field = "pkey:string;nid:uint64;score:float:false:false:int8#3";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, 
                "pk:primarykey64:pkey",
                "score", "");
     
        string docString = "cmd=add,pkey=pkey1,nid=1,score=1.1;"
                           "cmd=add,pkey=pkey2,nid=2,score=2.1;"
                           "cmd=add,pkey=pkey3,nid=3";

        string rtDoc = "cmd=update_field,pkey=pkey3,nid=3,score=1.1;"
                       "cmd=add,pkey=pkey5,nid=1,score=4.2;";

        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEST_DATA_PATH() + "/int8"));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pkey1", "score=1.11024"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pkey2", "score=2.10236"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pkey3", "score=0"));

        // rt build
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "","pk:pkey3", "score=1.11024"));
        ASSERT_TRUE(psm.Transfer(QUERY, "","pk:pkey5", "score=3"));

        // ASSERT_TRUE(psm.Transfer(BUILD_INC, "cmd=update_field,pkey=pkey3,nid=3,score=-1.1", "", ""));
        // ASSERT_TRUE(psm.Transfer(QUERY, "","pk:pkey3", "score=-1.11024"));

        const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
        autil::mem_pool::Pool pool;
        const AttributeReaderPtr attrReader = indexPart->GetReader()->GetAttributeReader("score");
        AttributeIteratorTyped<float>* attrIter = 
            static_cast<AttributeIteratorTyped<float>*>(attrReader->CreateIterator(&pool));
        float value;
        attrIter->Seek(0, value);
        ASSERT_FLOAT_EQ(1.1102362, value);
        
        const string segDir = "segment_1_level_0";
        const file_system::DirectoryPtr& segmentDir =
            indexPart->GetRootDirectory()->GetDirectory(segDir, true);
        ASSERT_EQ(3, segmentDir->GetFileLength("attribute/score/data"));
    }
}

void OnlinePartitionInteTest::TestAttributeFloatPackCompress()
{
    // fp16 + pack field
    {
        string field = "pkey:string;nid:float:false:true:fp16;score:float:false:true:fp16;";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, 
                "pk:primarykey64:pkey",
                "packAttr1:score,nid", "");
        
        string docString = "cmd=add,pkey=pkey1,nid=1.1,score=1.1;"
                           "cmd=add,pkey=pkey2,nid=2,score=2.1;"
                           "cmd=add,pkey=pkey3,nid=3;";
        
        string rtDoc = "cmd=update_field,pkey=pkey1,nid=1,score=11.4;"
                       "cmd=add,pkey=pkey5,nid=1.1,score=4.2;";
        
        PartitionStateMachine psm;
        string rootPath = GET_TEST_DATA_PATH() + "/fp16pack";
        INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, rootPath));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:pkey1", "score=1.09961,nid=1.09961"));
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:pkey2", "score=2.09961"));
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:pkey3", "score=0,nid=3"));

        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, "cmd=update_field,pkey=pkey3,nid=3,score=-1.1", "", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, "cmd=add, pkey=pkey4,nid=3,score=-1.1", "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pkey1", "score=11.3984"));
        ASSERT_TRUE(psm.Transfer(QUERY, "","pk:pkey3", "score=-1.09961"));
        ASSERT_TRUE(psm.Transfer(QUERY, "","pk:pkey4", "score=-1.09961"));
        ASSERT_TRUE(psm.Transfer(QUERY, "","pk:pkey5", "score=4.19922,nid=1.09961"));

        const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
        autil::mem_pool::Pool pool;
        const PackAttributeReaderPtr attrReader = indexPart->GetReader()->GetPackAttributeReader("packAttr1");
        PackAttributeIteratorTyped<float>* attrIter = 
            static_cast<PackAttributeIteratorTyped<float>*>(attrReader->CreateIterator(&pool, "score"));
        float value;
        attrIter->Seek(0, value);
        ASSERT_FLOAT_EQ(11.398438, value);
    }
    
    // int8 + packField
    {
        string field = "pkey:string;nid:float;score:float:false:false:int8#3";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, 
                "pk:primarykey64:pkey",
                "packAttr1:nid,score", "");
     
        string docString = "cmd=add,pkey=pkey1,nid=1,score=1.1;"
                           "cmd=add,pkey=pkey2,nid=2,score=2.1;"
                           "cmd=add,pkey=pkey3,nid=3,score=0;";

        string rtDoc = "cmd=update_field,pkey=pkey1,nid=1,score=11.4;"
                       "cmd=add,pkey=pkey5,nid=1,score=4.2;";

        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, mOptions, GET_TEST_DATA_PATH() + "/int8pack"));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pkey1", "score=1.11024"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pkey2", "score=2.10236"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pkey3", "score=0"));

        // rt build
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, "cmd=update_field,pkey=pkey3,nid=3,score=-1.1", "", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, "cmd=add, pkey=pkey4,nid=4,score=-10", "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pkey1", "score=3"));
        ASSERT_TRUE(psm.Transfer(QUERY, "","pk:pkey3", "score=-1.11024"));
        ASSERT_TRUE(psm.Transfer(QUERY, "","pk:pkey4", "score=-3"));
        ASSERT_TRUE(psm.Transfer(QUERY, "","pk:pkey5", "score=3"));

        const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
        autil::mem_pool::Pool pool;
        const PackAttributeReaderPtr attrReader = indexPart->GetReader()->GetPackAttributeReader("packAttr1");
        PackAttributeIteratorTyped<float>* attrIter = 
            static_cast<PackAttributeIteratorTyped<float>*>(attrReader->CreateIterator(&pool, "score"));
        float value;
        attrIter->Seek(0, value);
        ASSERT_FLOAT_EQ(3, value);
    }
}

void OnlinePartitionInteTest::TestAttributeFloatCompressUpdateField()
{
    // fp16
    {
        string field = "pkey:string;nid:uint64;score:float:false:false:fp16";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, 
            "pk:primarykey64:pkey",
            "score", "");
        
        string docString = "cmd=add,pkey=pkey1,nid=1,score=1.1;"
                           "cmd=add,pkey=pkey2,nid=2,score=2.1;"
                           "cmd=add,pkey=pkey3,nid=3,score=0;";
        
        string rtDoc = "cmd=add,pkey=pkey5,nid=1,score=4.2;"
                       "cmd=update_field,pkey=pkey1,nid=1,score=11.4;";

        PartitionStateMachine psm;
        string rootPath = GET_TEST_DATA_PATH() + "/fp16";
        INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, rootPath));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
        const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
        autil::mem_pool::Pool pool;
        auto attrReader = DYNAMIC_POINTER_CAST(FloatAttributeReader, indexPart->GetReader()->GetAttributeReader("score"));
        auto buildingAttr = attrReader->GetBuildingAttributeReader();
        float updateScore = 1.4;
        char* output = new char[2];
        util::Fp16Encoder::Encode(updateScore, output);
        ASSERT_TRUE(buildingAttr->UpdateField(3, (uint8_t*)output, 2));
        float value;
        ASSERT_TRUE(attrReader->Read(3, value));
        ASSERT_FLOAT_EQ(1.3994141, value);
    }
}

string OnlinePartitionInteTest::PrepareLongMultiValue(size_t numOfValue, size_t value)
{
    stringstream updateData;
    for (size_t i = 0; i < numOfValue; i++)
    {
        updateData << value << "";
    }
    return updateData.str();
}

void OnlinePartitionInteTest::TestOpenFullIndex()
{
    SCOPED_TRACE("Failed");
    string docString = "cmd=add,pk=1,string1=hello,long1=2,multi_long=3,"
                       "updatable_multi_long=5";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "index1:hello", "docid=0,long1=2,multi_long=3,"
                                    "updatable_multi_long=5,string1=hello;"));

    string docString2 = "cmd=add,pk=2,text1=t1 t2";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString2, "pack1:t1", 
                                    "docid=1,pos=0;"));
}

void OnlinePartitionInteTest::TestLoadIndexWithMultiThreadLoadPatch()
{
    SCOPED_TRACE("Failed");
    string field = "pk:uint64:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint64:true;long2:uint32;long3:uint32;"
                   "updatable_multi_long:uint64:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;packAttr1:long2,long3";
    string summary = "string1;";
    auto schema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);
    
    IndexPartitionOptions options = mOptions;
    options.GetOnlineConfig().loadPatchThreadNum = 3;
    options.GetOnlineConfig().onlineKeepVersionCount = 100;
    options.GetBuildConfig(true).keepVersionCount = 100;
    options.GetBuildConfig(false).keepVersionCount = 100;


    string fullDocs = "cmd=add,pk=1,string1=hello,long1=1,multi_long=1 2,updatable_multi_long=1 2 3,long2=1,long3=1;"
        "cmd=add,pk=2,string1=hello2,long1=2,multi_long=2 2,updatable_multi_long=2 2 3,long2=2,long3=2;";
    
    string incDocs1 =  "cmd=add,pk=3,string1=hello3,long1=3,multi_long=3 2,updatable_multi_long=3 2 3,long2=3,long3=3;"
        "cmd=add,pk=4,string1=hello4,long1=4,multi_long=4 2,updatable_multi_long=4 2 3,long2=4,long3=4;";

    string incDocs2 = "cmd=add,pk=5,string1=hello5,long1=5,multi_long=5 2,updatable_multi_long=5 2 3,long2=5,long3=5;";

    string incDocs3 = "cmd=update_field,pk=1,long1=31,updatable_multi_long=31 2 3,long2=31;"
        "cmd=update_field,pk=2,long1=32,updatable_multi_long=32 2 3,long3=22;"
        "cmd=update_field,pk=4,updatable_multi_long=34 2 3,long2=34,long3=34;";

    string incDocs4 = "cmd=update_field,pk=3,long1=43,updatable_multi_long=43 2 3,long3=43;"
        "cmd=update_field,pk=2,long1=42;";

    string incDocs5 = "cmd=update_field,pk=5,long1=55,updatable_multi_long=55 2 3,long2=55;"
        "cmd=update_field,pk=4,long1=54,long3=54;"; 

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootPath));

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_REOPEN, fullDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs1, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs2, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs3, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs4, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs5, "", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "string1=hello,long1=31,multi_long=1 2,updatable_multi_long=31 2 3,long2=31,long3=1;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "string1=hello2,long1=42,multi_long=2 2,updatable_multi_long=32 2 3,long2=2,long3=22;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", "string1=hello3,long1=43,multi_long=3 2,updatable_multi_long=43 2 3,long2=3,long3=43;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "string1=hello4,long1=54,multi_long=4 2,updatable_multi_long=34 2 3,long2=34,long3=54;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:5", "string1=hello5,long1=55,multi_long=5 2,updatable_multi_long=55 2 3,long2=55,long3=5;"));    
}

void OnlinePartitionInteTest::TestSubSchemaLoadIndexWithMultiThreadLoadPatch()
{
    string field = "pk:string;long1:int32;long2:int32;multi_int:int32:true:true;";
    string index = "pk:primarykey64:pk";
    string attr = "long1;packAttr1:long2,multi_int";
    string summary = "";
    auto schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            "sub_pk:string;sub_int:int32;sub_multi_int:int32:true:true",
            "sub_pk:primarykey64:sub_pk",
            "sub_pk;sub_int;sub_multi_int;",
            "");
    schema->SetSubIndexPartitionSchema(subSchema);
    IndexPartitionOptions options = mOptions;
    options.GetOnlineConfig().loadPatchThreadNum = 4;
    options.GetOnlineConfig().onlineKeepVersionCount = 100;
    options.GetBuildConfig(true).keepVersionCount = 100;
    options.GetBuildConfig(false).keepVersionCount = 100;

    string fullDocs = "cmd=add,pk=pk1,long1=1,multi_int=1 2 3,long2=1,"
        "sub_pk=sub11^sub12,sub_int=1^2,sub_multi_int=11 12^12 13;"
        "cmd=add,pk=pk2,long1=2,multi_int=2 2 3,long2=2,"
        "sub_pk=sub21^sub22,sub_int=2^3,sub_multi_int=21 22^22 23;" ;
    
    string incDocs1 =  "cmd=add,pk=pk3,long1=3,multi_int=3 2 3,long2=3,"
        "sub_pk=sub31^sub32,sub_int=3^4,sub_multi_int=31 32^32 33;"
        "cmd=add,pk=pk4,long1=4,multi_int=4 2 3,long2=4,"
        "sub_pk=sub41^sub42,sub_int=4^5,sub_multi_int=41 42^42 43;";

    string incDocs2 = "cmd=add,pk=pk5,long1=5,multi_int=5 2 3,long2=5,"
        "sub_pk=sub51^sub52,sub_int=5^6,sub_multi_int=51 52^52 53;"; 

    string incDocs3 = "cmd=update_field,pk=pk1,long1=31,multi_int=31 2 3,long2=31,"
        "sub_pk=sub11,sub_int=31,sub_multi_int=311 312;"
        "cmd=update_field,pk=pk2,long1=32,multi_int=32 2 3,"
        "sub_pk=sub22,sub_int=33,sub_multi_int=322 323;" 
        "cmd=update_field,pk=pk4,multi_int=34 2 3,long2=34,"
        "sub_pk=sub41^sub42,sub_int=34^35;";

    string incDocs4 = "cmd=update_field,pk=pk3,long1=43,multi_int=43 2 3,"
        "sub_pk=sub32,sub_int=42,sub_multi_int=432 433;"
        "cmd=update_field,pk=pk2,long1=42,"
        "sub_pk=sub21,sub_int=42,sub_multi_int=422 423;"; 

    string incDocs5 = "cmd=update_field,pk=pk5,long1=55,multi_int=55 2 3,long2=55,"
        "sub_pk=sub52,sub_int=552,sub_multi_int=522 553;"        
        "cmd=update_field,pk=pk4,long1=54,"
        "sub_pk=sub41,sub_int=534,sub_multi_int=534 535;";

    string incDocs6 = "cmd=delete,pk=pk5;cmd=delete_sub,pk=pk4,sub_pk=sub41;";
    
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootPath));
    
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_REOPEN, fullDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs1, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs2, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs3, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs4, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs5, "", "")); 
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs6, "", "")); 

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk1", "long1=31,multi_int=31 2 3,long2=31;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk2", "long1=42,multi_int=32 2 3,long2=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk3", "long1=43,multi_int=43 2 3,long2=3;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk4", "long1=54,multi_int=34 2 3,long2=34;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk5", "")); // deleted doc

    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub11", "sub_int=31,sub_multi_int=311 312;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub12", "sub_int=2,sub_multi_int=12 13;")); 
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub21", "sub_int=42,sub_multi_int=422 423;")); 
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub22", "sub_int=33,sub_multi_int=322 323;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub31", "sub_int=3,sub_multi_int=31 32;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub32", "sub_int=42,sub_multi_int=432 433;"));    
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub41", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub42", "sub_int=35,sub_multi_int=42 43;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub51", "")); // died with main pk5
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub52", "")); // died with main pk5
}

void OnlinePartitionInteTest::TestAsyncDumpWithRead()
{
    SCOPED_TRACE("Failed");
    string docString = "cmd=add,pk=1,string1=hello,long1=2,multi_long=3";

    SlowDumpSegmentContainerPtr dumpContainer(new SlowDumpSegmentContainer(1000));
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false,
                              partition::IndexPartitionResource(), dumpContainer);
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, 
                                    "index1:hello", "docid=0,long1=2,multi_long=3"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pk=2,string1=hello1,long1=3,multi_long=4,ts=3",
                                    "index1:hello1", "docid=1,long1=3,multi_long=4"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pk=3,string1=hello2,long1=4,multi_long=5,ts=4",
                                    "index1:hello2", "docid=2,long1=4,multi_long=5"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "",
                                    "index1:hello1", "docid=1,long1=3,multi_long=4"));
    ASSERT_EQ(2u, dumpContainer->GetDumpItemSize());
    dumpContainer->WaitEmpty();
    ASSERT_EQ(0u, dumpContainer->GetDumpItemSize());
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "",
                                    "index1:hello1", "docid=1,long1=3,multi_long=4"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "",
                                    "index1:hello2", "docid=2,long1=4,multi_long=5"));
    
}

void OnlinePartitionInteTest::TestOpenFullIndexWithBitmapIndex()
{
    SCOPED_TRACE("Failed");
    string field = "string1:string;";
    string index = "index1:string:string1;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    INDEXLIB_TEST_TRUE(schema);

    DictionaryConfigPtr dictConfig = schema->AddDictionaryConfig("dict", "hello");
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index1");
    indexConfig->SetDictConfig(dictConfig);

    string docString = "cmd=add,string1=hello;";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello", 
                                    "docid=0;"));
}

void OnlinePartitionInteTest::TestOpenSpecificIncIndex()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    string docString = "cmd=add,pk=1,string1=hello,long1=1,multi_long=10,"
                       "updatable_multi_long=100,ts=1";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    string incDocString1 = "cmd=add,pk=1,string1=hello,long1=1,multi_long=10,"
                           "updatable_multi_long=100,ts=10";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString1, "", ""));
    string incDocString2 = "cmd=add,pk=1,string1=hello,long1=1,multi_long=10,"
                           "updatable_multi_long=100,ts=100";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString2, "", ""));

    IndexPartitionOptions options;
    OnlinePartition part1;
    part1.Open(GET_TEST_DATA_PATH(), "", mSchema, options, 1);
    ASSERT_EQ((int64_t)10, part1.GetRtSeekTimestampFromIncVersion());

    OnlinePartition part2;
    part2.Open(GET_TEST_DATA_PATH(), "", mSchema, options, 2);
    ASSERT_EQ((int64_t)100, part2.GetRtSeekTimestampFromIncVersion());

    // test partition open after close and reset
    OnlinePartition reusePart;
    reusePart.Open(GET_TEST_DATA_PATH(), "", mSchema, options, 2);
    ASSERT_EQ((int64_t)100, reusePart.GetRtSeekTimestampFromIncVersion());
    reusePart.Close();
    reusePart.Reset();
    reusePart.Open(GET_TEST_DATA_PATH(), "", mSchema, options, 2);
    ASSERT_EQ((int64_t)100, reusePart.GetRtSeekTimestampFromIncVersion()); 
}

void OnlinePartitionInteTest::TestRtAddDocWithBuildingSegment()
{
    SCOPED_TRACE("TestRtAddDocWithBuildingSegment Failed");
    string docString = "cmd=add,pk=1,string1=hello,long1=1,multi_long=10,"
                       "updatable_multi_long=100,ts=1";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString,
                                    "index1:hello", "docid=0,long1=1,multi_long=10,"
                                    "updatable_multi_long=100,string1=hello;"));
    string rtDocString = "cmd=add,pk=2,string1=hello,long1=2,multi_long=20,"
                          "updatable_multi_long=200,ts=2";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "index1:hello",
                                    "docid=0,long1=1,multi_long=10,updatable_multi_long=100,string1=hello;"
                                    "docid=1,long1=2,multi_long=20,updatable_multi_long=200,string1=hello;"));
}

void OnlinePartitionInteTest::TestRtAddDocWithBuiltAndBuildingSegment()
{
    SCOPED_TRACE("TestRtAddDocWithBuiltAndBuildingSegment Failed");

    IndexPartitionOptions options;
    options.GetOnlineConfig().buildConfig.maxDocCount = 2;
    options.GetOfflineConfig().buildConfig.maxDocCount = 2;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootPath));

    string fullDocs = "cmd=add,pk=1,string1=hello,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                    "pk:1", "docid=0;"));

    string rtDocs = "cmd=add,pk=2,string1=hello,ts=2;";
    //test building
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "pk:2", "docid=1;"));
    //test built
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "pk:2", "docid=2;"));
    //test built and building
    rtDocs = "cmd=add,pk=3,string1=hello,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;docid=2;docid=3;"));
}

void OnlinePartitionInteTest::TestRtAddDocHavingSamePkWithIncDoc()
{
    SCOPED_TRACE("TestRtAddDocHavingSamePkWithIncDoc Failed");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

    string rtDocs = "cmd=add,pk=1,string1=hello,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;"));

    // when incDocs arrive, rt seg0's docs are all deleted, 
    // and it is trimmed because the rt version has two segments(
    // one data, one del data to rt).
    string incDocs = "cmd=add,pk=1,string1=hello,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                    "index1:hello", "docid=0;"));

    rtDocs = "cmd=add,pk=1,string1=hello,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=1;"));
}

void OnlinePartitionInteTest::TestRtAddDocWithSamePkInBuildingSegment()
{
    SCOPED_TRACE("TestRtAddDocWithSamePkInBuildingSegment Failed");

    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetOnlineConfig().buildConfig.maxDocCount = 10;
    options.GetOfflineConfig().buildConfig.maxDocCount = 10;

    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

    string rtDocs = "cmd=add,pk=1,string1=hello,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;"));

    rtDocs = "cmd=add,pk=1,string1=hello,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=1;"));

    rtDocs = "cmd=add,pk=1,string1=hello,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=2;"));
}

void OnlinePartitionInteTest::TestRtAddDocWithSamePkInBuiltSegment()
{
    SCOPED_TRACE("TestRtAddDocWithSamePkInBuildingSegment Failed");

    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetOnlineConfig().buildConfig.maxDocCount = 1;
    options.GetOfflineConfig().buildConfig.maxDocCount = 1;

    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

    string rtDocs = "cmd=add,pk=1,string1=hello,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;"));

    rtDocs = "cmd=add,pk=1,string1=hello,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=1;"));

    rtDocs = "cmd=add,pk=1,string1=hello,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=2;"));
}

void OnlinePartitionInteTest::TestRtAddDocWithDocByTsFiltered()
{
    SCOPED_TRACE("TestRtAddDocWithDocByTsFiltered Failed");
    {
        TearDown();
        SetUp();        
        EnvGuard env("VERSION_FORMAT_NUM", "2");
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

        string fullDocs = "cmd=add,pk=1,string1=hello,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                        "pk:1", "docid=0;"));


        string rtDocs = "cmd=add,pk=2,string1=hello,ts=1;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", "docid=0;"));

        rtDocs = "cmd=add,pk=2,string1=hello,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", "docid=0;docid=1;"));

        rtDocs = "cmd=add,pk=2,string1=hello,ts=3;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", "docid=0;docid=2;"));        
    }
    {
        TearDown();
        SetUp();        
        EnvGuard env("VERSION_FORMAT_NUM", "1");
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

        string fullDocs = "cmd=add,pk=1,string1=hello,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                        "pk:1", "docid=0;"));


        string rtDocs = "cmd=add,pk=2,string1=hello,ts=1;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", "docid=0;"));

        rtDocs = "cmd=add,pk=2,string1=hello,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", "docid=0;"));

        rtDocs = "cmd=add,pk=2,string1=hello,ts=3;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", "docid=0;docid=1;")); 
    }    

}

void OnlinePartitionInteTest::TestRtAddDocWithDocByTsFilteredByIncLocator()
{
    /*
    SCOPED_TRACE("TestRtAddDocWithDocByTsFilteredByIncLocator Failed");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    string fullDocs = "cmd=add,pk=1,string1=hello,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                    "pk:1", "docid=0;"));

    string incDocs = "cmd=add,pk=2,string1=hello,ts=1,locator=0:4";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs,
                                    "index1:hello", "docid=0;docid=1;"));
    
    string rtDocs = "cmd=add,pk=3,string1=hello,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;docid=1;"));

    rtDocs = "cmd=add,pk=3,string1=hello,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;docid=1;"));

    rtDocs = "cmd=add,pk=3,string1=hello,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;docid=1;"));

    rtDocs = "cmd=add,pk=3,string1=hello,ts=4;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;docid=1;docid=2;"));
    */
}

// kelude #5395815
void OnlinePartitionInteTest::TestAddIncDocTsBetweenTwoRtDocsWithSamePk()
{
    SCOPED_TRACE("TestAddIncDocTsBetweenTwoRtDocsWithSamePk Failed");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

    string rtDocs = "cmd=add,pk=1,string1=hello,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;"));

    rtDocs = "cmd=add,pk=1,string1=hello,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=1;"));

    string incDocs = "cmd=add,pk=1,string1=hello,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                    "index1:hello", "docid=2;"));
}


void OnlinePartitionInteTest::TestRtRemoveRtDoc()
{
    SCOPED_TRACE("Failed");

    IndexPartitionOptions options;
    options.GetOnlineConfig().buildConfig.maxDocCount = 3;
    options.GetOfflineConfig().buildConfig.maxDocCount = 3;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = mFlushRtIndex;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootPath));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

    string rtDocs = "cmd=add,pk=3,string1=hello,ts=4;"
                    "cmd=add,pk=4,string1=hello,ts=5;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;docid=1"));

    // delete doc in building  segment
    rtDocs = "cmd=delete,pk=3,ts=6;"; 
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=1;"));

    // the following 'add' will trigger dumping one segment
    rtDocs = "cmd=add,pk=5,string1=hello,ts=6;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=1;docid=2;"));

    // delete doc in built segment
    rtDocs = "cmd=delete,pk=4,ts=6;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=2;"));
    rtDocs = "cmd=add,pk=6,string1=hello,ts=6;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs,
                                    "index1:hello", "docid=2;docid=3"));

    // trigger switch reader for flush rt
    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    DirectoryPtr rootDir = indexPart->GetRootDirectory();
    IndexlibFileSystemPtr fileSystem = rootDir->GetFileSystem();
    fileSystem->Sync(true);
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", "docid=2;docid=3;"));
}

void OnlinePartitionInteTest::TestRtRemoveIncDoc()
{
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = mFlushRtIndex;
    SCOPED_TRACE("Failed");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    string fullDocs = "cmd=add,pk=1,string1=hello,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                    "index1:hello", "docid=0;"));

    string incDocs = "cmd=add,pk=2,string1=hello,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                    "index1:hello", "docid=0;docid=1;"));

    string rtDocs = "cmd=delete,pk=2,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;"));

    rtDocs = "cmd=add,pk=3,string1=hello1,ts=4;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs,
                                    "index1:hello", "docid=0;"));

    // trigger switch reader for flush rt
    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    DirectoryPtr rootDir = indexPart->GetRootDirectory();
    IndexlibFileSystemPtr fileSystem = rootDir->GetFileSystem();
    fileSystem->Sync(true);
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", "docid=0;"));
}

void OnlinePartitionInteTest::TestRtRemoveArrivingIncDoc()
{
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = mFlushRtIndex;
    SCOPED_TRACE("Failed");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    string fullDocs = "cmd=add,pk=1,string1=hello,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                    "index1:hello", "docid=0;"));

    string rtDocs = "cmd=delete,pk=2,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;"));

    string incDocs = "cmd=add,pk=2,string1=hello,ts=2;";
    ASSERT_FALSE(psm.Transfer(mBuildIncEvent, incDocs,
                              "index1:hello", "docid=0;"));

    // trigger switch reader for flush rt
    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    DirectoryPtr rootDir = indexPart->GetRootDirectory();
    IndexlibFileSystemPtr fileSystem = rootDir->GetFileSystem();
    fileSystem->Sync(true);
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    ASSERT_FALSE(psm.Transfer(QUERY, "", "index1:hello", "docid=0;"));
}

void OnlinePartitionInteTest::TestRtRemoveCurrentAndArrivingIncDoc()
{
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = mFlushRtIndex;
    SCOPED_TRACE("Failed");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    string fullDocs = "cmd=add,pk=1,string1=hello,ts=1;"
                      "cmd=add,pk=2,string1=hello,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                    "index1:hello", "docid=0;docid=1;"));

    string rtDocs = "cmd=delete,pk=2,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;"));

    string incDocs = "cmd=add,pk=2,string1=hello,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                    "index1:hello", "docid=0;"));

    // trigger switch reader for flush rt
    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    DirectoryPtr rootDir = indexPart->GetRootDirectory();
    IndexlibFileSystemPtr fileSystem = rootDir->GetFileSystem();
    fileSystem->Sync(true);
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", "docid=0;"));
}

void OnlinePartitionInteTest::TestRtRemoveWithDocByTsFiltered()
{
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = mFlushRtIndex;
    SCOPED_TRACE("Failed");
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "2");
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

        string fullDocs = "cmd=add,pk=1,string1=hello,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                        "pk:1", "docid=0;"));

        string rtDocs = "cmd=delete,pk=1,ts=1;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", "docid=0;"));

        rtDocs = "cmd=delete,pk=1,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", ""));

        rtDocs = "cmd=add,pk=2,string1=hello1,ts=4;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", ""));
        // trigger switch reader for flush rt
        IndexPartitionPtr indexPart = psm.GetIndexPartition();
        DirectoryPtr rootDir = indexPart->GetRootDirectory();
        IndexlibFileSystemPtr fileSystem = rootDir->GetFileSystem();
        fileSystem->Sync(true);
        INDEXLIB_TEST_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", ""));        
    }
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "1");
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

        string fullDocs = "cmd=add,pk=1,string1=hello,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                        "pk:1", "docid=0;"));

        string rtDocs = "cmd=delete,pk=1,ts=1;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", "docid=0;"));

        rtDocs = "cmd=delete,pk=1,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", "docid=0;"));

        rtDocs = "cmd=delete,pk=1,ts=3;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", ""));

        rtDocs = "cmd=add,pk=2,string1=hello1,ts=4;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", ""));
        // trigger switch reader for flush rt
        IndexPartitionPtr indexPart = psm.GetIndexPartition();
        DirectoryPtr rootDir = indexPart->GetRootDirectory();
        IndexlibFileSystemPtr fileSystem = rootDir->GetFileSystem();
        fileSystem->Sync(true);
        INDEXLIB_TEST_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", ""));        
    }    

}

void OnlinePartitionInteTest::TestRtRemoveRepeatedly()
{
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = mFlushRtIndex;
    SCOPED_TRACE("Failed");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    string fullDocs = "cmd=add,pk=1,string1=hello,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                    "pk:1", "docid=0;"));

    string rtDocs = "cmd=delete,pk=1,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", ""));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", ""));

    rtDocs = "cmd=add,pk=2,string1=hello1,ts=4;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", ""));
    // trigger switch reader for flush rt
    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    DirectoryPtr rootDir = indexPart->GetRootDirectory();
    IndexlibFileSystemPtr fileSystem = rootDir->GetFileSystem();
    fileSystem->Sync(true);
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", ""));
}

void OnlinePartitionInteTest::TestRtUpdateDocument()
{
    SCOPED_TRACE("Failed");
    string field = "string1:string;string2:string:false:true;"
                   "price:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "string2;price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    IndexPartitionOptions options;
    options.GetOnlineConfig().buildConfig.maxDocCount = 2;
    options.GetOfflineConfig().buildConfig.maxDocCount = 2;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootPath));

    string fullDocs =         
        "cmd=add,string1=hello1,string2=hello,ts=1;"
        "cmd=add,string1=hello2,string2=hello,ts=2;"
        "cmd=update_field,string1=hello2,string2=hello2,price=2,ts=3;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "pk:hello2",
                                    "docid=1,price=2,string2=hello2"));

    string rtDocs = "cmd=update_field,string1=hello1,string2=hello1,price=1,ts=2;"  // filter by ts
                    "cmd=update_field,string1=hello3,string2=hello3,ts=4;"
                    "cmd=update_field,string1=hello2,string2=hello4,ts=4;";
 
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "pk:hello1", "docid=0,price=0,string2=hello"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "",
                                    "pk:hello2", "docid=1,price=2,string2=hello4"));

}

void OnlinePartitionInteTest::TestRtUpdateRtDoc()
{
    SCOPED_TRACE("Failed");

    IndexPartitionOptions options;
    options.GetOnlineConfig().buildConfig.maxDocCount = 2;
    options.GetOfflineConfig().buildConfig.maxDocCount = 2;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

    string rtDocs = "cmd=add,pk=1,long1=1,updatable_multi_long=1 10,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "pk:1", 
                                    "docid=0,long1=1,updatable_multi_long=1 10"));

    // update doc in building segment
    rtDocs = "cmd=update_field,pk=1,long1=2,updatable_multi_long=2 20,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "pk:1",
                                    "docid=0,long1=2,updatable_multi_long=2 20"));

    rtDocs = "cmd=add,pk=3,long1=3,updatable_multi_long=3 30,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs, "", ""));

    // update doc in built segment
    rtDocs = "cmd=update_field,pk=1,long1=4,updatable_multi_long=4 40,ts=4;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "pk:1",
                                    "docid=0,long1=4,updatable_multi_long=4 40"));
}

void OnlinePartitionInteTest::TestRtUpdateIncDoc()
{
    SCOPED_TRACE("Failed");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

    string incDocs = "cmd=add,pk=1,long1=1,updatable_multi_long=1 10,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                    "pk:1", 
                                    "docid=0,long1=1,updatable_multi_long=1 10"));

    string rtDocs = "cmd=update_field,pk=1,long1=3,updatable_multi_long=3 30,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "pk:1",
                                    "docid=0,long1=3,updatable_multi_long=3 30"));

    incDocs = "cmd=update_field,pk=1,long1=2,updatable_multi_long=2 20,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                    "pk:1", 
                                    "docid=0,long1=3,updatable_multi_long=3 30"));

    incDocs = "cmd=update_field,pk=1,long1=4,updatable_multi_long=4 40,ts=4;";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                    "pk:1", 
                                    "docid=0,long1=4,updatable_multi_long=4 40"));
}

void OnlinePartitionInteTest::TestRtUpdateArrivingIncDoc()
{
    SCOPED_TRACE("Failed");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

    string rtDocs = "cmd=update_field,pk=1,long1=3,updatable_multi_long=3 30,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "pk:1",
                                    ""));

    string incDocs = "cmd=add,pk=1,long1=2,updatable_multi_long=2 20,ts=2;";
    ASSERT_FALSE(psm.Transfer(mBuildIncEvent, incDocs,
                              "pk:1", 
                              "docid=0,long1=3,updatable_multi_long=3 30"));
}

void OnlinePartitionInteTest::TestRtUpdateCurrentAndArrivingIncDoc()
{
    DoTestRtUpdateCurrentAndArrivingIncDoc(pk_default_hash);
}

void OnlinePartitionInteTest::TestRtUpdateWithDocByTsFiltered()
{
    SCOPED_TRACE("Failed");
    {
        EnvGuard env("VERSION_FORMAT_NUM", "1");
        TearDown();
        SetUp();
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

        string fullDocs = "cmd=add,pk=1,long1=2,updatable_multi_long=2 20,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                        "pk:1",
                                        "docid=0,long1=2,updatable_multi_long=2 20"));

        string rtDocs = "cmd=update_field,pk=1,long1=1,updatable_multi_long=1 10,ts=1;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "pk:1",
                                        "docid=0,long1=2,updatable_multi_long=2 20"));

        rtDocs = "cmd=update_field,pk=1,long1=3,updatable_multi_long=3 30,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "pk:1",
                                        "docid=0,long1=2,updatable_multi_long=2 20"));

        rtDocs = "cmd=update_field,pk=1,long1=4,updatable_multi_long=4 40,ts=4;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "pk:1",
                                        "docid=0,long1=4,updatable_multi_long=4 40"));        
    }
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "2");
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

        string fullDocs = "cmd=add,pk=1,long1=2,updatable_multi_long=2 20,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                        "pk:1",
                                        "docid=0,long1=2,updatable_multi_long=2 20"));

        string rtDocs = "cmd=update_field,pk=1,long1=1,updatable_multi_long=1 10,ts=1;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "pk:1",
                                        "docid=0,long1=2,updatable_multi_long=2 20"));

        rtDocs = "cmd=update_field,pk=1,long1=3,updatable_multi_long=3 30,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "pk:1",
                                        "docid=0,long1=3,updatable_multi_long=3 30"));

        rtDocs = "cmd=update_field,pk=1,long1=4,updatable_multi_long=4 40,ts=4;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "pk:1",
                                        "docid=0,long1=4,updatable_multi_long=4 40"));        
    }    

}

void OnlinePartitionInteTest::TestRtUpdateWithUnupdatableFieldDoc()
{
    SCOPED_TRACE("Failed");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    string fullDocs = "cmd=add,pk=1,long1=2,multi_long=2 20,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                    "pk:1",
                                    "docid=0,long1=2,multi_long=2 20"));

    string rtDocs = "cmd=update_field,pk=1,long1=3,multi_long=3 30,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "pk:1",
                                    "docid=0,long1=3,multi_long=2 20"));
}

void OnlinePartitionInteTest::TestRtUpdateDocWithoutAttributeSchema()
{
    string field = "pk:string:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint64:true;"
                   "updatable_multi_long:uint64:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootPath));

    string fullDocs = "cmd=add,pk=1,string1=hello,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                    "index1:hello", "docid=0"));

    string rtDocs = "cmd=update_field,pk=1,ts=2;"
                    "cmd=add,pk=2,string1=hello,ts=3;"
                    "cmd=update_field,pk=2,ts=4;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:hello", "docid=0;docid=1"));

}

void OnlinePartitionInteTest::TestReopenIncIndex()
{
    SCOPED_TRACE("TestReopenIncIndex Failed");
    string docString = "cmd=add,pk=1,string1=hello,long1=1,multi_long=10,"
                       "updatable_multi_long=100";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello", 
                                    "docid=0,long1=1,multi_long=10,updatable_multi_long=100,string1=hello;"));
    string incDocString = "cmd=add,pk=2,string1=hello,long1=2,multi_long=20,"
                          "updatable_multi_long=200";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocString, "index1:hello", 
                                    "docid=0,long1=1,multi_long=10,updatable_multi_long=100,string1=hello;"
                                    "docid=1,long1=2,multi_long=20,updatable_multi_long=200,string1=hello;"));
}

void OnlinePartitionInteTest::TestReopenIncIndexWithIOException()
{
    SCOPED_TRACE("TestReopenIncIndex Failed");
    string docString = "cmd=add,pk=1,string1=hello,long1=1,multi_long=10,"
                       "updatable_multi_long=100";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello", 
                                    "docid=0,long1=1,multi_long=10,updatable_multi_long=100,string1=hello;"));
    string incDocString = "cmd=add,pk=2,string1=hello,long1=2,multi_long=20,"
                          "updatable_multi_long=200";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDocString, "", ""));
    string dataPath = "";
    if (!mOptions.GetMergeConfig().GetEnablePackageFile())
    {
        dataPath = PathUtil::JoinPath(mRootPath, "segment_2_level_0/summary/data");
    }
    else
    {
        mRootDirectory->MountPackageFile(
            PathUtil::JoinPath("segment_2_level_0/", "package_file"));
        ASSERT_TRUE(mRootDirectory->IsExist("segment_2_level_0/summary/data"));
        PackageFileMeta packageMeta;
        packageMeta.Load(PathUtil::JoinPath(mRootPath, "segment_2_level_0"));
        dataPath = PathUtil::JoinPath(
            mRootPath, "segment_2_level_0", packageMeta.GetPhysicalFileName(0u));
    }
    ASSERT_TRUE(FileSystemWrapper::IsExist(dataPath));
    ASSERT_EQ(0, chmod(dataPath.c_str(), S_IXGRP));
    ASSERT_FALSE(psm.GetIndexPartition()->mNeedReload);
    ASSERT_FALSE(psm.Transfer(PE_REOPEN, "", "index1:hello", 
                                    "docid=0,long1=1,multi_long=10,updatable_multi_long=100,string1=hello;"
                                    "docid=1,long1=2,multi_long=20,updatable_multi_long=200,string1=hello;"));
    ASSERT_TRUE(psm.GetIndexPartition()->mNeedReload);
    string rtDocString = "cmd=add,pk=299,string1=hello,long1=2,multi_long=20,"
                          "updatable_multi_long=200";
    ASSERT_TRUE(psm.GetIndexPartition()->mNeedReload);
    ASSERT_FALSE(psm.Transfer(BUILD_RT, rtDocString, "", ""));

    ASSERT_EQ(0, chmod(dataPath.c_str(), S_IRUSR|S_IRGRP|S_IROTH));    
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "index1:hello", 
                                    "docid=0,long1=1,multi_long=10,updatable_multi_long=100,string1=hello;"
                                    "docid=1,long1=2,multi_long=20,updatable_multi_long=200,string1=hello;"));
    ASSERT_TRUE(psm.GetIndexPartition()->mNeedReload); // need force reload
    ASSERT_FALSE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
}

void OnlinePartitionInteTest::TestReopenSpecificIncIndex()
{
    SCOPED_TRACE("TestReopenSpecificIncIndex Failed");
    string docString = "cmd=add,pk=1,string1=hello,long1=1,multi_long=10,"
                       "updatable_multi_long=100,ts=1";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    IndexPartitionOptions options;
    OnlinePartition onlinePart;
    onlinePart.Open(GET_TEST_DATA_PATH(), "", mSchema, options);

    ASSERT_EQ((int64_t)1, onlinePart.GetRtSeekTimestampFromIncVersion());
    string incDocString1 = "cmd=add,pk=1,string1=hello,long1=1,multi_long=10,"
                           "updatable_multi_long=100,ts=10";
    string incDocString2 = "cmd=add,pk=1,string1=hello,long1=1,multi_long=10,"
                           "updatable_multi_long=100,ts=100";
    
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString1, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString2, "", ""));
    onlinePart.ReOpen(false, 1);
    ASSERT_EQ((int64_t)10, onlinePart.GetRtSeekTimestampFromIncVersion());

    onlinePart.ReOpen(false, INVALID_VERSION);
    ASSERT_EQ((int64_t)100, onlinePart.GetRtSeekTimestampFromIncVersion());
}

void OnlinePartitionInteTest::TestReopenIncIndexMultiTimes()
{
    SCOPED_TRACE("TestReopenIncIndexMultiTimes Failed");
    string docString = "cmd=add,pk=1,string1=hello,long1=1,multi_long=10,"
                       "updatable_multi_long=100";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello", 
                             "docid=0,long1=1,multi_long=10,updatable_multi_long=100,string1=hello;"));
    string incDocString = "cmd=add,pk=2,string1=hello,long1=2,multi_long=20,"
                          "updatable_multi_long=200";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocString, "index1:hello", 
                                    "docid=0,long1=1,multi_long=10,updatable_multi_long=100,string1=hello;"
                                    "docid=1,long1=2,multi_long=20,updatable_multi_long=200,string1=hello;"));
    string secondIncDocString = "cmd=add,pk=3,string1=hello,long1=3,multi_long=30,"
                                "updatable_multi_long=300";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, secondIncDocString, "index1:hello", 
                                    "docid=0,long1=1,multi_long=10,updatable_multi_long=100,string1=hello;"
                                    "docid=1,long1=2,multi_long=20,updatable_multi_long=200,string1=hello;"
                                    "docid=2,long1=3,multi_long=30,updatable_multi_long=300,string1=hello;"));
}

void OnlinePartitionInteTest::TestReopenIncIndexWithRtNotReclaimed()
{
    SCOPED_TRACE("TestReopenIncIndexWithRtNotReclaimed Failed");
    string docString = "cmd=add,pk=1,string1=hello,long1=1,multi_long=10,"
                       "updatable_multi_long=100,ts=1";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello", 
                                    "docid=0,long1=1;"));

    string rtDocString = "cmd=add,pk=2,string1=hello,long1=2,multi_long=20,"
                          "updatable_multi_long=200,ts=10";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "index1:hello", 
                                    "docid=0,long1=1;"
                                    "docid=1,long1=2;"));

    string incDocString = "cmd=add,pk=3,string1=hello,long1=3,multi_long=30,"
        "updatable_multi_long=300,ts=3";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocString, "index1:hello", 
                                    "docid=0,long1=1;"
                                    "docid=1,long1=3;"
                                    "docid=2,long1=2;"));
}

void OnlinePartitionInteTest::TestReopenIncIndexWithRtAllReclaimed()
{
    SCOPED_TRACE("TestReopenIncIndexWithRtAllReclaimed Failed");
    string docString = "cmd=add,pk=1,string1=hello,long1=1,ts=1";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello",
                                    "docid=0,long1=1;"));

    string rtDocString = "cmd=add,pk=2,string1=hello,long1=2,ts=2";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "index1:hello", 
                                    "docid=0,long1=1;"
                                    "docid=1,long1=2;"));

    string incDocString = "cmd=add,pk=3,string1=hello,long1=3,ts=3";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocString, "index1:hello",
                                    "docid=0,long1=1;"
                                    "docid=1,long1=3;"));
}

void OnlinePartitionInteTest::TestReopenIncIndexWithRtPartlyReclaimed()
{
    SCOPED_TRACE("TestReopenIncIndexWithRtPartlyReclaimed Failed");
    string docString = "cmd=add,pk=1,string1=hello,long1=1,ts=1";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello",
                                    "docid=0,long1=1;"));

    string rtDocString = "cmd=add,pk=2,string1=hello,long1=2,ts=2;"
                         "cmd=add,pk=4,string1=hello,long1=4,ts=4";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "index1:hello", 
                                    "docid=0,long1=1;"
                                    "docid=1,long1=2;"
                                    "docid=2,long1=4;"));

    string incDocString = "cmd=add,pk=3,string1=hello,long1=3,ts=3";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocString, "index1:hello",
                                    "docid=0,long1=1;"
                                    "docid=1,long1=3;"
                                    "docid=3,long1=4;"));
}

void OnlinePartitionInteTest::TestReopenIncIndexWithSamePkRtDocsNotReclaimed()
{
    DoTestReopenIncIndexWithSamePkRtDocsNotReclaimed(pk_default_hash);
}

void OnlinePartitionInteTest::TestReopenIncIndexWithSamePkRtDocsPartlyReclaimed()
{
    SCOPED_TRACE("Failed");
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "",""));

    string rtDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=1;"
                         "cmd=add,pk=1,string1=hello,long1=1,ts=3";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "index1:hello", 
                                    "docid=1,long1=1;"));

    string incDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=2";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocString, "index1:hello",
                                    "docid=2,long1=1;"));
}

void OnlinePartitionInteTest::TestReopenIncIndexWithRtReclaimedBySameDocAndSameTs()
{
    {
        TearDown();
        SetUp();
        SCOPED_TRACE("Failed");
        EnvGuard env("VERSION_FORMAT_NUM", "1");
        string docString = "cmd=add,pk=1,string1=hello,long1=1,ts=1";
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello",
                                        "docid=0,long1=1;"));

        string rtDocString = "cmd=add,pk=2,string1=hello,long1=2,ts=2;"
            "cmd=add,pk=2,string1=hello,long1=2,ts=4;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "index1:hello", 
                                        "docid=0,long1=1;docid=2,long1=2;"));

        string incDocString = "cmd=add,pk=2,string1=hello,long1=2,ts=4";
        INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocString, "index1:hello",
                                        "docid=0,long1=1;docid=1,long1=2;"));
    }
    {
        TearDown();
        SetUp();        
        SCOPED_TRACE("Failed");
        EnvGuard env("VERSION_FORMAT_NUM", "2");
        string docString = "cmd=add,pk=1,string1=hello,long1=1,ts=1";
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello",
                                        "docid=0,long1=1;"));

        string rtDocString = "cmd=add,pk=2,string1=hello,long1=2,ts=2;"
            "cmd=add,pk=2,string1=hello,long1=2,ts=4;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "index1:hello", 
                                        "docid=0,long1=1;docid=2,long1=2;"));

        string incDocString = "cmd=add,pk=2,string1=hello,long1=2,ts=4";
        INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocString, "index1:hello",
                                        "docid=0,long1=1;docid=3,long1=2;"));
    }    
}

void OnlinePartitionInteTest::TestReopenIncIndexWithRtReclaimedTwice()
{
    SCOPED_TRACE("TestReopenIncIndexWithRtReclaimedTwice Failed");
    string docString1 = "cmd=add,pk=1,string1=hello,long1=1,ts=1";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString1, "index1:hello",
                                    "long1=1;"));

    string docString2 = "cmd=add,pk=2,string1=hello,long1=2,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString2, "index1:hello", 
                                    "long1=1;long1=2;"));

    string docString3 = "cmd=add,pk=3,string1=hello,long1=3,ts=3";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, docString3, "index1:hello",
                                    "long1=1;long1=3;"));

    string docString4 = "cmd=add,pk=4,string1=hello,long1=4,ts=4;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString4, "index1:hello", 
                                    "long1=1;long1=3;long1=4;"));

    string docString5 = "cmd=add,pk=5,string1=hello,long1=5,ts=5";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, docString5, "index1:hello",
                                    "long1=1;long1=3;long1=5;"));
}

void OnlinePartitionInteTest::TestReopenIncIndexWhenRtIndexTimestampEqualToInc()
{
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "2");        
        SCOPED_TRACE("TestReopenIncIndexWhenRtIndexTimestampEqualToInc Failed");
        string docString = "cmd=add,pk=1,string1=hello,long1=1,ts=1";
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello",
                                        "docid=0,long1=1;"));

        string rtDocString = "cmd=add,pk=2,string1=hello,long1=2,ts=2;"
            "cmd=add,pk=3,string1=hello,long1=4,ts=4";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "index1:hello", 
                                        "docid=0,long1=1;"
                                        "docid=1,long1=2;"
                                        "docid=2,long1=4;"));

        string incDocString = "cmd=add,pk=4,string1=hello,long1=3,ts=4";
        INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocString, "index1:hello",
                                        "docid=0,long1=1;"
                                        "docid=1,long1=3;"
                                        "docid=3,long1=4;"));
    }
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "1");                
        SCOPED_TRACE("TestReopenIncIndexWhenRtIndexTimestampEqualToInc Failed");
        string docString = "cmd=add,pk=1,string1=hello,long1=1,ts=1";
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello",
                                        "docid=0,long1=1;"));

        string rtDocString = "cmd=add,pk=2,string1=hello,long1=2,ts=2;"
            "cmd=add,pk=3,string1=hello,long1=4,ts=4";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "index1:hello", 
                                        "docid=0,long1=1;"
                                        "docid=1,long1=2;"
                                        "docid=2,long1=4;"));

        string incDocString = "cmd=add,pk=4,string1=hello,long1=3,ts=4";
        INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocString, "index1:hello",
                                        "docid=0,long1=1;"
                                        "docid=1,long1=3;"));
    }    

}

void OnlinePartitionInteTest::TestReopenIncIndexWithRtUpdateInc()
{
    SCOPED_TRACE("TestReopenIncIndexWithRtUpdateInc Failed");

    string field = "string1:string;text1:text;long1:uint32;multi_long:uint32:true;"
                   "updatable_multi_long:uint32:true:true;";
    string index = "pack1:pack:text1;pk:primarykey64:string1";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "string1;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootPath));

    string fullDocs = "cmd=add,string1=hello,long1=10,updatable_multi_long=10,ts=1;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "pk:hello",
                                    "docid=0,long1=10,updatable_multi_long=10"));

    string rtDocs = "cmd=update_field,string1=hello,long1=20,updatable_multi_long=20,ts=3;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs, "pk:hello", 
                                    "docid=0,long1=20,updatable_multi_long=20"));

    string incDocs = "cmd=add,string1=hello,long1=30,updatable_multi_long=30,ts=2;";

    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                    "pk:hello", "docid=1,long1=20,updatable_multi_long=20"));
}

void OnlinePartitionInteTest::TestReopenIncTwiceWithNoRtAfterFirstInc()
{
    SCOPED_TRACE("TestReopenIncIndexWithRtReclaimedTwice Failed");

    string field = "pk:string;string2:string:false:true;"
                   "long1:int32";
    string index = "pk:primarykey64:pk";
    string attribute = "string2;long1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    schema->SetAutoUpdatePreference(false);

    string docString1 = "cmd=add,pk=hello,long1=1,ts=1";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString1, "pk:hello",
                                    "docid=0,long1=1;"));

    string docString3 = "cmd=update_field,pk=hello,long1=3,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString3, "pk:hello",
                                    "docid=0,long1=3;"));

    string docString2 = "cmd=add,pk=hello,long1=2,ts=2";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, docString2, "pk:hello",
                                    "docid=1,long1=3;"));

    string docString5 = "cmd=update_field,pk=hello,long1=5,ts=5";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, docString5, "pk:hello",
                                    "docid=1,long1=5;"));
}

void OnlinePartitionInteTest::TestReopenIncWithAddAddDelCombo()
{
    string docs = "cmd=add,pk=1,string1=hello,long1=1,ts=1;"
                  "cmd=add,pk=1,string1=hello,long1=2,ts=2;"
                  "cmd=delete,pk=1,ts=3";
    string expectedResults = "long1=1;long1=2;";
    string query = "index1:hello";

    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 10;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 10;
    DoTestReopenIncWithMultiDocsCombo(docs, expectedResults, query, mOptions);

    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 1;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 1;
    DoTestReopenIncWithMultiDocsCombo(docs, expectedResults, query, mOptions);
}

void OnlinePartitionInteTest::TestReopenIncWithAddDelAddCombo()
{
    string docs = "cmd=add,pk=1,string1=hello,long1=1,ts=1;"
                  "cmd=delete,pk=1,ts=2;"
                  "cmd=add,pk=1,string1=hello,long1=3,ts=3";
    string expectedResults = "long1=1;;long1=3";
    string query = "index1:hello";

    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 10;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 10;
    DoTestReopenIncWithMultiDocsCombo(docs, expectedResults, query, mOptions);

    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 1;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 1;
    DoTestReopenIncWithMultiDocsCombo(docs, expectedResults, query, mOptions);
}

void OnlinePartitionInteTest::TestReopenIncIndexWithTrim()
{
    SCOPED_TRACE("Failed");
    string field = "string1:string;string2:string";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "string1", "");
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootPath));

    string fullDocs =         
        "cmd=add,string1=hello1,string2=hello,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, fullDocs,
                                    "index2:hello", "docid=0;"));

    // it will generate rt seg0(hello2, hello3), rt seg1(hello5)
    string rtDocs = "cmd=add,string1=hello2,string2=hello,ts=2;"
                    "cmd=add,string1=hello3,string2=hello,ts=3;"
                    "cmd=add,string1=hello5,string2=hello,ts=5;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index2:hello", "docid=0;docid=1;docid=2;docid=3"));

    // when inc index arrives, rt seg 0 will be trimmed, and rt seg 1 remains
    string incDocs = "cmd=add,string1=hello4,string2=hello,ts=4";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                    "index2:hello", "docid=0,string1=hello1;"
                                    "docid=1,string1=hello4;"
                                    "docid=2,string1=hello5;"));
}

void OnlinePartitionInteTest::TestReopenIncIndexWithMultiNewVersion()
{
    string rtDoc = "cmd=add,pk=1,string1=hello,long1=1,ts=100";
    PartitionStateMachine psm;
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 1;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 1;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "index1:hello", "docid=0,long1=1"));
    string incDocString = "cmd=add,pk=2,string1=hello,long1=2,ts=2;"
                          "cmd=add,pk=3,string1=hello,long1=3,ts=3";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocString, "index1:hello", 
                                    "docid=0,long1=2;docid=1,long1=3;docid=2,long1=1"));
}

void OnlinePartitionInteTest::TestReopenWithRollbackVersion()
{
    DoTestReopenWithRollbackVersion(false);
}

void OnlinePartitionInteTest::TestReopenExecutorMemControl()
{
    SCOPED_TRACE("TestReopenIncIndex Failed");
    int64_t quota = 500 * 1024 * 1024;
    MemoryQuotaControllerPtr controller(new MemoryQuotaController(quota));
    IndexPartitionResource indexPartitionResource;
    indexPartitionResource.memoryQuotaController = controller;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, indexPartitionResource);
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "", "", ""));

    Version version = VersionMaker::Make(1, "");
    version.Store(mRootPath, false);

    int64_t quotaBeforeReopen = controller->GetFreeQuota();

    INDEXLIB_TEST_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    string docString = "cmd=add,pk=1,ts=100";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));

    int64_t quotaAfterReopen = controller->GetFreeQuota();

    ASSERT_LT(quotaAfterReopen, quotaBeforeReopen);
}

void OnlinePartitionInteTest::TestPartitionInfoWithOpenFullIndex()
{
    SCOPED_TRACE("Failed");
    string docString = "cmd=add,pk=1,string1=hello,ts=1;"
                       "cmd=add,pk=2,string1=hello,ts=1";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello",
                             "docid=0;docid=1"));

    IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    PartitionInfoPtr partInfo = reader->GetPartitionInfo();
    ASSERT_TRUE(partInfo);
    ASSERT_EQ((docid_t)2, partInfo->GetTotalDocCount());
 
    vector<docid_t> expectDocIds;
    StringUtil::fromString("0", expectDocIds, ",");
    ASSERT_EQ(expectDocIds, partInfo->GetBaseDocIds());

    DocIdRangeVector actualOrderRange;
    ASSERT_FALSE(partInfo->GetOrderedDocIdRanges(actualOrderRange));

    DocIdRange actualUnorderRange;
    ASSERT_TRUE(partInfo->GetUnorderedDocIdRange(actualUnorderRange));
    DocIdRange expectUnorderRange(0, 2);
    ASSERT_EQ(expectUnorderRange, actualUnorderRange);

    // Add rt
    string docStringRt = "cmd=add,pk=3,string1=hello,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docStringRt, "index1:hello",
                             "docid=0;docid=1;docid=2"));    
    reader = psm.GetIndexPartition()->GetReader();
    partInfo = reader->GetPartitionInfo();
    ASSERT_TRUE(partInfo);
    ASSERT_EQ((docid_t)3, partInfo->GetTotalDocCount());
 
    vector<docid_t> expectDocIdsRt;
    StringUtil::fromString("0,2", expectDocIdsRt, ",");
    ASSERT_EQ(expectDocIdsRt, partInfo->GetBaseDocIds());

    DocIdRangeVector actualOrderRangeRt;
    ASSERT_FALSE(partInfo->GetOrderedDocIdRanges(actualOrderRangeRt));

    DocIdRange actualUnorderRangeRt;
    ASSERT_TRUE(partInfo->GetUnorderedDocIdRange(actualUnorderRangeRt));
    DocIdRange expectUnorderRangeRt(0, 3);
    ASSERT_EQ(expectUnorderRangeRt, actualUnorderRangeRt);
}

void OnlinePartitionInteTest::TestPartitionInfoWithRtIndex()
{
    SCOPED_TRACE("Failed");
    string docString = "cmd=add,pk=1,string1=hello,ts=1;"
                       "cmd=add,pk=2,string1=hello,ts=2";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    string rtDocString = "cmd=add,pk=3,string1=hello,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));

    IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    PartitionInfoPtr partInfo = reader->GetPartitionInfo();
    ASSERT_TRUE(partInfo);
    ASSERT_EQ((docid_t)3, partInfo->GetTotalDocCount());
 
    vector<docid_t> expectDocIds;
    StringUtil::fromString("0,2", expectDocIds, ",");
    ASSERT_EQ(expectDocIds, partInfo->GetBaseDocIds());

    DocIdRangeVector actualOrderRange;
    ASSERT_FALSE(partInfo->GetOrderedDocIdRanges(actualOrderRange));

    DocIdRange actualUnorderRange;
    ASSERT_TRUE(partInfo->GetUnorderedDocIdRange(actualUnorderRange));
    DocIdRange expectUnorderRange(0, 3);
    ASSERT_EQ(expectUnorderRange, actualUnorderRange);
}

void OnlinePartitionInteTest::TestPartitionInfoWithRtReopen()
{
    SCOPED_TRACE("Failed");
    string docString = "cmd=add,pk=1,string1=hello,ts=1;"
                       "cmd=add,pk=2,string1=hello,ts=2";
    PartitionStateMachine psm;

    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 1;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    string rtDocString = "cmd=add,pk=3,string1=hello,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));

    IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    PartitionInfoPtr partInfo = reader->GetPartitionInfo();
    ASSERT_TRUE(partInfo);
    ASSERT_EQ((docid_t)3, partInfo->GetTotalDocCount());
 
    vector<docid_t> expectDocIds;
    StringUtil::fromString("0,2,3", expectDocIds, ",");
    ASSERT_EQ(expectDocIds, partInfo->GetBaseDocIds());

    DocIdRangeVector actualOrderRange;
    ASSERT_FALSE(partInfo->GetOrderedDocIdRanges(actualOrderRange));

    DocIdRange actualUnorderRange;
    ASSERT_TRUE(partInfo->GetUnorderedDocIdRange(actualUnorderRange));
    DocIdRange expectUnorderRange(0, 3);
    ASSERT_EQ(expectUnorderRange, actualUnorderRange);
}

void OnlinePartitionInteTest::TestPartitionInfoWithIncReopen()
{
    SCOPED_TRACE("Failed");
    string docString = "cmd=add,pk=1,string1=hello,ts=1;"
                       "cmd=add,pk=2,string1=hello,ts=2";
    PartitionStateMachine psm;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    string rtDocString = "cmd=add,pk=4,string1=hello,ts=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));

    string incDocString = "cmd=add,pk=3,string1=hello,ts=3;";
    ASSERT_TRUE(psm.Transfer(mBuildIncEvent, incDocString, "", ""));

    IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    PartitionInfoPtr partInfo = reader->GetPartitionInfo();
    ASSERT_TRUE(partInfo);
    ASSERT_EQ((docid_t)4, partInfo->GetTotalDocCount());
 
    vector<docid_t> expectDocIds;
    StringUtil::fromString("0,2,3,3,4", expectDocIds, ",");
    ASSERT_EQ(expectDocIds, partInfo->GetBaseDocIds());

    DocIdRangeVector actualOrderRange;
    ASSERT_FALSE(partInfo->GetOrderedDocIdRanges(actualOrderRange));

    DocIdRange actualUnorderRange;
    ASSERT_TRUE(partInfo->GetUnorderedDocIdRange(actualUnorderRange));
    DocIdRange expectUnorderRange(0, 4);
    ASSERT_EQ(expectUnorderRange, actualUnorderRange);
}

void OnlinePartitionInteTest::TestPartitionMetrics()
{
    SCOPED_TRACE("Failed");
    string docString = "cmd=add,pk=1,string1=hello,ts=1;"
                       "cmd=add,pk=2,string1=hello,ts=2";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    string rtDocString = "cmd=add,pk=3,string1=hello,ts=3;"
                         "cmd=delete,pk=1,ts=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "", ""));

    CheckMetricsValue(psm, "indexlib/partition/partitionDocCount", "count", 3);
    CheckMetricsValue(psm, "indexlib/partition/segmentCount", "count", 3);
    CheckMetricsValue(psm, "indexlib/partition/deletedDocCount", "count", 1);

    CheckMetricsValue(psm, "indexlib/online/partitionReaderVersionCount", "count", 3);
    CheckMetricsValue(psm, "indexlib/online/latestReaderVersionId", "count", 0);

    reader.reset();
    sleep(5);
    CheckMetricsValue(psm, "indexlib/online/partitionReaderVersionCount", "count", 1);
    // CheckMetricsValue(psm, "indexlib/online/oldestReaderVersionId", "count", 0); 
}

void OnlinePartitionInteTest::TestPartitionMemoryMetrics()
{
    SCOPED_TRACE("Failed");
    string docString = "cmd=add,pk=1,string1=hello,ts=1;"
                       "cmd=add,pk=2,string1=hello,ts=2";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(
            OnlinePartition, indexPart);
    // (true, true): value:56583456, rawValue.maxValue:56590743, delta:7287
    // (false, false): value:56583380, rawValue.maxValue:56588860, delta:5480
    CheckMetricsMaxValue(psm, "indexlib/online/partitionMemoryUse", "byte", kmonitor::GAUGE,
                         onlinePart->mResourceCalculator->GetCurrentMemoryUse());

    string rtDocString = "cmd=add,pk=3,string1=hello,ts=3;"
                         "cmd=delete,pk=1,ts=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    CheckMetricsMaxValue(psm, "indexlib/online/partitionMemoryUse", "byte", kmonitor::GAUGE,
                         onlinePart->mResourceCalculator->GetCurrentMemoryUse());
}

void OnlinePartitionInteTest::TestReaderContainerMetrics()
{
    SCOPED_TRACE("Failed");
    string docString = "cmd=add,pk=1,string1=hello,ts=1;"
                       "cmd=add,pk=2,string1=hello,ts=2";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    string rtDocString = "cmd=add,pk=3,string1=hello,ts=3;"
                         "cmd=delete,pk=1,ts=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "", ""));

    CheckMetricsValue(psm, "indexlib/partition/partitionDocCount", "count", 3);
    CheckMetricsValue(psm, "indexlib/partition/segmentCount", "count", 3);
    CheckMetricsValue(psm, "indexlib/partition/deletedDocCount", "count", 1);

    auto reader0 = psm.GetIndexPartition()->GetReader();

    string incDocString = "cmd=add,pk=4,string1=hello4,ts=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "", ""));

    auto reader1 = psm.GetIndexPartition()->GetReader();    

    CheckMetricsValue(psm, "indexlib/online/latestReaderVersionId", "count", 2);
    CheckMetricsValue(psm, "indexlib/online/oldestReaderVersionId", "count", 0);

    reader0.reset();
    incDocString = "cmd=add,pk=5,string1=hello5,ts=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "", ""));

    CheckMetricsValue(psm, "indexlib/online/latestReaderVersionId", "count", 4); 
    CheckMetricsValue(psm, "indexlib/online/oldestReaderVersionId", "count", 2); 
}

void OnlinePartitionInteTest::DoTestReopenIncWithMultiDocsCombo(
        const string& docs, const string& expectedResults,
        const std::string& query, const IndexPartitionOptions& options)
{
    vector<string> docVec = StringUtil::split(docs, ";", false);
    vector<string> expectedResultsVec = StringUtil::split(expectedResults, ";", false);
    assert(docVec.size() == expectedResultsVec.size());

    for (size_t incDocNum = 0; incDocNum <= docVec.size(); ++incDocNum)
    {
        TearDown();
        SetUp();

        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootPath));
        psm.Transfer(BUILD_FULL, "", "", "");

        size_t docCursor = 0;
        for (size_t i = 0; i < incDocNum; ++i, ++docCursor)
        {
            psm.Transfer(mBuildIncEvent, docVec[docCursor], query,
                         expectedResultsVec[docCursor]);
        }
        for (; docCursor < docVec.size(); ++docCursor)
        {
            psm.Transfer(BUILD_RT, docVec[docCursor], query,
                         expectedResultsVec[docCursor]);
        }
    }
}

void OnlinePartitionInteTest::CheckMetricsValue(
        const PartitionStateMachine& psm, const string &name,
        const string &unit, double value)
{
    misc::MetricProviderPtr provider = psm.GetMetricsProvider();
    misc::MetricPtr metric = provider->DeclareMetric(name, kmonitor::STATUS);
    ASSERT_TRUE(metric != nullptr);
    // NOTE: only support STATUS type
    EXPECT_DOUBLE_EQ(value, metric->mValue) << "metric [" << name << "]not equal";
}

void OnlinePartitionInteTest::CheckMetricsMaxValue(
        const PartitionStateMachine& psm, const string& name,
        const string& unit, kmonitor::MetricType type, double value)
{
    misc::MetricProviderPtr provider = psm.GetMetricsProvider();
    MetricPtr metric = provider->DeclareMetric(name, type);
    ASSERT_TRUE(metric != nullptr);
    ASSERT_LE(abs(value - metric->mValue), 10240)
        << "value:" << value << ", "
        << "rawValue.maxValue:" << metric->mValue;
}

void OnlinePartitionInteTest::TestAddVirtualAttributes()
{
    MakeSchema(true);    
    string docString = "cmd=add,pk=1,string1=hello,long1=0,subpkstr=sub1^sub2,substr1=s1^s2;"
                       "cmd=add,pk=2,string1=hello1,long1=1,subpkstr=sub3^sub4,substr1=s3^s4;";
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "", ""));
    OnlinePartitionPtr onlinePartition(new OnlinePartition);
    onlinePartition->Open(GET_TEST_DATA_PATH(), "", mSchema, mOptions);
    
    vector<AttributeConfigPtr> attrVec;
    vector<AttributeConfigPtr> subAttrVec;
    attrVec.push_back(VirtualAttributeConfigCreator::Create("vir_int32", ft_int32, false, "3"));
    subAttrVec.push_back(VirtualAttributeConfigCreator::Create("subvir_int32", ft_int32, false, "9"));
    onlinePartition->AddVirtualAttributes(attrVec, subAttrVec);
    CheckVirtualReader(onlinePartition, "vir_int32", 2, "3,3");
    CheckVirtualReader(onlinePartition, "subvir_int32", 4, "9,9,9,9", true);

    string rtDocStrings = "cmd=add,pk=2,string1=hello2,long1=2,ts=1,subpkstr=sub5,substr1=s5;"
                          "cmd=add,pk=3,string1=hello3,long1=3,ts=2,subpkstr=sub6^sub7,substr1=s6^s7;"
                          "cmd=add,pk=4,string1=hello4,long1=4,ts=3;";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateDocuments(mSchema, rtDocStrings);
    util::QuotaControlPtr quotaControl(new util::QuotaControl(1024 * 1024 * 1024));
    IndexBuilder builder(onlinePartition, quotaControl);
    INDEXLIB_TEST_TRUE(builder.Init());
    builder.Build(docs[0]);
    CheckVirtualReader(onlinePartition, "vir_int32", 3, "3,3,3");
    CheckVirtualReader(onlinePartition, "subvir_int32", 5, "9,9,9,9,9", true);

    // if (!hasRepeated)
    // {
    //     attrVec.clear();
    // }
    attrVec.push_back(VirtualAttributeConfigCreator::Create("vir_string", ft_string, false, "hello"));
    subAttrVec.push_back(VirtualAttributeConfigCreator::Create("subvir_string",
                    ft_string, false, "hello_hello"));
    onlinePartition->AddVirtualAttributes(attrVec, subAttrVec);
    CheckVirtualReader(onlinePartition, "vir_int32", 3, "3,3,3");
    CheckVirtualReader(onlinePartition, "vir_string", 3, "hello,hello,hello");
    CheckVirtualReader(onlinePartition, "subvir_int32", 5, "9,9,9,9,9", true);
    CheckVirtualReader(onlinePartition, "subvir_string", 5,
                       "hello_hello,hello_hello,hello_hello,hello_hello,hello_hello", true);

    builder.Build(docs[1]);    
    CheckVirtualReader(onlinePartition, "vir_int32", 4, "3,3,3,3");
    CheckVirtualReader(onlinePartition, "vir_string", 4, 
                       "hello,hello,hello,hello");
    CheckVirtualReader(onlinePartition, "subvir_int32", 7, "9,9,9,9,9,9,9", true);
    CheckVirtualReader(onlinePartition, "subvir_string", 7,
                       "hello_hello,hello_hello,hello_hello,hello_hello,hello_hello,hello_hello,hello_hello",
                       true);

    builder.Build(docs[2]);
    CheckVirtualReader(onlinePartition, "vir_int32", 5, "3,3,3,3,3");
    CheckVirtualReader(onlinePartition, "vir_string", 5,
                       "hello,hello,hello,hello,hello");
    CheckVirtualReader(onlinePartition, "subvir_int32", 7, "9,9,9,9,9,9,9", true);
    CheckVirtualReader(onlinePartition, "subvir_string", 7,
                       "hello_hello,hello_hello,hello_hello,hello_hello,hello_hello,hello_hello,hello_hello",
                       true);
}

// void OnlinePartitionInteTest::TestAddVirtualAttributes()
// {

//     InnerTestAddVirtualAttributes(true);
//     InnerTestAddVirtualAttributes(false);
// }

void OnlinePartitionInteTest::TestCleanVirtualAttributes()
{
    MakeSchema(true);
    string docString = "cmd=add,pk=1,string1=hello,long1=0,subpkstr=sub1^sub2,substr1=s1^s2;"
                       "cmd=add,pk=2,string1=hello1,long1=1,subpkstr=sub3^sub4,substr1=s3^s4;";
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 1;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 1;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "", ""));
    OnlinePartitionPtr onlinePartition(new OnlinePartition);
    onlinePartition->Open(GET_TEST_DATA_PATH(), "", mSchema, mOptions);
    
    vector<AttributeConfigPtr> attrVec;
    vector<AttributeConfigPtr> subAttrVec;
    attrVec.push_back(VirtualAttributeConfigCreator::Create("vir_int32", ft_int32, false, "3"));
    subAttrVec.push_back(VirtualAttributeConfigCreator::Create("subvir_int32", ft_int32, false, "9"));
    onlinePartition->AddVirtualAttributes(attrVec, subAttrVec);
    CheckVirtualReader(onlinePartition, "vir_int32", 2, "3,3");
    CheckVirtualReader(onlinePartition, "subvir_int32", 4, "9,9,9,9", true);

    //for merge
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, "", "", ""));

    DirectoryPtr directory = onlinePartition->GetRootDirectory();
    ASSERT_TRUE(directory->IsExist("segment_0_level_0/attribute/vir_int32"));
    ASSERT_TRUE(directory->IsExist("segment_1_level_0/attribute/vir_int32"));
    ASSERT_FALSE(directory->IsExist("segment_2_level_0/attribute/vir_int32"));
    ASSERT_TRUE(directory->IsExist("segment_0_level_0/sub_segment/attribute/subvir_int32"));
    ASSERT_TRUE(directory->IsExist("segment_1_level_0/sub_segment/attribute/subvir_int32"));
    ASSERT_FALSE(directory->IsExist("segment_2_level_0/sub_segment/attribute/subvir_int32"));

    // InMemVirtualAttributeCleaner not effect
    // because mReader holding virtual attribute
    onlinePartition->ReOpen(false);
    CheckVirtualReader(onlinePartition, "vir_int32", 2, "3,3");
    CheckVirtualReader(onlinePartition, "subvir_int32", 4, "9,9,9,9", true);
    ASSERT_TRUE(directory->IsExist("segment_0_level_0/attribute/vir_int32"));
    ASSERT_TRUE(directory->IsExist("segment_1_level_0/attribute/vir_int32"));
    ASSERT_TRUE(directory->IsExist("segment_2_level_0/attribute/vir_int32"));
    ASSERT_TRUE(directory->IsExist("segment_0_level_0/sub_segment/attribute/subvir_int32"));
    ASSERT_TRUE(directory->IsExist("segment_1_level_0/sub_segment/attribute/subvir_int32"));
    ASSERT_TRUE(directory->IsExist("segment_2_level_0/sub_segment/attribute/subvir_int32"));

    // InMemVirtualAttributeCleaner effect
    // mReader switched in last reopen
    onlinePartition->ReOpen(false);
    CheckVirtualReader(onlinePartition, "vir_int32", 2, "3,3");
    CheckVirtualReader(onlinePartition, "subvir_int32", 4, "9,9,9,9", true);
    ASSERT_FALSE(directory->IsExist("segment_0_level_0/attribute/vir_int32"));
    ASSERT_FALSE(directory->IsExist("segment_1_level_0/attribute/vir_int32"));
    ASSERT_TRUE(directory->IsExist("segment_2_level_0/attribute/vir_int32"));
    ASSERT_FALSE(directory->IsExist("segment_0_level_0/sub_segment/attribute/subvir_int32"));
    ASSERT_FALSE(directory->IsExist("segment_1_level_0/sub_segment/attribute/subvir_int32"));
    ASSERT_TRUE(directory->IsExist("segment_2_level_0/sub_segment/attribute/subvir_int32"));

}

void OnlinePartitionInteTest::TestForceReopenWithSpecificVersion()
{
    mOptions.GetOnlineConfig().onlineKeepVersionCount = 10;
    string docString = "cmd=add,pk=0,string1=hello,long1=0;"
                       "cmd=add,pk=1,string1=hello1,long1=1";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "", ""));
    OnlinePartitionPtr onlinePartition(new OnlinePartition);
    onlinePartition->Open(GET_TEST_DATA_PATH(), "", mSchema, mOptions);
    
    string incDoc = "cmd=add,pk=2,string1=hello2,long1=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    Version incVersion;
    VersionLoader::GetVersion(GET_TEST_DATA_PATH(), incVersion, INVALID_VERSION);

    Version illegalVersion(100);
    illegalVersion.AddSegment(100);
    illegalVersion.Store(GET_TEST_DATA_PATH(), false);
    ASSERT_EQ(IndexPartition::OS_OK, onlinePartition->ReOpen(true, incVersion.GetVersionId()));
    ASSERT_EQ(incVersion.GetVersionId(), onlinePartition->GetReader()->GetVersion().GetVersionId());

    //test normal reopen
    storage::FileSystemWrapper::DeleteFile(GET_TEST_DATA_PATH() + "version.100");
    incDoc = "cmd=add,pk=3,string1=hello3,long1=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    VersionLoader::GetVersion(GET_TEST_DATA_PATH(), incVersion, INVALID_VERSION);
    illegalVersion.Store(GET_TEST_DATA_PATH(), false);
    ASSERT_EQ(IndexPartition::OS_OK, onlinePartition->ReOpen(true, incVersion.GetVersionId()));
    ASSERT_EQ(incVersion.GetVersionId(), onlinePartition->GetReader()->GetVersion().GetVersionId());
}

void OnlinePartitionInteTest::TestForceReOpenCleanVirtualAttributes()
{
    string docString = "cmd=add,pk=0,string1=hello,long1=0;"
                       "cmd=add,pk=1,string1=hello1,long1=1";
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 1;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 1;
    mOptions.GetMergeConfig().mergeStrategyStr = "specific_segments";
    mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
            "merge_segments=1,2");
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "", ""));
    OnlinePartitionPtr onlinePartition(new OnlinePartition);
    onlinePartition->Open(GET_TEST_DATA_PATH(), "", mSchema, mOptions);
    
    vector<AttributeConfigPtr> attrVec;
    vector<AttributeConfigPtr> subAttrVec;
    attrVec.push_back(VirtualAttributeConfigCreator::Create("vir_int32", ft_int32, false, "3"));
    onlinePartition->AddVirtualAttributes(attrVec, subAttrVec);
    CheckVirtualReader(onlinePartition, "vir_int32", 2, "3,3");

    AttributeReaderPtr virtualAttrReader = 
        onlinePartition->GetReader()->GetAttributeReader("vir_int32");
    ASSERT_TRUE(virtualAttrReader);
    int32_t updateValue = 4; 
    virtualAttrReader->UpdateField(
            0, (uint8_t*)&updateValue, sizeof(int32_t));
    virtualAttrReader.reset();

    string incDoc = "cmd=add,pk=2,string1=hello2,long1=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));

    onlinePartition->ReOpen(true);
    CheckVirtualReader(onlinePartition, "vir_int32", 3, "4,3,3");
    DirectoryPtr directory = onlinePartition->GetRootDirectory();
    ASSERT_TRUE(directory->IsExist("segment_0_level_0/attribute/vir_int32"));

    ASSERT_FALSE(directory->IsExist("segment_1_level_0/attribute/vir_int32"));
    ASSERT_FALSE(directory->IsExist("segment_2_level_0/attribute/vir_int32"));
    ASSERT_TRUE(directory->IsExist("segment_3_level_0/attribute/vir_int32"));
}

void OnlinePartitionInteTest::TestCombinedPkSegmentCleanSliceFile()
{
    string field = "nid:int32";
    string index = "pk:primarykey64:nid";
    IndexPartitionSchemaPtr schema = 
                       SchemaMaker::MakeSchema(field, index, "", "");
    IndexPartitionOptions options = IndexPartitionOptions();
    options.GetOnlineConfig().enableAsyncCleanResource = false;
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReader = true;
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReaderParam = 
        "combine_segments=true,max_doc_count=100";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, mRootPath));

    //Test inc clean combined pk slice files
    string fullDocs = "cmd=add,nid=1,ts=1;";
    string incDocs = "cmd=add,nid=2,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "pk:1", "docid=0"));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "pk:2", "docid=1"));
    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    IndexlibFileSystemMetrics metrics = indexPartition->TEST_GetFileSystemMetrics();
    int64_t pkSliceFileLen = 
        metrics.GetLocalStorageMetrics().GetFileLength(FSFT_SLICE);
    indexPartition->ReOpen(false);
    metrics = indexPartition->TEST_GetFileSystemMetrics();
    int64_t newPkSliceFileLen = 
        metrics.GetLocalStorageMetrics().GetFileLength(FSFT_SLICE);
    ASSERT_TRUE(newPkSliceFileLen < pkSliceFileLen)
        << newPkSliceFileLen << ":" << pkSliceFileLen;
    
    //Test rt clean combined pk slice files 
    string rtDocs1 = "cmd=add,nid=3,ts=2;";
    string rtDocs2 = "cmd=add,nid=4,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs1, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs2, "pk:3", "docid=2"));

    metrics = indexPartition->TEST_GetFileSystemMetrics();
    pkSliceFileLen = metrics.GetInMemStorageMetrics().GetFileLength(FSFT_SLICE);
    indexPartition->ReOpen(false);
    metrics = indexPartition->TEST_GetFileSystemMetrics();
    newPkSliceFileLen = metrics.GetInMemStorageMetrics().GetFileLength(FSFT_SLICE);
    ASSERT_TRUE(newPkSliceFileLen < pkSliceFileLen)
        << newPkSliceFileLen << ":" << pkSliceFileLen;
}

void OnlinePartitionInteTest::CheckVirtualReader(
        const OnlinePartitionPtr& partition, 
        const string& attrName, int32_t docCount,
        const string& expectValues,
        bool isSub)
{
    IndexPartitionReaderPtr reader = partition->GetReader();
    if (isSub)
    {
        reader = reader->GetSubPartitionReader();
    }
    ASSERT_TRUE(reader);
    AttributeReaderPtr attrReader = reader->GetAttributeReader(attrName);
    ASSERT_TRUE(attrReader);

    vector<string> splitValues;
    StringUtil::fromString(expectValues, splitValues, ",");
    ASSERT_EQ((int32_t)splitValues.size(), docCount);

    for (docid_t i = 0; i < (docid_t)docCount; i++)
    {
        string value;
        attrReader->Read(i, value, NULL);
        ASSERT_EQ(splitValues[i], value);
    }
}

void OnlinePartitionInteTest::DoTestRtUpdateCurrentAndArrivingIncDoc(
        PrimaryKeyHashType hashType)
{
    TearDown();
    SetUp();

    SCOPED_TRACE("Failed");

    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    PrimaryKeyIndexConfigPtr pkConfig = 
        DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    pkConfig->SetPrimaryKeyHashType(hashType);

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

    string incDocs = "cmd=add,pk=1,long1=1,updatable_multi_long=1 10,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                    "pk:1", 
                                    "docid=0,long1=1,updatable_multi_long=1 10"));

    string rtDocs = "cmd=update_field,pk=1,long1=3,updatable_multi_long=3 30,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "pk:1",
                                    "docid=0,long1=3,updatable_multi_long=3 30"));

    incDocs = "cmd=add,pk=1,long1=2,updatable_multi_long=2 20,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                    "pk:1", 
                                    "docid=1,long1=3,updatable_multi_long=3 30"));
}

void OnlinePartitionInteTest::DoTestReopenIncIndexWithSamePkRtDocsNotReclaimed(
        PrimaryKeyHashType hashType)
{
    TearDown();
    SetUp();

    SCOPED_TRACE("Failed");
    PartitionStateMachine psm;
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    PrimaryKeyIndexConfigPtr pkConfig = 
        DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    pkConfig->SetPrimaryKeyHashType(hashType);
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "",""));

    string rtDocString = "cmd=add,pk=1,string1=hello,long1=2,ts=2";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "index1:hello", 
                                    "docid=0,long1=2;"));

    string incDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=1";
    INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocString, "index1:hello",
                                    "docid=1,long1=2;"));
}

void OnlinePartitionInteTest::DoTestReopenWithRollbackVersion(bool forceReopen)
{
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    string doc1 = "cmd=add,pk=1,string1=hello,long1=1,ts=1;";
    string doc2 = "cmd=add,pk=2,string1=hello,long1=2,ts=2;";
    string doc3 = "cmd=add,pk=3,string1=hello,long1=3,ts=3;";

    // version.0 segment[0]
    ASSERT_TRUE(psm.Transfer(mBuildIncEvent, doc1, "", ""));
    // version.1 segment[0,1]
    ASSERT_TRUE(psm.Transfer(mBuildIncEvent, doc2, "", ""));
    // version.2 segment[0,1,2]
    ASSERT_TRUE(psm.Transfer(mBuildIncEvent, doc3, "", ""));

    string rtDoc = "cmd=add,pk=4,string1=hello,long1=4,ts=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc,
                             "index1:hello", "long1=1;long1=2;long1=3;long1=4"));
    // version.3 segment[0]
    Version rollbackVersion(3, 3);
    rollbackVersion.AddSegment(0);
    rollbackVersion.Store(GET_PARTITION_DIRECTORY(), false);
    psm.GetIndexPartition()->ReOpen(forceReopen);
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "",
                             "index1:hello", "long1=1;long1=4"));
}

void OnlinePartitionInteTest::TestRtBuildFromEmptyVersionAndIncReopen()
{
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "1"); 
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

        string rtDocs = "cmd=add,pk=1,string1=hello,ts=1;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", "docid=0;"));

        string incDocs = "cmd=add,pk=1,string1=hello1,ts=1;";
        INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                        "index1:hello1", "docid=0;"));

        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", ""));

        rtDocs = "cmd=add,pk=1,string1=hello,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", "docid=1;"));
    }
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "2"); 
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

        string rtDocs = "cmd=add,pk=1,string1=hello,ts=1;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", "docid=0;"));

        string incDocs = "cmd=add,pk=1,string1=hello1,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                        "index1:hello1", "docid=0;"));

        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", ""));

        rtDocs = "cmd=add,pk=1,string1=hello,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                        "index1:hello", "docid=1;"));
        
    }    

}

void OnlinePartitionInteTest::TestCleanReadersAndInMemoryIndex()
{
    PartitionStateMachine psm;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 1;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 1;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    string rtDoc = "cmd=add,pk=1,string1=hello,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));

    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(
            OnlinePartition, indexPart);
    const IndexlibFileSystemPtr& fs = onlinePart->GetFileSystem();
    DirectoryPtr partitionDir = DirectoryCreator::Get(fs, mRootPath, true);
    DirectoryPtr rtDir = partitionDir->GetDirectory(RT_INDEX_PARTITION_DIR_NAME, true);

    rtDoc = "cmd=add,pk=2,string1=hello,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
    // reopen rt not clean reader or in memory index
    ASSERT_EQ((size_t)4, onlinePart->mReaderContainer->Size());

    string rtSeg0 = "segment_" + StringUtil::toString(
            RealtimeSegmentDirectory::ConvertToRtSegmentId(0)) + "_level_0";
    string rtSeg1 = "segment_" + StringUtil::toString(
            RealtimeSegmentDirectory::ConvertToRtSegmentId(1)) + "_level_0";
    ASSERT_TRUE(rtDir->IsExist(rtSeg0));
    ASSERT_TRUE(rtDir->IsExist(rtSeg1));
    ASSERT_TRUE(rtDir->IsExist("version.0"));
    ASSERT_TRUE(rtDir->IsExist("version.1"));

    string incDoc = "cmd=add,pk=3,string1=hello,ts=3;";
    // reopen inc clean old readers(2), and create new reader(1)
    // 3 - 2 + 1 = 2
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    ASSERT_EQ((size_t)2, onlinePart->mReaderContainer->Size());

    // no new version, clean old reader
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    ASSERT_EQ((size_t)1, onlinePart->mReaderContainer->Size());
    ASSERT_FALSE(rtDir->IsExist(rtSeg0));
    ASSERT_FALSE(rtDir->IsExist(rtSeg1));
    ASSERT_FALSE(rtDir->IsExist("version.0"));
    ASSERT_FALSE(rtDir->IsExist("version.1"));
}

void OnlinePartitionInteTest::TestCleanOnDiskIndex()
{
    PartitionStateMachine psm;
    // online keepVersionCount
    mOptions.GetOnlineConfig().onlineKeepVersionCount = 3;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    
    // offline keepVersionCount
    mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 100;
    
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    // version.0, segment_0
    string incDoc = "cmd=add,pk=1,string1=hello,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, incDoc, "", ""));

    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(
            OnlinePartition, indexPart);
    const IndexlibFileSystemPtr& fs = onlinePart->GetFileSystem();
    DirectoryPtr partitionDir = DirectoryCreator::Get(fs, mRootPath, true);

    ASSERT_TRUE(partitionDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.0"));

    // version.2, segment_2
    // total version count is 3, no index will be removed
    incDoc = "cmd=add,pk=2,string1=hello,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));

    ASSERT_TRUE(partitionDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.0"));
    ASSERT_TRUE(partitionDir->IsExist("segment_1_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.1"));
    ASSERT_TRUE(partitionDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.2"));
}

void OnlinePartitionInteTest::TestCleanOnDiskIndexMultiPath()
{
    // online keepVersionCount
    mOptions.GetOnlineConfig().onlineKeepVersionCount = 3;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    mOptions.GetOnlineConfig().needReadRemoteIndex = true;
    mOptions.GetOnlineConfig().needDeployIndex = false;
    mOptions.GetOnlineConfig().loadConfigList.SetLoadMode(LoadConfig::LoadMode::REMOTE_ONLY);

    // offline keepVersionCount
    mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 100;
    mOptions.GetBuildConfig(false).enablePackageFile = true;

    string firstPath = util::PathUtil::JoinPath(mRootPath, "first");
    string secondPath = util::PathUtil::JoinPath(mRootPath, "second");
    FileSystemWrapper::MkDir(firstPath);
    FileSystemWrapper::MkDir(secondPath);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, firstPath, "psm", secondPath));

    // version.0, segment_0
    string incDoc = "cmd=add,pk=1,string1=hello,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, incDoc, "", ""));

    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(
            OnlinePartition, indexPart);
    // const IndexliIbFileSystemPtr& fs = onlinePart->GetFileSystem();
    DirectoryPtr secondDir = DirectoryCreator::Create(secondPath);

    ASSERT_TRUE(secondDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(secondDir->IsExist("version.0"));

    // version.2, segment_2
    // total version count is 3, no index will be removed
    incDoc = "cmd=add,pk=2,string1=hello,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));

    ASSERT_TRUE(secondDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(secondDir->IsExist("version.0"));
    ASSERT_TRUE(secondDir->IsExist("segment_1_level_0"));
    ASSERT_TRUE(secondDir->IsExist("version.1"));
    ASSERT_TRUE(secondDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(secondDir->IsExist("version.2"));

    // version.4, segment_4_level_0
    // total version count is 5, version.0 and version.1 will be removed
    incDoc = "cmd=add,pk=3,string1=hello,ts=3;";
    fslib::FileList copyFiles;
    FileSystemWrapper::ListDir(secondPath, copyFiles, true);
    for (const auto& copyFile : copyFiles) {
        FileSystemWrapper::Copy(util::PathUtil::JoinPath(secondPath, copyFile), firstPath);
    }
    ASSERT_TRUE(FileSystemWrapper::IsExist(util::PathUtil::JoinPath(firstPath, "version.0")));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
    ASSERT_TRUE(secondDir->IsExist("segment_0_level_0/package_file.__data__0"));
    ASSERT_TRUE(secondDir->IsExist("segment_0_level_0/package_file.__meta__"));
    ASSERT_TRUE(secondDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(secondDir->IsExist("version.2"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(util::PathUtil::JoinPath(secondPath, "version.0")));
    ASSERT_TRUE(FileSystemWrapper::IsExist(util::PathUtil::JoinPath(firstPath, "version.0")));
}

void OnlinePartitionInteTest::TestCleanOnDiskIndexSecondaryPath()
{
    // online keepVersionCount
    mOptions.GetOnlineConfig().onlineKeepVersionCount = 3;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    mOptions.GetOnlineConfig().needReadRemoteIndex = true;
    mOptions.GetOnlineConfig().needDeployIndex = false;
    mOptions.GetOnlineConfig().loadConfigList.SetLoadMode(LoadConfig::LoadMode::REMOTE_ONLY);

    // offline keepVersionCount
    mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 100;
    mOptions.GetBuildConfig(false).enablePackageFile = true;

    string firstPath = util::PathUtil::JoinPath(mRootPath, "first"); // online first
    FileSystemWrapper::MkDir(firstPath);
    string secondPath = util::PathUtil::JoinPath(mRootPath, "second"); // online second & build
    FileSystemWrapper::MkDir(secondPath);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, firstPath, "psm", secondPath));
    
        // version.0, segment_0
    string incDoc = "cmd=add,pk=1,string1=hello,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, incDoc, "", ""));

    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(
            OnlinePartition, indexPart);
    const IndexlibFileSystemPtr& fs = onlinePart->GetFileSystem();
    DirectoryPtr partitionDir = DirectoryCreator::Get(fs, firstPath, true);

    ASSERT_TRUE(partitionDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.0"));

    // version.2, segment_2
    // total version count is 3, no index will be removed
    incDoc = "cmd=add,pk=2,string1=hello,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));

    ASSERT_TRUE(partitionDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.0"));
    ASSERT_TRUE(partitionDir->IsExist("segment_1_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.1"));
    ASSERT_TRUE(partitionDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.2"));

    // version.4, segment_4_level_0
    // total version count is 5, version.0 and version.1 will be removed
    incDoc = "cmd=add,pk=3,string1=hello,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
    ASSERT_TRUE(partitionDir->IsExist("segment_0_level_0/package_file.__data__0"));
    ASSERT_TRUE(partitionDir->IsExist("segment_0_level_0/package_file.__meta__"));
    ASSERT_TRUE(partitionDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.2"));
}


void OnlinePartitionInteTest::TestCleanOnDiskIndexWithVersionHeldByReader()
{
    PartitionStateMachine psm;
    // online keepVersionCount
    mOptions.GetOnlineConfig().onlineKeepVersionCount = 1;
    // offline keepVersionCount
    mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 100;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    // version.0, segment_0
    string incDoc = "cmd=add,pk=1,string1=hello,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, incDoc, "", ""));

    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(
            OnlinePartition, indexPart);
    const IndexlibFileSystemPtr& fs = onlinePart->GetFileSystem();
    DirectoryPtr partitionDir = DirectoryCreator::Get(fs, mRootPath, true);

    ASSERT_TRUE(partitionDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.0"));

    // version.2, SEGMENT_2_level_0
    // total version count is 3, version.0 is held by reader
    incDoc = "cmd=add,pk=2,string1=hello,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));

    ASSERT_TRUE(partitionDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.0"));
    ASSERT_TRUE(partitionDir->IsExist("segment_1_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.1"));
    ASSERT_TRUE(partitionDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.2"));

    // the reader which hold version.0 will be released
    // version.0 version.1 will be removed
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    ASSERT_FALSE(partitionDir->IsExist("segment_0_level_0"));
    ASSERT_FALSE(partitionDir->IsExist("version.0"));
    ASSERT_FALSE(partitionDir->IsExist("segment_1_level_0"));
    ASSERT_FALSE(partitionDir->IsExist("version.1"));
    ASSERT_TRUE(partitionDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(partitionDir->IsExist("version.2"));
}

void OnlinePartitionInteTest::TestUpdateCompressValueLongCaseTest()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "string:false:true:equal", SFP_ATTRIBUTE);

    const size_t COUNT = 1000000;
    const size_t UPDATE_COUNT = 500000;

    stringstream ss;
    for (size_t i = 0; i < COUNT; i++)
    {
        ss << "hello,";
    }

    provider.Build(ss.str(), SFP_OFFLINE);

    cout << "***************" << endl;

    stringstream upss;
    for (size_t i = 0; i < UPDATE_COUNT; i++)
    {
        upss << "update_field:" << i << ":hello" << i << ",";
    }
    provider.Build(upss.str(), SFP_REALTIME);

    PartitionDataPtr partData = provider.GetPartitionData();
    config::AttributeConfigPtr attrConfig = provider.GetAttributeConfig();

    StringAttributeReader attrReader;
    attrReader.Open(attrConfig, partData);

    string value;
    for (size_t i = 0; i < COUNT; i++)
    {
        attrReader.Read(i, value, NULL);
        if (i < UPDATE_COUNT)
        {
            stringstream ss;
            ss << "hello" << i;
            ASSERT_EQ(ss.str(), value);
        }
        else
        {
            stringstream a;
            a << i;
            SCOPED_TRACE(a.str().c_str());
            ASSERT_EQ(string("hello"), value);
        }
    }
}

void OnlinePartitionInteTest::TestOptimizedReopenForIncNewerThanRtLongCaseTest()
{
    DoTestOptimizedReopen("0,1,2", "3", "3,4", false);
}

void OnlinePartitionInteTest::TestOptimizedReopenForAllModifiedOnDiskDocUnchanged()
{
    DoTestOptimizedReopen("0,1,2", "3,4", "3", false);
}

void OnlinePartitionInteTest::TestOptimizedReopenForAllModifiedOnDiskDocChanged()
{
    DoTestOptimizedReopen("0,1,2", "3,4", "3", true);
}

void OnlinePartitionInteTest::TestOptimizedReopenForAllModifiedRtDocBeenCovered()
{
    DoTestOptimizedReopen("0", "1,2,3,4", "1,2,3", false);
}

void OnlinePartitionInteTest::TestOptimizedReopenForAllModifiedRtDocNotBeenCovered()
{
    DoTestOptimizedReopen("0", "1,2,5,3,4", "1,2", false);
}

void OnlinePartitionInteTest::TestOptimizedReopenForDeleteDocLongCaseTest()
{
    {
        EnvGuard env("VERSION_FORMAT_NUM", "2");
        // rt del full
        DoTestOptimizedReopenForDelete("a0,a1,a2", "d0,d1", "d0", false, "2");
        DoTestOptimizedReopenForDelete("a0,a1,a2", "d0,d1", "d0", true, "1");

        // rt del rt and covered by inc
        DoTestOptimizedReopenForDelete("a0", "a1,a2,d1", "a1,a2", false, "0,4");
        DoTestOptimizedReopenForDelete("a0", "a1,a2,d1", "a1,a2", true, "0,4");
    
        // rt del full and add
        DoTestOptimizedReopenForDelete("a0", "d0,a0", "d0", false, "1");
        DoTestOptimizedReopenForDelete("a0", "d0,a0", "d0", true, "0");
    
        // all rt covered by inc
        DoTestOptimizedReopenForDelete("a0,a1", "d0,a0", "d0,a0,d0", false, "1");
        DoTestOptimizedReopenForDelete("a0,a1", "d0,a0", "d0,a0,d0", true, "0");

        DoTestOptimizedReopenForDelete("a0,a1", "a0", "a0,a1", false, "2,3");
        DoTestOptimizedReopenForDelete("a0,a1", "a0", "a0,a1", true, "0,1");

        DoTestOptimizedReopenForDelete("a0", "a1,a2,a1", "a1,a2", false, "0,4,5");
        DoTestOptimizedReopenForDelete("a0", "a1,a2,a1", "a1,a2", true, "0,4,5");

        DoTestOptimizedReopenForDelete("a0", "a2,a1,a1", "a2", false, "0,2,4");
        DoTestOptimizedReopenForDelete("a0", "a2,a1,a1", "a2", true, "0,2,4");        
        
    }
    {
        EnvGuard env("VERSION_FORMAT_NUM", "1");
        // rt del full
        DoTestOptimizedReopenForDelete("a0,a1,a2", "d0,d1", "d0", false, "2");
        DoTestOptimizedReopenForDelete("a0,a1,a2", "d0,d1", "d0", true, "1");

        // rt del rt and covered by inc
        DoTestOptimizedReopenForDelete("a0", "a1,a2,d1", "a1,a2", false, "0,2");
        DoTestOptimizedReopenForDelete("a0", "a1,a2,d1", "a1,a2", true, "0,2");
    
        // rt del full and add
        DoTestOptimizedReopenForDelete("a0", "d0,a0", "d0", false, "1");
        DoTestOptimizedReopenForDelete("a0", "d0,a0", "d0", true, "0");
    
        // all rt covered by inc
        DoTestOptimizedReopenForDelete("a0,a1", "d0,a0", "d0,a0,d0", false, "1");
        DoTestOptimizedReopenForDelete("a0,a1", "d0,a0", "d0,a0,d0", true, "0");

        DoTestOptimizedReopenForDelete("a0,a1", "a0", "a0,a1", false, "2,3");
        DoTestOptimizedReopenForDelete("a0,a1", "a0", "a0,a1", true, "0,1");

        DoTestOptimizedReopenForDelete("a0", "a1,a2,a1", "a1,a2", false, "0,2,5");
        DoTestOptimizedReopenForDelete("a0", "a1,a2,a1", "a1,a2", true, "0,2,5");

        DoTestOptimizedReopenForDelete("a0", "a2,a1,a1", "a2", false, "0,1,4");
        DoTestOptimizedReopenForDelete("a0", "a2,a1,a1", "a2", true, "0,1,4");        
        
    }    

}

void OnlinePartitionInteTest::DoTestOptimizedReopenForDelete(
        const string& fullDocSeq, const string& rtDocSeq, 
        const string& incDocSeq, bool isIncMerge,
        const string& expectDocids)
{
    TearDown();
    SetUp();
    int64_t fullIndexTs = 0;
    int64_t incIndexTs = 10000;
    int64_t rtIndexTs = incIndexTs;

    string fullDoc = MakeDocs(fullDocSeq, fullIndexTs);
    string rtDoc = MakeDocs(rtDocSeq, rtIndexTs);
    string incDoc = MakeDocs(incDocSeq, incIndexTs);

    IndexPartitionOptions options;
    options.GetOnlineConfig().isIncConsistentWithRealtime = true;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
    if (isIncMerge)
    {
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
    }
    else
    {
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    }
    vector<string> docids = StringUtil::split(expectDocids, ",");
    string expectResult;
    for (size_t i = 0; i < docids.size(); ++i)
    {
        expectResult += string("docid=") + docids[i] + ";";
    }
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:hello", expectResult));
}

string OnlinePartitionInteTest::MakeDocs(const string& docsStr, int64_t timeStamp)
{
    vector<string> docStrs;
    StringUtil::fromString(docsStr, docStrs, ",");
    string formatedDocs;
    for (size_t i = 0; i < docStrs.size(); ++i)
    {
        if (docStrs[i][0] == 'a')
        {
            formatedDocs += "cmd=add,pk=" + docStrs[i].substr(1) +
                            ",string1=hello,ts=" + 
                            StringUtil::toString(timeStamp++) + ";";
        }
        else if (docStrs[i][0] == 'd')
        {
            formatedDocs += "cmd=delete,pk=" + docStrs[i].substr(1) + ",ts=" + 
                            StringUtil::toString(timeStamp++) + ";";
        }
        else
        {
            assert(false);
        }
    }
    return formatedDocs;
}

void OnlinePartitionInteTest::DoTestOptimizedReopen(const string& fullDocSeq,
        const string& rtDocSeq, const string& incDocSeq, bool isIncMerge)
{
    vector<string> docs;
    docs.push_back("cmd=add,pk=0,string1=hello,long1=0,ts=0;");
    docs.push_back("cmd=update_field,pk=0,long1=0,ts=1;");

    docs.push_back("cmd=add,pk=1,string1=hello,long1=1,ts=10;"
                   "cmd=add,pk=2,string1=hello,long1=2,ts=20;"
                   "cmd=add,pk=3,string1=hello,long1=3,ts=30;");

    docs.push_back("cmd=update_field,pk=0,long1=0,ts=100;");

    docs.push_back("cmd=add,pk=1,string1=hello,long1=11,ts=110;"
                   "cmd=update_field,pk=2,long1=12,ts=120;"
                   "cmd=delete,pk=3,ts=130;");

    docs.push_back("cmd=add,pk=1,string1=hello,long1=1,ts=40;"
                   "cmd=add,pk=2,string1=hello,long1=2,ts=50;"
                   "cmd=add,pk=3,string1=hello,long1=3,ts=60;");

    string fullDoc = GetDoc(docs, fullDocSeq);
    string rtDoc = GetDoc(docs, rtDocSeq);
    string incDoc = GetDoc(docs, incDocSeq);

    IndexPartitionOptions options;
    options.GetOnlineConfig().isIncConsistentWithRealtime = true;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
    if (isIncMerge)
    {
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
    }
    else
    {
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    }

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=11"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "long1=12"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", ""));
}

void OnlinePartitionInteTest::TestIncCoverRtWithoutPk()
{
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "2");
        IndexPartitionOptions options;
        options.GetOnlineConfig().buildConfig.maxDocCount = 1;
        options.GetOfflineConfig().buildConfig.maxDocCount = 1;
        PartitionStateMachine psm;
        string field = "string1:string;long1:uint32;";
        string index = "index1:string:string1;";
        string attr = "long1;";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, "");

        string fullDocs = "cmd=add,string1=hello,long1=0,ts=2;";
        ASSERT_TRUE(psm.Init(schema, options, mRootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));
    
        string rtDocs = "cmd=add,string1=hello,long1=1,ts=3;"
            "cmd=add,string1=hello,long1=2,ts=4;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "index1:hello", "long1=0;1;2"));

        string incDocs = "cmd=add,string1=hello,long1=1,ts=3;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "index1:hello", "long1=0;1;1;2")); 
    }
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "1");
        IndexPartitionOptions options;
        options.GetOnlineConfig().buildConfig.maxDocCount = 1;
        options.GetOfflineConfig().buildConfig.maxDocCount = 1;
        PartitionStateMachine psm;
        string field = "string1:string;long1:uint32;";
        string index = "index1:string:string1;";
        string attr = "long1;";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, "");

        string fullDocs = "cmd=add,string1=hello,long1=0,ts=2;";
        ASSERT_TRUE(psm.Init(schema, options, mRootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));
    
        string rtDocs = "cmd=add,string1=hello,long1=1,ts=3;"
            "cmd=add,string1=hello,long1=2,ts=4;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "index1:hello", "long1=0;1;2"));

        string incDocs = "cmd=add,string1=hello,long1=1,ts=3;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "index1:hello", "long1=0;1;2"));        
    }    
}

void OnlinePartitionInteTest::TestIncCoverPartialRtWithVersionTsLTStopTs()
{
    {
        EnvUtil::SetEnv("VERSION_FORMAT_NUM", "1");
        IndexPartitionOptions options;
        options.GetOnlineConfig().buildConfig.maxDocCount = 10;
        options.GetOnlineConfig().SetEnableRedoSpeedup(true);
        options.GetOfflineConfig().buildConfig.maxDocCount = 10;
        PartitionStateMachine psm;
        string fullDocs = "cmd=add,pk=1,string1=hello,long1=0,ts=2;";
        ASSERT_TRUE(psm.Init(mSchema, options, mRootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
    
        string rtDocs = "cmd=add,pk=2,string1=hello,long1=1,ts=3;"
            "cmd=add,pk=3,string1=hello,long1=2,ts=4;"
            "cmd=add,pk=4,string1=hello,long1=3,ts=8;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "index1:hello", "long1=0;1;2;3"));

        string incDocs1 = "cmd=add,pk=2,string1=hello,long1=1,ts=3;"
            "cmd=add,pk=3,string1=hello,long1=2,ts=4;";

        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs1, "index1:hello", "long1=0;1;2;3"));
    
        string incDocs2 = "cmd=add,pk=4,string1=hello,long1=3,ts=8;"
            "cmd=add,pk=5,string1=hello,long1=4,ts=9;";

        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs2, "index1:hello", "long1=0;1;2;3;4"));
        EnvUtil::UnsetEnv("VERSION_FORMAT_NUM"); 
    }
    {
        TearDown();
        SetUp();        
        EnvUtil::SetEnv("VERSION_FORMAT_NUM", "2");
        IndexPartitionOptions options;
        options.GetOnlineConfig().buildConfig.maxDocCount = 10;
        options.GetOnlineConfig().SetEnableRedoSpeedup(true);
        options.GetOnlineConfig().SetVersionTsAlignment(1); 
        options.GetOfflineConfig().buildConfig.maxDocCount = 10;
        PartitionStateMachine psm;
        string fullDocs = "cmd=add,pk=1,string1=hello,long1=0,ts=2;";
        ASSERT_TRUE(psm.Init(mSchema, options, mRootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
    
        string rtDocs = "cmd=add,pk=2,string1=hello,long1=1,ts=3;"
            "cmd=add,pk=3,string1=hello,long1=2,ts=4;"
            "cmd=add,pk=4,string1=hello,long1=3,ts=8;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "index1:hello", "long1=0;1;2;3"));

        string incDocs1 = "cmd=add,pk=2,string1=hello,long1=1,ts=3;"
            "cmd=add,pk=3,string1=hello,long1=2,ts=4;";

        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs1, "index1:hello", "long1=0;1;2;3"));
    
        string incDocs2 = "cmd=add,pk=4,string1=hello,long1=3,ts=8;"
            "cmd=add,pk=5,string1=hello,long1=4,ts=9;";

        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs2, "index1:hello", "long1=0;1;2;3;4"));
        EnvUtil::UnsetEnv("VERSION_FORMAT_NUM"); 
    }
    {
        // test doc lost if rtBuild and incBuild start at different timestamp
        TearDown();
        SetUp(); 
        EnvUtil::SetEnv("VERSION_FORMAT_NUM", "2");
        IndexPartitionOptions options;
        options.GetOnlineConfig().buildConfig.maxDocCount = 10;
        options.GetOfflineConfig().buildConfig.maxDocCount = 10;
        options.GetOnlineConfig().SetEnableRedoSpeedup(true);
        options.GetOnlineConfig().SetVersionTsAlignment(1);         
        PartitionStateMachine psm;
        string fullDocs = "cmd=add,pk=1,string1=hello,long1=0,ts=2;"
        "cmd=add,pk=2,string1=hello,long1=1,ts=3;";
        ASSERT_TRUE(psm.Init(mSchema, options, mRootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
    
        string rtDocs = "cmd=add,pk=2,string1=hello,long1=1,ts=3;"
            "cmd=add,pk=3,string1=hello,long1=2,ts=4;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "index1:hello", "long1=0;1;2"));

        string incDocs1 = "cmd=add,pk=3,string1=hello,long1=2,ts=4;"
            "cmd=add,pk=4,string1=hello,long1=3,ts=5;"; 
        // pk = 2 lost
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs1, "index1:hello", "long1=0;2;3"));
        EnvUtil::UnsetEnv("VERSION_FORMAT_NUM"); 
    } 
}

void OnlinePartitionInteTest::TestBuildRtDocsFilteredByIncTimestampLongCaseTest()
{
    IndexPartitionOptions options;
    options.GetOnlineConfig().buildConfig.maxDocCount = 1;
    options.GetOfflineConfig().buildConfig.maxDocCount = 1;
    options.GetOnlineConfig().SetEnableRedoSpeedup(true);
    options.GetOnlineConfig().SetVersionTsAlignment(1);     
    PartitionStateMachine psm;

    string fullDocs = "cmd=add,pk=0,string1=hello,long1=0,ts=2;";

    ASSERT_TRUE(psm.Init(mSchema, options, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));
    
    string rtDocs = "cmd=add,pk=1,string1=hello,long1=0,ts=3;"
                    "cmd=add,pk=2,string1=hello,long1=1,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "pk:2", "long1=1"));

    string incDocs = "cmd=add,pk=1,string1=hello,long1=0,ts=2;"
                     "cmd=add,pk=3,string1=hello,long1=2,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "", ""));
    
    rtDocs = "cmd=add,pk=4,string1=hello,long1=4,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "pk:4", "long1=4"));
}

string OnlinePartitionInteTest::GetDoc(const vector<string>& docs,
                                       const string& docSeq)
{
    vector<size_t> docIndexes;
    StringUtil::fromString(docSeq, docIndexes, ",");

    string docStr = "";
    for (size_t i = 0; i < docIndexes.size(); ++i)
    {
        docStr += docs[docIndexes[i]];
    }
    return docStr;
}

void OnlinePartitionInteTest::TestGetDocIdWithCommand(string command)
{
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    string fullDocs = "cmd=add,pk=1,string1=hello,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                    "index1:hello", "docid=0;"));

    OnlinePartitionPtr onlinePartition(new OnlinePartition);
    onlinePartition->Open(GET_TEST_DATA_PATH(), "", mSchema, mOptions);

    string docStr = "cmd=" + command + ",pk=1,ts=2;";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateDocuments(mSchema, docStr);
    util::QuotaControlPtr quotaControl(new util::QuotaControl(1024 * 1024 * 1024));
    IndexBuilder builder(onlinePartition, quotaControl);
    ASSERT_TRUE(builder.Init());
    builder.Build(docs[0]);
    ASSERT_EQ((segmentid_t)0, docs[0]->GetSegmentIdBeforeModified());
}

void OnlinePartitionInteTest::TestGetDocIdAfterUpdateDocumentLongCaseTest()
{
    TestGetDocIdWithCommand("update_field");
}

void OnlinePartitionInteTest::TestEnableOpenLoadSpeedLimit()
{
    string field = "pk:string:pk;string1:string;";
    string index = "pk:primarykey64:pk";
    string attr = "string1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            field, index, attr, "");
    
    string content(4096 * 10, 'a');
    stringstream ss;
    ss << "cmd=add,pk=0,string1=" << content << ";";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, mOptions, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, ss.str(), 
                             "pk:0", "docid=0;"));
    
    string jsonStr = "                                                \
    {                                                                   \
    \"load_config\" :                                                   \
    [                                                                   \
    {                                                                   \
        \"file_patterns\" : [\"_ATTRIBUTE_\"],                          \
        \"load_strategy\" : \"mmap\",                                   \
        \"load_strategy_param\" : {                                     \
            \"advise_random\" : true,                                   \
            \"lock\" : true,                                            \
            \"slice\" : 4096,                                           \
            \"interval\" : 1000                                         \
        }                                                               \
    }                                                                   \
    ]                                                                   \
    }                                                                   \
    ";
    
    IndexPartitionOptions options;
    FromJsonString(options.GetOnlineConfig().loadConfigList, jsonStr);
    {
        OnlinePartitionPtr onlinePartition(new OnlinePartition);

        int64_t beginTime = TimeUtility::currentTimeInMicroSeconds();
        onlinePartition->Open(mRootPath, "", schema, options);
        int64_t endTime = TimeUtility::currentTimeInMicroSeconds();
        int64_t interval = endTime - beginTime;
        ASSERT_TRUE(interval < 10000000) << interval;
    }

    {
        options.GetOnlineConfig().enableLoadSpeedControlForOpen = true;
        OnlinePartitionPtr onlinePartition(new OnlinePartition);

        int64_t beginTime = TimeUtility::currentTimeInMicroSeconds();
        onlinePartition->Open(mRootPath, "", schema, options);
        int64_t endTime = TimeUtility::currentTimeInMicroSeconds();

        int64_t interval = endTime - beginTime;
        ASSERT_TRUE(interval > 10000000) << interval;
    }
}

void OnlinePartitionInteTest::MakeSchema(bool needSub)
{
    string field = "pk:uint64:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint64:true;"
                   "updatable_multi_long:uint64:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;pk;";
    string summary = "string1;";
    mSchema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);
    if (needSub)
    {
        string subfield = "substr1:string;subpkstr:string;sub_long:uint32;";
        string subindex = "subindex1:string:substr1;sub_pk:primarykey64:subpkstr;";
        string subattr = "substr1;subpkstr;sub_long;";
        string subsummary = "";

        IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
                subfield, subindex, subattr,subsummary);
        mSchema->SetSubIndexPartitionSchema(subSchema);
    }

}

void OnlinePartitionInteTest::TestPreloadPatchFile()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    string fullDocs = "cmd=add,pk=1,long1=1,updatable_multi_long=1 10,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "pk:1",
                                    "docid=0,long1=1,updatable_multi_long=1 10"));

    const OnlinePartitionPtr& partition =
        DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    const IndexlibFileSystemPtr& fs = partition->GetFileSystem();
    StorageMetrics metrics = fs->GetStorageMetrics(FSST_LOCAL);
    const FileCacheMetrics& fileCacheMetrics1 = metrics.GetFileCacheMetrics();
    ASSERT_EQ((size_t)0, fileCacheMetrics1.replaceCount.getValue());

    string incDocs = "cmd=update_field,pk=1,long1=4,updatable_multi_long=4 40,ts=4;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "pk:1",
                                    "docid=0,long1=4,updatable_multi_long=4 40"));
    metrics = fs->GetStorageMetrics(FSST_LOCAL);
    const FileCacheMetrics& fileCacheMetrics2 = metrics.GetFileCacheMetrics();
    ASSERT_EQ((size_t)0, fileCacheMetrics2.replaceCount.getValue());
}

void OnlinePartitionInteTest::TestReopenForCompressOperationBlocks()
{
    // for building rt segment load patch
    DoTestReopenWithCompressOperationsForBuildingSegment("0,1,2", "3", "3,4", false);
    DoTestReopenWithCompressOperationsForBuildingSegment("0,1,2", "3,4", "3", false);
    DoTestReopenWithCompressOperationsForBuildingSegment("0,1,2", "3,4", "3", true);
    DoTestReopenWithCompressOperationsForBuildingSegment("0", "1,2,3,4", "1,2,3", false);
    DoTestReopenWithCompressOperationsForBuildingSegment("0", "1,2,5,3,4", "1,2", false);

    // for built rt segment load patch
    DoTestReopenWithCompressOperationsForBuiltSegment("0,1,2", "3", "3,4", false);
    DoTestReopenWithCompressOperationsForBuiltSegment("0,1,2", "3,4", "3", false);
    DoTestReopenWithCompressOperationsForBuiltSegment("0,1,2", "3,4", "3", true);
    DoTestReopenWithCompressOperationsForBuiltSegment("0", "1,2,3,4", "1,2,3", false);
    DoTestReopenWithCompressOperationsForBuiltSegment("0", "1,2,5,3,4", "1,2", false);
}

void OnlinePartitionInteTest::DoTestReopenWithCompressOperationsForBuildingSegment(
        const std::string& fullDocSeq, const std::string& rtDocSeq,
        const std::string& incDocSeq, bool isIncMerge)
{
    TearDown();
    SetUp();

    vector<string> docs;
    docs.push_back("cmd=add,pk=0,string1=hello,long1=0,ts=0;");
    docs.push_back("cmd=update_field,pk=0,long1=0,ts=1;");

    docs.push_back("cmd=add,pk=1,string1=hello,long1=1,ts=10;"
                   "cmd=add,pk=2,string1=hello,long1=2,ts=20;"
                   "cmd=add,pk=3,string1=hello,long1=3,ts=30;");

    docs.push_back("cmd=update_field,pk=0,long1=0,ts=100;");

    docs.push_back("cmd=add,pk=1,string1=hello,long1=11,ts=110;"
                   "cmd=update_field,pk=2,long1=12,ts=120;"
                   "cmd=delete,pk=3,ts=130;");

    docs.push_back("cmd=add,pk=1,string1=hello,long1=1,ts=40;"
                   "cmd=add,pk=2,string1=hello,long1=2,ts=50;"
                   "cmd=add,pk=3,string1=hello,long1=3,ts=60;");

    string fullDoc = GetDoc(docs, fullDocSeq);
    string rtDoc = GetDoc(docs, rtDocSeq);
    string incDoc = GetDoc(docs, incDocSeq);

    IndexPartitionOptions options;
    options.GetOnlineConfig().isIncConsistentWithRealtime = false;
    options.GetOnlineConfig().enableCompressOperationBlock = true;
    options.GetOnlineConfig().maxOperationQueueBlockSize = 2;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
    if (isIncMerge)
    {
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
    }
    else
    {
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    }

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=11"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "long1=12"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", ""));
}

void OnlinePartitionInteTest::TestSegmentDirNameCompatibility()
{
    string indexDir = string(TEST_DATA_PATH) + "/compatible_index_for_segment_directory_name";
    string rootDir = GET_TEST_DATA_PATH() + "/part_dir";
    fslib::fs::FileSystem::copy(indexDir, rootDir);
    IndexPartitionOptions options;
    IndexPartitionSchemaPtr schema =
        SchemaAdapter::LoadAndRewritePartitionSchema(rootDir, options);

    PartitionStateMachine psm;    
    ASSERT_TRUE(psm.Init(schema, options, rootDir));
    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "index:hello",
                             "price=0;price=1;price=2;"));

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE,
                             "cmd=add,pk=3,string1=hello,price=3;",
                             "index:hello",
                             "price=0;price=1;price=2;price=3"));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "",
                             "index:hello",
                             "price=0;price=1;price=2;price=3"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT,
                             "cmd=add,pk=4,string1=hello,price=4,ts=100;",
                             "index:hello",
                             "price=0;price=1;price=2;price=3;price=4"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT,
                             "cmd=add,pk=5,string1=hello,price=5,ts=101;",
                             "index:hello",
                             "price=0;price=1;price=2;price=3;price=4;price=5"));
}

void OnlinePartitionInteTest::DoTestReopenWithCompressOperationsForBuiltSegment(
        const std::string& fullDocSeq, const std::string& rtDocSeq,
        const std::string& incDocSeq, bool isIncMerge)
{
    TearDown();
    SetUp();

    vector<string> docs;
    docs.push_back("cmd=add,pk=0,string1=hello,long1=0,ts=0;");
    docs.push_back("cmd=update_field,pk=0,long1=0,ts=1;");

    docs.push_back("cmd=add,pk=1,string1=hello,long1=1,ts=10;"
                   "cmd=add,pk=2,string1=hello,long1=2,ts=20;"
                   "cmd=add,pk=3,string1=hello,long1=3,ts=30;");

    docs.push_back("cmd=update_field,pk=0,long1=0,ts=100;");

    docs.push_back("cmd=add,pk=1,string1=hello,long1=11,ts=110;"
                   "cmd=update_field,pk=2,long1=12,ts=120;"
                   "cmd=delete,pk=3,ts=130;");

    docs.push_back("cmd=add,pk=1,string1=hello,long1=1,ts=40;"
                   "cmd=add,pk=2,string1=hello,long1=2,ts=50;"
                   "cmd=add,pk=3,string1=hello,long1=3,ts=60;");

    string fullDoc = GetDoc(docs, fullDocSeq);
    string incDoc = GetDoc(docs, incDocSeq);

    IndexPartitionOptions options;
    options.GetOnlineConfig().isIncConsistentWithRealtime = false;
    options.GetOnlineConfig().enableCompressOperationBlock = true;
    options.GetOnlineConfig().maxOperationQueueBlockSize = 2;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));

    vector<size_t> docIndexes;
    StringUtil::fromString(rtDocSeq, docIndexes, ",");
    for (size_t i = 0; i < docIndexes.size(); ++i)
    {
        string rtDoc = docs[docIndexes[i]];
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDoc, "", ""));
    }

    if (isIncMerge)
    {
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
    }
    else
    {
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    }

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=11"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "long1=12"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", ""));
}

void OnlinePartitionInteTest::TestLatestNeedSkipIncTimestamp()
{
    string docString = "cmd=add,pk=1,string1=hello,long1=2,multi_long=3,ts=1000000";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    string rtDocs = "cmd=add,pk=1,string1=hello,long1=2,multi_long=3,ts=2000000,locator=1:2000000;"
                    "cmd=add,pk=1,string1=hello,long1=2,multi_long=3,ts=3000000,locator=1:3000000";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs, "", ""));

    string incDoc1 = "cmd=add,pk=1,string1=hello,long1=2,multi_long=3,ts=2000000,locator=1:2000000;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDoc1, "", ""));

    // not full cover
    IndexPartitionReaderPtr reader = indexPart->GetReader();
    OnlinePartitionReaderPtr onlineReader = DYNAMIC_POINTER_CAST(OnlinePartitionReader, reader);
    assert(reader);
    ASSERT_EQ((int64_t)-1, onlineReader->mLatestNeedSkipIncTs);

    string incDoc2 = "cmd=add,pk=1,string1=hello,long1=2,multi_long=3,ts=3000000,locator=1:3000000;"
                     "cmd=add,pk=1,string1=hello,long1=2,multi_long=3,ts=4000000,locator=1:4000000";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDoc2, "", ""));

    // full cover
    reader = indexPart->GetReader();
    onlineReader = DYNAMIC_POINTER_CAST(OnlinePartitionReader, reader);
    assert(reader);
    ASSERT_EQ((int64_t)4000000, onlineReader->mLatestNeedSkipIncTs);
}

void OnlinePartitionInteTest::TestAsyncDumpWithIncReopen()
{
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "1");
        DumpSegmentContainerPtr dumpContainer(new SlowDumpSegmentContainer(1000));
        mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
        PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false,
                                  partition::IndexPartitionResource(), dumpContainer);
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

        string rtDocs = "cmd=add,pk=1,string1=hello,ts=1;"
            "cmd=add,pk=2,string1=hello2,ts=2";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs,
                                        "index1:hello", "docid=0;"));
    
        ASSERT_EQ(1u, dumpContainer->GetDumpItemSize());
        string incDocs = "cmd=add,pk=1,string1=hello1,ts=1;";
        INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                        "index1:hello1", "docid=0;"));
        ASSERT_EQ(0u, dumpContainer->GetDumpItemSize());    
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello2", "docid=2;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", ""));        
    }
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "2");
        DumpSegmentContainerPtr dumpContainer(new SlowDumpSegmentContainer(1000));
        mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
        PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false,
                                  partition::IndexPartitionResource(), dumpContainer);
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

        string rtDocs = "cmd=add,pk=1,string1=hello,ts=1;"
            "cmd=add,pk=2,string1=hello2,ts=2";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs,
                                        "index1:hello", "docid=0;"));
    
        ASSERT_EQ(1u, dumpContainer->GetDumpItemSize());
        string incDocs = "cmd=add,pk=1,string1=hello1,ts=2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(mBuildIncEvent, incDocs,
                                        "index1:hello1", "docid=0;"));
        ASSERT_EQ(0u, dumpContainer->GetDumpItemSize());    
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello2", "docid=2;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", ""));        
    }    

}

void OnlinePartitionInteTest::TestBuildThreadReopenNewSegment()
{
    DumpSegmentContainerPtr dumpContainer(new SlowDumpSegmentContainer(1000));
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false,
                              partition::IndexPartitionResource(), dumpContainer);
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

    string rtDocs = "cmd=add,pk=1,string1=hello,ts=1;"
        "cmd=add,pk=2,string1=hello2,ts=2";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs,
                                    "index1:hello", "docid=0;")); //push to dump container

    usleep(1000000);
    for (size_t i = 0; i < 20 && dumpContainer->GetUnDumpedSegmentSize() > 0; i ++)
    {
        usleep(500000);
    }
    ASSERT_EQ(0u, dumpContainer->GetUnDumpedSegmentSize());
    ASSERT_EQ(1u, dumpContainer->GetDumpItemSize());
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pk=3,string1=hello3,ts=3",
                                    "index1:hello3", "docid=2;")); //push to dump container reopen
    ASSERT_EQ(1u, dumpContainer->GetDumpItemSize());
    ASSERT_EQ(1u, dumpContainer->GetUnDumpedSegmentSize());
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "cmd=add,pk=4,string1=hello4,ts=4",
                                    "index1:hello4", "docid=3;")); //push to dump container reopen
}

void OnlinePartitionInteTest::TestRedoWithMultiDumpingSegment()
{
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "2");
        DumpSegmentContainerPtr dumpContainer(new SlowDumpSegmentContainer(2000, false, true));
        mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
        mOptions.GetBuildConfig(true).maxDocCount = 1;
        mOptions.GetBuildConfig(false).maxDocCount = 1;
    
        PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false,
                                  partition::IndexPartitionResource(), dumpContainer);
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

        string fullDocs = "cmd=add,pk=1,string1=hello,long1=1,ts=1;"
            "cmd=add,pk=2,string1=hello2,long1=2,ts=2;"
            "cmd=add,pk=3,string1=hello3,long1=3,ts=3;";
    
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                        "index1:hello", "docid=0,long1=1"));

        string rtDocs = "cmd=add,pk=1,string1=hello,long1=4,ts=4;"
            "cmd=add,pk=2,string1=hello2,long1=5,ts=5;"
            "cmd=add,pk=4,string1=hello4,long1=6,ts=6;";

        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs, "", ""));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", "docid=3,long1=4"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello2", "docid=4,long1=5"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello3", "docid=2,long1=3"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello4", "docid=5,long1=6"));
    
        string incDocs = "cmd=add,pk=1,string1=hello,long1=7,ts=4;"
            "cmd=add,pk=2,string1=hello2,long1=8,ts=5;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", "docid=3,long1=7"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello2", "docid=5,long1=5"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello3", "docid=2,long1=3"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello4", "docid=6,long1=6"));        
    }
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "1");
        DumpSegmentContainerPtr dumpContainer(new SlowDumpSegmentContainer(2000, false, true));
        mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
        mOptions.GetBuildConfig(true).maxDocCount = 1;
        mOptions.GetBuildConfig(false).maxDocCount = 1;
    
        PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false,
                                  partition::IndexPartitionResource(), dumpContainer);
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

        string fullDocs = "cmd=add,pk=1,string1=hello,long1=1,ts=1;"
            "cmd=add,pk=2,string1=hello2,long1=2,ts=2;"
            "cmd=add,pk=3,string1=hello3,long1=3,ts=3;";
    
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                        "index1:hello", "docid=0,long1=1"));

        string rtDocs = "cmd=add,pk=1,string1=hello,long1=4,ts=4;"
            "cmd=add,pk=2,string1=hello2,long1=5,ts=5;"
            "cmd=add,pk=4,string1=hello4,long1=6,ts=6;";

        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs, "", ""));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", "docid=3,long1=4"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello2", "docid=4,long1=5"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello3", "docid=2,long1=3"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello4", "docid=5,long1=6"));
    
        string incDocs = "cmd=add,pk=1,string1=hello,long1=7,ts=4;"
            "cmd=add,pk=2,string1=hello2,long1=8,ts=5;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello", "docid=3,long1=7"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello2", "docid=4,long1=8"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello3", "docid=2,long1=3"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello4", "docid=5,long1=6"));        
    }    
}

void OnlinePartitionInteTest::TestSharedRealtimeQuotaControl()
{
    misc::MetricProviderPtr metricProvider(new misc::MetricProvider(nullptr));
    TaskSchedulerPtr taskScheduler(new TaskScheduler);
    MemoryStatReporterPtr statReporter(new MemoryStatReporter);
    statReporter->Init("", util::SearchCachePartitionWrapperPtr(),
                       file_system::FileBlockCachePtr(), taskScheduler,
                       metricProvider);

    int64_t totalRtQuota = 8 * 1024 * 1024; // 8MB
    MemoryQuotaControllerPtr rtQuotaController(new MemoryQuotaController(totalRtQuota));

    IndexPartitionResource partitionResource;
    partitionResource.partitionGroupName = "test_group";
    partitionResource.metricProvider = metricProvider;
    partitionResource.taskScheduler = taskScheduler;
    partitionResource.memStatReporter = statReporter;
    partitionResource.fileBlockCache.reset(new file_system::FileBlockCache());
    partitionResource.realtimeQuotaController = rtQuotaController;

    string field = "string1:string;";
    string index = "index1:string:string1;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    INDEXLIB_TEST_TRUE(schema);

    // create OnlinePartition : p1, allocate 4MB
    string path1 = GET_TEST_DATA_PATH() + "/p1";
    OnlinePartitionPtr onlinePart1 = CreatePartitionForRealtimeMemoryLimit(
            schema, path1, partitionResource);
    ASSERT_EQ(IndexPartition::MS_OK, onlinePart1->CheckMemoryStatus());
    ASSERT_EQ((int64_t)4 * 1024 * 1024, rtQuotaController->GetFreeQuota());

    // create OnlinePartition : p2, allocate 4MB
    string path2 = GET_TEST_DATA_PATH() + "/p2";
    OnlinePartitionPtr onlinePart2 = CreatePartitionForRealtimeMemoryLimit(
            schema, path2, partitionResource);
    ASSERT_EQ(IndexPartition::MS_REACH_MAX_RT_INDEX_SIZE, onlinePart2->CheckMemoryStatus());
    ASSERT_EQ((int64_t)0, rtQuotaController->GetFreeQuota());

    // release p1
    onlinePart1.reset();
    ASSERT_EQ((int64_t)4 * 1024 * 1024, rtQuotaController->GetFreeQuota());

    sleep(5);  // wait onlinePart2 call report metrics
    ASSERT_EQ(IndexPartition::MS_OK, onlinePart2->CheckMemoryStatus());
    onlinePart2.reset();
    ASSERT_EQ((int64_t)totalRtQuota, rtQuotaController->GetFreeQuota());
}

void OnlinePartitionInteTest::TestSyncRealtimeQuota()
{
    misc::MetricProviderPtr metricProvider(new misc::MetricProvider());
    TaskSchedulerPtr taskScheduler(new TaskScheduler);
    MemoryStatReporterPtr statReporter(new MemoryStatReporter);
    statReporter->Init("", util::SearchCachePartitionWrapperPtr(),
                       file_system::FileBlockCachePtr(), taskScheduler,
                       metricProvider);

    int64_t totalRtQuota = 8 * 1024 * 1024; // 8MB
    MemoryQuotaControllerPtr rtQuotaController(new MemoryQuotaController(totalRtQuota));

    IndexPartitionResource partitionResource;
    partitionResource.partitionGroupName = "test_group";
    partitionResource.metricProvider = metricProvider;
    partitionResource.taskScheduler = taskScheduler;
    partitionResource.memStatReporter = statReporter;
    partitionResource.fileBlockCache.reset(new file_system::FileBlockCache());
    partitionResource.realtimeQuotaController = rtQuotaController;

    string field = "string1:string;string2:string";
    string index = "index1:string:string1;";
    string attribute = "string2";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    INDEXLIB_TEST_TRUE(schema);

    IndexPartitionOptions options;
    options.GetBuildConfig().maxDocCount = 1;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource);
    psm.Init(schema, options, GET_TEST_DATA_PATH());
    psm.DisableTestQuickExit();

    string value(4 * 1024 * 1024, 'a');
    string rtDocStr = "cmd=add,string1=hello,string2=" + value + ",ts=1;"
                      "cmd=add,string1=hello1,string2=str,ts=2;";

    psm.Transfer(BUILD_RT, rtDocStr, "index1:hello", "docid=0;");
    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    assert(onlinePart);
    ASSERT_EQ(IndexPartition::MS_REACH_MAX_RT_INDEX_SIZE, onlinePart->CheckMemoryStatus());
    ASSERT_EQ((int64_t)0, rtQuotaController->GetFreeQuota());

    string incDocStr = "cmd=add,string1=hello,string2=str,ts=1;"
                       "cmd=add,string1=hello1,string2=str,ts=2;";
    psm.Transfer(BUILD_INC, incDocStr, "index1:hello", "docid=0;");
    psm.Transfer(PE_REOPEN, "", "", ""); // make sure cleanCache happened

    ASSERT_EQ(IndexPartition::MS_OK, onlinePart->CheckMemoryStatus());
    ASSERT_EQ((int64_t)4 * 1024 * 1024, rtQuotaController->GetFreeQuota());
}

OnlinePartitionPtr OnlinePartitionInteTest::CreatePartitionForRealtimeMemoryLimit(
        const IndexPartitionSchemaPtr& schema, const string& path,
        const IndexPartitionResource& partitionResource)
{
    // create online partition: with tiny memory allocate
    // block memory quota control, default allocate one block (4MB)
    IndexPartitionOptions options;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource);
    bool ret = psm.Init(schema, options, path);
    assert(ret); (void)ret;
    psm.DisableTestQuickExit();

    string docString = "cmd=add,string1=hello,ts=1;";
    ret = psm.Transfer(BUILD_RT_SEGMENT, docString, "index1:hello", "docid=0;");
    assert(ret); (void)ret;
    return DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
}

void OnlinePartitionInteTest::TestMultiThreadInitAttributeReader()
{
    string field = "pk:uint64:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint64:true;long2:uint32;long3:uint32;"
                   "updatable_multi_long:uint64:true:true;"
                   "char:uint8;short:int16";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;char;short;packAttr1:long2,long3";
    string summary = "string1;";
    auto schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    
    IndexPartitionOptions options = mOptions;
    options.GetOnlineConfig().loadPatchThreadNum = 3;
    options.GetOnlineConfig().onlineKeepVersionCount = 100;
    options.GetOnlineConfig().SetInitReaderThreadCount(4);
    options.GetBuildConfig(true).keepVersionCount = 100;
    options.GetBuildConfig(false).keepVersionCount = 100;

    string fullDocs =
        "cmd=add,pk=1,string1=hello,long1=1,multi_long=1 2,updatable_multi_long=1 2 3,long2=1,long3=1,char=1,short=1;"
        "cmd=add,pk=2,string1=hello2,long1=2,multi_long=2 2,updatable_multi_long=2 2 3,long2=2,long3=2,char=2,short=2;";
    string incDocs1 =
        "cmd=add,pk=3,string1=hello3,long1=3,multi_long=3 2,updatable_multi_long=3 2 3,long2=3,long3=3;"
        "cmd=add,pk=4,string1=hello4,long1=4,multi_long=4 2,updatable_multi_long=4 2 3,long2=4,long3=4;";
    string incDocs2 =
        "cmd=add,pk=5,string1=hello5,long1=5,multi_long=5 2,updatable_multi_long=5 2 3,long2=5,long3=5;";
    string incDocs3 =
        "cmd=update_field,pk=1,long1=31,updatable_multi_long=31 2 3,long2=31;"
        "cmd=update_field,pk=2,long1=32,updatable_multi_long=32 2 3,long3=22;"
        "cmd=update_field,pk=4,updatable_multi_long=34 2 3,long2=34,long3=34;";
    string incDocs4 =
        "cmd=update_field,pk=3,long1=43,updatable_multi_long=43 2 3,long3=43;"
        "cmd=update_field,pk=2,long1=42;";
    string incDocs5 =
        "cmd=update_field,pk=5,long1=55,updatable_multi_long=55 2 3,long2=55;"
        "cmd=update_field,pk=4,long1=54,long3=54;"; 

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootPath));

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_REOPEN, fullDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs1, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs2, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs3, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs4, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs5, "", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "string1=hello,long1=31,multi_long=1 2,updatable_multi_long=31 2 3,long2=31,long3=1,char=1,short=1;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "string1=hello2,long1=42,multi_long=2 2,updatable_multi_long=32 2 3,long2=2,long3=22,char=2,short=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", "string1=hello3,long1=43,multi_long=3 2,updatable_multi_long=43 2 3,long2=3,long3=43;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "string1=hello4,long1=54,multi_long=4 2,updatable_multi_long=34 2 3,long2=34,long3=54;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:5", "string1=hello5,long1=55,multi_long=5 2,updatable_multi_long=55 2 3,long2=55,long3=5;"));    
}


IE_NAMESPACE_END(partition);

