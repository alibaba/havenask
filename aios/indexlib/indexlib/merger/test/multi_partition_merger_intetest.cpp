#include "indexlib/misc/metric_provider.h"
#include <fslib/fs/FileSystem.h>
#include <fslib/cache/FSCacheModule.h>
#include "indexlib/merger/test/multi_partition_merger_intetest.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/util/counter/state_counter.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MultiPartitionMergerInteTest);

MultiPartitionMergerInteTest::MultiPartitionMergerInteTest()
{
}

MultiPartitionMergerInteTest::~MultiPartitionMergerInteTest()
{
}

void MultiPartitionMergerInteTest::CaseSetUp()
{
    if (!GET_CASE_PARAM())
    {
        setenv("FSLIB_ENABLE_META_CACHE", "true", 1);
        setenv("FSLIB_CACHE_SUPPORTING_FS_TYPE", "LOCAL", 1);
        FileSystem::getCacheModule()->reset();
    }
    
    string field = "string1:string;price:uint32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
}

void MultiPartitionMergerInteTest::CaseTearDown()
{
    unsetenv("FSLIB_ENABLE_META_CACHE");
    unsetenv("FSLIB_CACHE_SUPPORTING_FS_TYPE");
    FileSystem::getCacheModule()->reset();
}

void MultiPartitionMergerInteTest::TestMergeWithDeleteAndUpdate()
{
    string part1DocString = "cmd=add,string1=hello,price=4;"
                            "cmd=add,string1=hello,price=5;"
                            "cmd=add,string1=hello1,price=6;"
                            "cmd=update_field,string1=hello1,price=7;"
                            "cmd=delete,string1=hello;"
                            "cmd=add,string1=hello2,price=8;"
                            "cmd=add,string1=hello3,price=9;"
                            "cmd=update_field,string1=hello1,price=10;";

    string part2DocString = "cmd=add,string1=world,price=4;"
                            "cmd=add,string1=world,price=5;"
                            "cmd=add,string1=world1,price=6;"
                            "cmd=update_field,string1=world1,price=7;"
                            "cmd=delete,string1=world;"
                            "cmd=add,string1=world2,price=8;"
                            "cmd=add,string1=world3,price=9;"
                            "cmd=update_field,string1=world1,price=10;";

    IndexPartitionOptions options;
    BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
    buildConfig.enablePackageFile = GET_CASE_PARAM();
    buildConfig.maxDocCount = 2;

    MakeOnePartitionData(mSchema, options, "part1", part1DocString);
    MakeOnePartitionData(mSchema, options, "part2", part2DocString);

    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");

    GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";

    options.SetIsOnline(false);
    MultiPartitionMerger multiPartMerger(options, NULL, "");
    multiPartMerger.Merge(mergeSrcs, mergePartPath);

    PartitionStateMachine psm;
    psm.Init(mSchema, options, mergePartPath);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello1", "docid=0,price=10"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello2", "docid=1,price=8"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello3", "docid=2,price=9"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:world", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:world1", "docid=3,price=10"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:world2", "docid=4,price=8"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:world3", "docid=5,price=9"));
}

void MultiPartitionMergerInteTest::MakeOnePartitionData(
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const string& partRootDir, const string& docStrs)
{
    GET_PARTITION_DIRECTORY()->MakeDirectory(partRootDir);
    string rootDir = GET_TEST_DATA_PATH() + partRootDir;
    PartitionStateMachine psm;
    psm.Init(schema, options, rootDir);
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrs, "", ""));
}

void MultiPartitionMergerInteTest::TestMergeWithSubDocDeleteAndUpdate(bool withPackage)
{
    string field = "pkstr:string;long1:uint32;";
    string index = "pk:primarykey64:pkstr;";
    string attr = "long1;";
    string summary = "pkstr;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            field, index, attr, summary);

    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            "subpkstr:string;sub_long:uint32;",
            "sub_pk:primarykey64:subpkstr",
            "sub_long;", "");
    schema->SetSubIndexPartitionSchema(subSchema);

    string docStrings1 = "cmd=add,pkstr=hello0,long1=0,subpkstr=sub0,sub_long=0;"
                         "cmd=add,pkstr=hello1,long1=1,subpkstr=sub1,sub_long=1;"
                         "cmd=add,pkstr=hello0,modify_fields=long1#sub_long,"
                         "subpkstr=sub0,sub_long=200,sub_modify_fields=sub_long;"
                         "cmd=update_field,pkstr=hello1,subpkstr=sub1,sub_long=11;"
                         "cmd=add,pkstr=hello2,long1=2,subpkstr=sub2,sub_long=2;"
                         "cmd=delete_sub,pkstr=hello2,subpkstr=sub2";

    string docStrings2 = "cmd=add,pkstr=hello3,long1=3,subpkstr=sub3,sub_long=3;"
                         "cmd=add,pkstr=hello4,long1=4,subpkstr=sub4,sub_long=4;"
                         "cmd=add,pkstr=hello3,long1=3,modify_fields=long1#sub_long,"
                         "subpkstr=sub3,sub_long=33,sub_modify_fields=sub_long;"
                         "cmd=update_field,pkstr=hello4,subpkstr=sub4,sub_long=44;"
                         "cmd=add,pkstr=hello5,long1=5,subpkstr=sub5,sub_long=5;"
                         "cmd=delete_sub,pkstr=hello5,subpkstr=sub5";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    if (withPackage)
    {
        options.GetMergeConfig().SetCheckpointInterval(0);
        options.GetOnlineConfig().onlineKeepVersionCount = 100;
        options.GetOfflineConfig().buildConfig.enablePackageFile = true;
        options.GetOfflineConfig().buildConfig.keepVersionCount = 100;
        options.GetMergeConfig().SetEnablePackageFile(true);
    }

    BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
    buildConfig.enablePackageFile = GET_CASE_PARAM();
    buildConfig.maxDocCount = 1;
    MakeOnePartitionData(schema, options, "part1", docStrings1);
    MakeOnePartitionData(schema, options, "part2", docStrings2);

    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");

    GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";

    misc::MetricProviderPtr metricProvider(new misc::MetricProvider);
    MultiPartitionMerger multiPartMerger(options, metricProvider, "");
    MetricPtr metric = metricProvider->DeclareMetric("indexlib/merge/progress", kmonitor::STATUS);

    ASSERT_TRUE(metric != nullptr);
    ASSERT_DOUBLE_EQ(std::numeric_limits<double>::min(), metric->GetValue());
    multiPartMerger.Merge(mergeSrcs, mergePartPath);
    ASSERT_DOUBLE_EQ(100.0, metric->GetValue());
    
    PartitionStateMachine psm;
    psm.Init(schema, options, mergePartPath);
    //check main doc
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello0",
                             "docid=0,main_join=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello1",
                             "docid=1,main_join=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello2",
                             "docid=2,main_join=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello3",
                             "docid=3,main_join=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello4",
                             "docid=4,main_join=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello5", 
                             "docid=5,main_join=4"));
}

void MultiPartitionMergerInteTest::TestMergePackageWithSubDocDeleteAndUpdate()
{
    TestMergeWithSubDocDeleteAndUpdate(true);
}

void MultiPartitionMergerInteTest::TestMultiPartFullMergeWithAllSegmentsMergeToOneSegment()
{
    // AllSegmentsMergeToOneSegment
    // before merge: part1(seg_0, seg_1), part2(seg_0, seg_1, seg_2)
    // after merge: merge_part(seg_0)
    DoTestMerge(true, true, OPTIMIZE_MERGE_STRATEGY_STR, "");
}

void MultiPartitionMergerInteTest::TestMultiPartFullMergeWithMultiSegmentsMergeToOneSegment()
{
    // MultiSegmentsMergeToOneSegment
    // before merge: part1(seg_0, seg_1), part2(seg_0, seg_1, seg_2)
    // after merge: merge_part(seg_0, seg_1, seg_2)
    DoTestMerge(true, true, OPTIMIZE_MERGE_STRATEGY_STR, "after-merge-max-doc-count=4");
}

void MultiPartitionMergerInteTest::TestMultiPartFullMergeWithSingleSegmentMergeToOneSegment()
{
    // SingleSegmentMergeToOneSegment
    // before merge: part1(seg_0, seg_1), part2(seg_0, seg_1, seg_2)
    // after merge: merge_part(seg_0, seg_1, seg_2, seg_3, seg_4)
    DoTestMerge(true, true, OPTIMIZE_MERGE_STRATEGY_STR, "after-merge-max-doc-count=1");
}

void MultiPartitionMergerInteTest::TestSinglePartFullMergeWithAllSegmentsMergeToOneSegment()
{
    DoTestMerge(true, false, OPTIMIZE_MERGE_STRATEGY_STR, "");
}

void MultiPartitionMergerInteTest::TestSinglePartFullMergeWithMultiSegmentsMergeToOneSegment()
{
    DoTestMerge(true, false, OPTIMIZE_MERGE_STRATEGY_STR, "after-merge-max-doc-count=4");
}

void MultiPartitionMergerInteTest::TestSinglePartFullMergeWithSingleSegmentMergeToOneSegment()
{
    DoTestMerge(true, false, OPTIMIZE_MERGE_STRATEGY_STR, "after-merge-max-doc-count=1");
}

void MultiPartitionMergerInteTest::TestIncOptimizeMergeWithAllSegmentsMergeToOneSegment()
{
    DoTestMerge(false, false, OPTIMIZE_MERGE_STRATEGY_STR, "");
}

void MultiPartitionMergerInteTest::TestIncOptimizeMergeWithMultiSegmentsMergeToOneSegment()
{
    DoTestMerge(false, false, OPTIMIZE_MERGE_STRATEGY_STR, "after-merge-max-doc-count=4");
}

void MultiPartitionMergerInteTest::TestIncOptimizeMergeWithSingleSegmentMergeToOneSegment()
{
    DoTestMerge(false, false, OPTIMIZE_MERGE_STRATEGY_STR, "after-merge-max-doc-count=1");
}

void MultiPartitionMergerInteTest::TestIncBalanceTreeMerge()
{
    DoTestMerge(false, false, BALANCE_TREE_MERGE_STRATEGY_STR, "conflict-segment-number=2;base-doc-count=2;max-doc-count=8");
}

void MultiPartitionMergerInteTest::TestIncPriorityQueueMerge()
{
    DoTestMerge(false, false, PRIORITY_QUEUE_MERGE_STRATEGY_STR, 
                "max-doc-count=8|priority-feature=valid-doc-count;conflict-segment-count=2|max-merged-segment-doc-count=7");
}

void MultiPartitionMergerInteTest::TestIncRealtimeMerge()
{
    DoTestMerge(false, false, REALTIME_MERGE_STRATEGY_STR, "max-small-segment-count=10;merge-size-upperbound=0;merge-size-lowerbound=0");
}

void MultiPartitionMergerInteTest::TestMaxDocCountWithFullMerge()
{
    DoTestMerge(true, false, OPTIMIZE_MERGE_STRATEGY_STR, "max-doc-count=2");
    // seg_0,1,2,3,4,5-->seg_0
    // max-doc-count does not work when full merge
    Version version;
    VersionLoader versionLoader;
    versionLoader.GetVersion(GET_TEST_DATA_PATH() + "mergePart", version, INVALID_VERSION);
    ASSERT_TRUE(version.GetSegmentCount() == (size_t)1);
}

void MultiPartitionMergerInteTest::TestMaxDocCountWithIncOptimizeMerge()
{
    DoTestMerge(false, false, OPTIMIZE_MERGE_STRATEGY_STR, "max-doc-count=2");
    //seg_0,1,2,3,4,5-->seg_0,1,2,3,4,6
    Version version;
    VersionLoader versionLoader;
    versionLoader.GetVersion(GET_TEST_DATA_PATH() + "mergePart", version, INVALID_VERSION);
    ASSERT_TRUE(version.GetSegmentCount() > (size_t)1);
}

void MultiPartitionMergerInteTest::TestOptimizeMergeToOneSegmentMergeTwice()
{
    DoTestMerge(true, false, OPTIMIZE_MERGE_STRATEGY_STR, "", true);
    Version version;
    VersionLoader::GetVersion(GET_TEST_DATA_PATH() + "mergePart", version, INVALID_VERSION);
    // after build: version 0
    // after first merge: version 1
    // after second merge: version 1
    ASSERT_EQ((versionid_t)1, version.GetVersionId());
}

void MultiPartitionMergerInteTest::TestOptimizeMergeToMultiSegmentMergeTwice()
{
    DoTestMerge(true, false, OPTIMIZE_MERGE_STRATEGY_STR, "after-merge-max-doc-count=4", true);
    Version version;
    VersionLoader::GetVersion(GET_TEST_DATA_PATH() + "mergePart", version, INVALID_VERSION);
    // after build: version 0
    // after first merge: version 1
    // after second merge: version 1
    ASSERT_EQ((versionid_t)1, version.GetVersionId());
}

void MultiPartitionMergerInteTest::TestMergeWithEmptyPartitions()
{
    string part1DocString = "";

    string part2DocString = "";

    IndexPartitionOptions options;
    BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
    buildConfig.enablePackageFile = GET_CASE_PARAM();

    Version version;
    VersionLoader versionLoader;
    
    MakeOnePartitionData(mSchema, options, "part1", part1DocString);
    MakeOnePartitionData(mSchema, options, "part2", part2DocString);
    
    ASSERT_NO_THROW(versionLoader.GetVersion(GET_TEST_DATA_PATH() + "part1", version, versionid_t(0)));
    EXPECT_EQ(size_t(0), version.GetSegmentCount());

    ASSERT_NO_THROW(versionLoader.GetVersion(GET_TEST_DATA_PATH() + "part2", version, versionid_t(0)));
    EXPECT_EQ(size_t(0), version.GetSegmentCount());

    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");

    GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";

    options.SetIsOnline(false);
    MultiPartitionMerger multiPartMerger(options, NULL, "");
    multiPartMerger.Merge(mergeSrcs, mergePartPath);

    ASSERT_NO_THROW(versionLoader.GetVersion(GET_TEST_DATA_PATH() + "mergePart", version, versionid_t(0)));
    EXPECT_EQ(size_t(0), version.GetSegmentCount());
}

void MultiPartitionMergerInteTest::TestDiffPartitionHasSamePk()
{
    string part1DocString = "cmd=add,string1=hello,price=0;";
    string part2DocString = "cmd=add,string1=hello,price=1;";

    IndexPartitionOptions options;
    BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
    buildConfig.enablePackageFile = GET_CASE_PARAM();
    // buildConfig.maxDocCount = 2;

    MakeOnePartitionData(mSchema, options, "part1", part1DocString);
    MakeOnePartitionData(mSchema, options, "part2", part2DocString);

    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");

    GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";

    options.SetIsOnline(false);
    MultiPartitionMerger multiPartMerger(options, NULL, "");
    multiPartMerger.Merge(mergeSrcs, mergePartPath);

    PartitionStateMachine psm;
    psm.Init(mSchema, options, mergePartPath);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello", "docid=1,price=1"));
}

void MultiPartitionMergerInteTest::DoTestMerge(
        bool isFullMerge,
        bool isMultiPart,
        const string& mergeStrategy,
        const string& mergeStrategyParam,
        bool isMergeTwice)
{
    string field = "pk:string:pk;long1:uint32;";
    string index = "pk:primarykey64:pk;";
    string attr = "long1;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, "");
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
    buildConfig.enablePackageFile = GET_CASE_PARAM();
    buildConfig.maxDocCount = 3;
    MergeConfig& mergeConfig = options.GetMergeConfig();
    mergeConfig.mergeStrategyStr = mergeStrategy;

    if (mergeStrategyParam.find("|") == string::npos)
    {
        mergeConfig.mergeStrategyParameter.SetLegacyString(mergeStrategyParam);
    }
    else
    {
        vector<string> param = StringUtil::split(mergeStrategyParam, "|", false);
        MergeStrategyParameter parameter;
        parameter.inputLimitParam = param[0];
        parameter.strategyConditions = param[1];
        parameter.outputLimitParam = param[2];
        mergeConfig.mergeStrategyParameter = parameter;
    }

    // before merge
    // part 1
    // seg_0 (1,2,3)
    // seg_1 (1,d2,u3,7,8)
    // part 2
    // seg_0 (11,12,13)
    // seg_1 (21,22,23)
    // seg_2 (11,d12,u13,17,18)

    string part1DocString = "cmd=add,pk=1,long1=1;"
                            "cmd=add,pk=2,long1=2;"
                            "cmd=add,pk=3,long1=3;"
                            "cmd=add,pk=1,long1=4;"
                            "cmd=delete,pk=2;"
                            "cmd=update_field,pk=3,long1=6;"
                            "cmd=add,pk=7,long1=7;"
                            "cmd=add,pk=8,long1=8;";

    string part2DocString = "cmd=add,pk=11,long1=11;"
                            "cmd=add,pk=12,long1=12;"
                            "cmd=add,pk=13,long1=13;"
                            "cmd=add,pk=21,long1=21;"
                            "cmd=add,pk=22,long1=22;"
                            "cmd=add,pk=23,long1=23;"
                            "cmd=add,pk=11,long1=14;"
                            "cmd=delete,pk=12;"
                            "cmd=update_field,pk=13,long1=16;"
                            "cmd=add,pk=17,long1=17;"
                            "cmd=add,pk=18,long1=18;";

    string mergePartPath;
    PartitionMergerPtr merger;
    if (isMultiPart)
    {
        MakeOnePartitionData(schema, options, "part1", part1DocString);
        MakeOnePartitionData(schema, options, "part2", part2DocString);
        vector<string> mergeSrcs;
        mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
        mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");
        GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
        mergePartPath = GET_TEST_DATA_PATH() + "mergePart";
        MultiPartitionMerger multiPartMerger(options, NULL, "");
        merger = multiPartMerger.CreatePartitionMerger(
                mergeSrcs, mergePartPath);
    }
    else
    {
        MakeOnePartitionData(schema, options, "mergePart", part1DocString + part2DocString);
        mergePartPath = GET_TEST_DATA_PATH() + "mergePart";
        merger.reset(PartitionMergerCreator::CreateSinglePartitionMerger(
                        mergePartPath, options, NULL, ""));

    }
    merger->Merge(isFullMerge);

    if (isMergeTwice)
    {
        merger.reset(PartitionMergerCreator::CreateSinglePartitionMerger(
                        mergePartPath, options, NULL, ""));
        merger->Merge(isFullMerge);
    }

    PartitionStateMachine psm;
    psm.Init(schema, options, mergePartPath);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", "long1=6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:7", "long1=7"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:8", "long1=8"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:11", "long1=14"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:12", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:13", "long1=16"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:17", "long1=17"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:18", "long1=18"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:21", "long1=21"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:22", "long1=22"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:23", "long1=23"));
}

void MultiPartitionMergerInteTest::TestMergerMetrics()
{
    string field = "pkstr:string;long1:uint32;";
    string index = "pk:primarykey64:pkstr;";
    string attr = "long1;";
    string summary = "pkstr;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            field, index, attr, summary);
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            "subpkstr:string;sub_long:uint32;",
            "sub_pk:primarykey64:subpkstr",
            "sub_long;", "");
    schema->SetSubIndexPartitionSchema(subSchema);

    string docStrings1 = "cmd=add,pkstr=hello0,long1=0,subpkstr=sub0,sub_long=0;"
                         "cmd=add,pkstr=hello1,long1=1,subpkstr=sub1,sub_long=1;"
                         "cmd=add,pkstr=hello0,modify_fields=long1#sub_long,"
                         "subpkstr=sub0,sub_long=200,sub_modify_fields=sub_long;"
                         "cmd=update_field,pkstr=hello1,subpkstr=sub1,sub_long=11;"
                         "cmd=add,pkstr=hello2,long1=2,subpkstr=sub2,sub_long=2;"
                         "cmd=delete_sub,pkstr=hello2,subpkstr=sub2";
    string docStrings2 = "cmd=add,pkstr=hello3,long1=3,subpkstr=sub3,sub_long=3;"
                         "cmd=add,pkstr=hello4,long1=4,subpkstr=sub4,sub_long=4;"
                         "cmd=add,pkstr=hello3,long1=3,modify_fields=long1#sub_long,"
                         "subpkstr=sub3,sub_long=33,sub_modify_fields=sub_long;"
                         "cmd=update_field,pkstr=hello4,subpkstr=sub4,sub_long=44;"
                         "cmd=add,pkstr=hello5,long1=5,subpkstr=sub5,sub_long=5;"
                         "cmd=delete_sub,pkstr=hello5,subpkstr=sub5";
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
    buildConfig.enablePackageFile = GET_CASE_PARAM();
    buildConfig.maxDocCount = 1;
    MakeOnePartitionData(schema, options, "part1", docStrings1);
    MakeOnePartitionData(schema, options, "part2", docStrings2);

    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");

    GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";

    misc::MetricProviderPtr metricProvider(new misc::MetricProvider);

    MultiPartitionMerger multiPartMerger(options, metricProvider, "");
    MetricPtr progress = metricProvider->DeclareMetric("indexlib/merge/progress",
                                                       kmonitor::STATUS);
    MetricPtr leftItemCount = metricProvider->DeclareMetric("indexlib/merge/leftItemCount",
                                                            kmonitor::STATUS);
    ASSERT_TRUE(progress != nullptr);
    ASSERT_TRUE(leftItemCount != nullptr);
    ASSERT_DOUBLE_EQ(std::numeric_limits<double>::min(), progress->GetValue());
    ASSERT_DOUBLE_EQ(std::numeric_limits<double>::min(), leftItemCount->GetValue());
    multiPartMerger.Merge(mergeSrcs, mergePartPath);
    ASSERT_DOUBLE_EQ(100.0, progress->GetValue());
    ASSERT_DOUBLE_EQ(0, leftItemCount->GetValue());
}

void MultiPartitionMergerInteTest::TestMergeCounters()
{
    string part1DocString = "cmd=add,string1=hello,price=4;"
                            "cmd=add,string1=part1,price=4;";

    string part2DocString = "cmd=add,string1=world,price=4;";

    string part3DocString = "";

    IndexPartitionOptions options;
    BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
    buildConfig.enablePackageFile = GET_CASE_PARAM();
    buildConfig.maxDocCount = 1;

    MakeOnePartitionData(mSchema, options, "part1", part1DocString);
    MakeOnePartitionData(mSchema, options, "part2", part2DocString);
    MakeOnePartitionData(mSchema, options, "part3", part3DocString);

 
    auto OverWriteCounterFile = [this](const string& partPath, segmentid_t segId, int64_t val1, int64_t val2)
    {
        string segName = "segment_" + StringUtil::toString(segId) + "_level_0";
        auto partDir = this->GET_PARTITION_DIRECTORY()->GetDirectory(partPath, true);
        auto segDir = partDir->GetDirectory(segName, true);
        ASSERT_TRUE(segDir->IsExist(COUNTER_FILE_NAME));
        segDir->RemoveFile(COUNTER_FILE_NAME);
        util::CounterMap offlineCounterMap;
        offlineCounterMap.GetStateCounter("offline.test1")->Set(val1); 
        offlineCounterMap.GetAccCounter("offline.test2")->Increase(val2); 
        segDir->Store(COUNTER_FILE_NAME, offlineCounterMap.ToJsonString());    
    };

    OverWriteCounterFile("part1", 1, 10, 11);
    OverWriteCounterFile("part2", 0, 20, 21);

    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part3");    

    GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";

    options.SetIsOnline(false);
    MultiPartitionMerger multiPartMerger(options, NULL, "");
    multiPartMerger.Merge(mergeSrcs, mergePartPath);

    auto mergedSegDir =
        GET_PARTITION_DIRECTORY()->GetDirectory("mergePart/segment_0_level_0", true);

    ASSERT_TRUE(mergedSegDir->IsExist(COUNTER_FILE_NAME));

    util::CounterMap counterMap;
    string counterMapContent;
    mergedSegDir->Load(COUNTER_FILE_NAME, counterMapContent);
    counterMap.FromJsonString(counterMapContent);

    EXPECT_EQ(20, counterMap.GetStateCounter("offline.test1")->Get());
    EXPECT_EQ(32, counterMap.GetAccCounter("offline.test2")->Get());    


}

void MultiPartitionMergerInteTest::TestMergeLocatorAndTimestamp()
{
    string part1DocString = "cmd=add,string1=hello,price=4;"
                            "cmd=add,string1=part1,price=4;";

    string part2DocString = "cmd=add,string1=world,price=4;";

    string part3DocString = "";

    IndexPartitionOptions options;
    BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
    buildConfig.enablePackageFile = GET_CASE_PARAM();
    buildConfig.maxDocCount = 1;

    MakeOnePartitionData(mSchema, options, "part1", part1DocString);
    MakeOnePartitionData(mSchema, options, "part2", part2DocString);
    MakeOnePartitionData(mSchema, options, "part3", part3DocString);

    auto OverWriteSegmentInfoFile =
        [this](const string& partPath, segmentid_t segId, int64_t timestamp, int64_t src, int64_t offset)
        {
            string segName = "segment_" + StringUtil::toString(segId) + "_level_0";
            auto partDir = this->GET_PARTITION_DIRECTORY()->GetDirectory(partPath, true);
            auto segDir = partDir->GetDirectory(segName, true);
            SegmentInfo segInfo;
            segInfo.Load(segDir);
            segDir->RemoveFile(SEGMENT_INFO_FILE_NAME);
            segInfo.timestamp = timestamp;
            IndexLocator indexLocator(src, offset);
            segInfo.locator.SetLocator(indexLocator.toString());
            segInfo.Store(segDir);
        };

    OverWriteSegmentInfoFile("part1", 0, 10, 1, 9);
    OverWriteSegmentInfoFile("part1", 1, 10, 1, 18);
    OverWriteSegmentInfoFile("part2", 0, 20, 1, 10);

    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part3");    

    GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";

    options.SetIsOnline(false);
    MultiPartitionMerger multiPartMerger(options, NULL, "");
    multiPartMerger.Merge(mergeSrcs, mergePartPath);

    auto mergedSegDir =
        GET_PARTITION_DIRECTORY()->GetDirectory("mergePart/segment_0_level_0", true);

    SegmentInfo segInfo;
    segInfo.Load(mergedSegDir);
    EXPECT_EQ((int64_t)20, segInfo.timestamp);

    IndexLocator indexLocator;
    indexLocator.fromString(segInfo.locator.GetLocator());
    EXPECT_EQ(1, indexLocator.getSrc());
    EXPECT_EQ(10, indexLocator.getOffset());
}

void MultiPartitionMergerInteTest::TestMergeWithStopTimestamp()
{
    string part1DocString = "cmd=add,string1=hello,price=4,ts=10;"
                            "cmd=add,string1=part1,price=4,ts=20;"
                            "##stopTs=25;";

    string part2DocString = "cmd=add,string1=world,price=4,ts=30;"
                            "##stopTs=25;";

    string part3DocString = "##stopTs=25;";

    IndexPartitionOptions options;
    BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
    buildConfig.enablePackageFile = GET_CASE_PARAM();
    buildConfig.maxDocCount = 20;

    MakeOnePartitionData(mSchema, options, "part1", part1DocString);
    MakeOnePartitionData(mSchema, options, "part2", part2DocString);
    MakeOnePartitionData(mSchema, options, "part3", part3DocString);

    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part3");    

    GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";

    options.SetIsOnline(false);
    MultiPartitionMerger multiPartMerger(options, NULL, "");
    multiPartMerger.Merge(mergeSrcs, mergePartPath);

    auto partitionDir =
        GET_PARTITION_DIRECTORY()->GetDirectory("mergePart", true);
    Version version;
    VersionLoader::GetVersion(partitionDir, version, INVALID_VERSION);
    EXPECT_EQ((int64_t)25, version.GetTimestamp());
}

IE_NAMESPACE_END(merger);

