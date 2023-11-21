#define INDEX_PARTITION_MERGER_UNITTEST
#include "autil/EnvUtil.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/legacy/json.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/deletionmap/deletion_map_merger.h"
#include "indexlib/index/normal/framework/index_merger.h"
#include "indexlib/index/normal/summary/local_disk_summary_merger.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/merge_strategy/balance_tree_merge_strategy.h"
#include "indexlib/merger/merger_branch_hinter.h"
#include "indexlib/merger/multi_threaded_merge_scheduler.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/merger/test/merge_task_maker.h"
#include "indexlib/merger/test/merge_work_item_creator_mock.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Timer.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace fslib;
using namespace fslib::fs;
using namespace testing;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::merger;
using namespace indexlib::test;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::index;
using namespace indexlib::util;

namespace indexlib { namespace merger {

class IndexPartitionMergerMock : public IndexPartitionMerger
{
public:
    IndexPartitionMergerMock(const SegmentDirectoryPtr& dir, const config::IndexPartitionSchemaPtr& schema,
                             const IndexPartitionOptions& options)
        : IndexPartitionMerger(dir, schema, options, merger::DumpStrategyPtr(), NULL, plugin::PluginManagerPtr(),
                               CommonBranchHinterOption::Test())
    {
    }

    virtual ~IndexPartitionMergerMock() {}

    void SetMergeTask(const MergeTask& task) { mTask = task; }
    void MakeOnePartitionData(const config::IndexPartitionSchemaPtr& schema,
                              const config::IndexPartitionOptions& options, const std::string& partRootDir,
                              const std::string& docStrs);

protected:
    ReclaimMapPtr CreateSortByWeightReclaimMap(const SegmentMergeInfos& segMergeInfos)
    {
        return ReclaimMapPtr(new ReclaimMap());
    }

    MergeWorkItemCreatorPtr CreateMergeWorkItemCreator(bool optimize, const IndexMergeMeta& mergeMeta,
                                                       const MergeFileSystemPtr& mergeFileSystem) override
    {
        return MergeWorkItemCreatorPtr(new MergeWorkItemCreatorMock(mSchema, mMergeConfig, &mergeMeta,
                                                                    mSegmentDirectory, SegmentDirectoryPtr(),
                                                                    IsSortMerge(), mOptions, mergeFileSystem));
    }

    virtual MergeTask CreateMergeTaskByStrategy(bool optimize, const config::MergeConfig& mergeConfig,
                                                const SegmentMergeInfos& segMergeInfos,
                                                const indexlibv2::framework::LevelInfo& levelInfo) const override
    {
        return mTask;
    }

private:
    MergeTask mTask;
};

// not delete commit files and progress
class FakeMergerBranchHinter : public MergerBranchHinter
{
    FakeMergerBranchHinter() : MergerBranchHinter(CommonBranchHinterOption::Test()) {}

private:
    virtual void GetClearFile(const std::string&, const std::string&, std::vector<std::string>&) override {}
};

class IndexPartitionMergerTest : public INDEXLIB_TESTBASE
{
public:
    IndexPartitionMergerTest() {}

    DECLARE_CLASS_NAME(IndexPartitionMergerTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEMP_DATA_PATH();
        mOptions.SetIsOnline(false);
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mSchema,
                                         "string1:string;long1:uint32", // above is field schema
                                         "stringI:string:string1;",     // above is index schema
                                         "long1",                       // above is attribute schema
                                         "string1");                    // above is summary schema
        RESET_FILE_SYSTEM("ut", false, FileSystemOptions::Offline());
    }

    void CaseTearDown() override {}

    void TestCaseForMerge()
    {
        string versionStr = "1,1024,2,2000,3,1024,4,800,5,50";
        string mergeTaskStr = "1,2;4,5";
        InternalTestCaseForMerge(versionStr, mergeTaskStr);
    }

    void TestCaseForMergeWithEmptyInstance()
    {
        SingleFieldPartitionDataProvider provider;
        provider.Init(mRootDir, "int32", SFP_PK_INDEX);
        provider.Build("1,2,3#4,5,6#7,delete:4,delete:5,delete:6", SFP_OFFLINE);
        IndexPartitionOptions options = provider.GetOptions();
        MergeConfig& mergeConfig = options.GetMergeConfig();
        mergeConfig.mergeStrategyStr = SPECIFIC_SEGMENTS_MERGE_STRATEGY_STR;
        mergeConfig.mergeStrategyParameter.SetLegacyString("merge_segments=1");

        BuildConfig& buildConfig = options.GetBuildConfig();
        buildConfig.keepVersionCount = 100;
        IndexPartitionMergerPtr merger((IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
            mRootDir, options, NULL, ""));
        MergeMetaPtr mergeMeta = merger->CreateMergeMeta(false, 3, 0);
        merger->DoMerge(false, mergeMeta, 1);
        merger->DoMerge(false, mergeMeta, 0);
        merger->DoMerge(false, mergeMeta, 2);
        merger->EndMerge(mergeMeta);
        OnlinePartition partition;
        partition.Open(mRootDir, "", provider.GetSchema(), options);
        IndexPartitionReaderPtr reader = partition.GetReader();
        PrimaryKeyIndexReaderPtr pkReader = reader->GetPrimaryKeyReader();
        ASSERT_EQ((docid_t)0, pkReader->Lookup("1"));
        ASSERT_EQ(INVALID_DOCID, pkReader->Lookup("4"));
        ASSERT_EQ(INVALID_DOCID, pkReader->Lookup("5"));
        ASSERT_EQ(INVALID_DOCID, pkReader->Lookup("6"));
        ASSERT_EQ((docid_t)3, pkReader->Lookup("7"));
    }

    void TestCaseForMergeWithDeployIndex()
    {
        string versionStr = "1,1024,2,2000,3,1024,4,800,5,50";
        string mergeTaskStr = "1,2;4,5";
        InternalTestCaseForMerge(versionStr, mergeTaskStr);
    }

    void TestCaseForMergeMultiVersion()
    {
        string versionStr = "1,1024,2,2000;3,1024,4,800,5,50,6,100,7,200";
        string mergeTaskStr = "3,4,5;6,7";
        InternalTestCaseForMerge(versionStr, mergeTaskStr);
    }

    void TestCaseForCachedSegmentInfo()
    {
        string versionStr = "1,1024,2,2000,3,1024,4,800,5,50";
        string mergeTaskStr = "1,2;4,5";

        Version lastVersion;
        SegmentMergeInfos segMergeInfos;
        MakeFakeSegmentDir(versionStr, lastVersion, segMergeInfos);

        SegmentDirectoryPtr segDir(new SegmentDirectory(GET_PARTITION_DIRECTORY(), lastVersion));
        segDir->Init(false, true);
        vector<uint32_t> docCounts;
        for (size_t i = 0; i < segMergeInfos.size(); ++i) {
            docCounts.push_back((uint32_t)segMergeInfos[i].segmentInfo.docCount);
        }

        IndexPartitionMergerMock merger(segDir, mSchema, mOptions);

        stringstream ss;
        ss << mRootDir << lastVersion.GetSegmentDirName(2) << "/" << SEGMENT_INFO_FILE_NAME;
        FslibWrapper::DeleteDirE(ss.str(), DeleteOption::NoFence(false));

        Version mergedVersion;
        SegmentInfos mergedSegInfos;
        MergeTask task;
        MakeMergeTask(mergeTaskStr, segMergeInfos, task, mergedVersion, mergedSegInfos);
        merger.SetMergeTask(task);
        ASSERT_NO_THROW(merger.Merge());
    }

    void TestCaseForInvalidMergePlan()
    {
        string versionStr = "1,1024,2,2000;3,1024,4,800,5,50";

        Version lastVersion;
        SegmentMergeInfos segMergeInfos;
        MakeFakeSegmentDir(versionStr, lastVersion, segMergeInfos);
        SegmentDirectoryPtr segDir(new SegmentDirectory(GET_PARTITION_DIRECTORY(), lastVersion));
        segDir->Init(false, true);

        vector<uint32_t> docCounts;
        for (size_t i = 0; i < segMergeInfos.size(); ++i) {
            docCounts.push_back((uint32_t)segMergeInfos[i].segmentInfo.docCount);
        }

        IndexPartitionMergerMock merger(segDir, mSchema, mOptions);
        MergePlan plan;
        SegmentMergeInfo segMergeInfo;
        segMergeInfo.segmentId = 2;
        plan.AddSegment(segMergeInfo);
        plan.AddSegment(segMergeInfos[1]);
        MergeTask task;
        task.AddPlan(plan);

        merger.SetMergeTask(task);
        INDEXLIB_EXPECT_EXCEPTION(merger.Merge(), BadParameterException);
    }

    void TestCaseForCreateMergeScheduler()
    {
        IndexPartitionOptions simpleOptions = mOptions;
        simpleOptions.GetMergeConfig().mergeThreadCount = 1;
        Version lastVersion(INVALID_VERSIONID);
        SegmentDirectoryPtr segDir(new SegmentDirectory(GET_PARTITION_DIRECTORY(), lastVersion));
        segDir->Init(false, true);

        IndexPartitionMergerPtr merger;
        merger.reset(new IndexPartitionMerger(segDir, mSchema, simpleOptions, merger::DumpStrategyPtr(), NULL,
                                              plugin::PluginManagerPtr(), CommonBranchHinterOption::Test()));
        MergeSchedulerPtr scheduler = merger->CreateMergeScheduler(simpleOptions.GetMergeConfig().maxMemUseForMerge,
                                                                   simpleOptions.GetMergeConfig().mergeThreadCount);

        MultiThreadedMergeSchedulerPtr mulThreadedScheduler =
            dynamic_pointer_cast<MultiThreadedMergeScheduler>(scheduler);
        INDEXLIB_TEST_TRUE(mulThreadedScheduler != NULL);
        ASSERT_EQ((size_t)1, mulThreadedScheduler->GetThreadNum());

        IndexPartitionOptions mulThreadedOptions = mOptions;
        mulThreadedOptions.GetMergeConfig().mergeThreadCount = 3;
        merger.reset(new IndexPartitionMerger(segDir, mSchema, mulThreadedOptions, merger::DumpStrategyPtr(), NULL,
                                              plugin::PluginManagerPtr(), CommonBranchHinterOption::Test()));
        scheduler = merger->CreateMergeScheduler(mulThreadedOptions.GetMergeConfig().maxMemUseForMerge,
                                                 mulThreadedOptions.GetMergeConfig().mergeThreadCount);
        mulThreadedScheduler = dynamic_pointer_cast<MultiThreadedMergeScheduler>(scheduler);
        INDEXLIB_TEST_TRUE(mulThreadedScheduler != NULL);
        ASSERT_EQ((size_t)3, mulThreadedScheduler->GetThreadNum());
    }

    void TestCaseForGetInstanceTempMergeDir()
    {
        EXPECT_EQ(string("/root/instance_2"), IndexPartitionMerger::GetInstanceMergeDir("/root", 2));
    }

    void TestCaseForSplitFileName()
    {
        string folder, fileName;
        {
            // normal
            IndexPartitionMerger::SplitFileName("/a/b/c", folder, fileName);
            EXPECT_EQ(string("/a/b"), folder);
            EXPECT_EQ(string("c"), fileName);
        }
        {
            // end with dir
            IndexPartitionMerger::SplitFileName("/a/b/c/", folder, fileName);
            EXPECT_EQ(string("/a/b/c"), folder);
            EXPECT_EQ(string(), fileName);
        }
        {
            // multi /
            IndexPartitionMerger::SplitFileName("///a///b//c", folder, fileName);
            EXPECT_EQ(string("///a///b/"), folder);
            EXPECT_EQ(string("c"), fileName);
        }
    }

    void TestCaseForPrepareMergeDir()
    {
        uint32_t instanceId = 2;
        IndexMergeMetaPtr mergeMeta(new IndexMergeMeta());
        MergePlan emptyMergePlan;
        SegmentInfo emptySegmentInfo;
        SegmentMergeInfos emtpySegmentMergeInfos;
        Version targetVersion(0);
        mergeMeta->TEST_AddMergePlanMeta(emptyMergePlan, 0, emptySegmentInfo, emptySegmentInfo, emtpySegmentMergeInfos,
                                         emtpySegmentMergeInfos);
        mergeMeta->TEST_AddMergePlanMeta(emptyMergePlan, 2, emptySegmentInfo, emptySegmentInfo, emtpySegmentMergeInfos,
                                         emtpySegmentMergeInfos);
        mergeMeta->TEST_AddMergePlanMeta(emptyMergePlan, 4, emptySegmentInfo, emptySegmentInfo, emtpySegmentMergeInfos,
                                         emtpySegmentMergeInfos);
        targetVersion.AddSegment(0);
        targetVersion.AddSegment(2);
        targetVersion.AddSegment(4);
        mergeMeta->SetTargetVersion(targetVersion);
        FslibWrapper::MkDirE(mRootDir + "/instance_2");
        FslibWrapper::AtomicStoreE(mRootDir + "/instance_2/data", "");
        SegmentDirectoryPtr segDir(
            new SegmentDirectory(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSIONID)));
        segDir->Init(false, true);
        IndexPartitionMergerMock merger(segDir, mSchema, mOptions);
        merger.CreateMergeFileSystem(instanceId, mergeMeta);
        FileList list;
        // instance2 is the last instance
        string branchRootDir =
            util::PathUtil::JoinPath(mRootDir, "instance_2", merger.TEST_GetBranchFs()->GetBranchName());
        FslibWrapper::ListDirE(branchRootDir, list);
        EXPECT_THAT(list, UnorderedElementsAre("check_points", "segment_0_level_0", "segment_2_level_0",
                                               "segment_4_level_0", "entry_table.preload"));
    }

    void TestCaseForMergeInstanceDir()
    {
        vector<MergePlan> mergePlans;
        Version targetVersion;
        MergePlan planMeta2;
        planMeta2.SetTargetSegmentId(0, 2);
        planMeta2.GetTargetSegmentInfo(0).docCount = 1000;
        planMeta2.GetSubTargetSegmentInfo(0).docCount = 2000;
        mergePlans.push_back(planMeta2);
        targetVersion.AddSegment(2);
        MergePlan planMeta4;
        planMeta4.SetTargetSegmentId(0, 4);
        planMeta4.GetTargetSegmentInfo(0).docCount = 100;
        planMeta4.GetSubTargetSegmentInfo(0).docCount = 200;
        mergePlans.push_back(planMeta4);
        targetVersion.AddSegment(4);

        prepareOneInstanceDir(GET_PARTITION_DIRECTORY(), 1);
        prepareOneInstanceDir(GET_PARTITION_DIRECTORY(), 3);

        SegmentDirectoryPtr segDir(
            new SegmentDirectory(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSIONID)));
        IndexPartitionOptions options;
        IndexPartitionMerger merger(segDir, mSchema, options, merger::DumpStrategyPtr(), NULL,
                                    plugin::PluginManagerPtr(), CommonBranchHinterOption::Test());

        auto fs = FileSystemCreator::Create("TestCaseForMergeInstanceDir", mRootDir, FileSystemOptions::Offline())
                      .GetOrThrow();
        const file_system::DirectoryPtr& rootDirectory = file_system::Directory::Get(fs);
        merger.MergeInstanceDir(rootDirectory, mergePlans, targetVersion, "{}", true);
        ASSERT_EQ(FSEC_OK, fs->TEST_Commit(0));

        FileList list;
        FileSystemTestUtil::CleanEntryTables(mRootDir);
        FslibWrapper::ListDirE(mRootDir, list);
        EXPECT_THAT(list, UnorderedElementsAre("segment_2_level_0", "segment_4_level_0", "adaptive_bitmap_meta"));
        vector<string> segmentDirs;
        segmentDirs.push_back(mRootDir + "segment_2_level_0/");
        segmentDirs.push_back(mRootDir + "segment_4_level_0/");

        for (size_t i = 0; i < segmentDirs.size(); i++) {
            FslibWrapper::ListDirE(segmentDirs[i], list);
            EXPECT_THAT(list, UnorderedElementsAre(string("index"), string("summary"), string("attribute"),
                                                   string("segment_info"), COUNTER_FILE_NAME, string("sub_segment"),
                                                   string("segment_metrics")));

            FslibWrapper::ListDirE(segmentDirs[i] + "index/index1", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("dict"), string("posting")));
            FslibWrapper::ListDirE(segmentDirs[i] + "index/index3", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("dict"), string("posting")));
            FslibWrapper::ListDirE(segmentDirs[i] + "attribute/attr1", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("data"), string("offset")));
            FslibWrapper::ListDirE(segmentDirs[i] + "attribute/attr3", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("data"), string("offset")));
            // TODO: sub
            string subSegmentPath = segmentDirs[i] + "/" + SUB_SEGMENT_DIR_NAME + "/";
            FslibWrapper::ListDirE(subSegmentPath, list);
            EXPECT_THAT(list, UnorderedElementsAre(string("index"), string("summary"), string("attribute"),
                                                   string("segment_info"), COUNTER_FILE_NAME));
            FslibWrapper::ListDirE(subSegmentPath + "index/index1", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("dict"), string("posting")));
            FslibWrapper::ListDirE(subSegmentPath + "index/index3", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("dict"), string("posting")));
            FslibWrapper::ListDirE(subSegmentPath + "attribute/attr1", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("data"), string("offset")));
            FslibWrapper::ListDirE(subSegmentPath + "attribute/attr3", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("data"), string("offset")));
        }
    }

    void TestCaseForDeployIndexFail()
    {
        Version lastVersion;
        SegmentDirectoryPtr segDir(new SegmentDirectory(GET_PARTITION_DIRECTORY(), lastVersion));
        segDir->Init(false, true);
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        IndexPartitionMerger merger(segDir, mSchema, options, merger::DumpStrategyPtr(), NULL,
                                    plugin::PluginManagerPtr(), CommonBranchHinterOption::Test());
        vector<MergePlan> mergePlans;
        MergePlan plan;
        plan.SetTargetSegmentId(0, 2);
        mergePlans.push_back(plan);

        // segment_2_tmp not exist
        Version targetVersion(0);
        targetVersion.AddSegment(2);
        ASSERT_THROW(merger.SinglePartEndMerge(GET_PARTITION_DIRECTORY(), mergePlans, targetVersion), FileIOException);
    }

    void TestMergeWithTemperature()
    {
        string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature.json";
        string jsonString;
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
        FromJsonString(*schema, jsonString);
        IndexPartitionOptions options;
        options.GetOfflineConfig().buildConfig.maxDocCount = 2;
        options.GetOfflineConfig().buildConfig.enablePackageFile = false;
        MergeConfig& mergeConfig = options.GetMergeConfig();
        mergeConfig.mergeStrategyStr = "temperature";
        mergeConfig.mergeStrategyParameter.outputLimitParam = "max_hot_segment_size=1024";
        mergeConfig.mergeStrategyParameter.strategyConditions =
            "priority-feature=temperature_conflict;select-sequence=HOT,WARM,COLD";

        mergeConfig.SetCalculateTemperature(true);
        std::string splitConfigStr = "{\"class_name\":\"temperature\",\"parameters\":{}}";
        autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), splitConfigStr);
        PartitionStateMachine psm;
        string rootPath = GET_TEMP_DATA_PATH();
        ASSERT_TRUE(psm.Init(schema, options, rootPath));
        uint64_t currentTime = util::Timer::GetCurrentTime();
        string docStrings = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 1) +
                            ";" // hot
                            "cmd=add,pk=2,status=1,time=" +
                            StringUtil::toString(currentTime - 3000) +
                            ";" // warm
                            "cmd=add,pk=3,status=0,time=" +
                            StringUtil::toString(currentTime - 20000) +
                            ";" // cold
                            "cmd=add,pk=4,status=0,time=" +
                            StringUtil::toString(currentTime - 240000) + ";"; // cold
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
        Version version;
        VersionLoader::GetVersionS(rootPath, version, 0);
        ASSERT_EQ(version.GetSegmentCount(), version.GetSegTemperatureMetas().size());
        SegmentTemperatureMeta meta;
        ASSERT_TRUE(version.GetSegmentTemperatureMeta(0, meta));
        ASSERT_EQ("HOT", meta.segTemperature);
        ASSERT_EQ("HOT:1;WARM:1;COLD:0", meta.segTemperatureDetail);
        ASSERT_TRUE(version.GetSegmentTemperatureMeta(1, meta));
        ASSERT_EQ("COLD", meta.segTemperature);
        ASSERT_EQ("HOT:0;WARM:0;COLD:2", meta.segTemperatureDetail);

        string incDocs = "cmd=update_field,pk=2,status=0;" // hot
                         "cmd=add,pk=5,status=1,time=" +
                         StringUtil::toString(currentTime - 3) +
                         ";" // hot
                         "cmd=delete,pk=1;"
                         "cmd=add,pk=6,status=1,time=" +
                         StringUtil::toString(currentTime - 3000) + ";"; // warm
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "", ""));
        VersionLoader::GetVersionS(rootPath, version, 2);
        ASSERT_EQ(3, version.GetSegTemperatureMetas().size());
        ASSERT_TRUE(version.GetSegmentTemperatureMeta(3, meta));
        ASSERT_EQ("HOT", meta.segTemperature);
        ASSERT_EQ("HOT:2;WARM:0;COLD:0", meta.segTemperatureDetail);
        ASSERT_TRUE(version.GetSegmentTemperatureMeta(4, meta));
        ASSERT_EQ("COLD", meta.segTemperature);
        ASSERT_EQ("HOT:0;WARM:0;COLD:2", meta.segTemperatureDetail);
        ASSERT_TRUE(version.GetSegmentTemperatureMeta(5, meta));
        ASSERT_EQ("WARM", meta.segTemperature);
        ASSERT_EQ("HOT:0;WARM:1;COLD:0", meta.segTemperatureDetail);
    }

    void MakeOnePartitionData(const config::IndexPartitionSchemaPtr& schema,
                              const config::IndexPartitionOptions& options, const std::string& partRootDir,
                              const std::string& docStrs)
    {
        string rootDir = GET_TEMP_DATA_PATH() + partRootDir;
        PartitionStateMachine psm;
        psm.Init(schema, options, rootDir, "psm", "");
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrs, "", ""));
    }

    void TestFullMergeWithTemperature()
    {
        string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature.json";
        string jsonString;
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
        FromJsonString(*schema, jsonString);
        uint64_t currentTime = util::Timer::GetCurrentTime();
        string part1DocString = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 10) +
                                ";" // hot
                                "cmd=add,pk=2,status=1,time=" +
                                StringUtil::toString(currentTime - 5000) +
                                ";"
                                "cmd=add,pk=3,status=1,time=" +
                                StringUtil::toString(currentTime - 5000) +
                                ";"
                                "cmd=add,pk=4,status=1,time=" +
                                StringUtil::toString(currentTime - 5000) + ";";

        string part2DocString = "cmd=add,pk=3,status=1,time=" + StringUtil::toString(currentTime - 1000000) +
                                ";" // hot
                                "cmd=add,pk=4,status=0,time=" +
                                StringUtil::toString(currentTime - 200000000) + ";"; // cold

        string part3DocString = "";

        IndexPartitionOptions options;
        options.SetIsOnline(false);
        BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
        buildConfig.maxDocCount = 1;

        options.GetMergeConfig().mergeStrategyStr = "optimize";
        options.GetMergeConfig().mergeStrategyParameter.SetLegacyString("after-merge-max-segment-count=2");
        options.GetMergeConfig().SetCalculateTemperature(true);
        std::string splitConfigStr = "{\"class_name\":\"temperature\",\"parameters\":{}}";
        autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), splitConfigStr);

        MakeOnePartitionData(schema, options, "part1", part1DocString);
        string rootDir = GET_TEMP_DATA_PATH() + "part1";
        PartitionStateMachine psmBuild;
        string doc = "cmd=update_field,pk=1,status=1,time=" + StringUtil::toString(currentTime - 5000) + ";";
        psmBuild.Init(schema, options, rootDir, "psm", "");
        ASSERT_TRUE(psmBuild.Transfer(BUILD_FULL_NO_MERGE, doc, "", ""));

        MakeOnePartitionData(schema, options, "part2", part2DocString);
        MakeOnePartitionData(schema, options, "part3", part3DocString);

        vector<string> mergeSrcs;
        mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part1");
        mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part2");
        mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part3");
        string mergePartPath = GET_TEMP_DATA_PATH() + "mergePart";

        options.SetIsOnline(false);
        IndexPartitionMergerPtr partitionMerger(
            (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(
                mergeSrcs, mergePartPath, options, NULL, ""));

        ASSERT_NO_THROW(partitionMerger->PrepareMerge(0));
        MergeMetaPtr mergeMeta = partitionMerger->CreateMergeMeta(true, 1, 0);
        ASSERT_EQ(3, mergeMeta->GetTargetVersion().GetSegTemperatureMetas().size());
        partitionMerger->DoMerge(true, mergeMeta, 0);
        partitionMerger->EndMerge(mergeMeta, 0);
        PartitionStateMachine psm;
        psm.Init(schema, options, mergePartPath);
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "status=1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "status=1"));
        index_base::Version vs;
        index_base::VersionLoader::GetVersionS(mergePartPath, vs, 0);

        ASSERT_EQ(3, vs.GetSegmentCount());
        ASSERT_EQ(3, vs.GetSegTemperatureMetas().size());
    }

    void TestFullMergeWithNoRecaTemperature()
    {
        string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature.json";
        string jsonString;
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
        FromJsonString(*schema, jsonString);
        uint64_t currentTime = util::Timer::GetCurrentTime();
        string part1DocString = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 10) +
                                ";" // hot
                                "cmd=add,pk=2,status=1,time=" +
                                StringUtil::toString(currentTime - 5000) +
                                ";"
                                "cmd=add,pk=3,status=1,time=" +
                                StringUtil::toString(currentTime - 5000) +
                                ";"
                                "cmd=add,pk=4,status=1,time=" +
                                StringUtil::toString(currentTime - 5000) + ";";

        string part2DocString = "cmd=add,pk=3,status=1,time=" + StringUtil::toString(currentTime - 1000000) +
                                ";" // hot
                                "cmd=add,pk=4,status=0,time=" +
                                StringUtil::toString(currentTime - 200000000) + ";"; // cold

        string part3DocString = "";

        IndexPartitionOptions options;
        options.SetIsOnline(false);
        BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
        buildConfig.maxDocCount = 1;
        buildConfig.enablePackageFile = false;

        options.GetMergeConfig().mergeStrategyStr = "optimize";
        options.GetMergeConfig().mergeStrategyParameter.SetLegacyString("after-merge-max-segment-count=2");
        std::string splitConfigStr = "{\"class_name\":\"temperature\",\"parameters\":{}}";
        autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), splitConfigStr);

        MakeOnePartitionData(schema, options, "part1", part1DocString);
        string rootDir = GET_TEMP_DATA_PATH() + "part1";
        PartitionStateMachine psmBuild;
        string doc = "cmd=update_field,pk=1,status=1,time=" + StringUtil::toString(currentTime - 5000) + ";";
        psmBuild.Init(schema, options, rootDir, "psm", "");
        ASSERT_TRUE(psmBuild.Transfer(BUILD_FULL_NO_MERGE, doc, "", ""));

        MakeOnePartitionData(schema, options, "part2", part2DocString);
        MakeOnePartitionData(schema, options, "part3", part3DocString);

        vector<string> mergeSrcs;
        mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part1");
        mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part2");
        mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part3");
        string mergePartPath = GET_TEMP_DATA_PATH() + "mergePart";

        options.SetIsOnline(false);
        IndexPartitionMergerPtr partitionMerger(
            (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(
                mergeSrcs, mergePartPath, options, NULL, ""));

        ASSERT_NO_THROW(partitionMerger->PrepareMerge(0));
        MergeMetaPtr mergeMeta = partitionMerger->CreateMergeMeta(true, 1, 0);
        ASSERT_EQ(3, mergeMeta->GetTargetVersion().GetSegTemperatureMetas().size());
        partitionMerger->DoMerge(true, mergeMeta, 0);
        partitionMerger->EndMerge(mergeMeta, 0);
        PartitionStateMachine psm;
        psm.Init(schema, options, mergePartPath);
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "status=1"));
        // ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "status=1"));
        index_base::Version vs;
        index_base::VersionLoader::GetVersionS(mergePartPath, vs, 0);

        ASSERT_EQ(vs.GetSegmentCount(), vs.GetSegTemperatureMetas().size());
    }

    void TestMergeForRecalculatorMetric()
    {
        string configFile = GET_PRIVATE_TEST_DATA_PATH() + "temperature_schema_use_merge.json";
        string jsonString;
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
        FromJsonString(*schema, jsonString);
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        options.GetOfflineConfig().buildConfig.enablePackageFile = false;
        MergeConfig& mergeConfig = options.GetMergeConfig();
        mergeConfig.mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
        mergeConfig.SetCalculateTemperature(true);
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
        uint64_t currentTime = util::Timer::GetCurrentTime();
        string docStrings = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 1) + ";"; // hot
        try {
            ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
        } catch (...) {
            ASSERT_TRUE(false);
        }
        IndexPartitionMergerPtr merger((IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
            GET_TEMP_DATA_PATH(), options, NULL, ""));
        vector<string> temperature = {"HOT", "WARM", "COLD"};
        vector<string> detail = {"HOT:1;WARM:0;COLD:0", "HOT:0;WARM:1;COLD:0", "HOT:0;WARM:0;COLD:1"};
        int64_t currsor = -1;
        int sleepCount = 0;
        while (true) {
            sleepCount++;
            sleep(1);
            if (currsor == 2) {
                break;
            }
            if (sleepCount > 15) {
                ASSERT_TRUE(false) << currsor;
                break;
            }
            string getKey = "temperature";
            try {
                MergeMetaPtr mergeMeta = merger->CreateMergeMeta(false, 1, 0);
                IndexMergeMetaPtr indexMergeMeta = DYNAMIC_POINTER_CAST(IndexMergeMeta, mergeMeta);
                const index_base::SegmentMergeInfos& segmentInfos = indexMergeMeta->GetMergeSegmentInfo(0);
                ASSERT_EQ(1, segmentInfos.size());
                json::JsonMap jsonMap;
                jsonMap = segmentInfos[0]
                              .segmentMetrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP)
                              ->Get<json::JsonMap>(getKey);
                if (AnyCast<string>(jsonMap[LIFECYCLE]) == temperature[currsor + 1]) {
                    ASSERT_EQ(detail[currsor + 1], AnyCast<string>(jsonMap[LIFECYCLE_DETAIL]));
                    currsor++;
                }
            } catch (...) {
                ASSERT_FALSE(true);
            }
        }
        ASSERT_EQ(2, currsor);
    }

    void TestMergeForRecalculatorMetricWithDelete()
    {
        string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature.json";
        string jsonString;
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
        FromJsonString(*schema, jsonString);
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        options.GetOfflineConfig().buildConfig.enablePackageFile = false;
        MergeConfig& mergeConfig = options.GetMergeConfig();
        mergeConfig.mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
        mergeConfig.SetCalculateTemperature(true);
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
        uint64_t currentTime = util::Timer::GetCurrentTime();
        string docStrings = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 1) +
                            ";" // hot
                            "cmd=add,pk=2,status=1,time=" +
                            StringUtil::toString(currentTime - 5000) + ";"; // warm
        try {
            ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
        } catch (...) {
            ASSERT_TRUE(false);
        }
        IndexPartitionMergerPtr merger((IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
            GET_TEMP_DATA_PATH(), options, NULL, ""));
        vector<string> temperature = {"HOT", "WARM"};
        vector<string> detail = {"HOT:2;WARM:0;COLD:0", "HOT:0;WARM:2;COLD:0"};

        MergeMetaPtr mergeMeta = merger->CreateMergeMeta(false, 1, 0);
        IndexMergeMetaPtr indexMergeMeta = DYNAMIC_POINTER_CAST(IndexMergeMeta, mergeMeta);
        index_base::SegmentMergeInfos segmentInfos = indexMergeMeta->GetMergeSegmentInfo(0);
        ASSERT_EQ(1, segmentInfos.size());
        json::JsonMap jsonMap;
        string getKey = "temperature";
        jsonMap = segmentInfos[0]
                      .segmentMetrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP)
                      ->Get<json::JsonMap>(getKey);
        ASSERT_EQ("HOT", AnyCast<string>(jsonMap[LIFECYCLE]));
        ASSERT_EQ("HOT:1;WARM:1;COLD:0", AnyCast<string>(jsonMap[LIFECYCLE_DETAIL]));
        string incDocs = "cmd=delete,pk=1;"; // hot
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "pk:1", ""));

        merger.reset((IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
            GET_TEMP_DATA_PATH(), options, NULL, ""));
        mergeMeta = merger->CreateMergeMeta(false, 1, 0);
        indexMergeMeta = DYNAMIC_POINTER_CAST(IndexMergeMeta, mergeMeta);
        segmentInfos = indexMergeMeta->GetMergeSegmentInfo(0);
        ASSERT_EQ(2, segmentInfos.size());
        jsonMap = segmentInfos[0]
                      .segmentMetrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP)
                      ->Get<json::JsonMap>(getKey);
        ASSERT_EQ("WARM", AnyCast<string>(jsonMap[LIFECYCLE]));
        ASSERT_EQ("HOT:0;WARM:1;COLD:0", AnyCast<string>(jsonMap[LIFECYCLE_DETAIL]));
        jsonMap = segmentInfos[1]
                      .segmentMetrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP)
                      ->Get<json::JsonMap>(getKey);
        ASSERT_EQ("HOT", AnyCast<string>(jsonMap[LIFECYCLE]));
        ASSERT_EQ("HOT:0;WARM:0;COLD:0", AnyCast<string>(jsonMap[LIFECYCLE_DETAIL]));
    }

    void TestMergeForRecalculatorMetricWithUpdate()
    {
        string configFile = GET_PRIVATE_TEST_DATA_PATH() + "temperature_schema_use_merge2.json";
        string jsonString;
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
        FromJsonString(*schema, jsonString);
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        options.GetOfflineConfig().buildConfig.enablePackageFile = false;
        MergeConfig& mergeConfig = options.GetMergeConfig();
        mergeConfig.mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
        mergeConfig.SetCalculateTemperature(true);
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
        uint64_t currentTime = util::Timer::GetCurrentTime();
        string docStrings = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 1) +
                            ";" // hot
                            "cmd=add,pk=2,status=1,time=" +
                            StringUtil::toString(currentTime - 4) + ";"; // warm
        try {
            ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
        } catch (...) {
            ASSERT_TRUE(false);
        }
        IndexPartitionMergerPtr merger((IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
            GET_TEMP_DATA_PATH(), options, NULL, ""));

        MergeMetaPtr mergeMeta = merger->CreateMergeMeta(false, 1, 0);
        IndexMergeMetaPtr indexMergeMeta = DYNAMIC_POINTER_CAST(IndexMergeMeta, mergeMeta);
        index_base::SegmentMergeInfos segmentInfos = indexMergeMeta->GetMergeSegmentInfo(0);
        ASSERT_EQ(1, segmentInfos.size());
        json::JsonMap jsonMap;
        string getKey = "temperature";
        jsonMap = segmentInfos[0]
                      .segmentMetrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP)
                      ->Get<json::JsonMap>(getKey);
        ASSERT_EQ("HOT", AnyCast<string>(jsonMap[LIFECYCLE]));
        ASSERT_EQ("HOT:1;WARM:1;COLD:0", AnyCast<string>(jsonMap[LIFECYCLE_DETAIL]));
        int acc = 0;
        auto CheckValue = [&](string expectTemperature, string expectTemperatureDetail, int32_t expectSegmentCount) {
            while (true) {
                merger.reset((IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
                    GET_TEMP_DATA_PATH(), options, NULL, ""));
                mergeMeta = merger->CreateMergeMeta(false, 1, 0);
                indexMergeMeta = DYNAMIC_POINTER_CAST(IndexMergeMeta, mergeMeta);
                segmentInfos = indexMergeMeta->GetMergeSegmentInfo(0);
                ASSERT_EQ(expectSegmentCount, segmentInfos.size());
                int segmentIdx = 1;
                while (segmentIdx < expectSegmentCount) {
                    jsonMap = segmentInfos[segmentIdx]
                                  .segmentMetrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP)
                                  ->Get<json::JsonMap>(getKey);
                    ASSERT_EQ("HOT", AnyCast<string>(jsonMap[LIFECYCLE]));
                    ASSERT_EQ("HOT:0;WARM:0;COLD:0", AnyCast<string>(jsonMap[LIFECYCLE_DETAIL]));
                    segmentIdx++;
                }
                jsonMap = segmentInfos[0]
                              .segmentMetrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP)
                              ->Get<json::JsonMap>(getKey);
                if (AnyCast<string>(jsonMap[LIFECYCLE]) == expectTemperature &&
                    AnyCast<string>(jsonMap[LIFECYCLE_DETAIL]) == expectTemperatureDetail) {
                    break;
                }
                acc++;
                if (acc == 10) {
                    ASSERT_EQ("TIMEOUT", "");
                }
                sleep(1);
            }
        };

        string incDocs = "cmd=update_field,pk=1,status=0;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));
        CheckValue("WARM", "HOT:0;WARM:1;COLD:1", 2);

        string incDocs2 = "cmd=update_field,pk=2,status=0;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs2, "", ""));
        acc = 0;
        CheckValue("COLD", "HOT:0;WARM:0;COLD:2", 3);
    }

    void TestCaseForTemperatureSplit()
    {
        string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature_2.json";
        string jsonString;
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
        FromJsonString(*schema, jsonString);
        IndexPartitionOptions options;
        MergeConfig& mergeConfig = options.GetMergeConfig();
        mergeConfig.mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
        mergeConfig.mergeThreadCount = 1;
        std::string splitConfigStr = "{\"class_name\":\"temperature\",\"parameters\":{}}";
        autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), splitConfigStr);
        string rootPath = GET_TEMP_DATA_PATH();
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, rootPath));
        uint64_t currentTime = util::Timer::GetCurrentTime();
        string docStrings = "cmd=add,pk=1,status=0,time=" + StringUtil::toString(currentTime - 1) + ";"; // hot
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
        Version version;
        VersionLoader::GetVersionS(rootPath, version, INVALID_VERSIONID);
        ASSERT_EQ(version.GetSegmentCount(), version.GetSegTemperatureMetas().size());
        SegmentTemperatureMeta meta;
        ASSERT_TRUE(version.GetSegmentTemperatureMeta(0, meta));
        ASSERT_EQ("HOT:1;WARM:0;COLD:0", meta.segTemperatureDetail);
        ASSERT_EQ("HOT", meta.segTemperature);

        string incDoc = "cmd=update_field,pk=1,status=1,time=" + StringUtil::toString(currentTime - 2000) + ";";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
        VersionLoader::GetVersionS(rootPath, version, INVALID_VERSIONID);
        ASSERT_EQ(version.GetSegmentCount(), version.GetSegTemperatureMetas().size());
        ASSERT_TRUE(version.GetSegmentTemperatureMeta(2, meta));
        ASSERT_EQ("WARM", meta.segTemperature);
        ASSERT_EQ("HOT:0;WARM:1;COLD:0", meta.segTemperatureDetail);
    }

    void TestMergeBranchProgress()
    {
        string field = "string1:string;string2:string;price:uint32";
        string index = "index2:string:string2;"
                       "pk:primarykey64:string1";

        string attribute = "string1;price";
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, field, index, attribute, "");

        string docString = "cmd=add,string1=hello,price=4,ts=100;"
                           "cmd=add,string1=hello,price=5,ts=101;"
                           "cmd=add,string1=hello,price=6,ts=102;";
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        options.GetBuildConfig().maxDocCount = 1;
        options.GetOfflineConfig().mergeConfig.SetEnablePackageFile(false);

        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH(), "name", "", 1));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

        PartitionMergerPtr merger(PartitionMergerCreator::CreateSinglePartitionMerger(
            GET_TEMP_DATA_PATH(), options, nullptr, "", PartitionRange(), CommonBranchHinterOption::Test(1)));

        auto indexMerger = DYNAMIC_POINTER_CAST(IndexPartitionMerger, merger);
        indexMerger->mHinter.reset(new FakeMergerBranchHinter());
        indexMerger->PrepareMerge(123);
        MergeMetaPtr meta = indexMerger->CreateMergeMeta(false, 1, 123);
        indexMerger->CleanTargetDirs(meta);
        indexMerger->DoMerge(false, meta, 0);
        indexMerger->SyncBranchProgress();
        sleep(1);

        string branchName = indexMerger->TEST_GetBranchFs()->GetBranchName();
        string progressFile = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "instance_0/merger_progress_for_branch");
        ASSERT_TRUE(FslibWrapper::IsExist(progressFile).GetOrThrow());
        string branchContent;
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(progressFile, branchContent).Code());
        vector<string> infos;
        StringUtil::split(infos, branchContent, " ");
        ASSERT_EQ(2, infos.size());
        ASSERT_EQ(branchName, infos[0]);
        ASSERT_EQ("100", infos[1]);
    }

    void TestBeginCkptConcurrency()
    {
        autil::EnvGuard env("INDEXLIB_PACKAGE_MERGE_META", "true");
        string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature_2.json";
        string jsonString;
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
        FromJsonString(*schema, jsonString);
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        MergeConfig& mergeConfig = options.GetMergeConfig();
        mergeConfig.mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
        mergeConfig.mergeThreadCount = 1;
        mergeConfig.SetCalculateTemperature(true);
        mergeConfig.SetEnablePackageFile(true);
        std::string splitConfigStr = "{\"class_name\":\"temperature\",\"parameters\":{}}";
        autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), splitConfigStr);
        string rootPath = GET_TEMP_DATA_PATH();
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, rootPath));
        uint64_t currentTime = util::Timer::GetCurrentTime();
        string docStrings = "cmd=add,pk=1,status=0,time=" + StringUtil::toString(currentTime - 1) +
                            ";"
                            "cmd=add,pk=2,status=3,time=" +
                            StringUtil::toString(currentTime - 1) +
                            ";"
                            "cmd=add,pk=3,status=4,time=" +
                            StringUtil::toString(currentTime - 1) + ";"; // hot
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
        IndexPartitionMergerPtr merger1((IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
            rootPath, options, NULL, ""));
        merger1->SetCheckpointRootDir(merger1->GetMergeCheckpointDir() + "/12345");
        string metaRoot = merger1->GetMergeMetaDir() + "/12345";
        // 这个ut是测试多个begin merge任务抢zk的情况，通过branch fs模拟时存在不稳定的情况，降低begin
        // merge的任务的数量来减少不稳定的情况
        int32_t mergeCount = 2;
        int32_t failedCount = 0;
        srand((unsigned)time(NULL));
        bool needFailOver = (rand() % 2 == 0);
        vector<autil::ThreadPtr> threads;
        {
            for (size_t i = 0; i < mergeCount; i++) {
                autil::ThreadPtr thread = autil::Thread::createThread([&]() {
                    try {
                        IndexPartitionMergerPtr merger(
                            (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
                                rootPath, options, NULL, ""));
                        merger->SetCheckpointRootDir(merger->GetMergeCheckpointDir() + "/12345");
                        auto meta = merger->CreateMergeMeta(false, 1, 123);
                        auto fenceContext = merger->mDumpStrategy->GetRootDirectory()->GetFenceContext();
                        meta->Store(metaRoot, fenceContext.get());
                    } catch (util::ExceptionBase& e) {
                        failedCount++;
                    }
                });
                threads.push_back(thread);
            }
        }
        for (auto& thread : threads) {
            thread->join();
        }

        if (failedCount == mergeCount || needFailOver) {
            usleep(1000);
            IndexPartitionMergerPtr merger(
                (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(rootPath, options, NULL,
                                                                                                ""));
            merger->SetCheckpointRootDir(merger->GetMergeCheckpointDir() + "/12345");
            auto meta = merger->CreateMergeMeta(false, 1, 123);
            auto fenceContext = merger->mDumpStrategy->GetRootDirectory()->GetFenceContext();
            meta->Store(metaRoot, fenceContext.get());
        }

        fslib::FileList fileList;
        string rootCheckpointPath = merger1->GetCheckpointDirForPhase("begin_merge");
        ASSERT_EQ(FSEC_OK, FslibWrapper::ListDirRecursive(
                               util::PathUtil::JoinPath(rootCheckpointPath, "recalculate_temperature"), fileList)
                               .Code());
        ASSERT_EQ(3, fileList.size()) << autil::StringUtil::toString(fileList, " ") << endl;

        IE_LOG(INFO, "ut begin do-merge, one worker to doMerge")
        IndexPartitionMergerPtr newMerger(
            (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(rootPath, options, NULL,
                                                                                            ""));
        auto loadMeta = newMerger->LoadMergeMeta(metaRoot, false);
        ASSERT_TRUE(loadMeta != nullptr);
        newMerger->DoMerge(true, loadMeta, 0);

        IE_LOG(INFO, "ut begin do-merge, multi worker to endMerge")
        failedCount = 0;
        {
            for (size_t i = 0; i < mergeCount; i++) {
                usleep(1000);
                autil::ThreadPtr thread = autil::Thread::createThread([&]() {
                    try {
                        IndexPartitionMergerPtr merger(
                            (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
                                rootPath, options, NULL, ""));
                        merger->EndMerge(loadMeta, 4);
                    } catch (util::ExceptionBase& e) {
                        // do nothing
                        failedCount++;
                    }
                });
                threads.push_back(thread);
            }
        }
        for (auto& thread : threads) {
            thread->join();
        }
        if (failedCount == mergeCount || needFailOver) {
            usleep(100);
            IndexPartitionMergerPtr merger(
                (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(rootPath, options, NULL,
                                                                                                ""));
            merger->EndMerge(loadMeta, 4);
        }

        PartitionStateMachine psm2;
        ASSERT_TRUE(psm2.Init(schema, options, rootPath));
        ASSERT_TRUE(psm2.Transfer(QUERY, "", "pk:1", "status=0,time=" + StringUtil::toString(currentTime - 1)));
        ASSERT_TRUE(psm2.Transfer(QUERY, "", "pk:2", "status=3,time=" + StringUtil::toString(currentTime - 1)));
        ASSERT_TRUE(psm2.Transfer(QUERY, "", "pk:3", "status=4,time=" + StringUtil::toString(currentTime - 1)));
    }

    void TestMergeWithBackupMerger()
    {
        SingleFieldPartitionDataProvider provider;
        provider.Init(mRootDir, "int32", SFP_PK_INDEX);
        provider.Build("1,2,3#4,5,6#7,delete:4,delete:5,delete:6", SFP_OFFLINE);
        IndexPartitionOptions options = provider.GetOptions();
        MergeConfig& mergeConfig = options.GetMergeConfig();
        mergeConfig.mergeStrategyStr = SPECIFIC_SEGMENTS_MERGE_STRATEGY_STR;
        mergeConfig.mergeStrategyParameter.SetLegacyString("merge_segments=1");

        BuildConfig& buildConfig = options.GetBuildConfig();
        buildConfig.keepVersionCount = 100;
        IndexPartitionMergerPtr merger((IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
            mRootDir, options, NULL, ""));
        MergeMetaPtr mergeMeta = merger->CreateMergeMeta(false, 3, 0);
        merger->DoMerge(false, mergeMeta, 0);
        string branchNameForInstance0 = merger->TEST_GetBranchFs()->GetBranchName();
        merger->DoMerge(false, mergeMeta, 1);
        merger->DoMerge(false, mergeMeta, 2);
        string branchRootDir = util::PathUtil::JoinPath(mRootDir, "instance_0", branchNameForInstance0);
        FslibWrapper::DeleteFileE(util::PathUtil::JoinPath(branchRootDir, "check_points/"),
                                  DeleteOption::NoFence(false));
        FslibWrapper::MkDirE(util::PathUtil::JoinPath(branchRootDir, "check_points/"));

        {
            // backup merger
            IndexPartitionMergerPtr mergerBack(
                (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
                    mRootDir, options, NULL, "", PartitionRange(), CommonBranchHinterOption::Test(1)));
            mergerBack->DoMerge(false, mergeMeta, 1);
            mergerBack->DoMerge(false, mergeMeta, 0);
            string branchNameForInstance0 = mergerBack->TEST_GetBranchFs()->GetBranchName();
            mergerBack->DoMerge(false, mergeMeta, 2);
            IndexPartitionMergerPtr mergerBack2(
                (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
                    mRootDir, options, NULL, "", PartitionRange(), CommonBranchHinterOption::Test(2)));
            string branchRootDir = util::PathUtil::JoinPath(mRootDir, "instance_0", branchNameForInstance0);
            FslibWrapper::DeleteFileE(util::PathUtil::JoinPath(branchRootDir, "check_points/"),
                                      DeleteOption::NoFence(false));
            FslibWrapper::MkDirE(util::PathUtil::JoinPath(branchRootDir, "check_points/"));
            mergerBack2->DoMerge(false, mergeMeta, 0);
        }
        merger->DoMerge(false, mergeMeta, 2);
        merger->EndMerge(mergeMeta);
        OnlinePartition partition;
        partition.Open(mRootDir, "", provider.GetSchema(), options);
        IndexPartitionReaderPtr reader = partition.GetReader();
        PrimaryKeyIndexReaderPtr pkReader = reader->GetPrimaryKeyReader();
        ASSERT_EQ((docid_t)0, pkReader->Lookup("1"));
        ASSERT_EQ(INVALID_DOCID, pkReader->Lookup("6"));
        ASSERT_EQ((docid_t)3, pkReader->Lookup("7"));
    }

    void TestMergeConflict()
    {
        // old end-merger and next round do-merger conflicts
        // expected old merger not move dirs
        string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature_2.json";
        string jsonString;
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
        FromJsonString(*schema, jsonString);
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        MergeConfig& mergeConfig = options.GetMergeConfig();
        mergeConfig.mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
        mergeConfig.mergeThreadCount = 1;
        mergeConfig.SetCalculateTemperature(true);
        options.GetBuildConfig().maxDocCount = 1;
        std::string splitConfigStr = "{\"class_name\":\"temperature\",\"parameters\":{}}";
        autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), splitConfigStr);
        string rootPath = GET_TEMP_DATA_PATH();
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, rootPath));
        uint64_t currentTime = util::Timer::GetCurrentTime();
        string docStrings = "cmd=add,pk=1,status=0,time=" + StringUtil::toString(currentTime - 1) +
                            ";"
                            "cmd=add,pk=2,status=0,time=" +
                            StringUtil::toString(currentTime - 1) + ";"; // hot
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
        IndexPartitionMergerPtr merger((IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
            rootPath, options, NULL, ""));
        merger->SetCheckpointRootDir(merger->GetMergeCheckpointDir() + "/12345");
        auto mergeMeta = merger->CreateMergeMeta(false, 1, 123);

        merger->DoMerge(false, mergeMeta, 0);
        merger->EndMerge(mergeMeta, 4);

        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "cmd=add,pk=2,status=0,time=123;", "", ""));
        IndexPartitionMergerPtr merger2((IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
            rootPath, options, NULL, ""));
        merger2->SetCheckpointRootDir(merger2->GetMergeCheckpointDir() + "/12346");
        auto mergeMeta2 = merger2->CreateMergeMeta(true, 1, 124);
        merger2->DoMerge(true, mergeMeta2, 0);
        // 模拟僵死节点 end merge
        ASSERT_THROW(merger->EndMerge(mergeMeta2, 6), util::RuntimeException);
        merger2->EndMerge(mergeMeta2, 7);
    }

    void prepareOneInstanceDir(const file_system::DirectoryPtr& rootDir, uint32_t instanceId)
    {
        DirectoryPtr instanceDir = rootDir->MakeDirectory("instance_" + StringUtil::toString(instanceId));
        instanceDir->MakeDirectory(ADAPTIVE_DICT_DIR_NAME)->Store("/d" + StringUtil::toString(instanceId), "");
        prepareOneSegmentDir(instanceDir, 2, instanceId);
        prepareOneSegmentDir(instanceDir, 4, instanceId);
    }

    void prepareOneSegmentDir(const DirectoryPtr& segmentDir, uint32_t instanceId)
    {
        segmentDir->MakeDirectory("index");
        string indexName = "index" + StringUtil::toString(instanceId);
        segmentDir->MakeDirectory("index/" + indexName);
        segmentDir->Store("index/" + indexName + "/dict", "");
        segmentDir->Store("index/" + indexName + "/posting", "");
        segmentDir->MakeDirectory("summary");
        segmentDir->MakeDirectory("attribute");
        string attrName = "attr" + StringUtil::toString(instanceId);
        segmentDir->MakeDirectory("attribute/" + attrName);
        segmentDir->Store("attribute/" + attrName + "/data", "");
        segmentDir->Store("attribute/" + attrName + "/offset", "");
    }

    void prepareOneSegmentDir(const DirectoryPtr& partitionDir, segmentid_t segmentId, uint32_t instanceId)
    {
        DirectoryPtr segmentDir =
            partitionDir->MakeDirectory("segment_" + StringUtil::toString(segmentId) + "_level_0");
        prepareOneSegmentDir(segmentDir, instanceId);
        prepareOneSegmentDir(segmentDir->MakeDirectory(SUB_SEGMENT_DIR_NAME), instanceId);
    }

private:
    void InternalTestCaseForMerge(const string& versionsStr, const string& mergeTaskStr)
    {
        Version lastVersion;
        SegmentMergeInfos segMergeInfos;
        MakeFakeSegmentDir(versionsStr, lastVersion, segMergeInfos);

        SegmentDirectoryPtr segDir(new SegmentDirectory(GET_PARTITION_DIRECTORY(), lastVersion));
        segDir->Init(false, true);
        IndexPartitionOptions option = mOptions;
        IndexPartitionMergerMock merger(segDir, mSchema, option);
        Version mergedVersion;
        SegmentInfos mergedSegInfos;
        MergeTask task;
        MakeMergeTask(mergeTaskStr, segMergeInfos, task, mergedVersion, mergedSegInfos);
        merger.SetMergeTask(task);
        merger.Merge();
        mergedVersion.SetVersionId(lastVersion.GetVersionId() + 1);
        CheckMergeResult(mergedVersion, mergedSegInfos);
    }

    void MakeFakeSegmentDir(const string& versionsStr, Version& lastVersion, SegmentMergeInfos& segMergeInfos)
    {
        versionid_t versionId = 0;
        StringTokenizer st(versionsStr, ";", StringTokenizer::TOKEN_TRIM);
        for (size_t i = 0; i < st.getNumTokens(); ++i) {
            StringTokenizer st2(st[i], ",", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);

            segMergeInfos.clear();

            Version version(versionId++);
            size_t globalDocCount = 0;
            for (size_t j = 0; j < st2.getNumTokens(); j += 2) {
                segmentid_t segmentId;
                StringUtil::strToInt32(st2[j].c_str(), segmentId);
                version.AddSegment(segmentId);

                file_system::DirectoryPtr segDir =
                    GET_PARTITION_DIRECTORY()->MakeDirectory(version.GetSegmentDirName(segmentId));
                segDir->MakeDirectory(DELETION_MAP_DIR_NAME);

                SegmentInfo segInfo;
                segInfo.AddDescription(SEGMENT_INIT_TIMESTAMP, "0");
                segInfo.SetLocator(indexlibv2::framework::Locator(0, segmentId));
                StringTokenizer st3(st2[j + 1], "#", StringTokenizer::TOKEN_TRIM);
                uint32_t docCount = 0;
                if (st3.getNumTokens() != 2) {
                    StringUtil::strToUInt32(st2[j + 1].c_str(), docCount);
                    segInfo.docCount = docCount;
                } else {
                    StringUtil::strToUInt32(st3[0].c_str(), docCount);
                    segInfo.docCount = docCount;
                    if (st3[1] == "true") {
                        segInfo.mergedSegment = true;
                    }
                }
                segDir->Store(COUNTER_FILE_NAME, "{}");

                segInfo.Store(segDir);
                DeployIndexWrapper::DumpSegmentDeployIndex(segDir, "");

                SegmentMergeInfo segMergeInfo;
                segMergeInfo.segmentId = segmentId;
                segMergeInfo.segmentInfo = segInfo;
                segMergeInfo.baseDocId = globalDocCount;
                globalDocCount += docCount;
                segMergeInfos.push_back(segMergeInfo);
            }
            string content = version.ToString();
            stringstream ss;
            ss << VERSION_FILE_NAME_PREFIX << "." << version.GetVersionId();
            GET_PARTITION_DIRECTORY()->Store(ss.str(), content);

            if (i == st.getNumTokens() - 1) {
                lastVersion = version;
            }
        }
    }

    DeletionMapReaderPtr CreateDeletionMap(Version version, const SegmentMergeInfos& segMergeInfos)
    {
        vector<uint32_t> docCounts;
        for (size_t i = 0; i < segMergeInfos.size(); ++i) {
            docCounts.push_back((uint32_t)segMergeInfos[i].segmentInfo.docCount);
        }
        DeletionMapReaderPtr delMapReader =
            IndexTestUtil::CreateDeletionMap(version, docCounts, IndexTestUtil::NoDelete);
        return delMapReader;
    }

    void MakeMergeTask(const string& mergeTaskStr, const SegmentMergeInfos& segMergeInfos, MergeTask& task,
                       Version& mergedVersion, SegmentInfos& mergedSegInfos)
    {
        MergeTaskMaker::CreateMergeTask(mergeTaskStr, task, segMergeInfos);

        mergedSegInfos.resize(task.GetPlanCount());
        segmentid_t segId = 0;
        for (size_t i = 0; i < segMergeInfos.size(); i++) {
            segId = segMergeInfos[i].segmentId;
            bool inPlan = false;
            for (size_t j = 0; j < task.GetPlanCount(); j++) {
                if (task[j].HasSegment(segId)) {
                    mergedSegInfos[j].docCount = mergedSegInfos[j].docCount + segMergeInfos[i].segmentInfo.docCount;
                    mergedSegInfos[j].SetLocator((segMergeInfos.rbegin())->segmentInfo.GetLocator());
                    mergedSegInfos[j].mergedSegment = true;
                    inPlan = true;
                    break;
                }
            }
            if (!inPlan) {
                mergedVersion.AddSegment(segId);
            }
        }
        for (size_t i = 0; i < task.GetPlanCount(); i++) {
            segId++;
            mergedVersion.AddSegment(segId);
        }
    }

    void MakeInMemMergeAnswer(const vector<InMemorySegmentReaderPtr>& inMemSegReaders, Version& mergedVersion,
                              SegmentInfos& mergedSegInfos)
    {
        SegmentInfo segInfo;
        segInfo.AddDescription(SEGMENT_INIT_TIMESTAMP, "0");
        segInfo.docCount = 0;
        for (size_t i = 0; i < inMemSegReaders.size(); ++i) {
            const SegmentInfo& readerSegInfo = inMemSegReaders[i]->GetSegmentInfo();
            segInfo.docCount = segInfo.docCount + readerSegInfo.docCount;

            int64_t int64Locator = StringUtil::fromString<int64_t>(segInfo.GetLocator().Serialize());
            int64_t int64Locator2 = StringUtil::fromString<int64_t>(readerSegInfo.GetLocator().Serialize());
            int64Locator += int64Locator2;
            segInfo.SetLocator(indexlibv2::framework::Locator(0, int64Locator));
        }
        const SegmentInfo& lastSegInfo = inMemSegReaders.back()->GetSegmentInfo();
        segInfo.SetLocator(lastSegInfo.GetLocator());

        mergedSegInfos.push_back(segInfo);
        segmentid_t lastSegId = inMemSegReaders.back()->GetSegmentId();
        mergedVersion.AddSegment(lastSegId + 1);
    }

    void CheckMergeResult(const Version& mergedVersion, const SegmentInfos& mergedSegInfos)
    {
        stringstream ss;
        ss << mRootDir << VERSION_FILE_NAME_PREFIX << "." << mergedVersion.GetVersionId();
        Version version;
        INDEXLIB_TEST_TRUE(version.Load(ss.str()));

        INDEXLIB_TEST_EQUAL(mergedVersion, version);

        RESET_FILE_SYSTEM("ut");
        SegmentDirectory segDir(GET_PARTITION_DIRECTORY(), mergedVersion);
        segDir.Init(false, true);
        vector<file_system::DirectoryPtr> segDirs;
        SegmentDirectory::Iterator segPathIter = segDir.CreateIterator();
        while (segPathIter.HasNext()) {
            segDirs.push_back(segPathIter.Next());
        }

        INDEXLIB_TEST_TRUE(segDirs.size() >= mergedSegInfos.size());

        for (size_t i = segDirs.size() - mergedSegInfos.size(), j = 0; i < segDirs.size(); i++, j++) {
            SegmentInfo segInfo;
            INDEXLIB_TEST_TRUE(segDirs[i]->IsExist(SEGMENT_FILE_LIST));
            INDEXLIB_TEST_TRUE(segInfo.Load(segDirs[i]));
            SegmentInfo mergeInfo = mergedSegInfos[j];
            mergeInfo.AddDescription(SEGMENT_INIT_TIMESTAMP, string("0"));
            segInfo.AddDescription(SEGMENT_INIT_TIMESTAMP, string("0"));
            INDEXLIB_TEST_EQUAL(mergeInfo, segInfo);
        }
        string truncateMetaIndex = util::PathUtil::JoinPath(mRootDir, TRUNCATE_META_DIR_NAME);

        if (FslibWrapper::IsExist(truncateMetaIndex).GetOrThrow()) {
            string deployIndexFile = util::PathUtil::JoinPath(truncateMetaIndex, SEGMENT_FILE_LIST);
            INDEXLIB_TEST_TRUE(FslibWrapper::IsExist(deployIndexFile).GetOrThrow());
        }
    }

private:
    std::string mRootDir;
    IndexPartitionSchemaPtr mSchema;
    IndexPartitionOptions mOptions;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(merger, IndexPartitionMergerTest);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForMerge);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForMergeWithDeployIndex);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForMergeMultiVersion);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForCachedSegmentInfo);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForInvalidMergePlan);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForCreateMergeScheduler);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForSplitFileName);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForPrepareMergeDir);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForMergeInstanceDir);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForDeployIndexFail);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForMergeWithEmptyInstance);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestMergeWithBackupMerger);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestMergeForRecalculatorMetric);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestMergeForRecalculatorMetricWithDelete);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestMergeForRecalculatorMetricWithUpdate);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestMergeWithTemperature);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForTemperatureSplit);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestFullMergeWithTemperature);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestFullMergeWithNoRecaTemperature);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestBeginCkptConcurrency);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestMergeBranchProgress);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestMergeConflict);
}} // namespace indexlib::merger
