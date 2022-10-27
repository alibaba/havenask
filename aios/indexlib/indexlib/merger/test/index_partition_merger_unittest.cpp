#define INDEX_PARTITION_MERGER_UNITTEST    
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/deletionmap/deletion_map_merger.h"
#include "indexlib/index/normal/deletionmap/deletion_map_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/index/normal/summary/local_disk_summary_merger.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/merger/merge_strategy/balance_tree_merge_strategy.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/merger/test/merge_task_maker.h"
#include "indexlib/merger/multi_threaded_merge_scheduler.h"
#include "indexlib/merger/test/merge_work_item_creator_mock.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/index_base/deploy_index_wrapper.h"

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace testing;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(merger);

class IndexPartitionMergerMock : public IndexPartitionMerger
{
public:
    IndexPartitionMergerMock(const SegmentDirectoryPtr& dir, 
                             const config::IndexPartitionSchemaPtr& schema,
                             const IndexPartitionOptions& options)
        : IndexPartitionMerger(dir, schema, options, merger::DumpStrategyPtr(),
                               NULL, plugin::PluginManagerPtr())
    {}
    
    virtual ~IndexPartitionMergerMock()
    {}

    void SetMergeTask(const MergeTask& task) { mTask = task; }

protected:
    ReclaimMapPtr CreateSortByWeightReclaimMap(
            const SegmentMergeInfos& segMergeInfos)
    {
        return ReclaimMapPtr(new ReclaimMap());
    }

    
    MergeWorkItemCreatorPtr CreateMergeWorkItemCreator(
            bool optimize, const IndexMergeMeta &mergeMeta,
            const MergeFileSystemPtr& mergeFileSystem) override
    {
        return MergeWorkItemCreatorPtr(new MergeWorkItemCreatorMock(
                        mSchema, mMergeConfig, &mergeMeta, mSegmentDirectory,
                        SegmentDirectoryPtr(), IsSortMerge(), mOptions, mergeFileSystem));
    }

    virtual MergeTask CreateMergeTaskByStrategy(bool optimize,
                                                const config::MergeConfig& mergeConfig,
                                                const SegmentMergeInfos& segMergeInfos,
                                                const LevelInfo& levelInfo) const override
    { return mTask; }

private:
    MergeTask mTask;
};

class IndexPartitionMergerTest : public INDEXLIB_TESTBASE
{
public:
    IndexPartitionMergerTest()
    {
    }

    DECLARE_CLASS_NAME(IndexPartitionMergerTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
        mOptions.SetIsOnline(false);
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mSchema,
                "string1:string;long1:uint32", // above is field schema
                "stringI:string:string1;", // above is index schema
                "long1", // above is attribute schema
                "string1" );// above is summary schema
    }

    void CaseTearDown() override
    {    
    }

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
        IndexPartitionMergerPtr merger(
                (IndexPartitionMerger*)PartitionMergerCreator::
                CreateSinglePartitionMerger(mRootDir, options, NULL, ""));
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

        SegmentDirectoryPtr segDir(new SegmentDirectory(mRootDir, lastVersion));
        segDir->Init(false, true);
        vector<uint32_t> docCounts;
        for (size_t i = 0; i < segMergeInfos.size(); ++i)
        {
            docCounts.push_back((uint32_t)segMergeInfos[i].segmentInfo.docCount);
        }        

        IndexPartitionMergerMock merger(segDir, mSchema, mOptions);

        stringstream ss;
        ss << mRootDir << lastVersion.GetSegmentDirName(2) << "/"
           << SEGMENT_INFO_FILE_NAME;
        FileSystemWrapper::DeleteDir(ss.str());

        Version mergedVersion;
        SegmentInfos mergedSegInfos;
        MergeTask task;
        MakeMergeTask(mergeTaskStr, segMergeInfos, 
                      task, mergedVersion, mergedSegInfos);
        merger.SetMergeTask(task);
        ASSERT_NO_THROW(merger.Merge());
    }

    void TestCaseForInvalidMergePlan()
    {
        string versionStr = "1,1024,2,2000;3,1024,4,800,5,50";

        Version lastVersion;
        SegmentMergeInfos segMergeInfos;
        MakeFakeSegmentDir(versionStr, lastVersion, segMergeInfos);
        SegmentDirectoryPtr segDir(new SegmentDirectory(mRootDir, lastVersion));
        segDir->Init(false, true);

        vector<uint32_t> docCounts;
        for (size_t i = 0; i < segMergeInfos.size(); ++i)
        {
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
        Version lastVersion(INVALID_VERSION);
        SegmentDirectoryPtr segDir(new SegmentDirectory(mRootDir, lastVersion));
        segDir->Init(false, true);

        IndexPartitionMergerPtr merger;
        merger.reset(new IndexPartitionMerger(segDir, mSchema, simpleOptions,
                        merger::DumpStrategyPtr(), NULL, plugin::PluginManagerPtr()));
        MergeSchedulerPtr scheduler = merger->CreateMergeScheduler(
                simpleOptions.GetMergeConfig().maxMemUseForMerge,
                simpleOptions.GetMergeConfig().mergeThreadCount);
        
        MultiThreadedMergeSchedulerPtr mulThreadedScheduler = 
            dynamic_pointer_cast<MultiThreadedMergeScheduler>(scheduler);
        INDEXLIB_TEST_TRUE(mulThreadedScheduler != NULL);
        ASSERT_EQ((size_t)1, mulThreadedScheduler->GetThreadNum());

        IndexPartitionOptions mulThreadedOptions = mOptions;
        mulThreadedOptions.GetMergeConfig().mergeThreadCount = 3;
        merger.reset(new IndexPartitionMerger(segDir, mSchema, mulThreadedOptions,
                        merger::DumpStrategyPtr(), NULL, plugin::PluginManagerPtr())); 
        scheduler = merger->CreateMergeScheduler(
                mulThreadedOptions.GetMergeConfig().maxMemUseForMerge,
                mulThreadedOptions.GetMergeConfig().mergeThreadCount);
        mulThreadedScheduler = 
            dynamic_pointer_cast<MultiThreadedMergeScheduler>(scheduler);            
        INDEXLIB_TEST_TRUE(mulThreadedScheduler != NULL);
        ASSERT_EQ((size_t)3, mulThreadedScheduler->GetThreadNum());
    }



    void TestCaseForMkDirIfNotExist()
    {
        string dirPath = mRootDir + "/newDir";
        ASSERT_NO_THROW(IndexPartitionMerger::MkDirIfNotExist(dirPath));
        ASSERT_TRUE(FileSystemWrapper::IsExist(dirPath));
        ASSERT_NO_THROW(IndexPartitionMerger::MkDirIfNotExist(dirPath));
        ASSERT_TRUE(FileSystemWrapper::IsExist(dirPath));
        FileSystemWrapper::DeleteDir(dirPath);
    }

    void TestCaseForGetInstanceTempMergeDir()
    {
        EXPECT_EQ(string("/root/instance_2"),
                  IndexPartitionMerger::GetInstanceTempMergeDir("/root", 2));
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

    void TestCaseForMoveFiles()
    {
        //TODO: check sub segment
        string src = FileSystemWrapper::JoinPath(mRootDir, "srcDir");
        string dest = FileSystemWrapper::JoinPath(mRootDir, "destDir");
        FileSystemWrapper::MkDir(src);
        FileSystemWrapper::MkDir(dest);
        string file1 = FileSystemWrapper::JoinPath(src, "1");
        FileSystemWrapper::AtomicStore(file1, "");
        string dir1 = FileSystemWrapper::JoinPath(src, "2");
        FileSystemWrapper::MkDir(dir1);
        string file2 = FileSystemWrapper::JoinPath(dir1, "2");
        FileSystemWrapper::AtomicStore(file2, "");
        FileList list;
        list.push_back(file1);
        list.push_back(dir1);
        IndexPartitionMerger::MoveFiles(list, dest);
        EXPECT_TRUE(FileSystemWrapper::IsExist(mRootDir + "/destDir/1"));
        EXPECT_TRUE(FileSystemWrapper::IsDir(mRootDir + "/destDir/2"));        
        EXPECT_TRUE(FileSystemWrapper::IsExist(mRootDir + "/destDir/2/2"));

        // test different src dir with same dir name
        string newSrc = FileSystemWrapper::JoinPath(mRootDir, "newSrcDir");
        FileSystemWrapper::MkDir(newSrc);
        string dir2 = FileSystemWrapper::JoinPath(newSrc, "2");
        FileSystemWrapper::MkDir(dir2);
        string file3 = FileSystemWrapper::JoinPath(dir2, "3");
        FileSystemWrapper::AtomicStore(file3, "");

        FileList newList;
        newList.push_back(dir2);
        IndexPartitionMerger::MoveFiles(newList, dest);
        EXPECT_TRUE(FileSystemWrapper::IsExist(mRootDir + "/destDir/2/3"));

        {
            // test different src dir with different file name
            string file4 = FileSystemWrapper::JoinPath(dir2, "4");
            FileSystemWrapper::AtomicStore(file4, "");
            FileList newList;
            newList.push_back(dir2);
            IndexPartitionMerger::MoveFiles(newList, dest);
            EXPECT_TRUE(FileSystemWrapper::IsExist(mRootDir + "/destDir/2/4"));

            // test different src dir with same file name
            string file5 = FileSystemWrapper::JoinPath(dir2, "2");
            FileSystemWrapper::AtomicStore(file5, "");
            newList.clear();
            newList.push_back(dir2);
            ASSERT_ANY_THROW(IndexPartitionMerger::MoveFiles(newList, dest));
        }
        
    }

    void TestCaseForListInstanceFiles()
    {
        string dir = FileSystemWrapper::JoinPath(mRootDir, "segment_0");
        FileSystemWrapper::MkDir(dir);
        {
            // not exist or empty
            EXPECT_TRUE(IndexPartitionMerger::ListInstanceFiles(
                            mRootDir, "not_exist").empty());
            EXPECT_TRUE(IndexPartitionMerger::ListInstanceFiles(
                            mRootDir, "segment_0").empty());
        }
        {
            FileSystemWrapper::AtomicStore(FileSystemWrapper::JoinPath(dir, "data"), "");
            FileSystemWrapper::MkDir(FileSystemWrapper::JoinPath(dir, "dir"));
            FileList list = IndexPartitionMerger::ListInstanceFiles(mRootDir, "segment_0");
            EXPECT_THAT(list, UnorderedElementsAre(
                            mRootDir + "segment_0/data",
                            mRootDir + "segment_0/dir"));
        }
    }

    void TestCaseForListInstanceDirs()
    {
        FileList list = IndexPartitionMerger::ListInstanceDirs(mRootDir);
        ASSERT_TRUE(list.empty());
        FileSystemWrapper::MkDir(FileSystemWrapper::JoinPath(mRootDir, "instance_2"));
        FileSystemWrapper::MkDir(FileSystemWrapper::JoinPath(mRootDir, "instance_5"));
        list = IndexPartitionMerger::ListInstanceDirs(mRootDir);
        EXPECT_THAT(list, UnorderedElementsAre(
                        FileSystemWrapper::JoinPath(mRootDir, "instance_2"),
                        FileSystemWrapper::JoinPath(mRootDir, "instance_5")));
    }
    
    void TestCaseForPrepareMergeDir()
    {
        {
            FileList list;
            FileSystemWrapper::ListDir(mRootDir, list);
            ASSERT_TRUE(list.empty());
        }
        uint32_t instanceId = 2;
        IndexMergeMetaPtr mergeMeta(new IndexMergeMeta());
        MergePlan emptyMergePlan;
        SegmentInfo emptySegmentInfo;
        SegmentMergeInfos emtpySegmentMergeInfos;
        Version targetVersion(0);
        mergeMeta->TEST_AddMergePlanMeta(emptyMergePlan, 0, emptySegmentInfo, emptySegmentInfo,
                emtpySegmentMergeInfos, emtpySegmentMergeInfos);
        mergeMeta->TEST_AddMergePlanMeta(emptyMergePlan, 2, emptySegmentInfo, emptySegmentInfo,
                emtpySegmentMergeInfos, emtpySegmentMergeInfos);
        mergeMeta->TEST_AddMergePlanMeta(emptyMergePlan, 4, emptySegmentInfo, emptySegmentInfo,
                emtpySegmentMergeInfos, emtpySegmentMergeInfos);
        targetVersion.AddSegment(0);
        targetVersion.AddSegment(2);
        targetVersion.AddSegment(4);
        mergeMeta->SetTargetVersion(targetVersion);
        FileSystemWrapper::MkDir(mRootDir + "/instance_2");
        FileSystemWrapper::AtomicStore(mRootDir + "/instance_2/data", "");
        SegmentDirectoryPtr segDir(new SegmentDirectory(mRootDir, Version()));
        segDir->Init(false, true);
        IndexPartitionMergerMock merger(segDir, mSchema, mOptions);
        merger.CreateMergeFileSystem(mRootDir, instanceId, mergeMeta);
        FileList list;
        FileSystemWrapper::ListDir(mRootDir + "instance_2", list);
        EXPECT_THAT(list, UnorderedElementsAre(
                        string("check_points"), string("target_version"),
                        string("segment_0_level_0"), string("segment_2_level_0"), string("segment_4_level_0")));
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
        
        prepareOneInstanceDir(mRootDir, 1);
        prepareOneInstanceDir(mRootDir, 3);

        SegmentDirectoryPtr segDir(new SegmentDirectory(mRootDir, INVALID_VERSION));
        IndexPartitionOptions options;
        IndexPartitionMerger merger(segDir, mSchema, options,
                merger::DumpStrategyPtr(), NULL, plugin::PluginManagerPtr());
        const file_system::DirectoryPtr& rootDirectory =
            file_system::DirectoryCreator::Create(mRootDir, false, true, true);
        merger.MergeInstanceDir(rootDirectory, mergePlans, targetVersion, "{}", true);

        FileList list;
        FileSystemWrapper::ListDir(mRootDir, list);
        EXPECT_THAT(list, UnorderedElementsAre(
                        string("segment_2_level_0"), string("segment_4_level_0"),
                        string(ADAPTIVE_DICT_DIR_NAME)));
        FileSystemWrapper::ListDir(mRootDir + ADAPTIVE_DICT_DIR_NAME, list);
        EXPECT_THAT(list, UnorderedElementsAre(string("d1"), string("d3")));
        vector<string> segmentDirs;
        segmentDirs.push_back(mRootDir + "segment_2_level_0/");
        segmentDirs.push_back(mRootDir + "segment_4_level_0/");

        for (size_t i = 0; i < segmentDirs.size(); i++)
        {
            FileSystemWrapper::ListDir(segmentDirs[i], list);
            EXPECT_THAT(list, UnorderedElementsAre(string("index"), string("summary"), 
                            string("attribute"), string("segment_info"), COUNTER_FILE_NAME,
                            string("sub_segment"), string("segment_metrics")));

            FileSystemWrapper::ListDir(segmentDirs[i] + "index/index1", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("dict"), string("posting")));
            FileSystemWrapper::ListDir(segmentDirs[i] + "index/index3", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("dict"), string("posting")));
            FileSystemWrapper::ListDir(segmentDirs[i] + "attribute/attr1", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("data"), string("offset")));
            FileSystemWrapper::ListDir(segmentDirs[i] + "attribute/attr3", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("data"), string("offset")));
            //TODO: sub
            string subSegmentPath = 
                segmentDirs[i] + "/" + SUB_SEGMENT_DIR_NAME + "/";
            FileSystemWrapper::ListDir(subSegmentPath, list);
            EXPECT_THAT(list, UnorderedElementsAre(string("index"), string("summary"), 
                            string("attribute"), string("segment_info"), COUNTER_FILE_NAME));
            FileSystemWrapper::ListDir(subSegmentPath + "index/index1", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("dict"), string("posting")));
            FileSystemWrapper::ListDir(subSegmentPath + "index/index3", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("dict"), string("posting")));
            FileSystemWrapper::ListDir(subSegmentPath + "attribute/attr1", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("data"), string("offset")));
            FileSystemWrapper::ListDir(subSegmentPath + "attribute/attr3", list);
            EXPECT_THAT(list, UnorderedElementsAre(string("data"), string("offset")));

        }
    }

    void TestCaseForDeployIndexFail()
    {
        Version lastVersion;
        SegmentDirectoryPtr segDir(new SegmentDirectory(mRootDir, lastVersion));
        segDir->Init(false, true);
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        IndexPartitionMerger merger(segDir, mSchema, options,
                merger::DumpStrategyPtr(), NULL, plugin::PluginManagerPtr());
        vector<MergePlan> mergePlans;
        MergePlan plan;
        plan.SetTargetSegmentId(0, 2);
        mergePlans.push_back(plan);

        // segment_2_tmp not exist
        Version targetVersion(0);
        targetVersion.AddSegment(2);
        ASSERT_THROW(merger.SinglePartEndMerge(mRootDir, mergePlans, targetVersion),
                     FileIOException);
    }

    void prepareOneInstanceDir(const string &rootDir, uint32_t instanceId)
    {
        string instanceDir = rootDir + "/instance_" + StringUtil::toString(instanceId) + "/";
        FileSystemWrapper::MkDir(instanceDir);
        FileSystemWrapper::MkDir(instanceDir + ADAPTIVE_DICT_DIR_NAME);
        FileSystemWrapper::AtomicStore(instanceDir + ADAPTIVE_DICT_DIR_NAME + 
                "/d" + StringUtil::toString(instanceId), "");
        prepareOneSegmentDir(instanceDir, 2, instanceId);
        prepareOneSegmentDir(instanceDir, 4, instanceId);
    }

    void prepareOneSegmentDir(const string &segmentDir, uint32_t instanceId)
    {
        FileSystemWrapper::MkDir(segmentDir);
        FileSystemWrapper::MkDir(segmentDir + "/index");
        string indexName = "index" + StringUtil::toString(instanceId);
        FileSystemWrapper::MkDir(segmentDir + "/index/" + indexName);
        FileSystemWrapper::AtomicStore(segmentDir + "/index/" + indexName + "/dict", "");
        FileSystemWrapper::AtomicStore(segmentDir + "/index/" + indexName + "/posting", "");
        FileSystemWrapper::MkDir(segmentDir + "/summary");
        FileSystemWrapper::MkDir(segmentDir + "/attribute");
        string attrName = "attr" + StringUtil::toString(instanceId);
        FileSystemWrapper::MkDir(segmentDir + "/attribute/" + attrName);
        FileSystemWrapper::AtomicStore(segmentDir + "/attribute/" + attrName + "/data", "");
        FileSystemWrapper::AtomicStore(segmentDir + "/attribute/" + attrName + "/offset", "");
    }

    void prepareOneSegmentDir(const string &partitionDir, 
                              segmentid_t segmentId, uint32_t instanceId)
    {
        string segmentDir = partitionDir + "/" + "segment_" + StringUtil::toString(segmentId) + "_level_0";
        prepareOneSegmentDir(segmentDir, instanceId);
        prepareOneSegmentDir(segmentDir + "/" + SUB_SEGMENT_DIR_NAME, instanceId);
    }

private:
    void InternalTestCaseForMerge(const string& versionsStr,
                                  const string& mergeTaskStr)
    {
        Version lastVersion;
        SegmentMergeInfos segMergeInfos;
        MakeFakeSegmentDir(versionsStr, lastVersion, segMergeInfos);
        
        SegmentDirectoryPtr segDir(new SegmentDirectory(mRootDir, lastVersion));
        segDir->Init(false, true);
        IndexPartitionOptions option = mOptions;
        IndexPartitionMergerMock merger(segDir, mSchema, option);
        Version mergedVersion;
        SegmentInfos mergedSegInfos;
        MergeTask task;
        MakeMergeTask(mergeTaskStr, segMergeInfos, 
                      task, mergedVersion, mergedSegInfos);
        merger.SetMergeTask(task);
        merger.Merge();
        mergedVersion.SetVersionId(lastVersion.GetVersionId() + 1);
        CheckMergeResult(mergedVersion, mergedSegInfos);
    }

    void MakeFakeSegmentDir(const string& versionsStr, 
                            Version& lastVersion,
                            SegmentMergeInfos& segMergeInfos)
    {
        versionid_t versionId = 0;
        StringTokenizer st(versionsStr, ";", StringTokenizer::TOKEN_TRIM);
        for (size_t i = 0; i < st.getNumTokens(); ++i)
        {
            StringTokenizer st2(st[i], ",",
                    StringTokenizer::TOKEN_IGNORE_EMPTY 
                    | StringTokenizer::TOKEN_TRIM);
            
            segMergeInfos.clear();

            Version version(versionId++);
            size_t globalDocCount = 0;
            for (size_t j = 0; j < st2.getNumTokens(); j += 2)
            {
                segmentid_t segmentId;
                StringUtil::strToInt32(st2[j].c_str(), segmentId);
                version.AddSegment(segmentId);

                string segmentPath = mRootDir + version.GetSegmentDirName(segmentId) + "/";

                FileSystemWrapper::MkDir(segmentPath);

                FileSystemWrapper::MkDir(segmentPath + DELETION_MAP_DIR_NAME);

                SegmentInfo segInfo;
                segInfo.locator.SetLocator(StringUtil::toString(segmentId));
                StringTokenizer st3(st2[j+1], "#", StringTokenizer::TOKEN_TRIM);
                uint32_t docCount = 0;
                if (st3.getNumTokens() != 2)
                {
                    StringUtil::strToUInt32(st2[j+1].c_str(), docCount);
                    segInfo.docCount = docCount;
                }
                else
                {
                    StringUtil::strToUInt32(st3[0].c_str(), docCount);
                    segInfo.docCount = docCount;
                    if (st3[1] == "true")
                    {
                        segInfo.mergedSegment = true;
                    }
                }
                FileSystemWrapper::AtomicStore(segmentPath + COUNTER_FILE_NAME, "{}");

                string segInfoPath = segmentPath + SEGMENT_INFO_FILE_NAME;
                segInfo.Store(segInfoPath);

                DeployIndexWrapper wrapper(mRootDir);
                wrapper.DumpSegmentDeployIndex(version.GetSegmentDirName(segmentId));

                SegmentMergeInfo segMergeInfo;
                segMergeInfo.segmentId = segmentId;
                segMergeInfo.segmentInfo = segInfo;
                segMergeInfo.baseDocId = globalDocCount;
                globalDocCount += docCount;
                segMergeInfos.push_back(segMergeInfo);
            }
            string content = version.ToString();
            stringstream ss;
            ss << mRootDir << VERSION_FILE_NAME_PREFIX << "." 
               << version.GetVersionId();
            FileSystemWrapper::AtomicStore(ss.str(), content);

            if (i == st.getNumTokens() - 1)
            {
                lastVersion = version;
            }
        }
    }

    DeletionMapReaderPtr CreateDeletionMap(Version version, 
            const SegmentMergeInfos& segMergeInfos)
    {
        vector<uint32_t> docCounts;
        for (size_t i = 0; i < segMergeInfos.size(); ++i)
        {
            docCounts.push_back((uint32_t)segMergeInfos[i].segmentInfo.docCount);
        }        
        DeletionMapReaderPtr delMapReader  = IndexTestUtil::CreateDeletionMap(
                version, docCounts, IndexTestUtil::NoDelete);
        return delMapReader;
    }

    void MakeMergeTask(const string& mergeTaskStr, 
                       const SegmentMergeInfos& segMergeInfos,
                       MergeTask& task, 
                       Version& mergedVersion, 
                       SegmentInfos& mergedSegInfos)
    {
        MergeTaskMaker::CreateMergeTask(mergeTaskStr, task, segMergeInfos);

        mergedSegInfos.resize(task.GetPlanCount());
        segmentid_t segId = 0;
        for (size_t i = 0; i < segMergeInfos.size(); i++)
        {
            segId = segMergeInfos[i].segmentId;
            bool inPlan = false;
            for (size_t j = 0; j < task.GetPlanCount(); j++)
            {
                if (task[j].HasSegment(segId))
                {
                    mergedSegInfos[j].docCount += 
                        segMergeInfos[i].segmentInfo.docCount;
                    mergedSegInfos[j].locator = 
                        (segMergeInfos.rbegin())->segmentInfo.locator;
                    mergedSegInfos[j].mergedSegment = true;
                    inPlan = true;
                    break;
                }
                
            }
            if (!inPlan)
            {
                mergedVersion.AddSegment(segId);
            }
        }
        for (size_t i = 0; i < task.GetPlanCount(); i++)
        {
            segId++;
            mergedVersion.AddSegment(segId);
        }
    }

    void MakeInMemMergeAnswer(const vector<InMemorySegmentReaderPtr>& inMemSegReaders,
                              Version& mergedVersion, 
                              SegmentInfos& mergedSegInfos)
    {
        SegmentInfo segInfo;
        segInfo.docCount = 0;
        for (size_t i = 0; i < inMemSegReaders.size(); ++i)
        {
            const SegmentInfo& readerSegInfo = inMemSegReaders[i]->GetSegmentInfo();
            segInfo.docCount += readerSegInfo.docCount;

            int64_t int64Locator = StringUtil::fromString<int64_t>(segInfo.locator.ToString());
            int64_t int64Locator2 = StringUtil::fromString<int64_t>(readerSegInfo.locator.ToString());
            int64Locator += int64Locator2;
            segInfo.locator.SetLocator(StringUtil::toString(int64Locator)) ;
        }
        const SegmentInfo& lastSegInfo = inMemSegReaders.back()->GetSegmentInfo();
        segInfo.locator = lastSegInfo.locator;

        mergedSegInfos.push_back(segInfo);
        segmentid_t lastSegId = inMemSegReaders.back()->GetSegmentId();
        mergedVersion.AddSegment(lastSegId + 1);
    }

    void CheckMergeResult(const Version& mergedVersion, const SegmentInfos& mergedSegInfos)
    {
        stringstream ss;
        ss << mRootDir << VERSION_FILE_NAME_PREFIX << "." 
           << mergedVersion.GetVersionId();
        Version version;
        INDEXLIB_TEST_TRUE(version.Load(ss.str()));
        
        INDEXLIB_TEST_EQUAL(mergedVersion, version);

        SegmentDirectory segDir(mRootDir, mergedVersion);
        segDir.Init(false, true);
        vector<string> segPaths;
        SegmentDirectory::Iterator segPathIter = segDir.CreateIterator();
        while (segPathIter.HasNext())
        {
            segPaths.push_back(segPathIter.Next());
        }
        
        INDEXLIB_TEST_TRUE(segPaths.size() >= mergedSegInfos.size());

        for (size_t i = segPaths.size() - mergedSegInfos.size(), j = 0; 
             i < segPaths.size(); i++, j++)
        {
            SegmentInfo segInfo;
            string deployIndexFile = FileSystemWrapper::JoinPath(
                    segPaths[i], SEGMENT_FILE_LIST);
            INDEXLIB_TEST_TRUE(FileSystemWrapper::IsExist(deployIndexFile));
            INDEXLIB_TEST_TRUE(segInfo.Load(FileSystemWrapper::JoinPath(segPaths[i], SEGMENT_INFO_FILE_NAME)));
            INDEXLIB_TEST_EQUAL(mergedSegInfos[j], segInfo);
        }
        string truncateMetaIndex = FileSystemWrapper::JoinPath(
                mRootDir, TRUNCATE_META_DIR_NAME);
        
        if(FileSystemWrapper::IsExist(truncateMetaIndex))
        {
            string deployIndexFile = FileSystemWrapper::JoinPath(
                    truncateMetaIndex, SEGMENT_FILE_LIST);
            INDEXLIB_TEST_TRUE(FileSystemWrapper::IsExist(deployIndexFile));
        }
    }
private:
    std::string mRootDir;
    IndexPartitionSchemaPtr mSchema;
    IndexPartitionOptions mOptions;
};

INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForMerge);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForMergeWithDeployIndex);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForMergeMultiVersion);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForCachedSegmentInfo);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForInvalidMergePlan);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForCreateMergeScheduler);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForMkDirIfNotExist);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForSplitFileName);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForMoveFiles);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForListInstanceFiles);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForListInstanceDirs);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForPrepareMergeDir);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForMergeInstanceDir);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForDeployIndexFail);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerTest, TestCaseForMergeWithEmptyInstance);
IE_NAMESPACE_END(merger);
