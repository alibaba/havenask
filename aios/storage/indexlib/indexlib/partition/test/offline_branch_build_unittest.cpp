#include "autil/Thread.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/inc_parallel_partition_merger.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/test/fake_partition.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/unittest.h"
#include "indexlib/testlib/indexlib_partition_creator.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::testlib;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::test;
using namespace indexlib::merger;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {

class OfflineBranchBuildTest : public INDEXLIB_TESTBASE
{
public:
    OfflineBranchBuildTest();
    ~OfflineBranchBuildTest();

    DECLARE_CLASS_NAME(OfflineBranchBuildTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestBranchCreatedFromBrokenSrc();
    void TestBranchBrokenSrcForEntryTable();
    void TestMultiLevelBranch();
    void TestPrepareMergeFailover();
    void TestMultiTheadBuild();
    void TestFullBuildAndMerge();
    void TestParalleFullBuildAndMerge();

private:
    void BuildParallelIncData(const std::string& incDocString, uint32_t fsBranch, ParallelBuildInfo& info);
    void BuildParallelIncDataWithReTry(const std::string& incDocString, uint32_t fsBranch, ParallelBuildInfo& info);
    void MergeParallelIncData(uint32_t fsBranch, ParallelBuildInfo& info, MergeMetaPtr meta);
    void MergeParallelIncDataWithReTry(uint32_t fsBranch, ParallelBuildInfo& info, MergeMetaPtr meta);

private:
    std::string mRootPath;
    file_system::DirectoryPtr mRootDirectory;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(partition, OfflineBranchBuildTest);

INDEXLIB_UNIT_TEST_CASE(OfflineBranchBuildTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OfflineBranchBuildTest, TestBranchCreatedFromBrokenSrc);
INDEXLIB_UNIT_TEST_CASE(OfflineBranchBuildTest, TestBranchBrokenSrcForEntryTable);
INDEXLIB_UNIT_TEST_CASE(OfflineBranchBuildTest, TestPrepareMergeFailover);
INDEXLIB_UNIT_TEST_CASE(OfflineBranchBuildTest, TestMultiLevelBranch);
INDEXLIB_UNIT_TEST_CASE(OfflineBranchBuildTest, TestMultiTheadBuild);
INDEXLIB_UNIT_TEST_CASE(OfflineBranchBuildTest, TestFullBuildAndMerge);
INDEXLIB_UNIT_TEST_CASE(OfflineBranchBuildTest, TestParalleFullBuildAndMerge);

OfflineBranchBuildTest::OfflineBranchBuildTest() {}

OfflineBranchBuildTest::~OfflineBranchBuildTest() {}

void OfflineBranchBuildTest::CaseSetUp()
{
    string field = "string1:string;string2:string;score:float";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    string attribute = "score";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mRootPath = GET_TEMP_DATA_PATH();
    mRootDirectory = GET_PARTITION_DIRECTORY();
}

void OfflineBranchBuildTest::CaseTearDown() {}

void OfflineBranchBuildTest::TestSimpleProcess()
{
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOnlineConfig().maxCurReaderReclaimableMem = 64;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    string docString = "cmd=add,string1=hello,score=1";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath, "psm", "", /*FsBranch Id*/ 1));
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath, "psm", "", /*FsBranch Id*/ 0));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    {
        vector<string> mergeSrcs;
        mergeSrcs.push_back(mRootPath);
        merger::PartitionMergerPtr multiPartMerger(
            merger::PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(mergeSrcs, mRootPath, mOptions, NULL,
                                                                                   ""));
        multiPartMerger->Merge(true);
    }
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello", "score=1"));
    string incDocString = "cmd=add,string1=world,score=2";
    {
        ParallelBuildInfo info;
        info.parallelNum = 1;
        info.batchId = 1;
        info.instanceId = 0;
        BuildParallelIncData(incDocString, /*FsBranch Id*/ 1, info);
        {
            // INSTANCE_TARGET_VERSION_FILE must be created by master branch
            merger::PartitionMergerPtr merger(
                (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateIncParallelPartitionMerger(
                    mRootPath, info, mOptions, NULL, ""));
            auto incParallelPartitionMerger = dynamic_pointer_cast<IncParallelPartitionMerger>(merger);
            incParallelPartitionMerger->PrepareMerge(0);
            auto innerMerger = incParallelPartitionMerger->mMerger;
            auto meta = incParallelPartitionMerger->CreateMergeMeta(false, 1, 0);
            string instanceRootPath;
            incParallelPartitionMerger->mMerger->PrepareMergeDir(0, meta, instanceRootPath);
        }
        unique_ptr<IndexPartitionMerger> merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                mRootPath, info, mOptions, NULL, "", PartitionRange(), CommonBranchHinterOption::Test(2)));

        MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
        merger->DoMerge(false, meta, 0);
        merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
        // test upc, end merge do twice
        merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
    }
    psm.CreateOnlinePartition(INVALID_VERSIONID);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:world", "score=2"));
}

void OfflineBranchBuildTest::TestFullBuildAndMerge()
{
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOnlineConfig().maxCurReaderReclaimableMem = 64;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath, "psm", "", /*FsBranch Id*/ 1));
        string docString = "cmd=add,string1=hello,score=1;cmd=add,string1=world,score=2;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    }

    {
        merger::PartitionMergerPtr singlePartMerger(
            merger::PartitionMergerCreator::TEST_CreateSinglePartitionMerger(mRootPath, mOptions, NULL, ""));
        singlePartMerger->Merge(true);
    }
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath, "psm", ""));
    psm.CreateOnlinePartition(INVALID_VERSIONID);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:world", "score=2"));
}

void OfflineBranchBuildTest::TestParalleFullBuildAndMerge()
{
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOnlineConfig().maxCurReaderReclaimableMem = 64;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, PathUtil::JoinPath(mRootPath, "part1"), "psm", "", /*FsBranch Id*/ 1));
        string docString = "cmd=add,string1=hello,score=1;cmd=add,string1=world,score=2;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    }
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, PathUtil::JoinPath(mRootPath, "part2"), "psm", "", /*FsBranch Id*/ 1));
        string docString = "cmd=add,string1=america,score=1;cmd=add,string1=china,score=2;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    }
    vector<string> mergeSrcs;
    mergeSrcs.push_back(PathUtil::JoinPath(mRootPath, "part1"));
    mergeSrcs.push_back(PathUtil::JoinPath(mRootPath, "part2"));
    {
        // INSTANCE_TARGET_VERSION_FILE must be created by master branch
        merger::PartitionMergerPtr multiPartMerger(
            merger::PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(mergeSrcs, mRootPath, mOptions, NULL,
                                                                                   ""));
        auto indexPartitionMerger = dynamic_pointer_cast<IndexPartitionMerger>(multiPartMerger);
        auto meta = indexPartitionMerger->CreateMergeMeta(true, 1, 0);
        string instanceRootPath;
        indexPartitionMerger->PrepareMergeDir(0, meta, instanceRootPath);
    }
    merger::PartitionMergerPtr multiPartMerger(merger::PartitionMergerCreator::CreateFullParallelPartitionMerger(
        mergeSrcs, mRootPath, mOptions, NULL, "", PartitionRange(), CommonBranchHinterOption::Test(1)));
    multiPartMerger->Merge(true);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath, "psm", ""));
    psm.CreateOnlinePartition(INVALID_VERSIONID);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:world", "score=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:america", "score=1"));
}

void OfflineBranchBuildTest::TestBranchCreatedFromBrokenSrc()
{
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOnlineConfig().maxCurReaderReclaimableMem = 64;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    string docString = "cmd=add,string1=america,score=1;cmd=add,string1=china,score=2;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath, "psm", "", /*FsBranch Id*/ 0));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    string incDocString = "cmd=add,string1=world,score=2";
    {
        ParallelBuildInfo info;
        info.parallelNum = 1;
        info.batchId = 1;
        info.instanceId = 0;

        BuildParallelIncData(incDocString, /*FsBranch Id*/ 0, info);
        // fake a broken segment
        FslibWrapper::DeleteFileE(PathUtil::JoinPath(mRootPath, "segment_0_level_0/segment_info"),
                                  DeleteOption::NoFence(false));
        BuildParallelIncData(incDocString, /*FsBranch Id*/ 1, info);

        unique_ptr<IndexPartitionMerger> merger(
            (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateIncParallelPartitionMerger(mRootPath, info,
                                                                                                 mOptions, NULL, ""));
        merger->PrepareMerge(0);
        MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
        merger->DoMerge(false, meta, 0);

        string branchRootPath =
            PathUtil::JoinPath(mRootPath, "instance_0", merger->TEST_GetBranchFs()->GetBranchName());
        FslibWrapper::DeleteFileE(PathUtil::JoinPath(branchRootPath, "check_points"), DeleteOption::NoFence(false));
        FslibWrapper::DeleteFileE(PathUtil::JoinPath(branchRootPath, "entry_table.preload"),
                                  DeleteOption::NoFence(false));
        FslibWrapper::DeleteFileE(PathUtil::JoinPath(branchRootPath, "entry_table.preload.back"),
                                  DeleteOption::NoFence(false));
        unique_ptr<IndexPartitionMerger> mergerBranch1(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                mRootPath, info, mOptions, NULL, "", PartitionRange(), CommonBranchHinterOption::Test(1)));

        mergerBranch1->DoMerge(false, meta, 0);
        mergerBranch1->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
        // test upc, end merge do twice
        mergerBranch1->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
    }
    psm.CreateOnlinePartition(INVALID_VERSIONID);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:world", "score=2"));
}

void OfflineBranchBuildTest::TestBranchBrokenSrcForEntryTable()
{
    string field = "key:string;value:uint64;";
    mSchema = SchemaMaker::MakeKVSchema(field, "key", "value", INVALID_TTL, "plain");

    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 1;
    mOptions.GetOfflineConfig().buildConfig.enablePackageFile = false;
    mOptions.GetBuildConfig(true).buildTotalMemory = 22;
    mOptions.GetBuildConfig(false).buildTotalMemory = 40;
    mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    mOptions.GetBuildConfig(false).levelNum = 2;
    mOptions.GetBuildConfig(false).shardingColumnNum = 2;
    mOptions.GetBuildConfig(false).keepVersionCount = 100;

    string docString1 = "cmd=add,key=doc1,value=1;";
    string docString2 = "cmd=add,key=doc2,value=2;";

    PartitionStateMachine psm1;
    ASSERT_TRUE(psm1.Init(mSchema, mOptions, mRootPath, "psm1", "", /*FsBranch Id*/ 1));
    ASSERT_TRUE(psm1.Transfer(BUILD_FULL_NO_MERGE, docString1, "", ""));

    // make a broken version
    string branchName1;
    fslib::FileList fileList;
    ASSERT_TRUE(FslibWrapper::ListDir(mRootPath, fileList).OK());
    for (const auto& path : fileList) {
        if (autil::StringUtil::startsWith(path, BRANCH_DIR_NAME_PREFIX)) {
            ASSERT_TRUE(branchName1.empty()) << path << ":" << branchName1;
            branchName1 = path;
        }
    }
    auto deleteOption = DeleteOption::NoFence(false);
    string branchPath1 = PathUtil::JoinPath(mRootPath, branchName1);
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(mRootPath, "__MARKED_BRANCH_INFO"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath1, "entry_table.1"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath1, "deploy_meta.1"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath1, "version.1"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath1, "entry_table.0"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath1, "deploy_meta.0"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath1, "version.0"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteDir(PathUtil::JoinPath(branchPath1, "segment_1_level_0"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteDir(PathUtil::JoinPath(branchPath1, "summary_info"), deleteOption).OK());
    ASSERT_TRUE(
        FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath1, "segment_0_level_0/segment_info"), deleteOption).OK());
    ASSERT_TRUE(
        FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath1, "segment_0_level_0/segment_file_list"), deleteOption)
            .OK());
    ASSERT_TRUE(
        FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath1, "segment_0_level_0/deploy_index"), deleteOption).OK());
    ASSERT_TRUE(
        FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath1, "segment_0_level_0/counter"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath1, "segment_0_level_0/column_1/index/key/key"),
                                         deleteOption)
                    .OK());
    // make a unsealed file
    ASSERT_TRUE(
        FslibWrapper::Store(PathUtil::JoinPath(branchPath1, "segment_0_level_0/column_1/index/key/key"), "").OK());

    PartitionStateMachine psm2;
    ASSERT_TRUE(psm2.Init(mSchema, mOptions, mRootPath, "psm2", "", /*FsBranch Id*/ 2));
    ASSERT_TRUE(psm2.Transfer(BUILD_FULL_NO_MERGE, docString1 + docString2, "", ""));

    string branchName2;
    fileList.clear();
    ASSERT_TRUE(FslibWrapper::ListDir(mRootPath, fileList).OK());
    for (const auto& path : fileList) {
        if (autil::StringUtil::startsWith(path, BRANCH_DIR_NAME_PREFIX) && path != branchName1) {
            ASSERT_TRUE(branchName2.empty()) << path << ":" << branchName2 << ":" << branchName1;
            branchName2 = path;
        }
    }
    string branchPath2 = PathUtil::JoinPath(mRootPath, branchName2);
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(mRootPath, "__MARKED_BRANCH_INFO"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath2, "entry_table.1"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath2, "deploy_meta.1"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath2, "version.1"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath2, "entry_table.0"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath2, "deploy_meta.0"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteFile(PathUtil::JoinPath(branchPath2, "version.0"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteDir(PathUtil::JoinPath(branchPath2, "summary_info"), deleteOption).OK());
    ASSERT_TRUE(FslibWrapper::DeleteDir(PathUtil::JoinPath(branchPath2, "segment_2_level_0"), deleteOption).OK());

    PartitionStateMachine psm3;
    ASSERT_TRUE(psm3.Init(mSchema, mOptions, mRootPath, "psm3", "", /*FsBranch Id*/ 3));
    ASSERT_TRUE(psm3.Transfer(BUILD_FULL_NO_MERGE, docString2, "", ""));

    // Merge
    vector<string> mergeSrcs;
    mergeSrcs.push_back(mRootPath);
    merger::PartitionMergerPtr multiPartMerger(merger::PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(
        mergeSrcs, mRootPath, mOptions, NULL, ""));
    multiPartMerger->Merge(true);

    // Query
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath, "psm", "", /*FsBranch Id*/ 0));
    psm.CreateOnlinePartition(INVALID_VERSIONID);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "doc1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "doc2", "value=2"));
}

void OfflineBranchBuildTest::TestMultiLevelBranch()
{
    // mOptions.GetOnlineConfig().buildConfig.maxDocCount = 2;
    // mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;
    // mOptions.GetOnlineConfig().maxCurReaderReclaimableMem = 64;
    // mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    // string docString = "cmd=add,string1=america,score=1;cmd=add,string1=china,score=2;";

    // PartitionStateMachine psm;
    // ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath, "psm", "", /*FsBranch Id*/0));
    // ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
}

void OfflineBranchBuildTest::TestPrepareMergeFailover()
{
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOnlineConfig().maxCurReaderReclaimableMem = 64;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    string docString = "cmd=add,string1=america,score=1;cmd=add,string1=china,score=2;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath, "psm", "", /*FsBranch Id*/ 0));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ParallelBuildInfo info;
    info.parallelNum = 1;
    info.batchId = 1;
    info.instanceId = 0;
    {
        string incDocString = "cmd=add,string1=world,score=2;"
                              "cmd=add,string1=america,score=3;";
        BuildParallelIncData(incDocString, /*FsBranch Id*/ 0, info);
    }
    // {
    //     /*
    //       To fake prepare merge failed
    //     */
    //     mRootDirectory->RemoveDirectory("parallel_1_0", false);
    // }
    unique_ptr<IndexPartitionMerger> merger(
        (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateIncParallelPartitionMerger(mRootPath, info, mOptions,
                                                                                             NULL, ""));
    ASSERT_NO_THROW(merger->PrepareMerge(0));
}

void OfflineBranchBuildTest::TestMultiTheadBuild()
{
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetOnlineConfig().maxCurReaderReclaimableMem = 64;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    string docString = "cmd=add,string1=america,score=1;cmd=add,string1=china,score=2;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath, "psm", "", /*FsBranch Id*/ 0));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    ParallelBuildInfo info;
    info.parallelNum = 1;
    info.batchId = 1;
    info.instanceId = 0;
    {
        std::vector<autil::ThreadPtr> branchBuildThreads;
        string incDocString = "cmd=add,string1=world,score=2;"
                              "cmd=add,string1=america,score=3;"
                              "cmd=add,string1=africa,score=4;"
                              "cmd=add,string1=china,score=0;";
        for (size_t i = 1; i < 40; ++i) {
            autil::ThreadPtr thread = autil::Thread::createThread(
                std::bind(&OfflineBranchBuildTest::BuildParallelIncDataWithReTry, this, incDocString, i, info));
            branchBuildThreads.emplace_back(thread);
        }
        for (auto& thread : branchBuildThreads) {
            thread->join();
        }
        branchBuildThreads.clear();
    }
    merger::PartitionMergerPtr merger(
        (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateIncParallelPartitionMerger(mRootPath, info, mOptions,
                                                                                             NULL, ""));
    auto incParallelPartitionMerger = dynamic_pointer_cast<IncParallelPartitionMerger>(merger);
    incParallelPartitionMerger->PrepareMerge(0);
    MergeMetaPtr meta = incParallelPartitionMerger->CreateMergeMeta(false, 1, 0);
    {
        // INSTANCE_TARGET_VERSION_FILE must be created by master branch
        auto innerMerger = incParallelPartitionMerger->mMerger;
        string instanceRootPath;
        innerMerger->PrepareMergeDir(0, meta, instanceRootPath);
    }
    {
        std::vector<autil::ThreadPtr> branchMergeThreads;
        for (size_t i = 0; i < 40; ++i) {
            autil::ThreadPtr thread = autil::Thread::createThread(
                std::bind(&OfflineBranchBuildTest::MergeParallelIncDataWithReTry, this, i, info, meta));
            branchMergeThreads.emplace_back(thread);
        }
        branchMergeThreads.clear();
    }
    merger::PartitionMergerPtr merger2(
        (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateIncParallelPartitionMerger(mRootPath, info, mOptions,
                                                                                             NULL, ""));
    auto incParallelPartitionMerger2 = dynamic_pointer_cast<IncParallelPartitionMerger>(merger2);
    incParallelPartitionMerger2->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
    psm.CreateOnlinePartition(INVALID_VERSIONID);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:china", "score=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:world", "score=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:america", "score=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:africa", "score=4"));
}

void OfflineBranchBuildTest::BuildParallelIncDataWithReTry(const std::string& incDocString, uint32_t fsBranch,
                                                           ParallelBuildInfo& info)
{
    uint32_t tryCount = 0;
    while (true) {
        try {
            BuildParallelIncData(incDocString, fsBranch, info);
        } catch (...) {
            if (++tryCount == 3) {
                throw;
            }
            sleep(1);
            continue;
        }
        break;
    }
}

void OfflineBranchBuildTest::MergeParallelIncDataWithReTry(uint32_t fsBranch, ParallelBuildInfo& info,
                                                           MergeMetaPtr meta)
{
    uint32_t tryCount = 0;
    while (true) {
        try {
            MergeParallelIncData(fsBranch, info, meta);
        } catch (...) {
            if (++tryCount == 3) {
                throw;
            }
            sleep(1);
            continue;
        }
        break;
    }
}
void OfflineBranchBuildTest::BuildParallelIncData(const std::string& incDocString, uint32_t fsBranch,
                                                  ParallelBuildInfo& info)
{
    mOptions.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    IndexlibPartitionCreator::BuildParallelIncData(mSchema, mRootPath, info, incDocString, mOptions,
                                                   0 /*not INVALID_TIMESTAMP*/, /*FsBranch Id*/ fsBranch);
}

void OfflineBranchBuildTest::MergeParallelIncData(uint32_t fsBranch, ParallelBuildInfo& info, MergeMetaPtr meta)
{
    unique_ptr<IndexPartitionMerger> merger(
        (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
            mRootPath, info, mOptions, NULL, "", PartitionRange(), CommonBranchHinterOption::Test(fsBranch)));
    merger->DoMerge(false, meta, 0);
}
}} // namespace indexlib::partition
