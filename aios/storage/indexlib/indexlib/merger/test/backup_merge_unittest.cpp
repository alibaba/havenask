#include "autil/Thread.h"
#include "fslib/cache/FSCacheModule.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/config/test/truncate_config_maker.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::table;
using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::partition;

namespace indexlib { namespace merger {

class BackupMergeTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    BackupMergeTest();
    ~BackupMergeTest();

    DECLARE_CLASS_NAME(BackupMergeTest);

public:
    void CaseSetUp() override
    {
        if (!GET_CASE_PARAM()) {
            setenv("FSLIB_ENABLE_META_CACHE", "true", 1);
            setenv("FSLIB_CACHE_SUPPORTING_FS_TYPE", "LOCAL", 1);
            FileSystem::getCacheModule()->reset();
        }

        string field = "string1:string;price:uint32;count:uint32";
        string index = "pk:primarykey64:string1";
        string attribute = "price;count";
        mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    }

    void CaseTearDown() override
    {
        unsetenv("FSLIB_ENABLE_META_CACHE");
        unsetenv("FSLIB_CACHE_SUPPORTING_FS_TYPE");
        FileSystem::getCacheModule()->reset();
    }

    void MergeFullParallel(uint32_t fsBranch, const vector<string>& mergeSrcs, const MergeMetaPtr& meta,
                           const IndexPartitionOptions& options);

    void MergeFullParallelWithRetry(uint32_t fsBranch, const vector<string>& mergeSrcs, const MergeMetaPtr& meta,
                                    const IndexPartitionOptions& options);

    void TestSimpleProcess();
    void TestTruncateIndex();

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    void MakeOnePartitionDataWithBranchId(const config::IndexPartitionSchemaPtr& schema,
                                          const config::IndexPartitionOptions& options, const std::string& partRootDir,
                                          const std::string& docStrs, uint32_t branchId)
    {
        string rootDir = GET_TEMP_DATA_PATH() + partRootDir;
        PartitionStateMachine psm;
        psm.Init(schema, options, rootDir, "psm", "", /*FsBranch Id*/ branchId);
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrs, "", ""));
    }

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(UsePackFile, BackupMergeTest, testing::Values(true, false));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(BackupMergeTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(BackupMergeTest, TestTruncateIndex);

IE_LOG_SETUP(merger, BackupMergeTest);

BackupMergeTest::BackupMergeTest() {}

BackupMergeTest::~BackupMergeTest() {}

void BackupMergeTest::TestSimpleProcess()
{
    string part1DocString = "cmd=add,string1=hello,price=4,count=2;"
                            "cmd=add,string1=part1,price=4,count=1;";

    string part2DocString = "cmd=add,string1=world,price=4,count=2;";

    string part3DocString = "";

    IndexPartitionOptions options;
    BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
    buildConfig.enablePackageFile = GET_CASE_PARAM();
    buildConfig.maxDocCount = 1;

    MakeOnePartitionDataWithBranchId(mSchema, options, "part1", part1DocString, 0);
    FslibWrapper::DeleteDirE(GET_TEMP_DATA_PATH() + "part1/segment_1_level_0", DeleteOption::NoFence(true));
    FslibWrapper::DeleteFileE(GET_TEMP_DATA_PATH() + "part1/entry_table.preload", DeleteOption::NoFence(true));
    FslibWrapper::DeleteFileE(GET_TEMP_DATA_PATH() + "part1/entry_table.0", DeleteOption::NoFence(true));
    FslibWrapper::DeleteFileE(GET_TEMP_DATA_PATH() + "part1/version.0", DeleteOption::NoFence(true));
    FslibWrapper::DeleteFileE(GET_TEMP_DATA_PATH() + "part1/deploy_meta.0", DeleteOption::NoFence(true));
    FslibWrapper::DeleteFileE(GET_TEMP_DATA_PATH() + "part1/summary_info", DeleteOption::NoFence(true));
    FslibWrapper::DeleteFileE(GET_TEMP_DATA_PATH() + "part1/" + MARKED_BRANCH_INFO, DeleteOption::NoFence(true));
    MakeOnePartitionDataWithBranchId(mSchema, options, "part1", part1DocString, 1);
    MakeOnePartitionDataWithBranchId(mSchema, options, "part1", part1DocString, 2);

    MakeOnePartitionDataWithBranchId(mSchema, options, "part2", part2DocString, 0);
    MakeOnePartitionDataWithBranchId(mSchema, options, "part3", part3DocString, 0);

    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part2");
    mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part3");
    string mergePartPath = GET_TEMP_DATA_PATH() + "mergePart";

    options.SetIsOnline(false);
    IndexPartitionMergerPtr partitionMerger(
        (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(mergeSrcs, mergePartPath,
                                                                                              options, NULL, ""));

    ASSERT_NO_THROW(partitionMerger->PrepareMerge(0));
    MergeMetaPtr mergeMeta = partitionMerger->CreateMergeMeta(false, 1, 0);
    {
        IndexPartitionMergerPtr partitionMerger0(
            (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(
                mergeSrcs, mergePartPath, options, NULL, ""));

        partitionMerger0->DoMerge(false, mergeMeta, 0);
        FslibWrapper::DeleteDirE(GET_TEMP_DATA_PATH() + "mergePart/instance_0/check_points",
                                 DeleteOption::NoFence(true));
        FslibWrapper::DeleteFileE(GET_TEMP_DATA_PATH() + "mergePart/instance_0/__MARKED_BRANCH_INFO",
                                  DeleteOption::NoFence(true));
        {
            auto mergeFileSystem = partitionMerger0->CreateMergeFileSystem(0, mergeMeta);
            mergeFileSystem->MakeCheckpoint("MergePlan_0_attribute_price_0.checkpoint");
            mergeFileSystem->Close();
        }

        IndexPartitionMergerPtr partitionMerger0_back(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateFullParallelPartitionMerger(
                mergeSrcs, mergePartPath, options, NULL, "", PartitionRange(), CommonBranchHinterOption::Test(1)));
        partitionMerger0_back->DoMerge(false, mergeMeta, 0);
    }
    // old worker can't operate
    ASSERT_THROW(partitionMerger->EndMerge(mergeMeta, 0), util::RuntimeException);
    {
        IndexPartitionMergerPtr partitionMerger1(
            (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(
                mergeSrcs, mergePartPath, options, NULL, ""));
        partitionMerger1->EndMerge(mergeMeta, 0);
    }
    PartitionStateMachine psm;
    psm.Init(mSchema, options, mergePartPath);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello", "price=4,count=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:part1", "price=4,count=1;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:world", "price=4,count=2;"));
}

void BackupMergeTest::TestTruncateIndex()
{
    string field = "string:string;text2:text;text:text;number:uint32;pk:uint64;"
                   "ends:uint32;nid:uint32";
    string index = "index_string:string:string;index_pk:primarykey64:pk;"
                   "index_text2:text:text2;index_text:text:text;index_number:number:number";
    string attribute = "string;number;pk;ends;nid";
    string summary = "string;number;pk";
    auto schema = SchemaMaker::MakeSchema(field, index, attribute, summary);

    SchemaMaker::SetAdaptiveHighFrequenceDictionary("index_text2", "DOC_FREQUENCY#1", indexlib::index::hp_both, schema);
    TruncateParams param("3:10000000", "",
                         "index_text=ends:ends#DESC|nid#ASC:::;"
                         "index_text2=ends:ends#DESC|nid#ASC:::",
                         /*inputStrategyType=*/"default");
    TruncateConfigMaker::RewriteSchema(schema, param);

    IndexPartitionOptions options;
    BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
    buildConfig.enablePackageFile = GET_CASE_PARAM();
    options.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);
    options.GetMergeConfig().maxMemUseForMerge = 1;
    string fullDocs1 = "cmd=add,string=hello1,text=text1 text2 text3,"
                       "text2=text1 text2 text3,number=1,pk=1,ends=1,nid=1;";
    string fullDocs2 = "cmd=add,string=hello2,text=text1 text2 text3,"
                       "text2=text1 text2 text3,number=2,pk=2,ends=2,nid=2;";

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(
            psm.Init(schema, options, PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "part1"), "psm", "", /*FsBranch Id*/ 0));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs1, "", ""));
    }
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(
            psm.Init(schema, options, PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "part2"), "psm", "", /*FsBranch Id*/ 1));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs2, "", ""));
    }
    vector<string> mergeSrcs;
    mergeSrcs.push_back(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "part1"));
    mergeSrcs.push_back(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "part2"));

    // INSTANCE_TARGET_VERSION_FILE must be created by master branch
    merger::PartitionMergerPtr multiPartMerger(merger::PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(
        mergeSrcs, GET_TEMP_DATA_PATH(), options, NULL, ""));
    auto indexPartitionMerger = dynamic_pointer_cast<IndexPartitionMerger>(multiPartMerger);
    indexPartitionMerger->PrepareMerge(0);

    string mergeMetaPath = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "merge_meta");
    auto meta = indexPartitionMerger->CreateMergeMeta(true, 1, 0);
    meta->Store(mergeMetaPath);
    string instanceRootPath;
    indexPartitionMerger->PrepareMergeDir(0, meta, instanceRootPath);
    vector<MergeMetaPtr> mergeMetas;
    for (size_t i = 0; i < 20; ++i) {
        MergeMetaPtr mergeMeta(new IndexMergeMeta());
        mergeMeta->Load(mergeMetaPath);
        mergeMetas.emplace_back(mergeMeta);
    }
    {
        std::vector<autil::ThreadPtr> branchMergeThreads;
        for (size_t i = 0; i < 20; ++i) {
            autil::ThreadPtr thread = autil::Thread::createThread(
                std::bind(&BackupMergeTest::MergeFullParallelWithRetry, this, i, mergeSrcs, mergeMetas[i], options));
            branchMergeThreads.emplace_back(thread);
        }
        branchMergeThreads.clear();
    }
    ASSERT_THROW(indexPartitionMerger->EndMerge(meta), util::RuntimeException);
    {
        merger::PartitionMergerPtr multiPartMerger(
            merger::PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(mergeSrcs, GET_TEMP_DATA_PATH(),
                                                                                   options, NULL, ""));
        auto indexPartitionMerger = dynamic_pointer_cast<IndexPartitionMerger>(multiPartMerger);
        indexPartitionMerger->EndMerge(meta);
    }
    auto ec = FslibWrapper::IsExist(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "truncate_meta", "index_text2_ends"));
    ASSERT_EQ(ec.result, true);
    ec = FslibWrapper::IsExist(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "truncate_meta", "index_text_ends"));
    ASSERT_EQ(ec.result, true);
}

void BackupMergeTest::MergeFullParallelWithRetry(uint32_t fsBranch, const vector<string>& mergeSrcs,
                                                 const MergeMetaPtr& meta, const IndexPartitionOptions& options)
{
    uint32_t tryCount = 0;
    while (true) {
        try {
            MergeFullParallel(fsBranch, mergeSrcs, meta, options);
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

void BackupMergeTest::MergeFullParallel(uint32_t fsBranch, const vector<string>& mergeSrcs, const MergeMetaPtr& meta,
                                        const IndexPartitionOptions& options)

{
    merger::PartitionMergerPtr multiPartMerger(merger::PartitionMergerCreator::CreateFullParallelPartitionMerger(
        mergeSrcs, GET_TEMP_DATA_PATH(), options, NULL, "", PartitionRange(),
        CommonBranchHinterOption::Test(fsBranch)));
    auto indexPartitionMerger = dynamic_pointer_cast<IndexPartitionMerger>(multiPartMerger);
    indexPartitionMerger->DoMerge(false, meta, 0);
}
}} // namespace indexlib::merger
