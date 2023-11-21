#include <random>

#include "fslib/fs/ExceptionTrigger.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/config/test/truncate_config_maker.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/unittest.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using fslib::fs::ExceptionTrigger;

using namespace indexlib::test;
using namespace indexlib::testlib;
using namespace indexlib::merger;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {

class MergeExceptionTest : public INDEXLIB_TESTBASE
{
public:
    MergeExceptionTest() {}
    ~MergeExceptionTest() {}
    DECLARE_CLASS_NAME(MergeExceptionTest);

public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}
    void TestEndMergeBugForTruncateMeta();
};

INDEXLIB_UNIT_TEST_CASE(MergeExceptionTest, TestEndMergeBugForTruncateMeta);

void MergeExceptionTest::TestEndMergeBugForTruncateMeta()
{
    string field = "string:string;text2:text;text:text;number:uint32;pk:uint64;ends:uint32;nid:uint32";
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
    buildConfig.enablePackageFile = true;
    options.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);
    options.GetMergeConfig().maxMemUseForMerge = 1;
    {
        PartitionStateMachine psm;
        string fullDocs1 =
            "cmd=add,string=hello1,text=text1 text2 text3,text2=text1 text2 text3,number=1,pk=1,ends=1,nid=1;";
        ASSERT_TRUE(
            psm.Init(schema, options, PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "part1"), "psm", "", /*FsBranch Id*/ 0));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs1, "", ""));
    }

    vector<string> mergeSrcs {PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "part1")};
    merger::PartitionMergerPtr multiPartMerger(merger::PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(
        mergeSrcs, GET_TEMP_DATA_PATH(), options, NULL, ""));
    auto beginMerger = dynamic_pointer_cast<IndexPartitionMerger>(multiPartMerger);
    beginMerger->PrepareMerge(0);

    string mergeMetaPath = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "merge_meta");
    auto mergeMeta = beginMerger->CreateMergeMeta(true, 1, 0);
    mergeMeta->Store(mergeMetaPath);
    string instanceRootPath;
    beginMerger->PrepareMergeDir(0, mergeMeta, instanceRootPath);
    // MergeMetaPtr mergeMeta(new IndexMergeMeta());
    // mergeMeta->Load(mergeMetaPath);
    {
        merger::PartitionMergerPtr multiPartMerger(merger::PartitionMergerCreator::CreateFullParallelPartitionMerger(
            mergeSrcs, GET_TEMP_DATA_PATH(), options, NULL, "", PartitionRange(), CommonBranchHinterOption::Test(0)));
        auto doMerger1 = dynamic_pointer_cast<IndexPartitionMerger>(multiPartMerger);
        doMerger1->DoMerge(false, mergeMeta, 0);
    }
    {
        // simulate DoMerge fail over
        FslibWrapper::DeleteFileE(GET_TEMP_DATA_PATH() + "instance_0/__MARKED_BRANCH_INFO",
                                  DeleteOption::NoFence(true));
        merger::PartitionMergerPtr multiPartMerger(merger::PartitionMergerCreator::CreateFullParallelPartitionMerger(
            mergeSrcs, GET_TEMP_DATA_PATH(), options, NULL, "", PartitionRange(), CommonBranchHinterOption::Test(1)));
        auto doMerger2 = dynamic_pointer_cast<IndexPartitionMerger>(multiPartMerger);
        doMerger2->DoMerge(false, mergeMeta, 0);
    }
    {
        // simulate EndMerge fail over
        fslib::fs::FileSystem::_useMock = true;
        ExceptionTrigger::InitTrigger(0);
        ExceptionTrigger::SetExceptionCondition("rename", "segment_0_level_0/attribute/ends", fslib::EC_IO_ERROR);
        merger::PartitionMergerPtr multiPartMerger(
            merger::PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(mergeSrcs, GET_TEMP_DATA_PATH(),
                                                                                   options, NULL, ""));
        auto endMerger1 = dynamic_pointer_cast<IndexPartitionMerger>(multiPartMerger);
        ASSERT_ANY_THROW(endMerger1->EndMerge(mergeMeta));
    }
    {
        fslib::fs::FileSystem::_useMock = false;
        merger::PartitionMergerPtr multiPartMerger(
            merger::PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(mergeSrcs, GET_TEMP_DATA_PATH(),
                                                                                   options, NULL, ""));
        auto endMerger2 = dynamic_pointer_cast<IndexPartitionMerger>(multiPartMerger);
        ASSERT_NO_THROW(endMerger2->EndMerge(mergeMeta));
    }
    ASSERT_TRUE(FslibWrapper::IsExist(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "truncate_meta", "index_text2_ends"))
                    .GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "truncate_meta", "index_text_ends"))
                    .GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "segment_0_level_0/attribute/ends"))
                    .GetOrThrow());
}

}} // namespace indexlib::merger
