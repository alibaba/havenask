#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/partition/open_executor/preload_executor.h"
#include "indexlib/partition/open_executor/prepatch_executor.h"
#include "indexlib/partition/open_executor/reclaim_rt_index_executor.h"
#include "indexlib/partition/open_executor/switch_branch_executor.h"
#include "indexlib/test/document_parser.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/query.h"
#include "indexlib/test/query_parser.h"
#include "indexlib/test/result.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/searcher.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;

namespace indexlib { namespace partition {
using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::index_base;

class SwitchBranchExecutorTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    SwitchBranchExecutorTest();
    ~SwitchBranchExecutorTest();

    DECLARE_CLASS_NAME(SwitchBranchExecutorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(BuildMode, SwitchBranchExecutorTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SwitchBranchExecutorTest, TestSimpleProcess);
IE_LOG_SETUP(partition, SwitchBranchExecutorTest);

SwitchBranchExecutorTest::SwitchBranchExecutorTest() {}

SwitchBranchExecutorTest::~SwitchBranchExecutorTest() {}

void SwitchBranchExecutorTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    mSchema.reset(new IndexPartitionSchema);
    index::PartitionSchemaMaker::MakeSchema(mSchema,
                                            // Field schema
                                            "string1:string;long1:uint32",
                                            // Index schema
                                            ""
                                            // Primary key index schema
                                            "pk:primarykey64:string1",
                                            // Attribute schema
                                            "long1",
                                            // Summary schema
                                            "");
}

void SwitchBranchExecutorTest::CaseTearDown() {}

void SwitchBranchExecutorTest::TestSimpleProcess()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOnlineConfig().EnableOptimizedReopen();
    options.GetBuildConfig(true).maxDocCount = 2;
    options.GetBuildConfig(false).maxDocCount = 2;

    PartitionStateMachine psm;
    string fullDocString = "cmd=add,string1=pk1,ts=1,long1=1;"
                           "cmd=add,string1=pk2,ts=2,long1=1;"
                           "cmd=add,string1=pk3,ts=3,long1=1;"
                           "cmd=add,string1=pk4,ts=4,long1=1"; // segment 0 [pk1, pk2]
                                                               // segment 1 [pk3, pk4]
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));
    Version fullVersion;
    VersionLoader::GetVersionS(mRootDir, fullVersion, INVALID_VERSION);
    IE_LOG(INFO, "version [%s]", ToJsonString(fullVersion).c_str());
    ASSERT_EQ(2, fullVersion.GetSegmentCount());

    string incDocString = "cmd=add,string1=pk500,ts=7,long1=2;"
                          "cmd=add,string1=pk600,ts=8,long1=2;"
                          "cmd=add,string1=pk700,ts=9,long1=2;";

    string rtDocString = "cmd=add,string1=pk6,ts=5,long1=3;"
                         "cmd=add,string1=pk7,ts=5,long1=3;"
                         "cmd=add,string1=pk8,ts=5,long1=3;"
                         "cmd=add,string1=pk9,ts=10,long1=3;";

    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, incDocString, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "pk:pk6", "long1=3"));

    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    ASSERT_TRUE(indexPartition);
    OnlinePartition* onlinePartition = dynamic_cast<OnlinePartition*>(indexPartition.get());
    Version incVersion;
    VersionLoader::GetVersionS(mRootDir, incVersion, INVALID_VERSION);
    IE_LOG(INFO, "version [%s]", ToJsonString(incVersion).c_str());
    ASSERT_EQ(4, incVersion.GetSegmentCount());
    ASSERT_EQ(file_system::FSEC_OK,
              onlinePartition->GetFileSystem()->MountVersion(onlinePartition->GetRootPath(), incVersion.GetVersionId(),
                                                             "", file_system::FSMT_READ_ONLY, nullptr));
    ExecutorResource resource = onlinePartition->CreateExecutorResource(incVersion, false);
    PreloadExecutor preloadExec;
    PrepatchExecutor prepatchExec(indexPartition->GetDataLock());
    ReclaimRtIndexExecutor reclaimExec(true);
    SwitchBranchExecutor switchExecutor("");
    ASSERT_TRUE(preloadExec.Execute(resource));
    ASSERT_TRUE(prepatchExec.Execute(resource));
    ASSERT_TRUE(reclaimExec.Execute(resource));

    string rtDocString2 = "cmd=add,string1=pk10,ts=11,long1=3;"
                          "cmd=add,string1=pk11,ts=11,long1=3;"
                          "cmd=add,string1=pk12,ts=11,long1=3;"
                          "cmd=add,string1=pk13,ts=11,long1=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString2, "pk:pk13", "long1=3"));

    ASSERT_TRUE(resource.mBranchPartitionDataHolder.Get());

    auto branchDeletionMapReader = resource.mBranchPartitionDataHolder.Get()->GetDeletionMapReader();
    branchDeletionMapReader->Delete(0);
    OnlinePartitionReaderPtr oldReader(
        new OnlinePartitionReader(options, resource.mSchema, resource.mSearchCache, NULL));
    oldReader->Open(resource.mPartitionDataHolder.Get());
    Searcher oldSearcher;
    oldSearcher.Init(oldReader, mSchema.get());
    ASSERT_TRUE(switchExecutor.Execute(resource));

    // old partition reopen new segment and reinit reader
    {
        {
            QueryPtr query = QueryParser::Parse("pk:pk1", oldReader);
            ASSERT_TRUE(query);
            ResultPtr result = oldSearcher.Search(query, tsc_default);
            ResultPtr expectResult = DocumentParser::ParseResult("long1=1");
            ASSERT_TRUE(ResultChecker::Check(result, expectResult));
        }
        {
            QueryPtr query = QueryParser::Parse("pk:pk6", oldReader);
            ASSERT_TRUE(query);
            ResultPtr result = oldSearcher.Search(query, tsc_default);
            ResultPtr expectResult = DocumentParser::ParseResult("long1=3");
            ASSERT_TRUE(ResultChecker::Check(result, expectResult));
        }
        {
            QueryPtr query = QueryParser::Parse("pk:pk8", oldReader);
            ASSERT_TRUE(query);
            ResultPtr result = oldSearcher.Search(query, tsc_default);
            ResultPtr expectResult = DocumentParser::ParseResult("long1=3");
            ASSERT_TRUE(ResultChecker::Check(result, expectResult));
        }
        {
            QueryPtr query = QueryParser::Parse("pk:pk9", oldReader);
            ASSERT_TRUE(query);
            ResultPtr result = oldSearcher.Search(query, tsc_default);
            ResultPtr expectResult = DocumentParser::ParseResult("long1=3");
            ASSERT_TRUE(ResultChecker::Check(result, expectResult));
        }
    }

    OnlinePartitionReaderPtr newReader(
        new OnlinePartitionReader(options, resource.mSchema, resource.mSearchCache, NULL));
    newReader->Open(resource.mPartitionDataHolder.Get());
    Searcher newSearcher;
    newSearcher.Init(newReader, mSchema.get());
    {
        {
            // pk1 docid 0 is delete
            QueryPtr query = QueryParser::Parse("pk:pk1", newReader);
            ASSERT_FALSE(query);
        }
        {
            QueryPtr query = QueryParser::Parse("pk:pk2", newReader);
            ASSERT_TRUE(query);
            ResultPtr result = newSearcher.Search(query, tsc_default);
            ResultPtr expectResult = DocumentParser::ParseResult("long1=1");
            ASSERT_TRUE(ResultChecker::Check(result, expectResult));
        }
        {
            QueryPtr query = QueryParser::Parse("pk:pk6", newReader);
            ASSERT_FALSE(query);
        }
        {
            QueryPtr query = QueryParser::Parse("pk:pk8", newReader);
            ASSERT_FALSE(query);
        }
        for (int i = 9; i <= 13; i++) {
            string queryPk = "pk:pk" + autil::StringUtil::toString(i);
            QueryPtr query = QueryParser::Parse(queryPk, newReader);
            ASSERT_TRUE(query) << queryPk;
            ResultPtr result = newSearcher.Search(query, tsc_default);
            ResultPtr expectResult = DocumentParser::ParseResult("long1=3");
            ASSERT_TRUE(ResultChecker::Check(result, expectResult));
        }
    }
}
}} // namespace indexlib::partition
