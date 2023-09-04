#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/partition/open_executor/preload_executor.h"
#include "indexlib/partition/open_executor/prepatch_executor.h"
#include "indexlib/partition/open_executor/redo_and_lock_executor.h"
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

class RedoAndLockExecutorTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    RedoAndLockExecutorTest();
    ~RedoAndLockExecutorTest();

    DECLARE_CLASS_NAME(RedoAndLockExecutorTest);

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

INSTANTIATE_TEST_CASE_P(BuildMode, RedoAndLockExecutorTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RedoAndLockExecutorTest, TestSimpleProcess);
IE_LOG_SETUP(partition, RedoAndLockExecutorTest);

RedoAndLockExecutorTest::RedoAndLockExecutorTest() {}

RedoAndLockExecutorTest::~RedoAndLockExecutorTest() {}

void RedoAndLockExecutorTest::CaseSetUp()
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

void RedoAndLockExecutorTest::CaseTearDown() {}

void RedoAndLockExecutorTest::TestSimpleProcess()
{
    autil::ThreadMutex dataLock;
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
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
    config::MergeConfig mergeConfig;
    mergeConfig.mergeStrategyStr = "specific_segments";
    mergeConfig.mergeStrategyParameter.SetLegacyString("merge_segments=0;");
    psm.SetMergeConfig(mergeConfig);
    Version fullVersion;
    VersionLoader::GetVersionS(mRootDir, fullVersion, INVALID_VERSION);
    IE_LOG(INFO, "version [%s]", ToJsonString(fullVersion).c_str());
    ASSERT_EQ(2, fullVersion.GetSegmentCount());

    string incDocString = "cmd=add,string1=pk5,ts=5,long1=2;";

    string rtDocString = "cmd=add,string1=pk5,ts=5,long1=2;"
                         "cmd=update_field,string1=pk1,ts=6,long1=3;"
                         "cmd=update_field,string1=pk3,ts=7,long1=3;"
                         "cmd=update_field,string1=pk5,ts=7,long1=3;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDocString, "", "")); // segment [1, 2, 3]
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "", ""));

    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    ASSERT_TRUE(indexPartition);
    OnlinePartition* onlinePartition = dynamic_cast<OnlinePartition*>(indexPartition.get());
    Version incVersion;
    VersionLoader::GetVersionS(mRootDir, incVersion, INVALID_VERSION);
    IE_LOG(INFO, "version [%s]", ToJsonString(incVersion).c_str());
    ASSERT_EQ(3, incVersion.GetSegmentCount());

    ASSERT_EQ(file_system::FSEC_OK, onlinePartition->mFileSystem->MountVersion(onlinePartition->mOpenIndexPrimaryDir,
                                                                               incVersion.GetVersionId(), "",
                                                                               file_system::FSMT_READ_ONLY, nullptr));
    ExecutorResource resource = onlinePartition->CreateExecutorResource(incVersion, false);
    PreloadExecutor preloadExec;
    PrepatchExecutor prepatchExec(indexPartition->GetDataLock());
    ASSERT_TRUE(preloadExec.Execute(resource));
    ASSERT_TRUE(prepatchExec.Execute(resource));
    RedoAndLockExecutor redoExecutor(&dataLock, -1);
    ASSERT_TRUE(redoExecutor.Execute(resource));
    ASSERT_TRUE(resource.mBranchPartitionDataHolder.Get());

    ASSERT_NE(0, dataLock.trylock());
    OnlinePartitionReaderPtr reader(new OnlinePartitionReader(options, resource.mSchema, resource.mSearchCache, NULL));
    reader->Open(resource.mBranchPartitionDataHolder.Get());
    Searcher searcher;
    searcher.Init(reader, mSchema.get());

    OnlinePartitionReaderPtr oldReader(
        new OnlinePartitionReader(options, resource.mSchema, resource.mSearchCache, NULL));
    oldReader->Open(resource.mPartitionDataHolder.Get());
    Searcher oldSearcher;
    oldSearcher.Init(oldReader, mSchema.get());
    // old partition reopen new segment and reinit reader
    {
        QueryPtr query = QueryParser::Parse("pk:pk1", oldReader);
        ASSERT_TRUE(query);
        ResultPtr result = oldSearcher.Search(query, tsc_default);
        ResultPtr expectResult = DocumentParser::ParseResult("long1=3");
        ASSERT_TRUE(ResultChecker::Check(result, expectResult));
    }
    {
        QueryPtr query = QueryParser::Parse("pk:pk3", oldReader);
        ASSERT_TRUE(query);
        ResultPtr result = oldSearcher.Search(query, tsc_default);
        ResultPtr expectResult = DocumentParser::ParseResult("long1=3");
        ASSERT_TRUE(ResultChecker::Check(result, expectResult));
    }
    {
        QueryPtr query = QueryParser::Parse("pk:pk3", oldReader);
        ASSERT_TRUE(query);
        ResultPtr result = oldSearcher.Search(query, tsc_default);
        ResultPtr expectResult = DocumentParser::ParseResult("long1=3");
        ASSERT_TRUE(ResultChecker::Check(result, expectResult));
    }

    // branch info is delete, use inc value
    {
        QueryPtr query = QueryParser::Parse("pk:pk1", reader);
        ASSERT_TRUE(query);
        ResultPtr result = searcher.Search(query, tsc_default);
        ResultPtr expectResult = DocumentParser::ParseResult("long1=3");
        ASSERT_TRUE(ResultChecker::Check(result, expectResult));
    }
    {
        QueryPtr query = QueryParser::Parse("pk:pk3", reader);
        ASSERT_TRUE(query);
        ResultPtr result = searcher.Search(query, tsc_default);
        ResultPtr expectResult = DocumentParser::ParseResult("long1=3");
        ASSERT_TRUE(ResultChecker::Check(result, expectResult));
    }
    {
        QueryPtr query = QueryParser::Parse("pk:pk5", reader);
        ASSERT_TRUE(query);
        ResultPtr result = searcher.Search(query, tsc_default);
        ResultPtr expectResult = DocumentParser::ParseResult("long1=3");
        ASSERT_TRUE(ResultChecker::Check(result, expectResult));
    }
    ASSERT_EQ(0, dataLock.unlock());
}
}} // namespace indexlib::partition
