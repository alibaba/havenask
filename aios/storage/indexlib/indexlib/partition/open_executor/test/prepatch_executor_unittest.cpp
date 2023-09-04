#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/partition/open_executor/preload_executor.h"
#include "indexlib/partition/open_executor/prepatch_executor.h"
#include "indexlib/test/document_parser.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/query.h"
#include "indexlib/test/query_parser.h"
#include "indexlib/test/result.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/searcher.h"
#include "indexlib/test/slow_dump_segment_container.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;

namespace indexlib { namespace partition {
using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::index_base;

class PrepatchExecutorTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    PrepatchExecutorTest();
    ~PrepatchExecutorTest();

    DECLARE_CLASS_NAME(PrepatchExecutorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestNewSegmentUnvisiable();

private:
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(BuildMode, PrepatchExecutorTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrepatchExecutorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrepatchExecutorTest, TestNewSegmentUnvisiable);
IE_LOG_SETUP(partition, PrepatchExecutorTest);

PrepatchExecutorTest::PrepatchExecutorTest() {}

PrepatchExecutorTest::~PrepatchExecutorTest() {}

void PrepatchExecutorTest::CaseSetUp()
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

void PrepatchExecutorTest::CaseTearDown() {}

void PrepatchExecutorTest::TestSimpleProcess()
{
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetBuildConfig(true).maxDocCount = 2;
    options.GetBuildConfig(false).maxDocCount = 2;
    options.GetOnlineConfig().EnableOptimizedReopen();

    PartitionStateMachine psm;
    string fullDocString = "cmd=add,string1=pk1,ts=1,long1=1;"
                           "cmd=add,string1=pk2,ts=1,long1=1;"
                           "cmd=add,string1=pk3,ts=1,long1=1;"
                           "cmd=add,string1=pk4,ts=1,long1=1"; // segment 0 [pk1, pk2]
                                                               // segment 1 [pk3, pk4]
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));
    Version fullVersion;
    VersionLoader::GetVersionS(mRootDir, fullVersion, INVALID_VERSION);
    IE_LOG(INFO, "version [%s]", ToJsonString(fullVersion).c_str());
    ASSERT_EQ(2, fullVersion.GetSegmentCount());

    string incDocString = "cmd=add,string1=pk5,ts=2,long1=1;"
                          "cmd=add,string1=pk6,ts=2,long1=1;"
                          "cmd=add,string1=pk7,ts=2,long1=1;"
                          "cmd=update_field,string1=pk1,ts=2,long1=100;"
                          "cmd=update_field,string1=pk5,ts=2,long1=100";
    string rtDocString = "cmd=add,string1=pk8,ts=3,long1=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, incDocString, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "", ""));

    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    ASSERT_TRUE(indexPartition);
    OnlinePartition* onlinePartition = dynamic_cast<OnlinePartition*>(indexPartition.get());
    Version incVersion;
    VersionLoader::GetVersionS(mRootDir, incVersion, INVALID_VERSION);
    IE_LOG(INFO, "version [%s]", ToJsonString(incVersion).c_str());
    ASSERT_EQ(4, incVersion.GetSegmentCount());
    ASSERT_EQ(file_system::FSEC_OK, onlinePartition->mFileSystem->MountVersion(onlinePartition->mOpenIndexPrimaryDir,
                                                                               incVersion.GetVersionId(), "",
                                                                               file_system::FSMT_READ_ONLY, nullptr));
    ExecutorResource resource = onlinePartition->CreateExecutorResource(incVersion, false);
    PreloadExecutor preloadExec;
    PrepatchExecutor prepatchExec(indexPartition->GetDataLock());
    ASSERT_TRUE(preloadExec.Execute(resource));
    ASSERT_TRUE(prepatchExec.Execute(resource));
    ASSERT_TRUE(resource.mBranchPartitionDataHolder.Get());
    OnlinePartitionReaderPtr reader(new OnlinePartitionReader(options, resource.mSchema, resource.mSearchCache, NULL));
    reader->Open(resource.mBranchPartitionDataHolder.Get());
    Searcher searcher;
    searcher.Init(reader, mSchema.get());
    {
        // no patch to pk1-pk4
        QueryPtr query = QueryParser::Parse("pk:pk1", reader);
        ResultPtr result = searcher.Search(query, tsc_default);
        ResultPtr expectResult = DocumentParser::ParseResult("long1=1");
        ASSERT_TRUE(ResultChecker::Check(result, expectResult));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk1", "long1=1"));
    }
    {
        // do patch to pk1-pk4
        QueryPtr query = QueryParser::Parse("pk:pk5", reader);
        ResultPtr result = searcher.Search(query, tsc_default);
        ResultPtr expectResult = DocumentParser::ParseResult("long1=100");
        ASSERT_TRUE(ResultChecker::Check(result, expectResult));
    }
    {
        // do patch to pk1-pk4
        QueryPtr query = QueryParser::Parse("pk:pk6", reader);
        ResultPtr result = searcher.Search(query, tsc_default);
        ResultPtr expectResult = DocumentParser::ParseResult("long1=1");
        ASSERT_TRUE(ResultChecker::Check(result, expectResult));
    }
}

void PrepatchExecutorTest::TestNewSegmentUnvisiable()
{
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetBuildConfig(true).maxDocCount = 2;
    options.GetBuildConfig(false).maxDocCount = 2;
    options.GetOnlineConfig().EnableOptimizedReopen();
    options.GetOnlineConfig().enableAsyncDumpSegment = true;
    SlowDumpSegmentContainerPtr dumpContainer(new SlowDumpSegmentContainer(500));

    PartitionStateMachine psm(false, partition::IndexPartitionResource(), dumpContainer);
    string fullDocString = "cmd=add,string1=pk1,ts=1,long1=1;";
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));

    string incDocString = "cmd=add,string1=pk5,ts=2,long1=1;";
    string rtDocString = "cmd=add,string1=pk6,ts=3,long1=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, incDocString, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "", ""));

    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    ASSERT_TRUE(indexPartition);
    OnlinePartition* onlinePartition = dynamic_cast<OnlinePartition*>(indexPartition.get());
    Version incVersion;
    VersionLoader::GetVersionS(mRootDir, incVersion, INVALID_VERSION);
    ASSERT_EQ(file_system::FSEC_OK, onlinePartition->mFileSystem->MountVersion(onlinePartition->mOpenIndexPrimaryDir,
                                                                               incVersion.GetVersionId(), "",
                                                                               file_system::FSMT_READ_ONLY, nullptr));
    ExecutorResource resource = onlinePartition->CreateExecutorResource(incVersion, false);
    PreloadExecutor preloadExec;
    PrepatchExecutor prepatchExec(indexPartition->GetDataLock());
    ASSERT_TRUE(preloadExec.Execute(resource));
    ASSERT_TRUE(prepatchExec.Execute(resource));
    ASSERT_TRUE(resource.mBranchPartitionDataHolder.Get());
    vector<segmentid_t> oldSegs, currentSegs;
    auto iter1 = resource.mBranchPartitionDataHolder.Get()->CreateSegmentIterator();
    while (iter1->IsValid()) {
        oldSegs.push_back(iter1->GetSegmentId());
        iter1->MoveToNext();
    }
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "", ""));
    ASSERT_EQ(0u, dumpContainer->GetDumpItemSize());

    rtDocString = "cmd=add,string1=pk7,ts=5,long1=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "", ""));
    rtDocString = "cmd=add,string1=pk8,ts=6,long1=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "", ""));
    rtDocString = "cmd=add,string1=pk9,ts=7,long1=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "", ""));
    ASSERT_NE(0u, dumpContainer->GetDumpItemSize());

    auto iter2 = resource.mBranchPartitionDataHolder.Get()->CreateSegmentIterator();
    while (iter2->IsValid()) {
        currentSegs.push_back(iter2->GetSegmentId());
        iter2->MoveToNext();
    }

    ASSERT_EQ(oldSegs, currentSegs);
    dumpContainer.reset();
}
}} // namespace indexlib::partition
