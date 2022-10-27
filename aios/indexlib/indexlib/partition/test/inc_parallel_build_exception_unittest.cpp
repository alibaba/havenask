#include "indexlib/partition/test/inc_parallel_build_exception_unittest.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/directory_checker.h"
#include "indexlib/merger/inc_parallel_partition_merger.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/storage/exception_trigger.h"
#include "indexlib/file_system/directory.h"
#include <autil/HashAlgorithm.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(merger);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IncParallelBuildExceptionTest);

IncParallelBuildExceptionTest::IncParallelBuildExceptionTest()
{
}

IncParallelBuildExceptionTest::~IncParallelBuildExceptionTest()
{
}

void IncParallelBuildExceptionTest::CaseSetUp()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    string attribute = "string1;price";
    string subField = "sub_string1:string;sub_string2:string";
    string subIndex = "sub_index2:string:sub_string2;"
                      "sub_pk:primarykey64:sub_string1";
    string subAttribute = "sub_string1";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            subField, subIndex, subAttribute, "");
    mSchema->SetSubIndexPartitionSchema(subSchema);
    mOptions.SetIsOnline(false);
    mOptions.GetBuildConfig(false).maxDocCount = 2;
    mOptions.GetBuildConfig(false).keepVersionCount = 5;
    mOptions.GetBuildConfig(true).maxDocCount = 2;
    mRootPath = GET_TEST_DATA_PATH();
}

void IncParallelBuildExceptionTest::CaseTearDown()
{
}

void IncParallelBuildExceptionTest::TestSimpleProcess()
{
    // disable log
    SCOPED_LOG_LEVEL(FATAL);
    ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
    std::string buildPath = FileSystemWrapper::JoinPath(mRootPath, "build_index");
    std::string mergePath = FileSystemWrapper::JoinPath(mRootPath, "merge_index");
    ASSERT_NO_THROW(InnerParallelBuild(buildPath));
    FileSystemWrapper::Copy(buildPath, mergePath);
    ASSERT_NO_THROW(InnerMergeBuildIndex(mergePath));
    vector<string> excludeFiles {
        "deploy_meta", "deploy_index", "segment_file_list", "package_file"};
    auto results = DirectoryChecker::GetAllFileHashes(mergePath, excludeFiles);
    // since we reduce fileSystem accession, we have to reduce exceptionNum
    for (size_t i = 0; i < 800; i += 43)
    {
        FileSystemWrapper::Reset();
        ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
        FileSystemWrapper::DeleteIfExist(mergePath);
        FileSystemWrapper::Copy(buildPath, mergePath);
        cout << "## [" << GET_TEST_NAME() << "] Exception begin: " << i << endl;
        ExceptionTrigger::InitTrigger(i);
        ASSERT_ANY_THROW(InnerMergeBuildIndex(mergePath)) << i;
        cout << "## [" << GET_TEST_NAME() << "] Exception end: " << i << endl;
        ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
        ASSERT_NO_THROW(InnerMergeBuildIndex(mergePath)) << i;
        auto ans = DirectoryChecker::GetAllFileHashes(mergePath, excludeFiles);
        ASSERT_EQ(results.size(), ans.size());
        for (size_t i = 0; i < results.size(); ++i)
        {
            ASSERT_EQ(results[i], ans[i]);
        }
    }
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mergePath));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_index2:wang1",
                             "sub_string1=wang_sub9;sub_string1=wang_sub10;"));
    storage::FileSystemWrapper::Reset();
    ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
}

void IncParallelBuildExceptionTest::InnerParallelBuild(const std::string& buildPath)
{
    string docString = "cmd=add,string1=hello1,price=4,string2=dengdeng,"
                       "sub_string1=sub1^sub2,sub_string2=d1^d2;"
                       "cmd=add,string1=hello2,price=5,string2=jiangjiang,"
                       "sub_string1=sub3^sub4,sub_string2=j1^j2;"
                       "cmd=add,string1=hello3,price=6,string2=huhu,"
                       "sub_string1=sub5^sub6,sub_string2=h1^h2;"
                       "cmd=add,string1=hello4,price=7,string2=xixi,"
                       "sub_string1=sub7^sub8,sub_string2=x1^x2;";
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, buildPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,"", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_index2:j1", "docid=2,sub_string1=sub3"));
    }
    size_t parallelNum = 2;
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    mOptions.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        mMergeSrc.push_back(info.GetParallelInstancePath(buildPath));
        string incDoc = "cmd=add,string1=hello" + StringUtil::toString(i + 1) +
                        ",string2=die,price=8,sub_string1=die_sub" + StringUtil::toString(i + 1) + ",sub_string2=die1,ts=2;"
                        "cmd=add,string1=hello" + StringUtil::toString(i + 5) +
                        ",string2=wangwang,price=9,sub_string1=wangwang_sub" + StringUtil::toString(i + 5) + ",sub_string2=wangwang1,ts=3;" +
                        "cmd=add,string1=hello" + StringUtil::toString(i + 9) +
                        ",string2=wang,price=8,sub_string1=wang_sub" + StringUtil::toString(i + 9) + ",sub_string2=wang1,ts=4;"
                        "cmd=delete,string1=hello" + StringUtil::toString(i + 5) + ",ts=5;";
        IndexlibPartitionCreator::BuildParallelIncData(mSchema, buildPath, info, incDoc, mOptions);
    }
}

void IncParallelBuildExceptionTest::InnerMergeBuildIndex(const string& buildPath)
{
    IncParallelPartitionMerger* merger = NULL;
    try
    {
        ParallelBuildInfo info(2, 0, 0);
        merger = (IncParallelPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
            buildPath, info, mOptions, NULL, "");
        //merger->Merge(false);
        merger->PrepareMerge(0);
        merger::MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
        // this step would merge all segments into one segment
        merger->DoMerge(false, meta, 0);
        merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
        delete merger;
    }
    catch (...)
    {
        delete merger;
        throw;
    }
}

IE_NAMESPACE_END(partition);
