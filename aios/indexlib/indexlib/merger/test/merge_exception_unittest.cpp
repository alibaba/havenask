#include <random>
#include "indexlib/merger/test/merge_exception_unittest.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/directory_checker.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/storage/exception_trigger.h"
#include "indexlib/util/path_util.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/schema_adapter.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergeExceptionUnittestTest);

MergeExceptionUnittestTest::MergeExceptionUnittestTest()
{
}

MergeExceptionUnittestTest::~MergeExceptionUnittestTest()
{
}

void MergeExceptionUnittestTest::CaseSetUp()
{
    mRootPath = GET_TEST_DATA_PATH();
    mBuildPath = PathUtil::JoinPath(mRootPath, "build_index");
    mMergePath = PathUtil::JoinPath(mRootPath, "merge_index");
    mBaselineMergePath = PathUtil::JoinPath(mRootPath, "merge_index_baseline");
    mExceptionTimepointMergePath = PathUtil::JoinPath(mRootPath, "merge_index_exception_timepoint");
    mMergeMetaPath = PathUtil::JoinPath(mRootPath, "merge_meta");

    FileSystemWrapper::Reset();
    ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);

    // disable log
    mLogLevel = alog::Logger::getLogger("indexlib")->getLevel();
    alog::Logger::getLogger("indexlib")->setLevel(alog::LOG_LEVEL_FATAL);
    alog::Logger::getLogger("indexlib.merger.MergeExceptionUnittestTest")->setLevel(alog::LOG_LEVEL_INFO);
}

void MergeExceptionUnittestTest::CaseTearDown()
{
    FileSystemWrapper::Reset();
    ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
    alog::Logger::getLogger("indexlib")->setLevel(mLogLevel);
}

void MergeExceptionUnittestTest::TestNormalTable()
{
    IndexPartitionSchemaPtr schema;
    IndexPartitionOptions options;
    options.GetMergeConfig().EnableArchiveFile();
    ParallelBuildInfo parallelBuildInfo;
    PrepareNormalIndex(mBuildPath, schema, options, parallelBuildInfo);
    auto mergerCreator =
        [parallelBuildInfo] (const string& path, const IndexPartitionOptions& options)
        -> IndexPartitionMergerPtr {
            return IndexPartitionMergerPtr(
                (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    path, parallelBuildInfo, options, NULL, ""));
        };
    InnerTestMerge(mergerCreator, schema, options, 37);
    CheckNormalIndex(mMergePath, schema, options);
}

void MergeExceptionUnittestTest::TestNormalTableWithTruncate()
{
    IndexPartitionSchemaPtr schema;
    IndexPartitionOptions options;
    options.GetMergeConfig().EnableArchiveFile();
    PrepareNormalWithTruncateIndex(mBuildPath, schema, options);
    auto mergerCreator =
        [] (const string& path, const IndexPartitionOptions& options)
        -> IndexPartitionMergerPtr {
            return IndexPartitionMergerPtr(
                (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
                    path, options, NULL, ""));
        };
    InnerTestMerge(mergerCreator, schema, options, 37);
    CheckNormalWithTruncateIndex(mMergePath, schema, options);
}

/////////////////////////
template <class MergerCreator>
void MergeExceptionUnittestTest::InnerTestMerge(
    MergerCreator mergerCreator, IndexPartitionSchemaPtr schema,
    const IndexPartitionOptions& options, uint32_t exceptionStep)
{
    FileSystemWrapper::DeleteIfExist(mMergePath);
    FileSystemWrapper::Copy(mBuildPath, mMergePath);
    PrepareBaseline(mergerCreator(mMergePath, options));
    vector<string> excludeFiles {
        "deploy_meta", "deploy_index", "segment_file_list", "package_file",
            "INDEXLIB_ARCHIVE_FILES", "index_summary"};
    auto baselineFileHashes = DirectoryChecker::GetAllFileHashes(mBaselineMergePath, excludeFiles);
    srand(time(NULL));
    for (uint32_t i = 0; true; i += 1 + rand() % exceptionStep)
    {
        bool enablePackageFile = (rand() % 2 == 0);
        FileSystemWrapper::Reset();
        ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
        FileSystemWrapper::DeleteIfExist(mMergePath);
        FileSystemWrapper::Copy(mBuildPath, mMergePath);
        IndexPartitionOptions newOptions(options);
        newOptions.GetMergeConfig().SetEnablePackageFile(enablePackageFile);
        newOptions.GetMergeConfig().SetCheckpointInterval(0);
        IndexPartitionMergerPtr merger = mergerCreator(mMergePath, newOptions);
        IE_LOG(INFO, "=== start merge with enablePackageFile [%d,%d] ===", i, enablePackageFile);
        ExceptionTrigger::InitTrigger(i);
        bool isExceptionHappend = false;
        try
        {
            merger->PrepareMerge(0);
            StoreMergeMeta(merger->CreateMergeMeta(false, 1, 0));
        }
        catch(const autil::legacy::ExceptionBase& e)
        {
            isExceptionHappend = true;
            IE_LOG(INFO, "exception happend on [%u]th IO in BEGIN_MERGE", i);
            ResetFsAfterException();
            ASSERT_NO_THROW(merger->PrepareMerge(0));
            ASSERT_NO_THROW(StoreMergeMeta(merger->CreateMergeMeta(false, 1, 0)));
        }
        MergeMetaPtr mergeMeta = LoadMergeMeta();
        try
        {
            merger->DoMerge(false, mergeMeta, 0);
        }
        catch(const autil::legacy::ExceptionBase& e)
        {
            ASSERT_FALSE(isExceptionHappend);
            isExceptionHappend = true;
            IE_LOG(INFO, "exception happend on [%u]th IO in DO_MERGE", i);
            ResetFsAfterException();
            ASSERT_NO_THROW(merger->DoMerge(false, LoadMergeMeta(), 0));
            // try {
            //     merger->DoMerge(false, LoadMergeMeta(), 0);
            // }
            // catch(const autil::legacy::ExceptionBase& e) {
            //     assert(false);
            // }
        }
        mergeMeta = LoadMergeMeta();
        try
        {
            merger->EndMerge(mergeMeta, mergeMeta->GetTargetVersion().GetVersionId());
        }
        catch(const autil::legacy::ExceptionBase& e)
        {
            ASSERT_FALSE(isExceptionHappend);
            isExceptionHappend = true;
            IE_LOG(INFO, "exception happend on [%u]th IO in END_MERGE", i);
            ResetFsAfterException();
            mergeMeta = LoadMergeMeta();
            ASSERT_NO_THROW(merger->EndMerge(mergeMeta, mergeMeta->GetTargetVersion().GetVersionId()));
        }
        if (!isExceptionHappend)
        {
            ASSERT_GE(i + 1, ExceptionTrigger::GetTriggerIOCount());
            IE_LOG(INFO, "merge need less than [%u] times IO, will stop test", i);
            break;
        }
        auto fileHashes = DirectoryChecker::GetAllFileHashes(mMergePath, excludeFiles);
        // if (!DirectoryChecker::CheckEqual(baselineFileHashes, fileHashes))
        // {
        //     assert(false);
        // }
        ASSERT_TRUE(DirectoryChecker::CheckEqual(baselineFileHashes, fileHashes));
    }
    FileSystemWrapper::Reset();
    ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
}

void MergeExceptionUnittestTest::PrepareBaseline(IndexPartitionMergerPtr merger)
{
    FileSystemWrapper::Reset();
    ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);

    merger->PrepareMerge(0);
    MergeMetaPtr mergeMeta = merger->CreateMergeMeta(false, 1, 0);
    merger->DoMerge(false, mergeMeta, 0);
    merger->EndMerge(mergeMeta, mergeMeta->GetTargetVersion().GetVersionId());

    FileSystemWrapper::DeleteIfExist(mBaselineMergePath);
    FileSystemWrapper::Copy(mMergePath, mBaselineMergePath);
}

void MergeExceptionUnittestTest::ResetFsAfterException()
{
    FileSystemWrapper::Reset();
    ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
    FileSystemWrapper::DeleteIfExist(mExceptionTimepointMergePath);
    FileSystemWrapper::Copy(mMergePath, mExceptionTimepointMergePath);
};

MergeMetaPtr MergeExceptionUnittestTest::LoadMergeMeta()
{
    MergeMetaPtr mergeMeta(new IndexMergeMeta());
    ExceptionTrigger::PauseTrigger();
    mergeMeta->Load(mMergeMetaPath);
    ExceptionTrigger::ResumeTrigger();
    return mergeMeta;
}

void MergeExceptionUnittestTest::StoreMergeMeta(MergeMetaPtr mergeMeta)
{
    ExceptionTrigger::PauseTrigger();
    mergeMeta->Store(mMergeMetaPath);
    ExceptionTrigger::ResumeTrigger();
}

/////////////////////////////////////////////////
void MergeExceptionUnittestTest::PrepareNormalIndex(
    const string& path, IndexPartitionSchemaPtr& schema, IndexPartitionOptions& options,
    ParallelBuildInfo& parallelBuildInfo)
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    string attribute = "string1;price";
    string subField = "sub_string1:string;sub_string2:string";
    string subIndex = "sub_index2:string:sub_string2;"
                      "sub_pk:primarykey64:sub_string1";
    string subAttribute = "sub_string1";
    schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            subField, subIndex, subAttribute, "");
    schema->SetSubIndexPartitionSchema(subSchema);
    options.SetIsOnline(false);
    options.GetBuildConfig(false).maxDocCount = 2;
    options.GetBuildConfig(false).keepVersionCount = 5;
    options.GetBuildConfig(true).maxDocCount = 2;

    string docString = "cmd=add,string1=hello1,price=4,string2=dengdeng,"
                       "sub_string1=sub1^sub2,sub_string2=d1^d2;"
                       "cmd=add,string1=hello2,price=5,string2=jiangjiang,"
                       "sub_string1=sub3^sub4,sub_string2=j1^j2;"
                       "cmd=add,string1=hello3,price=6,string2=huhu,"
                       "sub_string1=sub5^sub6,sub_string2=h1^h2;"
                       "cmd=add,string1=hello4,price=7,string2=xixi,"
                       "sub_string1=sub7^sub8,sub_string2=x1^x2;";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, path));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,"", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_index2:j1", "docid=2,sub_string1=sub3"));

    parallelBuildInfo.parallelNum = 2;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelBuildInfo.parallelNum; i ++) {
        parallelBuildInfo.instanceId = i;
        string incDoc =
            "cmd=add,string1=hello" + ToStr(i + 1) + ",string2=die,price=8,"
            "sub_string1=die_sub" + ToStr(i + 1) + ",sub_string2=die1,ts=2;"
            "cmd=add,string1=hello" + ToStr(i + 5) + ",string2=wangwang,price=9,"
            "sub_string1=wangwang_sub" + ToStr(i + 5) + ",sub_string2=wangwang1,ts=3;"
            "cmd=add,string1=hello" + ToStr(i + 9) + ",string2=wang,price=8,"
            "sub_string1=wang_sub" + ToStr(i + 9) + ",sub_string2=wang1,ts=4;"
            "cmd=delete,string1=hello" + ToStr(i + 5) + ",ts=5;";
        IndexlibPartitionCreator::BuildParallelIncData(
            schema, path, parallelBuildInfo, incDoc, options);
    }
}

void MergeExceptionUnittestTest::CheckNormalIndex(
    const string& path, IndexPartitionSchemaPtr schema,
    const IndexPartitionOptions& options)
{
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, path));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_index2:wang1",
                             "sub_string1=wang_sub9;sub_string1=wang_sub10;"));
}

//////////
void MergeExceptionUnittestTest::PrepareNormalWithTruncateIndex(
    const string& path, IndexPartitionSchemaPtr& schema, IndexPartitionOptions& options)
{
    schema = SchemaAdapter::LoadSchema(string(TEST_DATA_PATH), "truncate_example.json");

    string jsonStr;
    FileSystemWrapper::AtomicLoad(
        string(TEST_DATA_PATH) + "simple_truncate_for_sharding_index.json", jsonStr);
    FromJsonString(options.GetOfflineConfig().mergeConfig, jsonStr);
    options.GetOfflineConfig().buildConfig.keepVersionCount = 10;

    string fullDocs =
        "cmd=add,product_id=0,title=hello world,body=taobao,price=10,user_id=1;"
        "cmd=add,product_id=1,title=hello beijing,body=chaoyang,price=20,user_id=2;"
        "cmd=add,product_id=2,title=hello tonghui,body=chaoyang,price=30,user_id=3;"
        "cmd=add,product_id=3,title=hello chaoyang,body=taobao,price=40,user_id=4;"
        "cmd=add,product_id=4,title=bye chaoyang,body=taobao,price=50,user_id=5;"
        "cmd=add,product_id=5,title=bye tonghui,body=taobao,price=60,user_id=6;"
        "cmd=add,product_id=6,title=hello beijing chaoyang,body=taobao,price=70,user_id=7;";
    PartitionStateMachine psm;
    psm.Init(schema, options, path);
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
}

void MergeExceptionUnittestTest::CheckNormalWithTruncateIndex(
    const string& path, IndexPartitionSchemaPtr schema,
    const IndexPartitionOptions& options)
{
    PartitionStateMachine psm;
    psm.Init(schema, options, path);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title_asc_user_id:hello", "docid=0;docid=1;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title_desc_price:hello", "docid=3;docid=6;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase_desc_price:hello", "docid=3;docid=6;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase_desc_price:taobao", "docid=5;docid=6;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase_desc_price:chaoyang", "docid=4;docid=6;"));
}

IE_NAMESPACE_END(merger);
