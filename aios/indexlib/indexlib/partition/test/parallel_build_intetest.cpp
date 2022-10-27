#include "indexlib/partition/test/parallel_build_intetest.h"
#include "indexlib/index_base/patch/partition_patch_meta.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/counter/state_counter.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(merger);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, ParallelBuildInteTest);

ParallelBuildInteTest::ParallelBuildInteTest()
{
}

ParallelBuildInteTest::~ParallelBuildInteTest()
{
}

void ParallelBuildInteTest::CaseSetUp()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    string attribute = "string1;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mSchema->SetSchemaVersionId(1);
    mOptions.SetIsOnline(false);
    mOptions.GetBuildConfig(false).maxDocCount = 2;
    mOptions.GetBuildConfig(false).buildTotalMemory = 20;
    mOptions.GetBuildConfig(true).maxDocCount = 2;
}

void ParallelBuildInteTest::CaseTearDown()
{
}

void ParallelBuildInteTest::TestSimpleProcess()
{
    string docString = "cmd=add,string1=hello,string2=abc,price=4;"
                       "cmd=add,string1=hello,string2=123,price=5,modify_fields=price;"
                       "cmd=add,string1=hello1,price=6;"
                       "cmd=add,string1=hello1,price=7,modify_fields=string2#price;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    string rootPath = GET_TEST_DATA_PATH();

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,"", ""));
    }
    size_t parallelNum = 4;
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 0));
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 1));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_0")));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_1")));
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    info.batchId = 1;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        string incDoc = "cmd=add,string1=hello" + StringUtil::toString(i + 4) + ",price=8,ts=2;"
                        "cmd=add,string1=hello" + StringUtil::toString(i + 8) +
                        ",price=9,modify_fields=string2#price;ts=" + StringUtil::toString(i + 2);
        IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc, options);
    }
    unique_ptr<IndexPartitionMerger> merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    rootPath, info, options, NULL, ""));
    //merger->Merge(false);

    vector<string> lostSegments = MakeLostSegments(rootPath, 20, 3);
    CheckPathExist(lostSegments, true);
    merger->PrepareMerge(0);
    CheckPathExist(lostSegments, false);
    CheckMetaFile(rootPath);
    MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
    merger->DoMerge(false, meta, 0);
    merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_INC,
                             "",
                            "pk:hello8", "docid=3"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_0")));
    ASSERT_FALSE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_1")));
}

void ParallelBuildInteTest::TestCustomizedTable()
{
    auto schema = SchemaAdapter::LoadSchema(
        TEST_DATA_PATH,"demo_table_schema.json");
    string pluginRoot = TEST_DATA_PATH;
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().enablePackageFile = false;
    string rootPath = GET_TEST_DATA_PATH();

    {
        IndexPartitionResource partitionResource;
        partitionResource.indexPluginPath = pluginRoot;
        PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, true, partitionResource);
        ASSERT_TRUE(psm.Init(schema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(PE_BUILD_FULL, docString, "", ""));
    }
    size_t parallelNum = 4;
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 0));
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 1));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_0")));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_1")));
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    info.batchId = 1;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        string incDoc = "cmd=add,pk=k" + StringUtil::toString(i + 4) +
                        ",cfield1=" + StringUtil::toString(i + 4) +
                        "v1,cfield2=" + StringUtil::toString(i + 4) +
                        "v2,ts=" + StringUtil::toString(i + 2) + ";";
        vector<DocumentPtr> docs;
        {
            IndexPartitionResource partitionResource;
            partitionResource.indexPluginPath = pluginRoot;
            PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, true,
                                    partitionResource);
            ASSERT_TRUE(psm.Init(schema, options, rootPath));
            docs = psm.CreateDocuments(incDoc);
        }
        QuotaControlPtr memoryQuotaControl(
            new QuotaControl(1024 * 1024 * 1024));
        IndexBuilder indexBuilder(rootPath, info, options, schema,
                                  memoryQuotaControl);
        indexBuilder.Init();
        for (size_t i = 0; i < docs.size(); i++)
        {
            indexBuilder.Build(docs[i]);
        }
        indexBuilder.EndIndex(INVALID_TIMESTAMP);
    }
    unique_ptr<IndexPartitionMerger> merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    rootPath, info, options, NULL, pluginRoot));

    vector<string> lostSegments = MakeLostSegments(rootPath, 30, 3);
    CheckPathExist(lostSegments, true);
    merger->PrepareMerge(0);
    CheckPathExist(lostSegments, false);
    CheckMetaFile(rootPath, false);
    string metaPath = FileSystemWrapper::JoinPath(rootPath, "merge_meta");
    {
        auto mergeMeta = merger->CreateMergeMeta(false, 1, 0);
        mergeMeta->Store(metaPath);
    }
    auto meta = merger->LoadMergeMeta(metaPath, false);
    merger->DoMerge(false, meta, 0);
    merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, rootPath));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "k6", "cfield1=6v1,cfield2=6v2;"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_0")));
    ASSERT_FALSE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_1")));
}

void ParallelBuildInteTest::TestBeginMergeRestartForUpdateConfig()
{
    string docString = "cmd=add,string1=hello,string2=abc,price=4;"
                       "cmd=add,string1=hello,string2=123,price=5,modify_fields=price;"
                       "cmd=add,string1=hello1,price=6;"
                       "cmd=add,string1=hello1,price=7,modify_fields=string2#price;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    string rootPath = GET_TEST_DATA_PATH();

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,"", ""));
    }
    size_t parallelNum = 4;
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 0));
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 1));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_0")));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_1")));
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    info.batchId = 1;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        string incDoc = "cmd=add,string1=hello" + StringUtil::toString(i + 4) + ",price=8,ts=2;"
                        "cmd=add,string1=hello" + StringUtil::toString(i + 8) +
                        ",price=9,modify_fields=string2#price;ts=" + StringUtil::toString(i + 2);
        IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc, options);
    }
    unique_ptr<IndexPartitionMerger> merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    rootPath, info, options, NULL, ""));
    //merger->Merge(false);

    vector<string> lostSegments = MakeLostSegments(rootPath, 20, 3);
    CheckPathExist(lostSegments, true);

    merger->PrepareMerge(0);
    CheckPathExist(lostSegments, false);
    CheckMetaFile(rootPath);
    MergeMetaPtr meta1 = merger->CreateMergeMeta(false, 1, 0);
    IndexMergeMetaPtr metaPtr1 = DYNAMIC_POINTER_CAST(IndexMergeMeta, meta1);

    merger->PrepareMerge(1);
    CheckPathExist(lostSegments, false);
    CheckMetaFile(rootPath);
    MergeMetaPtr meta2 = merger->CreateMergeMeta(false, 1, 1);
    IndexMergeMetaPtr metaPtr2 = DYNAMIC_POINTER_CAST(IndexMergeMeta, meta2);

    merger->PrepareMerge(1);
    CheckPathExist(lostSegments, false);
    CheckMetaFile(rootPath);
    MergeMetaPtr meta3 = merger->CreateMergeMeta(false, 1, 1);
    IndexMergeMetaPtr metaPtr3 = DYNAMIC_POINTER_CAST(IndexMergeMeta, meta3);

    ASSERT_EQ(0, metaPtr1->GetTimestamp());
    ASSERT_EQ(1, metaPtr2->GetTimestamp());

    ASSERT_EQ(metaPtr2->GetTimestamp(), metaPtr3->GetTimestamp());
    ASSERT_EQ(metaPtr2->GetCounterMapContent(), metaPtr3->GetCounterMapContent());
}

void ParallelBuildInteTest::TestEndMergeRestartForUpdateConfig()
{
    string docString = "cmd=add,string1=hello,string2=abc,price=4;"
                       "cmd=add,string1=hello,string2=123,price=5,modify_fields=price;"
                       "cmd=add,string1=hello1,price=6;"
                       "cmd=add,string1=hello1,price=7,modify_fields=string2#price;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    string rootPath = GET_TEST_DATA_PATH();

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,"", ""));
    }
    size_t parallelNum = 4;
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 0));
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 1));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_0")));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_1")));
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    info.batchId = 1;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        string incDoc = "cmd=add,string1=hello" + StringUtil::toString(i + 4) + ",price=8,ts=2;"
                        "cmd=add,string1=hello" + StringUtil::toString(i + 8) +
                        ",price=9,modify_fields=string2#price;ts=" + StringUtil::toString(i + 2);
        IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc, options);
    }
    unique_ptr<IndexPartitionMerger> merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    rootPath, info, options, NULL, ""));
    //merger->Merge(false);

    vector<string> lostSegments = MakeLostSegments(rootPath, 20, 3);
    CheckPathExist(lostSegments, true);
    merger->PrepareMerge(0);
    CheckPathExist(lostSegments, false);
    CheckMetaFile(rootPath);
    MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
    merger->DoMerge(false, meta, 0);
    merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
    //test upc, end merge do twice
    merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_INC,
                             "",
                             "pk:hello8", "docid=3"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_0")));
    ASSERT_FALSE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_1")));
}

void ParallelBuildInteTest::TestBuildWithConflictSegmentInParentPath()
{
    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello,price=5,modify_fields=price;"
                       "cmd=add,string1=hello1,price=6;"
                       "cmd=add,string1=hello1,price=7,modify_fields=string2#price;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    string rootPath = GET_TEST_DATA_PATH();

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,"", ""));
    }

    vector<string> uselessSegments;
    for (segmentid_t i = 4; i < 12; i++)
    {
        string segName = "segment_" + StringUtil::toString(i) + "_level_0";
        string segPath = FileSystemWrapper::JoinPath(rootPath, segName);
        FileSystemWrapper::MkDir(segPath);
        if (i > 8)
        {
            // segment 4 ~ 8 will be generate by BeginMerge
            uselessSegments.push_back(segPath);
        }
    }

    size_t parallelNum = 4;
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 0));
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 1));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_0")));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_1")));
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    info.batchId = 1;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    options.GetBuildConfig(false).enablePackageFile = true;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        string incDoc = "cmd=add,string1=hello" + StringUtil::toString(i + 4) + ",price=8,ts=2;"
                        "cmd=add,string1=hello" + StringUtil::toString(i + 8) +
                        ",price=9,modify_fields=string2#price;ts=" + StringUtil::toString(i + 2);
        IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc, options);
    }

    unique_ptr<IndexPartitionMerger> merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    rootPath, info, options, NULL, ""));

    CheckPathExist(uselessSegments, true);
    merger->PrepareMerge(0);
    CheckPathExist(uselessSegments, false);
    CheckMetaFile(rootPath);
    MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
    merger->DoMerge(false, meta, 0);
    merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_INC,
                             "",
                             "pk:hello8", "docid=3"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_0")));
    ASSERT_FALSE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_1")));
}

void ParallelBuildInteTest::TestRtBuildNotInParallelPath()
{
    string fullDocs = "cmd=add,string1=hello,price=4,ts=1;"
                      "cmd=add,string1=hello,price=5,modify_fields=price,ts=1;";
    string rtDocs = "cmd=add,string1=hello1,price=6,ts=2;"
                    "cmd=add,string1=hello1,price=7,modify_fields=string2#price,ts=3;";
    string rtDocs2 = "cmd=add,string1=hello2,price=8,ts=4;"
                     "cmd=add,string1=hello3,price=9,ts=5;";
    IndexPartitionOptions options;
    options.GetOnlineConfig().buildConfig.maxDocCount = 2;
    options.GetOfflineConfig().buildConfig.maxDocCount = 2;
    PartitionStateMachine psm;
    string rootDir = GET_TEST_DATA_PATH();
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, rootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs,
                                    "pk:hello", "docid=0;"));
    auto indexPart = psm.GetIndexPartition();
    string primaryDir = indexPart->GetSecondaryRootPath();

    string relativePath;
    ASSERT_TRUE(PathUtil::GetRelativePath(rootDir, primaryDir, relativePath));
    ASSERT_TRUE(relativePath.find(ParallelBuildInfo::PARALLEL_BUILD_PREFIX) == string::npos);
    ASSERT_FALSE(FileSystemWrapper::IsExist(ParallelBuildInfo::GetParallelPath(primaryDir, 0, 0)));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs,
                                    "pk:hello1", "docid=2;"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs2,
                                    "pk:hello3", "docid=4;"));
    indexPart = psm.GetIndexPartition();
    primaryDir = indexPart->GetSecondaryRootPath();
    ASSERT_TRUE(PathUtil::GetRelativePath(rootDir, primaryDir, relativePath));
    ASSERT_TRUE(relativePath.find(ParallelBuildInfo::PARALLEL_BUILD_PREFIX) == string::npos);
    ASSERT_FALSE(FileSystemWrapper::IsExist(ParallelBuildInfo::GetParallelPath(primaryDir, 0, 0)));
}

void ParallelBuildInteTest::TestDeletionMapMerged()
{
    string rootPath = GET_TEST_DATA_PATH();
    PrepareFullData();
    size_t parallelNum = 4;
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    auto options = mOptions;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        string incDoc = "cmd=add,string1=hello" + StringUtil::toString(i + 1) +
                        ",string2=die,price=8,ts=2;cmd=add,string1=hello" +
                        StringUtil::toString(i + 5) + ",string2=wangwang,price=9,ts=3;" +
                        "cmd=add,string1=hello" + StringUtil::toString(i + 9) +
                        ",string2=wang,price=8,ts=4;cmd=delete,string1=hello" +
                        StringUtil::toString(i + 5) + ",ts=5;";
        IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc, options);
    }
    unique_ptr<IndexPartitionMerger> merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    rootPath, info, options, NULL, ""));
    //merger->Merge(false);
    vector<string> lostSegments = MakeLostSegments(rootPath, 20, 3);
    CheckPathExist(lostSegments, true);
    merger->PrepareMerge(0);
    CheckPathExist(lostSegments, false);
    CheckMetaFile(rootPath);
    MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
    // this step would merge all segments into one segment
    merger->DoMerge(false, meta, 0);
    merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:dengdeng", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:wangwang", ""));
    // ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:wang", "docid=6,pk=hello9;docid=9,pk=hello10;"
    //                          "docid=12,pk=hello11;docid=15,pk=hello12;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:wang", "docid=4,string1=hello9;"
                             "docid=5,string1=hello10;"
                             "docid=6,string1=hello11;docid=7,string1=hello12;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:die", "docid=0,string1=hello1;"
                             "docid=1,string1=hello2;" "docid=2,string1=hello3;"
                             "docid=3,string1=hello4;"));
}


void ParallelBuildInteTest::TestBuildEmptyDataForIncInstance()
{
    string rootPath = GET_TEST_DATA_PATH();
    PrepareFullData(false);
    size_t parallelNum = 4;
    vector<string> mergeSrc;
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    info.batchId = 1;
    auto options = mOptions;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        mergeSrc.push_back(info.GetParallelInstancePath(rootPath));
        IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, "", options, 100);
    }

    for (auto instPath : mergeSrc)
    {
        Version version;
        VersionLoader::GetVersion(instPath, version, INVALID_VERSION);
        ASSERT_TRUE(version.GetVersionId() != INVALID_VERSION);
        ASSERT_EQ(100, version.GetTimestamp());
    }
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;

    unique_ptr<IndexPartitionMerger> merger(
        (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
            rootPath, info, options, NULL, ""));
    merger->PrepareMerge(0);
    MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
    // this step would merge all segments into one segment
    merger->DoMerge(false, meta, 0);
    merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());

    Version version;
    VersionLoader::GetVersion(rootPath, version, INVALID_VERSION);
    ASSERT_EQ(100, version.GetTimestamp());
}

void ParallelBuildInteTest::TestBuildFromEmptyForIncInstance()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    string rootPath = GET_TEST_DATA_PATH();

    {
        // all empty
        size_t parallelNum = 2;
        ParallelBuildInfo info;
        info.parallelNum = parallelNum;
        info.batchId = 1;
        options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
        for (size_t i = 0; i < parallelNum; i ++) {
            info.instanceId = i;
            IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, "", options, 100);
        }

        unique_ptr<IndexPartitionMerger> merger(
                (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                        rootPath, info, options, NULL, ""));
        merger->PrepareMerge(0);
        merger::MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
        // this step would merge all segments into one segment
        merger->DoMerge(false, meta, 0);
        merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());

        Version version;
        VersionLoader::GetVersion(rootPath, version, INVALID_VERSION);
        ASSERT_EQ(0, version.GetVersionId());
        ASSERT_TRUE(version.GetSegmentVector().empty());
        ASSERT_EQ(mSchema->GetSchemaVersionId(), version.GetSchemaVersionId());
        ASSERT_EQ(100, version.GetTimestamp());
    }

    TearDown();
    SetUp();

    {
        // parallel empty
        size_t parallelNum = 2;
        ParallelBuildInfo info;
        info.parallelNum = parallelNum;
        info.batchId = 1;
        options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
        for (size_t i = 0; i < parallelNum; i ++) {
            info.instanceId = i;
            if (i % parallelNum == 0)
            {
                IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, "", options);
            }
            else
            {
                string incDoc = "cmd=add,string1=hello" + StringUtil::toString(i + 4) + ",price=8,ts=2;"
                                "cmd=add,string1=hello" + StringUtil::toString(i + 8) +
                                ",price=9,modify_fields=string2#price;ts=" + StringUtil::toString(i + 2);
                IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc, options);
            }
        }
        unique_ptr<IndexPartitionMerger> merger(
                (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                        rootPath, info, options, NULL, ""));
        merger->PrepareMerge(0);
        merger::MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
        // this step would merge all segments into one segment
        merger->DoMerge(false, meta, 0);
        merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());

        Version version;
        VersionLoader::GetVersion(rootPath, version, INVALID_VERSION);
        ASSERT_EQ(1, version.GetVersionId());
        ASSERT_TRUE(!version.GetSegmentVector().empty());
        ASSERT_EQ(mSchema->GetSchemaVersionId(), version.GetSchemaVersionId());
    }
}

void ParallelBuildInteTest::TestBuildWithPackageFileInParentPath()
{
    string rootPath = GET_TEST_DATA_PATH();
    PrepareFullData(false);
    size_t parallelNum = 4;
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    auto options = mOptions;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        string incDoc = "cmd=add,string1=hello" + StringUtil::toString(i + 1) +
                        ",string2=die,price=8,ts=2;cmd=add,string1=hello" +
                        StringUtil::toString(i + 5) + ",string2=wangwang,price=9,ts=3;" +
                        "cmd=add,string1=hello" + StringUtil::toString(i + 9) +
                        ",string2=wang,price=8,ts=4;cmd=delete,string1=hello" +
                        StringUtil::toString(i + 5) + ",ts=5;";
        IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc, options);
    }
    unique_ptr<IndexPartitionMerger> merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    rootPath, info, options, NULL, ""));
    //merger->Merge(false);
    vector<string> lostSegments = MakeLostSegments(rootPath, 20, 3);
    CheckPathExist(lostSegments, true);
    merger->PrepareMerge(0);
    CheckPathExist(lostSegments, false);
    CheckMetaFile(rootPath);
    MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
    // this step would merge all segments into one segment
    merger->DoMerge(false, meta, 0);
    merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:dengdeng", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:wangwang", ""));
    // ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:wang", "docid=6,pk=hello9;docid=9,pk=hello10;"
    //                          "docid=12,pk=hello11;docid=15,pk=hello12;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:wang", "docid=4,string1=hello9;"
                             "docid=5,string1=hello10;"
                             "docid=6,string1=hello11;docid=7,string1=hello12;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:die", "docid=0,string1=hello1;"
                             "docid=1,string1=hello2;" "docid=2,string1=hello3;"
                             "docid=3,string1=hello4;"));
}

void ParallelBuildInteTest::TestDeletionMapMergedForSubDoc()
{
    string subField = "sub_string1:string;sub_string2:string";
    string subIndex = "sub_index2:string:sub_string2;"
                      "sub_pk:primarykey64:sub_string1";
    string subAttribute = "sub_string1";
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            subField, subIndex, subAttribute, "");
    mSchema->SetSubIndexPartitionSchema(subSchema);

    string docString = "cmd=add,string1=hello1,price=4,string2=dengdeng,sub_string1=sub1^sub2,sub_string2=d1^d2;"
                       "cmd=add,string1=hello2,price=5,string2=jiangjiang,sub_string1=sub3^sub4,sub_string2=j1^j2;"
                       "cmd=add,string1=hello3,price=6,string2=huhu,sub_string1=sub5^sub6,sub_string2=h1^h2;"
                       "cmd=add,string1=hello4,price=7,string2=xixi,sub_string1=sub7^sub8,sub_string2=x1^x2;";
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig(false).maxDocCount = 2;
    options.GetBuildConfig(true).maxDocCount = 2;
    string rootPath = GET_TEST_DATA_PATH();
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,"", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_index2:j1", "docid=2,sub_string1=sub3"));
    }

    size_t parallelNum = 4;
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        string incDoc = "cmd=add,string1=hello" + StringUtil::toString(i + 1) +
                        ",string2=die,price=8,sub_string1=die_sub" + StringUtil::toString(i + 1) + ",sub_string2=die1,ts=2;"
                        "cmd=add,string1=hello" + StringUtil::toString(i + 5) +
                        ",string2=wangwang,price=9,sub_string1=wangwang_sub" + StringUtil::toString(i + 5) + ",sub_string2=wangwang1,ts=3;" +
                        "cmd=add,string1=hello" + StringUtil::toString(i + 9) +
                        ",string2=wang,price=8,sub_string1=wang_sub" + StringUtil::toString(i + 9) + ",sub_string2=wang1,ts=4;"
                        "cmd=delete,string1=hello" + StringUtil::toString(i + 5) + ",ts=5;";
        IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc, options);
    }
    unique_ptr<IndexPartitionMerger> merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    rootPath, info, options, NULL, ""));
    //merger->Merge(false);
    vector<string> lostSegments = MakeLostSegments(rootPath, 20, 3);
    CheckPathExist(lostSegments, true);
    merger->PrepareMerge(0);
    CheckPathExist(lostSegments, false);
    CheckMetaFile(rootPath);
    MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
    // this step would merge all segments into one segment
    merger->DoMerge(false, meta, 0);
    merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_index2:d1", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:wangwang", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_index2:wangwang1", ""));
    // ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:wang", "docid=6,pk=hello9;docid=9,pk=hello10;"
    //                          "docid=12,pk=hello11;docid=15,pk=hello12;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_index2:wang1",
                             "sub_string1=wang_sub9;sub_string1=wang_sub10;"
                             "sub_string1=wang_sub11;sub_string1=wang_sub12;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_index2:die1",
                             "sub_string1=die_sub1;sub_string1=die_sub2;"
                             "sub_string1=die_sub3;sub_string1=die_sub4;"));
}

void ParallelBuildInteTest::TestCounterMerge()
{
    string rootPath = GET_TEST_DATA_PATH();
    PrepareFullData();
    size_t parallelNum = 4;
    vector<string> mergeSrc;
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    auto options = mOptions;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        mergeSrc.push_back(info.GetParallelInstancePath(rootPath));
        string incDoc = "cmd=add,string1=hello" +
                        StringUtil::toString(i + 5) + ",string2=wangwang,price=9,ts=3;" +
                        "cmd=add,string1=hello" + StringUtil::toString(i + 9) +
                        ",string2=wang,price=8,ts=4;cmd=delete,string1=hello" +
                        StringUtil::toString(i + 1) + ",ts=5;";
        IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc, options);
        CounterMapPtr counterMap = LoadCounterMap(mergeSrc.back());
        ASSERT_EQ(6, counterMap->GetStateCounter("offline.partitionDocCount")->Get());
        //ASSERT_EQ(1, counterMap->GetStateCounter("offline.deletedDocCount")->Get());
        ASSERT_EQ(2, counterMap->GetAccCounter("offline.build.addDocCount")->Get());
        ASSERT_EQ(1, counterMap->GetAccCounter("offline.build.deleteDocCount")->Get());
    }
    unique_ptr<IndexPartitionMerger> merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    rootPath, info, options, NULL, ""));

    vector<string> lostSegments = MakeLostSegments(rootPath, 20, 3);
    CheckPathExist(lostSegments, true);
    merger->PrepareMerge(0);
    CheckPathExist(lostSegments, false);
    CheckMetaFile(rootPath);
    CounterMapPtr counterMap = LoadCounterMap(rootPath);
    ASSERT_EQ(12, counterMap->GetStateCounter("offline.partitionDocCount")->Get());
    ASSERT_EQ(4, counterMap->GetStateCounter("offline.deletedDocCount")->Get());
    ASSERT_EQ(12, counterMap->GetAccCounter("offline.build.addDocCount")->Get());
    ASSERT_EQ(4, counterMap->GetAccCounter("offline.build.deleteDocCount")->Get());
}

void ParallelBuildInteTest::TestRecoverBuildVersionForIncInstance()
{
    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello,price=5,modify_fields=price;"
                       "cmd=add,string1=hello1,price=6;"
                       "cmd=add,string1=hello1,price=7,modify_fields=string2#price;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    string rootPath = GET_TEST_DATA_PATH();

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,"", ""));
    }

    Version version;
    VersionLoader::GetVersion(rootPath, version, INVALID_VERSION);

    size_t parallelNum = 2;
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    info.batchId = 1;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        string incDoc = "cmd=add,string1=hello" + StringUtil::toString(i + 2) + ",price=8,ts=2;"
                        "cmd=add,string1=hello" + StringUtil::toString(i + 4) +
                        ",price=9,modify_fields=string2#price;ts=" + StringUtil::toString(i + 2);
        IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc, options);

        string instancePath = info.GetParallelInstancePath(rootPath);
        Version instanceVersion;
        VersionLoader::GetVersion(instancePath, instanceVersion, INVALID_VERSION);
        ASSERT_EQ(instanceVersion.GetVersionId(), version.GetVersionId() + 1);
    }

    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        string incDoc = "cmd=add,string1=hello" + StringUtil::toString(i + 6) + ",price=8,ts=2;"
                        "cmd=add,string1=hello" + StringUtil::toString(i + 8) +
                        ",price=9,modify_fields=string2#price;ts=" + StringUtil::toString(i + 2);
        IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc, options);
        string instancePath = info.GetParallelInstancePath(rootPath);
        Version instanceVersion;
        VersionLoader::GetVersion(instancePath, instanceVersion, INVALID_VERSION);
        ASSERT_EQ(instanceVersion.GetVersionId(), version.GetVersionId() + 2);
    }
}

void ParallelBuildInteTest::TestMergeRecoveredBuildSegment()
{
    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello,price=5,modify_fields=price;"
                       "cmd=add,string1=hello1,price=6;"
                       "cmd=add,string1=hello1,price=7,modify_fields=string2#price;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    string rootPath = GET_TEST_DATA_PATH();

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,"", ""));
    }
    size_t parallelNum = 4;
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 0));
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 1));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_0")));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_1")));
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    info.batchId = 1;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        if (i % 2 == 0)
        {
            string incDoc = "cmd=add,string1=hello" + StringUtil::toString(i + 4) + ",price=8,ts=2;"
                            "cmd=add,string1=hello" + StringUtil::toString(i + 8) +
                            ",price=9,modify_fields=string2#price;ts=" + StringUtil::toString(i + 2);
            IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc, options);
        }
        else
        {
            string incDoc1 = "cmd=add,string1=hello" + StringUtil::toString(i + 4) + ",price=8,ts=2;";
            string incDoc2 = "cmd=add,string1=hello" + StringUtil::toString(i + 8) +
                             ",price=9,modify_fields=string2#price;ts=" + StringUtil::toString(i + 2);
            IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc1, options);
            IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc2, options);
        }
    }
    unique_ptr<IndexPartitionMerger> merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    rootPath, info, options, NULL, ""));
    //merger->Merge(false);

    vector<string> lostSegments = MakeLostSegments(rootPath, 20, 3);
    CheckPathExist(lostSegments, true);
    merger->PrepareMerge(0);
    CheckPathExist(lostSegments, false);
    CheckMetaFile(rootPath);
    MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
    merger->DoMerge(false, meta, 0);
    merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_INC,
                             "",
                             "pk:hello8", "docid=3"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_0")));
    ASSERT_FALSE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_1")));
}

void ParallelBuildInteTest::TestRecoverableSegmentNotInInstanceVersion()
{
    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello,price=5,modify_fields=price;"
                       "cmd=add,string1=hello1,price=6;"
                       "cmd=add,string1=hello1,price=7,modify_fields=string2#price;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    string rootPath = GET_TEST_DATA_PATH();

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,"", ""));
    }
    size_t parallelNum = 4;
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 0));
    FileSystemWrapper::MkDir(ParallelBuildInfo::GetParallelPath(rootPath, parallelNum, 1));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_0")));
    ASSERT_TRUE(FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(rootPath,
                            "parallel_4_1")));
    ParallelBuildInfo info;
    info.parallelNum = parallelNum;
    info.batchId = 1;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < parallelNum; i ++) {
        info.instanceId = i;
        string incDoc = "cmd=add,string1=hello" + StringUtil::toString(i + 4) + ",price=8,ts=2;"
                        "cmd=add,string1=hello" + StringUtil::toString(i + 8) +
                        ",price=9,modify_fields=string2#price;ts=" + StringUtil::toString(i + 2);
        IndexlibPartitionCreator::BuildParallelIncData(mSchema, rootPath, info, incDoc, options);

        string instancePath = info.GetParallelInstancePath(rootPath);
        fslib::FileList fileList;
        VersionLoader::ListVersion(instancePath, fileList);
        for (auto file : fileList)
        {
            FileSystemWrapper::DeleteFile(FileSystemWrapper::JoinPath(instancePath, file));
        }
    }
    unique_ptr<IndexPartitionMerger> merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    rootPath, info, options, NULL, ""));
    ASSERT_THROW(merger->PrepareMerge(0), InconsistentStateException);
}

void ParallelBuildInteTest::PrepareFullData(bool needMerge)
{
    string docString = "cmd=add,string1=hello1,price=4,string2=dengdeng;"
                       "cmd=add,string1=hello2,price=5,string2=jiangjiang;"
                       "cmd=add,string1=hello3,price=6,string2=huhu;"
                       "cmd=add,string1=hello4,price=7,string2=xixi";

    string rootPath = GET_TEST_DATA_PATH();
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, rootPath));
        if (needMerge)
        {
            ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,"", ""));
        }
        else
        {
            ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString,"", ""));
        }

        Version version;
        VersionLoader::GetVersion(rootPath, version, INVALID_VERSION);
        version.SetLastSegmentId(version.GetLastSegment() + 1);
        version.Store(rootPath, true);
        ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:dengdeng", "docid=0"));
    }
}

CounterMapPtr ParallelBuildInteTest::LoadCounterMap(const std::string& rootPath)
{
    Version version;
    VersionLoader::GetVersion(rootPath, version, INVALID_VERSION);
    string segmentDir = version.GetSegmentDirName(version.GetLastSegment());
    string segmentPath = FileSystemWrapper::JoinPath(rootPath, segmentDir);
    string counterString;
    FileSystemWrapper::AtomicLoad(FileSystemWrapper::JoinPath(segmentPath, COUNTER_FILE_NAME)
                                  , counterString);
    CounterMapPtr counterMap(new CounterMap);
    counterMap->FromJsonString(counterString);
    return counterMap;
}


vector<string> ParallelBuildInteTest::MakeLostSegments(
        const string& rootPath, segmentid_t startSegId, size_t segCount)
{
    vector<string> segPathVec;
    Version version;
    VersionLoader::GetVersion(rootPath, version, INVALID_VERSION);
    for (size_t i = 0; i < segCount; i++)
    {
        segmentid_t segId = startSegId + i;
        version.AddSegment(segId);
        string path = FileSystemWrapper::JoinPath(rootPath, version.GetSegmentDirName(segId));
        FileSystemWrapper::MkDirIfNotExist(path);
        segPathVec.push_back(path);
    }
    return segPathVec;
}

void ParallelBuildInteTest::CheckPathExist(const vector<string>& pathVec, bool exist)
{
    for (auto& path : pathVec)
    {
        ASSERT_EQ(exist, FileSystemWrapper::IsExist(path)) << path;
    }
}

void ParallelBuildInteTest::CheckMetaFile(const string& rootPath, bool checkPartitionPatch)
{
    Version version;
    VersionLoader::GetVersion(rootPath, version, INVALID_VERSION);
    if (checkPartitionPatch) {
        string patchMetaFile = PartitionPatchMeta::GetPatchMetaFileName(version.GetVersionId());
        ASSERT_TRUE(FileSystemWrapper::IsExist(
                        PathUtil::JoinPath(rootPath, patchMetaFile)))
            << PathUtil::JoinPath(rootPath, patchMetaFile);
    }
    string dpMetaFile = DeployIndexWrapper::GetDeployMetaFileName(version.GetVersionId());
    ASSERT_TRUE(FileSystemWrapper::IsExist(PathUtil::JoinPath(rootPath, dpMetaFile))) << PathUtil::JoinPath(rootPath, dpMetaFile);
}

IE_NAMESPACE_END(partition);
