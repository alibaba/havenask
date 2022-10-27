#include "indexlib/merger/document_reclaimer/test/document_reclaimer_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/merger/multi_partition_merger.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer_param.h"
#include "indexlib/storage/file_system_wrapper.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, DocumentReclaimerTest);

DocumentReclaimerTest::DocumentReclaimerTest()
{
}
DocumentReclaimerTest::~DocumentReclaimerTest()
{
}

void DocumentReclaimerTest::CaseSetUp()
{
    string field = "pk:string:pk;long1:uint32;string1:string";
    string index = "pk:primarykey64:pk;long1:number:long1;string1:string:string1;";
    string attr = "long1;pk";
    string summary = "long1;";
    mSchema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);

    mRootDir = GET_TEST_DATA_PATH();
}

void DocumentReclaimerTest::CaseTearDown()
{
}

void DocumentReclaimerTest::testSearchTermFailInAndQuery()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string docString = "cmd=add,pk=1,long1=1,string1=str1;"
                       "cmd=add,pk=2,long1=2,string1=str1;"
                       "cmd=add,pk=3,long1=3,string1=str2;"
                       "cmd=add,pk=4,long1=2,string1=str2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString,
                                    "long1:2", "docid=1;docid=3"));

    string paramJson = R"(
    [
        {
            "reclaim_operator": "AND",
            "reclaim_index_info" : [
                {"reclaim_index": "long1", "reclaim_term" : "2"},
                {"reclaim_index": "long1", "reclaim_term" : "9"}
            ]
        }
    ])";
    PrepareReclaimConfig(paramJson);
    PartitionMergerPtr partMerger(PartitionMergerCreator::CreateSinglePartitionMerger(
                    mRootDir, mOptions, NULL, ""));
    partMerger->Merge();
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:2", "docid=1;docid=3"));

    paramJson = R"(
    [
        {
            "reclaim_operator": "AND",
            "reclaim_index_info" : [
                {"reclaim_index": "long1", "reclaim_term" : "2"},
                {"reclaim_index": "long2", "reclaim_term" : "9"}
            ]
        }
    ])";
    PrepareReclaimConfig(paramJson);
    partMerger.reset(PartitionMergerCreator::CreateSinglePartitionMerger(
                   mRootDir, mOptions, NULL, ""));
    ASSERT_THROW(partMerger->Merge(), BadParameterException);
}

void DocumentReclaimerTest::TestSinglePartitionMerge()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string docString = "cmd=add,pk=1,long1=1;"
                       "cmd=add,pk=2,long1=2;"
                       "cmd=add,pk=3,long1=3;"
                       "cmd=add,pk=4,long1=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString,
                                    "long1:2", "docid=1;docid=3"));

    SetDocReclaimParam("long1=2;");
    PartitionMergerPtr partMerger(PartitionMergerCreator::CreateSinglePartitionMerger(
                    mRootDir, mOptions, NULL, ""));
    partMerger->Merge();
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:2", ""));
}

void DocumentReclaimerTest::TestSinglePartitionMergeWithReclaimOperator()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string docString = "cmd=add,pk=1,long1=1,string1=str1;"
                       "cmd=add,pk=2,long1=2,string1=str1;"
                       "cmd=add,pk=3,long1=3,string1=str2;"
                       "cmd=add,pk=4,long1=2,string1=str2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString,
                                    "long1:2", "docid=1;docid=3"));

    string paramJson = R"(
    [
        {
            "reclaim_operator": "AND",
            "reclaim_index_info" : [
                {"reclaim_index": "long1", "reclaim_term" : "2"},
                {"reclaim_index": "long1", "reclaim_term" : "9"},
                {"reclaim_index": "string1", "reclaim_term" : "str1"}
            ]
        }
    ])";
    PrepareReclaimConfig(paramJson);
    PartitionMergerPtr partMerger(PartitionMergerCreator::CreateSinglePartitionMerger(
                    mRootDir, mOptions, NULL, ""));
    partMerger->Merge();
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:2", "docid=1;docid=3"));
}

void DocumentReclaimerTest::TestSinglePartitionMergeWithReclaimOperatorWithSpecialParam()
{
    {
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        string docString = "cmd=add,pk=1,long1=1,string1=str1;"
            "cmd=add,pk=2,long1=2,string1=str1;"
            "cmd=add,pk=3,long1=3,string1=str2;"
            "cmd=add,pk=4,long1=2,string1=str2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString,
                                        "long1:2", "docid=1;docid=3"));        
        string paramJson = R"(
        [
            {
                "reclaim_operator": "AND",
                "reclaim_index_info" : [
                    {"reclaim_index": "non-existed-index", "reclaim_ter" : "2"}
                ]
            }
        ])";
        PrepareReclaimConfig(paramJson);
        PartitionMergerPtr partMerger(PartitionMergerCreator::CreateSinglePartitionMerger(
                                          mRootDir, mOptions, NULL, ""));
        ASSERT_THROW(partMerger->Merge(), BadParameterException);
    }
    TearDown();
    SetUp();
    {
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        string docString = "cmd=add,pk=1,long1=1,string1=str1;"
            "cmd=add,pk=2,long1=2,string1=str1;"
            "cmd=add,pk=3,long1=3,string1=str2;"
            "cmd=add,pk=4,long1=2,string1=str2;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString,
                                        "long1:2", "docid=1;docid=3")); 
        string paramJson = R"(
        [
            {
                "reclaim_operator": "AND",
                "reclaim_index_info" : [
                    {"reclaim_inde": "long1", "reclaim_term" : "2"}
                ]
            }
        ])";
        PrepareReclaimConfig(paramJson);
        PartitionMergerPtr partMerger(PartitionMergerCreator::CreateSinglePartitionMerger(
                                          mRootDir, mOptions, NULL, ""));
        ASSERT_THROW(partMerger->Merge(), BadParameterException);
    } 
}

void DocumentReclaimerTest::TestMultiPartitionMerge()
{
    int64_t currentTime = TimeUtility::currentTimeInSeconds();
    string obsoleteTTLField =
        string("ttl=") + StringUtil::toString(currentTime - 1) + ";";
    string notObsoleteTTLField =
        string("ttl=") + StringUtil::toString(currentTime + 10000) + ";";
    string partOneDocStr = "cmd=add,pk=1,long1=1," + notObsoleteTTLField +
        "cmd=add,pk=6,long1=6," + obsoleteTTLField +
        "cmd=add,pk=2,long1=2," + notObsoleteTTLField;

    string partTwoDocStr = "cmd=add,pk=3,long1=3," + notObsoleteTTLField +
        "cmd=add,pk=4,long1=2," + notObsoleteTTLField +
        "cmd=add,pk=7,long1=7," + obsoleteTTLField;
    // prepare partitions
    vector<string> docStrs;
    docStrs.push_back(partOneDocStr);
    docStrs.push_back(partTwoDocStr);
    vector<string> partPaths;
    partPaths.push_back("part1");
    partPaths.push_back("part2");
    assert(docStrs.size() == partPaths.size());
    mOptions.GetBuildConfig(false).maxDocCount = 2;
    mSchema->SetEnableTTL(true, DEFAULT_REGIONID, "ttl");
    for (size_t i = 0; i < docStrs.size(); ++i)
    {
        GET_PARTITION_DIRECTORY()->MakeDirectory(partPaths[i]);
        string rootDir = GET_TEST_DATA_PATH() + partPaths[i];
        PartitionStateMachine psm;
        psm.Init(mSchema, mOptions, rootDir);
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrs[i], "", ""));
    }
    // multi partition merge
    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");

    GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";
    mOptions.SetIsOnline(false);
    SetDocReclaimParam("long1=2;");
    MultiPartitionMerger multiPartMerger(mOptions, NULL, "");
    multiPartMerger.Merge(mergeSrcs, mergePartPath);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mergePartPath));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "long1:2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "long1:6", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "long1:7", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "long1:1", "docid=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "long1:3", "docid=1"));
}

void DocumentReclaimerTest::testReclaimOldTTLDoc()
{
    string field = "string1:string;price:uint32";
    string index = "pk:primarykey64:string1;index:number:price";
    string attribute = "string1;price";
    IndexPartitionSchemaPtr schema = 
        SchemaMaker::MakeSchema(field, index, attribute, "");
    schema->SetEnableTTL(true, DEFAULT_REGIONID, "ttl_field");
    if (schema->GetAttributeSchema()->GetAttributeConfig(DOC_TIME_TO_LIVE_IN_SECONDS)) {
        cout << "has old ttl attribute" << endl;
    }
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;
    options.GetBuildConfig(false).keepVersionCount = 100;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetMergeConfig().mergeStrategyStr = "specific_segments";
    options.GetMergeConfig().mergeStrategyParameter.SetLegacyString("merge_segments=0,2");

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // make doc not deleted by ttl
    int64_t currentTime = TimeUtility::currentTimeInSeconds();
    string obsoleteTTLField = string("ttl_field") + 
        "=" + StringUtil::toString<int64_t>(currentTime - 1000);
    string notObsoleteTTLField = string("ttl_field") + 
        "=" + StringUtil::toString<int64_t>(currentTime + 5000);

    // seg0 all obsolete, seg1 all obsolete,
    // seg2 one doc obsolete, seg3 one doc obsolete
    string fullDocs = "cmd=add,string1=pk1,price=1," + obsoleteTTLField + ";"
        "cmd=add,string1=pk2,price=1," + obsoleteTTLField + ";"
        "cmd=add,string1=pk3,price=1," + obsoleteTTLField + ";"
        "cmd=add,string1=pk4,price=1," + obsoleteTTLField + ";"
        "cmd=add,string1=pk5,price=1," + obsoleteTTLField + ";"
        "cmd=add,string1=pk6,price=1," + notObsoleteTTLField + ";"
        "cmd=add,string1=pk7,price=1," + obsoleteTTLField + ";"
        "cmd=add,string1=pk8,price=1," + notObsoleteTTLField + ";";
    ASSERT_TRUE(psm.Transfer(PE_BUILD_FULL, fullDocs, "index:1",
                             "docid=0;docid=1;docid=2;docid=3;docid=4;docid=5;docid=6;docid=7"));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "index:1", "string1=pk8,docid=1;string1=pk6,docid=2"));
}

// void DocumentReclaimerTest::testReclaimOldTTLDocWithParallelBuild()
// {
//     string partOneDocStr = "cmd=add,pk=1,long1=1;"
//                            "cmd=add,pk=2,long1=2;";
//     string partTwoDocStr = "cmd=add,pk=3,long1=3;"
//                            "cmd=add,pk=4,long1=2;";
//     // prepare partitions
//     vector<string> docStrs;
//     docStrs.push_back(partOneDocStr);
//     docStrs.push_back(partTwoDocStr);
//     vector<string> partPaths;
//     partPaths.push_back("part1");
//     partPaths.push_back("part2");
//     assert(docStrs.size() == partPaths.size());
//     for (size_t i = 0; i < docStrs.size(); ++i)
//     {
//         GET_PARTITION_DIRECTORY()->MakeDirectory(partPaths[i]);
//         string rootDir = GET_TEST_DATA_PATH() + partPaths[i];
//         PartitionStateMachine psm;
//         psm.Init(mSchema, mOptions, rootDir);
//         ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrs[i], "", ""));
//     }
//     // multi partition merge
//     vector<string> mergeSrcs;
//     mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
//     mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");

//     GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
//     string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";
//     mOptions.SetIsOnline(false);
//     SetDocReclaimParam("long1=2;");
//     MultiPartitionMerger multiPartMerger(mOptions, NULL, "");
//     multiPartMerger.Merge(mergeSrcs, mergePartPath);

//     PartitionStateMachine psm;
//     ASSERT_TRUE(psm.Init(mSchema, mOptions, mergePartPath));
//     ASSERT_TRUE(psm.Transfer(QUERY, "", "long1:2", ""));
// }

void DocumentReclaimerTest::TestMultiPartitionMergeWithReclaimOperator()
{
    string partOneDocStr =
        "cmd=add,pk=1,long1=1,string1=hi;" 
        "cmd=add,pk=2,long1=2,string1=hello;"
        "cmd=add,pk=3,long1=2,string1=hi;"
        "cmd=add,pk=4,long1=2,string1=hello;";
    string partTwoDocStr =
        "cmd=add,pk=5,long1=3,string1=hi;"
        "cmd=add,pk=6,long1=2,string1=hello;"
        "cmd=add,pk=7,long1=1,string1=are;" 
        "cmd=add,pk=8,long1=2,string1=you;";
    // prepare partitions
    vector<string> docStrs;
    docStrs.push_back(partOneDocStr);
    docStrs.push_back(partTwoDocStr);
    vector<string> partPaths;
    partPaths.push_back("part1");
    partPaths.push_back("part2");
    assert(docStrs.size() == partPaths.size());
    for (size_t i = 0; i < docStrs.size(); ++i)
    {
        GET_PARTITION_DIRECTORY()->MakeDirectory(partPaths[i]);
        string rootDir = GET_TEST_DATA_PATH() + partPaths[i];
        PartitionStateMachine psm;
        psm.Init(mSchema, mOptions, rootDir);
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrs[i], "", ""));
    }
    // multi partition merge
    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");
    GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";
    mOptions.SetIsOnline(false);
    
    string paramJson = R"(
    [
        {
            "reclaim_index" : "long1",
            "reclaim_terms" : ["1"],
            "reclaim_operator" : "",
            "reclaim_index_info" : []
        },
        {
            "reclaim_index":   "",
            "reclaim_terms" : [],
            "reclaim_operator": "AND",
            "reclaim_index_info" : [
                {"reclaim_index": "long1", "reclaim_term" : "2"},
                {"reclaim_index": "string1", "reclaim_term" : "hello"}
            ]
        }
    ])";
    PrepareReclaimConfig(paramJson);
    MultiPartitionMerger multiPartMerger(mOptions, NULL, "");
    multiPartMerger.Merge(mergeSrcs, mergePartPath);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mergePartPath));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "long1:2", "docid=0;docid=2"));

    paramJson = R"(
    [
        {
            "reclaim_index" : "long1",
            "reclaim_terms" : ["1"],
            "reclaim_operator" : "",
            "reclaim_index_info" : []
        },
        {
            "reclaim_index":   "",
            "reclaim_terms" : [],
            "reclaim_operator": "AND",
            "reclaim_index_info" : [
                {"reclaim_index": "long1", "reclaim_term" : "10"},
                {"reclaim_index": "string1", "reclaim_term" : "test"}
            ]
        }
    ])";
    PrepareReclaimConfig(paramJson);
    MultiPartitionMerger multiPartMerger2(mOptions, NULL, "");
    multiPartMerger2.Merge(mergeSrcs, mergePartPath);
    PartitionStateMachine psm2;
    ASSERT_TRUE(psm2.Init(mSchema, mOptions, mergePartPath));
    ASSERT_TRUE(psm2.Transfer(QUERY, "", "long1:2", "docid=0;docid=2"));
}

void DocumentReclaimerTest::TestSinglePartitionMergeWithSubDoc()
{

    int64_t currentTime = TimeUtility::currentTimeInSeconds();
    string obsoleteTTLField =
        string("doc_time_to_live_in_seconds=") + StringUtil::toString(currentTime - 1) + ";";
    string notObsoleteTTLField =
        string("doc_time_to_live_in_seconds=") + StringUtil::toString(currentTime + 10000) + ";";
    PartitionStateMachine psm;
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            "subpkstr:string;", "sub_pk:primarykey64:subpkstr", "", "");
    mSchema->SetSubIndexPartitionSchema(subSchema);
    mSchema->SetEnableTTL(true);
    mOptions.GetMergeConfig().mergeStrategyStr = "specific_segments";
    mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString("merge_segments=0");
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string docString =
        "cmd=add,pk=1,subpkstr=sub1,long1=1," + notObsoleteTTLField +
        "cmd=add,pk=2,subpkstr=sub2,long1=2," + notObsoleteTTLField +
        "cmd=add,pk=3,subpkstr=sub3,long1=3," + notObsoleteTTLField +
        "cmd=add,pk=4,subpkstr=sub4,long1=2," + notObsoleteTTLField +
        "cmd=add,pk=5,subpkstr=sub5,long1=4,"  + obsoleteTTLField;
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString,
                                    "long1:2", "docid=1;docid=3"));

    SetDocReclaimParam("long1=2;");
    PartitionMergerPtr partMerger(PartitionMergerCreator::CreateSinglePartitionMerger(
                    mRootDir, mOptions, NULL, ""));
    partMerger->Merge();
    DeployIndexMeta deployIndex;
    string deployIndexPath = GET_TEST_DATA_PATH() + "segment_1_level_0/deploy_index";
    deployIndex.Load(deployIndexPath);
    string checkFilePath = "sub_segment/segment_info";
    ASSERT_TRUE(FindFromDeployIndex(checkFilePath, deployIndex));
    checkFilePath = "sub_segment/counter";
    ASSERT_TRUE(FindFromDeployIndex(checkFilePath, deployIndex));
    checkFilePath = "counter";
    ASSERT_TRUE(FindFromDeployIndex(checkFilePath, deployIndex));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "sub_pk:sub1", "docid=0;"));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "sub_pk:sub2", ""));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "sub_pk:sub3", "docid=1"));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "sub_pk:sub4", ""));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "sub_pk:sub5", ""));
}

void DocumentReclaimerTest::TestSinglePartitionMergeWithRecover()
{
    PartitionStateMachinePtr psm(new PartitionStateMachine);
    INDEXLIB_TEST_TRUE(psm->Init(mSchema, mOptions, mRootDir));
    string docString = "cmd=add,pk=1,long1=1;"
                       "cmd=add,pk=2,long1=2;"
                       "cmd=add,pk=3,long1=3;"
                       "cmd=add,pk=4,long1=2;";
    INDEXLIB_TEST_TRUE(psm->Transfer(BUILD_FULL_NO_MERGE, docString,
                                    "long1:2", "docid=1;docid=3"));
    storage::FileSystemWrapper::MkDir(mRootDir + "segment_1/deletionmap", true);
    storage::FileSystemWrapper::AtomicStore(mRootDir + "segment_1/deletionmap/data_0", "");

    SetDocReclaimParam("long1=2;");
    PartitionMergerPtr partMerger(PartitionMergerCreator::CreateSinglePartitionMerger(
                    mRootDir, mOptions, NULL, ""));
    partMerger->Merge();
    ASSERT_TRUE(psm->Transfer(PE_REOPEN, "", "long1:2", ""));
}

void DocumentReclaimerTest::TestMultiPartitionMergeWithRecover()
{
    string partOneDocStr = "cmd=add,pk=1,long1=1;"
                           "cmd=add,pk=2,long1=2;";
    string partTwoDocStr = "cmd=add,pk=3,long1=3;"
                           "cmd=add,pk=4,long1=2;";
    // prepare partitions
    vector<string> docStrs;
    docStrs.push_back(partOneDocStr);
    docStrs.push_back(partTwoDocStr);
    vector<string> partPaths;
    partPaths.push_back("part1");
    partPaths.push_back("part2");
    assert(docStrs.size() == partPaths.size());
    for (size_t i = 0; i < docStrs.size(); ++i)
    {
        GET_PARTITION_DIRECTORY()->MakeDirectory(partPaths[i]);
        string rootDir = GET_TEST_DATA_PATH() + partPaths[i];
        PartitionStateMachine psm;
        psm.Init(mSchema, mOptions, rootDir);
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrs[i], "", ""));
    }
    {
        // part1 merged succussfully
        PartitionStateMachine psm;
        psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH() + "part1");
        ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    }
    {
        // part2 merged failed
        string delMapPath = GET_TEST_DATA_PATH() + "part2/segment_1/deletionmap";
        storage::FileSystemWrapper::MkDir(delMapPath, true);
        storage::FileSystemWrapper::AtomicStore(delMapPath + "/data_0", "");
    }
    // multi partition merge
    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");

    GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";
    SetDocReclaimParam("long1=2;");
    MultiPartitionMerger multiPartMerger(mOptions, NULL, "");
    multiPartMerger.Merge(mergeSrcs, mergePartPath);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mergePartPath));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "long1:2", ""));
}

void DocumentReclaimerTest::TestDumplicateIndexName()
{
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string docString = "cmd=add,pk=1,long1=1;"
                       "cmd=add,pk=2,long1=2;"
                       "cmd=add,pk=3,long1=3;"
                       "cmd=add,pk=4,long1=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    SetDocReclaimParam("long1=2;long1=3;");
    PartitionMergerPtr partMerger(PartitionMergerCreator::CreateSinglePartitionMerger(
                    mRootDir, mOptions, NULL, ""));
    partMerger->Merge();
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:1", "pk=1"));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:2", ""));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:3", ""));
}

void DocumentReclaimerTest::TestDumplicateTerms()
{
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string docString = "cmd=add,pk=1,long1=1;"
                       "cmd=add,pk=2,long1=2;"
                       "cmd=add,pk=3,long1=3;"
                       "cmd=add,pk=4,long1=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    SetDocReclaimParam("long1=2;long1=2;");
    PartitionMergerPtr partMerger(PartitionMergerCreator::CreateSinglePartitionMerger(
                    mRootDir, mOptions, NULL, ""));
    partMerger->Merge();
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:1", "pk=1"));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:2", ""));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:3", "pk=3"));
}

void DocumentReclaimerTest::TestReclaimByPKIndex()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string docString = "cmd=add,pk=1,long1=1;"
                       "cmd=add,pk=2,long1=2;"
                       "cmd=add,pk=3,long1=3;"
                       "cmd=add,pk=4,long1=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString,
                                    "pk:2", "docid=1;"));

    SetDocReclaimParam("pk=2;");
    PartitionMergerPtr partMerger(PartitionMergerCreator::CreateSinglePartitionMerger(
                    mRootDir, mOptions, NULL, ""));
    partMerger->Merge();
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "pk:2", ""));
}

void DocumentReclaimerTest::TestConfigFileExceptions()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string docString = "cmd=add,pk=1,long1=1;"
                       "cmd=add,pk=2,long1=1;"
                       "cmd=add,pk=3,long1=1;"
                       "cmd=add,pk=4,long1=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    {
        // config file does not exist
        mOptions.GetMergeConfig().docReclaimConfigPath = GET_TEST_DATA_PATH()
                                                         + "no_such_file.json";
        PartitionMergerPtr partMerger(PartitionMergerCreator::
                CreateSinglePartitionMerger(mRootDir, mOptions, NULL, ""));
        ASSERT_NO_THROW(partMerger->Merge());
        ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:1", "pk=1;pk=2;pk=3;pk=4;"));
    }
    {
        // config file not json format
        string filePath = GET_TEST_DATA_PATH() + "not_json_format.json";
        string jsonStr = "[{"
                         "\"reclaim_index\" : \"long1\","
                         "\"reclaim_terms\" : [\"1\"]"
                         "}"; // missing "]"
        storage::FileSystemWrapper::AtomicStore(filePath, jsonStr);
        mOptions.GetMergeConfig().docReclaimConfigPath = filePath;
        PartitionMergerPtr partMerger(PartitionMergerCreator::
                CreateSinglePartitionMerger(mRootDir, mOptions, NULL, ""));
        ASSERT_THROW(partMerger->Merge(), BadParameterException);
        ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:1", "pk=1;pk=2;pk=3;pk=4;"));
    }
    {
        // config file is an empty file
        string filePath = GET_TEST_DATA_PATH() + "empty_file.json";
        string jsonStr = "";
        storage::FileSystemWrapper::AtomicStore(filePath, jsonStr);
        mOptions.GetMergeConfig().docReclaimConfigPath = filePath;
        PartitionMergerPtr partMerger(PartitionMergerCreator::
                CreateSinglePartitionMerger(mRootDir, mOptions, NULL, ""));
        ASSERT_THROW(partMerger->Merge(), BadParameterException);
        ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:1", "pk=1;pk=2;pk=3;pk=4;"));
    }
    {
        // config file is an empty list
        string filePath = GET_TEST_DATA_PATH() + "empty_list_file.json";
        string jsonStr = "[]";
        storage::FileSystemWrapper::AtomicStore(filePath, jsonStr);
        mOptions.GetMergeConfig().docReclaimConfigPath = filePath;
        PartitionMergerPtr partMerger(PartitionMergerCreator::
                CreateSinglePartitionMerger(mRootDir, mOptions, NULL, ""));
        ASSERT_NO_THROW(partMerger->Merge());
        ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:1", "pk=1;pk=2;pk=3;pk=4;"));
    }
    {
        // specify non-exist index name
        string filePath = GET_TEST_DATA_PATH() + "wrong_index.json";
        string jsonStr = "["
                             "{"
                                 "\"reclaim_index\" : \"long1\","
                                 "\"reclaim_terms\" : [\"1\"]"
                             "},"
                             "{"
                                 "\"reclaim_index\" : \"wrongindex\","
                                 "\"reclaim_terms\" : [\"1\"]"
                             "}"
                         "]"; 
        storage::FileSystemWrapper::AtomicStore(filePath, jsonStr);
        mOptions.GetMergeConfig().docReclaimConfigPath = filePath;
        PartitionMergerPtr partMerger(PartitionMergerCreator::
                CreateSinglePartitionMerger(mRootDir, mOptions, NULL, ""));
        ASSERT_THROW(partMerger->Merge(), BadParameterException);
        ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:1", "pk=1;pk=2;pk=3;pk=4;"));
    }
}

void DocumentReclaimerTest::SetDocReclaimParam(const string& param)
{
    string reclaimConfigPath = GET_TEST_DATA_PATH() + "reclaim_config_file.json";

    vector<vector<string> > paramVec;
    StringUtil::fromString(param, paramVec, "=", ";");

    vector<IndexReclaimerParam> indexReclaimerParamVec;
    for (size_t i = 0; i < paramVec.size(); ++i)
    {
        IndexReclaimerParam param;
        assert(paramVec[i].size() == 2);
        param.SetReclaimIndex(paramVec[i][0]);
        vector<string> terms;
        terms.push_back(paramVec[i][1]);
        param.SetReclaimTerms(terms);
        indexReclaimerParamVec.push_back(param);
    }
    string reclaimConfigJsonStr = ToJsonString(indexReclaimerParamVec);
    storage::FileSystemWrapper::AtomicStore(reclaimConfigPath, reclaimConfigJsonStr);
    mOptions.GetMergeConfig().docReclaimConfigPath = reclaimConfigPath;
}

void DocumentReclaimerTest::PrepareReclaimConfig(const string& paramStr)
{
    string reclaimConfigPath = GET_TEST_DATA_PATH() + "reclaim_config_file.json";
    storage::FileSystemWrapper::DeleteIfExist(reclaimConfigPath);
    vector<IndexReclaimerParam> reclaimParams;
    FromJsonString(reclaimParams, paramStr);
    string fileContent = ToJsonString(reclaimParams);
    storage::FileSystemWrapper::AtomicStore(reclaimConfigPath, fileContent);
    mOptions.GetMergeConfig().docReclaimConfigPath = reclaimConfigPath;
}

void DocumentReclaimerTest::TestReopenReclaimSegment()
{
    PartitionStateMachine psm;
    mSchema->SetEnableTTL(true);
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    int64_t currentTime = TimeUtility::currentTimeInSeconds();    
    string ttl100 = DOC_TIME_TO_LIVE_IN_SECONDS + "=" +
        StringUtil::toString<int64_t>(currentTime + 100);
    string ttl200 = DOC_TIME_TO_LIVE_IN_SECONDS + "=" +
        StringUtil::toString<int64_t>(currentTime + 200);
    
    string docString = "cmd=add,pk=1,long1=1," + ttl100 + ",ts=1,locator=1;"
                       "cmd=add,pk=2,long1=2," + ttl100 + ",ts=2,locator=2;"
                       "cmd=add,pk=3,long1=3," + ttl100 + ",ts=3,locator=3;"
                       "cmd=add,pk=4,long1=2," + ttl100 + ",ts=4,locator=4;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString,
                                    "long1:2", "docid=1;docid=3"));

    string docString2 = "cmd=add,pk=5,long1=5," + ttl200 + ",ts=5,locator=5;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString2,
                                    "long1:2", "docid=1;docid=3"));

    SetDocReclaimParam("long1=2;");
    mOptions.GetMergeConfig().mergeStrategyStr = "balance_tree";
    mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString("conflict-segment-number=10;base-doc-count=10;max-doc-count=10;");
    PartitionMergerPtr partMerger(
            PartitionMergerCreator::CreateSinglePartitionMerger(
                    mRootDir, mOptions, NULL, ""));
    partMerger->Merge();
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:2", ""));
    SegmentInfo segInfo;
    segInfo.Load(mRootDir + "segment_2_level_0/segment_info");
    ASSERT_EQ((uint32_t)0, segInfo.docCount);
    ASSERT_EQ((int64_t)5, segInfo.timestamp);
    Locator expectLocator;
    expectLocator.SetLocator("5");
    ASSERT_EQ(expectLocator, segInfo.locator);
    ASSERT_EQ((uint32_t)currentTime + 100, segInfo.maxTTL);
    ASSERT_EQ(false, segInfo.mergedSegment);
}

bool DocumentReclaimerTest::FindFromDeployIndex(const string& filePath, DeployIndexMeta& deployIndex) const
{
    for (auto fileInfo : deployIndex.deployFileMetas) {
        if (filePath == fileInfo.filePath) {
            return true;
        }
    }
    return false;
}

IE_NAMESPACE_END(merger);

