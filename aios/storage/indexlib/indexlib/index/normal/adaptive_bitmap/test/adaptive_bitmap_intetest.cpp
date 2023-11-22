#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/test/truncate_config_maker.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/file_system/archive/ArchiveFile.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/test/deploy_index_checker.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;

using namespace indexlib::common;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::test;
using namespace indexlib::document;
using namespace indexlib::file_system;

namespace indexlib { namespace index { namespace legacy {

#define VOL_NAME "title_vol"
#define VOL_CONTENT "token1"

class AdaptiveBitmapInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    typedef DocumentMaker::MockIndexPart MockIndexPart;

public:
    DECLARE_CLASS_NAME(AdaptiveBitmapInteTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEMP_DATA_PATH();

        mOptions = IndexPartitionOptions();
        util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &mOptions);
        mOptions.GetMergeConfig().mergeThreadCount = 1;

        std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
        autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

        ResetIndexPartitionSchema();
        mRawDocStr = "{ 1, "
                     "[title: (token1)],"
                     "[ends: (1)],"
                     "[userid: (1)],"
                     "[nid: (1)],"
                     "[url: (url1)],"
                     "};"
                     "{ 2, "
                     "[title: (token1 token2)],"
                     "[ends: (2)],"
                     "[userid: (2)],"
                     "[url: (url2)],"
                     "[nid: (2)],"
                     "};"
                     "{ 3, "
                     "[title: (token1 token2 token3)],"
                     "[ends: (3)],"
                     "[userid: (3)],"
                     "[nid: (3)],"
                     "[url: (url3)],"
                     "};"
                     "{ 4, "
                     "[title: (token1 token2 token3 token4)],"
                     "[ends: (4)],"
                     "[userid: (4)],"
                     "[nid: (4)],"
                     "[url: (url4)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token2 token3 token4 token5)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (6)],"
                     "[url: (url5)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token3 token5)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (5)],"
                     "[url: (url6)],"
                     "};"
                     "{ 4, "
                     "[title: (token2 token4)],"
                     "[ends: (4)],"
                     "[userid: (4)],"
                     "[nid: (7)],"
                     "[url: (url7)],"
                     "};";
    }

    void CaseTearDown() override {}

    void TestCaseForAdaptiveBitmapWithoutHFDict_1()
    {
        InnerTestAdaptiveBitmap("title", "DOC_FREQUENCY#3",
                                "token1#0,1,2,3,4,5;token2#1,2,3,4,6;token3#2,3,4,5;token4#3,4,6", false, hp_bitmap);

        InnerTestAdaptiveBitmap("title", "DOC_FREQUENCY#3",
                                "token1#0,1,2,4,5,6;token2#1,2,3,4,5;token3#1,2,5,6;token4#2,3,5", false, hp_bitmap,
                                false, true);

        InnerTestAdaptiveBitmap("title", "DOC_FREQUENCY#3",
                                "token1#0,1,2,3,4,5;token2#1,2,3,4,6;token3#2,3,4,5;token4#3,4,6", false, hp_both);
    }

    void TestCaseForAdaptiveBitmapWithoutHFDict_2()
    {
        InnerTestAdaptiveBitmap("title", "DOC_FREQUENCY#3",
                                "token1#0,1,2,4,5,6;token2#1,2,3,4,5;token3#1,2,5,6;token4#2,3,5", false, hp_both,
                                false, true);

        InnerTestAdaptiveBitmap("title", "PERCENT#43",
                                "token1#0,1,2,3,4,5;token2#1,2,3,4,6;token3#2,3,4,5;token4#3,4,6", false, hp_bitmap);

        InnerTestAdaptiveBitmap("title", "PERCENT#43",
                                "token1#0,1,2,4,5,6;token2#1,2,3,4,5;token3#1,2,5,6;token4#2,3,5", false, hp_bitmap,
                                false, true);
    }

    void TestCaseForAdaptiveBitmapWithoutHFDict_3()
    {
        InnerTestAdaptiveBitmap("title", "PERCENT#43",
                                "token1#0,1,2,3,4,5;token2#1,2,3,4,6;token3#2,3,4,5;token4#3,4,6", false, hp_both);

        InnerTestAdaptiveBitmap("title", "PERCENT#43",
                                "token1#0,1,2,4,5,6;token2#1,2,3,4,5;token3#1,2,5,6;token4#2,3,5", false, hp_both,
                                false, true);

        InnerTestAdaptiveBitmap("title", "INDEX_SIZE#-1",
                                "token1#0,1,2,3,4,5;token2#1,2,3,4,6;token3#2,3,4,5;token4#3,4,6;token5#4,5", false,
                                hp_bitmap);
    }

    void TestCaseForAdaptiveBitmapWithoutHFDict_4()
    {
        InnerTestAdaptiveBitmap("title", "INDEX_SIZE#-1",
                                "token1#0,1,2,4,5,6;token2#1,2,3,4,5;token3#1,2,5,6;token4#2,3,5;token5#2,6", false,
                                hp_bitmap, false, true);

        InnerTestAdaptiveBitmap("title", "INDEX_SIZE#-1",
                                "token1#0,1,2,3,4,5;token2#1,2,3,4,6;token3#2,3,4,5;token4#3,4,6;token5#4,5", false,
                                hp_both);

        InnerTestAdaptiveBitmap("title", "INDEX_SIZE#-1",
                                "token1#0,1,2,4,5,6;token2#1,2,3,4,5;token3#1,2,5,6;token4#2,3,5;token5#2,6", false,
                                hp_both, false, true);
    }

    void TestCaseForAdaptiveBitmapWithHFDict_1()
    {
        InnerTestAdaptiveBitmap("title", "DOC_FREQUENCY#3",
                                "token1#0,1,2,3,4,5;token2#1,2,3,4,6;token3#2,3,4,5;token4#3,4,6", true, hp_bitmap);

        InnerTestAdaptiveBitmap("title", "DOC_FREQUENCY#3",
                                "token1#0,1,2,4,5,6;token2#1,2,3,4,5;token3#1,2,5,6;token4#2,3,5", true, hp_bitmap,
                                false, true);

        InnerTestAdaptiveBitmap("title", "DOC_FREQUENCY#3",
                                "token1#0,1,2,3,4,5;token2#1,2,3,4,6;token3#2,3,4,5;token4#3,4,6", true, hp_both);
    }

    void TestCaseForAdaptiveBitmapWithHFDict_2()
    {
        InnerTestAdaptiveBitmap("title", "DOC_FREQUENCY#3",
                                "token1#0,1,2,4,5,6;token2#1,2,3,4,5;token3#1,2,5,6;token4#2,3,5", true, hp_both, false,
                                true);

        InnerTestAdaptiveBitmap("title", "PERCENT#43",
                                "token1#0,1,2,3,4,5;token2#1,2,3,4,6;token3#2,3,4,5;token4#3,4,6", true, hp_bitmap);

        InnerTestAdaptiveBitmap("title", "PERCENT#43",
                                "token1#0,1,2,4,5,6;token2#1,2,3,4,5;token3#1,2,5,6;token4#2,3,5", true, hp_bitmap,
                                false, true);
    }

    void TestCaseForAdaptiveBitmapWithHFDict_3()
    {
        InnerTestAdaptiveBitmap("title", "PERCENT#43",
                                "token1#0,1,2,3,4,5;token2#1,2,3,4,6;token3#2,3,4,5;token4#3,4,6", true, hp_both);

        InnerTestAdaptiveBitmap("title", "PERCENT#43",
                                "token1#0,1,2,4,5,6;token2#1,2,3,4,5;token3#1,2,5,6;token4#2,3,5", true, hp_both, false,
                                true);

        InnerTestAdaptiveBitmap("title", "INDEX_SIZE#-1",
                                "token1#0,1,2,3,4,5;token2#1,2,3,4,6;token3#2,3,4,5;token4#3,4,6;token5#4,5", true,
                                hp_bitmap);
    }

    void TestCaseForAdaptiveBitmapWithHFDict_4()
    {
        InnerTestAdaptiveBitmap("title", "INDEX_SIZE#-1",
                                "token1#0,1,2,4,5,6;token2#1,2,3,4,5;token3#1,2,5,6;token4#2,3,5;token5#2,6", true,
                                hp_bitmap, false, true);

        InnerTestAdaptiveBitmap("title", "INDEX_SIZE#-1",
                                "token1#0,1,2,3,4,5;token2#1,2,3,4,6;token3#2,3,4,5;token4#3,4,6;token5#4,5", true,
                                hp_both);

        InnerTestAdaptiveBitmap("title", "INDEX_SIZE#-1",
                                "token1#0,1,2,4,5,6;token2#1,2,3,4,5;token3#1,2,5,6;token4#2,3,5;token5#2,6", true,
                                hp_both, false, true);
    }

    void TestCaseForNotAdaptiveBitmapWithRealTime_1()
    {
        InnerTestAdaptiveBitmap("title", "DOC_FREQUENCY#3", "token1#0,1,2,3,4,5", /*realtime=*/true, hp_both,
                                /*useTestSplit=*/true);

        InnerTestAdaptiveBitmap("title", "DOC_FREQUENCY#3", "token1#0,1,2,3,4,5", /*realtime=*/true, hp_bitmap,
                                /*useTestSplit=*/true);

        InnerTestAdaptiveBitmap("title", "PERCENT#43", "token1#0,1,2,3,4,5", /*realtime=*/true, hp_both,
                                /*useTestSplit=*/true);
    }

    void TestCaseForNotAdaptiveBitmapWithRealTime_2()
    {
        InnerTestAdaptiveBitmap("title", "PERCENT#43", "token1#0,1,2,3,4,5", /*realtime=*/true, hp_bitmap,
                                /*useTestSplit=*/true);

        InnerTestAdaptiveBitmap("title", "INDEX_SIZE#-1", "token1#0,1,2,3,4,5", /*realtime=*/true, hp_both,
                                /*useTestSplit=*/true);

        InnerTestAdaptiveBitmap("title", "INDEX_SIZE#-1", "token1#0,1,2,3,4,5", /*realtime=*/true, hp_bitmap,
                                /*useTestSplit=*/true);
    }

    // adaptiveRuleStr:
    //     DOC_FREQUENCY#10, PERCENT#20, INDEX_SIZE#-1
    // expectedBitmapStr :
    //     token1#doclist1;token2#doclist2;...    eg: token1#0,1,2,3;....
    void InnerTestAdaptiveBitmap(string indexName, string adaptiveRuleStr, string expectedBtimapStr, bool hasHfDict,
                                 HighFrequencyTermPostingType type, bool realtime = false, bool useTestSplit = false)
    {
        tearDown();
        setUp();
        if (useTestSplit) {
            std::string mergeConfigStr = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"2\"}}";
            autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
        }
        string dictName = SetHighFrequenceDictionary(indexName, hasHfDict, type, mSchema);
        SetAdaptiveHighFrequenceDictionary(indexName, adaptiveRuleStr, type, mSchema);

        // build index
        string docStr = mRawDocStr;
        partition::IndexPartitionPtr indexPartition;
        if (realtime) {
            // prepare empty partition
            QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
            partition::IndexBuilder indexBuilder(mRootDir, mOptions, mSchema, quotaControl,
                                                 BuilderBranchHinter::Option::Test());
            INDEXLIB_TEST_TRUE(indexBuilder.Init());
            indexBuilder.EndIndex();
            indexBuilder.TEST_BranchFSMoveToMainRoad();

            indexPartition.reset(new partition::OnlinePartition());
            indexPartition->Open(mRootDir, "", mSchema, mOptions);
        }
        BuildIndex(mSchema, docStr, indexPartition);
        CheckIndex(indexName, expectedBtimapStr, type, indexPartition, dictName);
    }

    void TestCaseForMergeMultiSegment()
    {
        bool hasHfDict = false;
        HighFrequencyTermPostingType type = hp_bitmap;
        string indexName = "title";
        string subIndexName = "sub_title";

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(subSchema, "sub_title:text;sub_url:string",
                                         "sub_title:text:sub_title;sub_pk:primarykey64:sub_url", "", "");
        subSchema->AddDictionaryConfig(subIndexName, VOL_CONTENT);
        mSchema->SetSubIndexPartitionSchema(subSchema);

        string dictName = SetHighFrequenceDictionary(indexName, hasHfDict, type, mSchema);
        string subDictName = SetHighFrequenceDictionary(subIndexName, hasHfDict, type, subSchema);

        SetAdaptiveHighFrequenceDictionary(indexName, "DOC_FREQUENCY#3", type, mSchema);
        SetAdaptiveHighFrequenceDictionary(subIndexName, "DOC_FREQUENCY#2", type, subSchema);

        mOptions.GetMergeConfig().mergeStrategyStr = "optimize";
        std::string mergeConfigStr = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"2\"}}";
        autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));

        string docString = "cmd=add,url=1,title=t1,sub_title=s1^s1 s2,sub_url=1^2;"
                           "cmd=add,url=2,title=t1 t2,sub_title=s1 s2 s3^s2,sub_url=3^4;"
                           "cmd=add,url=3,title=t1 t2 t3;"
                           "cmd=add,url=4,title=t1 t2 t3 t4;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "title:t1", "docid=0;docid=1;docid=2;docid=3;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "sub_title:s1", "docid=0;docid=1;docid=2;"));

        partition::IndexPartitionPtr indexPart = psm.GetIndexPartition();
        mSchema = indexPart->GetSchema();

        subSchema = mSchema->GetSubIndexPartitionSchema();

        partition::IndexPartitionReaderPtr reader = indexPart->GetReader();
        string expectedBtimapStr = "t1#0,1,2,3;t2#1,2,3;";
        CheckIndex(indexName, expectedBtimapStr, type, reader, false, dictName, mSchema);

        partition::IndexPartitionReaderPtr subReader = reader->GetSubPartitionReader();
        string subExpectedBtimapStr = "s1#0,1,2;s2#1,2,3;";
        CheckIndex(subIndexName, subExpectedBtimapStr, type, subReader, false, subDictName, subSchema);

        string docString2 = "cmd=add,url=5,title=t1,sub_title=s2,sub_url=5";
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_INC_NO_MERGE, docString2, "title:t1", "docid=0;docid=1;docid=2;docid=3;docid=4;"));

        reader = indexPart->GetReader();
        string expectedBtimapStr2 = "t1#0,1,2,3,4;t2#1,2,3;";
        CheckIndex(indexName, expectedBtimapStr2, type, reader, false, dictName, mSchema);

        subReader = reader->GetSubPartitionReader();
        string subExpectedBtimapStr2 = "s1#0,1,2;s2#1,2,3,4;";
        CheckIndex(subIndexName, subExpectedBtimapStr2, type, subReader, false, subDictName, subSchema);

        DeployIndexChecker::CheckDeployIndexMeta(GET_TEMP_DATA_PATH(), 1, INVALID_VERSIONID);
        DeployIndexChecker::CheckDeployIndexMeta(GET_TEMP_DATA_PATH(), 2, INVALID_VERSIONID);
        DeployIndexChecker::CheckDeployIndexMeta(GET_TEMP_DATA_PATH(), 2, 1);
    }

    void TestCaseForSubDoc()
    {
        bool hasHfDict = false;
        HighFrequencyTermPostingType type = hp_bitmap;
        string indexName = "title";
        string subIndexName = "sub_title";

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(subSchema, "sub_title:text;sub_url:string",
                                         "sub_title:text:sub_title;sub_pk:primarykey64:sub_url", "", "");
        subSchema->AddDictionaryConfig(subIndexName, VOL_CONTENT);

        mSchema->SetSubIndexPartitionSchema(subSchema);

        string dictName = SetHighFrequenceDictionary(indexName, hasHfDict, type, mSchema);
        string subDictName = SetHighFrequenceDictionary(subIndexName, hasHfDict, type, subSchema);

        SetAdaptiveHighFrequenceDictionary(indexName, "DOC_FREQUENCY#3", type, mSchema);
        SetAdaptiveHighFrequenceDictionary(subIndexName, "DOC_FREQUENCY#2", type, subSchema);

        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));

        string docString = "cmd=add,url=1,title=t1,sub_title=s1^s1 s2,sub_url=1^2;"
                           "cmd=add,url=2,title=t1 t2,sub_title=s1 s2 s3^s2,sub_url=3^4;"
                           "cmd=add,url=3,title=t1 t2 t3;"
                           "cmd=add,url=4,title=t1 t2 t3 t4;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "title:t1", "docid=0;docid=1;docid=2;docid=3;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "sub_title:s1", "docid=0;docid=1;docid=2;"));

        partition::IndexPartitionPtr indexPart = psm.GetIndexPartition();
        mSchema = indexPart->GetSchema();
        subSchema = mSchema->GetSubIndexPartitionSchema();

        partition::IndexPartitionReaderPtr reader = indexPart->GetReader();
        string expectedBtimapStr = "t1#0,1,2,3;t2#1,2,3;";
        CheckIndex(indexName, expectedBtimapStr, type, reader, false, dictName, mSchema);

        partition::IndexPartitionReaderPtr subReader = reader->GetSubPartitionReader();
        string subExpectedBtimapStr = "s1#0,1,2;s2#1,2,3;";
        CheckIndex(subIndexName, subExpectedBtimapStr, type, subReader, false, subDictName, subSchema);

        string docString2 = "cmd=add,url=5,title=t1,sub_title=s2,sub_url=5";
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_INC_NO_MERGE, docString2, "title:t1", "docid=0;docid=1;docid=2;docid=3;docid=4;"));

        reader = indexPart->GetReader();
        string expectedBtimapStr2 = "t1#0,1,2,3,4;t2#1,2,3;";
        CheckIndex(indexName, expectedBtimapStr2, type, reader, false, dictName, mSchema);

        subReader = reader->GetSubPartitionReader();
        string subExpectedBtimapStr2 = "s1#0,1,2;s2#1,2,3,4;";
        CheckIndex(subIndexName, subExpectedBtimapStr2, type, subReader, false, subDictName, subSchema);

        // TODO: @qingran
        // DeployIndexChecker::CheckDeployIndexMeta(GET_TEMP_DATA_PATH(), 1, INVALID_VERSIONID);
        // DeployIndexChecker::CheckDeployIndexMeta(GET_TEMP_DATA_PATH(), 2, INVALID_VERSIONID);
        // DeployIndexChecker::CheckDeployIndexMeta(GET_TEMP_DATA_PATH(), 2, 1);
    }

    void TestCaseForMultiSegmentSort()
    {
        bool hasHfDict = false;
        HighFrequencyTermPostingType type = hp_bitmap;
        string indexName = "title";
        string subIndexName = "sub_title";

        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema,
                                         // field schema
                                         "title:text;ends:uint32;userid:uint32;nid:uint32;url:string",
                                         // index schema
                                         "title:text:title;pk:primarykey64:url",
                                         // attribute schema
                                         "ends;userid;nid;url;",
                                         // summary schema
                                         "");
        schema->AddDictionaryConfig(VOL_NAME, VOL_CONTENT);
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(subSchema, "sub_title:text;sub_url:string",
                                         "sub_title:text:sub_title;sub_pk:primarykey64:sub_url", "sub_url;", "");
        subSchema->AddDictionaryConfig(subIndexName, VOL_CONTENT);
        schema->SetSubIndexPartitionSchema(subSchema);

        string dictName = SetHighFrequenceDictionary(indexName, hasHfDict, type, schema);
        string subDictName = SetHighFrequenceDictionary(subIndexName, hasHfDict, type, subSchema);

        SetAdaptiveHighFrequenceDictionary(indexName, "DOC_FREQUENCY#2", type, schema);
        SetAdaptiveHighFrequenceDictionary(subIndexName, "DOC_FREQUENCY#2", type, subSchema);

        mOptions.GetMergeConfig().mergeStrategyStr = "optimize";
        std::string mergeConfigStr = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"3\"}}";
        PartitionMeta partitionMeta;
        partitionMeta.AddSortDescription("userid", indexlibv2::config::sp_asc);
        partitionMeta.Store(GET_PARTITION_DIRECTORY());

        autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, GET_TEMP_DATA_PATH()));

        string docString = "cmd=add,url=1,title=t1,sub_title=s1^s1 s2,sub_url=1^2,userid=0;"
                           "cmd=add,url=2,title=t1 t2,sub_title=s1 s2 s3^s2,sub_url=3^4,userid=1;"
                           "cmd=add,url=3,title=t1 t2 t3,userid=2;"
                           "cmd=add,url=3,title=t1 t2 t3 t4,userid=3;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "title:t1", "docid=0;docid=1;docid=2;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "sub_title:s1", "docid=0;docid=1;docid=2;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "title:t2", "docid=1;docid=2;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "title:t3", "docid=1;"));

        partition::IndexPartitionPtr indexPart = psm.GetIndexPartition();
        schema = indexPart->GetSchema();

        subSchema = schema->GetSubIndexPartitionSchema();

        partition::IndexPartitionReaderPtr reader = indexPart->GetReader();
        string expectedBtimapStr = "t1#0,1,2;t2#1,2;";
        CheckIndex(indexName, expectedBtimapStr, type, reader, false, dictName, schema);

        partition::IndexPartitionReaderPtr subReader = reader->GetSubPartitionReader();
        string subExpectedBtimapStr = "s1#0,1,2;s2#1,2,3;";
        CheckIndex(subIndexName, subExpectedBtimapStr, type, subReader, false, subDictName, subSchema);
    }

private:
    void ResetIndexPartitionSchema()
    {
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mSchema,
                                         // field schema
                                         "title:text;ends:uint32;userid:uint32;nid:uint32;url:string",
                                         // index schema
                                         "title:text:title;pk:primarykey64:url",
                                         // attribute schema
                                         "ends;userid;nid",
                                         // summary schema
                                         "");
        mSchema->AddDictionaryConfig(VOL_NAME, VOL_CONTENT);
    }

    string SetHighFrequenceDictionary(const string& indexName, bool hasHighFreq, HighFrequencyTermPostingType type,
                                      const IndexPartitionSchemaPtr& schema)
    {
        IndexSchemaPtr indexSchema = schema->GetIndexSchema();
        assert(indexSchema != NULL);
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
        assert(indexConfig != NULL);
        DictionarySchemaPtr dictSchema = schema->GetDictSchema();
        assert(dictSchema != NULL);

        string adaptiveDictName = "";
        if (hasHighFreq) {
            std::shared_ptr<DictionaryConfig> dictConfig = dictSchema->GetDictionaryConfig(VOL_NAME);
            assert(dictConfig != NULL);
            indexConfig->SetDictConfig(dictConfig);
            indexConfig->SetHighFreqencyTermPostingType(type);
            adaptiveDictName = indexName + "_" + VOL_NAME;
        } else {
            indexConfig->SetDictConfig(std::shared_ptr<DictionaryConfig>());
            adaptiveDictName = indexName;
        }
        return adaptiveDictName;
    }

    // adaptiveRuleStr:
    // DOC_FREQUENCY#10, PERCENT#20, INDEX_SIZE#-1
    void SetAdaptiveHighFrequenceDictionary(const string& indexName, const string& adaptiveRuleStr,
                                            HighFrequencyTermPostingType type, const IndexPartitionSchemaPtr& schema)
    {
        IndexSchemaPtr indexSchema = schema->GetIndexSchema();
        assert(indexSchema != NULL);
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
        assert(indexConfig != NULL);

        vector<string> adaptiveParams = StringUtil::split(adaptiveRuleStr, "#");
        assert(adaptiveParams.size() == 2);

        std::shared_ptr<AdaptiveDictionaryConfig> dictConfig(new AdaptiveDictionaryConfig(
            "adaptive_rule", adaptiveParams[0], StringUtil::fromString<int32_t>(adaptiveParams[1])));
        AdaptiveDictionarySchemaPtr adaptiveDictSchema(new AdaptiveDictionarySchema);
        adaptiveDictSchema->AddAdaptiveDictionaryConfig(dictConfig);
        schema->SetAdaptiveDictSchema(adaptiveDictSchema);
        indexConfig->SetAdaptiveDictConfig(dictConfig);
        indexConfig->SetHighFreqencyTermPostingType(type);
    }

    void BuildIndex(const IndexPartitionSchemaPtr& schema, const string& rawDocStr,
                    const partition::IndexPartitionPtr& partition)
    {
        DocumentMaker::DocumentVect docVect;
        MockIndexPart mockIndexPart;
        DocumentMaker::MakeDocuments(rawDocStr, schema, docVect, mockIndexPart);

        // build index
        partition::IndexBuilderPtr builder;
        QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));

        if (partition) {
            builder.reset(new IndexBuilder(partition, quotaControl));
        } else {
            builder.reset(
                new IndexBuilder(mRootDir, mOptions, schema, quotaControl, BuilderBranchHinter::Option::Test()));
        }
        INDEXLIB_TEST_TRUE(builder->Init());

        for (size_t i = 0; i < docVect.size(); ++i) {
            builder->Build(docVect[i]);
        }
        builder->EndIndex();

        if (partition == nullptr) {
            builder->TEST_BranchFSMoveToMainRoad();
            builder->Merge(mOptions, true);
        }
    }

    // expectedBitmapStr :
    // token1#doclist1;token2#doclist2;...              token1#0,1,2,3;....
    void CheckIndex(const string& indexName, const string& expectedBitmapStr, HighFrequencyTermPostingType type,
                    const partition::IndexPartitionPtr& partition, string dictName)
    {
        partition::IndexPartitionPtr indexPart = partition;
        if (!indexPart) {
            indexPart.reset(new partition::OnlinePartition);
            indexPart->Open(mRootDir, "", IndexPartitionSchemaPtr(), mOptions);
        }

        partition::IndexPartitionReaderPtr indexPartReader = indexPart->GetReader();

        mSchema = indexPart->GetSchema();

        CheckIndex(indexName, expectedBitmapStr, type, indexPartReader, partition != NULL, dictName, mSchema);
    }

    void CheckIndex(const string& indexName, const string& expectedBitmapStr, HighFrequencyTermPostingType type,
                    const partition::IndexPartitionReaderPtr& indexPartReader, bool realtime, string dictName,
                    const IndexPartitionSchemaPtr& schema)
    {
        std::shared_ptr<InvertedIndexReader> indexReader = indexPartReader->GetInvertedIndexReader(indexName);

        INDEXLIB_TEST_TRUE(indexReader.get() != NULL);

        vector<string> termInfos;
        StringUtil::fromString(expectedBitmapStr, termInfos, ";");
        for (size_t i = 0; i < termInfos.size(); ++i) {
            CheckTerm(indexName, termInfos[i], indexPartReader, type);
        }
        CheckDictionary(indexName, termInfos.size(), realtime, dictName, schema);
    }

    void CheckTerm(const string& indexName, const string& termStr,
                   const partition::IndexPartitionReaderPtr& indexPartReader, HighFrequencyTermPostingType type)
    {
        std::shared_ptr<InvertedIndexReader> indexReader = indexPartReader->GetInvertedIndexReader(indexName);
        vector<string> termData;
        StringUtil::fromString(termStr, termData, "#");
        assert(termData.size() == 2);

        vector<docid_t> docLists;
        StringUtil::fromString(termData[1], docLists, ",");

        Term term(termData[0], "");
        PostingIterator* iter = indexReader->Lookup(term, 1000, pt_bitmap, NULL).ValueOrThrow();
        INDEXLIB_TEST_TRUE(iter != NULL);
        docid_t docId = INVALID_DOCID;
        for (size_t i = 0; i < docLists.size(); ++i) {
            docId = iter->SeekDoc(docId);
            INDEXLIB_TEST_EQUAL(docLists[i], docId) << termStr;
        }
        docId = iter->SeekDoc(docId);
        DeletionMapReaderPtr deletionMapReader = indexPartReader->GetDeletionMapReader();
        if (docId != INVALID_DOCID) {
            ASSERT_TRUE(deletionMapReader && deletionMapReader->IsDeleted(docId));
        }
        delete iter;

        if (type == hp_bitmap) {
            PostingIterator* iter = indexReader->Lookup(term, 1000, pt_normal, NULL).ValueOrThrow();
            INDEXLIB_TEST_TRUE(iter == NULL);
        } else // hp_both
        {
            PostingIterator* iter = indexReader->Lookup(term, 1000, pt_normal, NULL).ValueOrThrow();
            INDEXLIB_TEST_TRUE(iter != NULL);
            INDEXLIB_TEST_TRUE(iter->GetType() != pi_bitmap);
            docid_t docId = INVALID_DOCID;
            for (size_t i = 0; i < docLists.size(); ++i) {
                docId = iter->SeekDoc(docId);
                INDEXLIB_TEST_EQUAL(docLists[i], docId);
            }
            docId = iter->SeekDoc(docId);
            if (docId != INVALID_DOCID) {
                ASSERT_TRUE(deletionMapReader && deletionMapReader->IsDeleted(docId));
            }
            delete iter;
        }
    }

    void CheckDictionary(const string& indexName, size_t bitmapTermCount, bool realtime, string dictName,
                         const IndexPartitionSchemaPtr& schema)
    {
        // check isAdaptiveDict & dictname, term count
        IndexSchemaPtr indexSchema = schema->GetIndexSchema();
        INDEXLIB_TEST_TRUE(indexSchema.get() != NULL);

        IndexConfigPtr indexConf = indexSchema->GetIndexConfig(indexName);
        INDEXLIB_TEST_TRUE(indexConf.get() != NULL);
        std::shared_ptr<HighFrequencyVocabulary> vol = indexConf->GetHighFreqVocabulary();
        if (!realtime) {
            INDEXLIB_TEST_TRUE(vol.get() != NULL);
            INDEXLIB_TEST_EQUAL(dictName, vol->GetVocabularyName());
        }
        INDEXLIB_TEST_EQUAL(bitmapTermCount, vol->GetTermCount());
        // check adaptive dict file exist & term count
        string rootPath = mRootDir;
        if (realtime) {
            rootPath = util::PathUtil::JoinPath(rootPath, "rt_index_partition");
            string adaptivePath = util::PathUtil::JoinPath(rootPath, ADAPTIVE_DICT_DIR_NAME);
            INDEXLIB_TEST_TRUE(!FslibWrapper::IsExist(adaptivePath).GetOrThrow());
        } else {
            ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountDir(GET_TEMP_DATA_PATH(), ADAPTIVE_DICT_DIR_NAME,
                                                           ADAPTIVE_DICT_DIR_NAME, FSMT_READ_ONLY, true));
            ArchiveFolderPtr archiveFolder(new ArchiveFolder(false));
            ASSERT_EQ(FSEC_OK,
                      archiveFolder->Open(
                          GET_PARTITION_DIRECTORY()->GetDirectory(ADAPTIVE_DICT_DIR_NAME, true)->GetIDirectory()));
            auto dictFile = archiveFolder->CreateFileReader(dictName).GetOrThrow();

            StringTokenizer tokens(dictFile->TEST_Load(), "\n",
                                   StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
            INDEXLIB_TEST_EQUAL(bitmapTermCount, tokens.getNumTokens());
            ASSERT_EQ(FSEC_OK, dictFile->Close());
            ASSERT_EQ(FSEC_OK, archiveFolder->Close());
        }
    }

private:
    string mRootDir;
    string mRawDocStr;
    IndexPartitionSchemaPtr mSchema;
    IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, AdaptiveBitmapInteTest);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AdaptiveBitmapInteTest, TestCaseForAdaptiveBitmapWithoutHFDict_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AdaptiveBitmapInteTest, TestCaseForAdaptiveBitmapWithoutHFDict_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AdaptiveBitmapInteTest, TestCaseForAdaptiveBitmapWithoutHFDict_3);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AdaptiveBitmapInteTest, TestCaseForAdaptiveBitmapWithoutHFDict_4);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AdaptiveBitmapInteTest, TestCaseForAdaptiveBitmapWithHFDict_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AdaptiveBitmapInteTest, TestCaseForAdaptiveBitmapWithHFDict_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AdaptiveBitmapInteTest, TestCaseForAdaptiveBitmapWithHFDict_3);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AdaptiveBitmapInteTest, TestCaseForAdaptiveBitmapWithHFDict_4);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AdaptiveBitmapInteTest, TestCaseForNotAdaptiveBitmapWithRealTime_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AdaptiveBitmapInteTest, TestCaseForNotAdaptiveBitmapWithRealTime_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AdaptiveBitmapInteTest, TestCaseForSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AdaptiveBitmapInteTest, TestCaseForMergeMultiSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AdaptiveBitmapInteTest, TestCaseForMultiSegmentSort);

INSTANTIATE_TEST_CASE_P(BuildMode, AdaptiveBitmapInteTest, testing::Values(0, 1, 2));
}}} // namespace indexlib::index::legacy
