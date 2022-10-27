#include "indexlib/index/normal/inverted_index/truncate/test/truncate_index_intetest.h"
#include <autil/StringUtil.h>
#include <autil/HashAlgorithm.h>
#include "indexlib/common/term.h"
#include "indexlib/storage/line_reader.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/document_checker_for_gtest.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_creator.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/config/truncate_index_name_mapper.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/util/path_util.h"
#include "indexlib/index/test/deploy_index_checker.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/inverted_index/accessor/term_match_data.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/storage/archive_folder.h"
#include "indexlib/storage/archive_file.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TruncateIndexTest);

#define VOL_NAME "title_vol" 
#define VOL_CONTENT "token1"
typedef DocumentMaker::MockIndexPart MockIndexPart;

TruncateIndexTest::TruncateIndexTest()
{
}

TruncateIndexTest::~TruncateIndexTest()
{
}

void TruncateIndexTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mOptions = IndexPartitionOptions();
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    mOptions.GetMergeConfig().mergeThreadCount = 1; 
    mRawDocStr = "{ 1, "                                            \
                 "[title: (token1)],"                               \
                 "[ends: (1)],"                                     \
                 "[userid: (1)],"                                   \
                 "[nid: (1)],"                                      \
                 "[url: (url1)],"                                   \
                 "};"                                               \
                 "{ 2, "                                            \
                 "[title: (token1 token2)],"                        \
                 "[ends: (2)],"                                     \
                 "[userid: (2)],"                                   \
                 "[url: (url2)],"                                   \
                 "[nid: (2)],"                                      \
                 "};"                                               \
                 "{ 3, "                                            \
                 "[title: (token1 token2 token3)],"                 \
                 "[ends: (3)],"                                     \
                 "[userid: (3)],"                                   \
                 "[nid: (3)],"                                      \
                 "[url: (url3)],"                                   \
                 "};"                                               \
                 "{ 4, "                                            \
                 "[title: (token1 token2 token3 token4)],"          \
                 "[ends: (4)],"                                     \
                 "[userid: (4)],"                                   \
                 "[nid: (4)],"                                      \
                 "[url: (url4)],"                                   \
                 "};"                                               \
                 "{ 5, "                                            \
                 "[title: (token1 token2 token3 token4 token5)],"   \
                 "[ends: (5)],"                                     \
                 "[userid: (5)],"                                   \
                 "[nid: (6)],"                                      \
                 "[url: (url5)],"                                   \
                 "};"                                               \
                 "{ 5, "                                            \
                 "[title: (token1 token3 token5)],"                 \
                 "[ends: (5)],"                                     \
                 "[userid: (5)],"                                   \
                 "[nid: (5)],"                                      \
                 "[url: (url6)],"                                   \
                 "};"                                               \
                 "{ 4, "                                            \
                 "[title: (token2 token4)],"                        \
                 "[ends: (4)],"                                     \
                 "[userid: (4)],"                                   \
                 "[nid: (7)],"                                      \
                 "[url: (url7)],"                                   \
                 "};";
    mQuotaControl.reset(new util::QuotaControl(1024 * 1024 * 1024));
}

void TruncateIndexTest::CaseTearDown()
{
}

void TruncateIndexTest::TestCaseForNoTruncateByAttribute()
{
    string expectedDocStr = mRawDocStr;
    TruncateParams param("", "", "title=ends:ends#DESC:::");
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "0;1;2;3;4;5;6", 
                         "token1;token2;token3;token4;token5",
                         param, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "0;1;2;3;4;5;6", 
                         "token1;token2;token3;token4;token5",
                         param, "title_ends", true);

    expectedDocStr = "{ 2, "                                        \
                     "[title: (token1 token2)],"                    \
                     "[ends: (2)],"                                 \
                     "[userid: (2)],"                               \
                     "[nid: (2)],"                                  \
                     "};"                                           \
                     "{ 3, "                                        \
                     "[title: (token1 token2 token3)],"             \
                     "[ends: (3)],"                                 \
                     "[userid: (3)],"                               \
                     "[nid: (3)],"                                  \
                     "};"                                           \
                     "{ 4, "                                        \
                     "[title: (token1 token2 token3 token4)],"      \
                     "[ends: (4)],"                                 \
                     "[userid: (4)],"                               \
                     "[nid: (4)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token2 token3 token4 token5)]," \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (6)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token3 token5)],"             \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (5)],"                                  \
                     "};"                                           \
                     "{ 4, "                                        \
                     "[title: (token2 token4)],"                    \
                     "[ends: (4)],"                                 \
                     "[userid: (4)],"                               \
                     "[nid: (7)],"                                  \
                     "};";

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, 
                               "1;2;3;4;5;6", "token2;token3;token4;token5",
                               param, "title_ends", "0;1;2;3;4;5");
}

void TruncateIndexTest::TestCaseForNoTruncateByDocPayload()
{
    string expectedDocStr = mRawDocStr;
    TruncateParams param("", "", "title=doc_payload:DOC_PAYLOAD#DESC:DOC_PAYLOAD:1:5");
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "0;1;2;3;4;5;6", 
                         "token1;token2;token3;token4;token5", param, 
                         "title_doc_payload", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "0;1;2;3;4;5;6", 
                         "token1;token2;token3;token4;token5", param, 
                         "title_doc_payload", true);

    expectedDocStr = "{ 2, "                                        \
                     "[title: (token1 token2)],"                    \
                     "[ends: (2)],"                                 \
                     "[userid: (2)],"                               \
                     "[nid: (2)],"                                  \
                     "};"                                           \
                     "{ 3, "                                        \
                     "[title: (token1 token2 token3)],"             \
                     "[ends: (3)],"                                 \
                     "[userid: (3)],"                               \
                     "[nid: (3)],"                                  \
                     "};"                                           \
                     "{ 4, "                                        \
                     "[title: (token1 token2 token3 token4)],"      \
                     "[ends: (4)],"                                 \
                     "[userid: (4)],"                               \
                     "[nid: (4)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token2 token3 token4 token5)]," \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (6)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token3 token5)],"             \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (5)],"                                  \
                     "};"                                           \
                     "{ 4, "                                        \
                     "[title: (token2 token4)],"                    \
                     "[ends: (4)],"                                 \
                     "[userid: (4)],"                               \
                     "[nid: (7)],"                                  \
                     "};";
        
    // doc_payload for token1 is 0, not in 1~5, not found
    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "1;2;3;4;5;6", 
                               "token2;token3;token4;token5",
                               param, "title_doc_payload", "");
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithoutDistinct()
{
    // no truncate index generated
    TruncateParams param1("6:2", "", "title=ends:ends#DESC:::");
    string expectedDocStr = "";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "", "", param1,
                         "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "", "", param1, 
                         "title_ends", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "", "",
                               param1, "title_ends", "");

    // df >= 4 --> df = 2
    TruncateParams param2("3:2", "", "title=ends:ends#DESC:::");
    expectedDocStr = "{ 4, "                                        \
                     "[title: (unused_token token2)],"              \
                     "[ends: (4)],"                                 \
                     "[userid: (4)],"                               \
                     "[nid: (4)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token2 token3 )],"            \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (6)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token3)],"                    \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (5)],"                                  \
                     "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "3;4;5",
                         "token1;token2;token3", param2, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "3;4;5",
                         "token1;token2;token3", param2, "title_ends", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "3;4;5",
                               "token2;token3",
                               param2, "title_ends", "4;5");
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithoutDistinctWithTestSplit()
{
    // no truncate index generated
    TruncateParams param1("6:2", "", "title=ends:ends#DESC:::");
    string expectedDocStr = "";
    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "", "", param1,
                         "title_ends", false);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "", "", param1, 
                         "title_ends", true);

    InnerTestForBitmapTruncateWithTestSplit(mRawDocStr, expectedDocStr, "", "",
                               param1, "title_ends", "");

    // df >= 4 --> df = 2
    TruncateParams param2("3:2", "", "title=ends:ends#DESC:::");
    expectedDocStr = "{ 4, "                                        \
                     "[title: (unused_token token2)],"              \
                     "[ends: (4)],"                                 \
                     "[userid: (4)],"                               \
                     "[nid: (4)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token2 token3 )],"            \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (6)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token3)],"                    \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (5)],"                                  \
                     "};";
    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6",
                         "token1;token2;token3", param2, "title_ends", false);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6",
                         "token1;token2;token3", param2, "title_ends", true);

    InnerTestForBitmapTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6",
                               "token2;token3",
                               param2, "title_ends", "4;6");
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithoutDistinctWithTestSplitWithEmpty()
{
    // no truncate index generated
    TruncateParams param1("6:2", "", "title=ends:ends#DESC:::");
    string expectedDocStr = "";
    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "", "", param1,
            "title_ends", false, 5);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "", "", param1, 
            "title_ends", true, 5);

    InnerTestForBitmapTruncateWithTestSplit(mRawDocStr, expectedDocStr, "", "",
            param1, "title_ends", "", 5);

    TruncateParams param2("3:2", "", "title=ends:ends#DESC:::");
    expectedDocStr = 
        "{ 5, "                                            \
        "[title: (token1 token3 token5)],"                 \
        "[ends: (5)],"                                     \
        "[userid: (5)],"                                   \
        "[nid: (5)],"                                      \
        "[url: (url6)],"                                   \
        "};"                                               \
        "{ 4, "                                            \
        "[title: (token2 token4)],"                        \
        "[ends: (4)],"                                     \
        "[userid: (4)],"                                   \
        "[nid: (7)],"                                      \
        "[url: (url7)],"                                   \
        "};"                                               \
        "{ 5, "                                            \
        "[title: (token1 token2 token3 token4 token5)],"   \
        "[ends: (5)],"                                     \
        "[userid: (5)],"                                   \
        "[nid: (6)],"                                      \
        "[url: (url5)],"                                   \
        "};";


    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;3;6",
            "token1;token2;token3", param2, "title_ends", false, 5);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;3;6",
            "token1;token2;token3", param2, "title_ends", true, 5);

    InnerTestForBitmapTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;3;6",
            "token2;token3",
            param2, "title_ends", "1;6", 5);
}


void TruncateIndexTest::TestCaseForTruncateByMultiAttribute()
{
    // df >= 4 --> df = 1, multi level sort
    TruncateParams param("3:1", "", "title=ends:ends#DESC|nid#ASC:::");
    string expectedDocStr = "{ 5, "                                        \
                            "[title: (token1 token3)],"                    \
                            "[ends: (5)],"                                 \
                            "[userid: (5)],"                               \
                            "[nid: (5)],"                                  \
                            "};";

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "5", "token1;token3",
                         param, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "5", "token1;token3",
                         param, "title_ends", true);

    TruncateParams param1("3:1", "", "title=ends:ends#DESC|nid#DESC:::");

    expectedDocStr = "{ 5, "                                        \
                     "[title: (token1 unused_token token3 )],"      \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (6)],"                                  \
                     "};";

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "4", "token1;token3",
                         param1, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "4", "token1;token3",
                         param1, "title_ends", true);

}

void TruncateIndexTest::TestCaseForTruncateByMultiAttributeWithTestSplit()
{
    // df >= 4 --> df = 1, multi level sort
    TruncateParams param("3:1", "", "title=ends:ends#DESC|nid#ASC:::");
    string expectedDocStr = "{ 5, "                                        \
                            "[title: (token1 token3)],"                    \
                            "[ends: (5)],"                                 \
                            "[userid: (5)],"                               \
                            "[nid: (5)],"                                  \
                            "};";

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "6", "token1;token3",
                         param, "title_ends", false);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "6", "token1;token3",
                         param, "title_ends", true);

    TruncateParams param1("3:1", "", "title=ends:ends#DESC|nid#DESC:::");

    expectedDocStr = "{ 5, "                                        \
                     "[title: (token1 unused_token token3 )],"      \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (6)],"                                  \
                     "};";

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "4", "token1;token3",
                         param1, "title_ends", false);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "4", "token1;token3",
                         param1, "title_ends", true);

}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithDistinctNoExpand()
{
    TruncateParams param1("3:2", "userid:1:3", "title=ends:ends#DESC:::");
    string expectedDocStr;
    expectedDocStr = "{ 4, "                                        \
                     "[title: (unused_token token2)],"              \
                     "[ends: (4)],"                                 \
                     "[userid: (4)],"                               \
                     "[nid: (4)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token2 token3 )],"            \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (6)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token3)],"                    \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (5)],"                                  \
                     "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "3;4;5",
                         "token1;token2;token3",
                         param1, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "3;4;5",
                         "token1;token2;token3",
                         param1, "title_ends", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "3;4;5",
                               "token2;token3",
                               param1, "title_ends", "4;5");
        
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithDistinctNoExpandWithTestSplit()
{
    TruncateParams param1("3:2", "userid:1:3", "title=ends:ends#DESC:::");
    string expectedDocStr;
    expectedDocStr = "{ 4, "                                        \
                     "[title: (unused_token token2)],"              \
                     "[ends: (4)],"                                 \
                     "[userid: (4)],"                               \
                     "[nid: (4)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token2 token3 )],"            \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (6)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token3)],"                    \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (5)],"                                  \
                     "};";
    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6",
                         "token1;token2;token3",
                         param1, "title_ends", false);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6",
                         "token1;token2;token3",
                         param1, "title_ends", true);

    InnerTestForBitmapTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6",
                               "token2;token3",
                               param1, "title_ends", "4;6");
        
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithDistinctExpand()
{
    TruncateParams param("3:2", "userid:2:4", "title=ends:ends#DESC:::");
    string expectedDocStr;
    expectedDocStr = "{ 4, "                                        \
                     "[title: (token1 token2 token3)],"       \
                     "[ends: (4)],"                                 \
                     "[userid: (4)],"                               \
                     "[nid: (4)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token2 token3)],"             \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (6)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token3)],"                    \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (5)],"                                  \
                     "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "3;4;5",
                         "token1;token2;token3",
                         param, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "3;4;5",
                         "token1;token2;token3",
                         param, "title_ends", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "3;4;5",
                               "token2;token3",
                               param, "title_ends", "3;4;5");
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithDistinctExpandWithTestSplit()
{
    TruncateParams param("3:2", "userid:2:4", "title=ends:ends#DESC:::");
    string expectedDocStr;
    expectedDocStr = "{ 4, "                                        \
                     "[title: (token1 token2 token3)],"       \
                     "[ends: (4)],"                                 \
                     "[userid: (4)],"                               \
                     "[nid: (4)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token2 token3)],"             \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (6)],"                                  \
                     "};"                                           \
                     "{ 5, "                                        \
                     "[title: (token1 token3)],"                    \
                     "[ends: (5)],"                                 \
                     "[userid: (5)],"                               \
                     "[nid: (5)],"                                  \
                     "};";
    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6",
                         "token1;token2;token3",
                         param, "title_ends", false);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6",
                         "token1;token2;token3",
                         param, "title_ends", true);

    InnerTestForBitmapTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6",
                               "token2;token3",
                               param, "title_ends", "1;4;6");
}

void TruncateIndexTest::TestCaseForDocPayloadWithoutDistinctWithDefautLimit()
{
    TruncateParams param("", "", "title=doc_payload::DOC_PAYLOAD:3:5");
    string expectedDocStr;
    expectedDocStr = 
        "{ 3, "                                                     \
        "[title: (token1 token2 token3)],"                          \
        "[ends: (3)],"                                              \
        "[userid: (3)],"                                            \
        "[nid: (3)],"                                               \
        "};"                                                        \
        "{ 4, "                                                     \
        "[title: (token1 token2 token3 token4)],"                   \
        "[ends: (4)],"                                              \
        "[userid: (4)],"                                            \
        "[nid: (4)],"                                               \
        "};"                                                        \
        "{ 5, "                                                     \
        "[title: (token1 token2 token3 token4 token5)],"            \
        "[ends: (5)],"                                              \
        "[userid: (5)],"                                            \
        "[nid: (6)],"                                               \
        "};"                                                        \
        "{ 5, "                                                     \
        "[title: (token1 token3 token5)],"                          \
        "[ends: (5)],"                                              \
        "[userid: (5)],"                                            \
        "[nid: (5)],"                                               \
        "};"                                                        \
        "{ 4, "                                                     \
        "[title: (token2 token4)],"                                 \
        "[ends: (4)],"                                              \
        "[userid: (4)],"                                            \
        "[nid: (7)],"                                               \
        "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "2;3;4;5;6", 
                         "token1;token2;token3;token4;token5", param, 
                         "title_doc_payload", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "2;3;4;5;6", 
                         "token1;token2;token3;token4;token5", param, 
                         "title_doc_payload", true);

    // bitmap doc payload for token1 is 0
    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "2;3;4;5;6", 
                               "token2;token3;token4;token5",
                               param, "title_doc_payload", "");
}

void TruncateIndexTest::TestCaseForDocPayloadWithoutDistinct()
{
    TruncateParams param("3:2", "", "title=doc_payload::DOC_PAYLOAD:3:5");
    string expectedDocStr = 
        "{ 3, "                                                     \
        "[title: (token1 token2 token3)],"                          \
        "[ends: (3)],"                                              \
        "[userid: (3)],"                                            \
        "[nid: (3)],"                                               \
        "};"                                                        \
        "{ 4, "                                                     \
        "[title: (token1 token2 token3)],"                          \
        "[ends: (4)],"                                              \
        "[userid: (4)],"                                            \
        "[nid: (4)],"                                               \
        "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "2;3", 
                         "token1;token2;token3", param, 
                         "title_doc_payload", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "2;3", 
                         "token1;token2;token3", param, 
                         "title_doc_payload", true);

    // bitmap doc payload for token1 is 0
    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "2;3", 
                               "token2;token3",
                               param, "title_doc_payload", "");
}

void TruncateIndexTest::TestCaseForDocPayloadWithDistinctNoExpand()
{
    TruncateParams param("3:2", "userid:2:4", 
                         "title=doc_payload::DOC_PAYLOAD:1:5");
    string expectedDocStr =
        "{ 1, "                                                     \
        "[title: (token1)],"                                        \
        "[ends: (1)],"                                              \
        "[userid: (1)],"                                            \
        "[nid: (1)],"                                               \
        "};"                                                        \
        "{ 2, "                                                     \
        "[title: (token1 token2)],"                                 \
        "[ends: (2)],"                                              \
        "[userid: (2)],"                                            \
        "[nid: (2)],"                                               \
        "};"                                                        \
        "{ 3, "                                                     \
        "[title: (unused_token token2 token3)],"                    \
        "[ends: (3)],"                                              \
        "[userid: (3)],"                                            \
        "[nid: (3)],"                                               \
        "};"                                                        \
        "{ 4, "                                                     \
        "[title: (unused_token unused_token token3)],"              \
        "[ends: (4)],"                                              \
        "[userid: (4)],"                                            \
        "[nid: (4)],"                                               \
        "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "0;1;2;3", 
                         "token1;token2;token3", param, 
                         "title_doc_payload", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "0;1;2;3", 
                         "token1;token2;token3", param, 
                         "title_doc_payload", true);

    expectedDocStr =
        "{ 2, "                                                     \
        "[title: (token1 token2)],"                                 \
        "[ends: (2)],"                                              \
        "[userid: (2)],"                                            \
        "[nid: (2)],"                                               \
        "};"                                                        \
        "{ 3, "                                                     \
        "[title: (unused_token token2 token3)],"                    \
        "[ends: (3)],"                                              \
        "[userid: (3)],"                                            \
        "[nid: (3)],"                                               \
        "};"                                                        \
        "{ 4, "                                                     \
        "[title: (unused_token unused_token token3)],"              \
        "[ends: (4)],"                                              \
        "[userid: (4)],"                                            \
        "[nid: (4)],"                                               \
        "};";

    // bitmap doc payload for token1 is 0
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "1;2;3", 
                         "token2;token3", param, "title_doc_payload", "");
}
    
void TruncateIndexTest::TestCaseForDocPayloadWithDistinctExpand()
{
    TruncateParams param("3:2", "userid:2:2", 
                         "title=doc_payload::DOC_PAYLOAD:5:5");
    string expectedDocStr =
        "{ 5, "                                                     \
        "[title: (token1 token2 token3)],"                          \
        "[ends: (5)],"                                              \
        "[userid: (5)],"                                            \
        "[nid: (6)],"                                               \
        "};"                                                        \
        "{ 5, "                                                     \
        "[title: (token1 token3)],"                                 \
        "[ends: (5)],"                                              \
        "[userid: (5)],"                                            \
        "[nid: (5)],"                                               \
        "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "4;5", 
                         "token1;token2;token3", param, 
                         "title_doc_payload", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "4;5", 
                         "token1;token2;token3", param, 
                         "title_doc_payload", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "4;5", 
                               "token2;token3",
                               param, "title_doc_payload", "");
}

void TruncateIndexTest::TestCaseForDocPayloadWithNoDocLeftForSomeTerm()
{
    string rawDocStr = "{ 1, "                                      \
                 "[url: (pk1)],"                                    \
                 "[title: (token1)],"                               \
                 "[ends: (1)],"                                     \
                 "[userid: (1)],"                                   \
                 "[nid: (1)],"                                      \
                 "};"                                               \
                 "{ 2, "                                            \
                 "[url: (pk2)],"                                    \
                 "[title: (token2)],"                               \
                 "[ends: (2)],"                                     \
                 "[userid: (2)],"                                   \
                 "[nid: (2)],"                                      \
                 "};"                                               \
                 "{ 3, "                                            \
                 "[url: (pk3)],"                                    \
                 "[title: (token2 token3)],"                        \
                 "[ends: (3)],"                                     \
                 "[userid: (3)],"                                   \
                 "[nid: (3)],"                                      \
                 "};";
    TruncateParams param("", "", "title=doc_payload::DOC_PAYLOAD:2:3");
    string expectedDocStr = 
        "{ 2, "                                                     \
        "[url: (pk2)],"                                             \
        "[title: (token2)],"                                        \
        "[ends: (2)],"                                              \
        "[userid: (2)],"                                            \
        "[nid: (2)],"                                               \
        "};"                                                        \
        "{ 3, "                                                     \
        "[url: (pk3)],"                                             \
        "[title: (token2 token3)],"                                 \
        "[ends: (3)],"                                              \
        "[userid: (3)],"                                            \
        "[nid: (3)],"                                               \
        "};";
    InnerTestForTruncate(rawDocStr, expectedDocStr, "1;2", 
                         "token2;token3", param, 
                         "title_doc_payload", false);

    // lookup for token1 should return null
    partition::IndexPartitionReaderPtr indexPartReader = CreatePartitionReader();
    IndexReaderPtr indexReader = indexPartReader->GetIndexReader();
    Term term("token1", "title_doc_payload");
    PostingIteratorPtr postingIter(indexReader->Lookup(term));
    INDEXLIB_TEST_TRUE(!postingIter);
}

void TruncateIndexTest::TestCaseForTruncateWithOnlyBitmap()
{
    TruncateParams param("", "", "title=ends:ends#DESC:ends::","truncate_meta");
    ResetIndexPartitionSchema(param);
    SetHighFrequenceDictionary("title", true, hp_bitmap);

    mOptions.GetMergeConfig().truncateOptionConfig = 
        TruncateConfigMaker::MakeConfig(param);

    CreateTruncateMeta("title_ends", "token1;token2", "4;3", "title", false);
        
    string docStr = "{ 11, "                                        \
                    "[title: (token1)],"                            \
                    "[ends: (1)],"                                  \
                    "[userid: (1)],"                                \
                    "[nid: (1)],"                                   \
                    "[url: (url1)],"                                \
                    "};";
        
    // build truncate index
    BuildTruncateIndex(mSchema, docStr);

    {
        //merge
        file_system::DirectoryPtr segDirectory = 
            GET_PARTITION_DIRECTORY()->GetDirectory("segment_1_level_0", true);
        segDirectory->MountPackageFile(PACKAGE_FILE_PREFIX);

        file_system::DirectoryPtr indexDirectory = 
            segDirectory->GetDirectory("index/title", true);

        DictionaryReaderPtr dictReader(DictionaryCreator::CreateDiskReader(
                        mSchema->GetIndexSchema()->GetIndexConfig("title")));
        dictReader->Open(indexDirectory, DICTIONARY_FILE_NAME);

        DictionaryIteratorPtr iter = dictReader->CreateIterator();
        INDEXLIB_TEST_TRUE(!iter->HasNext());
    }

    {
        //build
        file_system::DirectoryPtr segDirectory = 
            GET_PARTITION_DIRECTORY()->GetDirectory("segment_0_level_0", true);
        segDirectory->MountPackageFile(PACKAGE_FILE_PREFIX);

        file_system::DirectoryPtr indexDirectory = 
            segDirectory->GetDirectory("index/title", true);

        DictionaryReaderPtr dictReader(DictionaryCreator::CreateDiskReader(
                        mSchema->GetIndexSchema()->GetIndexConfig("title")));
        dictReader->Open(indexDirectory, DICTIONARY_FILE_NAME);
    }
}

void TruncateIndexTest::TestCaseForReadWithOnlyBitmap()
{
    //normal posting can not be readed
    TruncateParams param("", "", "title=ends:ends#DESC:ends::","truncate_meta");
    ResetIndexPartitionSchema(param);
    SetHighFrequenceDictionary("title", true, hp_bitmap);

    mOptions.GetMergeConfig().truncateOptionConfig = 
        TruncateConfigMaker::MakeConfig(param);

    CreateTruncateMeta("title_ends", "token1;token2", "4;3", "title");

    string docStr = "{ 11, "                                        \
                    "[title: (token1)],"                            \
                    "[ends: (1)],"                                  \
                    "[userid: (1)],"                                \
                    "[nid: (1)],"                                   \
                    "[url: (url1)],"                                \
                    "};";
        
    // build truncate index
    BuildTruncateIndex(mSchema, docStr, false);
        
    partition::IndexPartitionReaderPtr reader = CreatePartitionReader();
    IndexReaderPtr indexReader = reader->GetIndexReader();
    Term term(VOL_CONTENT, "title");
    PostingIteratorPtr postingIter(indexReader->Lookup(term, 1000, pt_normal));
    INDEXLIB_TEST_TRUE(!postingIter);
    postingIter.reset(indexReader->Lookup(term));
}

void TruncateIndexTest::TestCaseForReadTruncateWithOnlyBitmap()
{
    //normal posting can be readed by truncate
    TruncateParams param("", "", "title=ends:ends#DESC:ends::","truncate_meta");
    ResetIndexPartitionSchema(param);
    SetHighFrequenceDictionary("title", true, hp_bitmap);

    mOptions.GetMergeConfig().truncateOptionConfig = 
        TruncateConfigMaker::MakeConfig(param);

    CreateTruncateMeta("title_ends", "token1;token2", "4;3", "title", false);

    string docStr = "{ 11, "                                        \
                    "[title: (token1)],"                            \
                    "[ends: (1)],"                                  \
                    "[userid: (1)],"                                \
                    "[nid: (1)],"                                   \
                    "[url: (url1)],"                                \
                    "};"
                    "{ 12, "                                        \
                    "[title: (token1)],"                            \
                    "[ends: (5)],"                                  \
                    "[userid: (2)],"                                \
                    "[nid: (2)],"                                   \
                    "[url: (url2)],"                                \
                    "};";
        
    // build truncate index
    BuildTruncateIndex(mSchema, docStr, true);
   
    partition::IndexPartitionReaderPtr indexPartReader = CreatePartitionReader();
    IndexReaderPtr indexReader = indexPartReader->GetIndexReader("title");

    Term term(VOL_CONTENT, "title", "ends");
    PostingIteratorPtr postingIter(indexReader->Lookup(term, 1000, pt_default));
    TermMeta *termMeta = postingIter->GetTermMeta();
    df_t docFreq = termMeta->GetDocFreq();
    INDEXLIB_TEST_EQUAL(2, docFreq);


    PostingIteratorPtr postingIterTrunc(indexReader->Lookup(term, 1000, pt_normal));
    termMeta = postingIterTrunc->GetTruncateTermMeta();
    docFreq = termMeta->GetDocFreq();
    INDEXLIB_TEST_EQUAL(1, docFreq);
}

void TruncateIndexTest::TestCaseForTruncateMetaStrategy()
{
    TruncateParams param("", "", "title=ends:ends#DESC:ends::","truncate_meta");
    ResetIndexPartitionSchema(param);

    mOptions.GetMergeConfig().truncateOptionConfig = 
        TruncateConfigMaker::MakeConfig(param);

    CreateTruncateMeta("title_ends", "token1;token2", "4;3");

    // build truncate index
    BuildTruncateIndex(mSchema, mRawDocStr);

    string expectedDocStr = 
        "{ 3, "                                                     \
        "[title: (not_used token2 token3)],"                        \
        "[ends: (3)],"                                              \
        "[userid: (3)],"                                            \
        "[nid: (3)],"                                               \
        "};"                                                        \
        "{ 4, "                                                     \
        "[title: (token1 token2 token3 token4)],"                   \
        "[ends: (4)],"                                              \
        "[userid: (4)],"                                            \
        "[nid: (4)],"                                               \
        "};"                                                        \
        "{ 5, "                                                     \
        "[title: (token1 token2 token3 token4 token5)],"            \
        "[ends: (5)],"                                              \
        "[userid: (5)],"                                            \
        "[nid: (6)],"                                               \
        "};"                                                        \
        "{ 5, "                                                     \
        "[title: (token1 token3 token5)],"                          \
        "[ends: (5)],"                                              \
        "[userid: (5)],"                                            \
        "[nid: (5)],"                                               \
        "};"                                                        \
        "{ 4, "                                                     \
        "[title: (token2 token4)],"                                 \
        "[ends: (4)],"                                              \
        "[userid: (4)],"                                            \
        "[nid: (7)],"                                               \
        "};";
    CheckTruncateIndex(expectedDocStr, "2;3;4;5;6", "token1;token2", 
                       "title_ends");
}


void TruncateIndexTest::TestCaseForTruncateMetaStrategyWithNoDoc()
{
    TruncateParams param("", "", "title=ends:ends#DESC:ends::","truncate_meta");
    ResetIndexPartitionSchema(param);

    mOptions.GetMergeConfig().truncateOptionConfig = 
        TruncateConfigMaker::MakeConfig(param);

    CreateTruncateMeta("title_ends", "token1;token2", "10;15");

    // build truncate index
    BuildTruncateIndex(mSchema, mRawDocStr);

    // token1 and token2 do not exist in truncate index
    partition::IndexPartitionReaderPtr indexPartReader = CreatePartitionReader();
    IndexReaderPtr indexReader = indexPartReader->GetIndexReader();
    Term term("token1", "title_ends");
    PostingIteratorPtr postingIter(indexReader->Lookup(term));
    INDEXLIB_TEST_TRUE(!postingIter);

    Term term2("token2", "title_ends");
    postingIter.reset(indexReader->Lookup(term2));
    INDEXLIB_TEST_TRUE(!postingIter);
}

    // for bug #230304 
void TruncateIndexTest::TestCaseForTruncateWithEmptySegment()
{
    TruncateParams param("", "", "title=ends:ends#DESC:::");
    ResetIndexPartitionSchema(param);

    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    DocumentMaker::DocumentVect docVect;
    MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(mRawDocStr, mSchema, 
                                 docVect, mockIndexPart);

    // build segment_0
    partition::IndexBuilder indexBuilder(mRootDir, mOptions, mSchema, mQuotaControl);
    INDEXLIB_TEST_TRUE(indexBuilder.Init());
    for (size_t i = 0; i < docVect.size(); ++i)
    {
        indexBuilder.BuildDocument(docVect[i]);
    }
    indexBuilder.DumpSegment();

    // build segment_1, which only has delete docs
    DocumentPtr delDoc = DocumentMaker::MakeDeletionDocument("url1", 0);
    indexBuilder.BuildDocument(delDoc);
    delDoc = DocumentMaker::MakeDeletionDocument("url4", 0);
    indexBuilder.BuildDocument(delDoc);
    indexBuilder.EndIndex();
    // merge, generate truncate index
    indexBuilder.Merge(mOptions);

    string expectedDocStr = "{ 2, "                                 \
                            "[title: (token1 token2)],"             \
                            "[ends: (2)],"                          \
                            "[userid: (2)],"                        \
                            "[url: (url2)],"                        \
                            "[nid: (2)],"                           \
                            "};"                                    \
                            "{ 3, "                                 \
                            "[title: (token1 token2 token3)],"      \
                            "[ends: (3)],"                          \
                            "[userid: (3)],"                        \
                            "[nid: (3)],"                           \
                            "[url: (url3)],"                        \
                            "};"                                    \
                            "{ 5, "                                 \
                            "[title: (token1 token2 token3 token4 token5)]," \
                            "[ends: (5)],"                          \
                            "[userid: (5)],"                        \
                            "[nid: (6)],"                           \
                            "[url: (url5)],"                        \
                            "};"                                    \
                            "{ 5, "                                 \
                            "[title: (token1 token3 token5)],"      \
                            "[ends: (5)],"                          \
                            "[userid: (5)],"                        \
                            "[nid: (5)],"                           \
                            "[url: (url6)],"                        \
                            "};"                                    \
                            "{ 4, "                                 \
                            "[title: (token2 token4)],"             \
                            "[ends: (4)],"                          \
                            "[userid: (4)],"                        \
                            "[nid: (7)],"                           \
                            "[url: (url7)],"                        \
                            "};";
    CheckTruncateIndex(expectedDocStr, "0;1;2;3;4", 
                       "token1;token2;token3;token4;token5", "title_ends");
}

    // for bug #233692
void TruncateIndexTest::TestCaseForInvalidConfig()
{
    TruncateParams param("", "", "title=ends:not_exist_field#DESC:ends::",
                         "truncate_meta");

    ResetIndexPartitionSchema(param);
    mOptions.GetMergeConfig().truncateOptionConfig = 
        TruncateConfigMaker::MakeConfig(param);

    partition::IndexBuilder indexBuilder(mRootDir, mOptions, mSchema, mQuotaControl);
    INDEXLIB_TEST_TRUE(!indexBuilder.Init());
}

void TruncateIndexTest::TestCaseForTruncateWithBalanceTreeMerge()
{
    TruncateParams param("", "", "title=ends_filter::::,ends_filter2::::");
    ResetIndexPartitionSchema(param);
    mOptions.GetMergeConfig().mergeStrategyStr = "balance_tree";
    mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
            "conflict-segment-number=3;base-doc-count=10240;max-doc-count=102400");

    string optionFile = string(TEST_DATA_PATH) + 
                        "/truncate_index_intetest_option.json";
    string optionStr = LoadOptionConfigString(optionFile);
    TruncateOptionConfigPtr truncateOptionConfig =
	TruncateConfigMaker::MakeConfigFromJson(optionStr);
    ASSERT_TRUE(truncateOptionConfig);
    mOptions.GetMergeConfig().truncateOptionConfig = truncateOptionConfig;

    DocumentMaker::DocumentVect docVect;
    MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(mRawDocStr, mSchema, 
                                 docVect, mockIndexPart);
  
    partition::IndexBuilder indexBuilder(mRootDir, mOptions, mSchema, mQuotaControl);
    INDEXLIB_TEST_TRUE(indexBuilder.Init());
    for (size_t i = 0; i < docVect.size(); ++i)
    {
        docVect[i]->SetTimestamp(4 * 1000 * 1000);
        indexBuilder.BuildDocument(docVect[i]);
    }
    indexBuilder.EndIndex();
    indexBuilder.Merge(mOptions, false);

    string expectedDocStr = 
        "{ 4, "                                                     \
        "[title: (token1 token2 token3 token4)],"                   \
        "[ends: (4)],"                                              \
        "[userid: (4)],"                                            \
        "[nid: (4)],"                                               \
        "[url: (url4)],"                                            \
        "};"                                                        \
        "{ 5, "                                                     \
        "[title: (token1 token2 token3 token4 token5)],"            \
        "[ends: (5)],"                                              \
        "[userid: (5)],"                                            \
        "[nid: (6)],"                                               \
        "[url: (url5)],"                                            \
        "};"                                                        \
        "{ 5, "                                                     \
        "[title: (token1 token3 token5)],"                          \
        "[ends: (5)],"                                              \
        "[userid: (5)],"                                            \
        "[nid: (5)],"                                               \
        "[url: (url6)],"                                            \
        "};"                                                        \
        "{ 4, "                                                     \
        "[title: (token2 token4)],"                                 \
        "[ends: (4)],"                                              \
        "[userid: (4)],"                                            \
        "[nid: (7)],"                                               \
        "[url: (url7)],"                                            \
        "};";
    CheckTruncateIndex(expectedDocStr, "3;4;5;6", 
                       "token1;token2;token3;token4;token5", 
                       "title_ends_filter");
}

void TruncateIndexTest::TestCaseForInvalidBeginTime()
{
    TruncateParams param("", "", "title=ends_filter::::,ends_filter2::::");
    ResetIndexPartitionSchema(param);
    mOptions.GetMergeConfig().mergeStrategyStr = "balance_tree";
    mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
            "conflict-segment-number=3;base-doc-count=10240;max-doc-count=102400");
    string optionFile = string(TEST_DATA_PATH) + 
                        "/truncate_index_intetest_option.json";
    string optionStr = LoadOptionConfigString(optionFile);
    TruncateOptionConfigPtr truncateOptionConfig =
	TruncateConfigMaker::MakeConfigFromJson(optionStr);
    ASSERT_TRUE(truncateOptionConfig);
    mOptions.GetMergeConfig().truncateOptionConfig = truncateOptionConfig;
    
    DocumentMaker::DocumentVect docVect;
    MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(mRawDocStr, mSchema, 
                                 docVect, mockIndexPart);

    partition::IndexBuilder indexBuilder(mRootDir, mOptions, mSchema, mQuotaControl);
    INDEXLIB_TEST_TRUE(indexBuilder.Init());
    for (size_t i = 0; i < docVect.size(); ++i)
    {
        docVect[i]->SetTimestamp(-1);
        indexBuilder.BuildDocument(docVect[i]);
    }
    indexBuilder.EndIndex();
    ASSERT_THROW(indexBuilder.Merge(mOptions, false), BadParameterException);
}

void TruncateIndexTest::TestCaseForCheckTruncateMeta()
{
    TruncateParams param("3:2", "userid:1:3", "title=ends:ends#DESC:::");

    ResetIndexPartitionSchema(param);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    // build index
    BuildTruncateIndex(mSchema, mRawDocStr);
    std::map<dictkey_t, int> mapMeta;
    GetMetaFromMetafile(mapMeta, "title_ends");
    CheckTruncateMeta("token1;token2;token3", "5;4;5", mapMeta);

    DeployIndexChecker::CheckDeployIndexMeta(GET_TEST_DATA_PATH(), 0, INVALID_VERSION);
    DeployIndexChecker::CheckDeployIndexMeta(GET_TEST_DATA_PATH(), 1, INVALID_VERSION);
    DeployIndexChecker::CheckDeployIndexMeta(GET_TEST_DATA_PATH(), 1, 0,
            {"adaptive_bitmap_meta/","adaptive_bitmap_meta/deploy_index",
                    "adaptive_bitmap_meta/segment_file_list","truncate_meta/",
                    "truncate_meta/deploy_index", "truncate_meta/segment_file_list",
                    "truncate_meta/deploy_index",
                    "truncate_meta/index.mapper",
                    "truncate_meta/title_ends"});
}

void TruncateIndexTest::TestCaseForCheckTruncateMetaWithDiffSortValue()
{
    TruncateParams param1("3:2", "userid:2:4", "title=ends:ends#DESC:::");

    ResetIndexPartitionSchema(param1);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param1);
    std::string docStr = "{ 1, "                                            \
                         "[title: (token1)],"                               \
                         "[ends: (1)],"                                     \
                         "[userid: (1)],"                                   \
                         "[nid: (1)],"                                      \
                         "[url: (url1)],"                                   \
                         "};"                                               \
                         "{ 2, "                                            \
                         "[title: (token1 token2)],"                        \
                         "[ends: (2)],"                                     \
                         "[userid: (2)],"                                   \
                         "[url: (url2)],"                                   \
                         "[nid: (2)],"                                      \
                         "};"                                               \
                         "{ 3, "                                            \
                         "[title: (token1 token2 token3)],"                 \
                         "[ends: (3)],"                                     \
                         "[userid: (3)],"                                   \
                         "[nid: (3)],"                                      \
                         "[url: (url3)],"                                   \
                         "};"                                               \
                         "{ 4, "                                            \
                         "[title: (token1 token2 token3 token4)],"          \
                         "[ends: (4)],"                                     \
                         "[userid: (4)],"                                   \
                         "[nid: (4)],"                                      \
                         "[url: (url4)],"                                   \
                         "};"                                               \
                         "{ 5, "                                            \
                         "[title: (token1 token2 token3 token4 token5)],"   \
                         "[ends: (5)],"                                     \
                         "[userid: (5)],"                                   \
                         "[nid: (6)],"                                      \
                         "[url: (url5)],"                                   \
                         "};"                                               \
                         "{ 5, "                                            \
                         "[title: (token1 token3 token5)],"                 \
                         "[ends: (6)],"                                     \
                         "[userid: (5)],"                                   \
                         "[nid: (5)],"                                      \
                         "[url: (url6)],"                                   \
                         "};"                                               \
                         "{ 4, "                                            \
                         "[title: (token1 token2 token4)],"                 \
                         "[ends: (7)],"                                     \
                         "[userid: (4)],"                                   \
                         "[nid: (7)],"                                      \
                         "[url: (url7)],"                                   \
                         "};"                                               \
                         "{ 6, "                                            \
                         "[title: (token2 token3 token4)],"                 \
                         "[ends: (8)],"                                     \
                         "[userid: (4)],"                                   \
                         "[nid: (8)],"                                      \
                         "[url: (url8)],"                                   \
                         "};"                                               \
                         "{ 7, "                                            \
                         "[title: (token2 token4 token5)],"                 \
                         "[ends: (9)],"                                     \
                         "[userid: (4)],"                                   \
                         "[nid: (7)],"                                      \
                         "[url: (url9)],"                                   \
                         "};";

    // build index
    BuildTruncateIndex(mSchema, docStr);
    std::map<dictkey_t, int> mapMeta;
    GetMetaFromMetafile(mapMeta, "title_ends");
    CheckTruncateMeta("token1;token2;token3;token4", "6;5;6;5", mapMeta);
}

string TruncateIndexTest::LoadOptionConfigString(const string& optionFile)
{
    ifstream in(optionFile.c_str());
    if (!in)
    {
        IE_LOG(ERROR, "OPEN INPUT FILE[%s] ERROR", optionFile.c_str());
        return "";
    }
    string line;
    string jsonString;
    while(getline(in, line))
    {
        jsonString += line;
    }

    IE_LOG(DEBUG, "jsonString:%s", jsonString.c_str());
    return jsonString;
}

void TruncateIndexTest::InnerTestForTruncate(
    const string& docStr, 
    const string& expecetedTruncDocStr,
    const string& expectedDocIds,
    const string& toCheckTokenStr,
    const TruncateParams& param, 
    const string& truncIndexNameStr, 
    bool hasHighFreq)
{
    TearDown();
    SetUp();
    ResetIndexPartitionSchema(param);
    
    SetHighFrequenceDictionary("title", hasHighFreq, hp_both);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    // build index
    BuildTruncateIndex(mSchema, docStr);

    // check truncate index
    CheckTruncateIndex(expecetedTruncDocStr, expectedDocIds, 
                       toCheckTokenStr, truncIndexNameStr);
}

void TruncateIndexTest::InnerTestForTruncateWithTestSplit(
    const string& docStr, 
    const string& expecetedTruncDocStr,
    const string& expectedDocIds,
    const string& toCheckTokenStr,
    const TruncateParams& param, 
    const string& truncIndexNameStr, 
    bool hasHighFreq,
    int segmentCount)
{
    TearDown();
    SetUp();
    ResetIndexPartitionSchema(param);
    std::string mergeConfigStr = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\""+ StringUtil::toString(segmentCount) +"\"}}";
    autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    
    SetHighFrequenceDictionary("title", hasHighFreq, hp_both);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    // build index
    BuildTruncateIndex(mSchema, docStr);

    // check truncate index
    CheckTruncateIndex(expecetedTruncDocStr, expectedDocIds, 
                       toCheckTokenStr, truncIndexNameStr);
}

void TruncateIndexTest::InnerTestForBitmapTruncate(const string& docStr, 
        const string& expecetedTruncDocStr,
        const string& expectedDocIds,
        const string& toCheckTokenStr,
        const TruncateParams& param, 
        const string& truncIndexNameStr,
        const string& expectedHighFreqTermDocIds)
{
    TearDown();
    SetUp();
    ResetIndexPartitionSchema(param);
    
    // high frequence term : only bitmap
    SetHighFrequenceDictionary("title", true, hp_bitmap);
    mOptions.GetMergeConfig().truncateOptionConfig = 
        TruncateConfigMaker::MakeConfig(param);
        
    BuildTruncateIndex(mSchema, docStr);

    // check truncate index
    CheckTruncateIndex(expecetedTruncDocStr, expectedDocIds, 
                       toCheckTokenStr, truncIndexNameStr);

    CheckBitmapTruncateIndex(expectedHighFreqTermDocIds, 
                             truncIndexNameStr);
}


void TruncateIndexTest::InnerTestForBitmapTruncateWithTestSplit(const string& docStr, 
        const string& expecetedTruncDocStr,
        const string& expectedDocIds,
        const string& toCheckTokenStr,
        const TruncateParams& param, 
        const string& truncIndexNameStr,
        const string& expectedHighFreqTermDocIds,
        int segmentCount)
{
    TearDown();
    SetUp();
    ResetIndexPartitionSchema(param);
    std::string mergeConfigStr = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\""+ StringUtil::toString(segmentCount) +"\"}}";
    autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
    
    // high frequence term : only bitmap
    SetHighFrequenceDictionary("title", true, hp_bitmap);
    mOptions.GetMergeConfig().truncateOptionConfig = 
        TruncateConfigMaker::MakeConfig(param);
        
    BuildTruncateIndex(mSchema, docStr);

    // check truncate index
    CheckTruncateIndex(expecetedTruncDocStr, expectedDocIds, 
                       toCheckTokenStr, truncIndexNameStr);

    CheckBitmapTruncateIndex(expectedHighFreqTermDocIds, 
                             truncIndexNameStr);
}


void TruncateIndexTest::BuildTruncateIndex(
        const IndexPartitionSchemaPtr& schema, 
        const string& rawDocStr, bool needMerge)
{
    DocumentMaker::DocumentVect docVect;
    MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(rawDocStr, schema, 
                                 docVect, mockIndexPart);

    // build index
    partition::IndexBuilder indexBuilder(mRootDir, mOptions, schema, mQuotaControl);
    INDEXLIB_TEST_TRUE(indexBuilder.Init());

    for (size_t i = 0; i < docVect.size(); ++i)
    {
        indexBuilder.BuildDocument(docVect[i]);
    }
    indexBuilder.EndIndex();
    if (needMerge)
    {
        indexBuilder.Merge(mOptions);
    }
}

void TruncateIndexTest::CheckTruncateIndex(const string& expectedTrucDocStr, 
        const string& expectedDocIds,
        const string& toCheckTokenStr,
        const string& indexNameStr)
{
    partition::OnlinePartition indexPart;
    indexPart.Open(mRootDir, "", IndexPartitionSchemaPtr(), mOptions);
    IndexReaderPtr indexReader = indexPart.GetReader()->GetIndexReader();
    mSchema = indexPart.GetSchema();

    DocumentMaker::DocumentVect docVect;
    MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(expectedTrucDocStr, mSchema, 
                                 docVect, mockIndexPart, expectedDocIds);

    uint32_t docCount = mockIndexPart.GetDocCount();
    INDEXLIB_TEST_EQUAL(docCount, (uint32_t)docVect.size());

    vector<string> indexNames;
    StringUtil::fromString(indexNameStr, indexNames, ";");
    INDEXLIB_TEST_TRUE(indexNames.size() > 0);

    vector<string> toCheckTokens;
    StringUtil::fromString(toCheckTokenStr, toCheckTokens, ";");

    DocumentCheckerForGtest checker;
    for (size_t i = 0; i < indexNames.size(); i++)
    {
        if (docCount > 0)
        {
            checker.CheckIndexData(indexReader, 
                    mSchema->GetIndexSchema(), indexNames[i], 
                    mockIndexPart, toCheckTokens);
        }
        else
        {
            DirectoryPtr segmentDir = DirectoryCreator::Create(
                PathUtil::JoinPath(mRootDir, "segment_1_level_0"));
            segmentDir->MountPackageFile(PACKAGE_FILE_PREFIX);
            // check dictionary and posting files
            DirectoryPtr indexDir = segmentDir->GetDirectory(
                PathUtil::JoinPath("index", indexNames[i]), false);
            if (indexDir)
            {
                ASSERT_FALSE(indexDir->IsExist(DICTIONARY_FILE_NAME));
                ASSERT_FALSE(indexDir->IsExist(POSTING_FILE_NAME));
            }
        }
    }
}

partition::IndexPartitionReaderPtr TruncateIndexTest::CreatePartitionReader()
{
    IndexPartitionSchemaPtr schema = 
        SchemaAdapter::LoadAndRewritePartitionSchema(mRootDir, mOptions);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    PartitionDataPtr partitionData = 
        PartitionDataCreator::CreateBuildingPartitionData(
                GET_FILE_SYSTEM(), schema, mOptions,
                util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
                container);
    partition::IndexPartitionReaderPtr reader(
        new partition::OnlinePartitionReader(
            mOptions, schema, util::SearchCachePartitionWrapperPtr()));
    reader->Open(partitionData);
    return reader;
}

void TruncateIndexTest::CheckBitmapTruncateIndex(const string& expectedDocIds,
        const string& indexNameStr)
{
    partition::IndexPartitionReaderPtr reader = CreatePartitionReader();
    IndexReaderPtr indexReader = reader->GetIndexReader();
    vector<docid_t> docIds;
    autil::StringUtil::fromString(expectedDocIds, docIds, ";");

    IndexConfigPtr indexConfig = 
        mSchema->GetIndexSchema()->GetIndexConfig(indexNameStr);
    assert(indexConfig);

    Term term(VOL_CONTENT, indexNameStr);
    PostingIteratorPtr postingIter(indexReader->Lookup(term));
    if (docIds.size() == 0)
    {
        INDEXLIB_TEST_TRUE(postingIter == NULL);
        return;
    }
    else
    {
        INDEXLIB_TEST_TRUE(postingIter != NULL);
    }

    size_t count = 0;
    docid_t docId = INVALID_DOCID;
    while ((docId = postingIter->SeekDoc(docId)) != INVALID_DOCID)
    {
        INDEXLIB_TEST_EQUAL(docId, docIds[count++]);
        // default bitmap doc payload is 0 
        INDEXLIB_TEST_EQUAL((docpayload_t)0, 
                            postingIter->GetDocPayload());
        if (!postingIter->HasPosition())
        {
            continue;
        }

        TermMatchData termMatchData;
        postingIter->Unpack(termMatchData);
        // default bitmap truncate tf is 1
        INDEXLIB_TEST_EQUAL((tf_t)1, termMatchData.GetTermFreq());

        if (indexConfig->GetIndexType() == it_expack)
        {
            INDEXLIB_TEST_EQUAL((fieldmap_t)1,
                    termMatchData.GetFieldMap());
        }

        InDocPositionIteratorPtr positionIter =
            termMatchData.GetInDocPositionIterator();
        pos_t pos = 0;
        pos = positionIter->SeekPosition(pos);
        // default bitmap truncate position info : pos 0, pos_payload 0
        INDEXLIB_TEST_EQUAL((pos_t)0, pos);
        INDEXLIB_TEST_EQUAL((pospayload_t)0, 
                            positionIter->GetPosPayload());
        INDEXLIB_TEST_EQUAL(INVALID_POSITION, 
                            positionIter->SeekPosition(pos));
    }
    INDEXLIB_TEST_EQUAL(count, docIds.size());
}

void TruncateIndexTest::SetHighFrequenceDictionary(const string& indexName, bool hasHighFreq,
        HighFrequencyTermPostingType type)
{
    IndexSchemaPtr indexSchema = mSchema->GetIndexSchema();
    assert(indexSchema != NULL);
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
    assert(indexConfig != NULL);
    DictionarySchemaPtr dictSchema = mSchema->GetDictSchema();
    assert(dictSchema != NULL);

    if (hasHighFreq)
    {
        DictionaryConfigPtr dictConfig = 
            dictSchema->GetDictionaryConfig(VOL_NAME);
        assert(dictConfig != NULL);
        indexConfig->SetDictConfig(dictConfig);
        indexConfig->SetHighFreqencyTermPostingType(type);
    }
    else
    {
        indexConfig->SetDictConfig(DictionaryConfigPtr());
    }
}

void TruncateIndexTest::ResetIndexPartitionSchema(const TruncateParams& param)
{
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(
        mSchema,
        // field schema
        "title:text;ends:uint32;userid:uint32;nid:uint32;url:string",
        // index schema
        "title:text:title;pk:primarykey64:url",
        // attribute schema
        "ends;userid;nid",
        // summary schema
        "");
    mSchema->AddDictionaryConfig(VOL_NAME, VOL_CONTENT);
    TruncateConfigMaker::RewriteSchema(mSchema, param);
}

void TruncateIndexTest::CreateTruncateMeta(const string &truncateName, const string &tokenStr, 
        const string &valueStr, const string &indexName, bool useArchiveFolder)
{
    // make truncate meta
    string truncateMetaDir = FileSystemWrapper::JoinPath(mRootDir, 
            TRUNCATE_META_DIR_NAME);
    if (FileSystemWrapper::IsExist(truncateMetaDir))
    {
        FileSystemWrapper::DeleteDir(truncateMetaDir);
    }
    FileSystemWrapper::MkDir(truncateMetaDir);
    ArchiveFolderPtr archiveFolder(new ArchiveFolder(!useArchiveFolder));
    archiveFolder->Open(truncateMetaDir);
    // string metaFile = FileSystemWrapper::JoinPath(
    //         truncateMetaDir, truncateName);
    vector<string> tokens;
    vector<int64_t> values;

    StringUtil::fromString(tokenStr, tokens, ";");
    StringUtil::fromString(valueStr, values, ";");
    INDEXLIB_TEST_TRUE(tokens.size() == values.size());
    
    FileWrapperPtr writer;
    writer = archiveFolder->GetInnerFile(truncateName, fslib::WRITE);
    for (size_t i = 0; i < tokens.size(); i++)
    {
        dictkey_t key = autil::HashAlgorithm::hashString64(
                tokens[i].c_str());
        string keyStr = StringUtil::toString(key);
        string valueStr = StringUtil::toString(values[i]);
        string line = keyStr + "\t" + valueStr + "\n";
        writer->Write(line.c_str(), line.size());
    }
    writer->Close();
    writer.reset();
    if (archiveFolder)
    {
        archiveFolder->Close();
    }

    if (indexName != "")
    {
        TruncateIndexNameMapper truncMapper("");
        vector<string> truncateIndexNames;
        truncateIndexNames.push_back(truncateName);
        truncMapper.AddItem(indexName, truncateIndexNames);
        string filePath = FileSystemWrapper::JoinPath(
                truncateMetaDir, "index.mapper");
        truncMapper.DoDump(filePath);
    }
    DeployIndexWrapper deployIndexWrapper(mRootDir);
    deployIndexWrapper.DumpTruncateMetaIndex();
}

void TruncateIndexTest::GetMetaFromMetafile(std::map<dictkey_t, int>& mapMeta, 
        std::string truncate_index_name)
{
    string truncateMetaDir = FileSystemWrapper::JoinPath(mRootDir, 
            TRUNCATE_META_DIR_NAME);
    ArchiveFolderPtr archiveFolder(new ArchiveFolder(true));
    archiveFolder->Open(truncateMetaDir, "0");
    // string metaFile = FileSystemWrapper::JoinPath(truncateMetaDir, 
    //         truncate_index_name);
    // INDEXLIB_TEST_TRUE(FileSystemWrapper::IsExist(metaFile));
    FileWrapperPtr metaFile = archiveFolder->GetInnerFile(truncate_index_name, fslib::READ);
    ASSERT_TRUE(metaFile);
    LineReader reader;
    reader.Open(metaFile);
    string line;
    dictkey_t key;
    int64_t value;
    while (reader.NextLine(line))
    {
        vector<string> strVec = StringUtil::split(line, "\t");
        INDEXLIB_TEST_EQUAL(strVec.size(),  (size_t)2);
        StringUtil::fromString(strVec[0], key);
        StringUtil::fromString(strVec[1], value);
        mapMeta[key]=value;
        line.clear();
    }
    archiveFolder->Close();
}

void TruncateIndexTest::CheckTruncateMeta(std::string keys, std::string values, 
        const std::map<dictkey_t, int>& mapMeta)
{
    vector<std::string> keyVec;
    vector<int> valueVec;

    StringUtil::fromString(keys, keyVec, ";");
    StringUtil::fromString(values, valueVec, ";");
    INDEXLIB_TEST_EQUAL(keyVec.size(), keyVec.size());
    INDEXLIB_TEST_EQUAL(mapMeta.size(), valueVec.size());
    std::map<dictkey_t, int>::const_iterator ite;
    for (size_t i = 0; i < keyVec.size(); ++i)
    {
        dictkey_t key = autil::HashAlgorithm::hashString64(keyVec[i].c_str());
        ite = mapMeta.find(key);
        INDEXLIB_TEST_TRUE(ite != mapMeta.end());
        INDEXLIB_TEST_EQUAL(ite->second , valueVec[i]) << i;
    }
} 

IE_NAMESPACE_END(index);

