#include "indexlib/index/merger_util/truncate/test/truncate_index_intetest.h"

#include <fstream>

#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/config/truncate_index_name_mapper.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/archive/ArchiveFile.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/file_system/archive/LineReader.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/index/test/deploy_index_checker.h"
#include "indexlib/index/test/document_checker_for_gtest.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace autil;

using namespace indexlib::common;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::document;

namespace indexlib::index::legacy {
using namespace indexlibv2::index;
IE_LOG_SETUP(index, TruncateIndexTest);

#define VOL_NAME "title_vol"
#define VOL_CONTENT "token1"
typedef DocumentMaker::MockIndexPart MockIndexPart;

TruncateIndexTest::TruncateIndexTest() {}

TruncateIndexTest::~TruncateIndexTest() {}

void TruncateIndexTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    mOptions = IndexPartitionOptions();
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    mOptions.GetMergeConfig().mergeThreadCount = 1;
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
    mQuotaControl.reset(new util::QuotaControl(1024 * 1024 * 1024));
}

void TruncateIndexTest::CaseTearDown() {}

void TruncateIndexTest::TestCaseForNoTruncateByAttribute()
{
    string expectedDocStr = mRawDocStr;
    TruncateParams param("", "", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "0;1;2;3;4;5;6", "token1;token2;token3;token4;token5", param,
                         "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "0;1;2;3;4;5;6", "token1;token2;token3;token4;token5", param,
                         "title_ends", true);

    expectedDocStr = "{ 2, "
                     "[title: (token1 token2)],"
                     "[ends: (2)],"
                     "[userid: (2)],"
                     "[nid: (2)],"
                     "};"
                     "{ 3, "
                     "[title: (token1 token2 token3)],"
                     "[ends: (3)],"
                     "[userid: (3)],"
                     "[nid: (3)],"
                     "};"
                     "{ 4, "
                     "[title: (token1 token2 token3 token4)],"
                     "[ends: (4)],"
                     "[userid: (4)],"
                     "[nid: (4)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token2 token3 token4 token5)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (6)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token3 token5)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (5)],"
                     "};"
                     "{ 4, "
                     "[title: (token2 token4)],"
                     "[ends: (4)],"
                     "[userid: (4)],"
                     "[nid: (7)],"
                     "};";

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "1;2;3;4;5;6", "token2;token3;token4;token5", param,
                               "title_ends", "0;1;2;3;4;5");
}

void TruncateIndexTest::TestCaseForNoTruncateByDocPayload()
{
    // do not support sort by doc payload
    string expectedDocStr = mRawDocStr;
    TruncateParams param("", "", "title=doc_payload:DOC_PAYLOAD#DESC:DOC_PAYLOAD:1:5", /*inputStrategyType=*/"default",
                         /*inputTruncateProfile2PayloadNames=*/"doc_payload:unused_payload_name");

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "0;1;2;3;4;5;6", "token1;token2;token3;token4;token5", param,
                         "title_doc_payload", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "0;1;2;3;4;5;6", "token1;token2;token3;token4;token5", param,
                         "title_doc_payload", true);

    expectedDocStr = "{ 2, "
                     "[title: (token1 token2)],"
                     "[ends: (2)],"
                     "[userid: (2)],"
                     "[nid: (2)],"
                     "};"
                     "{ 3, "
                     "[title: (token1 token2 token3)],"
                     "[ends: (3)],"
                     "[userid: (3)],"
                     "[nid: (3)],"
                     "};"
                     "{ 4, "
                     "[title: (token1 token2 token3 token4)],"
                     "[ends: (4)],"
                     "[userid: (4)],"
                     "[nid: (4)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token2 token3 token4 token5)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (6)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token3 token5)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (5)],"
                     "};"
                     "{ 4, "
                     "[title: (token2 token4)],"
                     "[ends: (4)],"
                     "[userid: (4)],"
                     "[nid: (7)],"
                     "};";

    // doc_payload for token1 is 0, not in 1~5, not found
    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "1;2;3;4;5;6", "token2;token3;token4;token5", param,
                               "title_doc_payload", "");
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithoutDistinct_1()
{
    // no truncate index generated
    TruncateParams param1("6:2", "", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");
    string expectedDocStr = "";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "", "", param1, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "", "", param1, "title_ends", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "", "", param1, "title_ends", "");
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithoutDistinct_2()
{
    // df >= 4 --> df = 2
    TruncateParams param2("3:2", "", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");
    string expectedDocStr = "{ 4, "
                            "[title: (unused_token token2)],"
                            "[ends: (4)],"
                            "[userid: (4)],"
                            "[nid: (4)],"
                            "};"
                            "{ 5, "
                            "[title: (token1 token2 token3 )],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (6)],"
                            "};"
                            "{ 5, "
                            "[title: (token1 token3)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (5)],"
                            "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "3;4;5", "token1;token2;token3", param2, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "3;4;5", "token1;token2;token3", param2, "title_ends", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "3;4;5", "token2;token3", param2, "title_ends", "4;5");
}

void TruncateIndexTest::TestCaseForTruncateByDocPayloadWithoutDistinct()
{
    // df >= 4 --> df = 2
    TruncateParams param2("3:2", "", "title=ends:DOC_PAYLOAD#DESC:DOC_PAYLOAD:1:5", /*inputStrategyType=*/"default",
                          /*inputTruncateProfile2PayloadNames=*/"ends:unused_payload_name");
    string expectedDocStr = "{ 5, "
                            "[title: (token1 token2 token3 )],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (6)],"
                            "};"
                            "{ 5, "
                            "[title: (token1 token3)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (5)],"
                            "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "4;5", "token1;token3", param2, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "4;5", "token1;token3", param2, "title_ends", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "4;5", "token3", param2, "title_ends", "");
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithoutDistinctWithTestSplit_1()
{
    // no truncate index generated
    TruncateParams param1("6:2", "", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");
    string expectedDocStr = "";
    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "", "", param1, "title_ends", false);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "", "", param1, "title_ends", true);

    InnerTestForBitmapTruncateWithTestSplit(mRawDocStr, expectedDocStr, "", "", param1, "title_ends", "");
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithoutDistinctWithTestSplit_2()
{
    // df >= 4 --> df = 2
    TruncateParams param2("3:2", "", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");
    string expectedDocStr = "{ 4, "
                            "[title: (unused_token token2)],"
                            "[ends: (4)],"
                            "[userid: (4)],"
                            "[nid: (4)],"
                            "};"
                            "{ 5, "
                            "[title: (token1 token2 token3 )],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (6)],"
                            "};"
                            "{ 5, "
                            "[title: (token1 token3)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (5)],"
                            "};";
    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6", "token1;token2;token3", param2, "title_ends",
                                      false);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6", "token1;token2;token3", param2, "title_ends",
                                      true);

    InnerTestForBitmapTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6", "token2;token3", param2, "title_ends",
                                            "4;6");
}

void TruncateIndexTest::TestCaseForTruncateByDocPayloadWithoutDistinctWithTestSplit_2()
{
    // df >= 4 --> df = 2
    TruncateParams param2("3:2", "", "title=ends:DOC_PAYLOAD#DESC:DOC_PAYLOAD:1:5", /*inputStrategyType=*/"default",
                          /*inputTruncateProfile2PayloadNames=*/"ends:unused_payload_name");
    string expectedDocStr = "{ 5, "
                            "[title: (token1 token2 token3 )],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (6)],"
                            "};"
                            "{ 5, "
                            "[title: (token1 token3)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (5)],"
                            "};";
    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "4;6", "token1;token3", param2, "title_ends", false);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "4;6", "token1;token3", param2, "title_ends", true);

    InnerTestForBitmapTruncateWithTestSplit(mRawDocStr, expectedDocStr, "4;6", "token3", param2, "title_ends", "");
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithoutDistinctWithTestSplitWithEmpty_1()
{
    // no truncate index generated
    TruncateParams param1("6:2", "", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");
    string expectedDocStr = "";
    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "", "", param1, "title_ends", false, 5);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "", "", param1, "title_ends", true, 5);

    InnerTestForBitmapTruncateWithTestSplit(mRawDocStr, expectedDocStr, "", "", param1, "title_ends", "", 5);
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithoutDistinctWithTestSplitWithEmpty_2()
{
    TruncateParams param2("3:2", "", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");
    string expectedDocStr = "{ 5, "
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
                            "};"
                            "{ 5, "
                            "[title: (token1 token2 token3 token4 token5)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (6)],"
                            "[url: (url5)],"
                            "};";

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;3;6", "token1;token2;token3", param2, "title_ends",
                                      false, 5);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;3;6", "token1;token2;token3", param2, "title_ends",
                                      true, 5);

    InnerTestForBitmapTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;3;6", "token2;token3", param2, "title_ends",
                                            "1;6", 5);
}

void TruncateIndexTest::TestCaseForTruncateByMultiAttribute()
{
    // df >= 4 --> df = 1, multi level sort
    TruncateParams param("3:1", "", "title=ends:ends#DESC|nid#ASC:::", /*inputStrategyType=*/"default");
    string expectedDocStr = "{ 5, "
                            "[title: (token1 token3)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (5)],"
                            "};";

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "5", "token1;token3", param, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "5", "token1;token3", param, "title_ends", true);

    TruncateParams param1("3:1", "", "title=ends:ends#DESC|nid#DESC:::", /*inputStrategyType=*/"default");

    expectedDocStr = "{ 5, "
                     "[title: (token1 unused_token token3 )],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (6)],"
                     "};";

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "4", "token1;token3", param1, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "4", "token1;token3", param1, "title_ends", true);
}

void TruncateIndexTest::TestCaseForTruncateByAttributeAndDocPayload()
{
    // df >= 4 --> df = 1, multi level sort
    TruncateParams param("3:1", "", "title=ends:DOC_PAYLOAD#DESC|nid#ASC:::", /*inputStrategyType=*/"default",
                         /*inputTruncateProfile2PayloadNames=*/"ends:unused_payload_name");
    string expectedDocStr = "{ 5, "
                            "[title: (token1 token3)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (5)],"
                            "};";

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "5", "token1;token3", param, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "5", "token1;token3", param, "title_ends", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "5", "token3", param, "title_ends", "0");

    TruncateParams param2("3:1", "",
                          "title=ends:ends#DESC|DOC_PAYLOAD#DESC|nid#ASC:::", /*inputStrategyType=*/"default",
                          /*inputTruncateProfile2PayloadNames=*/"ends:unused_payload_name");

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "5", "token1;token3", param2, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "5", "token1;token3", param2, "title_ends", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "5", "token3", param2, "title_ends", "5");
}

void TruncateIndexTest::TestCaseForTruncateByMultiAttributeWithTestSplit_1()
{
    // df >= 4 --> df = 1, multi level sort
    TruncateParams param("3:1", "", "title=ends:ends#DESC|nid#ASC:::", /*inputStrategyType=*/"default");
    string expectedDocStr = "{ 5, "
                            "[title: (token1 token3)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (5)],"
                            "};";

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "6", "token1;token3", param, "title_ends", false);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "6", "token1;token3", param, "title_ends", true);
}

void TruncateIndexTest::TestCaseForTruncateByMultiAttributeWithTestSplit_2()
{
    TruncateParams param1("3:1", "", "title=ends:ends#DESC|nid#DESC:::", /*inputStrategyType=*/"default");

    string expectedDocStr = "{ 5, "
                            "[title: (token1 unused_token token3 )],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (6)],"
                            "};";

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "4", "token1;token3", param1, "title_ends", false);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "4", "token1;token3", param1, "title_ends", true);
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithDistinctNoExpand()
{
    TruncateParams param1("3:2", "userid:1:3", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");
    string expectedDocStr;
    expectedDocStr = "{ 4, "
                     "[title: (unused_token token2)],"
                     "[ends: (4)],"
                     "[userid: (4)],"
                     "[nid: (4)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token2 token3 )],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (6)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token3)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (5)],"
                     "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "3;4;5", "token1;token2;token3", param1, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "3;4;5", "token1;token2;token3", param1, "title_ends", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "3;4;5", "token2;token3", param1, "title_ends", "4;5");
}

void TruncateIndexTest::TestCaseForTruncateByDocPayloadWithDistinctNoExpand()
{
    TruncateParams param1("3:2", "userid:1:3", "title=ends:DOC_PAYLOAD#DESC:DOC_PAYLOAD:1:5",
                          /*inputStrategyType=*/"default",
                          /*inputTruncateProfile2PayloadNames=*/"ends:unused_payload_name");
    string expectedDocStr;
    expectedDocStr = "{ 5, "
                     "[title: (token1 token2 token3 )],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (6)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token3)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (5)],"
                     "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "4;5", "token1;token3", param1, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "4;5", "token1;token3", param1, "title_ends", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "4;5", "token3", param1, "title_ends", "");
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithDistinctNoExpandWithTestSplit()
{
    TruncateParams param1("3:2", "userid:1:3", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");
    string expectedDocStr;
    expectedDocStr = "{ 4, "
                     "[title: (unused_token token2)],"
                     "[ends: (4)],"
                     "[userid: (4)],"
                     "[nid: (4)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token2 token3 )],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (6)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token3)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (5)],"
                     "};";
    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6", "token1;token2;token3", param1, "title_ends",
                                      false);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6", "token1;token2;token3", param1, "title_ends",
                                      true);

    InnerTestForBitmapTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6", "token2;token3", param1, "title_ends",
                                            "4;6");
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithDistinctExpand()
{
    TruncateParams param("3:2", "userid:2:4", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");
    string expectedDocStr;
    expectedDocStr = "{ 4, "
                     "[title: (token1 token2 token3)],"
                     "[ends: (4)],"
                     "[userid: (4)],"
                     "[nid: (4)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token2 token3)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (6)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token3)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (5)],"
                     "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "3;4;5", "token1;token2;token3", param, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "3;4;5", "token1;token2;token3", param, "title_ends", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "3;4;5", "token2;token3", param, "title_ends", "3;4;5");
}

void TruncateIndexTest::TestCaseForTruncateByDocPayloadWithDistinctExpand()
{
    TruncateParams param("3:3", "userid:3:4", "title=ends:DOC_PAYLOAD#DESC:DOC_PAYLOAD:1:5",
                         /*inputStrategyType=*/"default",
                         /*inputTruncateProfile2PayloadNames=*/"ends:unused_payload_name");
    string expectedDocStr;
    expectedDocStr = "{ 3, "
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
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "2;3;4;5;6", "token1;token2;token3", param, "title_ends", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "2;3;4;5;6", "token1;token2;token3", param, "title_ends", true);

    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "2;3;4;5;6", "token2;token3", param, "title_ends", "");
}

void TruncateIndexTest::TestCaseForTruncateByAttributeWithDistinctExpandWithTestSplit()
{
    TruncateParams param("3:2", "userid:2:4", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");
    string expectedDocStr;
    expectedDocStr = "{ 4, "
                     "[title: (token1 token2 token3)],"
                     "[ends: (4)],"
                     "[userid: (4)],"
                     "[nid: (4)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token2 token3)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (6)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token3)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (5)],"
                     "};";
    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6", "token1;token2;token3", param, "title_ends",
                                      false);

    InnerTestForTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6", "token1;token2;token3", param, "title_ends",
                                      true);

    InnerTestForBitmapTruncateWithTestSplit(mRawDocStr, expectedDocStr, "1;4;6", "token2;token3", param, "title_ends",
                                            "1;4;6");
}

void TruncateIndexTest::TestCaseForDocPayloadWithoutDistinctWithDefautLimit()
{
    TruncateParams param("", "", "title=doc_payload::DOC_PAYLOAD:3:5", /*inputStrategyType=*/"default");
    string expectedDocStr;
    expectedDocStr = "{ 3, "
                     "[title: (token1 token2 token3)],"
                     "[ends: (3)],"
                     "[userid: (3)],"
                     "[nid: (3)],"
                     "};"
                     "{ 4, "
                     "[title: (token1 token2 token3 token4)],"
                     "[ends: (4)],"
                     "[userid: (4)],"
                     "[nid: (4)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token2 token3 token4 token5)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (6)],"
                     "};"
                     "{ 5, "
                     "[title: (token1 token3 token5)],"
                     "[ends: (5)],"
                     "[userid: (5)],"
                     "[nid: (5)],"
                     "};"
                     "{ 4, "
                     "[title: (token2 token4)],"
                     "[ends: (4)],"
                     "[userid: (4)],"
                     "[nid: (7)],"
                     "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "2;3;4;5;6", "token1;token2;token3;token4;token5", param,
                         "title_doc_payload", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "2;3;4;5;6", "token1;token2;token3;token4;token5", param,
                         "title_doc_payload", true);

    // bitmap doc payload for token1 is 0
    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "2;3;4;5;6", "token2;token3;token4;token5", param,
                               "title_doc_payload", "");
}

void TruncateIndexTest::TestCaseForDocPayloadWithoutDistinct()
{
    TruncateParams param("3:2", "", "title=doc_payload::DOC_PAYLOAD:3:5", /*inputStrategyType=*/"default");
    string expectedDocStr = "{ 3, "
                            "[title: (token1 token2 token3)],"
                            "[ends: (3)],"
                            "[userid: (3)],"
                            "[nid: (3)],"
                            "};"
                            "{ 4, "
                            "[title: (token1 token2 token3)],"
                            "[ends: (4)],"
                            "[userid: (4)],"
                            "[nid: (4)],"
                            "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "2;3", "token1;token2;token3", param, "title_doc_payload", false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "2;3", "token1;token2;token3", param, "title_doc_payload", true);

    // bitmap doc payload for token1 is 0
    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "2;3", "token2;token3", param, "title_doc_payload", "");
}

void TruncateIndexTest::TestCaseForDocPayloadWithDistinctNoExpand()
{
    TruncateParams param("3:2", "userid:2:4", "title=doc_payload::DOC_PAYLOAD:1:5", /*inputStrategyType=*/"default");
    string expectedDocStr = "{ 1, "
                            "[title: (token1)],"
                            "[ends: (1)],"
                            "[userid: (1)],"
                            "[nid: (1)],"
                            "};"
                            "{ 2, "
                            "[title: (token1 token2)],"
                            "[ends: (2)],"
                            "[userid: (2)],"
                            "[nid: (2)],"
                            "};"
                            "{ 3, "
                            "[title: (unused_token token2 token3)],"
                            "[ends: (3)],"
                            "[userid: (3)],"
                            "[nid: (3)],"
                            "};"
                            "{ 4, "
                            "[title: (unused_token unused_token token3)],"
                            "[ends: (4)],"
                            "[userid: (4)],"
                            "[nid: (4)],"
                            "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "0;1;2;3", "token1;token2;token3", param, "title_doc_payload",
                         false);

    InnerTestForTruncate(mRawDocStr, expectedDocStr, "0;1;2;3", "token1;token2;token3", param, "title_doc_payload",
                         true);

    expectedDocStr = "{ 2, "
                     "[title: (token1 token2)],"
                     "[ends: (2)],"
                     "[userid: (2)],"
                     "[nid: (2)],"
                     "};"
                     "{ 3, "
                     "[title: (unused_token token2 token3)],"
                     "[ends: (3)],"
                     "[userid: (3)],"
                     "[nid: (3)],"
                     "};"
                     "{ 4, "
                     "[title: (unused_token unused_token token3)],"
                     "[ends: (4)],"
                     "[userid: (4)],"
                     "[nid: (4)],"
                     "};";

    // bitmap doc payload for token1 is 0
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "1;2;3", "token2;token3", param, "title_doc_payload", "");
}

void TruncateIndexTest::TestCaseForDocPayloadWithDistinctExpand()
{
    TruncateParams param("3:2", "userid:2:2", "title=doc_payload::DOC_PAYLOAD:5:5", /*inputStrategyType=*/"default");
    string expectedDocStr = "{ 5, "
                            "[title: (token1 token2 token3)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (6)],"
                            "};"
                            "{ 5, "
                            "[title: (token1 token3)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (5)],"
                            "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "4;5", "token1;token2;token3", param, "title_doc_payload", false);
}
void TruncateIndexTest::TestCaseForDocPayloadWithDistinctExpand2()
{
    TruncateParams param("3:2", "userid:2:2", "title=doc_payload::DOC_PAYLOAD:5:5", /*inputStrategyType=*/"default");
    string expectedDocStr = "{ 5, "
                            "[title: (token1 token2 token3)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (6)],"
                            "};"
                            "{ 5, "
                            "[title: (token1 token3)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (5)],"
                            "};";
    InnerTestForTruncate(mRawDocStr, expectedDocStr, "4;5", "token1;token2;token3", param, "title_doc_payload", true);
}

void TruncateIndexTest::TestCaseForDocPayloadWithDistinctExpand3()
{
    TruncateParams param("3:2", "userid:2:2", "title=doc_payload::DOC_PAYLOAD:5:5", /*inputStrategyType=*/"default");
    string expectedDocStr = "{ 5, "
                            "[title: (token1 token2 token3)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (6)],"
                            "};"
                            "{ 5, "
                            "[title: (token1 token3)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (5)],"
                            "};";
    InnerTestForBitmapTruncate(mRawDocStr, expectedDocStr, "4;5", "token2;token3", param, "title_doc_payload", "");
}

void TruncateIndexTest::TestCaseForDocPayloadWithNoDocLeftForSomeTerm()
{
    string rawDocStr = "{ 1, "
                       "[url: (pk1)],"
                       "[title: (token1)],"
                       "[ends: (1)],"
                       "[userid: (1)],"
                       "[nid: (1)],"
                       "};"
                       "{ 2, "
                       "[url: (pk2)],"
                       "[title: (token2)],"
                       "[ends: (2)],"
                       "[userid: (2)],"
                       "[nid: (2)],"
                       "};"
                       "{ 3, "
                       "[url: (pk3)],"
                       "[title: (token2 token3)],"
                       "[ends: (3)],"
                       "[userid: (3)],"
                       "[nid: (3)],"
                       "};";
    TruncateParams param("", "", "title=doc_payload::DOC_PAYLOAD:2:3", /*inputStrategyType=*/"default");
    string expectedDocStr = "{ 2, "
                            "[url: (pk2)],"
                            "[title: (token2)],"
                            "[ends: (2)],"
                            "[userid: (2)],"
                            "[nid: (2)],"
                            "};"
                            "{ 3, "
                            "[url: (pk3)],"
                            "[title: (token2 token3)],"
                            "[ends: (3)],"
                            "[userid: (3)],"
                            "[nid: (3)],"
                            "};";
    InnerTestForTruncate(rawDocStr, expectedDocStr, "1;2", "token2;token3", param, "title_doc_payload", false);

    // lookup for token1 should return null
    partition::IndexPartitionReaderPtr indexPartReader = CreatePartitionReader();
    std::shared_ptr<InvertedIndexReader> indexReader = indexPartReader->GetInvertedIndexReader();
    Term term("token1", "title_doc_payload");
    std::shared_ptr<PostingIterator> postingIter(indexReader->Lookup(term).ValueOrThrow());
    INDEXLIB_TEST_TRUE(!postingIter);
}

void TruncateIndexTest::TestCaseForTruncateWithOnlyBitmap()
{
    TruncateParams param("", "", "title=ends:ends#DESC:ends::", /*inputStrategyType=*/"truncate_meta");
    ResetIndexPartitionSchema(param);
    SetHighFrequenceDictionary("title", true, hp_bitmap);

    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    CreateTruncateMeta("title_ends", "token1;token2", "4;3", "title", false);

    string docStr = "{ 11, "
                    "[title: (token1)],"
                    "[ends: (1)],"
                    "[userid: (1)],"
                    "[nid: (1)],"
                    "[url: (url1)],"
                    "};";

    // build truncate index
    BuildTruncateIndex(mSchema, docStr);

    RESET_FILE_SYSTEM("ut", false);
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    {
        // merge
        file_system::DirectoryPtr segDirectory = GET_PARTITION_DIRECTORY()->GetDirectory("segment_1_level_0", true);

        file_system::DirectoryPtr indexDirectory = segDirectory->GetDirectory("index/title", true);

        std::shared_ptr<DictionaryReader> dictReader(
            DictionaryCreator::CreateDiskReader(mSchema->GetIndexSchema()->GetIndexConfig("title")));
        ASSERT_TRUE(dictReader->Open(indexDirectory, DICTIONARY_FILE_NAME, false).IsOK());

        std::shared_ptr<DictionaryIterator> iter = dictReader->CreateIterator();
        INDEXLIB_TEST_TRUE(!iter->HasNext());
    }
}

void TruncateIndexTest::TestCaseForReadWithOnlyBitmap()
{
    // normal posting can not be readed
    TruncateParams param("", "", "title=ends:ends#DESC:ends::", /*inputStrategyType=*/"truncate_meta");
    ResetIndexPartitionSchema(param);
    SetHighFrequenceDictionary("title", true, hp_bitmap);

    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    CreateTruncateMeta("title_ends", "token1;token2", "4;3", "title");

    string docStr = "{ 11, "
                    "[title: (token1)],"
                    "[ends: (1)],"
                    "[userid: (1)],"
                    "[nid: (1)],"
                    "[url: (url1)],"
                    "};";

    // build truncate index
    BuildTruncateIndex(mSchema, docStr, false);

    partition::IndexPartitionReaderPtr reader = CreatePartitionReader();
    std::shared_ptr<InvertedIndexReader> indexReader = reader->GetInvertedIndexReader();
    Term term(VOL_CONTENT, "title");
    std::shared_ptr<PostingIterator> postingIter(indexReader->Lookup(term, 1000, pt_normal).ValueOrThrow());
    INDEXLIB_TEST_TRUE(!postingIter);
    postingIter.reset(indexReader->Lookup(term).ValueOrThrow());
}

void TruncateIndexTest::TestCaseForReadTruncateWithOnlyBitmap()
{
    // normal posting can be readed by truncate
    TruncateParams param("", "", "title=ends:ends#DESC:ends::", /*inputStrategyType=*/"truncate_meta");
    ResetIndexPartitionSchema(param);
    SetHighFrequenceDictionary("title", true, hp_bitmap);

    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    CreateTruncateMeta("title_ends", "token1;token2", "4;3", "title", false);

    string docStr = "{ 11, "
                    "[title: (token1)],"
                    "[ends: (1)],"
                    "[userid: (1)],"
                    "[nid: (1)],"
                    "[url: (url1)],"
                    "};"
                    "{ 12, "
                    "[title: (token1)],"
                    "[ends: (5)],"
                    "[userid: (2)],"
                    "[nid: (2)],"
                    "[url: (url2)],"
                    "};";

    // build truncate index
    BuildTruncateIndex(mSchema, docStr, true);

    partition::IndexPartitionReaderPtr indexPartReader = CreatePartitionReader();
    std::shared_ptr<InvertedIndexReader> indexReader = indexPartReader->GetInvertedIndexReader("title");

    Term term(VOL_CONTENT, "title", "ends");
    std::shared_ptr<PostingIterator> postingIter(indexReader->Lookup(term, 1000, pt_default).ValueOrThrow());
    TermMeta* termMeta = postingIter->GetTermMeta();
    df_t docFreq = termMeta->GetDocFreq();
    INDEXLIB_TEST_EQUAL(2, docFreq);

    std::shared_ptr<PostingIterator> postingIterTrunc(indexReader->Lookup(term, 1000, pt_normal).ValueOrThrow());
    termMeta = postingIterTrunc->GetTruncateTermMeta();
    docFreq = termMeta->GetDocFreq();
    INDEXLIB_TEST_EQUAL(1, docFreq);
}

void TruncateIndexTest::TestCaseForTruncateMetaStrategy()
{
    TruncateParams param("", "", "title=ends:ends#DESC:ends::", /*inputStrategyType=*/"truncate_meta");
    ResetIndexPartitionSchema(param);

    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    CreateTruncateMeta("title_ends", "token1;token2", "4;3");

    // build truncate index
    BuildTruncateIndex(mSchema, mRawDocStr);

    string expectedDocStr = "{ 3, "
                            "[title: (not_used token2 token3)],"
                            "[ends: (3)],"
                            "[userid: (3)],"
                            "[nid: (3)],"
                            "};"
                            "{ 4, "
                            "[title: (token1 token2 token3 token4)],"
                            "[ends: (4)],"
                            "[userid: (4)],"
                            "[nid: (4)],"
                            "};"
                            "{ 5, "
                            "[title: (token1 token2 token3 token4 token5)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (6)],"
                            "};"
                            "{ 5, "
                            "[title: (token1 token3 token5)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (5)],"
                            "};"
                            "{ 4, "
                            "[title: (token2 token4)],"
                            "[ends: (4)],"
                            "[userid: (4)],"
                            "[nid: (7)],"
                            "};";
    CheckTruncateIndex(expectedDocStr, "2;3;4;5;6", "token1;token2", "title_ends");
}

void TruncateIndexTest::TestCaseForTruncateByDocPayloadMetaStrategy()
{
    TruncateParams param("", "", "title=ends:DOC_PAYLOAD#DESC:DOC_PAYLOAD::", /*inputStrategyType=*/"truncate_meta",
                         /*inputTruncateProfile2PayloadNames=*/"ends:unused_payload_name");
    ResetIndexPartitionSchema(param);

    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    CreateTruncateMeta("title_ends", "token1;token2", "4;3");

    // build truncate index
    BuildTruncateIndex(mSchema, mRawDocStr);

    string expectedDocStr = "{ 3, "
                            "[title: (not_used token2 token3)],"
                            "[ends: (3)],"
                            "[userid: (3)],"
                            "[nid: (3)],"
                            "};"
                            "{ 4, "
                            "[title: (token1 token2 token3 token4)],"
                            "[ends: (4)],"
                            "[userid: (4)],"
                            "[nid: (4)],"
                            "};"
                            "{ 5, "
                            "[title: (token1 token2 token3 token4 token5)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (6)],"
                            "};"
                            "{ 5, "
                            "[title: (token1 token3 token5)],"
                            "[ends: (5)],"
                            "[userid: (5)],"
                            "[nid: (5)],"
                            "};"
                            "{ 4, "
                            "[title: (token2 token4)],"
                            "[ends: (4)],"
                            "[userid: (4)],"
                            "[nid: (7)],"
                            "};";
    CheckTruncateIndex(expectedDocStr, "2;3;4;5;6", "token1;token2", "title_ends");
}

void TruncateIndexTest::TestCaseForTruncateMetaStrategyWithNoDoc()
{
    TruncateParams param("", "", "title=ends:ends#DESC:ends::", /*inputStrategyType=*/"truncate_meta");
    ResetIndexPartitionSchema(param);

    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    CreateTruncateMeta("title_ends", "token1;token2", "10;15");

    // build truncate index
    BuildTruncateIndex(mSchema, mRawDocStr);

    // token1 and token2 do not exist in truncate index
    partition::IndexPartitionReaderPtr indexPartReader = CreatePartitionReader();
    std::shared_ptr<InvertedIndexReader> indexReader = indexPartReader->GetInvertedIndexReader();
    Term term("token1", "title_ends");
    std::shared_ptr<PostingIterator> postingIter(indexReader->Lookup(term).ValueOrThrow());
    INDEXLIB_TEST_TRUE(!postingIter);

    Term term2("token2", "title_ends");
    postingIter.reset(indexReader->Lookup(term2).ValueOrThrow());
    INDEXLIB_TEST_TRUE(!postingIter);
}

// for bug #230304
void TruncateIndexTest::TestCaseForTruncateWithEmptySegment()
{
    TruncateParams param("", "", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");
    ResetIndexPartitionSchema(param);

    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    DocumentMaker::DocumentVect docVect;
    MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(mRawDocStr, mSchema, docVect, mockIndexPart);

    // build segment_0
    partition::IndexBuilder indexBuilder(mRootDir, mOptions, mSchema, mQuotaControl,
                                         BuilderBranchHinter::Option::Test());
    INDEXLIB_TEST_TRUE(indexBuilder.Init());
    for (size_t i = 0; i < docVect.size(); ++i) {
        indexBuilder.DoBuildSingleDocument(docVect[i]);
    }
    indexBuilder.DumpSegment();

    // build segment_1, which only has delete docs
    DocumentPtr delDoc = DocumentMaker::MakeDeletionDocument("url1", 0);
    indexBuilder.DoBuildSingleDocument(delDoc);
    delDoc = DocumentMaker::MakeDeletionDocument("url4", 0);
    indexBuilder.DoBuildSingleDocument(delDoc);
    indexBuilder.EndIndex();
    indexBuilder.TEST_BranchFSMoveToMainRoad();
    // merge, generate truncate index
    indexBuilder.Merge(mOptions);

    string expectedDocStr = "{ 2, "
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
    CheckTruncateIndex(expectedDocStr, "0;1;2;3;4", "token1;token2;token3;token4;token5", "title_ends");
}

// for bug #233692
void TruncateIndexTest::TestCaseForInvalidConfig()
{
    TruncateParams param("", "", "title=ends:not_exist_field#DESC:ends::", /*inputStrategyType=*/"truncate_meta");

    ResetIndexPartitionSchema(param);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    partition::IndexBuilder indexBuilder(mRootDir, mOptions, mSchema, mQuotaControl,
                                         BuilderBranchHinter::Option::Test());
    INDEXLIB_TEST_TRUE(!indexBuilder.Init());
}

void TruncateIndexTest::TestCaseForTruncateWithBalanceTreeMerge()
{
    TruncateParams param("", "", "title=ends_filter::::,ends_filter2::::", /*inputStrategyType=*/"default");
    ResetIndexPartitionSchema(param);
    mOptions.GetMergeConfig().mergeStrategyStr = "balance_tree";
    mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
        "conflict-segment-number=3;base-doc-count=10240;max-doc-count=102400");

    string optionFile = GET_PRIVATE_TEST_DATA_PATH() + "/truncate_index_intetest_option.json";
    string optionStr = LoadOptionConfigString(optionFile);
    TruncateOptionConfigPtr truncateOptionConfig = TruncateConfigMaker::MakeConfigFromJson(optionStr);
    ASSERT_TRUE(truncateOptionConfig);
    mOptions.GetMergeConfig().truncateOptionConfig = truncateOptionConfig;

    DocumentMaker::DocumentVect docVect;
    MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(mRawDocStr, mSchema, docVect, mockIndexPart);

    partition::IndexBuilder indexBuilder(mRootDir, mOptions, mSchema, mQuotaControl,
                                         BuilderBranchHinter::Option::Test());
    INDEXLIB_TEST_TRUE(indexBuilder.Init());
    for (size_t i = 0; i < docVect.size(); ++i) {
        docVect[i]->SetTimestamp(4 * 1000 * 1000);
        indexBuilder.DoBuildSingleDocument(docVect[i]);
    }
    indexBuilder.EndIndex();
    indexBuilder.TEST_BranchFSMoveToMainRoad();
    indexBuilder.Merge(mOptions, false);

    string expectedDocStr = "{ 4, "
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
    CheckTruncateIndex(expectedDocStr, "3;4;5;6", "token1;token2;token3;token4;token5", "title_ends_filter");
}

void TruncateIndexTest::TestCaseForInvalidBeginTime()
{
    TruncateParams param("", "", "title=ends_filter::::,ends_filter2::::", /*inputStrategyType=*/"default");
    ResetIndexPartitionSchema(param);
    mOptions.GetMergeConfig().mergeStrategyStr = "balance_tree";
    mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
        "conflict-segment-number=3;base-doc-count=10240;max-doc-count=102400");
    string optionFile = GET_PRIVATE_TEST_DATA_PATH() + "/truncate_index_intetest_option.json";
    string optionStr = LoadOptionConfigString(optionFile);
    TruncateOptionConfigPtr truncateOptionConfig = TruncateConfigMaker::MakeConfigFromJson(optionStr);
    ASSERT_TRUE(truncateOptionConfig);
    mOptions.GetMergeConfig().truncateOptionConfig = truncateOptionConfig;

    DocumentMaker::DocumentVect docVect;
    MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(mRawDocStr, mSchema, docVect, mockIndexPart);

    partition::IndexBuilder indexBuilder(mRootDir, mOptions, mSchema, mQuotaControl,
                                         BuilderBranchHinter::Option::Test());
    INDEXLIB_TEST_TRUE(indexBuilder.Init());
    for (size_t i = 0; i < docVect.size(); ++i) {
        docVect[i]->SetTimestamp(-1);
        indexBuilder.DoBuildSingleDocument(docVect[i]);
    }
    indexBuilder.EndIndex();
    indexBuilder.TEST_BranchFSMoveToMainRoad();
    ASSERT_THROW(indexBuilder.Merge(mOptions, false), BadParameterException);
}

void TruncateIndexTest::TestCaseForCheckTruncateMeta()
{
    TruncateParams param("3:2", "userid:1:3", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");

    ResetIndexPartitionSchema(param);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    // build index
    BuildTruncateIndex(mSchema, mRawDocStr);
    std::map<dictkey_t, int> mapMeta;
    GetMetaFromMetafile(mapMeta, "title_ends");
    CheckTruncateMeta("token1;token2;token3", "5;4;5", mapMeta);

    DeployIndexChecker::CheckDeployIndexMeta(GET_TEMP_DATA_PATH(), 0, INVALID_VERSIONID);
    DeployIndexChecker::CheckDeployIndexMeta(GET_TEMP_DATA_PATH(), 1, INVALID_VERSIONID);
    DeployIndexChecker::CheckDeployIndexMeta(
        GET_TEMP_DATA_PATH(), 1, 0,
        {"adaptive_bitmap_meta/", "adaptive_bitmap_meta/deploy_index", "adaptive_bitmap_meta/segment_file_list",
         "truncate_meta/", "truncate_meta/deploy_index", "truncate_meta/segment_file_list",
         "truncate_meta/deploy_index", "truncate_meta/index.mapper", "truncate_meta/title_ends"});
}

void TruncateIndexTest::TestCaseForCheckTruncateMetaWithDiffSortValue()
{
    TruncateParams param1("3:2", "userid:2:4", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");

    ResetIndexPartitionSchema(param1);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param1);
    std::string docStr = "{ 1, "
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
                         "[ends: (6)],"
                         "[userid: (5)],"
                         "[nid: (5)],"
                         "[url: (url6)],"
                         "};"
                         "{ 4, "
                         "[title: (token1 token2 token4)],"
                         "[ends: (7)],"
                         "[userid: (4)],"
                         "[nid: (7)],"
                         "[url: (url7)],"
                         "};"
                         "{ 6, "
                         "[title: (token2 token3 token4)],"
                         "[ends: (8)],"
                         "[userid: (4)],"
                         "[nid: (8)],"
                         "[url: (url8)],"
                         "};"
                         "{ 7, "
                         "[title: (token2 token4 token5)],"
                         "[ends: (9)],"
                         "[userid: (4)],"
                         "[nid: (7)],"
                         "[url: (url9)],"
                         "};";

    // build index
    BuildTruncateIndex(mSchema, docStr);
    std::map<dictkey_t, int> mapMeta;
    GetMetaFromMetafile(mapMeta, "title_ends");
    CheckTruncateMeta("token1;token2;token3;token4", "6;5;6;5", mapMeta);
}

void TruncateIndexTest::TestCaseForReTruncate()
{
    TruncateParams param1("4:3", "userid:2:4", "title=ends:ends#DESC:::", /*inputStrategyType=*/"default");
    param1.reTruncateLimit = 4;
    param1.reTruncateDistincStr = "userid:3:5";
    param1.reTruncateFilterStr = "ends";

    ResetIndexPartitionSchema(param1);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param1);
    std::string docStr = "{ 1, "
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
                         "[ends: (6)],"
                         "[userid: (5)],"
                         "[nid: (5)],"
                         "[url: (url6)],"
                         "};"
                         "{ 4, "
                         "[title: (token1 token2 token4)],"
                         "[ends: (7)],"
                         "[userid: (4)],"
                         "[nid: (7)],"
                         "[url: (url7)],"
                         "};"
                         "{ 6, "
                         "[title: (token2 token3 token4)],"
                         "[ends: (8)],"
                         "[userid: (4)],"
                         "[nid: (8)],"
                         "[url: (url8)],"
                         "};"
                         "{ 7, "
                         "[title: (token2 token4 token5)],"
                         "[ends: (9)],"
                         "[userid: (4)],"
                         "[nid: (7)],"
                         "[url: (url9)],"
                         "};";

    std::string expectedDocStr = "{ 5, "
                                 "[title: (token1 token2 token3 token4 token5)],"
                                 "[ends: (5)],"
                                 "[userid: (5)],"
                                 "[nid: (6)],"
                                 "[url: (url5)],"
                                 "};"
                                 "{ 5, "
                                 "[title: (token1 token3 token5)],"
                                 "[ends: (6)],"
                                 "[userid: (5)],"
                                 "[nid: (5)],"
                                 "[url: (url6)],"
                                 "};"
                                 "{ 4, "
                                 "[title: (token1 token2 token4)],"
                                 "[ends: (7)],"
                                 "[userid: (4)],"
                                 "[nid: (7)],"
                                 "[url: (url7)],"
                                 "};"
                                 "{ 6, "
                                 "[title: (token2 token3 token4)],"
                                 "[ends: (8)],"
                                 "[userid: (4)],"
                                 "[nid: (8)],"
                                 "[url: (url8)],"
                                 "};"
                                 "{ 7, "
                                 "[title: (token2 token4 token5)],"
                                 "[ends: (9)],"
                                 "[userid: (4)],"
                                 "[nid: (7)],"
                                 "[url: (url9)],"
                                 "};";
    // build index
    BuildTruncateIndex(mSchema, docStr);
    std::map<dictkey_t, int> mapMeta;
    GetMetaFromMetafile(mapMeta, "title_ends");
    CheckTruncateMeta("token1;token2;token3;token4", "5;5;5;5", mapMeta);
    // token1: 6,5,4
    // token2: 8,7,6,4
    // token3: 7,5,4
    // token4: 8,7,6,4
    // 4,5,6,7,8
    CheckTruncateIndex(expectedDocStr, "4;5;6;7;8", "token1;token2;token3;token4", "title_ends");
}

string TruncateIndexTest::LoadOptionConfigString(const string& optionFile)
{
    ifstream in(optionFile.c_str());
    if (!in) {
        IE_LOG(ERROR, "OPEN INPUT FILE[%s] ERROR", optionFile.c_str());
        return "";
    }
    string line;
    string jsonString;
    while (getline(in, line)) {
        jsonString += line;
    }

    IE_LOG(DEBUG, "jsonString:%s", jsonString.c_str());
    return jsonString;
}

void TruncateIndexTest::InnerTestForTruncate(const string& docStr, const string& expecetedTruncDocStr,
                                             const string& expectedDocIds, const string& toCheckTokenStr,
                                             const TruncateParams& param, const string& truncIndexNameStr,
                                             bool hasHighFreq)
{
    tearDown();
    setUp();
    ResetIndexPartitionSchema(param);

    SetHighFrequenceDictionary("title", hasHighFreq, hp_both);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    // build index
    BuildTruncateIndex(mSchema, docStr);

    // check truncate index
    CheckTruncateIndex(expecetedTruncDocStr, expectedDocIds, toCheckTokenStr, truncIndexNameStr);
}

void TruncateIndexTest::InnerTestForTruncateWithTestSplit(const string& docStr, const string& expecetedTruncDocStr,
                                                          const string& expectedDocIds, const string& toCheckTokenStr,
                                                          const TruncateParams& param, const string& truncIndexNameStr,
                                                          bool hasHighFreq, int segmentCount)
{
    tearDown();
    setUp();
    ResetIndexPartitionSchema(param);
    std::string mergeConfigStr =
        "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"" + StringUtil::toString(segmentCount) + "\"}}";
    autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    SetHighFrequenceDictionary("title", hasHighFreq, hp_both);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    // build index
    BuildTruncateIndex(mSchema, docStr);

    // check truncate index
    CheckTruncateIndex(expecetedTruncDocStr, expectedDocIds, toCheckTokenStr, truncIndexNameStr);
}

void TruncateIndexTest::InnerTestForBitmapTruncate(const string& docStr, const string& expecetedTruncDocStr,
                                                   const string& expectedDocIds, const string& toCheckTokenStr,
                                                   const TruncateParams& param, const string& truncIndexNameStr,
                                                   const string& expectedHighFreqTermDocIds)
{
    tearDown();
    setUp();
    ResetIndexPartitionSchema(param);

    // high frequence term : only bitmap
    SetHighFrequenceDictionary("title", true, hp_bitmap);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    BuildTruncateIndex(mSchema, docStr);

    // check truncate index
    CheckTruncateIndex(expecetedTruncDocStr, expectedDocIds, toCheckTokenStr, truncIndexNameStr);

    CheckBitmapTruncateIndex(expectedHighFreqTermDocIds, truncIndexNameStr);
}

void TruncateIndexTest::InnerTestForBitmapTruncateWithTestSplit(
    const string& docStr, const string& expecetedTruncDocStr, const string& expectedDocIds,
    const string& toCheckTokenStr, const TruncateParams& param, const string& truncIndexNameStr,
    const string& expectedHighFreqTermDocIds, int segmentCount)
{
    tearDown();
    setUp();
    ResetIndexPartitionSchema(param);
    std::string mergeConfigStr =
        "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"" + StringUtil::toString(segmentCount) + "\"}}";
    autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    // high frequence term : only bitmap
    SetHighFrequenceDictionary("title", true, hp_bitmap);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);

    BuildTruncateIndex(mSchema, docStr);

    // check truncate index
    CheckTruncateIndex(expecetedTruncDocStr, expectedDocIds, toCheckTokenStr, truncIndexNameStr);

    CheckBitmapTruncateIndex(expectedHighFreqTermDocIds, truncIndexNameStr);
}

void TruncateIndexTest::BuildTruncateIndex(const IndexPartitionSchemaPtr& schema, const string& rawDocStr,
                                           bool needMerge)
{
    DocumentMaker::DocumentVect docVect;
    MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(rawDocStr, schema, docVect, mockIndexPart);

    // build index
    partition::IndexBuilder indexBuilder(mRootDir, mOptions, schema, mQuotaControl,
                                         BuilderBranchHinter::Option::Test());
    INDEXLIB_TEST_TRUE(indexBuilder.Init());

    for (size_t i = 0; i < docVect.size(); ++i) {
        indexBuilder.DoBuildSingleDocument(docVect[i]);
    }
    indexBuilder.EndIndex();
    indexBuilder.TEST_BranchFSMoveToMainRoad();
    if (needMerge) {
        string truncateMetaDir = GET_TEMP_DATA_PATH("truncate_meta");
        if (FslibWrapper::IsExist(truncateMetaDir).GetOrThrow()) {
            FileSystemOptions fsOptions;
            fsOptions.isOffline = true;
            auto fs = FileSystemCreator::Create("TruncateIndexTest", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
            fs->TEST_MountLastVersion();
            ASSERT_EQ(FSEC_OK,
                      fs->MountDir(GET_TEMP_DATA_PATH(), "truncate_meta", "truncate_meta", FSMT_READ_ONLY, true));
            Version lastVersion;
            VersionLoader::GetVersionS(GET_TEMP_DATA_PATH(), lastVersion, -1);
            Version newVersion = lastVersion;
            newVersion.IncVersionId();
            DirectoryPtr rootDir = Directory::Get(fs);
            VersionCommitter versionCommitter(rootDir, newVersion, 999);
            versionCommitter.Commit();
        }
        indexBuilder.Merge(mOptions);
    }
}

void TruncateIndexTest::CheckTruncateIndex(const string& expectedTrucDocStr, const string& expectedDocIds,
                                           const string& toCheckTokenStr, const string& indexNameStr)
{
    partition::OnlinePartition indexPart;
    indexPart.Open(mRootDir, "", IndexPartitionSchemaPtr(), mOptions);
    std::shared_ptr<InvertedIndexReader> indexReader = indexPart.GetReader()->GetInvertedIndexReader();
    mSchema = indexPart.GetSchema();

    DocumentMaker::DocumentVect docVect;
    MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(expectedTrucDocStr, mSchema, docVect, mockIndexPart, expectedDocIds);

    uint32_t docCount = mockIndexPart.GetDocCount();
    INDEXLIB_TEST_EQUAL(docCount, (uint32_t)docVect.size());

    vector<string> indexNames;
    StringUtil::fromString(indexNameStr, indexNames, ";");
    INDEXLIB_TEST_TRUE(indexNames.size() > 0);

    vector<string> toCheckTokens;
    StringUtil::fromString(toCheckTokenStr, toCheckTokens, ";");

    DocumentCheckerForGtest checker;
    for (size_t i = 0; i < indexNames.size(); i++) {
        if (docCount > 0) {
            checker.CheckIndexData(indexReader, mSchema->GetIndexSchema(), indexNames[i], mockIndexPart, toCheckTokens);
        } else {
            DirectoryPtr segmentDir = test::DirectoryCreator::Create(PathUtil::JoinPath(mRootDir, "segment_1_level_0"));
            // check dictionary and posting files
            DirectoryPtr indexDir = segmentDir->GetDirectory(PathUtil::JoinPath("index", indexNames[i]), false);
            if (indexDir) {
                ASSERT_FALSE(indexDir->IsExist(DICTIONARY_FILE_NAME));
                ASSERT_FALSE(indexDir->IsExist(POSTING_FILE_NAME));
            }
        }
    }
}

partition::IndexPartitionReaderPtr TruncateIndexTest::CreatePartitionReader()
{
    RESET_FILE_SYSTEM("ut", false);
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadAndRewritePartitionSchema(GET_PARTITION_DIRECTORY(), mOptions);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    auto memoryController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    util::MetricProviderPtr metricProvider;
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    BuildingPartitionParam param(mOptions, schema, memoryController, container, counterMap, pluginManager,
                                 metricProvider, document::SrcSignature());
    PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(param, GET_FILE_SYSTEM());
    partition::IndexPartitionReaderPtr reader(
        new partition::OnlinePartitionReader(mOptions, schema, util::SearchCachePartitionWrapperPtr()));
    reader->Open(partitionData);
    return reader;
}

void TruncateIndexTest::CheckBitmapTruncateIndex(const string& expectedDocIds, const string& indexNameStr)
{
    partition::IndexPartitionReaderPtr reader = CreatePartitionReader();
    std::shared_ptr<InvertedIndexReader> indexReader = reader->GetInvertedIndexReader();
    vector<docid_t> docIds;
    autil::StringUtil::fromString(expectedDocIds, docIds, ";");

    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig(indexNameStr);
    assert(indexConfig);

    Term term(VOL_CONTENT, indexNameStr);
    std::shared_ptr<PostingIterator> postingIter(indexReader->Lookup(term).ValueOrThrow());
    if (docIds.size() == 0) {
        INDEXLIB_TEST_TRUE(postingIter == NULL);
        return;
    } else {
        INDEXLIB_TEST_TRUE(postingIter != NULL);
    }

    size_t count = 0;
    docid_t docId = INVALID_DOCID;
    while ((docId = postingIter->SeekDoc(docId)) != INVALID_DOCID) {
        INDEXLIB_TEST_EQUAL(docId, docIds[count++]);
        // default bitmap doc payload is 0
        INDEXLIB_TEST_EQUAL((docpayload_t)0, postingIter->GetDocPayload());
        if (!postingIter->HasPosition()) {
            continue;
        }

        TermMatchData termMatchData;
        postingIter->Unpack(termMatchData);
        // default bitmap truncate tf is 1
        INDEXLIB_TEST_EQUAL((tf_t)1, termMatchData.GetTermFreq());

        if (indexConfig->GetInvertedIndexType() == it_expack) {
            INDEXLIB_TEST_EQUAL((fieldmap_t)1, termMatchData.GetFieldMap());
        }

        std::shared_ptr<InDocPositionIterator> positionIter = termMatchData.GetInDocPositionIterator();
        pos_t pos = 0;
        pos = positionIter->SeekPosition(pos);
        // default bitmap truncate position info : pos 0, pos_payload 0
        INDEXLIB_TEST_EQUAL((pos_t)0, pos);
        INDEXLIB_TEST_EQUAL((pospayload_t)0, positionIter->GetPosPayload());
        INDEXLIB_TEST_EQUAL(INVALID_POSITION, positionIter->SeekPosition(pos));
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

    if (hasHighFreq) {
        std::shared_ptr<DictionaryConfig> dictConfig = dictSchema->GetDictionaryConfig(VOL_NAME);
        assert(dictConfig != NULL);
        indexConfig->SetDictConfig(dictConfig);
        indexConfig->SetHighFreqencyTermPostingType(type);
    } else {
        indexConfig->SetDictConfig(std::shared_ptr<DictionaryConfig>());
    }
}

void TruncateIndexTest::ResetIndexPartitionSchema(const TruncateParams& param)
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
    TruncateConfigMaker::RewriteSchema(mSchema, param);
}

void TruncateIndexTest::CreateTruncateMeta(const string& truncateName, const string& tokenStr, const string& valueStr,
                                           const string& indexName, bool useArchiveFolder)
{
    // make truncate meta
    string truncateMetaDir = util::PathUtil::JoinPath(mRootDir, TRUNCATE_META_DIR_NAME);
    FslibWrapper::DeleteDirE(truncateMetaDir, DeleteOption::NoFence(true));
    FslibWrapper::MkDirE(truncateMetaDir);

    file_system::DirectoryPtr truncateMetaDirectory = GET_PARTITION_DIRECTORY()->MakeDirectory(TRUNCATE_META_DIR_NAME);
    ASSERT_TRUE(truncateMetaDirectory);

    ArchiveFolderPtr archiveFolder(new ArchiveFolder(!useArchiveFolder));
    ASSERT_EQ(FSEC_OK, archiveFolder->Open(truncateMetaDirectory->GetIDirectory(), ""));
    vector<string> tokens;
    vector<int64_t> values;

    StringUtil::fromString(tokenStr, tokens, ";");
    StringUtil::fromString(valueStr, values, ";");
    INDEXLIB_TEST_TRUE(tokens.size() == values.size());

    auto writer = archiveFolder->CreateFileWriter(truncateName).GetOrThrow();
    for (size_t i = 0; i < tokens.size(); i++) {
        dictkey_t key = autil::HashAlgorithm::hashString64(tokens[i].c_str());
        string keyStr = StringUtil::toString(key);
        string valueStr = StringUtil::toString(values[i]);
        string line = keyStr + "\t" + valueStr + "\n";
        writer->Write(line.c_str(), line.size()).GetOrThrow();
    }
    ASSERT_EQ(FSEC_OK, writer->Close());
    writer.reset();
    if (archiveFolder) {
        ASSERT_EQ(FSEC_OK, archiveFolder->Close());
    }

    if (indexName != "") {
        TruncateIndexNameMapper truncMapper(truncateMetaDirectory);
        vector<string> truncateIndexNames;
        truncateIndexNames.push_back(truncateName);
        truncMapper.AddItem(indexName, truncateIndexNames);
        truncMapper.DoDump();
    }
    DeployIndexWrapper::DumpTruncateMetaIndex(GET_PARTITION_DIRECTORY());
}

void TruncateIndexTest::GetMetaFromMetafile(std::map<dictkey_t, int>& mapMeta, std::string truncate_index_name)
{
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr truncateMetaDirectory = GET_PARTITION_DIRECTORY()->GetDirectory(TRUNCATE_META_DIR_NAME, true);
    ArchiveFolderPtr archiveFolder(new ArchiveFolder(true));
    ASSERT_EQ(FSEC_OK, archiveFolder->Open(truncateMetaDirectory->GetIDirectory(), "0").Code());
    // string metaFile = util::PathUtil::JoinPath(truncateMetaDir,
    //         truncate_index_name);
    // INDEXLIB_TEST_TRUE(FslibWrapper::IsExist(metaFile).GetOrThrow());
    auto metaFile = archiveFolder->CreateFileReader(truncate_index_name).GetOrThrow();
    ASSERT_TRUE(metaFile);
    LineReader reader;
    reader.Open(metaFile);
    string line;
    dictkey_t key;
    int64_t value;
    file_system::ErrorCode ec;
    while (reader.NextLine(line, ec)) {
        vector<string> strVec = StringUtil::split(line, "\t");
        INDEXLIB_TEST_EQUAL(strVec.size(), (size_t)2);
        StringUtil::fromString(strVec[0], key);
        StringUtil::fromString(strVec[1], value);
        mapMeta[key] = value;
        line.clear();
    }
    ASSERT_EQ(FSEC_OK, ec);
    ASSERT_EQ(FSEC_OK, archiveFolder->Close());
}

void TruncateIndexTest::CheckTruncateMeta(std::string keys, std::string values, const std::map<dictkey_t, int>& mapMeta)
{
    vector<std::string> keyVec;
    vector<int> valueVec;

    StringUtil::fromString(keys, keyVec, ";");
    StringUtil::fromString(values, valueVec, ";");
    INDEXLIB_TEST_EQUAL(keyVec.size(), keyVec.size());
    INDEXLIB_TEST_EQUAL(mapMeta.size(), valueVec.size());
    std::map<dictkey_t, int>::const_iterator ite;
    for (size_t i = 0; i < keyVec.size(); ++i) {
        dictkey_t key = autil::HashAlgorithm::hashString64(keyVec[i].c_str());
        ite = mapMeta.find(key);
        INDEXLIB_TEST_TRUE(ite != mapMeta.end());
        INDEXLIB_TEST_EQUAL(ite->second, valueVec[i]) << i;
    }
}
} // namespace indexlib::index::legacy
