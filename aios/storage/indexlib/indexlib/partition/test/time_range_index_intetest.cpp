#include "indexlib/partition/test/time_range_index_intetest.h"

#include "indexlib/config/date_index_config.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/common/NumberTerm.h"
#include "indexlib/index/inverted_index/DocValueFilter.h"
#include "indexlib/index/inverted_index/SeekAndFilterIterator.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/atomic_query.h"
#include "indexlib/test/document_parser.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/searcher.h"
#include "indexlib/test/slow_dump_segment_container.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlibv2::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, TimeRangeIndexInteTest);

std::string GetQueryResultString(uint64_t startPk, uint64_t endPk)
{
    string resultString;
    for (auto i = startPk; i <= endPk; i++) {
        resultString += "pk=" + StringUtil::toString(i) + ";";
    }
    return resultString;
}

TimeRangeIndexInteTest::TimeRangeIndexInteTest() {}

TimeRangeIndexInteTest::~TimeRangeIndexInteTest() {}

void TimeRangeIndexInteTest::CaseSetUp()
{
    string field = "time:uint64;pk:string";
    string index = "time_index:date:time;pk:primarykey64:pk";
    string attribute = "time;pk";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    bool enableFileCompress = GET_PARAM_VALUE(0);
    if (enableFileCompress) {
        string compressorStr = "compressor:zstd;";
        string indexCompressStr = "time_index:compressor";
        string attrCompressStr = "time:compressor";
        mSchema = SchemaMaker::EnableFileCompressSchema(mSchema, compressorStr, indexCompressStr, attrCompressStr);
    }

    mRootDir = GET_TEMP_DATA_PATH();
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GET_PARAM_VALUE(1), &mOptions);
}

void TimeRangeIndexInteTest::CaseTearDown() {}

void TimeRangeIndexInteTest::TestBug23953311()
{
    // test date range loss some doc
    string fullDocString = "cmd=add,time=1516086342000,pk=a,ts=1;"
                           "cmd=add,time=1516096346000,pk=b,ts=1";
    string rtDocString = "cmd=add,time=1016086341000,pk=e,ts=4;"
                         "cmd=add,time=1616086342000,pk=f,ts=4";
    DateIndexConfigPtr dateConfig =
        DYNAMIC_POINTER_CAST(DateIndexConfig, mSchema->GetIndexSchema()->GetIndexConfig("time_index"));
    DateLevelFormat format;
    format.Init(DateLevelFormat::MILLISECOND);
    dateConfig->SetDateLevelFormat(format);
    dateConfig->SetBuildGranularity(DateLevelFormat::MILLISECOND);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "time_index:[1516076342000, 1516096346000]",
                                    "docid=0,pk=a;docid=1,pk=b"));
    CheckTimeRangeIndexCompressInfo(psm.GetFileSystem());
    INDEXLIB_TEST_TRUE(
        psm.Transfer(BUILD_RT, rtDocString, "time_index:[1516076342000, 1516096346000]", "docid=0,pk=a;docid=1,pk=b"));
}

void TimeRangeIndexInteTest::TestSimpleProcess()
{
    string fullDocString = "cmd=add,time=1516086342000,pk=a,ts=1;"
                           "cmd=add,time=1516096346000,pk=b,ts=1";
    string incDocString = "cmd=add,time=1516086340000,pk=c,ts=2;"
                          "cmd=add,time=1516076342000,pk=d,ts=2";
    string incDocString2 = "cmd=add,time=1516903678901,pk=g,ts=3;"
                           "cmd=add,time=1500001200000,pk=k,ts=3;";
    string rtDocString = "cmd=add,time=1516086341000,pk=e,ts=4;"
                         "cmd=add,time=1516086342000,pk=f,ts=4";
    string rtDocString2 = "cmd=add,time=1506903678901,pk=g,ts=5";

    DateIndexConfigPtr dateConfig =
        DYNAMIC_POINTER_CAST(DateIndexConfig, mSchema->GetIndexSchema()->GetIndexConfig("time_index"));
    {
        tearDown();
        setUp();
        DateLevelFormat format;
        format.Init(DateLevelFormat::MILLISECOND);
        dateConfig->SetDateLevelFormat(format);
        dateConfig->SetBuildGranularity(DateLevelFormat::MILLISECOND);
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_FULL, fullDocString, "time_index:[1516076342000, 1516096342000]", "docid=0,pk=a;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString, "time_index:[1516076342000, 1516096342000]",
                                        "docid=0,pk=a;docid=2,pk=c;docid=3,pk=d"));
        CheckTimeRangeIndexCompressInfo(psm.GetFileSystem());
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_INC, incDocString2, "time_index:[1500001000000,1500001200001)", "docid=5,pk=k;"));
        // 1516086340000,1516086341000
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "time_index:[1516086340000, 1516086342000)",
                                        "docid=2,pk=c;docid=6,pk=e;"));
        // test query illegal
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "", "time_index:[1516186340000, 1516086342000)", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "", "time_index:[1516086340000, 151618abc42000)", ""));
    }
    {
        tearDown();
        setUp();
        DateLevelFormat format;
        format.Init(DateLevelFormat::MINUTE);
        dateConfig->SetDateLevelFormat(format);
        dateConfig->SetBuildGranularity(DateLevelFormat::MINUTE);
        PartitionStateMachine psm;
        fullDocString = "cmd=add,time=1516096320000,pk=b,ts=1";
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_FULL, fullDocString, "time_index:[1516076340000,1516096380000)", "docid=0,pk=b"));
        CheckTimeRangeIndexCompressInfo(psm.GetFileSystem());
    }
}

void TimeRangeIndexInteTest::TestAddEmptyFieldDoc()
{
    string fullDocString = "cmd=add,pk=a,ts=1;"
                           "cmd=add,time=1516096346000,pk=b,ts=1";
    string incDocString = "cmd=add,time=1516086340000,pk=c,ts=2;"
                          "cmd=add,time=1516076342000,pk=d,ts=2";
    string incDocString1 = "cmd=add,pk=e,ts=1";
    string incDocString2 = "cmd=delete,pk=a;cmd=delete,pk=b;"
                           "cmd=delete,pk=c;cmd=delete,pk=d;"
                           "cmd=delete,pk=e";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(
        psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "time_index:[1516076342000, 1516096342000]", ""));
    CheckTimeRangeIndexCompressInfo(psm.GetFileSystem());
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString, "time_index:[1516076342000, 1516096342000]",
                                    "docid=2,pk=c;docid=3,pk=d"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString1, "time_index:[1516076342000, 1516096342000]",
                                    "docid=2,pk=c;docid=3,pk=d"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString2, "time_index:[1516076342000, 1516096342000]", ""));
}

void TimeRangeIndexInteTest::TestRtDocCoverFullDoc()
{
    DateIndexConfigPtr dateConfig =
        DYNAMIC_POINTER_CAST(DateIndexConfig, mSchema->GetIndexSchema()->GetIndexConfig("time_index"));
    DateLevelFormat format;
    format.Init(DateLevelFormat::MILLISECOND);
    dateConfig->SetDateLevelFormat(format);
    dateConfig->SetBuildGranularity(DateLevelFormat::MILLISECOND);

    string fullDocString = "cmd=add,time=1506903500000,pk=20,ts=1;"
                           "cmd=add,time=1500001200000,pk=30,ts=1;";
    string rtDocString = "cmd=add,time=1506903500000,pk=1,ts=2;"
                         "cmd=add,time=1506903500000,pk=2,ts=2;"
                         "cmd=add,time=1514764800000,pk=3,ts=2;"
                         "cmd=add,time=1514764800000,pk=4,ts=2;"
                         "cmd=add,time=1514764799999,pk=5,ts=2;"
                         "cmd=add,time=1514764800001,pk=6,ts=2;";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(
        psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "time_index:[1500001000000,1500001200001]", "docid=1,pk=30;"));
    CheckTimeRangeIndexCompressInfo(psm.GetFileSystem());
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "time_index:[1506903500000,1506903500000]",
                                    "docid=0,pk=20;"
                                    "docid=2,pk=1;"
                                    "docid=3,pk=2;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[1514764799999,1514764800001]",
                                    "docid=4,pk=3;docid=5,pk=4;"
                                    "docid=6,pk=5;docid=7,pk=6;"));
}

void TimeRangeIndexInteTest::TestAbnormalInput()
{
    // one doc overflow time limit, one normal doc, one doc no time field, one time = 0 doc
    string fullDocString = "cmd=add,time=21506903500000,pk=1,ts=1;"
                           "cmd=add,time=1500001200000,pk=2,ts=1;"
                           "cmd=add,pk=3,ts=1;cmd=add,pk=4,time=0,ts=1;"
                           "cmd=add,time=4102444800000,pk=5,ts=1;";
    string rtDocString = "cmd=add,time=23506903500000,pk=6,ts=2;"
                         "cmd=add,time=1506903500000,pk=7,ts=2;"
                         "cmd=add,pk=8,ts=2;cmd=add,pk=9,time=0,ts=2;"
                         "cmd=add,time=4102444800000,pk=10,ts=2;";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "time_index:[1500001000000,1500001200001]", "pk=2;"));
    CheckTimeRangeIndexCompressInfo(psm.GetFileSystem());
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "time_index:[1500001000000,1600001200001]", "pk=2;pk=7"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[0,101506903500000]",
                                    "pk=1;pk=2;pk=4;pk=5;"
                                    "pk=6;pk=7;pk=9;pk=10"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(0,101506903500000]",
                                    "pk=1;pk=2;pk=5;"
                                    "pk=6;pk=7;pk=10"));

    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(0,4102444800000]",
                                    "pk=1;pk=2;pk=5;"
                                    "pk=6;pk=7;pk=10"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(0,4102444800000)", "pk=2;pk=7"));
}

void TimeRangeIndexInteTest::TestLookupBuildingSegmentWithAsyncDump()
{
    DateIndexConfigPtr dateConfig =
        DYNAMIC_POINTER_CAST(DateIndexConfig, mSchema->GetIndexSchema()->GetIndexConfig("time_index"));
    DateLevelFormat format;
    format.Init(DateLevelFormat::YEAR);
    dateConfig->SetDateLevelFormat(format);
    dateConfig->SetBuildGranularity(DateLevelFormat::YEAR);

    // 599616000000: 1989-01-01 00:00:00
    // 915148800000: 1999-01-01 00:00:00
    // 946684800000: 2000-01-01 00:00:00
    // 631152000000: 1990-01-01 00:00:00
    string rtDocString1 = "cmd=add,time=599616000000,pk=1,ts=1;"
                          "cmd=add,time=599616000001,pk=2,ts=1;"
                          "cmd=add,time=915148800000,pk=3,ts=1;"
                          "cmd=add,time=915148800002,pk=4,ts=1;"
                          "cmd=add,time=946684800000,pk=11,ts=1;"
                          "cmd=add,time=631152000000,pk=5,ts=1;";

    string rtDocString2 = "cmd=add,time=599616000000,pk=6,ts=2;"
                          "cmd=add,time=599616000001,pk=7,ts=2;"
                          "cmd=add,time=915148800000,pk=8,ts=2;"
                          "cmd=add,time=915148800001,pk=9,ts=2;"
                          "cmd=add,time=631152000000,pk=10,ts=2;";

    SlowDumpSegmentContainerPtr dumpContainer(new SlowDumpSegmentContainer(1000));
    PartitionStateMachine psm(false, partition::IndexPartitionResource(), dumpContainer);
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(
        psm.Transfer(BUILD_RT_SEGMENT, rtDocString1, "time_index:[599616000001,915148800001]", "pk=3;pk=4;pk=5"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString2, "time_index:[599616000001,915148800001]",
                                    "pk=3;pk=4;pk=5;pk=8;pk=9;pk=10"));
    ASSERT_EQ(1u, dumpContainer->GetDumpItemSize());
    dumpContainer->WaitEmpty();
    ASSERT_EQ(0u, dumpContainer->GetDumpItemSize());
    INDEXLIB_TEST_TRUE(
        psm.Transfer(QUERY, "", "time_index:[599616000001,915148800001]", "pk=3;pk=4;pk=5;pk=8;pk=9;pk=10"));
}

void TimeRangeIndexInteTest::TestOpenRangeQuery()
{
    DateIndexConfigPtr dateConfig =
        DYNAMIC_POINTER_CAST(DateIndexConfig, mSchema->GetIndexSchema()->GetIndexConfig("time_index"));
    DateLevelFormat format;
    format.Init(DateLevelFormat::YEAR);
    dateConfig->SetDateLevelFormat(format);
    dateConfig->SetBuildGranularity(DateLevelFormat::YEAR);

    // 599616000000: 1989-01-01 00:00:00
    // 915148800000: 1999-01-01 00:00:00
    // 946684800000: 2000-01-01 00:00:00
    // 631152000000: 1990-01-01 00:00:00
    string fullDocString = "cmd=add,time=599616000000,pk=1,ts=1;"
                           "cmd=add,time=599616000001,pk=2,ts=1;"
                           "cmd=add,time=915148800000,pk=3,ts=1;"
                           "cmd=add,time=915148800002,pk=4,ts=1;"
                           "cmd=add,time=946684800000,pk=11,ts=1;"
                           "cmd=add,time=631152000000,pk=5,ts=1;";

    string rtDocString = "cmd=add,time=599616000000,pk=6,ts=2;"
                         "cmd=add,time=599616000001,pk=7,ts=2;"
                         "cmd=add,time=915148800000,pk=8,ts=2;"
                         "cmd=add,time=915148800001,pk=9,ts=2;"
                         "cmd=add,time=631152000000,pk=10,ts=2;";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "time_index:(599616000000,915148800000)", "pk=5"));
    CheckTimeRangeIndexCompressInfo(psm.GetFileSystem());
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[599616000001,915148800001]", "pk=3;pk=4;pk=5"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(599615999999,946684800000)", "pk=1;pk=2;pk=3;pk=4;pk=5"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "time_index:(599616000000,915148800000)", "pk=5;pk=10"));
    INDEXLIB_TEST_TRUE(
        psm.Transfer(QUERY, "", "time_index:[599616000001,915148800001]", "pk=3;pk=4;pk=5;pk=8;pk=9;pk=10"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(599615999999,946684800000)",
                                    "pk=1;pk=2;pk=3;pk=4;pk=5;"
                                    "pk=6;pk=7;pk=8;pk=9;pk=10"));
}

const string dictInlineResult = "pk=0";
const string dictInlineQuery = "time_index:[1500001200000,1500001200000]";
const string shortListResult = GetQueryResultString(10002, 10006);
const string shortListQuery = "time_index:[1516090000000,1516090005000]";
const string allQuery = "time_index:[1500001200000,)";
// Note: GetRangeDocString has status, means call order is important
void TimeRangeIndexInteTest::TestDecoderDoc_1()
{
    string skipListQuery = "time_index:[1516076342000,1516086342000)";
    string dictInlineDoc = GetRangeDocString(1500001200000LL, 1500001200000LL, 1000, 0);
    string skipListDoc = GetRangeDocString(1516076342000LL, 1516086342000LL, 1000);
    string skipListResult = GetQueryResultString(1, 10000);
    string shortListDoc = GetRangeDocString(1516090000000LL, 1516090004000LL, 1000, 10002);
    InnerTestDecoderDoc(dictInlineDoc, dictInlineQuery, dictInlineResult, skipListDoc, skipListQuery, skipListResult,
                        shortListDoc, shortListQuery, shortListResult);
}

void TimeRangeIndexInteTest::TestDecoderDoc_2()
{
    string skipListQuery = "time_index:[1516076342000,1516086342000)";
    string skipListResult = GetQueryResultString(1, 10000);
    string shortListDoc = GetRangeDocString(1516090000000LL, 1516090004000LL, 1000, 10002);
    string dictInlineDoc = GetRangeDocString(1500001200000LL, 1500001200000LL, 1000, 0);
    string skipListDoc = GetRangeDocString(1516076342000LL, 1516086342000LL, 1000);
    InnerTestDecoderDoc(shortListDoc, shortListQuery, shortListResult, dictInlineDoc, dictInlineQuery, dictInlineResult,
                        skipListDoc, skipListQuery, skipListResult);
}

void TimeRangeIndexInteTest::TestDecoderDoc_3()
{
    string skipListQuery = "time_index:[1516076342000,1516086342000]";
    string skipListDoc = GetRangeDocString(1516076342000LL, 1516086342000LL, 1000);
    string shortListDoc = GetRangeDocString(1516090000000LL, 1516090004000LL, 1000, 10002);
    string dictInlineDoc = GetRangeDocString(1500001200000LL, 1500001200000LL, 1000, 0);
    string skipListResult = GetQueryResultString(1, 10001);
    InnerTestDecoderDoc(skipListDoc, skipListQuery, skipListResult, shortListDoc, shortListQuery, shortListResult,
                        dictInlineDoc, allQuery, skipListResult + shortListResult + dictInlineResult);
}

void TimeRangeIndexInteTest::InnerTestDecoderDoc(const string& fullDoc, const string& fullQuery,
                                                 const string& fullResult, const string& incDoc, const string& incQuery,
                                                 const string& incResult, const string& rtDoc, const string& rtQuery,
                                                 const string& rtResult)
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, fullQuery, fullResult));
    CheckTimeRangeIndexCompressInfo(psm.GetFileSystem());
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDoc, incQuery, incResult));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDoc, rtQuery, rtResult));
}

string TimeRangeIndexInteTest::GetRangeDocString(uint64_t startTime, uint64_t endTime, uint64_t unit, uint64_t startPk)
{
    string incDocString;
    uint64_t gtime = GetGlobalTime();
    for (uint64_t i = startTime, j = startPk; i <= endTime; i += unit, j++) {
        incDocString += "cmd=add,time=" + StringUtil::toString(i) + ",pk=" + StringUtil::toString(j) +
                        ",ts=" + StringUtil::toString(gtime) + ";";
    }
    return incDocString;
}

uint64_t TimeRangeIndexInteTest::GetGlobalTime()
{
    static uint64_t curTime = 0;
    curTime++;
    return curTime;
}

void TimeRangeIndexInteTest::TestIterator()
{
    string skipListDoc = GetRangeDocString(1516076342000LL, 1516086342000LL, 1000);
    string skipListResult = GetQueryResultString(1, 10000);
    string skipListQuery = "time_index:[1516076342000,1516086342000)";
    string shortListDoc = GetRangeDocString(1516090000000LL, 1516090004000LL, 1000, 10002);
    string shortListResult = GetQueryResultString(10002, 10006);
    string shortListQuery = "time_index:[1516090000000,1516090005000]";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, skipListDoc, skipListQuery, skipListResult));
    CheckTimeRangeIndexCompressInfo(psm.GetFileSystem());
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, shortListDoc, shortListQuery, shortListResult));
    auto reader = psm.GetIndexPartition()->GetReader();

    vector<string> indexTerms = StringUtil::split(shortListQuery, ":");
    InvertedIndexReaderPtr indexReader = reader->GetInvertedIndexReader();
    index::legacy::MultiFieldIndexReaderPtr multiReader =
        DYNAMIC_POINTER_CAST(index::legacy::MultiFieldIndexReader, indexReader);
    indexReader = multiReader->GetInvertedIndexReader("time_index");
    index::Int64Term term(1516090000000, true, 1516090005000, true, "time_index");
    PostingIterator* iter = indexReader->Lookup(term).ValueOrThrow();
    TermMeta* meta = iter->GetTermMeta();
    ASSERT_EQ(5, meta->GetDocFreq());
    InnerTestIterator(iter->Clone(), reader, shortListResult);
    InnerTestIterator(iter, reader, shortListResult);
}

void TimeRangeIndexInteTest::InnerTestIterator(index::PostingIterator* iter, IndexPartitionReaderPtr reader,
                                               const std::string& resultString)
{
    AtomicQueryPtr query(new AtomicQuery);
    ResultPtr expectResult = DocumentParser::ParseResult(resultString);
    query->Init(iter, "time_index");
    IndexPartitionSchema* schema = mSchema.get();
    ASSERT_FALSE(query->IsSubQuery());

    Searcher searcher;
    searcher.Init(reader, schema);
    ResultPtr result = searcher.Search(query, tsc_default);
    ASSERT_TRUE(ResultChecker::Check(result, expectResult));

    iter->Reset();
    Searcher searcher2;
    searcher2.Init(reader, schema);
    ResultPtr resetResult = searcher2.Search(query, tsc_default);
    ASSERT_TRUE(ResultChecker::Check(resetResult, expectResult));
}

void TimeRangeIndexInteTest::TestAttributeValueIterator()
{
    string fullDocString = "cmd=add,time=1516086342000,pk=a,ts=1;"
                           "cmd=add,time=1516096346000,pk=b,ts=1";

    DateIndexConfigPtr dateConfig =
        DYNAMIC_POINTER_CAST(DateIndexConfig, mSchema->GetIndexSchema()->GetIndexConfig("time_index"));

    DateLevelFormat format;
    format.Init(DateLevelFormat::MILLISECOND);
    dateConfig->SetDateLevelFormat(format);
    dateConfig->SetBuildGranularity(DateLevelFormat::MILLISECOND);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(
        psm.Transfer(BUILD_FULL, fullDocString, "time_index:[1516076342000, 1516096342000]", "docid=0,pk=a;"));
    CheckTimeRangeIndexCompressInfo(psm.GetFileSystem());
    IndexPartitionPtr part = psm.GetIndexPartition();
    ASSERT_TRUE(part);
    auto partReader = part->GetReader();
    auto indexReader = partReader->GetInvertedIndexReader();
    index::Int64Term term(1516076342000, true, 1516096342000, true, "time_index");
    PostingIterator* iter = indexReader->Lookup(term).ValueOrThrow();
    SeekAndFilterIterator* stIter = dynamic_cast<SeekAndFilterIterator*>(iter);
    ASSERT_TRUE(stIter);
    DocValueFilter* dvIter = stIter->GetDocValueFilter();
    ASSERT_TRUE(dvIter->Test(0));
    ASSERT_FALSE(dvIter->Test(1));
    delete iter;
}

void TimeRangeIndexInteTest::TestMergeToMulitSegment()
{
    {
        // 599616000000: 1989-01-01 00:00:00
        // 915148800000: 1999-01-01 00:00:00
        // 946684800000: 2000-01-01 00:00:00
        // 631152000000: 1990-01-01 00:00:00
        string fullDocString = "cmd=add,time=599616000000,pk=1,ts=1;"
                               "cmd=add,time=599616000001,pk=2,ts=1;"
                               "cmd=add,time=915148800000,pk=3,ts=1;"
                               "cmd=add,time=915148800002,pk=4,ts=1;"
                               "cmd=add,time=946684800000,pk=11,ts=1;"
                               "cmd=add,time=631152000000,pk=5,ts=1;";
        std::string mergeConfigStr = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"2\"}}";
        autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_FULL, fullDocString, "time_index:(599616000000,915148800000)", "docid=5,pk=5"));
        CheckTimeRangeIndexCompressInfo(psm.GetFileSystem());
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[599616000001,915148800001]",
                                        "docid=1,pk=3;docid=4,pk=4;docid=5,pk=5"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(599615999999,946684800000)",
                                        "docid=0,pk=1;docid=1,pk=3;docid=3,pk=2;docid=4,pk=4;docid=5,pk=5"));
    }
    FslibWrapper::DeleteDirE(GET_TEMP_DATA_PATH(), DeleteOption::NoFence(false));
    FslibWrapper::MkDirE(GET_TEMP_DATA_PATH());
    {
        string fullDocString = "cmd=add,time=1516086342000,pk=a,ts=1;"
                               "cmd=add,time=1516096346000,pk=b,ts=1";
        std::string mergeConfigStr = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"3\"}}";
        autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_FULL, fullDocString, "time_index:[1516076342000, 1516096342000]", "docid=0,pk=a;"));
        CheckTimeRangeIndexCompressInfo(psm.GetFileSystem());
    }
    FslibWrapper::DeleteDirE(GET_TEMP_DATA_PATH(), DeleteOption::NoFence(false));
    FslibWrapper::MkDirE(GET_TEMP_DATA_PATH());
    {
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        string field = "sub_time:uint64;sub_pk:string";
        string index = "sub_time:date:sub_time;sub_pk:primarykey64:sub_pk";
        string attribute = "sub_time;sub_pk";
        subSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
        mSchema->SetSubIndexPartitionSchema(subSchema);
        string fullDocString = "cmd=add,time=1516086342000,pk=a,ts=1,sub_time=1516087342000^1516088342000,sub_pk=c^d;"
                               "cmd=add,time=1516096346000,pk=b,ts=1,sub_time=1516089342000,sub_pk=e";
        std::string mergeConfigStr = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"3\"}}";
        autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));
        INDEXLIB_TEST_TRUE(
            psm.Transfer(QUERY, "", "sub_time:[1516087342000, 1516088342000]", "docid=0,sub_pk=c;docid=1,sub_pk=d;"));
    }
}

void TimeRangeIndexInteTest::TestSearchTime()
{
    string field = "time:time;pk:string";
    string index = "time_index:date:time;pk:primarykey64:pk";
    string attribute = "time;pk";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    bool enableFileCompress = GET_PARAM_VALUE(0);
    if (enableFileCompress) {
        string compressorStr = "compressor:zstd;";
        string indexCompressStr = "time_index:compressor";
        string attrCompressStr = "time:compressor";
        mSchema = SchemaMaker::EnableFileCompressSchema(mSchema, compressorStr, indexCompressStr, attrCompressStr);
    }

    string fullDocString = "cmd=add,time=01:13:14.326,pk=1,ts=1;"
                           "cmd=add,time=02:13:14.326,pk=2,ts=1;"
                           "cmd=add,time=02:13:14.361,pk=3,ts=1;"
                           "cmd=add,time=08:11:24.362,pk=4,ts=1;"
                           "cmd=add,time=11:00:00,pk=5,ts=1;";

    string rtDocString = "cmd=add,time=10:10:10,pk=6,ts=2;"
                         "cmd=add,time=11:11:11,pk=7,ts=2;"
                         "cmd=add,time=12:12:12,pk=8,ts=2;"
                         "cmd=add,time=13:13:13,pk=9,ts=2;"
                         "cmd=add,time=23:59:59,pk=10,ts=2;";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "time_index:(10:0:0.000,23:59:59.999)", "pk=5"));
    // second level
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[02:13:14.326,02:13:14.361]", ""));

    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[02:13:14.000,02:13:15.000]", "pk=2;pk=3"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "time_index:(12:12:12,23:59:59)", "pk=9"));

    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(10:10:10,12:12:12)", "pk=5;pk=7"));

    INDEXLIB_TEST_TRUE(
        psm.Transfer(QUERY, "", "time_index:[00:00:00,13:00:00]", "pk=1;pk=2;pk=3;pk=4;pk=5;pk=6;pk=7;pk=8"));

    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(12:12:11.999,13:13:13)", "pk=8"));
}

void TimeRangeIndexInteTest::TestSearchDate()
{
    string field = "date:date;pk:string";
    string index = "time_index:date:date;pk:primarykey64:pk";
    string attribute = "date;pk";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    string fullDocString = "cmd=add,date=1990-01-02,pk=1,ts=1;"
                           "cmd=add,date=1991-02-03,pk=2,ts=1;"
                           "cmd=add,date=1992-02-22,pk=3,ts=1;"
                           "cmd=add,date=1992-02-23,pk=4,ts=1;"
                           "cmd=add,date=1993-11-11,pk=5,ts=1;";

    string rtDocString = "cmd=add,date=2010-01-02,pk=6,ts=2;"
                         "cmd=add,date=2011-11-11,pk=7,ts=2;"
                         "cmd=add,date=2012-01-01,pk=8,ts=2;"
                         "cmd=add,date=2012-01-01,pk=9,ts=2;"
                         "cmd=add,date=2012-01-03,pk=10,ts=2;";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(
        psm.Transfer(BUILD_FULL, fullDocString, "time_index:(1970-01-02,2000-01-01)", "pk=1;pk=2;pk=3;pk=4;pk=5"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(1992-02-22,1992-02-23]", "pk=4"));

    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[1991-02-03,1992-02-23)", "pk=2;pk=3"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "time_index:(2010-01-02,2012-01-01)", "pk=7"));

    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(2011-11-11,2012-01-01]", "pk=8;pk=9"));

    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[1992-11-12,2011-12-31]", "pk=5;pk=6;pk=7"));

    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(2012-01-01,2012-01-03)", ""));
}

void TimeRangeIndexInteTest::TestSearchTimestamp()
{
    string field = "timestamp:timestamp;pk:string";
    string index = "time_index:date:timestamp;pk:primarykey64:pk";
    string attribute = "timestamp;pk";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    FieldConfigPtr fieldConfig = schema->GetFieldConfig("timestamp");
    fieldConfig->SetDefaultTimeZoneDelta(-3600 * 1000); // GMT+0100

    string fullDocString = "cmd=add,timestamp=1990-01-02 12:00:00,pk=1,ts=1;"
                           "cmd=add,timestamp=1991-02-03 13:00:00,pk=2,ts=1;"
                           "cmd=add,timestamp=1992-02-22 10:00:00,pk=3,ts=1;"
                           "cmd=add,timestamp=1992-02-22 10:00:01,pk=4,ts=1;"
                           "cmd=add,timestamp=1993-11-11 00:12:12,pk=5,ts=1;";

    string rtDocString = "cmd=add,timestamp=2010-01-02 12:10:10,pk=6,ts=2;"
                         "cmd=add,timestamp=2011-11-11 13:10:00,pk=7,ts=2;"
                         "cmd=add,timestamp=2012-01-01 22:00:00,pk=8,ts=2;"
                         "cmd=add,timestamp=2012-01-01 23:00:00,pk=9,ts=2;"
                         "cmd=add,timestamp=2012-01-01 23:00:00,pk=10,ts=2;";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "time_index:(1970-01-02 11:59:59,2020-01-01 00:00:00)",
                                    "pk=1;pk=2;pk=3;pk=4;pk=5"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(1992-02-22 10:00:00,1992-02-23 00:00:00]", "pk=4"));

    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[1991-02-03 13:00:00,1992-02-22 10:00:01)", "pk=2;pk=3"));

    INDEXLIB_TEST_TRUE(
        psm.Transfer(BUILD_RT, rtDocString, "time_index:(2010-01-02 12:10:10,2012-01-01 22:00:00)", "pk=7"));

    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(2011-11-11 13:10:00,2012-01-01 22:00:00]", "pk=8"));

    INDEXLIB_TEST_TRUE(
        psm.Transfer(QUERY, "", "time_index:[1992-11-12 00:00:00,2011-12-31 23:59:59]", "pk=5;pk=6;pk=7"));

    INDEXLIB_TEST_TRUE(
        psm.Transfer(QUERY, "", "time_index:[2012-01-01 10:10:10,2012-01-01 23:00:01)", "pk=8;pk=9;pk=10"));
}

void TimeRangeIndexInteTest::TestSearchNullTime()
{
    string field = "time:time;pk:string";
    string index = "time_index:date:time;pk:primarykey64:pk";
    string attribute = "time;pk";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    SchemaMaker::EnableNullFields(schema, "time");
    bool enableFileCompress = GET_PARAM_VALUE(0);
    if (enableFileCompress) {
        string compressorStr = "compressor:zstd;";
        string indexCompressStr = "time_index:compressor";
        string attrCompressStr = "time:compressor";
        schema = SchemaMaker::EnableFileCompressSchema(schema, compressorStr, indexCompressStr, attrCompressStr);
    }

    string fullDocString = "cmd=add,time=01:13:14.326,pk=1,ts=1;"
                           "cmd=add,time=__NULL__,pk=2,ts=1;"
                           "cmd=add,time=02:13:14.361,pk=3,ts=1;"
                           "cmd=add,pk=4,ts=1;"
                           "cmd=add,time=11:00:00,pk=5,ts=1;";

    string rtDocString = "cmd=add,time=__NULL__,pk=6,ts=2;"
                         "cmd=add,time=11:11:11,pk=7,ts=2;"
                         "cmd=add,time=12:12:12,pk=8,ts=2;"
                         "cmd=add,time=13:13:13,pk=9,ts=2;"
                         "cmd=add,time=__NULL__,pk=10,ts=2;";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "time_index:__NULL__", "pk=2;pk=4"));
    CheckTimeRangeIndexCompressInfo(psm.GetFileSystem());
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "time_index:__NULL__", "pk=2;pk=4;pk=6;pk=10"));
}

void TimeRangeIndexInteTest::TestSearchNullDate()
{
    string field = "date:date;pk:string";
    string index = "time_index:date:date;pk:primarykey64:pk";
    string attribute = "date;pk";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    SchemaMaker::EnableNullFields(schema, "date");

    string fullDocString = "cmd=add,date=1990-01-02,pk=1,ts=1;"
                           "cmd=add,pk=2,ts=1;"
                           "cmd=add,date=1992-02-22,pk=3,ts=1;"
                           "cmd=add,date=__NULL__,pk=4,ts=1;"
                           "cmd=add,date=__NULL__,pk=5,ts=1;";

    string rtDocString = "cmd=add,date=2010-01-02,pk=6,ts=2;"
                         "cmd=add,date=__NULL__,pk=7,ts=2;"
                         "cmd=add,date=2012-01-01,pk=8,ts=2;"
                         "cmd=add,date=2012-01-01,pk=9,ts=2;"
                         "cmd=add,pk=10,ts=2;";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "time_index:__NULL__", "pk=2;pk=4;pk=5"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "time_index:__NULL__", "pk=2;pk=4;pk=5;pk=7;pk=10"));
}

void TimeRangeIndexInteTest::TestSearchNullTimestamp()
{
    string field = "timestamp:timestamp;pk:string";
    string index = "time_index:date:timestamp;pk:primarykey64:pk";
    string attribute = "timestamp;pk";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    SchemaMaker::EnableNullFields(schema, "timestamp");

    string fullDocString = "cmd=add,timestamp=__NULL__,pk=1,ts=1;"
                           "cmd=add,timestamp=1991-02-03 13:00:00,pk=2,ts=1;"
                           "cmd=add,pk=3,ts=1;"
                           "cmd=add,timestamp=1992-02-22 10:00:01,pk=4,ts=1;"
                           "cmd=add,timestamp=__NULL__,pk=5,ts=1;";

    string rtDocString = "cmd=add,timestamp=2010-01-02 12:10:10,pk=6,ts=2;"
                         "cmd=add,timestamp=__NULL__,pk=7,ts=2;"
                         "cmd=add,timestamp=2012-01-01 22:00:00,pk=8,ts=2;"
                         "cmd=add,pk=9,ts=2;"
                         "cmd=add,timestamp=__NULL__,pk=10,ts=2;";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "time_index:__NULL__", "pk=1;pk=3;pk=5"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "time_index:__NULL__", "pk=1;pk=3;pk=5;pk=7;pk=9;pk=10"));
}

void TimeRangeIndexInteTest::TestSearchTimeByBlockCache()
{
    string field = "time:time;pk:string";
    string index = "time_index:date:time;pk:primarykey64:pk";
    string attribute = "time;pk";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    bool enableFileCompress = GET_PARAM_VALUE(0);
    if (enableFileCompress) {
        string compressorStr = "compressor:zstd;";
        string indexCompressStr = "time_index:compressor";
        string attrCompressStr = "time:compressor";
        mSchema = SchemaMaker::EnableFileCompressSchema(mSchema, compressorStr, indexCompressStr, attrCompressStr);
    }

    string fullDocString = "cmd=add,time=01:13:14.326,pk=1,ts=1;"
                           "cmd=add,time=02:13:14.326,pk=2,ts=1;"
                           "cmd=add,time=02:13:14.361,pk=3,ts=1;"
                           "cmd=add,time=08:11:24.362,pk=4,ts=1;"
                           "cmd=add,time=11:00:00,pk=5,ts=1;";

    PartitionStateMachine psm;
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GET_PARAM_VALUE(1));
    string loadConfig = R"(
        {
            "load_config":[
                {
                    "file_patterns":[
                        "index/time_index/dictionary"
                    ],
                    "load_strategy":"cache",
                    "load_strategy_param":{
                        "direct_io":false,
                        "global_cache":true
                    }
                }
            ]
        }
    )";
    FromJsonString(options.GetOnlineConfig().loadConfigList, loadConfig);
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "time_index:(10:0:0.000,23:59:59.999)", "pk=5"));
    // second level
    auto blockCache =
        psm.GetIndexPartitionResource().fileBlockCacheContainer->GetAvailableFileCache("")->GetBlockCache();
    ASSERT_TRUE(blockCache);

    ASSERT_EQ(1, blockCache->GetTotalMissCount());

    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[02:13:14.326,02:13:14.361]", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[02:13:14.000,02:13:15.000]", "pk=2;pk=3"));
}

void TimeRangeIndexInteTest::TestSearchTimeByBlockCacheConcurrency()
{
    string field = "time:time;pk:string";
    string index = "time_index:date:time;pk:primarykey64:pk";
    string attribute = "time;pk";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    bool enableFileCompress = GET_PARAM_VALUE(0);
    if (enableFileCompress) {
        string compressorStr = "compressor:zstd;";
        string indexCompressStr = "time_index:compressor";
        string attrCompressStr = "time:compressor";
        mSchema = SchemaMaker::EnableFileCompressSchema(mSchema, compressorStr, indexCompressStr, attrCompressStr);
    }

    string fullDocString = "cmd=add,time=01:02:00,pk=1,ts=1;"
                           "cmd=add,time=02:13:14.326,pk=2,ts=1;"
                           "cmd=add,time=03:13:14.361,pk=3,ts=1;"
                           "cmd=add,time=04:11:24.362,pk=4,ts=1;"
                           "cmd=add,time=05:00:20,pk=5,ts=1;"
                           "cmd=add,time=06:10:00,pk=6,ts=1;"
                           "cmd=add,time=07:12:00,pk=7,ts=1;"
                           "cmd=add,time=08:12:00,pk=8,ts=1;";
    PartitionStateMachine psm;
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GET_PARAM_VALUE(1));
    string loadConfig = R"(
        {
            "load_config":[
                {
                    "file_patterns":[
                        "index/time_index/dictionary"
                    ],
                    "load_strategy":"cache",
                    "load_strategy_param":{
                        "direct_io":false,
                        "global_cache":true
                    }
                }
            ]
        }
    )";
    FromJsonString(options.GetOnlineConfig().loadConfigList, loadConfig);
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "time_index:(07:0:0.000,23:59:59.999)", "pk=7;pk=8"));

    vector<autil::ThreadPtr> threads;
    for (size_t i = 0; i < 5; ++i) {
        autil::ThreadPtr thread = autil::Thread::createThread([&]() {
            for (int j = 0; j < 512; ++j) {
                INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[01:00:00,02:00:00]", "pk=1"));
                INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[02:00:00,03:00:00]", "pk=2"));
                INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[03:00:00,04:00:00]", "pk=3"));
                INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[04:00:00,05:00:00]", "pk=4"));
                INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[05:00:00,06:00:00]", "pk=5"));
                INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[06:00:00,07:00:00]", "pk=6"));
                INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[07:00:00,08:00:00]", "pk=7"));
                INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[08:00:00,09:00:00]", "pk=8"));
            }
        });
        threads.emplace_back(thread);
    }
    for (auto& thread : threads) {
        thread->join();
    }
}

void TimeRangeIndexInteTest::CheckTimeRangeIndexCompressInfo(const file_system::IFileSystemPtr& fileSystem)
{
    auto partData = OnDiskPartitionData::CreateOnDiskPartitionData(fileSystem);
    if (GET_PARAM_VALUE(0)) {
        CheckFileCompressInfo(partData, "index/time_index/posting", "zstd");
        CheckFileCompressInfo(partData, "attribute/time/data", "zstd");
        CheckFileCompressInfo(partData, "index/time_index/dictionary", "zstd");
    } else {
        CheckFileCompressInfo(partData, "index/time_index/posting", "null");
        CheckFileCompressInfo(partData, "attribute/time/data", "null");
        CheckFileCompressInfo(partData, "index/time_index/dictionary", "null");
    }
}

void TimeRangeIndexInteTest::CheckFileCompressInfo(const PartitionDataPtr& partitionData, const string& filePath,
                                                   const string& compressType)
{
    auto partitionDataIter = partitionData->CreateSegmentIterator();
    auto iter = partitionDataIter->CreateIterator(SIT_BUILT);
    while (iter->IsValid()) {
        SegmentData segData = iter->GetSegmentData();
        DirectoryPtr segDir = segData.GetDirectory();
        auto compressInfo = segDir->GetCompressFileInfo(filePath);
        if (compressType == "null") {
            EXPECT_TRUE(compressInfo.get() == nullptr);
        } else {
            EXPECT_TRUE(compressInfo.get() != nullptr) << segDir->DebugString() << filePath;
            EXPECT_EQ(compressType, compressInfo->compressorName);
        }
        iter->MoveToNext();
    }
}
}} // namespace indexlib::partition
