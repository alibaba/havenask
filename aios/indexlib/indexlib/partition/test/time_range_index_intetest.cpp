#include "indexlib/partition/test/time_range_index_intetest.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/index/normal/inverted_index/accessor/seek_and_filter_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/doc_value_filter.h"
#include "indexlib/config/date_index_config.h"
#include "indexlib/config/impl/date_index_config_impl.h"
#include "indexlib/common/number_term.h"
#include "indexlib/test/slow_dump_segment_container.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/document_parser.h"
#include "indexlib/test/atomic_query.h"
#include "indexlib/test/searcher.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, TimeRangeIndexInteTest);

TimeRangeIndexInteTest::TimeRangeIndexInteTest()
{
}

TimeRangeIndexInteTest::~TimeRangeIndexInteTest()
{
}

void TimeRangeIndexInteTest::CaseSetUp()
{
    string field = "time:uint64;pk:string";
    string index = "time_index:date:time;pk:primarykey64:pk";
    string attribute = "time;pk";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mRootDir = GET_TEST_DATA_PATH();
    std::string mergeConfigStr
        = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

}

void TimeRangeIndexInteTest::CaseTearDown()
{
}

void TimeRangeIndexInteTest::TestBug23953311()
{
    //test date range loss some doc
    string fullDocString = "cmd=add,time=1516086342000,pk=a,ts=1;"
                           "cmd=add,time=1516096346000,pk=b,ts=1";
    string rtDocString = "cmd=add,time=1016086341000,pk=e,ts=4;"
                          "cmd=add,time=1616086342000,pk=f,ts=4";
    DateIndexConfigPtr dateConfig = 
      DYNAMIC_POINTER_CAST(DateIndexConfig,
			   mSchema->GetIndexSchema()->GetIndexConfig("time_index"));
    DateLevelFormat format;
    format.Init(DateLevelFormat::MILLISECOND);
    dateConfig->SetDateLevelFormat(format);
    dateConfig->SetBuildGranularity(DateLevelFormat::MILLISECOND);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString,
				    "time_index:[1516076342000, 1516096346000]",
				    "docid=0,pk=a;docid=1,pk=b"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString,
				    "time_index:[1516076342000, 1516096346000]",
				    "docid=0,pk=a;docid=1,pk=b"));
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
        DYNAMIC_POINTER_CAST(DateIndexConfig,
                             mSchema->GetIndexSchema()->GetIndexConfig("time_index"));
    {
        TearDown();
        SetUp();
        DateLevelFormat format;
        format.Init(DateLevelFormat::MILLISECOND);
        dateConfig->SetDateLevelFormat(format);
        dateConfig->SetBuildGranularity(DateLevelFormat::MILLISECOND);
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString,
                        "time_index:[1516076342000, 1516096342000]",
                        "docid=0,pk=a;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString,
                        "time_index:[1516076342000, 1516096342000]",
                        "docid=0,pk=a;docid=2,pk=c;docid=3,pk=d"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString2,
                        "time_index:[1500001000000,1500001200001)",
                        "docid=5,pk=k;"));
        //1516086340000,1516086341000
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString,
                        "time_index:[1516086340000, 1516086342000)",
                        "docid=2,pk=c;docid=6,pk=e;"));
        //test query illegal
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "",
                        "time_index:[1516186340000, 1516086342000)",
                        ""));
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "",
                        "time_index:[1516086340000, 151618abc42000)",
                                     ""));
    }
    {
        TearDown();
        SetUp();
        DateLevelFormat format;
        format.Init(DateLevelFormat::MINUTE);
        dateConfig->SetDateLevelFormat(format);
        dateConfig->SetBuildGranularity(DateLevelFormat::MINUTE);
        PartitionStateMachine psm;
        fullDocString = "cmd=add,time=1516096320000,pk=b,ts=1";
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString,
                        "time_index:[1516076340000,1516096380000)",
                        "docid=0,pk=b"));
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
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString,
                                    "time_index:[1516076342000, 1516096342000]", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString,
                                    "time_index:[1516076342000, 1516096342000]",
                                    "docid=2,pk=c;docid=3,pk=d"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString1,
                                    "time_index:[1516076342000, 1516096342000]",
                                    "docid=2,pk=c;docid=3,pk=d"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString2,
                        "time_index:[1516076342000, 1516096342000]", ""));
}

void TimeRangeIndexInteTest::TestRtDocCoverFullDoc()
{
    DateIndexConfigPtr dateConfig = 
        DYNAMIC_POINTER_CAST(DateIndexConfig,
                             mSchema->GetIndexSchema()->GetIndexConfig("time_index"));
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
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString,
                                    "time_index:[1500001000000,1500001200001]",
                                    "docid=1,pk=30;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString,
                                    "time_index:[1506903500000,1506903500000]",
                                    "docid=0,pk=20;"
                                    "docid=2,pk=1;"
                                    "docid=3,pk=2;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "",
                                    "time_index:[1514764799999,1514764800001]",
                                    "docid=4,pk=3;docid=5,pk=4;"
                                    "docid=6,pk=5;docid=7,pk=6;"
                                    ));
}

void TimeRangeIndexInteTest::TestAbnormalInput()
{
    //one doc overflow time limit, one normal doc, one doc no time field, one time = 0 doc
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
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString,
                                    "time_index:[1500001000000,1500001200001]",
                                    "pk=2;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString,
                                    "time_index:[1500001000000,1600001200001]",
                                    "pk=2;pk=7"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "",
                                    "time_index:[0,101506903500000]",
                                    "pk=1;pk=2;pk=4;pk=5;"
                                    "pk=6;pk=7;pk=9;pk=10"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "",
                                    "time_index:(0,101506903500000]",
                                    "pk=1;pk=2;pk=5;"
                                    "pk=6;pk=7;pk=10"));

    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "",
                                    "time_index:(0,4102444800000]",
                                    "pk=1;pk=2;pk=5;"
                                    "pk=6;pk=7;pk=10"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "",
                                    "time_index:(0,4102444800000)",
                                    "pk=2;pk=7"));
}

void TimeRangeIndexInteTest::TestLookupBuildingSegmentWithAsyncDump()
{
    DateIndexConfigPtr dateConfig = 
        DYNAMIC_POINTER_CAST(DateIndexConfig,
                             mSchema->GetIndexSchema()->GetIndexConfig(
                                     "time_index"));
    DateLevelFormat format;
    format.Init(DateLevelFormat::YEAR);
    dateConfig->SetDateLevelFormat(format);
    dateConfig->SetBuildGranularity(DateLevelFormat::YEAR);

    //599616000000: 1989-01-01 00:00:00
    //915148800000: 1999-01-01 00:00:00
    //946684800000: 2000-01-01 00:00:00
    //631152000000: 1990-01-01 00:00:00
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
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false,
                              partition::IndexPartitionResource(), dumpContainer);
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString1,
                                    "time_index:[599616000001,915148800001]",
                                    "pk=3;pk=4;pk=5"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString2,
                                    "time_index:[599616000001,915148800001]",
                                    "pk=3;pk=4;pk=5;pk=8;pk=9;pk=10"));
    ASSERT_EQ(1u, dumpContainer->GetDumpItemSize());
    dumpContainer->WaitEmpty();
    ASSERT_EQ(0u, dumpContainer->GetDumpItemSize());
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "",
                                    "time_index:[599616000001,915148800001]",
                                    "pk=3;pk=4;pk=5;pk=8;pk=9;pk=10"));
}

void TimeRangeIndexInteTest::TestOpenRangeQuery()
{
    DateIndexConfigPtr dateConfig = 
        DYNAMIC_POINTER_CAST(DateIndexConfig,
                             mSchema->GetIndexSchema()->GetIndexConfig(
                                     "time_index"));
    DateLevelFormat format;
    format.Init(DateLevelFormat::YEAR);
    dateConfig->SetDateLevelFormat(format);
    dateConfig->SetBuildGranularity(DateLevelFormat::YEAR);

    //599616000000: 1989-01-01 00:00:00
    //915148800000: 1999-01-01 00:00:00
    //946684800000: 2000-01-01 00:00:00
    //631152000000: 1990-01-01 00:00:00
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
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString,
                                    "time_index:(599616000000,915148800000)",
                                    "pk=5"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "",
                                    "time_index:[599616000001,915148800001]",
                                    "pk=3;pk=4;pk=5"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "",
                                    "time_index:(599615999999,946684800000)",
                                    "pk=1;pk=2;pk=3;pk=4;pk=5"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString,
                                    "time_index:(599616000000,915148800000)",
                                    "pk=5;pk=10"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "",
                                    "time_index:[599616000001,915148800001]",
                                    "pk=3;pk=4;pk=5;pk=8;pk=9;pk=10"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "",
                                    "time_index:(599615999999,946684800000)",
                                    "pk=1;pk=2;pk=3;pk=4;pk=5;"
                                    "pk=6;pk=7;pk=8;pk=9;pk=10"));
}
void TimeRangeIndexInteTest::TestDecoderDoc()
{
    string dictInlineDoc = GetRangeDocString(1500001200000LL, 1500001200000LL, 1000, 0);
    string dictInlineResult = "pk=0";
    string dictInlineQuery = "time_index:[1500001200000,1500001200000]";
    string skipListDoc = GetRangeDocString(1516076342000LL, 1516086342000LL, 1000);
    string skipListResult = GetQueryResultString(1, 10000);
    string skipListQuery = "time_index:[1516076342000,1516086342000)";
    string shortListDoc = GetRangeDocString(1516090000000LL, 1516090004000LL, 1000, 10002);
    string shortListResult = GetQueryResultString(10002, 10006);
    string shortListQuery = "time_index:[1516090000000,1516090005000]";
    string allQuery = "time_index:[1500001200000,)";
    InnerTestDecoderDoc(dictInlineDoc, dictInlineQuery, dictInlineResult,
                        skipListDoc, skipListQuery, skipListResult,
                        shortListDoc, shortListQuery, shortListResult);
    {
        shortListDoc = GetRangeDocString(1516090000000LL, 1516090004000LL, 1000, 10002);
        dictInlineDoc = GetRangeDocString(1500001200000LL, 1500001200000LL, 1000, 0);
        skipListDoc = GetRangeDocString(1516076342000LL, 1516086342000LL, 1000);
        InnerTestDecoderDoc(shortListDoc, shortListQuery, shortListResult,
                            dictInlineDoc, dictInlineQuery, dictInlineResult,
                            skipListDoc, skipListQuery, skipListResult);
    }
    {
        skipListDoc = GetRangeDocString(1516076342000LL, 1516086342000LL, 1000);
        shortListDoc = GetRangeDocString(1516090000000LL, 1516090004000LL, 1000, 10002);
        dictInlineDoc = GetRangeDocString(1500001200000LL, 1500001200000LL, 1000, 0);
        skipListResult = GetQueryResultString(1, 10001);
        skipListQuery = "time_index:[1516076342000,1516086342000]";
        InnerTestDecoderDoc(skipListDoc, skipListQuery, skipListResult,
                            shortListDoc, shortListQuery, shortListResult,
                            dictInlineDoc, allQuery, skipListResult
                            + shortListResult + dictInlineResult);
    }
}

void TimeRangeIndexInteTest::InnerTestDecoderDoc(
        const string& fullDoc, const string& fullQuery, const string& fullResult,
        const string& incDoc, const string& incQuery, const string& incResult,
        const string& rtDoc, const string& rtQuery, const string& rtResult)
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, fullQuery,
                                    fullResult));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDoc, incQuery, incResult));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDoc, rtQuery, rtResult));
}

string TimeRangeIndexInteTest::GetRangeDocString(
        uint64_t startTime, uint64_t endTime, uint64_t unit, uint64_t startPk)
{
    string incDocString;
    uint64_t gtime = GetGlobalTime();
    for(uint64_t i = startTime, j = startPk; i <= endTime; i += unit, j ++)
    {
        incDocString += "cmd=add,time=" + StringUtil::toString(i) + ",pk="
                        + StringUtil::toString(j) + ",ts="
                        + StringUtil::toString(gtime) + ";";
    }
    return incDocString;
}

std::string TimeRangeIndexInteTest::GetQueryResultString(uint64_t startPk, uint64_t endPk)
{
    string resultString;
    for(auto i = startPk; i <= endPk; i ++)
    {
        resultString += "pk=" + StringUtil::toString(i) + ";";
    }
    return resultString;
}

uint64_t TimeRangeIndexInteTest::GetGlobalTime()
{
    static uint64_t curTime = 0;
    curTime ++;
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
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, skipListDoc, skipListQuery,
                                    skipListResult));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, shortListDoc, shortListQuery, shortListResult));
    auto reader = psm.GetIndexPartition()->GetReader();

    vector<string> indexTerms = StringUtil::split(shortListQuery, ":");
    IndexReaderPtr indexReader = reader->GetIndexReader();
    MultiFieldIndexReaderPtr multiReader = DYNAMIC_POINTER_CAST(MultiFieldIndexReader, indexReader);
    indexReader = multiReader->GetIndexReader("time_index");
    Int64Term term(1516090000000, true, 1516090005000, true, "time_index");
    PostingIterator* iter = indexReader->Lookup(term);
    index::TermMeta* meta = iter->GetTermMeta();
    ASSERT_EQ(5, meta->GetDocFreq());
    InnerTestIterator(iter->Clone(), reader, shortListResult);
    InnerTestIterator(iter, reader, shortListResult);
}

void TimeRangeIndexInteTest::InnerTestIterator(index::PostingIterator* iter,
        IndexPartitionReaderPtr reader, const std::string& resultString)
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
        DYNAMIC_POINTER_CAST(DateIndexConfig,
                             mSchema->GetIndexSchema()->GetIndexConfig("time_index"));

    DateLevelFormat format;
    format.Init(DateLevelFormat::MILLISECOND);
    dateConfig->SetDateLevelFormat(format);
    dateConfig->SetBuildGranularity(DateLevelFormat::MILLISECOND);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString,
                                    "time_index:[1516076342000, 1516096342000]",
                                    "docid=0,pk=a;"));
    IndexPartitionPtr part = psm.GetIndexPartition();
    ASSERT_TRUE(part);
    auto partReader = part->GetReader();
    auto indexReader = partReader->GetIndexReader();
    Int64Term term(1516076342000, true, 1516096342000, true, "time_index");
    PostingIterator* iter = indexReader->Lookup(term);
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
        std::string mergeConfigStr
            = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"2\"}}";
        autil::legacy::FromJsonString(
            mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        INDEXLIB_TEST_TRUE(psm.Transfer(
            BUILD_FULL, fullDocString, "time_index:(599616000000,915148800000)", "docid=5,pk=5"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:[599616000001,915148800001]",
            "docid=1,pk=3;docid=4,pk=4;docid=5,pk=5"));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "time_index:(599615999999,946684800000)",
            "docid=0,pk=1;docid=1,pk=3;docid=3,pk=2;docid=4,pk=4;docid=5,pk=5"));
    }
    FileSystemWrapper::DeleteDir(GET_TEST_DATA_PATH());
    FileSystemWrapper::MkDir(GET_TEST_DATA_PATH());
    {
        string fullDocString = "cmd=add,time=1516086342000,pk=a,ts=1;"
                               "cmd=add,time=1516096346000,pk=b,ts=1";
        std::string mergeConfigStr
            = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"3\"}}";
        autil::legacy::FromJsonString(
                mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString,
                        "time_index:[1516076342000, 1516096342000]",
                        "docid=0,pk=a;"));
    }
    FileSystemWrapper::DeleteDir(GET_TEST_DATA_PATH());
    FileSystemWrapper::MkDir(GET_TEST_DATA_PATH());
    {
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        string field = "sub_time:uint64;sub_pk:string";
        string index = "sub_time:date:sub_time;sub_pk:primarykey64:sub_pk";
        string attribute = "sub_time;sub_pk";
        subSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
        mSchema->SetSubIndexPartitionSchema(subSchema);
        string fullDocString = "cmd=add,time=1516086342000,pk=a,ts=1,sub_time=1516087342000^1516088342000,sub_pk=c^d;"
                               "cmd=add,time=1516096346000,pk=b,ts=1,sub_time=1516089342000,sub_pk=e";
        std::string mergeConfigStr
            = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"3\"}}";
        autil::legacy::FromJsonString(
                mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "sub_time:[1516087342000, 1516088342000]",
            "docid=0,sub_pk=c;docid=1,sub_pk=d;"));
  }
}

IE_NAMESPACE_END(partition);
