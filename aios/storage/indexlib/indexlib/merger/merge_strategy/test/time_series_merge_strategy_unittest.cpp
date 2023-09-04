#include "indexlib/merger/merge_strategy/test/time_series_merge_strategy_unittest.h"

#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;
using namespace indexlib::test;
using namespace indexlib::config;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, TimeSeriesMergeStrategyTest);

TimeSeriesMergeStrategyTest::TimeSeriesMergeStrategyTest() {}

TimeSeriesMergeStrategyTest::~TimeSeriesMergeStrategyTest() {}

void TimeSeriesMergeStrategyTest::CaseSetUp()
{
    mSchema.reset(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(mSchema,
                                          // Field schema
                                          "string0:string;string1:string;time:int64;string2:string:true",
                                          // Index schema
                                          "string2:string:string2",
                                          // Attribute schema
                                          "time;string0;string1;string2",
                                          // Summary schema
                                          "string1");
}

void TimeSeriesMergeStrategyTest::CaseTearDown() {}

void TimeSeriesMergeStrategyTest::TestSimpleProcess()
{
    string mergeParam = "max-doc-count=100;sort-field=time;|time=[0-20],output-interval=100;|"
                        "max-merged-segment-doc-count=50";
    InnerTestTimeSeriesMergeTask("20 10 1 1 time=10,15;20 10 1 1 time=13,17", mergeParam, "0,1", false);
    InnerTestTimeSeriesMergeTask("20 10 1 1 time=10,15;20 10 1 1 time=13,200", mergeParam, "", false);
    mergeParam = "max-doc-count=100;sort-field=time;|time=[10-30],output-interval=100;time=[50-100],"
                 "output-interval=200;|max-merged-segment-doc-count=50";
    InnerTestTimeSeriesMergeTask("20 10 1 1 time=10,40;20 10 1 1 time=30,70;20 10 1 1 time=30,120", mergeParam, "0,1",
                                 false);

    mergeParam = "max-doc-count=100;sort-field=time;|time=[0-30],output-interval=70;time=[50-80],"
                 "output-interval=200;|max-merged-segment-doc-count=50";

    InnerTestTimeSeriesMergeTask("20 10 1 1 time=60,90;20 10 1 1 time=30,70;20 10 1 1 time=30,50;20 10 1 1 time=80,120",
                                 mergeParam, "0,3;1,2", false);

    mergeParam = "max-doc-count=100;sort-field=time;|time=[ - ],output-interval=20;time=[50-80],"
                 "output-interval=200;|max-merged-segment-doc-count=50";

    InnerTestTimeSeriesMergeTask(
        "20 10 1 1 time=30,50;20 10 1 1 time=50,150;20 10 1 1 time=150,160;20 10 1 1 time=160,170", mergeParam, "2,3",
        false);
}

void TimeSeriesMergeStrategyTest::TestInputOutputLimit()
{
    string mergeParam = "max-doc-count=10;sort-field=time;max-segment-size=2097152;|"
                        "time=[0-30],output-interval=10;time=[50-100],"
                        "output-interval=200;|max-merged-segment-doc-count=50";
    InnerTestTimeSeriesMergeTask("20 10 1 1 time=10,15;5 10 1 1 time=13,17", mergeParam, "", false);
    InnerTestTimeSeriesMergeTask("20 10 1 1 time=10,15;5 1 1 2 time=13,17", mergeParam, "0,1", false);
    InnerTestTimeSeriesMergeTask("12 10 1 1 time=10,15;5 1 1 3 time=12,18;1 1 1 2 time=13,20", mergeParam, "", false);

    mergeParam = "max-doc-count=80;sort-field=time;max-segment-size=2097152;|"
                 "time=[0-30],output-interval=10;time=[50-100],"
                 "output-interval=200;|max-merged-segment-doc-count=20";
    InnerTestTimeSeriesMergeTask("60 10 1 1 time=10,15;5 1 1 1 time=12,14;20 1 1 1 time=10,20", mergeParam, "0,2",
                                 false);
}

void TimeSeriesMergeStrategyTest::TestRangeLimit()
{
    string mergeParam = "max-doc-count=30;sort-field=time;max-segment-size=2097152;|"
                        "time=[0-],output-interval=50;time=[50-100],"
                        "output-interval=200;|max-merged-segment-doc-count=50";
    InnerTestTimeSeriesMergeTask("20 10 1 1 time=10,15;12 10 1 1 time=13,17;12 10 1 1 time=12,17", mergeParam, "0,1,2",
                                 false);
    mergeParam = "max-doc-count=30;sort-field=time;max-segment-size=2097152;|"
                 "time=[-100],output-interval=50;time=[0-],"
                 "output-interval=10;|max-merged-segment-doc-count=20";
    InnerTestTimeSeriesMergeTask("20 10 1 1 time=10,15;12 10 1 1 time=200,210;12 10 1 1 time=50,70", mergeParam, "",
                                 false);

    mergeParam = "max-doc-count=30;sort-field=time;max-segment-size=2097152;|"
                 "time = [ -100 ],output-interval=50;time = [ 0 - ],"
                 "output-interval=10;|max-merged-segment-doc-count=20";
    InnerTestTimeSeriesMergeTask("20 10 1 1 time=10,15;12 10 1 1 time=200,210;12 10 1 1 time=50,70", mergeParam, "",
                                 false);

    mergeParam = "max-doc-count=30;sort-field=time;max-segment-size=2097152;|"
                 "time=[-],output-interval=80;|max-merged-segment-doc-count=20";
    string mergeInfos = "20 10 1 1 time=10,30;12 10 1 1 time=60,100;12 10 1 1 time=80,120;"
                        "12 10 1 1 time=90,120; 50 10 1 3 time=150,200";
    InnerTestTimeSeriesMergeTask(mergeInfos, mergeParam, "2,3;0,1", false);
}

void TimeSeriesMergeStrategyTest::TestException()
{
    string mergeParam = "max-doc-count=100;sort-field=time;|time=[0-20],output-interval=100;|"
                        "max-merged-segment-doc-count=50";
    InnerTestTimeSeriesMergeTask("20 10 1 1 time=10,15;20 10 1 1 noExist=13,17", mergeParam, "", false);
    InnerTestTimeSeriesMergeTask("20 10 1 1 noExist=10,15;20 10 1 1 noExist=13,17", mergeParam, "", false);
}

void TimeSeriesMergeStrategyTest::TestMergeWithTag()
{
    string mergeParam = "max-doc-count = 100 ; sort-field = time;max-segment-size=2097152;|"
                        "output-interval=30, lifecycle = warm;time=[50-100],"
                        "output-interval=200, lifecycle = cold;|max-merged-segment-doc-count =50";
    InnerTestTimeSeriesMergeTask("12 10 1 1 time=10,15 lifecycle=warm;11 10 1 1 time=20,25 lifecycle=warm", mergeParam,
                                 "0,1", false);
    InnerTestTimeSeriesMergeTask("12 10 1 1 time=10,15 lifecycle=hot;11 10 1 1 time=20,25 lifecycle=hot", mergeParam,
                                 "", false);
}

void TimeSeriesMergeStrategyTest::TestInvalidSortField()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        // Field schema
        "string0:string;string1:string;time:int64;string2:string:true",
        // Index schema
        "string2:string:string2",
        // Attribute schema
        "time;string0;string1;string2",
        // Summary schema
        "string1");

    SchemaMaker::EnableNullFields(schema, "time");
    SegmentDirectoryPtr segDir;
    TimeSeriesMergeStrategy ms(segDir, schema);
    string mergeParam = "max-doc-count=100;sort-field=time;max-segment-size=2097152;|"
                        "output-interval=30,lifecycle=warm;time=[50-100],"
                        "output-interval=200,lifecycle=cold;|max-merged-segment-doc-count=50";

    vector<string> param = autil::StringUtil::split(mergeParam, "|", false);
    MergeStrategyParameter parameter;
    parameter.inputLimitParam = param[0];
    parameter.strategyConditions = param[1];
    parameter.outputLimitParam = param[2];
    ASSERT_ANY_THROW(ms.SetParameter(parameter));
}

MergeTask TimeSeriesMergeStrategyTest::CreateMergeTask(const SegmentMergeInfos& segMergeInfos,
                                                       const string& mergeStrategyParams, bool optmize)

{
    vector<string> param = autil::StringUtil::split(mergeStrategyParams, "|", false);
    MergeStrategyParameter parameter;
    parameter.inputLimitParam = param[0];
    parameter.strategyConditions = param[1];
    parameter.outputLimitParam = param[2];

    SegmentDirectoryPtr segDir;
    TimeSeriesMergeStrategy ms(segDir, mSchema);
    ms.SetParameter(parameter);
    string root;
    indexlibv2::framework::LevelInfo levelInfo;
    if (optmize) {
        return ms.CreateMergeTaskForOptimize(segMergeInfos, levelInfo);
    } else {
        return ms.CreateMergeTask(segMergeInfos, levelInfo);
    }
}

void TimeSeriesMergeStrategyTest::CheckMergeTask(const string& expectResults, const MergeTask& task)
{
    vector<vector<segmentid_t>> expectResult;
    autil::StringUtil::fromString(expectResults, expectResult, ",", ";");
    ASSERT_EQ(expectResult.size(), task.GetPlanCount()) << TaskToString(task);
    for (size_t i = 0; i < task.GetPlanCount(); i++) {
        MergePlan plan = task[i];
        ASSERT_EQ(expectResult[i].size(), plan.GetSegmentCount()) << plan.ToString();
        MergePlan::Iterator segIter = plan.CreateIterator();
        uint32_t idx = 0;
        while (segIter.HasNext()) {
            segmentid_t segId = segIter.Next();
            ASSERT_EQ(expectResult[i][idx], segId);
            idx++;
        }
    }
}

void TimeSeriesMergeStrategyTest::InnerTestTimeSeriesMergeTask(const string& originalSegs, const string& mergeParams,
                                                               const string& expectMergeSegs, bool optimizeMerge)
{
    SegmentMergeInfos mergeInfos;
    MakeSegmentMergeInfos(originalSegs, mergeInfos);
    MergeTask task = CreateMergeTask(mergeInfos, mergeParams, optimizeMerge);
    CheckMergeTask(expectMergeSegs, task);
}

std::string TimeSeriesMergeStrategyTest::TaskToString(const MergeTask& task)
{
    stringstream ss;
    for (size_t i = 0; i < task.GetPlanCount(); i++) {
        ss << task[i].ToString() << "#";
    }
    return ss.str();
}

void TimeSeriesMergeStrategyTest::MakeSegmentMergeInfos(const string& str, SegmentMergeInfos& segMergeInfos)
{
    StringTokenizer st;
    st.tokenize(str, ";", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    segmentid_t segId = 0;
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it, ++segId) {
        string segInfoStr = *it;
        StringTokenizer stSeg;
        stSeg.tokenize(segInfoStr, " ", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        assert(stSeg.getNumTokens() == 5 || stSeg.getNumTokens() == 6);

        SegmentMergeInfo segMergeInfo;
        segMergeInfo.segmentId = segId;
        uint32_t docCount = 0;
        autil::StringUtil::strToUInt32(stSeg[0].c_str(), docCount);
        segMergeInfo.segmentInfo.docCount = docCount;

        autil::StringUtil::strToUInt32(stSeg[1].c_str(), segMergeInfo.deletedDocCount);

        int64_t timestamp;
        autil::StringUtil::strToInt64(stSeg[2].c_str(), timestamp);
        segMergeInfo.segmentInfo.timestamp = timestamp;

        int64_t segmentSize = 0;
        autil::StringUtil::strToInt64(stSeg[3].c_str(), segmentSize);
        segMergeInfo.segmentSize = segmentSize * 1024 * 1024;

        indexlib::framework::SegmentMetrics segmentMetrics;
        vector<string> rangeInfo;
        autil::StringUtil::fromString(stSeg[4].c_str(), rangeInfo, "=");
        vector<string> minMaxValue;

        autil::StringUtil::fromString(rangeInfo[1], minMaxValue, ",");
        int64_t min = std::numeric_limits<int64_t>::min();
        int64_t max = std::numeric_limits<int64_t>::max();
        if (!minMaxValue[0].empty()) {
            autil::StringUtil::strToInt64(minMaxValue[0].c_str(), min);
        }
        if (!minMaxValue[1].empty()) {
            autil::StringUtil::strToInt64(minMaxValue[1].c_str(), max);
        }
        autil::legacy::json::JsonMap groupMetics;
        groupMetics["min"] = min;
        groupMetics["max"] = max;
        segmentMetrics.Set(SEGMENT_CUSTOMIZE_METRICS_GROUP, "attribute:" + rangeInfo[0], groupMetics);

        if (stSeg.getNumTokens() == 6) {
            vector<string> tagInfo;
            autil::StringUtil::fromString(stSeg[5].c_str(), tagInfo, "=");
            assert(tagInfo.size() == 2);
            segmentMetrics.Set(SEGMENT_CUSTOMIZE_METRICS_GROUP, tagInfo[0], tagInfo[1]);
        }
        segMergeInfo.segmentMetrics = segmentMetrics;
        segMergeInfos.push_back(segMergeInfo);
    }
}
}} // namespace indexlib::merger
