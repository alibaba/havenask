#include "indexlib/merger/merge_strategy/test/priority_queue_merge_strategy_unittest.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, PriorityQueueMergeStrategyTest);

PriorityQueueMergeStrategyTest::PriorityQueueMergeStrategyTest() {}

PriorityQueueMergeStrategyTest::~PriorityQueueMergeStrategyTest() {}

void PriorityQueueMergeStrategyTest::CaseSetUp() {}

void PriorityQueueMergeStrategyTest::CaseTearDown() {}

void PriorityQueueMergeStrategyTest::TestSimpleProcess()
{
    string mergeParams = "max-doc-count=100;max-valid-doc-count=80|"
                         "priority-feature=doc-count#asc;conflict-segment-count=5;conflict-delete-percent=20|"
                         "max-merged-segment-doc-count=50;max-total-merge-doc-count=50";
    InnerTestPriorityMergeTask("20 10 1", mergeParams, "0", false);
}

void PriorityQueueMergeStrategyTest::TestInputLimits()
{
    // test max doc count
    InnerTestPriorityMergeTask("20 0 0;0 0 0;10 0 0;21 0 0", "max-doc-count=20|conflict-segment-count=1|", "0,1,2",
                               false);
    // test max valid doc count
    InnerTestPriorityMergeTask("20 0 0;0 0 0;10 0 0;21 0 0;21 1 0", "max-valid-doc-count=20|conflict-segment-count=1|",
                               "0,1,2,4", false);

    // test input limit combo
    InnerTestPriorityMergeTask("20 0 0;0 0 0;10 0 0;21 0 0;21 5 0;19 1 0;19 0 0",
                               "max-doc-count=20;max-valid-doc-count=18|conflict-segment-count=1|", "1,2,5", false);
}

void PriorityQueueMergeStrategyTest::TestSelectMergeSegments()
{
    // test conflict segments
    InnerTestPriorityMergeTask("20 0 0;0 0 0;10 0 0;21 0 0", "|conflict-segment-count=2|", "0,1,2,3", false);
    InnerTestPriorityMergeTask("20 0 0;0 0 0;10 0 0;21 0 0", "|conflict-segment-count=3|", "0,1,2", false);

    // test delete percent
    InnerTestPriorityMergeTask("20 11 0;0 0 0;10 5 0;21 10 0", "|conflict-delete-percent=50|", "0", false);
    // test conflict segment & delete percent
    InnerTestPriorityMergeTask("20 11 0;0 0 0;10 5 0;21 10 0;28 15 0",
                               "|conflict-segment-count=2;conflict-delete-percent=50|", "0,1,2,3,4", false);

    // test priority queue: default doc count
    InnerTestPriorityMergeTask("20 10 0;0 0 0;40 20 0;30 15 0;28 15 0",
                               "|conflict-segment-count=1;conflict-delete-percent=0"
                               "|max-merged-segment-doc-count=20",
                               "0,1;4;3;2", false);

    // test priority queue: timestamp
    InnerTestPriorityMergeTask("20 10 5;0 0 3;40 20 2;30 15 4;28 15 1",
                               "|priority-feature=timestamp#asc;conflict-segment-count=1;conflict-delete-percent=0"
                               "|max-merged-segment-doc-count=20",
                               "1,4;2;3;0", false);

    // test priority queue: delete doc
    InnerTestPriorityMergeTask(
        "20 10 5;0 0 3;40 20 2;30 15 4;28 15 1",
        "|priority-feature=delete-doc-count#desc;conflict-segment-count=1;conflict-delete-percent=0"
        "|max-merged-segment-doc-count=20",
        "1,2;3;4;0", false);

    // test priority queue: valid doc count
    InnerTestPriorityMergeTask(
        "20 10 5;0 0 3;40 20 2;30 15 4;28 15 1",
        "|priority-feature=valid-doc-count#asc;conflict-segment-count=1;conflict-delete-percent=0"
        "|max-merged-segment-doc-count=20",
        "0,1;4;3;2", false);
}

void PriorityQueueMergeStrategyTest::TestOutputLimits()
{
    // test max merged segment doc count
    InnerTestPriorityMergeTask("1 0 0;2 0 0;3 0 0;2 0 0;1 0 0;0 0 0;3 0 0",
                               "|conflict-segment-count=1|max-merged-segment-doc-count=6", "0,1,3,4,5;2,6", false);

    InnerTestPriorityMergeTask("5 0 0;2 0 0;3 0 0;4 0 0", "|conflict-segment-count=1|max-merged-segment-doc-count=5",
                               "1,2", false);

    InnerTestPriorityMergeTask("10 5 0;2 0 0;6 3 0;4 0 0",
                               "|priority-feature=valid-doc-count;conflict-segment-count=1;conflict-delete-percent=40"
                               "|max-merged-segment-doc-count=5",
                               "1,2;0", false);

    // test max total merge doc count
    InnerTestPriorityMergeTask("1 0 0;2 0 0;3 0 0;2 0 0;1 0 0;0 0 0;3 0 0",
                               "|conflict-segment-count=1|max-total-merge-doc-count=6", "0,1,3,4,5", false);

    // test output combo
    InnerTestPriorityMergeTask(
        "1 0 0;2 0 0;3 0 0;2 0 0;1 0 0;0 0 0;6 3 0",
        "|priority-feature=valid-doc-count#asc;conflict-segment-count=1;conflict-delete-percent=40|"
        "max-merged-segment-doc-count=4;max-total-merge-doc-count=7",
        "0,1,4,5;6", false);
}

void PriorityQueueMergeStrategyTest::TestMergeForOptimize()
{
    InnerTestPriorityMergeTask(
        "1 0 0;2 0 0;3 0 0;2 0 0;1 0 0;0 0 0;6 3 0",
        "|priority-feature=valid-doc-count#asc;conflict-segment-count=1;conflict-delete-percent=40|"
        "max-merged-segment-doc-count=4;max-total-merge-doc-count=7",
        "0,1,2,3,4,5,6", true);
}

void PriorityQueueMergeStrategyTest::TestMaxSegmentSize()
{
    InnerTestPriorityMergeTask("100 100 0 100;200 200 0 200;", "max-segment-size=150|conflict-delete-percent=50|", "0",
                               false);
}

void PriorityQueueMergeStrategyTest::TestMaxValidSegmentSize()
{
    InnerTestPriorityMergeTask("200 100 0 100;200 100 0 200;", "max-valid-segment-size=90|conflict-delete-percent=20|",
                               "0", false);
}

void PriorityQueueMergeStrategyTest::TestMaxTotalMergeSize()
{
    InnerTestPriorityMergeTask("100 100 0 100;200 200 0 200;", "max-total-merge-size=150|conflict-delete-percent=50|",
                               "0", false);
    InnerTestPriorityMergeTask("100 0 0 400;200 0 0 200;300 0 0 100",
                               "max-total-merge-size=300|conflict-segment-count=1|", "1,2", false);
    InnerTestPriorityMergeTask("100 0 0 100;100 0 0 100;100 0 0 100;100 0 0 100;100 0 0 100;100 0 0 100;",
                               "max-total-merge-size=400|conflict-segment-count=1|max-merged-segment-size=200",
                               "0,1;2,3", false);
}

void PriorityQueueMergeStrategyTest::TestMaxMergedSegmentSize()
{
    InnerTestPriorityMergeTask("100 0 0 100;100 0 0 100;100 0 0 100;100 0 0 100",
                               "|conflict-segment-count=1|max-merged-segment-size=250", "0,1;2,3", false);
    InnerTestPriorityMergeTask("400 300 0 400;", "|conflict-delete-percent=50|max-merged-segment-size=99", "", false);
    InnerTestPriorityMergeTask("100 0 0 400;101 0 0 300;102 0 0 200;103 0 0 100",
                               "|conflict-segment-count=1|max-merged-segment-size=500", "0,3;1,2", false);
    InnerTestPriorityMergeTask("100 90 0 100;200 0 0 200;300 250 0 300;",
                               "|conflict-segment-count=1|max-merged-segment-size=100", "0,2;", false);
    InnerTestPriorityMergeTask("100 0 0 2048;200 10 0 3872;300 10 0 2848;400 10 0 1024;",
                               "|conflict-segment-count=1|max-merged-segment-size=5120", "0,2;1,3;", false);

    InnerTestPriorityMergeTask("100 0 0 2048;200 10 0 3872;300 10 0 2848;400 10 0 1024;",
                               "|conflict-segment-count=1|max-merged-segment-size=5120", "0,2;1,3;", false);
    InnerTestPriorityMergeTask("85983314 0 0 204800;100 0 0 200;200 0 0 200;",
                               "|conflict-segment-count=1|max-merged-segment-size=5120", "1,2;", false);
}

void PriorityQueueMergeStrategyTest::TestMaxTotalMergedSize()
{
    InnerTestPriorityMergeTask("100 0 0 100;100 0 0 100;100 0 0 100;100 0 0 100",
                               "|conflict-segment-count=1|max-total-merged-size=250", "0,1;", false);
    InnerTestPriorityMergeTask("100 0 0 100;100 0 0 100;100 0 0 100;100 0 0 100",
                               "|conflict-segment-count=1|max-total-merged-size=250;max-merged-segment-size=500",
                               "0,1;", false);
    InnerTestPriorityMergeTask("100 90 0 100;200 0 0 200;300 250 0 300;",
                               "|conflict-segment-count=1|max-total-merged-size=100", "0,2;", false);
    InnerTestPriorityMergeTask("1 0 0 5;2 0 0 2;3 0 0 1;4 0 0 4;5 0 0 3;6 0 0 2;",
                               "|conflict-segment-count=1|max-total-merged-size=12;max-merged-segment-size=6",
                               "0,2;1,3;", false);
    InnerTestPriorityMergeTask("1 0 0 5;2 0 0 2;3 0 0 1;4 0 0 4;5 0 0 3;6 0 0 2;",
                               "|conflict-segment-count=1|max-total-merged-size=12;max-merged-segment-size=6",
                               "0,2;1,3;", false);
    InnerTestPriorityMergeTask("100 0 0 3072;100 0 0 3072;100 0 0 3072;100 0 0 3072",
                               "|conflict-segment-count=1|max-total-merged-size=8192", "0,1;", false);
}

void PriorityQueueMergeStrategyTest::TestSetParameter()
{
    // test normal
    InnerTestSetParameter("||", false);
    InnerTestSetParameter("max-doc-count=1000;max-valid-doc-count=20000|"
                          "conflict-segment-count=10;conflict-delete-percent=50|",
                          false);

    InnerTestSetParameter("max-doc-count=1000;max-valid-doc-count=20000|"
                          "priority-feature=doc-count#desc;conflict-segment-count=10;conflict-delete-percent=50|"
                          "max-merged-segment-doc-count=50000;max-total-merge-doc-count=100000",
                          false);

    // test invalid
    InnerTestSetParameter("max-doc-count=abc||", true);
    InnerTestSetParameter("max-valid-doc-count=abc||", true);
    InnerTestSetParameter("not-exist-item=1000||", true);
    InnerTestSetParameter("|priority-feature=not_support|", true);
    InnerTestSetParameter("|priority-feature=doc-count#abc|", true);
    InnerTestSetParameter("|priority-feature=doc-count#asc;conflict-segment-count=abc|", true);
    InnerTestSetParameter("|priority-feature=doc-count#asc;conflict-delete-percent=150|", true);
    InnerTestSetParameter("|priority-feature=doc-count#asc;conflict-delete-percent=-3|", true);
    InnerTestSetParameter("|priority-feature=doc-count#asc;not-exist-item=123|", true);
    InnerTestSetParameter("||max-merged-segment-doc-count=abc", true);
    InnerTestSetParameter("||max-total-merge-doc-count=abc", true);
    InnerTestSetParameter("||not-exist-item=123", true);
}

void PriorityQueueMergeStrategyTest::InnerTestPriorityMergeTask(const string& originalSegs, const string& mergeParams,
                                                                const string& expectMergeSegs, bool optimizeMerge)
{
    SegmentMergeInfos mergeInfos;
    MakeSegmentMergeInfos(originalSegs, mergeInfos);
    MergeTask task = CreateMergeTask(mergeInfos, mergeParams, optimizeMerge);
    CheckMergeTask(expectMergeSegs, task);
}

string PriorityQueueMergeStrategyTest::TaskToString(const MergeTask& task)
{
    stringstream ss;
    for (size_t i = 0; i < task.GetPlanCount(); i++) {
        ss << task[i].ToString() << "#";
    }
    return ss.str();
}

// seg1,seg2;seg3,seg4
void PriorityQueueMergeStrategyTest::CheckMergeTask(const string& expectResults, const MergeTask& task)
{
    vector<vector<segmentid_t>> expectResult;
    StringUtil::fromString(expectResults, expectResult, ",", ";");
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

void PriorityQueueMergeStrategyTest::InnerTestSetParameter(const string& mergeParams, bool badParam)
{
    vector<string> param = StringUtil::split(mergeParams, "|", false);
    MergeStrategyParameter parameter;
    parameter.inputLimitParam = param[0];
    parameter.strategyConditions = param[1];
    parameter.outputLimitParam = param[2];

    SegmentDirectoryPtr segDir;
    IndexPartitionSchemaPtr schema;
    PriorityQueueMergeStrategy ms(segDir, schema);

    if (badParam) {
        ASSERT_THROW(ms.SetParameter(parameter), BadParameterException);
    } else {
        ASSERT_NO_THROW(ms.SetParameter(parameter));
    }
}

MergeTask PriorityQueueMergeStrategyTest::CreateMergeTask(const SegmentMergeInfos& segMergeInfos,
                                                          const string& mergeStrategyParams, bool optmize)
{
    vector<string> param = StringUtil::split(mergeStrategyParams, "|", false);
    MergeStrategyParameter parameter;
    parameter.inputLimitParam = param[0];
    parameter.strategyConditions = param[1];
    parameter.outputLimitParam = param[2];

    SegmentDirectoryPtr segDir;
    IndexPartitionSchemaPtr schema;
    PriorityQueueMergeStrategy ms(segDir, schema);
    ms.SetParameter(parameter);
    string root;
    indexlibv2::framework::LevelInfo levelInfo;
    if (optmize) {
        return ms.CreateMergeTaskForOptimize(segMergeInfos, levelInfo);
    } else {
        return ms.CreateMergeTask(segMergeInfos, levelInfo);
    }
}

// segStr: "docCount deleteDocCount timestamp [segmentSize];docCount ..."
void PriorityQueueMergeStrategyTest::MakeSegmentMergeInfos(const string& str, SegmentMergeInfos& segMergeInfos)
{
    StringTokenizer st;
    st.tokenize(str, ";", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    segmentid_t segId = 0;
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it, ++segId) {
        string segInfoStr = *it;
        StringTokenizer stSeg;
        stSeg.tokenize(segInfoStr, " ", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        assert(stSeg.getNumTokens() == 3 || stSeg.getNumTokens() == 4);

        SegmentMergeInfo segMergeInfo;
        segMergeInfo.segmentId = segId;
        uint32_t docCount = 0;
        StringUtil::strToUInt32(stSeg[0].c_str(), docCount);
        segMergeInfo.segmentInfo.docCount = docCount;

        StringUtil::strToUInt32(stSeg[1].c_str(), segMergeInfo.deletedDocCount);

        int64_t timestamp;
        StringUtil::strToInt64(stSeg[2].c_str(), timestamp);
        segMergeInfo.segmentInfo.timestamp = timestamp;

        if (stSeg.getNumTokens() == 4) {
            int64_t segmentSize = 0;
            StringUtil::strToInt64(stSeg[3].c_str(), segmentSize);
            segMergeInfo.segmentSize = segmentSize * 1024 * 1024;
        }
        segMergeInfos.push_back(segMergeInfo);
    }
}
}} // namespace indexlib::merger
