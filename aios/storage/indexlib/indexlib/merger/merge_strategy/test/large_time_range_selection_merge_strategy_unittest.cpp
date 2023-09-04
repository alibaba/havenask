#include "indexlib/merger/merge_strategy/test/large_time_range_selection_merge_strategy_unittest.h"

#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;
using namespace indexlib::config;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, LargeTimeRangeSelectionMergeStrategyTest);

LargeTimeRangeSelectionMergeStrategyTest::LargeTimeRangeSelectionMergeStrategyTest() {}

LargeTimeRangeSelectionMergeStrategyTest::~LargeTimeRangeSelectionMergeStrategyTest() {}

void LargeTimeRangeSelectionMergeStrategyTest::CaseSetUp()
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

void LargeTimeRangeSelectionMergeStrategyTest::CaseTearDown() {}

void LargeTimeRangeSelectionMergeStrategyTest::TestSimpleProcess()
{
    string mergeParam = "attribute-field=time;|input-interval=100;|";
    InnerTestLargeTimeRangeSelectionMergeTask("20 10 1 1 time=10,15;20 10 1 1 time=13,17", mergeParam, "", false);
    InnerTestLargeTimeRangeSelectionMergeTask("20 10 1 1 time=10,15;20 10 1 1 time=13,200", mergeParam, "1", false);
}

MergeTask LargeTimeRangeSelectionMergeStrategyTest::CreateMergeTask(const SegmentMergeInfos& segMergeInfos,
                                                                    const string& mergeStrategyParams, bool optmize)
{
    vector<string> param = autil::StringUtil::split(mergeStrategyParams, "|", false);
    MergeStrategyParameter parameter;
    parameter.inputLimitParam = param[0];
    parameter.strategyConditions = param[1];
    parameter.outputLimitParam = param[2];

    SegmentDirectoryPtr segDir;
    LargeTimeRangeSelectionMergeStrategy ms(segDir, mSchema);
    ms.SetParameter(parameter);
    string root;
    indexlibv2::framework::LevelInfo levelInfo;
    if (optmize) {
        return ms.CreateMergeTaskForOptimize(segMergeInfos, levelInfo);
    } else {
        return ms.CreateMergeTask(segMergeInfos, levelInfo);
    }
}

void LargeTimeRangeSelectionMergeStrategyTest::CheckMergeTask(const string& expectResults, const MergeTask& task)
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

void LargeTimeRangeSelectionMergeStrategyTest::InnerTestLargeTimeRangeSelectionMergeTask(const string& originalSegs,
                                                                                         const string& mergeParams,
                                                                                         const string& expectMergeSegs,
                                                                                         bool optimizeMerge)
{
    SegmentMergeInfos mergeInfos;
    MakeSegmentMergeInfos(originalSegs, mergeInfos);
    MergeTask task = CreateMergeTask(mergeInfos, mergeParams, optimizeMerge);
    CheckMergeTask(expectMergeSegs, task);
}

std::string LargeTimeRangeSelectionMergeStrategyTest::TaskToString(const MergeTask& task)
{
    stringstream ss;
    for (size_t i = 0; i < task.GetPlanCount(); i++) {
        ss << task[i].ToString() << "#";
    }
    return ss.str();
}

void LargeTimeRangeSelectionMergeStrategyTest::MakeSegmentMergeInfos(const string& str,
                                                                     SegmentMergeInfos& segMergeInfos)
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
