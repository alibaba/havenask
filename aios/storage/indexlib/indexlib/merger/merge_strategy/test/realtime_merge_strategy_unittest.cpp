#include <string>
#include <vector>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/merger/merge_strategy/realtime_merge_strategy.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {

class RealtimeMergeStrategyTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(RealtimeMergeStrategyTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForSetParameter()
    {
        RealtimeMergeStrategy ms1;
        string badParam1 = RealtimeMergeStrategy::MAX_SMALL_SEGMENT_COUNT + "=10;" +
                           RealtimeMergeStrategy::DO_MERGE_SIZE_THRESHOLD + "=128";
        INDEXLIB_EXPECT_EXCEPTION(ms1.SetParameter(badParam1), BadParameterException);

        RealtimeMergeStrategy ms2;
        string badParam2 = RealtimeMergeStrategy::MAX_SMALL_SEGMENT_COUNT + "=10;" +
                           RealtimeMergeStrategy::DONT_MERGE_SIZE_THRESHOLD + "=128";
        INDEXLIB_EXPECT_EXCEPTION(ms2.SetParameter(badParam2), BadParameterException);

        RealtimeMergeStrategy ms3;
        string badParam3 = RealtimeMergeStrategy::DO_MERGE_SIZE_THRESHOLD + "=160;" +
                           RealtimeMergeStrategy::DONT_MERGE_SIZE_THRESHOLD + "=128";
        INDEXLIB_EXPECT_EXCEPTION(ms3.SetParameter(badParam3), BadParameterException);

        RealtimeMergeStrategy ms4;
        string badParam4 = RealtimeMergeStrategy::DO_MERGE_SIZE_THRESHOLD + "=160;" +
                           RealtimeMergeStrategy::DONT_MERGE_SIZE_THRESHOLD + "=128;" +
                           RealtimeMergeStrategy::MAX_SMALL_SEGMENT_COUNT + "=-1";
        INDEXLIB_EXPECT_EXCEPTION(ms4.SetParameter(badParam4), BadParameterException);

        RealtimeMergeStrategy ms5;
        string badParam5 = RealtimeMergeStrategy::DO_MERGE_SIZE_THRESHOLD + "=160;" +
                           RealtimeMergeStrategy::DONT_MERGE_SIZE_THRESHOLD + "=-1;" +
                           RealtimeMergeStrategy::MAX_SMALL_SEGMENT_COUNT + "=5";
        INDEXLIB_EXPECT_EXCEPTION(ms5.SetParameter(badParam5), BadParameterException);

        RealtimeMergeStrategy ms6;
        string badParam6 = RealtimeMergeStrategy::DO_MERGE_SIZE_THRESHOLD + "=a;" +
                           RealtimeMergeStrategy::DONT_MERGE_SIZE_THRESHOLD + "=128;" +
                           RealtimeMergeStrategy::MAX_SMALL_SEGMENT_COUNT + "=10";
        INDEXLIB_EXPECT_EXCEPTION(ms6.SetParameter(badParam6), BadParameterException);

        RealtimeMergeStrategy ms7;
        string badParam7 = RealtimeMergeStrategy::DO_MERGE_SIZE_THRESHOLD + "=100;" +
                           RealtimeMergeStrategy::DONT_MERGE_SIZE_THRESHOLD + "=128;" +
                           RealtimeMergeStrategy::MAX_SMALL_SEGMENT_COUNT + "=10";
        INDEXLIB_EXPECT_EXCEPTION(ms7.SetParameter(badParam7), BadParameterException);

        RealtimeMergeStrategy ms8;
        string badParam8 = RealtimeMergeStrategy::DO_MERGE_SIZE_THRESHOLD + "=100;" +
                           RealtimeMergeStrategy::DONT_MERGE_SIZE_THRESHOLD + "=128;" +
                           RealtimeMergeStrategy::MAX_SMALL_SEGMENT_COUNT + "=10;" + "not-exist-parameter=10";
        INDEXLIB_EXPECT_EXCEPTION(ms8.SetParameter(badParam8), BadParameterException);

        RealtimeMergeStrategy ms9;
        string goodParam = RealtimeMergeStrategy::MAX_SMALL_SEGMENT_COUNT + "=10;" +
                           RealtimeMergeStrategy::DO_MERGE_SIZE_THRESHOLD + "=160;" +
                           RealtimeMergeStrategy::DONT_MERGE_SIZE_THRESHOLD + "=128";
        ms9.SetParameter(goodParam);
        INDEXLIB_TEST_EQUAL(ms9.GetParameter(), goodParam);
    }

    void TestCaseForCreateMergeStrategy()
    {
        {
            // need merge
            string segStr = "1024 0 128;1024 0 70;1020 0 25;2048 0 60";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);

            MergeTask task = CreateMergeTask(segMergeInfos);
            INDEXLIB_TEST_EQUAL((size_t)1, task.GetPlanCount());

            const MergePlan& plan = task[0];
            INDEXLIB_TEST_EQUAL((size_t)3, plan.GetSegmentCount());

            vector<segmentid_t> answer;
            answer.push_back(1);
            answer.push_back(2);
            answer.push_back(3);
            CheckMergePlan(answer, plan);
        }

        {
            // need not merge
            string segStr = "100 0 20;100 0 40";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);

            MergeTask task = CreateMergeTask(segMergeInfos);
            INDEXLIB_TEST_EQUAL((size_t)0, task.GetPlanCount());
        }

        {
            // need merge, although (20 + 40 + 30) < 100
            string segStr = "100 0 20;100 0 40;100 0 30";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);

            MergeTask task = CreateMergeTask(segMergeInfos);
            INDEXLIB_TEST_EQUAL((size_t)1, task.GetPlanCount());

            const MergePlan& plan = task[0];

            vector<segmentid_t> answer;
            answer.push_back(0);
            answer.push_back(1);
            answer.push_back(2);
            CheckMergePlan(answer, plan);
        }

        {
            // need merge
            string segStr = "100 0 70;100 0 40";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);

            MergeTask task = CreateMergeTask(segMergeInfos);
            INDEXLIB_TEST_EQUAL((size_t)1, task.GetPlanCount());

            const MergePlan& plan = task[0];
            vector<segmentid_t> answer;
            answer.push_back(0);
            answer.push_back(1);
            CheckMergePlan(answer, plan);
        }

        {
            // need not merge
            string segStr = "100 0 20;100 0 90;100 0 30;100 0 40";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);

            MergeTask task = CreateMergeTask(segMergeInfos);
            INDEXLIB_TEST_EQUAL((size_t)0, task.GetPlanCount());
        }

        {
            // need merge
            string segStr = "100 0 20;100 0 90;100 0 30;100 0 75";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);

            MergeTask task = CreateMergeTask(segMergeInfos);
            INDEXLIB_TEST_EQUAL((size_t)1, task.GetPlanCount());

            const MergePlan& plan = task[0];
            vector<segmentid_t> answer;
            answer.push_back(2);
            answer.push_back(3);
            CheckMergePlan(answer, plan);
        }

        {
            // need  merge
            string segStr = "100 0 20;100 0 90;100 0 30;100 0 20;100 0 30";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);

            MergeTask task = CreateMergeTask(segMergeInfos);
            INDEXLIB_TEST_EQUAL((size_t)1, task.GetPlanCount());

            const MergePlan& plan = task[0];
            vector<segmentid_t> answer;
            answer.push_back(2);
            answer.push_back(3);
            answer.push_back(4);
            CheckMergePlan(answer, plan);
        }
    }

private:
    MergeTask CreateMergeTask(const SegmentMergeInfos& segMergeInfos, bool optimize = false)
    {
        RealtimeMergeStrategy ms;
        string str = RealtimeMergeStrategy::MAX_SMALL_SEGMENT_COUNT + "=3;" +
                     RealtimeMergeStrategy::DO_MERGE_SIZE_THRESHOLD + "=100;" +
                     RealtimeMergeStrategy::DONT_MERGE_SIZE_THRESHOLD + "=80";

        ms.SetParameter(str);
        indexlibv2::framework::LevelInfo levelInfo;
        if (optimize) {
            return ms.CreateMergeTaskForOptimize(segMergeInfos, levelInfo);
        } else {
            return ms.CreateMergeTask(segMergeInfos, levelInfo);
        }
    }

    void MakeSegmentMergeInfos(const string& segStr, SegmentMergeInfos& segMergeInfos)
    {
        StringTokenizer st;
        st.tokenize(segStr, ";", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        segmentid_t segId = 0;
        for (StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it, ++segId) {
            string segInfoStr = *it;
            StringTokenizer stSeg;
            stSeg.tokenize(segInfoStr, " ", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
            assert(stSeg.getNumTokens() == 3);

            SegmentMergeInfo segMergeInfo;
            segMergeInfo.segmentId = segId;
            uint32_t docCount = 0;
            StringUtil::strToUInt32(stSeg[0].c_str(), docCount);
            segMergeInfo.segmentInfo.docCount = docCount;
            StringUtil::strToUInt32(stSeg[1].c_str(), segMergeInfo.deletedDocCount);
            StringUtil::strToInt64(stSeg[2].c_str(), segMergeInfo.segmentSize);
            segMergeInfo.segmentSize <<= 20;
            segMergeInfos.push_back(segMergeInfo);
        }
    }

    void CheckMergePlan(const vector<segmentid_t>& answer, const MergePlan& plan)
    {
        INDEXLIB_TEST_EQUAL(answer.size(), plan.GetSegmentCount());
        MergePlan::Iterator segIter = plan.CreateIterator();
        uint32_t idx = 0;
        while (segIter.HasNext()) {
            segmentid_t segId = segIter.Next();
            INDEXLIB_TEST_EQUAL(answer[idx], segId);
            idx++;
        }
    }
};

INDEXLIB_UNIT_TEST_CASE(RealtimeMergeStrategyTest, TestCaseForSetParameter);
INDEXLIB_UNIT_TEST_CASE(RealtimeMergeStrategyTest, TestCaseForCreateMergeStrategy);
}} // namespace indexlib::merger
