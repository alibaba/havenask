#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/merger/merge_strategy/balance_tree_merge_strategy.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {

class BalanceTreeMergeStrategyTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(BalanceTreeMergeStrategyTest);
    void CaseSetUp() override { mDir = GET_TEMP_DATA_PATH(); }

    void CaseTearDown() override {}

    void TestCaseForMergeStrategy()
    {
        {
            //(0, 1, 2), 3
            //(4, 5)
            // 6, 7
            // merge: 0, 1, 2, 4, 5
            string segStr = "1024 30;1024 2;1024 1;1024 2;"
                            "3072 1;3072 1;"
                            "9216 1;8192 1";

            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);
            MergeTask mergeTask1 = CreateMergeTask(segMergeInfos);
            MergePlan& plan1 = mergeTask1[0];
            INDEXLIB_TEST_EQUAL((size_t)5, plan1.GetSegmentCount());

            vector<segmentid_t> answer;
            answer.push_back(0);
            answer.push_back(1);
            answer.push_back(2);
            answer.push_back(4);
            answer.push_back(5);
            CheckMergePlan(answer, plan1);
        }

        {
            //(0, 1, 2),3
            // 4
            //(5, 6, 7), 8
            // Merge 0, 1, 2, 5, 6, 7
            string segStr = "1024 30;1024 2;1024 1;1024 2;"
                            "3072 1;"
                            "4096 1;4096 1;4096 1;4096 1";

            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);
            MergeTask mergeTask = CreateMergeTask(segMergeInfos);
            MergePlan& plan = mergeTask[0];
            vector<segmentid_t> answer;
            answer.push_back(0);
            answer.push_back(1);
            answer.push_back(2);
            answer.push_back(5);
            answer.push_back(6);
            answer.push_back(7);
            CheckMergePlan(answer, plan);
        }

        {
            //(0, 1)
            //(2, 3, 4)
            // 5, 6
            // firstly merge 2, 3, 4 to x, then merge 0, 1, x
            string segStr = "1024 30;1024 2;"
                            "3072 3000;3072 3000;3072 3000;"
                            "4096 1;4096 1";

            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);
            MergeTask mergeTask = CreateMergeTask(segMergeInfos);
            MergePlan& plan = mergeTask[0];
            vector<segmentid_t> answer;
            answer.push_back(0);
            answer.push_back(1);
            answer.push_back(2);
            answer.push_back(3);
            answer.push_back(4);
            CheckMergePlan(answer, plan);
        }

        {
            //(1, 2, 6), 7
            //(3, 4)
            // 5, 0
            // Merge 1, 2, 6, 3, 4, (seg 0 exceeds the max doc count)
            string segStr = "10240 0;1024 624;"
                            "1024 424;3072 972; 3072 772;"
                            "5120 120;1024 824;1024 424";

            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);
            MergeTask mergeTask1 = CreateMergeTask(segMergeInfos);
            MergePlan& plan1 = mergeTask1[0];
            vector<segmentid_t> answer;
            answer.push_back(1);
            answer.push_back(2);
            answer.push_back(3);
            answer.push_back(4);
            answer.push_back(6);
            CheckMergePlan(answer, plan1);
        }
    }

    void TestCaseForMergeStrategyForOptimize()
    {
        string segStr = "1024 30;1024 2;1024 1;1024 2;"
                        "3072 1;3072 1;"
                        "9216 1;8192 1";

        SegmentMergeInfos segMergeInfos;
        MakeSegmentMergeInfos(segStr, segMergeInfos);
        MergeTask mergeTask = CreateMergeTask(segMergeInfos, true);
        MergePlan& plan = mergeTask[0];
        vector<segmentid_t> answer;
        answer.push_back(0);
        answer.push_back(1);
        answer.push_back(2);
        answer.push_back(3);
        answer.push_back(4);
        answer.push_back(5);
        answer.push_back(6);
        answer.push_back(7);
        CheckMergePlan(answer, plan);
    }

    void TestCaseForSetParameter()
    {
        BalanceTreeMergeStrategy ms1;
        string badParam1 = "conflict-segment-number=3;base-doc-count=1024";
        INDEXLIB_EXPECT_EXCEPTION(ms1.SetParameter(badParam1), BadParameterException);

        BalanceTreeMergeStrategy ms2;
        string badParam2 = "conflict-segment-number=3;base-doc-count=-10;max-doc-count=8000";
        INDEXLIB_EXPECT_EXCEPTION(ms2.SetParameter(badParam2), BadParameterException);

        BalanceTreeMergeStrategy ms3;
        string badParam3 = "conflict-segment-numbera=3;base-doc-count=102400;max-doc-count=8000";
        INDEXLIB_EXPECT_EXCEPTION(ms3.SetParameter(badParam3), BadParameterException);

        BalanceTreeMergeStrategy ms4;
        string goodParam = "base-doc-count=102400;max-doc-count=8000;conflict-segment-number=3";
        ms4.SetParameter(goodParam);
        INDEXLIB_TEST_EQUAL(ms4.GetParameter(), goodParam + ";conflict-delete-percent=100;max-valid-doc-count=" +
                                                    autil::StringUtil::toString(uint32_t(-1)));

        BalanceTreeMergeStrategy ms5;
        string goodParam2 = "base-doc-count=102400;max-doc-count=8000;conflict-segment-number=3;"
                            "conflict-delete-percent=50;max-valid-doc-count=30720";
        ms5.SetParameter(goodParam2);
        INDEXLIB_TEST_EQUAL(ms5.GetParameter(), goodParam2);
    }

    void TestCaseForMergeStrategyWithMaxValidDocCount()
    {
        {
            // 30720 - 21000 < maxValidDocCount(10240)
            string segStr = "30720 21000";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);
            MergeTask mergeTask1 = CreateMergeTask(segMergeInfos);
            INDEXLIB_TEST_EQUAL((size_t)1, mergeTask1.GetPlanCount());
            MergePlan& plan1 = mergeTask1[0];
            INDEXLIB_TEST_EQUAL((size_t)1, plan1.GetSegmentCount());
            vector<segmentid_t> answer;
            answer.push_back(0);
            CheckMergePlan(answer, plan1);
        }
        {
            // 30720 - 16000 > maxValidDocCount(10240)
            string segStr = "30720 16000";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);
            MergeTask mergeTask1 = CreateMergeTask(segMergeInfos);
            INDEXLIB_TEST_EQUAL((size_t)0, mergeTask1.GetPlanCount());
        }
        {
            string segStr = "1024 12;1024 13;1024 56;"
                            "3072 413;3072 2000;"
                            "30720 15000;30720 21000;";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);
            MergeTask mergeTask1 = CreateMergeTask(segMergeInfos);
            INDEXLIB_TEST_EQUAL((size_t)2, mergeTask1.GetPlanCount());
            MergePlan& plan1 = mergeTask1[0];
            INDEXLIB_TEST_EQUAL((size_t)5, plan1.GetSegmentCount());
            vector<segmentid_t> answer;
            answer.push_back(0);
            answer.push_back(1);
            answer.push_back(2);
            answer.push_back(3);
            answer.push_back(4);
            CheckMergePlan(answer, plan1);
            plan1 = mergeTask1[1];
            INDEXLIB_TEST_EQUAL((size_t)1, plan1.GetSegmentCount());
            answer.clear();
            answer.push_back(6);
            CheckMergePlan(answer, plan1);
        }
        {
            string segStr = "1024 12;1024 13;1024 56;"
                            "3072 100;3072 2000;"
                            "30720 21000;";
            SegmentMergeInfos segMergeInfos;
            string strategyString = "conflict-segment-number=3;"
                                    "base-doc-count=1024;"
                                    "max-doc-count=8000;"
                                    "max-valid-doc-count=2000;";
            MakeSegmentMergeInfos(segStr, segMergeInfos);
            MergeTask mergeTask1 = CreateMergeTask(segMergeInfos, false, strategyString);
            INDEXLIB_TEST_EQUAL((size_t)1, mergeTask1.GetPlanCount());
            MergePlan& plan1 = mergeTask1[0];
            INDEXLIB_TEST_EQUAL((size_t)5, plan1.GetSegmentCount());
            vector<segmentid_t> answer;
            answer.push_back(0);
            answer.push_back(1);
            answer.push_back(2);
            answer.push_back(3);
            answer.push_back(4);
            CheckMergePlan(answer, plan1);
        }
    }

    void TestCaseForMergeStrategyWithDelPercent()
    {
        {
            string segStr = "1024 700";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);
            MergeTask mergeTask1 = CreateMergeTask(segMergeInfos);
            INDEXLIB_TEST_EQUAL((size_t)1, mergeTask1.GetPlanCount());
            MergePlan& plan1 = mergeTask1[0];
            INDEXLIB_TEST_EQUAL((size_t)1, plan1.GetSegmentCount());
            vector<segmentid_t> answer;
            answer.push_back(0);
            CheckMergePlan(answer, plan1);
        }
        {
            string segStr = "1024 10;1024 800;1024 23";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);
            MergeTask mergeTask1 = CreateMergeTask(segMergeInfos);
            MergePlan& plan1 = mergeTask1[0];
            INDEXLIB_TEST_EQUAL((size_t)3, plan1.GetSegmentCount());
            vector<segmentid_t> answer;
            answer.push_back(0);
            answer.push_back(1);
            answer.push_back(2);
            CheckMergePlan(answer, plan1);
        }
        {
            string segStr = "1024 10;1024 800;1024 23;"
                            "8192 5000";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);
            MergeTask mergeTask1 = CreateMergeTask(segMergeInfos);
            MergePlan& plan1 = mergeTask1[0];
            INDEXLIB_TEST_EQUAL((size_t)3, plan1.GetSegmentCount());
            vector<segmentid_t> answer;
            answer.push_back(0);
            answer.push_back(1);
            answer.push_back(2);
            CheckMergePlan(answer, plan1);
            plan1 = mergeTask1[1];
            INDEXLIB_TEST_EQUAL((size_t)1, plan1.GetSegmentCount());
            answer.clear();
            answer.push_back(3);
            CheckMergePlan(answer, plan1);
        }
        {
            string segStr = "1024 10;1024 800;1024 23;"
                            "3072 12;3072 34;"
                            "8192 5000;9000 6000";
            SegmentMergeInfos segMergeInfos;
            MakeSegmentMergeInfos(segStr, segMergeInfos);
            MergeTask mergeTask1 = CreateMergeTask(segMergeInfos);
            MergePlan& plan1 = mergeTask1[0];
            INDEXLIB_TEST_EQUAL((size_t)5, plan1.GetSegmentCount());
            vector<segmentid_t> answer;
            answer.push_back(0);
            answer.push_back(1);
            answer.push_back(2);
            answer.push_back(3);
            answer.push_back(4);
            CheckMergePlan(answer, plan1);
            plan1 = mergeTask1[1];
            INDEXLIB_TEST_EQUAL((size_t)1, plan1.GetSegmentCount());
            answer.clear();
            answer.push_back(5);
            CheckMergePlan(answer, plan1);
        }
    }

    void TestCaseForMergeStrategyWithDefaultDelPercent()
    {
        string segStr = "1024 700";
        SegmentMergeInfos segMergeInfos;
        MakeSegmentMergeInfos(segStr, segMergeInfos);
        string strategyString = "conflict-segment-number=3;"
                                "base-doc-count=1024;"
                                "max-doc-count=8000;";
        MergeTask mergeTask1 = CreateMergeTask(segMergeInfos, false, strategyString);
        INDEXLIB_TEST_EQUAL((size_t)0, mergeTask1.GetPlanCount());
    }

    void TestCaseForMergeStrategyWithDefaultMaxValidDocCount()
    {
        string segStr = "30720 16000";
        SegmentMergeInfos segMergeInfos;
        MakeSegmentMergeInfos(segStr, segMergeInfos);
        string strategyString = "conflict-segment-number=3;"
                                "base-doc-count=1024;"
                                "max-doc-count=8000;"
                                "conflict-delete-percent=50;";
        MergeTask mergeTask1 = CreateMergeTask(segMergeInfos, false, strategyString);
        INDEXLIB_TEST_EQUAL((size_t)1, mergeTask1.GetPlanCount());
        MergePlan& plan1 = mergeTask1[0];
        INDEXLIB_TEST_EQUAL((size_t)1, plan1.GetSegmentCount());
        vector<segmentid_t> answer;
        answer.push_back(0);
        CheckMergePlan(answer, plan1);
    }

private:
    void MakeSegmentMergeInfos(const string& str, SegmentMergeInfos& segMergeInfos)
    {
        StringTokenizer st;
        st.tokenize(str, ";", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        segmentid_t segId = 0;
        for (StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it, ++segId) {
            string segInfoStr = *it;
            StringTokenizer stSeg;
            stSeg.tokenize(segInfoStr, " ", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
            assert(stSeg.getNumTokens() == 2);

            SegmentMergeInfo segMergeInfo;
            segMergeInfo.segmentId = segId;
            uint32_t docCount = 0;
            StringUtil::strToUInt32(stSeg[0].c_str(), docCount);
            segMergeInfo.segmentInfo.docCount = docCount;
            StringUtil::strToUInt32(stSeg[1].c_str(), segMergeInfo.deletedDocCount);
            segMergeInfos.push_back(segMergeInfo);
        }
    }

    MergeTask CreateMergeTask(const SegmentMergeInfos& segMergeInfos, bool optmize = false,
                              std::string strategyString = "")
    {
        BalanceTreeMergeStrategy ms;
        if (strategyString.empty()) {
            strategyString = "conflict-segment-number=3;"
                             "base-doc-count=1024;"
                             "max-doc-count=8000;"
                             "conflict-delete-percent=50;"
                             "max-valid-doc-count=10240;";
        }

        ms.SetParameter(strategyString);
        string root;
        indexlibv2::framework::LevelInfo levelInfo;
        if (optmize) {
            return ms.CreateMergeTaskForOptimize(segMergeInfos, levelInfo);
        } else {
            return ms.CreateMergeTask(segMergeInfos, levelInfo);
        }
    }

    void CheckMergePlan(const vector<segmentid_t>& answer, const MergePlan& plan)
    {
        assert(answer.size() == plan.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(answer.size(), plan.GetSegmentCount());
        MergePlan::Iterator segIter = plan.CreateIterator();
        uint32_t idx = 0;
        while (segIter.HasNext()) {
            segmentid_t segId = segIter.Next();
            INDEXLIB_TEST_EQUAL(answer[idx], segId);
            idx++;
        }
    }

private:
    string mDir;
};

INDEXLIB_UNIT_TEST_CASE(BalanceTreeMergeStrategyTest, TestCaseForMergeStrategy);
INDEXLIB_UNIT_TEST_CASE(BalanceTreeMergeStrategyTest, TestCaseForMergeStrategyForOptimize);
INDEXLIB_UNIT_TEST_CASE(BalanceTreeMergeStrategyTest, TestCaseForSetParameter);
INDEXLIB_UNIT_TEST_CASE(BalanceTreeMergeStrategyTest, TestCaseForMergeStrategyWithDelPercent);
INDEXLIB_UNIT_TEST_CASE(BalanceTreeMergeStrategyTest, TestCaseForMergeStrategyWithMaxValidDocCount);
INDEXLIB_UNIT_TEST_CASE(BalanceTreeMergeStrategyTest, TestCaseForMergeStrategyWithDefaultDelPercent);
INDEXLIB_UNIT_TEST_CASE(BalanceTreeMergeStrategyTest, TestCaseForMergeStrategyWithDefaultMaxValidDocCount);
}} // namespace indexlib::merger
