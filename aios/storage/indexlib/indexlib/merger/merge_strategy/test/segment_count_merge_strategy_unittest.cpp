#include "indexlib/merger/merge_strategy/segment_count_merge_strategy.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace indexlib::config;

namespace indexlib { namespace merger {

class SegmentCountMergeStrategyTest : public INDEXLIB_TESTBASE
{
public:
    SegmentCountMergeStrategyTest() {};
    ~SegmentCountMergeStrategyTest() {};
    DECLARE_CLASS_NAME(SegmentCountMergeStrategyTest);

public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}
    void TestSimpleProcess();

private:
    void DoTestSimple(uint32_t paramSegmentCount, std::vector<std::pair<uint32_t, uint32_t>> segmentInfos,
                      std::set<segmentid_t> mergedSegIds, std::set<segmentid_t> expectMergeSegIds);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SegmentCountMergeStrategyTest, TestSimpleProcess);

IE_LOG_SETUP(merger, SegmentCountMergeStrategyTest);

void SegmentCountMergeStrategyTest::TestSimpleProcess()
{
    DoTestSimple(3, {}, {}, {});
    DoTestSimple(4, {}, {}, {});
    DoTestSimple(3, {{3, 2}, {2, 2}}, {}, {0});
    DoTestSimple(3, {{1, 0}, {2, 2}}, {}, {1});
    DoTestSimple(3, {{9, 3}, {8, 0}, {10, 0}, {1, 0}}, {}, {0, 3});
    DoTestSimple(3, {{9, 3}, {8, 0}, {10, 0}, {1, 0}}, {0, 1, 2, 3}, {0, 3});
    DoTestSimple(3, {{9, 3}, {8, 0}, {10, 0}, {1, 0}}, {0, 1, 2}, {0, 3});
    DoTestSimple(3, {{9, 3}, {8, 0}, {10, 0}, {1, 0}}, {0, 1}, {2, 3});
    DoTestSimple(3, {{9, 3}, {8, 0}, {10, 0}, {1, 0}}, {0, 2}, {1, 3});
}

// @param segmentInfos: [(docCount, deleteDocCount)]
void SegmentCountMergeStrategyTest::DoTestSimple(uint32_t paramSegmentCount,
                                                 std::vector<std::pair<uint32_t, uint32_t>> segmentInfos,
                                                 std::set<segmentid_t> mergedSegIds,
                                                 std::set<segmentid_t> expectMergeSegIds)
{
    MergeStrategyParameter param;
    param.outputLimitParam = "segment-count=" + std::to_string(paramSegmentCount);
    SegmentCountMergeStrategy mergeStrategy;
    mergeStrategy.SetParameter(param);

    index_base::SegmentMergeInfos segmentMergeInfos;
    for (size_t i = 0; i < segmentInfos.size(); ++i) {
        index_base::SegmentMergeInfo segmentMergeInfo;
        segmentMergeInfo.segmentId = i;
        if (mergedSegIds.count(segmentMergeInfo.segmentId) > 0) {
            segmentMergeInfo.segmentInfo.mergedSegment = true;
        }
        segmentMergeInfo.segmentSize = int64_t(segmentInfos[i].first) * 1024 * 1024 * 1024;
        segmentMergeInfo.deletedDocCount = segmentInfos[i].second;
        segmentMergeInfo.segmentInfo.docCount = segmentInfos[i].first;
        segmentMergeInfos.push_back(segmentMergeInfo);
    }
    indexlibv2::framework::LevelInfo levelInfo;
    MergeTask mergeTask = mergeStrategy.CreateMergeTask(segmentMergeInfos, levelInfo);
    if (expectMergeSegIds.empty()) {
        ASSERT_TRUE(expectMergeSegIds.empty());
        return;
    }

    ASSERT_EQ(1, mergeTask.GetPlanCount());
    std::set<segmentid_t> actualSegmentIds;
    for (auto iter = mergeTask[0].CreateIterator(); iter.HasNext();) {
        actualSegmentIds.insert(iter.Next());
    }
    ASSERT_EQ(expectMergeSegIds, actualSegmentIds);
}
}} // namespace indexlib::merger
