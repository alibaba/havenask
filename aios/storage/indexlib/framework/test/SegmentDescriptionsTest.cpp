#include "indexlib/framework/SegmentDescriptions.h"

#include "unittest/unittest.h"

namespace indexlibv2::framework {

class SegmentDescriptionsTest : public TESTBASE
{
public:
    SegmentDescriptionsTest() = default;
    ~SegmentDescriptionsTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
    std::shared_ptr<framework::LevelInfo> MakeLevelInfo(const LevelTopology topology, const size_t levelNum,
                                                        const size_t shardCount, const std::string& levelStr) const
    {
        auto levelInfo = std::make_shared<LevelInfo>();
        levelInfo->Init(topology, levelNum, shardCount);
        std::vector<std::string> segmentIdStrs = autil::StringUtil::split(levelStr, ";", false);
        assert(segmentIdStrs.size() == levelNum);
        for (size_t level = 0; level < levelNum; level++) {
            std::vector<segmentid_t> segmentIds;
            autil::StringUtil::fromString(segmentIdStrs[level], segmentIds, ",");
            levelInfo->levelMetas[level].segments = segmentIds;
        }
        return levelInfo;
    }
    std::shared_ptr<SegmentDescriptions> MakeSegmentDescriptions(const std::vector<segmentid_t>& metaIds,
                                                                 const std::vector<segmentid_t>& statsIds,
                                                                 std::shared_ptr<LevelInfo> levelInfo)
    {
        auto segmentDescriptions = std::make_shared<SegmentDescriptions>();
        std::vector<SegmentStatistics> segmentStats;
        SegmentStatistics::IntegerStatsMapType integerStats;
        SegmentStatistics::StringStatsMapType strStatsMap;
        for (const auto statsId : statsIds) {
            segmentStats.emplace_back(statsId, integerStats, strStatsMap);
        }
        segmentDescriptions->SetSegmentStatistics(segmentStats);
        segmentDescriptions->SetLevelInfo(levelInfo);
        return segmentDescriptions;
    }
    void CheckLevelInfo(const std::shared_ptr<LevelInfo>& lhs, const std::shared_ptr<LevelInfo>& rhs)
    {
        ASSERT_EQ(lhs->GetTopology(), rhs->GetTopology());
        ASSERT_EQ(lhs->GetLevelCount(), rhs->GetLevelCount());
        ASSERT_EQ(lhs->GetShardCount(), rhs->GetShardCount());
        for (size_t i = 0; i < lhs->GetLevelCount(); i++) {
            ASSERT_EQ(lhs->levelMetas[i].segments.size(), rhs->levelMetas[i].segments.size());
            for (size_t j = 0; j < lhs->levelMetas[i].segments.size(); j++) {
                ASSERT_EQ(lhs->levelMetas[i].segments[j], rhs->levelMetas[i].segments[j]);
            }
        }
    }
    void CheckSegmentDescriptions(const std::shared_ptr<SegmentDescriptions>& segmentDescs,
                                  const std::vector<segmentid_t>& metaIds, const std::vector<segmentid_t>& statsIds,
                                  const std::shared_ptr<LevelInfo>& levelInfo)
    {
        const std::vector<SegmentStatistics>& segmentStats = segmentDescs->GetSegmentStatisticsVector();
        ASSERT_EQ(statsIds.size(), segmentStats.size());
        for (size_t i = 0; i < statsIds.size(); i++) {
            ASSERT_EQ(statsIds[i], segmentStats[i].GetSegmentId());
        }
        ASSERT_TRUE(segmentDescs->GetLevelInfo() != nullptr);
        CheckLevelInfo(segmentDescs->GetLevelInfo(), levelInfo);
    }
};

TEST_F(SegmentDescriptionsTest, TestSimpleImport)
{
    {
        auto baseLevelInfo = MakeLevelInfo(topo_hash_mod, 3, 2, "536870912,536870914;0,-1;-1,2");
        auto baseSegmentDescs = MakeSegmentDescriptions({536870912, 536870913}, {536870913, 536870914}, baseLevelInfo);

        auto levelInfo1 = MakeLevelInfo(topo_hash_mod, 3, 2, "536870913,536870915,536870917;-1,-1;-1,-1");
        auto segmentDescs1 = MakeSegmentDescriptions({536870913, 536870915}, {536870913, 536870917}, levelInfo1);

        auto levelInfo2 = MakeLevelInfo(topo_hash_mod, 3, 2, "536870912,536870914,536870916;-1,-1;-1,-1");
        auto segmentDescs2 = MakeSegmentDescriptions({536870912, 536870916}, {536870914, 536870916}, levelInfo2);

        ASSERT_TRUE(baseSegmentDescs->Import({segmentDescs1, segmentDescs2}, {536870915, 536870916, 536870917}).IsOK());
        auto finalLevelInfo =
            MakeLevelInfo(topo_hash_mod, 3, 2, "536870912,536870914,536870915,536870917,536870916;0,-1;-1,2");
        CheckSegmentDescriptions(baseSegmentDescs, {536870912, 536870913, 536870915, 536870916},
                                 {536870913, 536870914, 536870917, 536870916}, finalLevelInfo);
    }
    {
        // otherLevelInfos is empty
        auto baseLevelInfo = MakeLevelInfo(topo_hash_mod, 3, 2, "536870912,536870914;0,-1;-1,2");
        auto baseSegmentDescs = MakeSegmentDescriptions({536870912, 536870913}, {536870913, 536870914}, baseLevelInfo);

        auto segmentDescs1 = MakeSegmentDescriptions({536870913, 536870915}, {536870913, 536870917}, nullptr);

        auto segmentDescs2 = MakeSegmentDescriptions({536870912, 536870916}, {536870914, 536870916}, nullptr);

        ASSERT_TRUE(baseSegmentDescs->Import({segmentDescs1, segmentDescs2}, {536870915, 536870916, 536870917}).IsOK());
        auto finalLevelInfo = MakeLevelInfo(topo_hash_mod, 3, 2, "536870912,536870914;0,-1;-1,2");
        CheckSegmentDescriptions(baseSegmentDescs, {536870912, 536870913, 536870915, 536870916},
                                 {536870913, 536870914, 536870917, 536870916}, finalLevelInfo);
    }
}

TEST_F(SegmentDescriptionsTest, TestEmptyBase)
{
    auto baseSegmentDescs = MakeSegmentDescriptions({}, {}, nullptr);

    auto levelInfo1 = MakeLevelInfo(topo_hash_mod, 3, 2, "536870913,536870915;-1,-1;-1,-1");
    auto segmentDescs1 = MakeSegmentDescriptions({536870913, 536870915}, {536870913}, levelInfo1);

    auto levelInfo2 = MakeLevelInfo(topo_hash_mod, 3, 2, "536870912,536870914;-1,-1;-1,-1");
    auto segmentDescs2 = MakeSegmentDescriptions({536870912}, {536870914}, levelInfo2);

    ASSERT_TRUE(
        baseSegmentDescs->Import({segmentDescs1, segmentDescs2}, {536870912, 536870913, 536870914, 536870915}).IsOK());
    auto finalLevelInfo = MakeLevelInfo(topo_hash_mod, 3, 2, "536870913,536870915,536870912,536870914;-1,-1;-1,-1");
    CheckSegmentDescriptions(baseSegmentDescs, {536870913, 536870915, 536870912}, {536870913, 536870914},
                             finalLevelInfo);
}

TEST_F(SegmentDescriptionsTest, TestLevelInfoIsNull)
{
    auto baseSegmentDescs = MakeSegmentDescriptions({536870912, 536870913}, {536870913, 536870914}, nullptr);

    auto levelInfo1 = MakeLevelInfo(topo_sequence, 2, 1, "536870913,536870915,536870917;1");
    auto segmentDescs1 = MakeSegmentDescriptions({536870913, 536870915}, {536870913, 536870917}, levelInfo1);

    auto levelInfo2 = MakeLevelInfo(topo_sequence, 3, 1, "536870912,536870914,536870916;;2");
    auto segmentDescs2 = MakeSegmentDescriptions({536870912, 536870916}, {536870914, 536870916}, levelInfo2);

    ASSERT_TRUE(baseSegmentDescs->Import({segmentDescs1, segmentDescs2}, {536870915, 536870916, 536870917}).IsOK());
    auto finalLevelInfo = MakeLevelInfo(topo_sequence, 3, 1, "536870915,536870917,536870916;;");
    CheckSegmentDescriptions(baseSegmentDescs, {536870912, 536870913, 536870915, 536870916},
                             {536870913, 536870914, 536870917, 536870916}, finalLevelInfo);
}

TEST_F(SegmentDescriptionsTest, TestLevelInfoImportFail)
{
    auto baseLevelInfo = MakeLevelInfo(topo_hash_mod, 3, 2, "536870912,536870914;0,-1;-1,2");
    auto baseSegmentDescs = MakeSegmentDescriptions({536870912, 536870913}, {536870913, 536870914}, baseLevelInfo);

    auto levelInfo1 = MakeLevelInfo(topo_sequence, 3, 2, "536870913,536870915,536870917;;");
    auto segmentDescs1 = MakeSegmentDescriptions({536870913, 536870915}, {536870913, 536870917}, levelInfo1);

    auto levelInfo2 = MakeLevelInfo(topo_hash_mod, 3, 2, "536870912,536870914,536870916;-1,-1;-1,-1");
    auto segmentDescs2 = MakeSegmentDescriptions({536870912, 536870916}, {536870914, 536870916}, levelInfo2);

    // topology not equal. baseSegmentDescs stay the same
    ASSERT_FALSE(baseSegmentDescs->Import({segmentDescs1, segmentDescs2}, {536870915, 536870916, 536870917}).IsOK());
    // finalLevelInfo equals baseLevelInfo
    auto finalLevelInfo = MakeLevelInfo(topo_hash_mod, 3, 2, "536870912,536870914;0,-1;-1,2");
    CheckSegmentDescriptions(baseSegmentDescs, {536870912, 536870913}, {536870913, 536870914}, finalLevelInfo);
}

} // namespace indexlibv2::framework
