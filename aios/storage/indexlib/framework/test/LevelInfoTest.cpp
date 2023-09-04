#include "indexlib/framework/LevelInfo.h"

#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace framework {

class LevelInfoTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();
};

TEST_F(LevelInfoTest, TestSmallerBaseLevelNum)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_hash_mod, /*levelNum=*/4, /*shardCount=*/2);
    ASSERT_FALSE(baseLevelInfo->Import({levelInfo1}, {}).IsOK());
}

TEST_F(LevelInfoTest, TestDifferentShardCount)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/1);
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    ASSERT_FALSE(baseLevelInfo->Import({levelInfo1}, {}).IsOK());
}

TEST_F(LevelInfoTest, TestDifferentTopologyType)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_sequence, /*levelNum=*/3, /*shardCount=*/2);
    ASSERT_FALSE(baseLevelInfo->Import({levelInfo1}, {}).IsOK());
}

TEST_F(LevelInfoTest, TestEmptyInput)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_sequence, /*levelNum=*/3, /*shardCount=*/2);
    ASSERT_FALSE(baseLevelInfo->Import({}, {}).IsOK());
}

TEST_F(LevelInfoTest, TestSimple)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    baseLevelInfo->levelMetas[2].segments[0] = 0;
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo1->AddSegment(1);
    levelInfo1->AddSegment(2);
    ASSERT_TRUE(baseLevelInfo->Import({levelInfo1}, {0, 1, 2}).IsOK());
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(0, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 2);
        ASSERT_EQ(inLevelIdx, 0);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(1, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 0);
        ASSERT_EQ(inLevelIdx, 0);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(2, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 0);
        ASSERT_EQ(inLevelIdx, 1);
    }
}

TEST_F(LevelInfoTest, TestMultiLevelInfoWithDifferentTopo)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    baseLevelInfo->levelMetas[2].segments[0] = 0;
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_sequence, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo1->AddSegment(1);
    levelInfo1->AddSegment(2);
    auto levelInfo2 = std::make_shared<LevelInfo>();
    levelInfo2->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo2->AddSegment(3);
    levelInfo2->AddSegment(4);

    ASSERT_FALSE(baseLevelInfo->Import({levelInfo1, levelInfo2}, {0, 1, 2, 3, 4}).IsOK());
}

TEST_F(LevelInfoTest, TestWithMultiLevelInfo)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    baseLevelInfo->levelMetas[2].segments[0] = 0;
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo1->AddSegment(1);
    levelInfo1->AddSegment(2);
    auto levelInfo2 = std::make_shared<LevelInfo>();
    levelInfo2->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo2->AddSegment(3);
    levelInfo2->AddSegment(4);

    ASSERT_TRUE(baseLevelInfo->Import({levelInfo1, levelInfo2}, {0, 1, 2, 3, 4}).IsOK());
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(0, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 2);
        ASSERT_EQ(inLevelIdx, 0);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(1, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 0);
        ASSERT_EQ(inLevelIdx, 0);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(2, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 0);
        ASSERT_EQ(inLevelIdx, 1);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(3, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 0);
        ASSERT_EQ(inLevelIdx, 2);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(4, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 0);
        ASSERT_EQ(inLevelIdx, 3);
    }
}

TEST_F(LevelInfoTest, TestSequenceImport)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_sequence, /*levelNum=*/3, /*shardCount=*/2);
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_sequence, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo1->AddSegment(1);
    levelInfo1->AddSegment(2);
    auto levelInfo2 = std::make_shared<LevelInfo>();
    levelInfo2->Init(LevelTopology::topo_sequence, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo2->AddSegment(3);
    levelInfo2->AddSegment(4);

    ASSERT_TRUE(baseLevelInfo->Import({levelInfo1, levelInfo2}, {1, 2, 3, 4}).IsOK());
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(1, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 0);
        ASSERT_EQ(inLevelIdx, 0);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(2, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 0);
        ASSERT_EQ(inLevelIdx, 1);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(3, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 0);
        ASSERT_EQ(inLevelIdx, 2);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(4, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 0);
        ASSERT_EQ(inLevelIdx, 3);
    }
}

TEST_F(LevelInfoTest, TestWithNotInHintSegList)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    baseLevelInfo->levelMetas[2].segments[0] = 0;
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo1->AddSegment(1);
    levelInfo1->AddSegment(2);
    auto levelInfo2 = std::make_shared<LevelInfo>();
    levelInfo2->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo2->AddSegment(3);
    levelInfo2->AddSegment(4);

    ASSERT_TRUE(baseLevelInfo->Import({levelInfo1, levelInfo2}, {0, 1, 4}).IsOK());
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(0, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 2);
        ASSERT_EQ(inLevelIdx, 0);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(1, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 0);
        ASSERT_EQ(inLevelIdx, 0);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_FALSE(baseLevelInfo->FindPosition(2, levelIdx, inLevelIdx));
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_FALSE(baseLevelInfo->FindPosition(3, levelIdx, inLevelIdx));
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(4, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 0);
        ASSERT_EQ(inLevelIdx, 1);
    }
}

TEST_F(LevelInfoTest, TestHighLevelImport)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    baseLevelInfo->levelMetas[2].segments[0] = 0;
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo1->levelMetas[1].segments[0] = 3;
    auto levelInfo2 = std::make_shared<LevelInfo>();
    levelInfo2->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo2->levelMetas[1].segments[1] = 4;

    ASSERT_TRUE(baseLevelInfo->Import({levelInfo1, levelInfo2}, {0, 3, 4}).IsOK());
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(0, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 2);
        ASSERT_EQ(inLevelIdx, 0);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(3, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 1);
        ASSERT_EQ(inLevelIdx, 0);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(4, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 1);
        ASSERT_EQ(inLevelIdx, 1);
    }
}

TEST_F(LevelInfoTest, TestHighLevelImportNotInHint)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    baseLevelInfo->levelMetas[2].segments[0] = 0;
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo1->levelMetas[1].segments[0] = 3;
    auto levelInfo2 = std::make_shared<LevelInfo>();
    levelInfo2->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo2->levelMetas[1].segments[1] = 4;

    ASSERT_TRUE(baseLevelInfo->Import({levelInfo1, levelInfo2}, {0}).IsOK());
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(0, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 2);
        ASSERT_EQ(inLevelIdx, 0);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_FALSE(baseLevelInfo->FindPosition(3, levelIdx, inLevelIdx));
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_FALSE(baseLevelInfo->FindPosition(4, levelIdx, inLevelIdx));
    }
}

TEST_F(LevelInfoTest, TestHighLevelConflict)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    baseLevelInfo->levelMetas[2].segments[0] = 0;
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo1->AddSegment(1);
    levelInfo1->AddSegment(2);
    levelInfo1->levelMetas[2].segments[0] = 3;
    ASSERT_FALSE(baseLevelInfo->Import({levelInfo1}, {0, 1, 2, 3}).IsOK());
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(0, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 2);
        ASSERT_EQ(inLevelIdx, 0);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_FALSE(baseLevelInfo->FindPosition(1, levelIdx, inLevelIdx));
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_FALSE(baseLevelInfo->FindPosition(2, levelIdx, inLevelIdx));
    }
}

TEST_F(LevelInfoTest, TestHighLevelConflict2)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    baseLevelInfo->levelMetas[2].segments[0] = 3;
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo1->AddSegment(1);
    levelInfo1->AddSegment(2);
    levelInfo1->levelMetas[1].segments[0] = 3;
    ASSERT_FALSE(baseLevelInfo->Import({levelInfo1}, {0, 1, 2, 3}).IsOK());
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(3, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 2);
        ASSERT_EQ(inLevelIdx, 0);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_FALSE(baseLevelInfo->FindPosition(1, levelIdx, inLevelIdx));
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_FALSE(baseLevelInfo->FindPosition(2, levelIdx, inLevelIdx));
    }
}

TEST_F(LevelInfoTest, TestHighLevelConflict3)
{
    auto baseLevelInfo = std::make_shared<LevelInfo>();
    baseLevelInfo->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    baseLevelInfo->levelMetas[1].segments[0] = 3;
    auto levelInfo1 = std::make_shared<LevelInfo>();
    levelInfo1->Init(LevelTopology::topo_hash_mod, /*levelNum=*/3, /*shardCount=*/2);
    levelInfo1->AddSegment(1);
    levelInfo1->AddSegment(2);
    levelInfo1->levelMetas[1].segments[0] = 3;
    levelInfo1->levelMetas[1].segments.push_back(4);
    ASSERT_FALSE(baseLevelInfo->Import({levelInfo1}, {0, 1, 2, 3, 4}).IsOK());
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_TRUE(baseLevelInfo->FindPosition(3, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 1);
        ASSERT_EQ(inLevelIdx, 0);
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_FALSE(baseLevelInfo->FindPosition(1, levelIdx, inLevelIdx));
    }
    {
        uint32_t levelIdx = std::numeric_limits<uint32_t>::max();
        uint32_t inLevelIdx = std::numeric_limits<uint32_t>::max();
        ASSERT_FALSE(baseLevelInfo->FindPosition(2, levelIdx, inLevelIdx));
    }
}

TEST_F(LevelInfoTest, TestAddAndRemoveSegment)
{
    {
        LevelInfo levelInfo;
        levelInfo.Init(LevelTopology::topo_key_range, /*levelNum=*/5);
        ASSERT_EQ(levelInfo.GetLevelCount(), 5);
        ASSERT_TRUE(levelInfo.AddSegment(/*segmentId=*/0, /*levelIdx=*/5));
        ASSERT_EQ(levelInfo.GetLevelCount(), 6);
    }
    {
        LevelInfo levelInfo;
        levelInfo.Init(LevelTopology::topo_key_range, /*levelNum=*/5);
        ASSERT_FALSE(levelInfo.AddSegment(/*segmentId=*/0, /*levelIdx=*/6));
    }
    {
        LevelInfo levelInfo;
        levelInfo.Init(LevelTopology::topo_key_range, /*levelNum=*/5);
        ASSERT_TRUE(levelInfo.AddSegment(/*segmentId=*/0, /*levelIdx=*/4));
        ASSERT_FALSE(levelInfo.AddSegment(/*segmentId=*/0, /*levelIdx=*/4));
    }
    {
        LevelInfo levelInfo;
        levelInfo.Init(LevelTopology::topo_hash_mod, /*levelNum=*/5, /*shardCount=*/3);
        ASSERT_TRUE(levelInfo.AddSegment(/*segmentId=*/0, /*levelIdx=*/0));
        ASSERT_FALSE(levelInfo.AddSegment(/*segmentId=*/1, /*levelIdx=*/1));
    }

    {
        LevelInfo levelInfo;
        levelInfo.Init(LevelTopology::topo_key_range, /*levelNum=*/5);
        ASSERT_TRUE(levelInfo.AddSegment(/*segmentId=*/0, /*levelIdx=*/4));
        ASSERT_TRUE(levelInfo.AddSegment(/*segmentId=*/1, /*levelIdx=*/4));
        uint32_t levelIdx = 0;
        uint32_t inLevelIdx = 0;
        ASSERT_TRUE(levelInfo.FindPosition(/*segmentId=*/1, levelIdx, inLevelIdx));
        ASSERT_EQ(levelIdx, 4);
        ASSERT_EQ(inLevelIdx, 1);
        levelInfo.RemoveSegment(1);
        ASSERT_FALSE(levelInfo.FindPosition(/*segmentId=*/1, levelIdx, inLevelIdx));
    }
}

}} // namespace indexlibv2::framework
