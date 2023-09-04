#include "indexlib/index_base/index_meta/test/level_info_unittest.h"

using namespace std;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, LevelInfoTest);

LevelInfoTest::LevelInfoTest() {}

LevelInfoTest::~LevelInfoTest() {}

void LevelInfoTest::CaseSetUp() {}

void LevelInfoTest::CaseTearDown() {}

void LevelInfoTest::TestSimpleProcess()
{
    string jsonStr =
        R"(
     {
         "level_num" : 2,
         "topology" : "sharding_mod",
         "level_metas" :
         [
             {
                 "level_idx" : 0,
                 "cursor" : 0,
                 "topology" : "sequence",
                 "segments" :
                 [ 10, 11]
             },
             {
                 "level_idx" : 1,
                 "cursor" : 0,
                 "topology" : "hash_mod",
                 "segments" :
                 [ 1, 2 ]
             }
        ]
     })";
    indexlibv2::framework::LevelInfo info;
    FromJsonString(info, jsonStr);
    ASSERT_EQ(2u, info.GetLevelCount());
    ASSERT_EQ(2u, info.levelMetas.size());

    ASSERT_EQ(0, info.levelMetas[0].levelIdx);
    ASSERT_EQ(0, info.levelMetas[0].cursor);
    indexlibv2::framework::LevelTopology topo = info.levelMetas[0].topology;
    ASSERT_EQ(indexlibv2::framework::topo_sequence, topo);
    ASSERT_EQ(1, info.levelMetas[1].levelIdx);
    ASSERT_EQ(0, info.levelMetas[1].cursor);
    topo = info.levelMetas[1].topology;
    ASSERT_EQ(indexlibv2::framework::topo_hash_mod, info.levelMetas[1].topology);
}

void LevelInfoTest::TestRemoveSegment()
{
    indexlibv2::framework::LevelInfo levelInfo;
    levelInfo.Init(indexlibv2::framework::topo_hash_mod, 2, 2);
    levelInfo.levelMetas[0].segments = {1, 2};
    levelInfo.levelMetas[1].segments = {3, 4};

    levelInfo.RemoveSegment(1);
    levelInfo.RemoveSegment(2);
    levelInfo.RemoveSegment(3);

    vector<segmentid_t> expectSegmentsAfterRemove0 = {};
    vector<segmentid_t> expectSegmentsAfterRemove1 = {-1, 4};
    ASSERT_EQ(expectSegmentsAfterRemove0, levelInfo.levelMetas[0].segments);
    ASSERT_EQ(expectSegmentsAfterRemove1, levelInfo.levelMetas[1].segments);
}

void LevelInfoTest::TestGetSegmentIds()
{
    string jsonStr =
        R"(
     {
         "level_num" : 2,
         "topology" : "sharding_mod",
         "level_metas" :
         [
             {
                 "level_idx" : 0,
                 "cursor" : 0,
                 "topology" : "sequence",
                 "segments" :
                 [ 10, 11]
             },
             {
                 "level_idx" : 1,
                 "cursor" : 0,
                 "topology" : "hash_mod",
                 "segments" :
                 [ 1, 2 ]
             }
        ]
     })";
    indexlibv2::framework::LevelInfo info;
    FromJsonString(info, jsonStr);

    vector<segmentid_t> segmentIds = info.GetSegmentIds(0);
    ASSERT_THAT(segmentIds, ElementsAre(11, 10, 1));

    segmentIds = info.GetSegmentIds(1);
    ASSERT_THAT(segmentIds, ElementsAre(11, 10, 2));

    info.RemoveSegment(1);
    info.RemoveSegment(2);
    segmentIds = info.GetSegmentIds(1);
    ASSERT_THAT(segmentIds, ElementsAre(11, 10));
}
}} // namespace indexlib::index_base
