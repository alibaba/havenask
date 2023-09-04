#include "indexlib/merger/test/merge_helper.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/index_base/index_meta/version.h"

using namespace autil;
using namespace std;

using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeHelper);

MergeHelper::MergeHelper() {}

MergeHelper::~MergeHelper() {}

void MergeHelper::MakeSegmentMergeInfos(const string& str, SegmentMergeInfos& segMergeInfos)
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

void MergeHelper::MakeSegmentMergeInfosWithSegId(const string& str, SegmentMergeInfos& segMergeInfos)
{
    StringTokenizer st;
    st.tokenize(str, ";", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    segmentid_t segId = 0;
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it, ++segId) {
        string segInfoStr = *it;
        StringTokenizer stSeg;
        stSeg.tokenize(segInfoStr, " ", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        assert(stSeg.getNumTokens() == 3);

        SegmentMergeInfo segMergeInfo;
        uint32_t segId;
        StringUtil::strToUInt32(stSeg[0].c_str(), segId);
        segMergeInfo.segmentId = (segmentid_t)segId;
        uint32_t tempDocCount = 0;
        StringUtil::strToUInt32(stSeg[1].c_str(), tempDocCount);
        segMergeInfo.segmentInfo.docCount = tempDocCount;
        StringUtil::strToUInt32(stSeg[2].c_str(), segMergeInfo.deletedDocCount);
        segMergeInfos.push_back(segMergeInfo);
    }
}

bool MergeHelper::CheckMergePlan(std::string str, const MergePlan& plan)
{
    vector<segmentid_t> answer;
    StringUtil::fromString(str, answer, ",");
    if (answer.size() != plan.GetSegmentCount()) {
        return false;
    }
    MergePlan::Iterator segIter = plan.CreateIterator();
    uint32_t idx = 0;
    while (segIter.HasNext()) {
        segmentid_t segId = segIter.Next();
        if (answer[idx] != segId) {
            return false;
        }
        idx++;
    }
    return true;
}

bool MergeHelper::CheckMergePlan(const MergePlan& plan, const vector<segmentid_t>& inPlanIds,
                                 const vector<size_t>& targetPosition)
{
#define ASSERT_EQUAL(left, right)                                                                                      \
    if ((left) != (right)) {                                                                                           \
        IE_LOG(ERROR, "check failed : left[%d] and right[%d] not match", int(left), int(right));                       \
        return false;                                                                                                  \
    }

    ASSERT_EQUAL(inPlanIds.size(), plan.GetSegmentCount());
    const auto& segMergeInfos = plan.GetSegmentMergeInfos();
    auto expectedIter = inPlanIds.cbegin();
    for (const auto& segMergeInfo : segMergeInfos) {
        ASSERT_EQUAL(*expectedIter, segMergeInfo.segmentId);
        ++expectedIter;
    }
    ASSERT_EQUAL(targetPosition[0], plan.GetTargetTopologyInfo(0).levelIdx);
    ASSERT_EQUAL(targetPosition[1], plan.GetTargetTopologyInfo(0).columnIdx);
    ASSERT_EQUAL((bool)targetPosition[2], plan.GetTargetTopologyInfo(0).isBottomLevel);
#undef ASSERT_EQUAL
    return true;
}

// topo,columnCount#segid[:size,...],segid[|cursor];segid,segid;...
pair<SegmentMergeInfos, indexlibv2::framework::LevelInfo> MergeHelper::PrepareMergeInfo(const string& levelStr)
{
    SegmentMergeInfos segMergeInfos;
    Version version;
    indexlibv2::framework::LevelInfo& levelInfo = version.GetLevelInfo();
    vector<string> infos;
    StringUtil::fromString(levelStr, infos, "#");
    vector<string> metas;
    StringUtil::fromString(infos[0], metas, ",");
    indexlibv2::framework::LevelTopology topo = indexlibv2::framework::LevelMeta::StrToTopology(metas[0]);
    size_t columnCount = StringUtil::fromString<size_t>(metas[1]);
    vector<vector<string>> segInfos;
    StringUtil::fromString(infos[1], segInfos, "|", ";");
    uint32_t levelNum = segInfos.size();
    levelInfo.Init(topo, levelNum, columnCount);

    for (int level = segInfos.size() - 1; level >= 0; --level) {
        uint32_t cursor = 0;
        if (segInfos[level].size() == 2) {
            cursor = StringUtil::fromString<uint32_t>(segInfos[level][1]);
        }
        levelInfo.levelMetas[level].cursor = cursor;
        vector<string> segs;
        StringUtil::fromString(segInfos[level][0], segs, ",");
        for (size_t column = 0; column < segs.size(); ++column) {
            vector<int> segInfo;
            StringUtil::fromString(segs[column], segInfo, ":");
            SegmentTopologyInfo segTopo;
            if (segInfo[0] == -1) {
                continue;
            }
            segTopo.levelIdx = level;
            segTopo.levelTopology = (level == 0 ? indexlibv2::framework::topo_sequence : topo);
            segTopo.isBottomLevel = ((level == ((int)segInfos.size() - 1)) ? true : false);
            segTopo.columnCount = columnCount;
            segTopo.columnIdx = column;
            version.AddSegment(segInfo[0], segTopo);
            SegmentMergeInfo segMergeInfo;
            segMergeInfo.segmentId = segInfo[0];
            if (segInfo.size() > 1) {
                segMergeInfo.segmentSize = segInfo[1];
            }
            segMergeInfo.levelIdx = level;
            segMergeInfo.inLevelIdx = column;
            segMergeInfo.segmentInfo.shardCount = columnCount;
            segMergeInfos.push_back(segMergeInfo);
        }
    }
    return {segMergeInfos, levelInfo};
}
}} // namespace indexlib::merger
