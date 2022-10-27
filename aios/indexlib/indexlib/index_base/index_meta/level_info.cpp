#include "indexlib/index_base/index_meta/level_info.h"
#include "indexlib/util/column_util.h"
#include <autil/StringUtil.h>

using namespace std;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index_base);

LevelMeta::LevelMeta()
    : levelIdx(0)
    , cursor(0)
    , topology(topo_sequence)
{

}

LevelMeta::~LevelMeta()
{

}

bool LevelMeta::RemoveSegment(segmentid_t segmentId)
{
    for (auto iter = segments.begin(); iter != segments.end(); ++iter)
    {
        if (*iter == segmentId)
        {
            switch(topology)
            {
            case topo_sequence:
            case topo_hash_range:
                segments.erase(iter);
                break;
            case topo_hash_mod:
                *iter = INVALID_SEGMENTID;
                break;
            default:
                assert(false);
            }
            return true;
        }
    }
    return false;
}

void LevelMeta::Clear()
{
    switch(topology)
    {
    case topo_sequence:
    case topo_hash_range:
        segments.clear();
        break;
    case topo_hash_mod:
    {
        vector<segmentid_t> emptySegments(segments.size(), INVALID_SEGMENTID);
        segments.swap(emptySegments);
        break;
    }
    case topo_default:
        assert(false);
    }
}

void LevelMeta::Jsonize(JsonWrapper& json)
{
    json.Jsonize("level_idx", levelIdx);
    json.Jsonize("cursor", cursor);
    json.Jsonize("segments", segments);

    if (json.GetMode() == TO_JSON)
    {
        string topoStr = TopologyToStr(topology);
        json.Jsonize("topology", topoStr);
    }
    else
    {
        string topoStr;
        json.Jsonize("topology", topoStr);
        topology = StrToTopology(topoStr);
    }
}

bool LevelMeta::operator == (const LevelMeta& other) const
{
    return (levelIdx == other.levelIdx) &&
           (topology == other.topology) &&
           (segments == other.segments);
}

LevelInfo::LevelInfo()
{
}

LevelInfo::~LevelInfo()
{
}

void LevelInfo::Jsonize(JsonWrapper& json)
{
    json.Jsonize("level_metas", levelMetas);
}

void LevelInfo::Init(LevelTopology topo, uint32_t levelNum, size_t columnCount)
{
    levelMetas.clear();

    AddLevel(topo_sequence);
    for (uint32_t i = 1; i < levelNum; ++i)
    {
        AddLevel(topo);
        switch (topo)
        {
        case topo_hash_mod:
            levelMetas[i].segments.resize(columnCount, INVALID_SEGMENTID);
            break;
        case topo_sequence:
            break;
        case topo_hash_range:
        case topo_default:
            assert(false);
            break;
        }
    }
}

void LevelInfo::AddLevel(LevelTopology levelTopo)
{
    LevelMeta levelMeta;
    levelMeta.levelIdx = levelMetas.size();
    levelMeta.topology = levelTopo;
    levelMetas.push_back(levelMeta);
}

void LevelInfo::AddSegment(segmentid_t segmentId)
{
    levelMetas[0].segments.push_back(segmentId); // New segments push to Level 0, for build
}

void LevelInfo::RemoveSegment(segmentid_t segmentId)
{
    for (auto &levelMeta : levelMetas)
    {
        if (levelMeta.RemoveSegment(segmentId))
        {
            return;
        }
    }
}

bool LevelInfo::FindPosition(segmentid_t segmentId, uint32_t& levelIdx, uint32_t& inLevelIdx) const
{
    for (size_t i = 0; i < levelMetas.size(); ++i)
    {
        const vector<segmentid_t> &segments = levelMetas[i].segments;
        for (size_t j = 0; j < segments.size(); ++j)
        {
            if (segments[j] == segmentId)
            {
                levelIdx = i;
                inLevelIdx = j;
                return true;
            }
        }
    }
    return false;
}

void LevelInfo::Clear()
{
    for (auto &levelMeta : levelMetas)
    {
        levelMeta.Clear();
    }
}

LevelTopology LevelInfo::GetTopology() const
{
    return levelMetas.empty() ? topo_sequence : levelMetas.crbegin()->topology;
}

size_t LevelInfo::GetColumnCount() const
{
    size_t ret = 1;
    for (auto &levelMeta : levelMetas)
    {
        if (levelMeta.topology == topo_hash_mod)
        {
            ret = std::max(ret, levelMeta.segments.size());
        }
    }
    return ret;
}

vector<segmentid_t> LevelInfo::GetSegmentIds(size_t globalColumnId) const
{
    vector<segmentid_t> ret;
    size_t globalColumnCount = GetColumnCount();
    for (auto &levelMeta : levelMetas)
    {
        if (levelMeta.topology == topo_sequence || levelMeta.topology == topo_hash_range)
        {
            ret.insert(ret.end(), levelMeta.segments.rbegin(), levelMeta.segments.rend());
        }
        else if (levelMeta.topology == topo_hash_mod)
        {
            size_t currentColumnCount = levelMeta.segments.size();
            size_t currentColumnId = util::ColumnUtil::TransformColumnId(
                    globalColumnId, currentColumnCount, globalColumnCount);
            segmentid_t segmentId = levelMeta.segments[currentColumnId];
            if (segmentId != INVALID_SEGMENTID)
            {
                ret.push_back(segmentId);
            }
        }
        else
        {
            assert(false);
        }
    }
    return ret;
}

IE_NAMESPACE_END(index_base);

