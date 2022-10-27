#include "indexlib/index_base/index_meta/segment_topology_info.h"

using namespace std;

IE_NAMESPACE_BEGIN(index_base);

SegmentTopologyInfo::SegmentTopologyInfo()
    : levelTopology(topo_sequence)
    , levelIdx(0)
    , isBottomLevel(false)
    , columnCount(1)
    , columnIdx(0)
{
}

void SegmentTopologyInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON)
    {
        string topoStr = LevelMeta::TopologyToStr(levelTopology);
        json.Jsonize("topology", topoStr);
    }
    else
    {
        string topoStr;
        json.Jsonize("topology", topoStr);
        levelTopology = LevelMeta::StrToTopology(topoStr);
    }

    json.Jsonize("level_idx", levelIdx);
    json.Jsonize("is_bottom_level", isBottomLevel);
    json.Jsonize("column_count", columnCount);
    json.Jsonize("column_idx", columnIdx);
}

IE_NAMESPACE_END(index_base);
