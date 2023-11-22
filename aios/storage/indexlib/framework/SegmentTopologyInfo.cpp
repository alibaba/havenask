/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/framework/SegmentTopologyInfo.h"

#include <cstddef>
#include <cstdint>
#include <string>

#include "indexlib/framework/LevelInfo.h"

using namespace std;

namespace indexlibv2::framework {

SegmentTopologyInfo::SegmentTopologyInfo()
    : levelTopology(topo_sequence)
    , levelIdx(0)
    , isBottomLevel(false)
    , shardCount(1)
    , shardIdx(0)
{
}

void SegmentTopologyInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        string topoStr = LevelMeta::TopologyToStr(levelTopology);
        json.Jsonize("topology", topoStr);
    } else {
        string topoStr;
        json.Jsonize("topology", topoStr);
        levelTopology = LevelMeta::StrToTopology(topoStr);
    }

    json.Jsonize("level_idx", levelIdx);
    json.Jsonize("is_bottom_level", isBottomLevel);
    json.Jsonize("shard_count", shardCount);
    json.Jsonize("shard_idx", shardIdx);
}

} // namespace indexlibv2::framework
