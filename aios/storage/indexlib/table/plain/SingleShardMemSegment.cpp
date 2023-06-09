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
#include "indexlib/table/plain/SingleShardMemSegment.h"

#include "indexlib/framework/LevelInfo.h"

namespace indexlibv2::plain {

SingleShardMemSegment::SingleShardMemSegment(const config::TabletOptions* options,
                                             const std::shared_ptr<config::TabletSchema>& schema,
                                             const framework::SegmentMeta& segmentMeta, uint32_t levelNum)
    : PlainMemSegment(options, schema, segmentMeta)
    , _levelNum(levelNum)
{
}

void SingleShardMemSegment::CollectSegmentDescription(const std::shared_ptr<framework::SegmentDescriptions>& segDescs)
{
    auto levelInfo = segDescs->GetLevelInfo();
    auto segInfo = GetSegmentInfo();
    assert(1 == segInfo->shardCount);
    if (!levelInfo) {
        levelInfo.reset(new indexlibv2::framework::LevelInfo());
        levelInfo->Init(framework::topo_hash_mod, _levelNum, segInfo->shardCount);
        segDescs->SetLevelInfo(levelInfo);
    }
    auto& levelMeta = levelInfo->levelMetas[0];
    levelMeta.segments.push_back(GetSegmentId());
}

} // namespace indexlibv2::plain
