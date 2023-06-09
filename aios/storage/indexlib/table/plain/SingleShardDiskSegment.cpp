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
#include "indexlib/table/plain/SingleShardDiskSegment.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/framework/SegmentMeta.h"

namespace indexlibv2::plain {

SingleShardDiskSegment::SingleShardDiskSegment(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema,
                                               const framework::SegmentMeta& segmentMeta,
                                               const framework::BuildResource& buildResource)
    : PlainDiskSegment(schema, segmentMeta, buildResource)
{
}

void SingleShardDiskSegment::CollectSegmentDescription(const std::shared_ptr<framework::SegmentDescriptions>& segDescs)
{
    auto levelInfo = segDescs->GetLevelInfo();
    assert(levelInfo != nullptr);
    std::string segDirName = PathUtil::GetFileNameFromPath(GetSegmentDirectory()->GetPhysicalPath(""));
    uint32_t levelIdx = PathUtil::GetLevelIdxFromSegmentDirName(segDirName);
    assert(levelIdx < levelInfo->GetLevelCount());
    auto& levelMeta = levelInfo->levelMetas[levelIdx];
    if (levelIdx == 0) {
        levelMeta.segments.push_back(GetSegmentId());
    } else {
        uint32_t shardIdx = GetSegmentInfo()->GetShardId();
        assert(shardIdx != framework::SegmentInfo::INVALID_SHARDING_ID);
        assert(shardIdx < levelMeta.segments.size());
        levelMeta.segments[shardIdx] = GetSegmentId();
    }
}

} // namespace indexlibv2::plain
