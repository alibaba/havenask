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
#include "indexlib/framework/SegmentDescriptions.h"

#include "indexlib/base/Constant.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, SegmentDescriptions);
void SegmentDescriptions::Jsonize(JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        if (_levelInfo) {
            json.Jsonize("level_info", *_levelInfo);
        }
        if (!_segmentStats.empty()) {
            sort(_segmentStats.begin(), _segmentStats.end(), SegmentStatistics::CompareBySegmentId);
            json.Jsonize(SegmentStatistics::JSON_KEY, _segmentStats);
        }
    } else {
        autil::legacy::json::JsonMap jsonMap = json.GetMap();
        if (jsonMap.find("level_info") != jsonMap.end()) {
            _levelInfo.reset(new LevelInfo);
            json.Jsonize("level_info", *_levelInfo);
        }
        json.Jsonize(SegmentStatistics::JSON_KEY, _segmentStats, _segmentStats);
    }
}

Status SegmentDescriptions::Import(const std::vector<std::shared_ptr<SegmentDescriptions>>& otherSegmentDescs,
                                   const std::set<segmentid_t>& newSegmentsHint)
{
    std::vector<SegmentStatistics> newSegmentStats;
    std::vector<std::shared_ptr<LevelInfo>> otherLevelInfos;
    for (const auto& other : otherSegmentDescs) {
        const auto& otherSegmentStats = other->GetSegmentStatisticsVector();
        for (const auto& otherSegmentStat : otherSegmentStats) {
            if (newSegmentsHint.count(otherSegmentStat.GetSegmentId()) != 0) {
                newSegmentStats.push_back(otherSegmentStat);
            }
        }
        const auto& otherLevelInfo = other->GetLevelInfo();
        if (otherLevelInfo) {
            otherLevelInfos.push_back(other->GetLevelInfo());
        }
    }

    if (!otherLevelInfos.empty()) {
        if (_levelInfo == nullptr) {
            size_t levelCount = 1;
            for (const auto& otherLevelInfo : otherLevelInfos) {
                levelCount = std::max(levelCount, otherLevelInfo->GetLevelCount());
            }
            _levelInfo = std::make_shared<LevelInfo>();
            _levelInfo->Init(otherLevelInfos[0]->GetTopology(), levelCount, otherLevelInfos[0]->GetShardCount());
        }
        auto status = _levelInfo->Import(otherLevelInfos, newSegmentsHint);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "LevelInfo import failed.");
            return status;
        }
    }
    _segmentStats.insert(_segmentStats.end(), newSegmentStats.begin(), newSegmentStats.end());
    return Status::OK();
}

std::string SegmentDescriptions::GetSegmentDir(segmentid_t segId) const
{
    std::stringstream ss;
    if (!_levelInfo) {
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_" << 0;
        return ss.str();
    }
    uint32_t levelIdx = 0;
    uint32_t inLevelIdx = 0;
    bool ret = _levelInfo->FindPosition(segId, levelIdx, inLevelIdx);
    // ret should be true, but for old normal table index may not find
    (void)ret;
    ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_" << levelIdx;
    return ss.str();
}

void SegmentDescriptions::RemoveSegment(segmentid_t segmentId)
{
    if (_levelInfo) {
        _levelInfo->RemoveSegment(segmentId);
    }
    auto segmentStatIter = std::find_if(_segmentStats.begin(), _segmentStats.end(),
                                        [segmentId](const auto stat) { return stat.GetSegmentId() == segmentId; });
    if (segmentStatIter != _segmentStats.end()) {
        _segmentStats.erase(segmentStatIter);
    }
}

} // namespace indexlibv2::framework
