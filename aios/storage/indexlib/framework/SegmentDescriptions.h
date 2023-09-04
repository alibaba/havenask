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
#pragma once

#include <map>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/framework/SegmentStatistics.h"

namespace indexlibv2::framework {

class SegmentDescriptions : public autil::legacy::Jsonizable
{
public:
public:
    SegmentDescriptions() {}
    ~SegmentDescriptions() {}
    void Jsonize(JsonWrapper& json) override;
    Status Import(const std::vector<std::shared_ptr<SegmentDescriptions>>& otherSegmentDescs,
                  const std::set<segmentid_t>& newSegmentsHint);

public:
    static std::string GetSegmentDirName(segmentid_t segmentId, uint32_t levelIdx = 0)
    {
        return std::string(SEGMENT_FILE_NAME_PREFIX) + "_" + std::to_string(segmentId) + "_level_" +
               std::to_string(levelIdx);
    }

    std::string GetSegmentDir(segmentid_t segId) const;
    void SetLevelInfo(std::shared_ptr<LevelInfo>& levelInfo) { _levelInfo = levelInfo; }
    const std::shared_ptr<LevelInfo>& GetLevelInfo() const { return _levelInfo; }
    void RemoveSegment(segmentid_t);

    void SetSegmentStatistics(const std::vector<SegmentStatistics>& segmentStatistics)
    {
        _segmentStats = segmentStatistics;
    }
    const std::vector<SegmentStatistics>& GetSegmentStatisticsVector() const { return _segmentStats; }

private:
    std::shared_ptr<LevelInfo> _levelInfo;
    std::vector<SegmentStatistics> _segmentStats;

    // todo: add other segment descriptions

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
