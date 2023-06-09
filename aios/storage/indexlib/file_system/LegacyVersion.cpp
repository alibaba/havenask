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
#include "indexlib/file_system/LegacyVersion.h"

#include <assert.h>
#include <map>
#include <memory>
#include <ostream>
#include <stddef.h>

#include "alog/Logger.h"
#include "autil/legacy/json.h"
#include "indexlib/base/Constant.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/util/Exception.h"

using std::string;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, LegacyVersion);

void LegacyVersion::LevelMeta::Jsonize(JsonWrapper& json)
{
    assert(json.GetMode() == FROM_JSON);
    // json.Jsonize("level_idx", levelIdx);
    // json.Jsonize("cursor", cursor);
    json.Jsonize("segments", _segments);

    // string topoStr;
    // json.Jsonize("topology", topoStr);
}

const std::vector<segmentid_t>& LegacyVersion::LevelMeta::GetSegments() const { return _segments; }

void LegacyVersion::LevelInfo::Jsonize(JsonWrapper& json)
{
    assert(json.GetMode() == FROM_JSON);
    json.Jsonize("level_metas", levelMetas);
}

bool LegacyVersion::LevelInfo::FindPosition(segmentid_t segmentId, uint32_t& levelIdx, uint32_t& inLevelIdx) const
{
    for (size_t i = 0; i < levelMetas.size(); ++i) {
        const std::vector<segmentid_t>& segments = levelMetas[i].GetSegments();
        for (size_t j = 0; j < segments.size(); ++j) {
            if (segments[j] == segmentId) {
                levelIdx = i;
                inLevelIdx = j;
                return true;
            }
        }
    }
    return false;
}

void LegacyVersion::Jsonize(JsonWrapper& json)
{
    assert(json.GetMode() == FROM_JSON);
    json.Jsonize("segments", _segmentIds);

    autil::legacy::json::JsonMap jsonMap = json.GetMap();
    if (jsonMap.find("level_info") != jsonMap.end()) {
        _hasLevelInfo = true;
        json.Jsonize("level_info", _levelInfo);
    }
    Validate(_segmentIds);
}

void LegacyVersion::Validate(const std::vector<segmentid_t>& segmentIds)
{
    const segmentid_t INVALID_SEGMENTID = -1;
    segmentid_t lastSegId = INVALID_SEGMENTID;
    for (size_t i = 0; i < segmentIds.size(); ++i) {
        segmentid_t curSegId = segmentIds[i];
        if (lastSegId != INVALID_SEGMENTID && lastSegId >= curSegId) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "last segment id [%d] >= cur segment id [%d]", lastSegId, curSegId);
        }
        lastSegId = curSegId;
    }
}

std::string LegacyVersion::GetSegmentDirName(segmentid_t segId) const
{
    std::stringstream ss;
    if (!_hasLevelInfo) {
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId;
    } else {
        uint32_t levelIdx = 0;
        uint32_t inLevelIdx = 0;
        bool ret = _levelInfo.FindPosition(segId, levelIdx, inLevelIdx);
        assert(ret);
        (void)ret;
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_" << levelIdx;
    }
    return ss.str();
}

std::vector<segmentid_t> LegacyVersion::GetSegmentVector() { return _segmentIds; }
}} // namespace indexlib::file_system
