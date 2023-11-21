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
#include "indexlib/framework/Version.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <exception>
#include <memory>
#include <sstream>
#include <string>

#include "autil/NetUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable_exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/framework/IndexTaskQueue.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/framework/SegmentDescriptions.h"
#include "indexlib/framework/SegmentStatistics.h"
#include "indexlib/framework/VersionCoord.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, Version);

Version::Version()
    : _versionId(INVALID_VERSIONID)
    , _timestamp(-1)
    , _segmentDescs(new SegmentDescriptions)
    , _indexTaskQueue(std::make_shared<IndexTaskQueue>())
{
}

Version::Version(versionid_t versionId)
    : _versionId(versionId)
    , _segmentDescs(new SegmentDescriptions)
    , _indexTaskQueue(std::make_shared<IndexTaskQueue>())
{
}

Version::Version(versionid_t versionId, int64_t timestamp)
    : _versionId(versionId)
    , _timestamp(timestamp)
    , _segmentDescs(new SegmentDescriptions)
    , _indexTaskQueue(std::make_shared<IndexTaskQueue>())
{
}

void Version::LegacyFromJson(JsonWrapper& json)
{
    autil::legacy::json::JsonMap jsonMap = json.GetMap();
    if (jsonMap.find("level_info") != jsonMap.end()) {
        auto levelInfo = std::make_shared<LevelInfo>();
        json.Jsonize("level_info", *levelInfo);
        _segmentDescs->SetLevelInfo(levelInfo);
    }
    if (jsonMap.find(SegmentStatistics::JSON_KEY) != jsonMap.end()) {
        std::vector<SegmentStatistics> segmentStatisticVec;
        json.Jsonize(SegmentStatistics::JSON_KEY, segmentStatisticVec, segmentStatisticVec);
        _segmentDescs->SetSegmentStatistics(segmentStatisticVec);
    }
}

void Version::Jsonize(JsonWrapper& json)
{
    json.Jsonize("versionid", _versionId);
    json.Jsonize("schema_version", _schemaId, _schemaId);
    json.Jsonize("segment_descriptions", *_segmentDescs, *_segmentDescs);
    json.Jsonize("commit_time", _commitTime, _commitTime);
    json.Jsonize("description", _description, _description);
    json.Jsonize("version_line", _versionLine, _versionLine);

    if (json.GetMode() == TO_JSON) {
        json.Jsonize("format_version", _formatVersion);
        json.Jsonize("last_segmentid", _lastSegmentId);

        int64_t ts = _timestamp;
        json.Jsonize("timestamp", ts);

        std::string locatorString = autil::StringUtil::strToHexStr(_locator.Serialize());
        json.Jsonize("locator", locatorString);
        json.Jsonize("hostname", autil::NetUtil::getBindIp());
        json.Jsonize("fence_name", _fenceName);
        std::vector<segmentid_t> segments;
        std::vector<schemaid_t> schemaIds;
        for (const auto& seg : _segments) {
            segments.push_back(seg.segmentId);
            schemaIds.push_back(seg.schemaId);
        }
        json.Jsonize("segments", segments);
        json.Jsonize("segment_schema_versions", schemaIds);
        json.Jsonize("sealed", _sealed);
        json.Jsonize("read_schema_version", _readSchemaId);
        UpdateSchemaVersionRoadMap();
        json.Jsonize("schema_version_road_map", _schemaVersionRoadMap);
        json.Jsonize("index_task_history", _indexTaskHistory);
        if (!_indexTaskQueue->Empty()) {
            json.Jsonize("index_task_queue", _indexTaskQueue, _indexTaskQueue);
        }
    } else {
        autil::legacy::json::JsonMap jsonMap = json.GetMap();
        if (jsonMap.find("format_version") == jsonMap.end()) {
            _formatVersion = 0;
        } else {
            json.Jsonize("format_version", _formatVersion);
        }
        LegacyFromJson(json);
        int64_t ts = INVALID_TIMESTAMP;
        json.Jsonize("timestamp", ts, ts);
        _timestamp = ts;

        std::string locatorString;
        json.Jsonize("locator", locatorString, locatorString);
        locatorString = autil::StringUtil::hexStrToStr(locatorString);
        _locator.Deserialize(locatorString);
        if (_formatVersion < 3) {
            //升级场景，在线是新架构但离线还是老架构的场景，由于新架构是根据locator中的progress进行回收的，因此需要把老version的locator改成versionTs,
            //并升级version formate
            _locator.SetOffset({_timestamp, 0});
            _locator.SetLegacyLocator();
            UpdateVersionFormate2To3();
        }

        json.Jsonize("fence_name", _fenceName, "");

        std::vector<segmentid_t> segments;
        std::vector<schemaid_t> schemaIds;
        json.Jsonize("segments", segments, segments);
        if (jsonMap.find("segment_schema_versions") == jsonMap.end()) {
            schemaIds.resize(segments.size(), _schemaId);
        } else {
            json.Jsonize("segment_schema_versions", schemaIds, schemaIds);
            assert(schemaIds.size() == segments.size());
        }
        _segments.clear();
        for (size_t i = 0; i < segments.size(); ++i) {
            _segments.push_back({segments[i], schemaIds[i]});
        }

        _segmentIdFilter.insert(segments.begin(), segments.end());
        if (jsonMap.find("last_segmentid") == jsonMap.end()) {
            if (segments.size() == 0) {
                _lastSegmentId = INVALID_SEGMENTID;
            } else {
                _lastSegmentId = _segments.back().segmentId;
            }
        } else {
            json.Jsonize("last_segmentid", _lastSegmentId);
        }
        json.Jsonize("sealed", _sealed, _sealed);
        _readSchemaId = _schemaId;
        json.Jsonize("read_schema_version", _readSchemaId, _readSchemaId);
        // if (jsonMap.find("ongoing_modify_operations") != jsonMap.end()) {
        //     AUTIL_LOG(ERROR, "ongoing_modify_operations exist in version[%d]", _versionId);
        //     AUTIL_LEGACY_THROW(autil::legacy::WrongFormatJsonException, "ongoing_modify_operations exist in
        //     version");
        // }
        json.Jsonize("schema_version_road_map", _schemaVersionRoadMap, _schemaVersionRoadMap);
        UpdateSchemaVersionRoadMap();
        if (jsonMap.find("index_task_history") != jsonMap.end()) {
            json.Jsonize("index_task_history", _indexTaskHistory);
        }
        json.Jsonize("index_task_queue", _indexTaskQueue, _indexTaskQueue);
        if (_versionLine.GetHeadVersion().GetVersionId() != _versionId && (_versionId & PUBLIC_VERSION_ID_MASK) &&
            !_fenceName.empty()) {
            _versionLine.AddCurrentVersion(VersionCoord(_versionId, _fenceName));
        }
    }

    // Validate();
}

schemaid_t Version::GetSegmentSchemaId(segmentid_t segId) const
{
    for (auto& segInVersion : _segments) {
        if (segInVersion.segmentId == segId) {
            return segInVersion.schemaId;
        }
    }
    return INVALID_SCHEMAID;
}

std::string Version::GetSegmentDirName(segmentid_t segmentId) const
{
    std::stringstream ss;
    if (IsLegacySegmentDirName()) {
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId;
        return ss.str();
    }
    return _segmentDescs->GetSegmentDir(segmentId);
}

bool Version::Validate() const
{
    segmentid_t lastSegmentId = INVALID_SEGMENTID;
    for (const auto& [segmentId, _] : _segments) {
        if (lastSegmentId != INVALID_SEGMENTID && lastSegmentId >= segmentId) {
            AUTIL_LOG(ERROR, "Invalid segmentId, last segment id [%d] >= cur segment id [%d]", lastSegmentId,
                      segmentId);
            return false;
        }
        lastSegmentId = segmentId;
    }
    return true;
}

segmentid_t Version::operator[](size_t idx) const
{
    assert(idx < _segments.size());
    return _segments[idx].segmentId;
}

std::string Version::GetVersionFileName() const { return GetVersionFileName(_versionId); }

std::string Version::GetVersionFileName(versionid_t versionId)
{
    std::stringstream ss;
    ss << VERSION_FILE_NAME_PREFIX << "." << versionId;
    return ss.str();
}

void Version::AddSegment(segmentid_t segmentId) { AddSegment(segmentId, _schemaId); }

void Version::AddSegment(segmentid_t segmentId, schemaid_t schemaId)
{
    if (segmentId > _lastSegmentId) {
        _lastSegmentId = segmentId;
    }
    _segments.push_back({segmentId, schemaId});
    std::sort(_segments.begin(), _segments.end());
    _segmentIdFilter.emplace(segmentId);
    if (!IsInSchemaVersionRoadMap(schemaId)) {
        UpdateSchemaVersionRoadMap();
    }
}

void Version::UpdateSegmentSchemaId(segmentid_t segmentId, schemaid_t schemaId)
{
    for (size_t i = 0; i < _segments.size(); i++) {
        if (_segments[i].segmentId == segmentId) {
            assert(IsInSchemaVersionRoadMap(schemaId));
            _segments[i].schemaId = schemaId;
            break;
        }
    }
}

void Version::SetLocator(const Locator& locator)
{
    if (!_locator.IsSameSrc(locator, false)) {
        _sealed = false;
    }
    _locator = locator;
}

void Version::RemoveSegment(segmentid_t segmentId)
{
    auto iter = std::find_if(_segments.begin(), _segments.end(),
                             [segmentId](const auto& seg) { return seg.segmentId == segmentId; });
    if (iter == _segments.end()) {
        return;
    }
    _segments.erase(iter);
    _segmentIdFilter.erase(segmentId);
    _segmentDescs->RemoveSegment(segmentId);
}

Status Version::FromString(const std::string& str) noexcept
{
    try {
        autil::legacy::FromJsonString(*this, str);
    } catch (const std::exception& e) {
        auto status = Status::ConfigError("Invalid json string[%s], exception[%s]", str.c_str(), e.what());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    return Status::OK();
}

std::string Version::ToString() const { return autil::legacy::ToJsonString(*this); }
std::string Version::DebugString() const
{
    std::stringstream ss;
    ss << "versionId: " << _versionId << ", segments: ";
    for (auto [segmentId, schemaId] : _segments) {
        ss << segmentId << ":" << schemaId << ", ";
    }
    return ss.str();
}

Version Version::Clone(std::string newFenceName) const
{
    auto versionStr = ToString();
    Version newVersion;
    auto status = newVersion.FromString(versionStr);
    assert(status.IsOK());
    newVersion.SetFenceName(newFenceName);
    if (_locator.IsLegacyLocator()) {
        newVersion._locator.SetLegacyLocator();
    }
    return newVersion;
}

bool Version::HasSegment(segmentid_t segmentId) const
{
    auto it = _segmentIdFilter.find(segmentId);
    if (it == _segmentIdFilter.end()) {
        return false;
    }
    return true;
}

void Version::UpdateSchemaVersionRoadMap()
{
    std::vector<schemaid_t> newRoadMap = _schemaVersionRoadMap;
    if (std::find(newRoadMap.begin(), newRoadMap.end(), _schemaId) == newRoadMap.end()) {
        newRoadMap.push_back(_schemaId);
    }

    for (auto& segInVersion : _segments) {
        if (segInVersion.schemaId > _schemaId) {
            AUTIL_LOG(WARN, "schemaId [%u] from segment [%d] is bigger than current schemaId [%d], ignore it",
                      segInVersion.schemaId, segInVersion.segmentId, _schemaId);
            continue;
        }
        if (std::find(newRoadMap.begin(), newRoadMap.end(), segInVersion.schemaId) == newRoadMap.end()) {
            AUTIL_LOG(WARN,
                      "schema version road map is missing schemaId [%u] from segment [%d], "
                      "add to road map by default",
                      segInVersion.schemaId, segInVersion.segmentId);
            newRoadMap.push_back(segInVersion.schemaId);
        }
    }
    std::sort(newRoadMap.begin(), newRoadMap.end());
    if (newRoadMap.size() != _schemaVersionRoadMap.size()) {
        _schemaVersionRoadMap = newRoadMap;
    }
}

void Version::UpdateVersionFormate2To3()
{
    if (_formatVersion == 2) {
        _formatVersion = 3;
    }
}

bool Version::IsInSchemaVersionRoadMap(schemaid_t schemaId) const
{
    assert(_schemaVersionRoadMap.empty() || std::is_sorted(_schemaVersionRoadMap.begin(), _schemaVersionRoadMap.end()));
    auto iter = std::lower_bound(_schemaVersionRoadMap.begin(), _schemaVersionRoadMap.end(), schemaId);
    return (iter != _schemaVersionRoadMap.end()) && (*iter == schemaId);
}

void Version::SetSchemaVersionRoadMap(const std::vector<schemaid_t>& schemaVersions)
{
    assert(schemaVersions.empty() || std::is_sorted(schemaVersions.begin(), schemaVersions.end()));
    _schemaVersionRoadMap = schemaVersions;
}

void Version::SetIndexTaskHistory(const IndexTaskHistory& his) { _indexTaskHistory.FromString(his.ToString()); }

void Version::AddIndexTaskLog(const std::string& taskType, const std::shared_ptr<IndexTaskLog>& logItem)
{
    _indexTaskHistory.AddLog(taskType, logItem);
}

void Version::AddDescription(const std::string& key, const std::string& value) { _description[key] = value; }

bool Version::GetDescription(const std::string& key, std::string& value) const
{
    auto iter = _description.find(key);
    if (iter == _description.end()) {
        return false;
    }
    value = iter->second;
    return true;
}

void Version::SetDescriptions(const std::map<std::string, std::string>& descs)
{
    for (auto iter = descs.begin(); iter != descs.end(); iter++) {
        _description[iter->first] = iter->second;
    }
}

void Version::MergeDescriptions(const Version& version)
{
    for (auto iter = version._description.begin(); iter != version._description.end(); iter++) {
        _description[iter->first] = iter->second;
    }
}

const VersionLine& Version::GetVersionLine() const { return _versionLine; }
void Version::SetVersionLine(const VersionLine& versionLine) { _versionLine = versionLine; }

std::shared_ptr<IndexTaskQueue> Version::GetIndexTaskQueue() const { return _indexTaskQueue; }

void Version::Finalize()
{
    if (GetVersionId() & PUBLIC_VERSION_ID_MASK) {
        VersionCoord versionCoord(GetVersionId(), GetFenceName());
        _versionLine.AddCurrentVersion(versionCoord);
    }
}

bool Version::CanFastFowardFrom(const VersionCoord& versionCoord, bool hasBuildingSegment) const
{
    return _versionLine.CanFastFowardFrom(versionCoord, hasBuildingSegment);
}

} // namespace indexlibv2::framework
