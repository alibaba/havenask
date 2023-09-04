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

#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/VersionLine.h"
#include "indexlib/framework/index_task/IndexTaskHistory.h"

namespace indexlibv2::framework {

struct IndexTaskMeta : public autil::legacy::Jsonizable {
public:
    static const std::string READY;
    static const std::string SUSPENDED;
    static const std::string ABORTED;
    static const std::string DONE;
    static const std::string UNKNOWN;

    IndexTaskMeta() = default;
    IndexTaskMeta(const std::string& taskType, const std::string& taskName,
                  const std::map<std::string, std::string>& params)
        : taskType(taskType)
        , taskName(taskName)
        , params(params)
    {
    }
    bool operator==(const IndexTaskMeta& other) const
    {
        return taskType == other.taskType && taskName == other.taskName && state == other.state &&
               params == other.params && comment == other.comment && beginTimeInSecs == other.beginTimeInSecs &&
               endTimeInSecs == other.endTimeInSecs && committedVersionId == other.committedVersionId;
    }
    void Jsonize(JsonWrapper& json) override
    {
        json.Jsonize("task_type", taskType, taskType);
        json.Jsonize("task_name", taskName, taskName);
        json.Jsonize("state", state, state);
        json.Jsonize("begin_time_in_secs", beginTimeInSecs, beginTimeInSecs);
        json.Jsonize("end_time_in_secs", endTimeInSecs, endTimeInSecs);
        json.Jsonize("comment", comment, comment);
        json.Jsonize("committed_version_id", committedVersionId, committedVersionId);
        if (json.GetMode() == TO_JSON) {
            if (!params.empty()) {
                json.Jsonize("params", params, params);
            }
        } else {
            json.Jsonize("params", params, params);
        }
    }

    std::string taskType;
    std::string taskName;
    std::string state = READY;
    int64_t beginTimeInSecs = indexlib::INVALID_TIMESTAMP;
    int64_t endTimeInSecs = indexlib::INVALID_TIMESTAMP;
    std::map<std::string, std::string> params;
    std::string comment;
    versionid_t committedVersionId = INVALID_VERSIONID;
};

class SegmentDescriptions;

class Version : public autil::legacy::Jsonizable
{
private:
    struct SegmentInVersion {
        segmentid_t segmentId = INVALID_SEGMENTID;
        schemaid_t schemaId = DEFAULT_SCHEMAID;
        bool operator<(const SegmentInVersion& other) const { return segmentId < other.segmentId; }
        bool operator==(const SegmentInVersion& other) const
        {
            return segmentId == other.segmentId && schemaId == other.schemaId;
        }
    };

public:
    Version();
    explicit Version(versionid_t versionId);
    Version(versionid_t versionId, int64_t timestamp);
    Version(const Version&) = default;
    Version& operator=(const Version&) = default;
    Version(Version&&) = default;
    Version& operator=(Version&&) = default;
    ~Version() = default;

public:
    void Jsonize(JsonWrapper& json) override;
    Status FromString(const std::string& str) noexcept;
    std::string ToString() const;

public:
    void SetTabletName(std::string tabletName) { _tabletName = std::move(tabletName); }
    const std::string& GetTabletName() const { return _tabletName; }
    void SetFenceName(std::string fenceName) { _fenceName = std::move(fenceName); }
    const std::string& GetFenceName() const { return _fenceName; }
    bool IsValid() const { return _versionId != INVALID_VERSIONID; }
    void AddSegment(segmentid_t segmentId);
    void AddSegment(segmentid_t segmentId, schemaid_t schemaId);
    void RemoveSegment(segmentid_t segmentId);
    void UpdateSegmentSchemaId(segmentid_t segmentId, schemaid_t schemaId);
    void IncVersionId() { ++_versionId; }
    void SetTimestamp(int64_t timestamp) { _timestamp = timestamp; }
    int64_t GetTimestamp() const { return _timestamp; }
    void SetCommitTime(int64_t commitTime) { _commitTime = commitTime; }
    int64_t GetCommitTime() const { return _commitTime; }
    void SetLocator(const Locator& locator);
    const Locator& GetLocator() const { return _locator; }
    void SetSealed() { _sealed = true; }
    bool IsSealed() const { return _sealed; };
    segmentid_t GetLastSegmentId() const { return _lastSegmentId; }
    void SetLastSegmentId(segmentid_t segId) { _lastSegmentId = segId; }
    versionid_t GetVersionId() const { return _versionId; }
    void SetVersionId(versionid_t versionId) { _versionId = versionId; }
    size_t GetSegmentCount() const { return _segments.size(); }
    void SetFormatVersion(uint32_t formatVersion) { _formatVersion = formatVersion; }
    uint32_t GetFormatVersion() const { return _formatVersion; }
    std::shared_ptr<SegmentDescriptions> GetSegmentDescriptions() const { return _segmentDescs; }
    auto begin() { return _segments.begin(); }
    auto end() { return _segments.end(); }
    auto begin() const { return _segments.cbegin(); }
    auto end() const { return _segments.cend(); }
    segmentid_t operator[](size_t idx) const;
    bool HasSegment(segmentid_t segment) const;
    std::string GetSegmentDirName(segmentid_t segmentId) const;
    std::string GetVersionFileName() const;
    static std::string GetVersionFileName(versionid_t versionId);
    std::string DebugString() const;
    void SetSchemaId(schemaid_t schemaid)
    {
        _schemaId = schemaid;
        UpdateSchemaVersionRoadMap();
    }
    schemaid_t GetSchemaId() const { return _schemaId; }
    schemaid_t GetSegmentSchemaId(segmentid_t segId) const;
    void SetReadSchemaId(schemaid_t schemaId) { _readSchemaId = schemaId; }
    schemaid_t GetReadSchemaId() const { return _readSchemaId; }

    void SetSchemaVersionRoadMap(const std::vector<schemaid_t>& schemaVersions);
    const std::vector<schemaid_t>& GetSchemaVersionRoadMap() const { return _schemaVersionRoadMap; }

    const IndexTaskHistory& GetIndexTaskHistory() const { return _indexTaskHistory; }
    IndexTaskHistory& GetIndexTaskHistory() { return _indexTaskHistory; }
    void SetIndexTaskHistory(const IndexTaskHistory& his);
    void AddIndexTaskLog(const std::string& taskType, const std::shared_ptr<IndexTaskLog>& logItem);

    // TODO(yonghao.fyh) : refactor index task queue and index task history.
    void SetIndexTasks(const std::vector<std::shared_ptr<IndexTaskMeta>>& indexTasks);
    void AddIndexTask(const std::string& taskType, const std::string& taskName,
                      const std::map<std::string, std::string>& params, const std::string& state = IndexTaskMeta::READY,
                      const std::string& comment = "");
    void UpdateIndexTaskState(const std::string& taskType, const std::string& taskName, const std::string& state,
                              const std::string& comment = "");
    void OverwriteIndexTask(const std::string& taskType, const std::string& taskName,
                            const std::map<std::string, std::string>& params);

    std::shared_ptr<IndexTaskMeta> GetIndexTask(const std::string& taskType, const std::string& taskName) const;
    std::vector<std::shared_ptr<IndexTaskMeta>> GetIndexTasks() const;

    bool IsInSchemaVersionRoadMap(schemaid_t schemaId) const;

    void AddDescription(const std::string& key, const std::string& value);
    bool GetDescription(const std::string& key, std::string& value) const;

    const VersionLine& GetVersionLine() const;
    void SetVersionLine(const VersionLine& versionLine);
    void SetDescriptions(const std::map<std::string, std::string>& descs);
    void MergeDescriptions(const Version& version);

    Version Clone(std::string newFenceName) const;
    Version Clone() const { return Clone(_fenceName); }
    void Finalize();
    bool CanFastFowardFrom(const VersionCoord& versionCoord, bool hasBuildingSegment) const;

    static constexpr versionid_t PUBLIC_VERSION_ID_MASK = 0x1 << 29;
    static constexpr versionid_t PRIVATE_VERSION_ID_MASK = 0x1 << 30;
    static constexpr versionid_t MERGED_VERSION_ID_MASK = 0;

private:
    static constexpr uint32_t CURRENT_FORMAT_VERSION = 3;

private:
    void UpdateVersionFormate2To3();
    bool Validate() const;
    bool ValidateStateTransfer(const std::string& srcState, const std::string& dstState) const;
    void LegacyFromJson(JsonWrapper& json);
    void SortSegments();
    bool IsLegacySegmentDirName() const { return _formatVersion == 0; }
    void UpdateSchemaVersionRoadMap();

private:
    versionid_t _versionId = INVALID_VERSIONID;
    segmentid_t _lastSegmentId = INVALID_SEGMENTID;
    uint32_t _formatVersion = CURRENT_FORMAT_VERSION;
    int64_t _timestamp = INVALID_TIMESTAMP;
    int64_t _commitTime = 0;
    Locator _locator;
    bool _sealed = false;

    std::string _tabletName;
    std::string _fenceName;
    std::vector<SegmentInVersion> _segments;
    std::unordered_set<segmentid_t> _segmentIdFilter; // speed up to find
    std::shared_ptr<SegmentDescriptions> _segmentDescs;
    schemaid_t _schemaId = DEFAULT_SCHEMAID;
    schemaid_t _readSchemaId = DEFAULT_SCHEMAID;
    std::vector<schemaid_t> _schemaVersionRoadMap;
    IndexTaskHistory _indexTaskHistory;
    std::map<std::string, std::string> _description;
    VersionLine _versionLine;

    std::vector<std::shared_ptr<IndexTaskMeta>> _indexTaskQueue;
    // <taskType, taskName>
    std::set<std::pair<std::string, std::string>> _indexTaskSet;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
