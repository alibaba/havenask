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
#include "indexlib/framework/VersionMeta.h"

#include "autil/StringUtil.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::framework {

VersionMeta::VersionMeta(const Version& version, int64_t docCount, int64_t indexSize)
    : _versionId(version.GetVersionId())
    , _readSchemaId(version.GetReadSchemaId())
    , _commitTime(version.GetCommitTime())
    , _timestamp(version.GetTimestamp())
    , _minLocatorTs(version.GetLocator().GetOffset().first)
    , _maxLocatorTs(version.GetLocator().GetMaxOffset().first)
    , _docCount(docCount)
    , _indexSize(indexSize)
    , _fenceName(version.GetFenceName())
    , _versionLine(version.GetVersionLine())
{
    for (const auto& indexTask : version.GetIndexTasks()) {
        auto meta = *indexTask;
        // in case of params are too long
        meta.params.clear();
        _indexTaskQueue.emplace_back(meta);
    }
    for (auto [segId, _] : version) {
        _segments.push_back(segId);
    }
    _sealed = version.IsSealed();
}

VersionMeta::VersionMeta(versionid_t versionId) : _versionId(versionId) {}

void VersionMeta::Jsonize(JsonWrapper& json)
{
    json.Jsonize("versionid", _versionId, _versionId);
    json.Jsonize("read_schema_id", _readSchemaId, _readSchemaId);
    json.Jsonize("segments", _segments, _segments);
    json.Jsonize("commit_time", _commitTime, _commitTime);
    json.Jsonize("timestamp", _timestamp, _timestamp);
    json.Jsonize("min_locator_ts", _minLocatorTs, _minLocatorTs);
    json.Jsonize("max_locator_ts", _maxLocatorTs, _maxLocatorTs);
    json.Jsonize("doc_count", _docCount, _docCount);
    json.Jsonize("index_size", _indexSize, _indexSize);
    json.Jsonize("sealed", _sealed, _sealed);
    json.Jsonize("fence_name", _fenceName, _fenceName);
    json.Jsonize("version_line", _versionLine, _versionLine);
    json.Jsonize("index_task_queue", _indexTaskQueue, _indexTaskQueue);
}

bool VersionMeta::operator==(const VersionMeta& other) const
{
    return _versionId == other._versionId && _readSchemaId == other._readSchemaId && _segments == other._segments &&
           _commitTime == other._commitTime && _timestamp == other._timestamp && _docCount == other._docCount &&
           _indexSize == other._indexSize && _fenceName == other._fenceName && _sealed == other._sealed &&
           _minLocatorTs == other._minLocatorTs && _maxLocatorTs == other._maxLocatorTs &&
           _versionLine == other._versionLine && _indexTaskQueue == other._indexTaskQueue;
}

void VersionMeta::TEST_Set(versionid_t versionId, std::vector<segmentid_t> segments, int64_t commitTime)
{
    _versionId = versionId;
    _segments = std::move(segments);
    _commitTime = commitTime;
}

} // namespace indexlibv2::framework
