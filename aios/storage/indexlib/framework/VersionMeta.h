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
#include <limits>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/IndexTaskQueue.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionLine.h"

namespace indexlib::index_base {
class Version;
}

namespace indexlibv2::framework {

class VersionMeta : public autil::legacy::Jsonizable
{
public:
    VersionMeta() = default;
    explicit VersionMeta(versionid_t versionId);
    VersionMeta(const Version& version, int64_t docCount, int64_t indexSizeInBytes);

public:
    void Jsonize(JsonWrapper& json) override;

public:
    versionid_t GetVersionId() const { return _versionId; }
    schemaid_t GetReadSchemaId() const { return _readSchemaId; }
    const std::vector<segmentid_t>& GetSegments() const { return _segments; }
    int64_t GetCommitTime() const { return _commitTime; }
    int64_t GetTimestamp() const { return _timestamp; }
    int64_t GetMaxLocatorTs() const { return _maxLocatorTs; }
    int64_t GetMinLocatorTs() const { return _minLocatorTs; }
    int64_t GetDocCount() const { return _docCount; }
    // index size in bytes
    int64_t GetIndexSize() const { return _indexSize; }
    bool IsSealed() const { return _sealed; }
    std::string GetFenceName() const { return _fenceName; }
    const VersionLine& GetVersionLine() const { return _versionLine; }
    const std::vector<IndexTaskMeta>& GetIndexTaskQueue() const { return _indexTaskQueue; }
    bool operator==(const VersionMeta& other) const;

public:
    void TEST_Set(versionid_t versionId, std::vector<segmentid_t> segments, int64_t commitTime);
    void TEST_SetReadSchema(schemaid_t readSchemaId) { _readSchemaId = readSchemaId; }

private:
    versionid_t _versionId = INVALID_VERSIONID;
    schemaid_t _readSchemaId = DEFAULT_SCHEMAID;
    std::vector<segmentid_t> _segments;
    int64_t _commitTime = 0;
    int64_t _timestamp = INVALID_TIMESTAMP;
    int64_t _minLocatorTs = std::numeric_limits<int64_t>::max();
    int64_t _maxLocatorTs = INVALID_TIMESTAMP;
    int64_t _docCount = -1;
    int64_t _indexSize = -1;
    std::string _fenceName = "";
    VersionLine _versionLine;
    std::vector<IndexTaskMeta> _indexTaskQueue;
    bool _sealed = false;

private:
    friend class indexlib::index_base::Version; // compitable with legacy version
};

} // namespace indexlibv2::framework
