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

#include <deque>
#include <memory>
#include <mutex>
#include <optional>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/ImportOptions.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionMerger.h"

namespace indexlibv2::framework {

class IdGenerator;
class CommitOptions;

class TabletCommitter : public autil::NoMoveable
{
private:
    class Operation
    {
    public:
        static Operation DumpOp(segmentid_t segId) { return Operation(segId, /*seal=*/false); }
        static Operation SealOp() { return Operation(INVALID_SEGMENTID, /*seal=*/true); }
        static Operation AlterTableOp(schemaid_t schemaId)
        {
            Operation op(INVALID_SEGMENTID, /*seal=*/false);
            op.SetSchemaId(schemaId);
            return op;
        }
        static Operation ImportOp(const std::vector<Version>& versions, const ImportOptions& options)
        {
            Operation op(INVALID_SEGMENTID, /*seal=*/false);
            op.SetImportVersions(versions, options);
            return op;
        }

    public:
        bool IsSeal() const { return _seal; }
        bool IsDump() const { return _segId != INVALID_SEGMENTID; }
        bool IsAlterTable() const { return _schemaId.has_value(); }
        bool IsImport() const { return !(_versions.empty()); }
        segmentid_t GetSegmentId() const
        {
            assert(_segId != INVALID_SEGMENTID);
            return _segId;
        }
        schemaid_t GetAlterSchemaId() const
        {
            assert(IsAlterTable());
            return _schemaId.value();
        }
        std::vector<Version> GetImportVersions() const
        {
            assert(IsImport());
            return _versions;
        }
        const ImportOptions& GetImportOptions() const { return _importOptions; }

    private:
        Operation(segmentid_t segId, bool seal) : _segId(segId), _seal(seal) {}
        void SetSchemaId(schemaid_t schemaId) { _schemaId = schemaId; }
        void SetImportVersions(const std::vector<Version>& versions, const ImportOptions& options)
        {
            _versions = versions;
            _importOptions = options;
        }

    private:
        segmentid_t _segId;
        std::optional<schemaid_t> _schemaId;
        bool _seal;
        std::vector<Version> _versions;
        ImportOptions _importOptions;
    };

public:
    explicit TabletCommitter(const std::string& tabletName);
    ~TabletCommitter() = default;
    void Init(const std::shared_ptr<VersionMerger>& versionMerger, const std::shared_ptr<TabletData>& tabletData);
    bool NeedCommit() const;
    void Push(segmentid_t segId);
    void Seal();
    void SetSealed(bool sealed);
    void AlterTable(schemaid_t schemaId);
    void Import(const std::vector<Version>& versions, const ImportOptions& options);
    void SetLastPublicVersion(const Version& version);
    void SetDumpError(Status status);
    Status GetDumpError() const;
    std::pair<Status, Version> Commit(const std::shared_ptr<TabletData>& tabletData, const Fence& fence,
                                      int32_t commitRetryCount, IdGenerator* idGenerator,
                                      const CommitOptions& commitOptions);

private:
    std::pair<Status, Version>
    GenerateVersionWithoutId(const std::shared_ptr<TabletData>& tabletData, const Fence& fence,
                             std::shared_ptr<VersionMerger::MergedVersionInfo> mergedVersionInfo,
                             std::vector<std::string>& filteredDirs);

private:
    mutable std::mutex _mutex;
    std::shared_ptr<VersionMerger> _versionMerger;
    const std::string _tabletName;
    std::deque<Operation> _commitQueue;
    segmentid_t _lastCommitSegmentId;
    Version _lastPublicVersion;
    std::vector<schemaid_t> _schemaRoadMap;
    Status _dumpErrorStatus;
    bool _sealed;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
