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

#include <assert.h>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/TimeUtility.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/ITabletImporter.h"
#include "indexlib/framework/ImportOptions.h"
#include "indexlib/framework/IndexTaskQueue.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionMerger.h"
#include "indexlib/framework/index_task/MergeTaskDefine.h"

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
        static Operation ImportOp(const std::vector<Version>& versions,
                                  const std::shared_ptr<ITabletImporter>& importer, const ImportOptions& options)
        {
            Operation op(INVALID_SEGMENTID, /*seal=*/false);
            op.SetImportVersions(versions, importer, options);
            return op;
        }

        static Operation IndexTaskOp(const IndexTaskMeta& meta, Action action)
        {
            Operation op(INVALID_SEGMENTID, /*seal=*/false);
            op.SetIndexTask(meta, action);
            return op;
        }

    public:
        bool IsSeal() const { return _seal; }
        bool IsDump() const { return _segId != INVALID_SEGMENTID; }
        bool IsAlterTable() const { return _schemaId.has_value(); }
        bool IsImport() const { return !(_versions.empty()); }
        bool IsIndexTask() const { return _isIndexTaskOp; }
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
        // index task op
        IndexTaskMeta GetIndexTaskMeta() const { return _indexTaskMeta; }
        Action GetAction() const { return _action; }

        std::shared_ptr<ITabletImporter> GetImporter() const { return _importer; }
        const ImportOptions& GetImportOptions() const { return _importOptions; }

    private:
        Operation(segmentid_t segId, bool seal) : _segId(segId), _seal(seal) {}
        void SetSchemaId(schemaid_t schemaId) { _schemaId = schemaId; }
        void SetImportVersions(const std::vector<Version>& versions, const std::shared_ptr<ITabletImporter>& importer,
                               const ImportOptions& options)
        {
            _versions = versions;
            _importer = importer;
            _importOptions = options;
        }

        void SetIndexTask(const IndexTaskMeta& meta, Action action)
        {
            _indexTaskMeta = meta;
            _action = action;
            _isIndexTaskOp = true;
        }

    private:
        segmentid_t _segId;
        std::optional<schemaid_t> _schemaId;
        bool _seal;
        std::vector<Version> _versions;
        std::shared_ptr<ITabletImporter> _importer;
        ImportOptions _importOptions;
        // index task
        IndexTaskMeta _indexTaskMeta;
        Action _action;
        bool _isIndexTaskOp = false;
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
    void Import(const std::vector<Version>& versions, const std::shared_ptr<ITabletImporter>& importer,
                const ImportOptions& options);
    void HandleIndexTask(const IndexTaskMeta& meta, Action action);
    void SetLastPublicVersion(const Version& version);
    void SetDumpError(Status status);
    Status GetDumpError() const;
    std::pair<Status, Version> Commit(const std::shared_ptr<TabletData>& tabletData, const Fence& fence,
                                      int32_t commitRetryCount, IdGenerator* idGenerator,
                                      const CommitOptions& commitOptions);
    // check index task op exist in commit queue.
    bool HasIndexTask(const std::string& taskType, const std::string& taskTraceId) const;

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
