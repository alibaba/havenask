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

#include <memory>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/common/patch/PatchWriter.h"
#include "indexlib/index/operation_log/OperationBase.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"

namespace indexlib::index {
class BatchOpLogIterator;
}
namespace indexlibv2::index {
class DeletionMapPatchWriter;
} // namespace indexlibv2::index
namespace indexlibv2::config {
class TabletSchema;
}
namespace indexlibv2::table {
class SegmentMergePlan;
class PkState;

// For all build segments in mergep plan , convert operation log to patches.
// This operation only applies to normal table(containing index types of attribute, inverted index).

class OpLog2PatchOperation : public framework::IndexOperation
{
public:
    OpLog2PatchOperation(const framework::IndexOperationDescription& opDesc);
    ~OpLog2PatchOperation() {}

    Status Execute(const framework::IndexTaskContext& context) override;

private:
    void ProcessOperations(const std::vector<indexlib::index::OperationBase*> ops,
                           const index::IIndexMerger::SourceSegment& currentSegment,
                           const std::map<fieldid_t, std::vector<index::PatchWriter*>>* fieldIdToPatchWriters,
                           const std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>>*
                               indexKeyToPatchWriters,
                           index::DeletionMapPatchWriter* deletionMapPatchWriter, Status* result);

    Status ExecuteOp2Patch(indexlib::index::OperationBase* op, const index::IIndexMerger::SourceSegment& currentSegment,
                           const std::map<fieldid_t, std::vector<index::PatchWriter*>>& fieldIdToPatchWriters,
                           const std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>>&
                               indexKeyToPatchWriters,
                           index::DeletionMapPatchWriter* deletionMapPatchWriter);
    Status ConvertOpLog2Patch(const indexlibv2::index::IIndexMerger::SourceSegment& currentSegment,
                              const std::map<fieldid_t, std::vector<index::PatchWriter*>>& fieldIdToPatchWriters,
                              const std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>>&
                                  indexKeyToPatchWriters,
                              index::DeletionMapPatchWriter* deletionMapPatchWriter);
    std::pair<Status, std::shared_ptr<indexlib::index::BatchOpLogIterator>>
    CreateOpIterator(const config::TabletSchema* schema, const indexlibv2::index::IIndexMerger::SourceSegment& segment);
    Status GetOpLog2PatchInfos(const framework::IndexTaskContext& context, segmentid_t& targetPatchSegmentId,
                               std::vector<index::IIndexMerger::SourceSegment>& opSegments);
    Status LoadPrimaryKeyReader(const std::shared_ptr<framework::TabletData>& tabletData);

    Status CreatePatchWritersForSegment(
        const segmentid_t segmentId, std::map<fieldid_t, std::vector<index::PatchWriter*>>* fieldIdToPatchWriters,
        std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>>* indexKeyToPatchWriters);

    Status ConvertRemoveOpLog2Patch(docid_t localDocId, segmentid_t targetSegmentId,
                                    index::DeletionMapPatchWriter* deletionMapPatchWriter);
    Status ConvertUpdateOpLog2Patch(indexlib::index::OperationBase* op, docid_t localDocId, segmentid_t targetSegmentId,
                                    const std::map<fieldid_t, std::vector<index::PatchWriter*>>& fieldIdToPatchWriters);
    template <typename UpdateFieldOperationType>
    Status
    DoConvertUpdateOpLog2Patch(indexlib::index::OperationBase* op, docid_t localDocId, segmentid_t targetSegmentId,
                               const std::map<fieldid_t, std::vector<index::PatchWriter*>>& fieldIdToPatchWriters);

private:
    template <typename PkType>
    Status LoadPkTyped();
    // TODO: Move this function with other ones in the codebase.
    static std::pair<std::string, std::string> GenerateIndexMapKey(const std::string& indexType,
                                                                   const std::string& indexName);
    static void InsertPatchWriter(const fieldid_t fieldId, index::PatchWriter* patchWriter,
                                  std::map<fieldid_t, std::vector<index::PatchWriter*>>* fieldIdToPatchWriters);

    static bool CreatePatchWriterForAttribute(
        const std::shared_ptr<indexlib::file_system::IDirectory>& workDir, const segmentid_t segmentId,
        const std::shared_ptr<config::TabletSchema>& schema, const std::shared_ptr<config::IIndexConfig>& indexConfig,
        std::map<fieldid_t, std::vector<index::PatchWriter*>>* fieldIdToPatchWriters,
        std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>>* indexKeyToPatchWriters);
    static bool CreatePatchWriterForInvertedIndex(
        const std::shared_ptr<indexlib::file_system::IDirectory>& workDir, const segmentid_t segmentId,
        const std::shared_ptr<config::TabletSchema>& schema, const std::shared_ptr<config::IIndexConfig>& indexConfig,
        std::map<fieldid_t, std::vector<index::PatchWriter*>>* fieldIdToPatchWriters,
        std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>>* indexKeyToPatchWriters);

    Status LookupDocIdAndSegmentId(
        const indexlib::index::OperationBase* op, const indexlibv2::index::IIndexMerger::SourceSegment& currentSegment,
        const std::vector<std::pair<segmentid_t, std::pair<docid_t, docid_t>>>& segmentId2DocIdRange,
        docid_t* globalDocId, docid_t* localDocId, segmentid_t* targetSegmentId);

    static std::map<segmentid_t, uint64_t> GetAllSegmentDocCount(const framework::IndexTaskContext& context);

    docid_t GetDocId(const indexlib::index::OperationBase* op);

public:
    static constexpr char OPERATION_TYPE[] = "operation_log_to_patch";

private:
    const uint64_t THREAD_NUM = 10;

    framework::IndexOperationDescription _desc;
    std::shared_ptr<PkState> _pkState;
    std::pair<std::string, std::string> _deletionMapIndexKey;
    std::shared_ptr<indexlib::file_system::IDirectory> _workDir;
    std::shared_ptr<config::TabletSchema> _schema;
    std::vector<std::pair<segmentid_t, std::pair<docid_t, docid_t>>> _segmentId2DocIdRange;
    std::map<fieldid_t, std::vector<index::PatchWriter*>> _fieldIdToMergeSegmentPatchWriters;
    std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>>
        _indexKeyToMergeSegmentPatchWriters;

private:
    friend class OpLog2PatchOperationTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
