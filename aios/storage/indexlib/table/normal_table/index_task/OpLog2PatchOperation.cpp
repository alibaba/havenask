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
#include "indexlib/table/normal_table/index_task/OpLog2PatchOperation.h"

#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/archive/ArchiveDirectory.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/attribute/AttributePatchWriter.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"
#include "indexlib/index/common/patch/PatchWriter.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/deletionmap/DeletionMapConfig.h"
#include "indexlib/index/deletionmap/DeletionMapPatchWriter.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/patch/InvertedIndexPatchWriter.h"
#include "indexlib/index/operation_log/BatchOpLogIterator.h"
#include "indexlib/index/operation_log/OperationLogDiskIndexer.h"
#include "indexlib/index/operation_log/OperationLogIndexer.h"
#include "indexlib/index/operation_log/RemoveOperation.h"
#include "indexlib/index/operation_log/UpdateFieldOperation.h"
#include "indexlib/index/primary_key/config/PrimaryKeyLoadStrategyParam.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/normal_table/index_task/Common.h"
#include "indexlib/table/normal_table/index_task/PatchedDeletionMapLoader.h"

namespace indexlibv2 { namespace table {
namespace {
using indexlib::index::INVERTED_INDEX_TYPE_STR;
using indexlib::index::OPERATION_LOG_INDEX_NAME;
using indexlib::index::OPERATION_LOG_INDEX_TYPE_STR;
using indexlib::index::OperationBase;
using indexlib::index::OperationLogDiskIndexer;
using indexlib::index::OperationLogIndexer;
using indexlib::index::OperationMeta;
using indexlib::index::RemoveOperation;
using indexlibv2::index::IIndexMerger;
} // namespace

AUTIL_LOG_SETUP(indexlib.table, OpLog2PatchOperation);

class PkState
{
public:
    PkState()
    {
        _buffer = nullptr;
        _isPk128 = false;
    }
    ~PkState()
    {
        if (_buffer) {
            delete[] _buffer;
            _buffer = nullptr;
        }
    }

public:
    Status Init(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& pkConfig,
                const std::shared_ptr<framework::TabletData>& tabletData)
    {
        auto deletionMapConfig = std::make_shared<index::DeletionMapConfig>();
        _deletionMapReader.reset(new index::DeletionMapIndexReader);
        auto st = _deletionMapReader->Open(deletionMapConfig, tabletData.get());
        if (!st.IsOK()) {
            return st;
        }
        std::vector<framework::Segment*> segments;
        auto slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILT);
        for (auto iter = slice.begin(); iter != slice.end(); iter++) {
            segments.push_back(iter->get());
        }
        std::vector<index::SegmentDataAdapter::SegmentDataType> segmentDatas;
        index::SegmentDataAdapter::Transform(segments, segmentDatas);
        auto indexType = pkConfig->GetInvertedIndexType();
        if (indexType == it_primarykey64) {
            _isPk128 = false;
            return InnerInit(pkConfig, segmentDatas, _64pkHashTable, _64pkIter);
        } else if (indexType == it_primarykey128) {
            _isPk128 = true;
            return InnerInit(pkConfig, segmentDatas, _128pkHashTable, _128pkIter);
        }
        return Status::Corruption("invalid pk type");
    }
    docid_t Find(const uint64_t& key)
    {
        assert(_64pkHashTable);
        auto docid = _64pkHashTable->Find(key);
        if (_deletionMapReader->IsDeleted(docid)) {
            return INVALID_DOCID;
        }
        return docid;
    }
    docid_t Find(const autil::uint128_t& key)
    {
        assert(_128pkHashTable);
        auto docid = _128pkHashTable->Find(key);
        if (_deletionMapReader->IsDeleted(docid)) {
            return INVALID_DOCID;
        }
        return docid;
    }
    Status MoveUntil(docid_t endDocId)
    {
        if (!_isPk128) {
            return InnerMove(_64pkHashTable, _64pkIter, endDocId);
        }
        return InnerMove(_128pkHashTable, _128pkIter, endDocId);
    }
    bool Is128PK() { return _isPk128; }

private:
    template <typename T>
    Status InnerInit(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& pkConfig,
                     const std::vector<index::SegmentDataAdapter::SegmentDataType>& segmentDatas,
                     std::shared_ptr<index::PrimaryKeyHashTable<T>>& pkHashTable,
                     std::shared_ptr<index::PrimaryKeyIterator<T>>& iter);
    template <typename T>
    Status InnerMove(std::shared_ptr<index::PrimaryKeyHashTable<T>> pkHashTable,
                     std::shared_ptr<index::PrimaryKeyIterator<T>> iter, docid_t endDocId);
    template <typename T>
    docid_t InnerFind(const T& key);

private:
    std::shared_ptr<index::PrimaryKeyHashTable<uint64_t>> _64pkHashTable;
    std::shared_ptr<index::PrimaryKeyHashTable<autil::uint128_t>> _128pkHashTable;
    std::shared_ptr<index::PrimaryKeyIterator<uint64_t>> _64pkIter;
    std::shared_ptr<index::PrimaryKeyIterator<autil::uint128_t>> _128pkIter;
    std::shared_ptr<index::DeletionMapIndexReader> _deletionMapReader;
    char* _buffer;
    bool _isPk128;
};

template <typename T>
Status PkState::InnerMove(std::shared_ptr<index::PrimaryKeyHashTable<T>> pkHashTable,
                          std::shared_ptr<index::PrimaryKeyIterator<T>> iter, docid_t endDocId)
{
    while (iter->HasNext()) {
        auto currentPkPair = iter->GetCurrentPkPair();
        if (currentPkPair.docid >= endDocId) {
            break;
        }
        bool success = iter->Next(currentPkPair);
        if (!success) {
            return Status::Corruption();
        }
        pkHashTable->Insert(currentPkPair);
    }
    return Status::OK();
}

template <typename T>
Status PkState::InnerInit(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& pkConfig,
                          const std::vector<index::SegmentDataAdapter::SegmentDataType>& segmentDatas,
                          std::shared_ptr<index::PrimaryKeyHashTable<T>>& pkHashTable,
                          std::shared_ptr<index::PrimaryKeyIterator<T>>& iter)
{
    pkHashTable.reset(new index::PrimaryKeyHashTable<T>);
    iter.reset(new index::PrimaryKeyIterator<T>);
    if (!iter->Init(pkConfig, segmentDatas)) {
        return Status::Corruption();
    }
    size_t bufferLen = index::PrimaryKeyHashTable<T>::CalculateMemorySize(iter->GetPkCount(), iter->GetDocCount());
    _buffer = new char[bufferLen];
    pkHashTable->Init(_buffer, iter->GetPkCount(), iter->GetDocCount());
    return Status::OK();
}

OpLog2PatchOperation::OpLog2PatchOperation(const framework::IndexOperationDescription& opDesc)
    : framework::IndexOperation(opDesc.GetId(), opDesc.UseOpFenceDir())
    , _desc(opDesc)
    , _deletionMapIndexKey(GenerateIndexMapKey(/*indexType=*/index::DELETION_MAP_INDEX_TYPE_STR,
                                               /*indexName=*/index::DELETION_MAP_INDEX_NAME))
{
}

Status OpLog2PatchOperation::LoadPrimaryKeyReader(const std::shared_ptr<framework::TabletData>& tabletData)
{
    auto config = _schema->GetPrimaryKeyIndexConfig();
    std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig> pkConfig(
        std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(config));
    _pkState.reset(new PkState);
    RETURN_IF_STATUS_ERROR(_pkState->Init(pkConfig, tabletData), "init pk state failed");
    return Status::OK();
}

Status OpLog2PatchOperation::Execute(const framework::IndexTaskContext& context)
{
    segmentid_t targetPatchSegmentId;
    std::vector<IIndexMerger::SourceSegment> opSegments;
    auto status = GetOpLog2PatchInfos(context, targetPatchSegmentId, opSegments);
    RETURN_IF_STATUS_ERROR(status, "Failed to get source segments");
    if (opSegments.empty()) {
        return Status::OK();
    }
    auto [status1, fenceDir] = context.GetOpFenceRoot(_desc.GetId(), _desc.UseOpFenceDir());
    RETURN_IF_STATUS_ERROR(status1, "Failed to get op fence dir");
    if (!_desc.UseOpFenceDir()) {
        // legacy code todo delete by yijie
        status =
            fenceDir
                ->RemoveDirectory(OPERATION_LOG_TO_PATCH_WORK_DIR, indexlib::file_system::RemoveOption::MayNonExist())
                .Status();
        RETURN_IF_STATUS_ERROR(status, "remove directory [%s] failed", OPERATION_LOG_TO_PATCH_WORK_DIR);
    }
    auto ret = fenceDir->MakeDirectory(OPERATION_LOG_TO_PATCH_WORK_DIR, indexlib::file_system::DirectoryOption());
    status = ret.Status();
    RETURN_IF_STATUS_ERROR(status, "make directory failed, dir[%s]", OPERATION_LOG_TO_PATCH_WORK_DIR);
    _workDir = ret.result;
    if (context.GetMergeConfig().EnablePatchFileArchive()) {
        std::tie(status, _workDir) = _workDir->CreateArchiveDirectory(/*suffix*/ "").StatusWith();
        if (status.IsOK()) {
            AUTIL_LOG(INFO, "Create archive directory in [%s]", _workDir->DebugString().c_str());
        }
        if (!status.IsOK() || _workDir == nullptr) {
            return Status::IOError("Create archive directory in [%s] failed", ret.result->DebugString().c_str());
        }
    }
    _schema = context.GetTabletSchema();
    auto statusAndPKReader = LoadPrimaryKeyReader(context.GetTabletData());

    index::PatchFileInfos patchFileInfos;
    auto [mkdirDtatus, deletionMapWorkDir] =
        _workDir->MakeDirectory(indexlibv2::index::DELETION_MAP_INDEX_PATH, indexlib::file_system::DirectoryOption())
            .StatusWith();
    RETURN_STATUS_DIRECTLY_IF_ERROR(mkdirDtatus);
    std::unique_ptr<index::DeletionMapPatchWriter> deletionMapPatchWriter(
        new index::DeletionMapPatchWriter(deletionMapWorkDir, GetAllSegmentDocCount(context)));
    RETURN_STATUS_DIRECTLY_IF_ERROR(CreatePatchWritersForSegment(
        targetPatchSegmentId, &_fieldIdToMergeSegmentPatchWriters, &_indexKeyToMergeSegmentPatchWriters));

    for (const IIndexMerger::SourceSegment& segment : opSegments) {
        RETURN_IF_STATUS_ERROR(_pkState->MoveUntil(segment.baseDocid), "move pk stage failed");
        AUTIL_LOG(INFO, "Begin convert [segment_%d] op log to patch", segment.segment->GetSegmentId());
        std::map<fieldid_t, std::vector<index::PatchWriter*>> fieldIdToPatchWriters;
        std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>> indexKeyToPatchWriters;
        // TODO: Verify all file writers have different path+name pairs.
        RETURN_STATUS_DIRECTLY_IF_ERROR(CreatePatchWritersForSegment(segment.segment->GetSegmentId(),
                                                                     &fieldIdToPatchWriters, &indexKeyToPatchWriters));
        RETURN_STATUS_DIRECTLY_IF_ERROR(
            ConvertOpLog2Patch(segment, fieldIdToPatchWriters, indexKeyToPatchWriters, deletionMapPatchWriter.get()));
        for (const auto& pair : indexKeyToPatchWriters) {
            index::PatchWriter* patchWriter = pair.second.get();
            RETURN_IF_STATUS_ERROR(patchWriter->Close(), "close patch writer [%s, %s] failed", pair.first.first.c_str(),
                                   pair.first.second.c_str());
            // GetPatchFileInfos needs to be called after Close() because for some patch writers, PatchFileInfo(patch
            // file name etc) is determined at patch file dump/close stage.
            RETURN_STATUS_DIRECTLY_IF_ERROR(patchWriter->GetPatchFileInfos(&patchFileInfos));
        }
        AUTIL_LOG(INFO, "End convert [segment_%d] op log to patch", segment.segment->GetSegmentId());
    }

    for (const auto& pair : _indexKeyToMergeSegmentPatchWriters) {
        index::PatchWriter* patchWriter = pair.second.get();
        RETURN_IF_STATUS_ERROR(patchWriter->Close(), "close patch writer [%s, %s] failed", pair.first.first.c_str(),
                               pair.first.second.c_str());
        // GetPatchFileInfos needs to be called after Close() because for some patch writers, PatchFileInfo(patch
        // file name etc) is determined at patch file dump/close stage.
        RETURN_STATUS_DIRECTLY_IF_ERROR(patchWriter->GetPatchFileInfos(&patchFileInfos));
    }

    RETURN_IF_STATUS_ERROR(deletionMapPatchWriter->Close(), "close deletion patch writer failed");
    RETURN_STATUS_DIRECTLY_IF_ERROR(deletionMapPatchWriter->GetPatchFileInfos(&patchFileInfos));
    bool metaFileEnabled = context.GetMergeConfig().EnablePatchFileMeta();
    if (metaFileEnabled) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(_workDir
                                            ->Store(/*fileName=*/OPERATION_LOG_TO_PATCH_META_FILE_NAME,
                                                    /*fileContent=*/autil::legacy::ToJsonString(patchFileInfos),
                                                    indexlib::file_system::WriterOption::AtomicDump())
                                            .Status());
    }
    RETURN_IF_STATUS_ERROR(_workDir->Close().Status(), "close archive folder [%s] failed",
                           _workDir->DebugString().c_str());
    return Status::OK();
}

Status OpLog2PatchOperation::CreatePatchWritersForSegment(
    const segmentid_t segmentId, std::map<fieldid_t, std::vector<index::PatchWriter*>>* fieldIdToPatchWriters,
    std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>>* indexKeyToPatchWriters)
{
    auto indexConfigs = _schema->GetIndexConfigs();
    auto [status1, attributeWorkDir] =
        _workDir->MakeDirectory(indexlibv2::index::ATTRIBUTE_INDEX_PATH, indexlib::file_system::DirectoryOption())
            .StatusWith();
    RETURN_STATUS_DIRECTLY_IF_ERROR(status1);
    auto [status2, invertedIndexWorkDir] =
        _workDir->MakeDirectory(indexlib::index::INVERTED_INDEX_PATH, indexlib::file_system::DirectoryOption())
            .StatusWith();
    RETURN_STATUS_DIRECTLY_IF_ERROR(status2);
    for (const std::shared_ptr<config::IIndexConfig>& indexConfig : indexConfigs) {
        std::string indexType = indexConfig->GetIndexType();
        if (indexType == index::DELETION_MAP_INDEX_TYPE_STR) {
            continue;
        } else if (indexType == INVERTED_INDEX_TYPE_STR) {
            if (CreatePatchWriterForInvertedIndex(invertedIndexWorkDir, segmentId, _schema, indexConfig,
                                                  fieldIdToPatchWriters, indexKeyToPatchWriters)) {
                AUTIL_LOG(INFO, "Create patch writer for [segment_%d] [%s] [%s]", segmentId, indexType.c_str(),
                          indexConfig->GetIndexName().c_str());
            }
        } else if (indexType == index::ATTRIBUTE_INDEX_TYPE_STR) {
            if (CreatePatchWriterForAttribute(attributeWorkDir, segmentId, _schema, indexConfig, fieldIdToPatchWriters,
                                              indexKeyToPatchWriters)) {
                AUTIL_LOG(INFO, "Create patch writer for [segment_%d] [%s] [%s]", segmentId, indexType.c_str(),
                          indexConfig->GetIndexName().c_str());
            }
        } else {
            AUTIL_LOG(INFO, "Skipping index type: [%s]", indexType.c_str());
        }
    }
    return Status::OK();
}

bool OpLog2PatchOperation::CreatePatchWriterForAttribute(
    const std::shared_ptr<indexlib::file_system::IDirectory>& workDir, const segmentid_t segmentId,
    const std::shared_ptr<config::ITabletSchema>& schema, const std::shared_ptr<config::IIndexConfig>& indexConfig,
    std::map<fieldid_t, std::vector<index::PatchWriter*>>* fieldIdToPatchWriters,
    std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>>* indexKeyToPatchWriters)
{
    std::string indexType = indexConfig->GetIndexType();
    if (indexType != index::ATTRIBUTE_INDEX_TYPE_STR) {
        return false;
    }
    std::string indexName = indexConfig->GetIndexName();
    std::pair<std::string, std::string> indexMapKey = GenerateIndexMapKey(indexType, indexName);
    const auto& attributeConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(
        schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, indexName));
    if (attributeConfig == nullptr) {
        AUTIL_LOG(INFO, "Invalid attribute config encountered [%s]", indexName.c_str());
        return false;
    }
    fieldid_t fieldId = attributeConfig->GetFieldId();
    auto patchWriter = std::make_unique<index::AttributePatchWriter>(workDir, segmentId, attributeConfig);
    InsertPatchWriter(fieldId, patchWriter.get(), fieldIdToPatchWriters);
    assert(indexKeyToPatchWriters->find(indexMapKey) == indexKeyToPatchWriters->end());
    indexKeyToPatchWriters->insert({indexMapKey, std::move(patchWriter)});
    return true;
}

bool OpLog2PatchOperation::CreatePatchWriterForInvertedIndex(
    const std::shared_ptr<indexlib::file_system::IDirectory>& workDir, const segmentid_t segmentId,
    const std::shared_ptr<config::ITabletSchema>& schema, const std::shared_ptr<config::IIndexConfig>& indexConfig,
    std::map<fieldid_t, std::vector<index::PatchWriter*>>* fieldIdToPatchWriters,
    std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>>* indexKeyToPatchWriters)
{
    std::string indexType = indexConfig->GetIndexType();
    if (indexType != INVERTED_INDEX_TYPE_STR) {
        return false;
    }

    std::string indexName = indexConfig->GetIndexName();
    std::pair<std::string, std::string> indexMapKey = GenerateIndexMapKey(indexType, indexName);
    const auto& invertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(
        schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, indexName));
    if (invertedIndexConfig == nullptr) {
        AUTIL_LOG(WARN, "Invalid iondex config encountered[%s]", indexName.c_str());
        return false;
    }
    auto patchWriter = std::make_unique<indexlib::index::InvertedIndexPatchWriter>(workDir, segmentId, indexConfig);
    indexlibv2::config::InvertedIndexConfig::Iterator iter = invertedIndexConfig->CreateIterator();
    while (iter.HasNext()) {
        auto fieldConfig = iter.Next();
        fieldid_t fieldId = fieldConfig->GetFieldId();
        InsertPatchWriter(fieldId, patchWriter.get(), fieldIdToPatchWriters);
    }
    assert(indexKeyToPatchWriters->find(indexMapKey) == indexKeyToPatchWriters->end());
    indexKeyToPatchWriters->insert({indexMapKey, std::move(patchWriter)});
    return true;
}

std::pair<std::string, std::string> OpLog2PatchOperation::GenerateIndexMapKey(const std::string& indexType,
                                                                              const std::string& indexName)
{
    std::pair<std::string, std::string> indexMapKey = std::make_pair(indexType, indexName);
    return indexMapKey;
}

void OpLog2PatchOperation::InsertPatchWriter(
    const fieldid_t fieldId, index::PatchWriter* patchWriter,
    std::map<fieldid_t, std::vector<index::PatchWriter*>>* fieldIdToPatchWriters)
{
    if (fieldIdToPatchWriters->find(fieldId) == fieldIdToPatchWriters->end()) {
        fieldIdToPatchWriters->insert({fieldId, std::vector<index::PatchWriter*> {patchWriter}});
    } else {
        fieldIdToPatchWriters->at(fieldId).push_back(patchWriter);
    }
}

std::pair<Status, std::shared_ptr<indexlib::index::BatchOpLogIterator>>
OpLog2PatchOperation::CreateOpIterator(const config::ITabletSchema* schema, const IIndexMerger::SourceSegment& segment)
{
    auto opConfig = schema->GetIndexConfig(OPERATION_LOG_INDEX_TYPE_STR, OPERATION_LOG_INDEX_NAME);
    auto indexerPair = segment.segment->GetIndexer(opConfig->GetIndexType(), opConfig->GetIndexName());
    if (!indexerPair.first.IsOK()) {
        AUTIL_LOG(ERROR, "get indexer type [%s] name [%s] from [segment_%d] failed", opConfig->GetIndexType().c_str(),
                  opConfig->GetIndexName().c_str(), segment.segment->GetSegmentId());
        return {Status::Corruption("get operation log indexer failed"), nullptr};
    }
    auto indexer = indexerPair.second;
    std::shared_ptr<OperationLogIndexer> indexerBase;
    assert(segment.segment->GetSegmentStatus() == framework::Segment::SegmentStatus::ST_BUILT);
    auto typedIndexer = std::dynamic_pointer_cast<OperationLogDiskIndexer>(indexer);
    indexerBase = std::dynamic_pointer_cast<OperationLogIndexer>(typedIndexer);
    if (!indexerBase) {
        AUTIL_LOG(ERROR, "cast to operation log mem indexer failed");
        return {Status::Corruption("cast to operation log mem indexer failed"), nullptr};
    }
    OperationMeta operationMeta;
    if (indexerBase->GetOperationMeta(operationMeta)) {
        AUTIL_LOG(INFO, "create op iterator for [segment_%d] with [%lu] op", segment.segment->GetSegmentId(),
                  operationMeta.GetOperationCount());
    } else {
        AUTIL_LOG(WARN, "create op iterator for [segment_%d], but can not get operation meta",
                  segment.segment->GetSegmentId());
    }
    return indexerBase->CreateBatchIterator();
}

// Returns the globalDocId, localDocId, and targetSegmentId for the pk of the op from segments before currentSegment.
// If INVALID_DOCID and INVALID_SEGMENTID if the pk is not found in the segments before currentSegment.
Status OpLog2PatchOperation::LookupDocIdAndSegmentId(
    const OperationBase* op, const indexlibv2::index::IIndexMerger::SourceSegment& currentSegment,
    const std::vector<std::pair<segmentid_t, std::pair<docid_t, docid_t>>>& segmentId2DocIdRange, docid_t* globalDocId,
    docid_t* localDocId, segmentid_t* targetSegmentId)
{
    // docRange left closed right open

    *globalDocId = GetDocId(op);

    if (*globalDocId == INVALID_DOCID) {
        *localDocId = INVALID_DOCID;
        *targetSegmentId = currentSegment.segment->GetSegmentId();
        return Status::OK();
    }
    for (const auto& pair : segmentId2DocIdRange) {
        const docid_t baseDocId = pair.second.first;
        if (baseDocId <= *globalDocId and *globalDocId < pair.second.second) {
            *localDocId = *globalDocId - baseDocId;
            *targetSegmentId = pair.first;
            return Status::OK();
        }
    }
    AUTIL_LOG(ERROR, "unable to lookup doc id and segment id");
    return Status::Corruption("Unable to lookup doc id and segment id.");
}

docid_t OpLog2PatchOperation::GetDocId(const OperationBase* op)
{
    if (op->GetDocOperateType() == DocOperateType::DELETE_DOC) {
        if (_pkState->Is128PK()) {
            const RemoveOperation<autil::uint128_t>* castedOp =
                static_cast<const RemoveOperation<autil::uint128_t>*>(op);
            assert(castedOp != nullptr);
            return _pkState->Find(castedOp->GetPkHash());
        }
        const RemoveOperation<uint64_t>* castedOp = static_cast<const RemoveOperation<uint64_t>*>(op);
        assert(castedOp != nullptr);
        return _pkState->Find(castedOp->GetPkHash());
    }
    assert(op->GetDocOperateType() == DocOperateType::UPDATE_FIELD);
    if (_pkState->Is128PK()) {
        const indexlib::index::UpdateFieldOperation<autil::uint128_t>* castedOp =
            static_cast<const indexlib::index::UpdateFieldOperation<autil::uint128_t>*>(op);
        assert(castedOp != nullptr);
        return _pkState->Find(castedOp->GetPkHash());
    }
    const indexlib::index::UpdateFieldOperation<uint64_t>* castedOp =
        static_cast<const indexlib::index::UpdateFieldOperation<uint64_t>*>(op);
    assert(castedOp != nullptr);
    return _pkState->Find(castedOp->GetPkHash());
}

Status OpLog2PatchOperation::ExecuteOp2Patch(
    OperationBase* op, const IIndexMerger::SourceSegment& currentSegment,
    const std::map<fieldid_t, std::vector<index::PatchWriter*>>& fieldIdToPatchWriters,
    const std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>>& indexKeyToPatchWriters,
    index::DeletionMapPatchWriter* deletionMapPatchWriter)
{
    docid_t globalDocId = INVALID_DOCID;
    docid_t localDocId = INVALID_DOCID;
    segmentid_t targetSegmentId = INVALID_SEGMENTID;
    RETURN_IF_STATUS_ERROR(
        LookupDocIdAndSegmentId(op, currentSegment, _segmentId2DocIdRange, &globalDocId, &localDocId, &targetSegmentId),
        "look up docid and segment id failed");

    // If pk for op is not found in the segments before currentSegment, it means the op is invalid and can be
    // skipped.
    if (globalDocId == INVALID_DOCID) {
        return Status::OK();
    }
    // Even if the pk for op is found in the segments before currentSegment, it's possible that the op should
    // actually be applied to the same pk added in current segment. But it does no harm to generate a patch to
    // previous segment.
    // e.g. segment_0 add pk0, segment_1 add pk0, update_field pk0, current segment is segment_1.
    // In this case, update_field will have an op log, but it should actually be applied to the doc added in
    // segment_1. If we generate a patch to segment_0, the final value in segment_0 will become the update_field
    // value in segment_1, which is wrong. But since we will eventually use data from segment_1, segment_1 keeps the
    // right value so it does not matter whether segment_0 is right or not.
    assert(targetSegmentId != currentSegment.segment->GetSegmentId());
    DocOperateType docOperateType = op->GetDocOperateType();
    if (docOperateType == DocOperateType::DELETE_DOC) {
        RETURN_IF_STATUS_ERROR(ConvertRemoveOpLog2Patch(localDocId, targetSegmentId, deletionMapPatchWriter),
                               "Convert remove op log to patch on segment [%d] localDocId [%d] failed", targetSegmentId,
                               localDocId);
    } else {
        assert(docOperateType == DocOperateType::UPDATE_FIELD);
        if (framework::Segment::IsMergedSegmentId(targetSegmentId)) {
            RETURN_IF_STATUS_ERROR(
                ConvertUpdateOpLog2Patch(op, localDocId, targetSegmentId, _fieldIdToMergeSegmentPatchWriters),
                "Convert update op log to patch on segment [%d] localDocId [%d] failed", targetSegmentId, localDocId);
        } else {
            RETURN_IF_STATUS_ERROR(ConvertUpdateOpLog2Patch(op, localDocId, targetSegmentId, fieldIdToPatchWriters),
                                   "Convert update op log to patch on segment [%d] localDocId [%d] failed",
                                   targetSegmentId, localDocId);
        }
    }
    return Status::OK();
}

Status OpLog2PatchOperation::ConvertOpLog2Patch(
    const IIndexMerger::SourceSegment& currentSegment,
    const std::map<fieldid_t, std::vector<index::PatchWriter*>>& fieldIdToPatchWriters,
    const std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>>& indexKeyToPatchWriters,
    index::DeletionMapPatchWriter* deletionMapPatchWriter)
{
    ASSIGN_OR_RETURN(std::shared_ptr<indexlib::index::BatchOpLogIterator> opLogIterator,
                     CreateOpIterator(_schema.get(), currentSegment));
    if (opLogIterator == nullptr) {
        AUTIL_LOG(WARN, "op log iterator is null, skipping segment [%d]", currentSegment.segment->GetSegmentId());
        return Status::OK();
    }
    std::vector<std::vector<OperationBase*>> ops(THREAD_NUM);
    auto [status, allOperations] = opLogIterator->LoadOperations(THREAD_NUM);
    AUTIL_LOG(INFO, "begin convert operation for segment [%d] operation count [%lu]",
              currentSegment.segment->GetSegmentId(), allOperations.size());
    RETURN_IF_STATUS_ERROR(status, "iter load op failed");
    for (const auto& op : allOperations) {
        DocOperateType docOperateType = op->GetDocOperateType();
        if (docOperateType != DocOperateType::DELETE_DOC and docOperateType != DocOperateType::UPDATE_FIELD) {
            assert(false);
            continue;
        }
        const char* pkHash = op->GetPkHashPointer();
        int64_t groupIdx = 0;
        if (_pkState->Is128PK()) {
            groupIdx = (((const autil::uint128_t*)pkHash)->value[0] + ((const autil::uint128_t*)pkHash)->value[1]) %
                       THREAD_NUM;
        } else {
            groupIdx = (*(const uint64_t*)pkHash) % THREAD_NUM;
        }
        ops[groupIdx].push_back(op);
    }
    std::vector<autil::ThreadPtr> threads;
    std::vector<Status> results(THREAD_NUM, Status::Uninitialize());
    for (size_t i = 0; i < THREAD_NUM; ++i) {
        auto thread = autil::Thread::createThread(
            std::bind(&OpLog2PatchOperation::ProcessOperations, this, ops[i], currentSegment, &fieldIdToPatchWriters,
                      &indexKeyToPatchWriters, deletionMapPatchWriter, &results[i]),
            "oplog_2_patch");
        AUTIL_LOG(INFO, "process operation for thread [%lu], operation count [%lu]", i, ops[i].size());
        if (!thread) {
            AUTIL_LOG(ERROR, "create threadfailed, convert oplog to patch failed");
            return Status::Corruption();
        }
        threads.push_back(thread);
    }
    threads.clear();
    for (auto status : results) {
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "process operation has error");
            return status;
        }
    }
    AUTIL_LOG(INFO, "end convert operation for segment [%d]", currentSegment.segment->GetSegmentId());
    return Status::OK();
}

void OpLog2PatchOperation::ProcessOperations(
    const std::vector<OperationBase*> ops, const IIndexMerger::SourceSegment& currentSegment,
    const std::map<fieldid_t, std::vector<index::PatchWriter*>>* fieldIdToPatchWriters,
    const std::map<std::pair<std::string, std::string>, std::unique_ptr<index::PatchWriter>>* indexKeyToPatchWriters,
    index::DeletionMapPatchWriter* deletionMapPatchWriter, Status* result)
{
    for (auto op : ops) {
        auto st = ExecuteOp2Patch(op, currentSegment, *fieldIdToPatchWriters, *indexKeyToPatchWriters,
                                  deletionMapPatchWriter);
        if (!st.IsOK()) {
            *result = st;
            AUTIL_LOG(ERROR, "process operation failed");
            return;
        }
    }
    *result = Status::OK();
}

Status OpLog2PatchOperation::ConvertUpdateOpLog2Patch(
    OperationBase* op, docid_t localDocId, segmentid_t targetSegmentId,
    const std::map<fieldid_t, std::vector<index::PatchWriter*>>& fieldIdToPatchWriters)
{
    if (_pkState->Is128PK()) {
        return DoConvertUpdateOpLog2Patch<indexlib::index::UpdateFieldOperation<autil::uint128_t>>(
            op, localDocId, targetSegmentId, fieldIdToPatchWriters);
    } else {
        return DoConvertUpdateOpLog2Patch<indexlib::index::UpdateFieldOperation<uint64_t>>(
            op, localDocId, targetSegmentId, fieldIdToPatchWriters);
    }
}

template <typename UpdateFieldOperationType>
Status OpLog2PatchOperation::DoConvertUpdateOpLog2Patch(
    OperationBase* op, docid_t localDocId, segmentid_t targetSegmentId,
    const std::map<fieldid_t, std::vector<index::PatchWriter*>>& fieldIdToPatchWriters)
{
    auto updateOp = static_cast<UpdateFieldOperationType*>(op);
    size_t afterSeparatorI = 0;
    // 处理Attribute
    for (afterSeparatorI = 0; afterSeparatorI < updateOp->GetItemSize(); ++afterSeparatorI) {
        auto [fieldId, value] = updateOp->GetOperationItem(afterSeparatorI);
        if (fieldId == INVALID_FIELDID) {
            break;
        }
        assert(fieldIdToPatchWriters.find(fieldId) != fieldIdToPatchWriters.end());
        for (index::PatchWriter* patchWriter : fieldIdToPatchWriters.at(fieldId)) {
            index::AttributePatchWriter* attributePatchWriter = dynamic_cast<index::AttributePatchWriter*>(patchWriter);
            if (not attributePatchWriter) {
                continue;
            }
            RETURN_IF_STATUS_ERROR(attributePatchWriter->Write(targetSegmentId, localDocId, value, value.empty()),
                                   "value [%s]", value.data());
        }
    }
    // 处理InvertedIndex
    afterSeparatorI += 1;
    for (; afterSeparatorI < updateOp->GetItemSize(); ++afterSeparatorI) {
        auto [fieldId, value] = updateOp->GetOperationItem(afterSeparatorI);
        assert(fieldId != INVALID_FIELDID);
        autil::DataBuffer buffer((char*)value.data(), value.size());
        indexlib::document::ModifiedTokens modifiedTokens;
        modifiedTokens.Deserialize(buffer);
        assert(fieldId == modifiedTokens.FieldId());
        assert(fieldIdToPatchWriters.find(fieldId) != fieldIdToPatchWriters.end());
        for (index::PatchWriter* patchWriter : fieldIdToPatchWriters.at(fieldId)) {
            auto invertedPatchWriter = dynamic_cast<indexlib::index::InvertedIndexPatchWriter*>(patchWriter);
            if (not invertedPatchWriter) {
                continue;
            }
            for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
                uint64_t termKey = modifiedTokens[i].first;
                indexlib::document::ModifiedTokens::Operation op = modifiedTokens[i].second;
                auto status =
                    invertedPatchWriter->Write(targetSegmentId, localDocId, indexlib::index::DictKeyInfo(termKey),
                                               op == indexlib::document::ModifiedTokens::Operation::REMOVE);
                RETURN_IF_STATUS_ERROR(status, "value [%s]", value.data());
            }
            indexlib::document::ModifiedTokens::Operation nullTermOp;
            if (modifiedTokens.GetNullTermOperation(&nullTermOp)) {
                if (nullTermOp != indexlib::document::ModifiedTokens::Operation::NONE) {
                    auto status =
                        invertedPatchWriter->Write(targetSegmentId, localDocId, indexlib::index::DictKeyInfo::NULL_TERM,
                                                   nullTermOp == indexlib::document::ModifiedTokens::Operation::REMOVE);
                    RETURN_IF_STATUS_ERROR(status, "write null token to patch writer failed, segment [%d] docId [%d]",
                                           targetSegmentId, localDocId);
                }
            }
        }
    }
    return Status::OK();
}

Status OpLog2PatchOperation::ConvertRemoveOpLog2Patch(docid_t localDocId, segmentid_t targetSegmentId,
                                                      index::DeletionMapPatchWriter* deletionMapPatchWriter)
{
    return deletionMapPatchWriter->Write(targetSegmentId, localDocId);
}

Status OpLog2PatchOperation::GetOpLog2PatchInfos(const framework::IndexTaskContext& context,
                                                 segmentid_t& targetPatchSegmentId,
                                                 std::vector<IIndexMerger::SourceSegment>& opSegments)
{
    auto resourceManager = context.GetResourceManager();
    std::shared_ptr<MergePlan> mergePlan;
    auto status = resourceManager->LoadResource<MergePlan>(/*name=*/MERGE_PLAN, /*type=*/MERGE_PLAN, mergePlan);
    RETURN_IF_STATUS_ERROR(status, "load merge plan resource failed");
    std::set<segmentid_t> srcSegIds;
    targetPatchSegmentId = INVALID_SEGMENTID;
    for (size_t i = 0; i < mergePlan->Size(); ++i) {
        auto& segMergePlan = mergePlan->GetSegmentMergePlan(i);
        for (size_t idx = 0; idx < segMergePlan.GetSrcSegmentCount(); idx++) {
            srcSegIds.insert(segMergePlan.GetSrcSegmentId(idx));
        }
        for (size_t idx = 0; idx < segMergePlan.GetTargetSegmentCount(); idx++) {
            targetPatchSegmentId = std::max(targetPatchSegmentId, segMergePlan.GetTargetSegmentId(idx));
        }
    }

    const std::shared_ptr<framework::TabletData>& tabletData = context.GetTabletData();
    auto slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILT);
    for (const auto& seg : slice) {
        segmentid_t segmentId = seg->GetSegmentId();
        AUTIL_LOG(INFO, "begin process segment [%d]", segmentId);
        index::IIndexMerger::SourceSegment sourceSegment;
        std::tie(sourceSegment.segment, sourceSegment.baseDocid) = tabletData->GetSegmentWithBaseDocid(segmentId);
        _segmentId2DocIdRange.push_back(std::make_pair(
            segmentId, std::make_pair(sourceSegment.baseDocid,
                                      sourceSegment.baseDocid + sourceSegment.segment->GetSegmentInfo()->docCount)));
        if (srcSegIds.find(segmentId) == srcSegIds.end()) {
            AUTIL_LOG(INFO, "skip process segment [%d] not in src segments", segmentId);
            continue;
        }
        if (seg->GetSegmentInfo()->mergedSegment) {
            AUTIL_LOG(INFO, "skip segment merge [%d], is merged segment", segmentId);
            continue;
        }

        // legacy for old build segment, it has no op log
        if (framework::Segment::IsMergedSegmentId(segmentId)) {
            AUTIL_LOG(INFO, "skip segment [%d], is legacy build segment", segmentId);
            continue;
        }

        opSegments.emplace_back(std::move(sourceSegment));
    }
    return Status::OK();
}

std::map<segmentid_t, uint64_t> OpLog2PatchOperation::GetAllSegmentDocCount(const framework::IndexTaskContext& context)
{
    std::map<segmentid_t, uint64_t> segmentId2DocCount;
    auto slice = context.GetTabletData()->CreateSlice(framework::Segment::SegmentStatus::ST_BUILT);
    for (auto iter = slice.begin(); iter != slice.end(); iter++) {
        std::shared_ptr<framework::Segment> segment = *iter;
        segmentid_t segmentId = segment->GetSegmentId();
        segmentId2DocCount[segmentId] = segment->GetSegmentInfo()->docCount;
    }
    return segmentId2DocCount;
}

}} // namespace indexlibv2::table
