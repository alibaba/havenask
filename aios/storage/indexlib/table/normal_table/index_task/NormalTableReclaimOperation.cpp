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
#include "indexlib/table/normal_table/index_task/NormalTableReclaimOperation.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/SegmentDumpItem.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/index/attribute/AttributeIteratorTyped.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/MultiSliceAttributeDiskIndexer.h"
#include "indexlib/index/attribute/SingleValueAttributeDiskIndexer.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapPatchWriter.h"
#include "indexlib/table/index_task/merger/MergeUtil.h"
#include "indexlib/table/normal_table/NormalMemSegment.h"
#include "indexlib/table/normal_table/index_task/Common.h"
#include "indexlib/table/normal_table/index_task/PatchedDeletionMapLoader.h"
#include "indexlib/table/normal_table/index_task/PrepareIndexReclaimParamOperation.h"
#include "indexlib/table/normal_table/index_task/document_reclaim/IndexReclaimer.h"
#include "indexlib/table/normal_table/index_task/document_reclaim/IndexReclaimerCreator.h"
#include "indexlib/table/normal_table/index_task/document_reclaim/IndexReclaimerParam.h"
#include "indexlib/util/Clock.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableReclaimOperation);

NormalTableReclaimOperation::NormalTableReclaimOperation(const framework::IndexOperationDescription& desc)
    : framework::IndexOperation(desc.GetId(), desc.UseOpFenceDir())
    , _desc(desc)
{
}

std::pair<Status, std::vector<IndexReclaimerParam>>
NormalTableReclaimOperation::LoadIndexReclaimerParams(const framework::IndexTaskContext& context)
{
    auto dependOpIds = _desc.GetDepends();
    if (dependOpIds.empty()) {
        return {Status::OK(), {}};
    }

    assert(dependOpIds.size() == 1);
    if (dependOpIds.size() != 1) {
        std::string opsString = autil::StringUtil::toString(dependOpIds);
        return {Status::InternalError("can't support multi ops [%s]", opsString.c_str()), {}};
    }

    auto prepareIndexReclaimParamOpId = dependOpIds[0];
    auto fenceDirectory = context.GetDependOperationFenceRoot(prepareIndexReclaimParamOpId);
    if (!fenceDirectory) {
        AUTIL_LOG(ERROR, "get fence dir of depend op [%ld] failed", prepareIndexReclaimParamOpId);
        return {Status::IOError("get fence dir failed"), {}};
    }
    std::string reclaimParamStr;
    std::string filePath = PathUtil::JoinPath(PrepareIndexReclaimParamOperation::DOC_RECLAIM_DATA_DIR,
                                              PrepareIndexReclaimParamOperation::DOC_RECLAIM_CONF_FILE);

    std::vector<IndexReclaimerParam> indexReclaimParams;
    auto status =
        fenceDirectory
            ->Load(filePath, indexlib::file_system::ReaderOption(indexlib::file_system::FSOT_MEM), reclaimParamStr)
            .Status();
    RETURN2_IF_STATUS_ERROR(status, indexReclaimParams, "read file [%s] failed",
                            fenceDirectory->DebugString(filePath).c_str());

    try {
        FromJsonString(indexReclaimParams, reclaimParamStr);
    } catch (const indexlib::util::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "reclaim param [%s] from json failed", reclaimParamStr.c_str());
        return {Status::InvalidArgs(), {}};
    }

    return {Status::OK(), indexReclaimParams};
}

std::pair<Status, framework::SegmentMeta>
NormalTableReclaimOperation::CreateSegmentMeta(const framework::IndexTaskContext& context,
                                               const std::string& targetSegmentDirName, segmentid_t targetSegmentId,
                                               segmentid_t maxMergedSegmentId)
{
    auto segmentInfo = std::make_shared<framework::SegmentInfo>();
    auto segmentMetrics = std::make_shared<indexlib::framework::SegmentMetrics>();

    auto maxSegmentInfo = context.GetTabletData()->GetSegment(maxMergedSegmentId)->GetSegmentInfo();
    segmentInfo->SetLocator(maxSegmentInfo->GetLocator());
    segmentInfo->mergedSegment = true;
    segmentInfo->timestamp = maxSegmentInfo->GetTimestamp();
    segmentInfo->schemaId = context.GetTabletSchema()->GetSchemaId();
    framework::SegmentMeta segmentMeta;
    segmentMeta.segmentInfo = segmentInfo;
    segmentMeta.segmentMetrics = segmentMetrics;
    segmentMeta.schema = context.GetTabletSchema();
    segmentMeta.segmentId = targetSegmentId;

    auto [status, fenceRoot] = context.GetOpFenceRoot(_desc.GetId(), _desc.UseOpFenceDir());
    RETURN2_IF_STATUS_ERROR(status, framework::SegmentMeta(), "get op fence directory failed");
    if (!_desc.UseOpFenceDir()) {
        // legacy code, delete when all binary updated by chaofen
        auto status =
            fenceRoot->RemoveDirectory(targetSegmentDirName, indexlib::file_system::RemoveOption::MayNonExist())
                .Status();

        RETURN2_IF_STATUS_ERROR(status, framework::SegmentMeta(), "remove directory failed, dir[%s]",
                                targetSegmentDirName.c_str());
    }
    auto [status2, dir] =
        fenceRoot->MakeDirectory(targetSegmentDirName, indexlib::file_system::DirectoryOption()).StatusWith();
    RETURN2_IF_STATUS_ERROR(status2, framework::SegmentMeta(), "make directory failed, dir[%s]",
                            targetSegmentDirName.c_str());
    segmentMeta.segmentDir = indexlib::file_system::IDirectory::ToLegacyDirectory(dir);
    return {Status::OK(), segmentMeta};
}

Status NormalTableReclaimOperation::DumpSegment(const framework::IndexTaskContext& context,
                                                const framework::SegmentMeta& segmentMeta,
                                                const index::DeletionMapPatchWriter& deletionMapPatchWriter)
{
    // dump empty segment without segment info
    auto tabletOptions = context.GetTabletOptions();
    auto memSegment = std::make_unique<NormalMemSegment>(tabletOptions.get(), segmentMeta.schema, segmentMeta);
    framework::BuildResource resource;
    resource.memController = std::make_shared<MemoryQuotaController>(
        "merge", tabletOptions->GetOfflineConfig().GetMergeConfig().GetMaxMergeMemoryUse());

    auto status = memSegment->Open(resource, nullptr);
    RETURN_IF_STATUS_ERROR(status, "mem segment [%s] failed", segmentMeta.segmentDir->DebugString().c_str());
    memSegment->Seal();
    auto [dumpStatus, dumpItems] = memSegment->CreateSegmentDumpItems();
    RETURN_STATUS_DIRECTLY_IF_ERROR(dumpStatus);
    for (auto& dumpItem : dumpItems) {
        auto status = dumpItem->Dump();
        RETURN_IF_STATUS_ERROR(status, "dump item failed");
    }
    memSegment->EndDump();

    auto [mkdirStatus, patchDirectory] =
        segmentMeta.segmentDir->GetIDirectory()
            ->MakeDirectory(index::DELETION_MAP_INDEX_PATH, indexlib::file_system::DirectoryOption())
            .StatusWith();
    RETURN_IF_STATUS_ERROR(mkdirStatus, "make dir failed");

    std::vector<std::pair<segmentid_t, std::shared_ptr<index::DeletionMapDiskIndexer>>> patchedIndexerVec;
    RETURN_IF_STATUS_ERROR(PatchedDeletionMapLoader::GetPatchedDeletionMapDiskIndexers(context.GetTabletData(),
                                                                                       /*opPath dir*/ nullptr,
                                                                                       &patchedIndexerVec),
                           "get patch deletion map indexer from tablet and patch dir failed");

    std::map<segmentid_t, std::shared_ptr<index::DeletionMapDiskIndexer>> patchedIndexers(patchedIndexerVec.begin(),
                                                                                          patchedIndexerVec.end());

    for (const auto& [targetSegment, patchBitmap] : deletionMapPatchWriter.GetSegmentId2Bitmaps()) {
        auto deletionMapDiskIndexer = patchedIndexers[targetSegment];
        if (!deletionMapDiskIndexer) {
            RETURN_STATUS_ERROR(InternalError, "find segment [%d] in patchedIndexers failed", targetSegment);
        }
        RETURN_IF_STATUS_ERROR(deletionMapDiskIndexer->ApplyDeletionMapPatch(patchBitmap.get()),
                               "apply deletion map patch for seg [%d] failed", targetSegment);
        RETURN_STATUS_DIRECTLY_IF_ERROR(deletionMapDiskIndexer->Dump(patchDirectory));
        segmentMeta.segmentInfo->maxTTL =
            std::max(segmentMeta.segmentInfo->maxTTL,
                     context.GetTabletData()->GetSegment(targetSegment)->GetSegmentInfo()->GetMaxTTL());
    }
    RETURN_IF_STATUS_ERROR(segmentMeta.segmentInfo->Store(segmentMeta.segmentDir->GetIDirectory()),
                           "dump segment info failed");
    return Status::OK();
}

Status NormalTableReclaimOperation::ReclaimByTTL(const framework::IndexTaskContext& context,
                                                 const std::map<segmentid_t, size_t>& segmentId2DocCount,
                                                 index::DeletionMapPatchWriter* writer)
{
    int64_t reclaimDocCount = 0;
    auto currentTimeInSeconds = context.GetClock()->NowInSeconds();
    auto [status, ttlFieldName] = context.GetTabletSchema()->GetSetting<std::string>("ttl_field_name");
    RETURN_IF_STATUS_ERROR(status, "get ttl field failed");
    for (auto [segmentId, docCount] : segmentId2DocCount) {
        auto segment = context.GetTabletData()->GetSegment(segmentId);
        auto [status, indexer] = segment->GetIndexer(index::ATTRIBUTE_INDEX_TYPE_STR, ttlFieldName);
        RETURN_IF_STATUS_ERROR(status, "failed get attribute indexer for segment [%d], attribute [%s]", segmentId,
                               ttlFieldName.c_str());
        auto multiSliceIndexer = std::dynamic_pointer_cast<index::MultiSliceAttributeDiskIndexer>(indexer);
        assert(multiSliceIndexer);
        std::vector<std::shared_ptr<index::SingleValueAttributeDiskIndexer<uint32_t>>> indexers;
        std::vector<uint64_t> docCounts;
        for (int32_t i = 0; i < multiSliceIndexer->GetSliceCount(); i++) {
            auto diskIndexer = multiSliceIndexer->GetSliceIndexer<index::SingleValueAttributeDiskIndexer<uint32_t>>(i);
            if (!diskIndexer) {
                AUTIL_LOG(ERROR, "no indexer for segment [%d]", segmentId);
                return Status::InternalError("no indexer for segment [%d] with on OnDiskIndexer", segmentId);
            }
            auto sliceDocCount = multiSliceIndexer->GetSliceDocCount(i);
            indexers.push_back(diskIndexer);
            docCounts.push_back(sliceDocCount);
        }
        autil::mem_pool::Pool pool;
        index::AttributeIteratorTyped<uint32_t> iter(indexers, docCounts, nullptr, &pool);
        for (docid_t docId = 0; docId < docCount; ++docId) {
            uint32_t value;
            [[maybe_unused]] bool ret = iter.Seek(docId, value);
            assert(ret);
            if (value < currentTimeInSeconds) {
                reclaimDocCount++;
                auto status = writer->Write(segmentId, docId);
                RETURN_IF_STATUS_ERROR(status, "patch deletion writer segment [%d], docId [%d] failed", segmentId,
                                       docId);
            }
        }
    }
    AUTIL_LOG(INFO, "ttl enable and reclaim doc [%ld]", reclaimDocCount);
    INDEXLIB_FM_REPORT_METRIC_WITH_VALUE(TTLReclaim, reclaimDocCount);
    return Status::OK();
}

Status NormalTableReclaimOperation::Execute(const framework::IndexTaskContext& context)
{
    // generator reclaim segment
    // move to root
    AUTIL_LOG(INFO, "begin execute reclaim operation");
    _metricsReporter = context.GetMetricsManager()->GetMetricsReporter();
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, TTLReclaim, "docReclaim/TTLReclaim", kmonitor::GAUGE);

    std::string removeSegmentsStr;
    std::map<segmentid_t, size_t> segmentId2DocCount;
    std::map<segmentid_t, docid_t> segmentId2BaseDocId;
    std::vector<segmentid_t> removedSegmentVec;
    if (_desc.GetParameter<std::string>(REMOVED_SEGMENTS, removeSegmentsStr)) {
        try {
            autil::legacy::FromJsonString(removedSegmentVec, removeSegmentsStr);
        } catch (const indexlib::util::ExceptionBase& e) {
            RETURN_STATUS_ERROR(Corruption, "json param REMOVED_SEGMENTS [%s] failed", removeSegmentsStr.c_str());
        }
    } else {
        RETURN_STATUS_ERROR(Corruption, "get param REMOVED_SEGMENTS failed");
    }
    std::string targetSegmentDirName;
    if (!_desc.GetParameter(TARGET_SEGMENT_DIR, targetSegmentDirName)) {
        RETURN_STATUS_ERROR(Corruption, "get param TARGET_SEGMENT_DIR failed");
    }

    segmentid_t targetSegmentId;
    if (!_desc.GetParameter(TARGET_SEGMENT_ID, targetSegmentId)) {
        RETURN_STATUS_ERROR(Corruption, "get param TARGET_SEGMENT_ID failed");
    }

    std::set<segmentid_t> removedSegments(removedSegmentVec.begin(), removedSegmentVec.end());
    docid_t baseDocId = 0;
    segmentid_t maxMergedSegmentId = INVALID_SEGMENTID;
    for (auto segment : context.GetTabletData()->CreateSlice(framework::Segment::SegmentStatus::ST_BUILT)) {
        segmentid_t segmentId = segment->GetSegmentId();
        if (framework::Segment::IsMergedSegmentId(segmentId)) {
            maxMergedSegmentId = std::max(maxMergedSegmentId, segmentId);
            if (removedSegments.count(segmentId) == 0) {
                assert(segmentId < targetSegmentId);
                segmentId2DocCount[segmentId] = segment->GetSegmentInfo()->GetDocCount();
                segmentId2BaseDocId[segmentId] = baseDocId;
            }
        }
        baseDocId += segment->GetSegmentInfo()->GetDocCount();
    }
    index::DeletionMapPatchWriter writer(nullptr, segmentId2DocCount);

    // ttl reclaim
    auto [enableTTLStatus, enableTTL] = context.GetTabletSchema()->GetSetting<bool>("enable_ttl");
    if (enableTTLStatus.IsOK() && enableTTL) {
        auto status = ReclaimByTTL(context, segmentId2DocCount, &writer);
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);
    } else if (!enableTTLStatus.IsOK() && !enableTTLStatus.IsNotFound()) {
        RETURN_IF_STATUS_ERROR(enableTTLStatus, "get enable_ttl failed");
    }

    auto [loadParamsStatus, reclaimParams] = LoadIndexReclaimerParams(context);
    RETURN_IF_STATUS_ERROR(loadParamsStatus, "load index reclaim params invalid");

    IndexReclaimerCreator creator(context.GetTabletSchema(), context.GetTabletData(), segmentId2BaseDocId,
                                  segmentId2DocCount, _metricsReporter);

    for (const auto& reclaimParam : reclaimParams) {
        std::shared_ptr<IndexReclaimer> indexReclaimer(creator.Create(reclaimParam));
        if (indexReclaimer) {
            try {
                auto status = indexReclaimer->Reclaim(&writer);
                RETURN_STATUS_DIRECTLY_IF_ERROR(status);
            } catch (const autil::legacy::ExceptionBase& e) {
                AUTIL_LOG(ERROR, "catch exception [%s]", e.what());
                return Status::InternalError();
            }
        } else {
            std::string paramStr = ToJsonString(reclaimParam, true);
            RETURN_STATUS_ERROR(InvalidArgs, "create reclaimer for param[%s] failed", paramStr.c_str());
        }
    }

    auto [status, segmentMeta] = CreateSegmentMeta(context, targetSegmentDirName, targetSegmentId, maxMergedSegmentId);
    RETURN_STATUS_DIRECTLY_IF_ERROR(status);

    status = DumpSegment(context, segmentMeta, writer);
    RETURN_STATUS_DIRECTLY_IF_ERROR(status);

    AUTIL_LOG(INFO, "end execute reclaim operation");
    return Status::OK();
}

framework::IndexOperationDescription
NormalTableReclaimOperation::CreateOperationDescription(indexlibv2::framework::IndexOperationId id,
                                                        const std::string& segDirName, segmentid_t targetSegmentId,
                                                        const std::vector<segmentid_t>& removedSegments)
{
    framework::IndexOperationDescription opDesc(id, OPERATION_TYPE);
    opDesc.AddParameter(TARGET_SEGMENT_DIR, segDirName);
    opDesc.AddParameter(TARGET_SEGMENT_ID, targetSegmentId);
    opDesc.AddParameter(REMOVED_SEGMENTS, autil::legacy::ToJsonString(removedSegments));
    return opDesc;
}
} // namespace indexlibv2::table
