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
#include "indexlib/table/normal_table/index_task/ReclaimMapOperation.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/NormalSchemaResolver.h"
#include "indexlib/table/normal_table/config/SegmentGroupConfig.h"
#include "indexlib/table/normal_table/index_task/Common.h"
#include "indexlib/table/normal_table/index_task/NormalTableResourceCreator.h"
#include "indexlib/table/normal_table/index_task/PatchedDeletionMapLoader.h"
#include "indexlib/table/normal_table/index_task/ReclaimMap.h"
#include "indexlib/table/normal_table/index_task/SingleSegmentDocumentGroupSelector.h"
#include "indexlib/table/normal_table/index_task/SortedReclaimMap.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, ReclaimMapOperation);

ReclaimMapOperation::ReclaimMapOperation(const framework::IndexOperationDescription& description)
    : framework::IndexOperation(description.GetId(), description.UseOpFenceDir())
    , _description(description)
{
}

ReclaimMapOperation::~ReclaimMapOperation() {}

Status ReclaimMapOperation::Execute(const framework::IndexTaskContext& context)
{
    auto resourceManager = context.GetResourceManager();
    auto tabletData = context.GetTabletData();
    auto schema = context.GetTabletSchema();
    std::shared_ptr<MergePlan> mergePlan;
    RETURN_IF_STATUS_ERROR(
        resourceManager->LoadResource<MergePlan>(/*name=*/MERGE_PLAN, /*type=*/MERGE_PLAN, mergePlan),
        "get merge plan failed");
    int64_t idx = 0;
    if (!_description.GetParameter(MERGE_PLAN_INDEX, idx)) {
        AUTIL_LOG(ERROR, "reclaim map op,get merge plan idx from desc [%s] failed",
                  ToJsonString(_description, true).c_str());
        return Status::Corruption("reclaim map op,get merge plan idx from desc [%s] failed",
                                  ToJsonString(_description, true).c_str());
    }
    auto segmentMergePlan = mergePlan->GetSegmentMergePlan(idx);

    // apply patch for tablet data
    framework::IndexOperationId opLog2PatchOperationId;
    std::shared_ptr<indexlib::file_system::Directory> patchDir;
    std::vector<std::pair<segmentid_t, std::shared_ptr<index::DeletionMapDiskIndexer>>> indexPairs;
    if (_description.GetParameter(DEPENDENT_OPERATION_ID, opLog2PatchOperationId)) {
        auto [status, tmpDir] = GetPatchDir(context, opLog2PatchOperationId);
        RETURN_IF_STATUS_ERROR(status, "get patch dir failed");
        if (tmpDir) {
            patchDir = tmpDir;
        }
    }
    // create split handler
    std::string compactionType;
    ReclaimMap::SegmentSplitHandler splitHandler = nullptr;
    if (!_description.GetParameter(NORMAL_TABLE_COMPACTION_TYPE, compactionType)) {
        AUTIL_LOG(ERROR, "compaction type is not set");
        return Status::InvalidArgs("compaction type is not set");
    }
    AUTIL_LOG(INFO, "compaction type[%s]", compactionType.c_str());
    if (compactionType == NORMAL_TABLE_SPLIT_TYPE) {
        if (!schema) {
            AUTIL_LOG(ERROR, "schema is empty");
            return Status::InvalidArgs("hema is empty");
        }
        if (segmentMergePlan.GetSrcSegmentCount() != 1) {
            AUTIL_LOG(ERROR, "split only support source segment 1, but segment count [%lu]",
                      segmentMergePlan.GetSrcSegmentCount());
            return Status::InvalidArgs("split only support source segment 1, but segment count [%lu]",
                                       segmentMergePlan.GetSrcSegmentCount());
        }
        auto [status, segmentGroupConfig] = schema->GetSetting<SegmentGroupConfig>(NORMAL_TABLE_GROUP_CONFIG_KEY);
        RETURN_IF_STATUS_ERROR(status, "get segment group config failed");

        auto spliter = std::make_shared<SingleSegmentDocumentGroupSelector>();
        segmentid_t srcSegmentId = segmentMergePlan.GetSrcSegmentId(0);
        RETURN_IF_STATUS_ERROR(spliter->Init(srcSegmentId, resourceManager),
                               "segment spliter init failed, group config [%s]",
                               autil::legacy::ToJsonString(segmentGroupConfig, true).c_str());
        splitHandler = [spliter, srcSegmentId](segmentid_t segmentId,
                                               docid_t localId) -> std::pair<Status, segmentindex_t> {
            if (segmentId != srcSegmentId) {
                AUTIL_LOG(ERROR, "segment mismatch, expect [%d] but  [%d]", srcSegmentId, segmentId);
                return {Status::InvalidArgs("segment mismatch"), -1};
            }
            auto [status, groupId] = spliter->Select(localId);
            if (groupId > int32_t(std::numeric_limits<segmentindex_t>::max())) {
                AUTIL_LOG(ERROR, "target segment index [%d] overflow", groupId);
                return {Status::OutOfRange("target segment index [%d] overflow", groupId), -1};
            }
            return {status, (segmentindex_t)groupId};
        };
    }

    if (schema->GetIndexConfig(index::DELETION_MAP_INDEX_TYPE_STR, index::DELETION_MAP_INDEX_PATH)) {
        RETURN_IF_STATUS_ERROR(
            PatchedDeletionMapLoader::GetPatchedDeletionMapDiskIndexers(tabletData, patchDir, &indexPairs),
            "apply patch to deletion map failed");
    }
    auto [st, sortDescs] = schema->GetSetting<config::SortDescriptions>("sort_descriptions");
    assert(st.IsOK() || st.IsNotFound());
    bool isSorted = sortDescs.size() > 0;
    std::string reclaimmapName = NormalTableResourceCreator::GetReclaimMapName(idx, isSorted);
    if (isSorted) {
        std::shared_ptr<SortedReclaimMap> sortedReclaimMap;
        auto status =
            resourceManager->LoadResource(reclaimmapName, index::DocMapper::GetDocMapperType(), sortedReclaimMap);
        if (status.IsNoEntry()) {
            status =
                resourceManager->CreateResource(reclaimmapName, index::DocMapper::GetDocMapperType(), sortedReclaimMap);
        }
        RETURN_IF_STATUS_ERROR(status, "load or create reclaimmap failed");

        status = sortedReclaimMap->Init(schema, sortDescs, segmentMergePlan, tabletData, splitHandler);
        RETURN_IF_STATUS_ERROR(status, "init reclaimmap failed");
        status = resourceManager->CommitResource(reclaimmapName);
        RETURN_IF_STATUS_ERROR(status, "commit reclaimmap failed");
        return Status::OK();
    }
    std::shared_ptr<ReclaimMap> reclaimMap;
    auto status = resourceManager->LoadResource(reclaimmapName, index::DocMapper::GetDocMapperType(), reclaimMap);
    if (status.IsNoEntry()) {
        status = resourceManager->CreateResource(reclaimmapName, index::DocMapper::GetDocMapperType(), reclaimMap);
    }
    RETURN_IF_STATUS_ERROR(status, "load or create reclaimmap failed");

    status = reclaimMap->Init(segmentMergePlan, tabletData, splitHandler);
    RETURN_IF_STATUS_ERROR(status, "init reclaimmap failed");
    status = resourceManager->CommitResource(reclaimmapName);
    RETURN_IF_STATUS_ERROR(status, "commit reclaimmap failed");
    return Status::OK();
}

std::pair<Status, std::shared_ptr<indexlib::file_system::Directory>>
ReclaimMapOperation::GetPatchDir(const framework::IndexTaskContext& context,
                                 const framework::IndexOperationId& opLog2PatchOperationId)
{
    std::shared_ptr<indexlib::file_system::Directory> patchDir;
    std::shared_ptr<indexlib::file_system::IDirectory> opLog2PatchOpRootDir =
        context.GetDependOperationFenceRoot(opLog2PatchOperationId);
    if (opLog2PatchOpRootDir == nullptr) {
        AUTIL_LOG(ERROR, "Get op log2 patch dir failed, op id [%ld]", opLog2PatchOperationId)
        return {Status::Corruption("Get op log2 patch dir failed, op id [%ld]", opLog2PatchOperationId), nullptr};
    }

    auto [status, isDir] = opLog2PatchOpRootDir->IsDir(OPERATION_LOG_TO_PATCH_WORK_DIR).StatusWith();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "Check work dir [%s] of op [%ld] is exist failed",
                            OPERATION_LOG_TO_PATCH_WORK_DIR, opLog2PatchOperationId);
    if (!isDir) {
        return {Status::OK(), nullptr};
    }
    auto [status2, opLog2PatchWorkDir] =
        opLog2PatchOpRootDir->GetDirectory(OPERATION_LOG_TO_PATCH_WORK_DIR).StatusWith();
    RETURN2_IF_STATUS_ERROR(status2, nullptr, "Get work dir [%s] of op [%ld] failed", OPERATION_LOG_TO_PATCH_WORK_DIR,
                            opLog2PatchOperationId);
    auto [status3, archiveDir] = opLog2PatchWorkDir->CreateArchiveDirectory("").StatusWith();
    RETURN2_IF_STATUS_ERROR(status3, nullptr, "Create archive work dir [%s] of op [%ld] failed",
                            OPERATION_LOG_TO_PATCH_WORK_DIR, opLog2PatchOperationId);
    auto patchDirResult = archiveDir->GetDirectory(index::DELETION_MAP_INDEX_PATH);
    if (!patchDirResult.OK()) {
        AUTIL_LOG(ERROR, "get deletion map patch dir failed");
        return {Status::Corruption("get deletion map patch dir failed"), nullptr};
    }
    patchDir = indexlib::file_system::IDirectory::ToLegacyDirectory(patchDirResult.Value());

    return {Status::OK(), patchDir};
}

} // namespace indexlibv2::table
