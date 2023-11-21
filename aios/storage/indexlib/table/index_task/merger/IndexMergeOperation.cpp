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
#include "indexlib/table/index_task/merger/IndexMergeOperation.h"

#include "autil/StringUtil.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergePlan.h"

using namespace std;

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, IndexMergeOperation);

Status IndexMergeOperation::Init(const std::shared_ptr<config::ITabletSchema> schema)
{
    string indexType;
    if (!_desc.GetParameter(MERGE_INDEX_TYPE, indexType)) {
        AUTIL_LOG(ERROR, "get index type failed");
        assert(false);
        return Status::Corruption("get index type failed");
    }
    string indexName;
    if (!_desc.GetParameter(MERGE_INDEX_NAME, indexName)) {
        AUTIL_LOG(ERROR, "get index type [%s]'s index name failed", indexType.c_str());
        assert(false);
        return Status::Corruption("get index name failed");
    }

    auto [configStatus, indexConfig] = GetIndexConfig(_desc, indexType, indexName, schema);
    RETURN_IF_STATUS_ERROR(configStatus, "Get Index Config fail for [%s]", indexName.c_str());
    auto creator = index::IndexFactoryCreator::GetInstance();
    auto [status, indexFactory] = creator->Create(indexType);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create index factory for index type [%s] failed", indexType.c_str());
        return status;
    }
    _indexMerger = indexFactory->CreateIndexMerger(indexConfig);
    if (_indexMerger == nullptr) {
        AUTIL_LOG(ERROR, "create index merger faild. index type [%s], index name [%s]", indexType.c_str(),
                  indexName.c_str());
        assert(false);
        return Status::Corruption("create index mergeer failed");
    }

    std::map<std::string, std::any> params;
    for (const auto& [k, v] : _desc.GetAllParameters()) {
        params[k] = v;
    }

    status = _indexMerger->Init(indexConfig, params);
    RETURN_IF_STATUS_ERROR(status, "init index merger failed. index type [%s], index name [%s]",
                           indexConfig->GetIndexType().c_str(), indexConfig->GetIndexName().c_str());
    _indexConfig = indexConfig;
    _indexPath = indexFactory->GetIndexPath();
    return Status::OK();
}

std::pair<Status, std::shared_ptr<config::IIndexConfig>>
IndexMergeOperation::GetIndexConfig(const framework::IndexOperationDescription& opDesc, const std::string& indexType,
                                    const std::string& indexName, const std::shared_ptr<config::ITabletSchema>& schema)
{
    auto indexConfig = schema->GetIndexConfig(indexType, indexName);
    if (!indexConfig) {
        AUTIL_LOG(ERROR, "get index config failed. index type [%s], index name [%s]", indexType.c_str(),
                  indexName.c_str());
        assert(false);
        return std::make_pair(Status::Corruption("get index config failed"), nullptr);
    }
    return std::make_pair(Status::OK(), indexConfig);
}

std::string IndexMergeOperation::GetDebugString() const
{
    return "IndexMergeOp." + _indexConfig->GetIndexType() + "." + _indexConfig->GetIndexName();
}

Status IndexMergeOperation::Execute(const framework::IndexTaskContext& context)
{
    auto [status, segmentMergeInfos] = PrepareSegmentMergeInfos(context, true);
    RETURN_IF_STATUS_ERROR(status, "prepare segment merge infos failed.");

    auto resourceManager = context.GetResourceManager();
    RETURN_IF_STATUS_ERROR(_indexMerger->Merge(segmentMergeInfos, resourceManager), "index merge failed");
    if (_desc.UseOpFenceDir()) {
        return StoreMergedSegmentMetrics(segmentMergeInfos, context);
    }
    return StoreMergedSegmentMetricsLegacy(segmentMergeInfos);
}

Status IndexMergeOperation::StoreMergedSegmentMetrics(const index::IIndexMerger::SegmentMergeInfos& segMergeInfos,
                                                      const framework::IndexTaskContext& context)
{
    auto [status, fenceDir] = context.GetOpFenceRoot(_desc.GetId(), _desc.UseOpFenceDir());
    RETURN_IF_STATUS_ERROR(status, "get op fence root failed");
    auto [status1, metricsTmpDir] =
        fenceDir->MakeDirectory(SEGMENT_METRICS_TMP_PATH, indexlib::file_system::DirectoryOption()).StatusWith();
    RETURN_IF_STATUS_ERROR(status1, "make segment metrics tmp path failed");
    for (size_t i = 0; i < segMergeInfos.targetSegments.size(); i++) {
        auto segmentMeta = segMergeInfos.targetSegments[i];
        if (segmentMeta->segmentMetrics->IsEmpty()) {
            continue;
        }
        std::string segmentName = PathUtil::GetFileNameFromPath(segmentMeta->segmentDir->GetRootDir());
        auto result = metricsTmpDir->MakeDirectory(segmentName, indexlib::file_system::DirectoryOption());
        RETURN_IF_STATUS_ERROR(result.Status(), "make segment metrics index dir failed");
        auto metricsDir = result.result;
        RETURN_IF_STATUS_ERROR(segmentMeta->segmentMetrics->StoreSegmentMetrics(metricsDir),
                               "store segment metrics failed");
    }
    return Status::OK();
}

Status IndexMergeOperation::StoreMergedSegmentMetricsLegacy(const index::IIndexMerger::SegmentMergeInfos& segMergeInfos)
{
    for (size_t i = 0; i < segMergeInfos.targetSegments.size(); i++) {
        auto segmentMeta = segMergeInfos.targetSegments[i];
        if (segmentMeta->segmentMetrics->IsEmpty()) {
            continue;
        }
        auto directory = segmentMeta->segmentDir->GetIDirectory();
        auto result = directory->MakeDirectory(SEGMENT_METRICS_TMP_PATH, indexlib::file_system::DirectoryOption());
        RETURN_IF_STATUS_ERROR(result.Status(), "make segment metrics tmp dir failed");
        auto segmentMetricsDir = result.result;
        auto indexPaths = _indexConfig->GetIndexPath();
        assert(indexPaths.size() == 1);
        RETURN_IF_STATUS_ERROR(
            segmentMetricsDir->RemoveDirectory(indexPaths[0], indexlib::file_system::RemoveOption::MayNonExist())
                .Status(),
            "make segment metrics tmp dir failed");
        result = segmentMetricsDir->MakeDirectory(indexPaths[0], indexlib::file_system::DirectoryOption());
        RETURN_IF_STATUS_ERROR(result.Status(), "make segment metrics index dir failed");
        auto indexDir = result.result;
        RETURN_IF_STATUS_ERROR(segmentMeta->segmentMetrics->StoreSegmentMetrics(indexDir),
                               "store segment metrics failed");
    }
    return Status::OK();
}

std::pair<Status, std::shared_ptr<indexlib::file_system::Directory>>
IndexMergeOperation::PrepareTargetSegDir(const std::shared_ptr<indexlib::file_system::IDirectory>& fenceDir,
                                         const std::string& segDir, const std::vector<std::string>& mergeIndexDirs)
{
    auto [status, segmentDir] = fenceDir->MakeDirectory(segDir, indexlib::file_system::DirectoryOption()).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "prepare target segment dir failed");
        return std::make_pair(status, nullptr);
    }
    for (auto mergeIndexDir : mergeIndexDirs) {
        status =
            segmentDir->RemoveDirectory(mergeIndexDir, indexlib::file_system::RemoveOption::MayNonExist()).Status();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "delete target index dir failed");
            return std::make_pair(status, nullptr);
        }
    }
    auto oldSegDir = indexlib::file_system::IDirectory::ToLegacyDirectory(segmentDir);
    return std::make_pair(Status::OK(), oldSegDir);
}

Status IndexMergeOperation::GetSegmentMergePlan(const framework::IndexTaskContext& context,
                                                SegmentMergePlan& segMergePlan, framework::Version& targetVersion)
{
    auto resourceManager = context.GetResourceManager();
    std::shared_ptr<MergePlan> mergePlan;
    auto status = resourceManager->LoadResource<MergePlan>(/*name=*/MERGE_PLAN, /*type=*/MERGE_PLAN, mergePlan);
    RETURN_IF_STATUS_ERROR(status, "load merge plan resource failed");
    int64_t idx = 0;
    if (!_desc.GetParameter(MERGE_PLAN_INDEX, idx)) {
        AUTIL_LOG(ERROR, "get merge plan idx from desc [%s] failed", ToJsonString(_desc, true).c_str());
        return Status::Corruption("get merge plan idx failed");
    }
    segMergePlan = mergePlan->GetSegmentMergePlan(idx);
    targetVersion = mergePlan->GetTargetVersion();
    return Status::OK();
}

std::pair<Status, index::IIndexMerger::SegmentMergeInfos>
IndexMergeOperation::PrepareSegmentMergeInfos(const framework::IndexTaskContext& context, bool prepareTargetSegDir)
{
    auto tabletData = context.GetTabletData();
    assert(tabletData);
    SegmentMergePlan segMergePlan;
    framework::Version version;
    auto status = GetSegmentMergePlan(context, segMergePlan, version);
    if (!status.IsOK()) {
        return std::make_pair(status, index::IIndexMerger::SegmentMergeInfos());
    }
    index::IIndexMerger::SegmentMergeInfos segMergeInfos;
    segMergeInfos.relocatableGlobalRoot = context.GetGlobalRelocatableFolder();
    for (size_t i = 0; i < segMergePlan.GetSrcSegmentCount(); i++) {
        index::IIndexMerger::SourceSegment sourceSegment;
        std::tie(sourceSegment.segment, sourceSegment.baseDocid) =
            tabletData->GetSegmentWithBaseDocid(segMergePlan.GetSrcSegmentId(i));
        segMergeInfos.srcSegments.push_back(sourceSegment);
    }
    auto [status2, fenceDir] = context.GetOpFenceRoot(_desc.GetId(), _desc.UseOpFenceDir());
    if (!status2.IsOK()) {
        AUTIL_LOG(ERROR, "prepare target op fence dir failed");
        return std::make_pair(status2, index::IIndexMerger::SegmentMergeInfos());
    }
    for (size_t i = 0; i < segMergePlan.GetTargetSegmentCount(); i++) {
        segmentid_t targetSegmentId = segMergePlan.GetTargetSegmentId(i);
        std::shared_ptr<framework::SegmentMeta> segMeta(new framework::SegmentMeta(targetSegmentId));
        const auto& segInfo = segMergePlan.GetTargetSegmentInfo(i);
        segMeta->segmentInfo.reset(new framework::SegmentInfo(segInfo));
        segMeta->schema = context.GetTabletSchema();
        auto indexPaths = _indexConfig->GetIndexPath();
        if (prepareTargetSegDir) {
            auto [status, segDir] =
                PrepareTargetSegDir(fenceDir, version.GetSegmentDirName(targetSegmentId), indexPaths);
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "prepare target segment dir failed");
                return std::make_pair(status, index::IIndexMerger::SegmentMergeInfos());
            }
            segMeta->segmentDir = segDir;
        }
        segMergeInfos.targetSegments.push_back(segMeta);
    }
    return std::make_pair(Status::OK(), segMergeInfos);
}

}} // namespace indexlibv2::table
