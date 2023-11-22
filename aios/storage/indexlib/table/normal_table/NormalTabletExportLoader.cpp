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
#include "indexlib/table/normal_table/NormalTabletExportLoader.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/PhysicalDirectory.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletDataSchemaGroup.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/operation_log/OperationCursor.h"
#include "indexlib/index/operation_log/OperationLogIndexReader.h"
#include "indexlib/index/operation_log/OperationLogReplayer.h"
#include "indexlib/index/operation_log/TargetSegmentsRedoStrategy.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/NormalTabletModifier.h"
#include "indexlib/table/normal_table/NormalTabletPatcher.h"

namespace indexlib::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletExportLoader);

#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

NormalTabletExportLoader::NormalTabletExportLoader(const std::string& fenceName, const std::string& workPath,
                                                   const std::optional<std::set<segmentid_t>>& targetSegmentIds)
    : indexlibv2::table::CommonTabletLoader(fenceName)
    , _targetSegmentIds(targetSegmentIds)
    , _workPath(file_system::FslibWrapper::JoinPath(workPath, "loader_tmp_dir"))
{
    assert(!workPath.empty());
}

std::pair<Status, std::unique_ptr<indexlibv2::framework::TabletData>>
NormalTabletExportLoader::ProcessTabletData(const std::shared_ptr<indexlibv2::framework::TabletData>& sourceTabletData)
{
    indexlibv2::framework::TabletData emptyTabletData(sourceTabletData->GetTabletName());
    auto resourceMap = sourceTabletData->GetResourceMap();
    auto status = emptyTabletData.Init(indexlibv2::framework::Version(0), {}, resourceMap);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "empty tablet data init failed");

    std::vector<std::shared_ptr<indexlibv2::framework::Segment>> segments;
    for (auto segment : sourceTabletData->CreateSlice(indexlibv2::framework::Segment::SegmentStatus::ST_BUILT)) {
        segments.push_back(segment);
    }

    status = DoPreLoad(emptyTabletData, segments, sourceTabletData->GetOnDiskVersion());
    RETURN2_IF_STATUS_ERROR(status, nullptr, "export loader preload version [%d] failed",
                            sourceTabletData->GetOnDiskVersion().GetVersionId());
    auto [finalLoadStatus, newTabletData] = FinalLoad(emptyTabletData);
    RETURN2_IF_STATUS_ERROR(finalLoadStatus, nullptr, "final load tablet data failed");
    status = newTabletData->GetResourceMap()->AddVersionResource(
        indexlibv2::framework::TabletDataSchemaGroup::NAME,
        resourceMap->GetResource(indexlibv2::framework::TabletDataSchemaGroup::NAME));
    RETURN2_IF_STATUS_ERROR(status, nullptr, "new tablet data add schema group resource failed");
    return {Status::OK(), std::move(newTabletData)};
}

using RedoParam =
    std::tuple<Status, std::unique_ptr<index::OperationLogReplayer>,
               std::unique_ptr<indexlibv2::table::NormalTabletModifier>, std::unique_ptr<index::PrimaryKeyIndexReader>>;
RedoParam NormalTabletExportLoader::CreateRedoParameters(const indexlibv2::framework::TabletData& tabletData) const
{
    // prepare operatonReplayer
    indexlib::index::OperationLogIndexReader operationIndexReader;
    auto opConfig = _schema->GetIndexConfig(indexlib::index::OPERATION_LOG_INDEX_TYPE_STR,
                                            indexlib::index::OPERATION_LOG_INDEX_NAME);
    // assert(opConfig);
    auto status = operationIndexReader.Open(opConfig, &tabletData);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "open operation index reader failed");
        return {status, nullptr, nullptr, nullptr};
    }
    auto opReplayer = operationIndexReader.CreateReplayer();
    assert(opReplayer);

    // prepare modifier

    auto modifier = std::make_unique<indexlibv2::table::NormalTabletModifier>();
    status = modifier->Init(_schema, tabletData, /*deleteInBranch=*/true, _op2PatchDir);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "init modifier failed");
        return {status, nullptr, nullptr, nullptr};
    }

    // prepare pkReader
    auto [createStatus, pkIndexFactory] =
        indexlibv2::index::IndexFactoryCreator::GetInstance()->Create(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR);
    if (!createStatus.IsOK()) {
        TABLET_LOG(ERROR, "create PrimaryKeyIndexFactory failed, %s", createStatus.ToString().c_str());
        return {createStatus, nullptr, nullptr, nullptr};
    }
    indexlibv2::index::IndexReaderParameter indexReaderParam;
    auto pkConfigs = _schema->GetIndexConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR);
    assert(1 == pkConfigs.size());
    auto pkReader = pkIndexFactory->CreateIndexReader(pkConfigs[0], indexReaderParam);
    assert(pkReader);

    std::unique_ptr<index::PrimaryKeyIndexReader> typedPkReader(
        dynamic_cast<index::PrimaryKeyIndexReader*>(pkReader.release()));
    if (!typedPkReader) {
        TABLET_LOG(ERROR, "cast pkReader failed");
        return {Status::InvalidArgs("cast pkReader failed"), nullptr, nullptr, nullptr};
    }

    status = typedPkReader->OpenWithoutPKAttribute(pkConfigs[0], &tabletData);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "open pkReader failed");
        return {status, nullptr, nullptr, nullptr};
    }
    return {Status::OK(), std::move(opReplayer), std::move(modifier), std::move(typedPkReader)};
}

Status NormalTabletExportLoader::DoPreLoad(const indexlibv2::framework::TabletData& lastTabletData,
                                           Segments newOnDiskVersionSegments,
                                           const indexlibv2::framework::Version& newOnDiskVersion)
{
    assert(lastTabletData.GetSegmentCount() == 0);
    assert(_schema->GetTableType() == TABLE_TYPE_NORMAL);

    _tabletName = lastTabletData.GetTabletName();
    TABLET_LOG(INFO, "do preload for table begin");

    _op2PatchDir = std::make_shared<file_system::PhysicalDirectory>(_workPath);
    auto status = _op2PatchDir->RemoveDirectory("", file_system::RemoveOption::MayNonExist()).Status();
    RETURN_IF_STATUS_ERROR(status, "remove directory [%s] failed", _workPath.c_str());

    auto [statusMkdir, directory] = _op2PatchDir->MakeDirectory("", file_system::DirectoryOption()).StatusWith();
    RETURN_IF_STATUS_ERROR(statusMkdir, "mkdir dir [%s] failed", _workPath.c_str());

    _tabletName = lastTabletData.GetTabletName();
    auto newTabletData = std::make_unique<indexlibv2::framework::TabletData>(_tabletName);
    RETURN_IF_STATUS_ERROR(newTabletData->Init(newOnDiskVersion.Clone(), newOnDiskVersionSegments,
                                               lastTabletData.GetResourceMap()->Clone()),
                           "new tablet data init fail");

    auto [paramStatus, opReplayer, patchModifier, pkReader] = CreateRedoParameters(*newTabletData);
    RETURN_IF_STATUS_ERROR(paramStatus, "prepare redo params failed");

    std::vector<segmentid_t> targetRedoSegments;
    for (auto segment : newTabletData->CreateSlice()) {
        index::TargetSegmentsRedoStrategy redoStrategy(true /*reopenDataConsistent*/);
        RETURN_IF_STATUS_ERROR(redoStrategy.Init(newTabletData.get(), segment->GetSegmentId(), targetRedoSegments),
                               "init redo strategy failed");
        index::OperationLogReplayer::RedoParams redoParams = {pkReader.get(), patchModifier.get(), &redoStrategy,
                                                              _memoryQuotaController.get()};
        indexlibv2::framework::Locator redoLocator;
        auto [status, cursor] =
            opReplayer->RedoOperationsFromOneSegment(redoParams, segment->GetSegmentId(), redoLocator);
        RETURN_IF_STATUS_ERROR(status, "redo operation failed");
        if (!_targetSegmentIds || _targetSegmentIds.value().count(segment->GetSegmentId())) {
            targetRedoSegments.push_back(segment->GetSegmentId());
        }
    }

    RETURN_IF_STATUS_ERROR(patchModifier->Close(), "patch modifier close failed");

    RETURN_IF_STATUS_ERROR(indexlibv2::table::NormalTabletPatcher::LoadPatch(newOnDiskVersionSegments, *newTabletData,
                                                                             _schema, _op2PatchDir, nullptr),
                           "load patch failed");
    _tabletData = std::move(newTabletData);
    TABLET_LOG(INFO, "do preload for table end");
    return Status::OK();
}

std::pair<Status, std::unique_ptr<indexlibv2::framework::TabletData>>
NormalTabletExportLoader::FinalLoad(const indexlibv2::framework::TabletData& currentTabletData)
{
    return {Status::OK(), std::move(_tabletData)};
}

} // namespace indexlib::table
