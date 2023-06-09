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
#include "indexlib/framework/index_task/IndexTaskContext.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/relocatable/RelocatableFolder.h"
#include "indexlib/file_system/relocatable/Relocator.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/util/PathUtil.h"

namespace {
const std::string RELOCATABLE_PREFIX = "__relocatable__";
}

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, IndexTaskContext);

#define TABLET_LOG(level, format, args...)                                                                             \
    do {                                                                                                               \
        if (_tabletData != nullptr) {                                                                                  \
            AUTIL_LOG(level, "[%s] [%p] " format, _tabletData->GetTabletName().c_str(), this, ##args);                 \
        }                                                                                                              \
    } while (0)

std::shared_ptr<indexlib::file_system::IDirectory>
IndexTaskContext::GetDependOperationFenceRoot(IndexOperationId id) const
{
    auto iter = _finishedOpFenceRoots.find(id);
    if (iter == _finishedOpFenceRoots.end()) {
        TABLET_LOG(ERROR, "op[%ld] execute epoch not found", id);
        return nullptr;
    }
    return iter->second;
}

std::shared_ptr<config::TabletSchema> IndexTaskContext::GetTabletSchema(schemaid_t schemaId) const
{
    auto schema = _tabletData->GetTabletSchema(schemaId);
    if (schema) {
        return schema;
    }
    std::string root = GetIndexRoot()->GetPhysicalPath("");
    schema = std::make_shared<config::TabletSchema>();
    return TabletSchemaLoader::LoadSchema(GetIndexRoot()->GetIDirectory(),
                                          config::TabletSchema::GetSchemaFileName(schemaId));
}

const std::shared_ptr<indexlib::file_system::Directory>& IndexTaskContext::GetIndexRoot() const { return _indexRoot; }

std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
IndexTaskContext::CreateOpFenceRoot(IndexOperationId id, bool useOpFenceDir) const
{
    if (!useOpFenceDir) {
        // legacy code, todo delete by yijie
        return {Status::OK(), _fenceRoot->GetIDirectory()};
    }
    std::string opFenceDir = std::to_string(id);
    auto status = _fenceRoot->GetIDirectory()
                      ->RemoveDirectory(opFenceDir, indexlib::file_system::RemoveOption::MayNonExist())
                      .Status();
    if (!status.IsOK()) {
        return {status, nullptr};
    }
    indexlib::file_system::DirectoryOption directoryOption;
    return _fenceRoot->GetIDirectory()->MakeDirectory(opFenceDir, directoryOption).StatusWith();
}

std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
IndexTaskContext::GetOpFenceRoot(IndexOperationId id, bool useOpFenceDir) const
{
    if (!useOpFenceDir) {
        return {Status::OK(), _fenceRoot->GetIDirectory()};
    }
    auto [status, opFenceRoot] = _fenceRoot->GetIDirectory()->GetDirectory(std::to_string(id)).StatusWith();
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "get op[%ld] fence dir failed", id);
        return {status, nullptr};
    }
    return {Status::OK(), opFenceRoot};
}

const std::shared_ptr<util::Clock>& IndexTaskContext::GetClock() const { return _clock; }

const std::shared_ptr<TabletData>& IndexTaskContext::GetTabletData() const { return _tabletData; }

const std::shared_ptr<config::TabletOptions>& IndexTaskContext::GetTabletOptions() const { return _tabletOptions; }

void IndexTaskContext::SetTabletOptions(const std::shared_ptr<config::TabletOptions>& options)
{
    _tabletOptions = options;
}

const std::shared_ptr<config::TabletSchema>& IndexTaskContext::GetTabletSchema() const { return _tabletSchema; }

const std::shared_ptr<IndexTaskResourceManager>& IndexTaskContext::GetResourceManager() const
{
    return _resourceManager;
}

const std::shared_ptr<indexlib::util::MetricProvider>& IndexTaskContext::GetMetricProvider() const
{
    return _metricProvider;
}

const std::shared_ptr<framework::MetricsManager>& IndexTaskContext::GetMetricsManager() const
{
    return _metricsManager;
}

segmentid_t IndexTaskContext::GetMaxMergedSegmentId() const { return _maxMergedSegmentId; }

versionid_t IndexTaskContext::GetMaxMergedVersionId() const { return _maxMergedVersionId; }

const IndexTaskContext::Parameters& IndexTaskContext::GetAllParameters() const { return _parameters; }

Status IndexTaskContext::GetAllFenceRootInTask(std::vector<std::string>* fenceRoots) const
{
    if (_taskTempWorkRoot.empty()) {
        TABLET_LOG(ERROR, "task temp work root is null");
        return Status::InternalError("task temp work root is null");
    }
    auto [status, isExist] = indexlib::file_system::FslibWrapper::IsExist(_taskTempWorkRoot).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "is exist for [%s] failed", _taskTempWorkRoot.c_str());
    if (!isExist) {
        fenceRoots->clear();
        return Status::OK();
    }
    auto physicalTempDir = indexlib::file_system::Directory::GetPhysicalDirectory(_taskTempWorkRoot);
    assert(physicalTempDir);
    indexlib::file_system::FileList fileList;
    status = physicalTempDir->GetIDirectory()
                 ->ListDir(/*path*/ "", indexlib::file_system::ListOption::Recursive(false), fileList)
                 .Status();
    RETURN_IF_STATUS_ERROR(status, "list dir[%s] failed", _taskTempWorkRoot.c_str());
    fenceRoots->clear();
    for (const auto& name : fileList) {
        if (framework::Fence::IsFenceName(name)) {
            auto fenceRoot = indexlib::util::PathUtil::JoinPath(_taskTempWorkRoot, name);
            fenceRoots->push_back(std::move(fenceRoot));
        }
    }
    return Status::OK();
}

void IndexTaskContext::Log() const
{
    TABLET_LOG(INFO, "maxMergedSegmentId[%d], maxMergedSegmentId[%d]", _maxMergedSegmentId, _maxMergedVersionId);
    for (const auto& [opId, dir] : _finishedOpFenceRoots) {
        TABLET_LOG(INFO, "op[%ld] finished in path[%s]", opId, dir->DebugString().c_str());
    }
}

void IndexTaskContext::SetDesignateTask(const std::string& taskType, const std::string& taskName)
{
    _designateTask = std::pair(taskType, taskName);
}

std::optional<std::pair<std::string, std::string>> IndexTaskContext::GetDesignateTask() const
{
    if (_designateTask) {
        return std::make_optional<std::pair<std::string, std::string>>(_designateTask.value());
    }
    return std::nullopt;
}

bool IndexTaskContext::NeedSwitchIndexPath() const
{
    if (indexlib::util::PathUtil::NormalizePath(_sourceRoot) ==
        indexlib::util::PathUtil::NormalizePath(_indexRoot->GetOutputPath())) {
        return false;
    }
    return true;
}

std::string IndexTaskContext::GetRelocatableFolderWorkRoot(const std::string& fenceRoot, IndexOperationId opId)
{
    return indexlib::util::PathUtil::JoinPath(fenceRoot, RELOCATABLE_PREFIX + std::to_string(opId));
}

void IndexTaskContext::SetGlobalRelocatableFolder(
    const std::shared_ptr<indexlib::file_system::RelocatableFolder>& folder) const
{
    _globalRelocatableFolder = folder;
}

std::shared_ptr<indexlib::file_system::RelocatableFolder> IndexTaskContext::GetGlobalRelocatableFolder() const
{
    return _globalRelocatableFolder;
}

Status IndexTaskContext::RelocateOpFolders(const std::vector<std::pair<IndexOperationId, std::string>>& ops) const
{
    // TODO: support package relocator
    indexlib::file_system::Relocator relocator(_indexRoot->GetOutputPath());
    for (const auto& [opId, opRoot] : ops) {
        std::string sourceDir = GetRelocatableFolderWorkRoot(opRoot, opId);
        RETURN_IF_STATUS_ERROR(relocator.AddSource(sourceDir), "add source [%s] failed", sourceDir.c_str());
        TABLET_LOG(INFO, "relocate op[%ld] from [%s]", opId, opRoot.c_str());
    }
    return relocator.Relocate();
}

Status IndexTaskContext::RelocateAllDependOpFolders() const
{
    std::vector<std::pair<IndexOperationId, std::string>> ops;
    for (const auto& [opId, dir] : _finishedOpFenceRoots) {
        ops.push_back({opId, dir->GetOutputPath()});
    }
    return RelocateOpFolders(ops);
}

void IndexTaskContext::SetFinishedOpFences(
    std::map<IndexOperationId, std::shared_ptr<indexlib::file_system::IDirectory>> opFences)
{
    _finishedOpFenceRoots = std::move(opFences);
}

} // namespace indexlibv2::framework
