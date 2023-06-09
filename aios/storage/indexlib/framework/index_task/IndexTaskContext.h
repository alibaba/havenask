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
#include <map>
#include <memory>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/index_task/BasicDefs.h"
#include "indexlib/util/Clock.h"

namespace indexlib { namespace file_system {
class Directory;
class IDirectory;
class RelocatableFolder;
}} // namespace indexlib::file_system

namespace indexlib { namespace util {
class MetricProvider;
}} // namespace indexlib::util

namespace indexlibv2::util {
class Clock;
}

namespace indexlibv2::config {
class TabletOptions;
class TabletSchema;
} // namespace indexlibv2::config

namespace indexlibv2::framework {

class IndexTaskRoots;
class TabletData;
class IndexTaskResourceManager;
class IIndexOperationCreator;
class MetricsManager;

class IndexTaskContext
{
public:
    using Parameters = std::map<std::string, std::string>;

protected:
    // create IndexTaskContext from Creator
    IndexTaskContext() = default;

public:
    virtual ~IndexTaskContext() = default;

    const std::shared_ptr<indexlib::file_system::Directory>& GetIndexRoot() const;
    // TODO: delete useOpFenceDir, current use it for compatible with old legacy code
    std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>> GetOpFenceRoot(IndexOperationId id,
                                                                                         bool useOpFenceDir) const;
    std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>> CreateOpFenceRoot(IndexOperationId id,
                                                                                            bool useOpFenceDir) const;

    const std::shared_ptr<TabletData>& GetTabletData() const;

    void SetTabletOptions(const std::shared_ptr<config::TabletOptions>& options);
    const std::shared_ptr<config::TabletOptions>& GetTabletOptions() const;
    const std::shared_ptr<config::TabletSchema>& GetTabletSchema() const;
    std::shared_ptr<config::TabletSchema> GetTabletSchema(schemaid_t schemaId) const;
    const std::shared_ptr<IndexTaskResourceManager>& GetResourceManager() const;
    const std::shared_ptr<indexlib::util::MetricProvider>& GetMetricProvider() const;
    const std::shared_ptr<framework::MetricsManager>& GetMetricsManager() const;
    const std::shared_ptr<util::Clock>& GetClock() const;
    segmentid_t GetMaxMergedSegmentId() const;
    versionid_t GetMaxMergedVersionId() const;
    const std::string& GetTaskEpochId() const { return _taskEpochId; }
    template <typename T>
    void AddParameter(std::string key, T value);
    template <typename T>
    bool GetParameter(const std::string& key, T& value) const;
    const Parameters& GetAllParameters() const;
    // virtual for test fake
    virtual Status GetAllFenceRootInTask(std::vector<std::string>* fenceResults) const;
    virtual std::shared_ptr<indexlib::file_system::IDirectory> GetDependOperationFenceRoot(IndexOperationId id) const;

    void SetResult(std::string result) const { _result = std::move(result); }
    const std::string& GetResult() const { return _result; }
    const std::string& GetTaskTempWorkRoot() { return _taskTempWorkRoot; }
    const std::string& GetSourceRoot() const { return _sourceRoot; }
    bool NeedSwitchIndexPath() const;
    void Log() const;
    void SetDesignateTask(const std::string& taskType, const std::string& taskName);
    std::optional<std::pair<std::string, std::string>> GetDesignateTask() const;

    void SetFinishedOpFences(std::map<IndexOperationId, std::shared_ptr<indexlib::file_system::IDirectory>> opFences);

    void SetGlobalRelocatableFolder(const std::shared_ptr<indexlib::file_system::RelocatableFolder>& folder) const;
    std::shared_ptr<indexlib::file_system::RelocatableFolder> GetGlobalRelocatableFolder() const;
    Status RelocateOpFolders(const std::vector<std::pair<IndexOperationId, std::string>>& ops) const;
    Status RelocateAllDependOpFolders() const;

    static std::string GetRelocatableFolderWorkRoot(const std::string& fenceRoot, IndexOperationId opId);

public:
    void TEST_SetResourceManager(const std::shared_ptr<IndexTaskResourceManager>& manager)
    {
        _resourceManager = manager;
    }
    void TEST_SetTabletSchema(std::shared_ptr<config::TabletSchema> tabletSchema) { _tabletSchema = tabletSchema; }
    void TEST_SetSpecifyMaxMergedSegId(segmentid_t maxMergeSegId) { _maxMergedSegmentId = maxMergeSegId; }
    void TEST_SetSpecifyMaxMergedVersionId(versionid_t maxVersionId) { _maxMergedVersionId = maxVersionId; }
    void TEST_SetTabletData(const std::shared_ptr<framework::TabletData>& tabletData) { _tabletData = tabletData; }

    void TEST_SetFenceRoot(const std::shared_ptr<indexlib::file_system::Directory>& fenceRoot)
    {
        _fenceRoot = fenceRoot;
    }

protected:
    std::string _taskEpochId;
    std::string _taskTempWorkRoot;
    std::optional<std::pair<std::string, std::string>> _designateTask; /* first: taskType, second: taskName */
    std::shared_ptr<indexlib::file_system::Directory> _indexRoot;
    std::shared_ptr<indexlib::file_system::Directory> _fenceRoot;
    std::shared_ptr<TabletData> _tabletData;
    std::shared_ptr<config::TabletOptions> _tabletOptions;
    std::shared_ptr<config::TabletSchema> _tabletSchema;
    std::shared_ptr<IndexTaskResourceManager> _resourceManager;
    std::shared_ptr<indexlib::util::MetricProvider> _metricProvider;
    std::shared_ptr<framework::MetricsManager> _metricsManager;
    std::shared_ptr<util::Clock> _clock = std::make_shared<indexlibv2::util::Clock>();
    segmentid_t _maxMergedSegmentId = INVALID_SEGMENTID;
    versionid_t _maxMergedVersionId = INVALID_VERSIONID;
    Parameters _parameters;
    std::map<IndexOperationId, std::shared_ptr<indexlib::file_system::IDirectory>> _finishedOpFenceRoots;

    mutable std::shared_ptr<indexlib::file_system::RelocatableFolder> _globalRelocatableFolder;
    mutable std::string _result;
    std::string _sourceRoot;

private:
    friend class IndexTaskContextCreator;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
inline bool IndexTaskContext::GetParameter(const std::string& key, T& value) const
{
    auto iter = _parameters.find(key);
    if (iter == _parameters.end()) {
        return false;
    }
    return autil::StringUtil::fromString(iter->second, value);
}

template <typename T>
inline void IndexTaskContext::AddParameter(std::string key, T value)
{
    _parameters[key] = autil::StringUtil::toString(value);
}

} // namespace indexlibv2::framework
