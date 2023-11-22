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
#include "autil/NoCopyable.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/BasicDefs.h"
#include "indexlib/framework/index_task/IIndexTaskResourceCreator.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"

namespace indexlib::file_system {
class Directory;
class FileBlockCacheContainer;
} // namespace indexlib::file_system

namespace indexlib::util {
class MetricProvider;
}

namespace indexlibv2::config {
class TabletOptions;
class ITabletSchema;
} // namespace indexlibv2::config

namespace indexlibv2::framework {

class IIndexOperationCreator;

class IndexTaskContextCreator
{
public:
    IndexTaskContextCreator() = default;
    IndexTaskContextCreator(const std::shared_ptr<framework::MetricsManager>& metricsManager);
    ~IndexTaskContextCreator() = default;

public:
    IndexTaskContextCreator& SetTabletName(const std::string& tabletName);
    IndexTaskContextCreator& SetTaskTraceId(const std::string& taskTraceId);
    IndexTaskContextCreator& SetTaskEpochId(const std::string& taskEpochId);
    IndexTaskContextCreator& SetExecuteEpochId(const std::string& executeEpochId);
    IndexTaskContextCreator& SetTabletFactory(framework::ITabletFactory* factory);
    IndexTaskContextCreator& SetMemoryQuotaController(const std::shared_ptr<MemoryQuotaController>& controller);
    IndexTaskContextCreator& SetTaskParams(const IndexTaskContext::Parameters& params);
    IndexTaskContextCreator& AddSourceVersion(const std::string& root, versionid_t srcVersionId);
    IndexTaskContextCreator& SetDestDirectory(const std::string& destRoot);
    IndexTaskContextCreator& SetTabletOptions(const std::shared_ptr<config::TabletOptions>& options);
    IndexTaskContextCreator& SetTabletSchema(const std::shared_ptr<config::ITabletSchema>& schema);
    IndexTaskContextCreator& SetMetricProvider(const std::shared_ptr<indexlib::util::MetricProvider>& provider);
    IndexTaskContextCreator& SetFinishedOpExecuteEpochIds(const std::map<IndexOperationId, std::string>& epochIds,
                                                          bool useOpFenceDir);
    IndexTaskContextCreator& SetFileBlockCacheContainer(
        const std::shared_ptr<indexlib::file_system::FileBlockCacheContainer>& fileBlockCacheContainer);
    IndexTaskContextCreator& SetClock(const std::shared_ptr<util::Clock>& clock);
    IndexTaskContextCreator& SetTaskType(const std::string& taskType)
    {
        _context._taskType = taskType;
        return *this;
    }
    IndexTaskContextCreator& SetTaskName(const std::string& taskName)
    {
        _context._taskName = taskName;
        return *this;
    }
    IndexTaskContextCreator& SetDesignateTask(const std::string& taskType, const std::string& taskName);
    IndexTaskContextCreator& AddParameter(const std::string& key, const std::string& value);
    // NOT thread safe
    std::unique_ptr<IndexTaskContext> CreateContext();

private:
    using DirectoryPtr = std::shared_ptr<indexlib::file_system::Directory>;
    using TabletDataPtr = std::shared_ptr<framework::TabletData>;

private:
    Status LoadSrc(size_t srcIndex, bool useVirtualSegmentId, std::vector<std::shared_ptr<Segment>>* segments,
                   std::vector<Version>* srcVersions, Version* globalVersion) const;
    std::pair<Status, std::string> GetVersionRoot(size_t srcIndex) const;
    Status PrepareInput(TabletDataPtr* tabletData, std::string& sourceRoot) const;
    Status PrepareOutput(DirectoryPtr* indexRoot, DirectoryPtr* fenceRoot) const;
    Status UpdateMaxMergedId(const std::string& root);
    Status InitResourceManager();
    std::string GetFenceRoot(const std::string& epochId) const;
    std::pair<Status, std::shared_ptr<config::ITabletSchema>>
    LoadSchema(const std::shared_ptr<indexlib::file_system::IDirectory>& directory, schemaid_t schemaId) const;
    Status FillSchemaGroup(const std::string& root, const framework::Version& version,
                           framework::ResourceMap* resourceMap) const;

    struct SrcInfo {
        std::string root;
        versionid_t versionId;
    };

private:
    std::string _tabletName;
    std::string _executeEpochId;
    std::optional<std::pair<std::string, std::string>> _designateTask; /* first: taskType, second: taskName */
    framework::ITabletFactory* _tabletFactory;
    std::shared_ptr<MemoryQuotaController> _memoryQuotaController;
    std::vector<SrcInfo> _srcInfos;
    std::string _destRoot;
    std::shared_ptr<indexlib::file_system::FileBlockCacheContainer> _fileBlockCacheContainer;
    std::string _taskType;
    std::string _taskName;
    std::shared_ptr<framework::MetricsManager> _metricsManager;
    IndexTaskContext _context;

    bool _basicInited = false;

    mutable std::map<schemaid_t, std::shared_ptr<config::ITabletSchema>> _tabletSchemaCache;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
