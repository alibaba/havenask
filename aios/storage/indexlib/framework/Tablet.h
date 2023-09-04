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

#include <mutex>
#include <vector>

#include "autil/Log.h"
#include "future_lite/Future.h"
#include "future_lite/NamedTaskScheduler.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/framework/IdGenerator.h"
#include "indexlib/framework/OpenOptions.h"
#include "indexlib/framework/ReadResource.h"
#include "indexlib/framework/SegmentDumper.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletMetrics.h"
#include "indexlib/framework/TabletReaderContainer.h"
#include "indexlib/framework/TabletResource.h"
#include "indexlib/framework/TabletWriter.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionMerger.h"
#include "indexlib/framework/index_task/Constant.h"
#include "indexlib/framework/mem_reclaimer/IIndexMemoryReclaimer.h"

namespace indexlib {
namespace util {
class CounterMap;
class SearchCachePartitionWrapper;
} // namespace util

namespace file_system {
class IFileSystem;
class Directory;
class FileBlockCacheContainer;
class LifecycleTable;
} // namespace file_system
} // namespace indexlib

namespace indexlibv2 {
class MemoryQuotaController;
class MemoryQuotaSynchronizer;

namespace document {
class IDocumentParser;
}

namespace framework {
struct VersionDeployDescription;
class TabletDumper;
class TabletCommitter;
class TabletReaderContainer;
class MetricsManager;
class OnDiskIndexCleaner;
class ITabletMergeController;
class IIndexTaskResourceCreator;
class TabletSchemaManager;
class MemSegmentCreator;

class Tablet : public ITablet
{
public:
    Tablet(const TabletResource& resource);
    ~Tablet() override;

public:
    Status Open(const IndexRoot& indexRoot, const std::shared_ptr<config::ITabletSchema>& schema,
                const std::shared_ptr<config::TabletOptions>& options, const VersionCoord& versionCoord) override;
    Status Reopen(const ReopenOptions& reopenOptions, const VersionCoord& versionCoord) override;
    void Close() override;

    Status Build(const std::shared_ptr<document::IDocumentBatch>& batch) override;
    bool NeedCommit() const override;
    std::pair<Status, VersionMeta> Commit(const CommitOptions& commitOptions) override;
    Status Flush() override;
    Status Seal() override;
    Status CleanIndexFiles(const std::vector<versionid_t>& reservedVersions) override;
    Status CleanUnreferencedDeployFiles(const std::set<std::string>& toKeepFiles) override;
    Status AlterTable(const std::shared_ptr<config::ITabletSchema>& newSchema) override;
    std::pair<Status, versionid_t> ExecuteTask(const Version& sourceVersion, const std::string& taskType,
                                               const std::string& taskName,
                                               const std::map<std::string, std::string>& params) override;
    Status Import(const std::vector<Version>& versions, const ImportOptions& options) override;
    Status ImportExternalFiles(const std::string& bulkloadId, const std::vector<std::string>& externalFiles,
                               const std::shared_ptr<ImportExternalFileOptions>& options, Action action) override;

    std::shared_ptr<ITabletReader> GetTabletReader() const override;
    const TabletInfos* GetTabletInfos() const override;
    std::shared_ptr<config::ITabletSchema> GetTabletSchema() const override;
    std::shared_ptr<config::TabletOptions> GetTabletOptions() const override;
    std::shared_ptr<TabletData> GetTabletData() const;

public:
    using RunAfterCloseFunc = std::function<void()>;
    void SetRunAfterCloseFunction(const RunAfterCloseFunc&& func);
    std::shared_ptr<document::IDocumentParser> CreateDocumentParser();
    MemoryStatus CheckMemoryStatus() const;

protected:
    // for test mock
    virtual std::unique_ptr<ITabletFactory>
    CreateTabletFactory(const std::string& tableType, const std::shared_ptr<config::TabletOptions>& options) const;

private:
    enum class RefreshStrategy {
        NEW_BUILDING_SEGMENT,
        REPLACE_BUILDING_SEGMENT,
        KEEP,
    };

private:
    StatusOr<std::shared_ptr<indexlibv2::framework::VersionDeployDescription>>
    CreateVersionDeployDescription(versionid_t versionId);
    Status CheckDoc(document::IDocumentBatch* batch);
    std::pair<Status, Version>
    MountOnDiskVersion(const VersionCoord& versionCoord,
                       const std::shared_ptr<indexlibv2::framework::VersionDeployDescription>& versionDpDesc);
    Status LoadVersion(const VersionCoord& versionCoord, Version* version, std::string* versionRoot) const;
    Status PrepareResource();
    Status InitIndexDirectory(const IndexRoot& indexRoot);
    Status RecoverIndexInfo(const std::string& indexRoot, const std::string& fenceName);
    Status ReopenNewSegment(const std::shared_ptr<config::ITabletSchema>& schema);
    Status RefreshTabletData(RefreshStrategy strategy, const std::shared_ptr<config::ITabletSchema>& writeSchema);
    Status DoReopenUnsafe(const ReopenOptions& reopenOptions, const VersionCoord& versionCoord);
    void DumpSegmentOverInterval();
    Status FlushUnsafe();
    bool SealSegmentUnsafe();

    Status UpdateControlFlow(const ReopenOptions& reopenOptions);

    Status OpenWriterAndReader(std::shared_ptr<TabletData> tabletData, const framework::OpenOptions& openOptions,
                               const std::shared_ptr<indexlibv2::framework::VersionDeployDescription>& versionDpDesc);
    bool StartIntervalTask();
    void ReportMetrics(const std::shared_ptr<TabletWriter>& tabletWriter);
    Locator GetMemSegmentLocator();

    Status PrepareIndexRoot(const std::shared_ptr<config::ITabletSchema>& schema);
    bool IsEmptyDir(std::shared_ptr<indexlib::file_system::Directory> directory,
                    const std::shared_ptr<config::ITabletSchema>& schema);
    Status InitBasicIndexInfo(const std::shared_ptr<config::ITabletSchema>& schema);
    std::shared_ptr<TabletWriter> GetTabletWriter() const;
    void CloseWriterUnsafe();
    std::shared_ptr<indexlib::file_system::Directory> GetRootDirectory() const;
    void PrepareTabletMetrics(const std::shared_ptr<TabletWriter>& tabletWriter);

    BuildResource GenerateBuildResource(const std::string& counterPrefix);
    ReadResource GenerateReadResource() const;
    void UpdateDocCount();

    Status CheckAlterTableCompatible(const std::shared_ptr<TabletData>& tabletData,
                                     const config::ITabletSchema& oldSchema, const config::ITabletSchema& newSchema);
    Status FinalizeTabletData(TabletData* tabletData, const std::shared_ptr<config::ITabletSchema>& writeSchema);
    std::shared_ptr<config::ITabletSchema> GetReadSchema(const std::shared_ptr<TabletData>& tabletData);
    Status DoAlterTable(const std::shared_ptr<config::ITabletSchema>& newSchema);

    Version MakeEmptyVersion(schemaid_t schemaId) const;
    void SetTabletData(std::shared_ptr<TabletData> tabletData);
    void MemoryReclaim();
    int64_t GetSuggestBuildingSegmentMemoryUse();
    std::shared_ptr<indexlib::file_system::LifecycleTable> GetLifecycleTableFromVersion(const Version& version);
    Status RenewFenceLease(bool createIfNotExist);
    std::pair<Status, Version> RecoverLatestVersion(const std::string& path) const;
    Status PrepareEmptyTabletData(const std::shared_ptr<config::ITabletSchema>& tabletSchema);
    bool NeedRecoverFromLocal() const;
    std::string GetDiffSegmentDebugString(const Version& targetVersion,
                                          const std::shared_ptr<TabletData>& currentTabletData) const;
    void HandleIndexTask(const std::string& taskType, const std::string& taskName,
                         const std::map<std::string, std::string>& params, Action action, const std::string& comment);

private:
    mutable std::mutex _dataMutex;
    mutable std::mutex _readerMutex;
    mutable std::mutex _tabletDataMutex;

    std::mutex _reopenMutex;
    std::mutex _cleanerMutex;

    std::unique_ptr<TabletInfos> _tabletInfos;
    std::atomic<bool> _needSeek; // TODO: rename
    std::optional<Locator> _sealedSourceLocator;
    std::shared_ptr<config::TabletOptions> _tabletOptions;
    framework::OpenOptions _openOptions;
    std::shared_ptr<MemoryQuotaController> _tabletMemoryQuotaController;
    std::unique_ptr<MemoryQuotaSynchronizer> _buildMemoryQuotaSynchronizer;
    std::shared_ptr<indexlib::file_system::FileBlockCacheContainer> _fileBlockCacheContainer;
    std::shared_ptr<indexlib::util::SearchCachePartitionWrapper> _searchCache;
    Fence _fence;

    std::shared_ptr<ITabletFactory> _tabletFactory;
    std::shared_ptr<TabletData> _tabletData;
    std::shared_ptr<ITabletReader> _tabletReader;
    std::shared_ptr<TabletWriter> _tabletWriter;
    std::unique_ptr<TabletCommitter> _tabletCommitter;
    std::unique_ptr<TabletDumper> _tabletDumper;
    std::unique_ptr<TabletSchemaManager> _tabletSchemaMgr;
    std::unique_ptr<MemSegmentCreator> _memSegmentCreator;

    std::shared_ptr<TabletReaderContainer> _tabletReaderContainer;
    std::unique_ptr<future_lite::NamedTaskScheduler> _taskScheduler;

    std::shared_ptr<TabletMetrics> _tabletMetrics;
    std::unique_ptr<MetricsManager> _metricsManager;

    std::shared_ptr<ITabletMergeController> _mergeController;
    std::shared_ptr<IdGenerator> _idGenerator;
    std::shared_ptr<VersionMerger> _versionMerger;
    std::shared_ptr<framework::IIndexMemoryReclaimer> _memReclaimer;

    // TODO: Build thread pools should be passed in from outside(suez). The thread pools here exist only because
    // suez does not implement global thread pool control yet. REMOVE.
    std::shared_ptr<autil::ThreadPool> _consistentModeBuildThreadPool = nullptr;
    std::shared_ptr<autil::ThreadPool> _inconsistentModeBuildThreadPool = nullptr;

    RunAfterCloseFunc _closeRunFunc;
    volatile bool _isClosed = false;
    volatile bool _enableMemReclaimer = true;

private:
    friend class TabletTestAgent;
    AUTIL_LOG_DECLARE();
};

} // namespace framework
} // namespace indexlibv2
