#pragma once

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/TabletResource.h"
#include "indexlib/framework/index_task/Constant.h"

namespace future_lite {
class NamedTaskScheduler;
}

namespace indexlib { namespace file_system {
class Directory;
}} // namespace indexlib::file_system

namespace indexlibv2 {
namespace framework {
class Tablet;
class ITabletMergeController;
class IndexTaskResource;
class CommitOptions;
class IdGenerator;
class Locator;
} // namespace framework

namespace table {
enum MakeTabletOptionsFlag {
    TOF_LEADER = 0x1,
    TOF_FLUSH_LOCAL = 0x2,
    TOF_FLUSH_REMOTE = 0x4,
    TOF_DISABLE_BACKGROUD_TASK = 0x8
};

class TableTestHelper : private autil::NoCopyable
{
public:
    TableTestHelper(bool autoCleanIndex = false, bool needDeploy = false);
    virtual ~TableTestHelper();

public:
    struct MergeOption {
        bool mergeAutoReload;
        bool isOptimizeMerge;
        std::string taskName;
        std::string mergeStrategy;
        std::string mergeStrategyParam;
        MergeOption() : mergeAutoReload(true), isOptimizeMerge(false), taskName("") {}
        MergeOption(const std::string& _taskName) : mergeAutoReload(true), isOptimizeMerge(false), taskName(_taskName)
        {
        }
        static MergeOption OptimizeMergeOption()
        {
            MergeOption option;
            option.isOptimizeMerge = true;
            return option;
        }
        static MergeOption MergeAutoReadOption(bool _mergeAutoReload)
        {
            MergeOption option;
            option.mergeAutoReload = _mergeAutoReload;
            return option;
        }
    };

    struct StepInfo {
        int64_t stepNum = 0;
        versionid_t stepResultVersion = INVALID_VERSIONID;
        bool isFinished = false;
        std::optional<int64_t> stepMax;
        // 用于异常测试，指定相同的目录和taskPlan
        // 用于验证任务的可重入能力
        std::string specifyEpochId;
        std::string taskPlan;
        std::string msg;
        // TODO: delete
        segmentid_t specifyMaxMergedSegId = -2;
        versionid_t specifyMaxMergedVersionId = -2;
    };

    enum AlterTableStep {
        CHANGE_SCHEMA = 1,
        COMMIT_SCHEMA_VERSION = 2,
        REOPEN_SCHEMA_VERSION = 3,
        EXECUTE_ALTER_TABLE = 4,
        COMMIT_RESULT = 5,
        REOPEN_RESULT = 6,
    };

public:
    Status Open(const framework::IndexRoot& indexRoot, std::shared_ptr<config::ITabletSchema> tabletSchema,
                std::shared_ptr<config::TabletOptions> tabletOptions,
                framework::VersionCoord versionCoord = framework::VersionCoord(INVALID_VERSIONID));
    Status Reopen(const framework::VersionCoord& versionCoord);
    Status Reopen(const framework::ReopenOptions& reopenOptions, const framework::VersionCoord& versionCoord);
    Status Build(std::string docs, bool oneBatch = false);
    Status Build(const std::string& docStr, const framework::Locator& locator);
    Status BuildSegment(std::string docs, bool oneBatch = false, bool autoDumpAndReload = true);
    Status Flush();
    Status Seal();
    Status TriggerDump();
    bool NeedCommit() const;
    Status Import(std::vector<framework::Version> versions,
                  framework::ImportOptions importOptions = framework::ImportOptions());

    Status ImportExternalFiles(const std::string& bulkloadId, const std::vector<std::string>& externalFiles,
                               const std::shared_ptr<framework::ImportExternalFileOptions>& options,
                               indexlibv2::framework::Action action = indexlibv2::framework::Action::ADD,
                               bool commitAndReopen = true);

    std::pair<Status, versionid_t> Commit();
    std::pair<Status, versionid_t> Commit(const framework::CommitOptions& commitOptions);
    Status Merge(MergeOption option = MergeOption());
    std::pair<Status, framework::VersionCoord> OfflineMerge(const framework::VersionCoord& versionCoord,
                                                            MergeOption option);
    Status Merge(bool mergeAutoReload, StepInfo* step);
    // execute merge by step
    std::pair<Status, StepInfo> StepMerge(StepInfo step, const MergeOption& option);
    void Close();
    bool Deploy(const framework::IndexRoot& indexRoot, const config::TabletOptions* tabletOptions,
                versionid_t baseVersionId, versionid_t targetVersionId);

    bool Query(std::string indexType, std::string indexName, std::string queryStr, std::string expectValue);
    Status AlterTable(std::shared_ptr<config::ITabletSchema> tabletSchema,
                      const std::vector<std::shared_ptr<framework::IndexTaskResource>>& extendResources = {},
                      bool autoCommit = true, StepInfo* step = NULL);

    std::pair<Status, StepInfo>
    StepAlterTable(StepInfo step, std::shared_ptr<config::ITabletSchema> tabletSchema,
                   std::vector<std::shared_ptr<framework::IndexTaskResource>> extendResources = {});

    std::shared_ptr<framework::ITablet> GetITablet() const;
    std::shared_ptr<framework::ITabletReader> GetTabletReader() const;
    const std::shared_ptr<framework::Tablet>& GetTablet() const;
    std::shared_ptr<indexlibv2::config::ITabletSchema> GetSchema() const;
    const framework::Version& GetCurrentVersion() const;
    future_lite::NamedTaskScheduler* GetTaskScheduler() const;
    framework::TabletResource* GetTabletResource();
    std::shared_ptr<indexlib::file_system::Directory> GetRootDirectory() const;

public:
    // optional, need execute before Open
    TableTestHelper& SetExecutor(future_lite::Executor* dumpExecutor, future_lite::TaskScheduler* taskScheduler);
    TableTestHelper& SetMemoryQuotaController(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
                                              const std::shared_ptr<MemoryQuotaController>& buildMemoryQuotaController);
    TableTestHelper& SetFileBlockCacheContainer(
        const std::shared_ptr<indexlib::file_system::FileBlockCacheContainer>& fileBlockCacheContainer);
    TableTestHelper& SetMergeController(const std::shared_ptr<framework::ITabletMergeController>& mergeController);
    TableTestHelper& SetIdGenerator(const std::shared_ptr<framework::IdGenerator>& idGenerator);
    TableTestHelper& SetSearchCache(const std::shared_ptr<indexlib::util::SearchCache>& searchCache);

public:
    static std::shared_ptr<config::TabletOptions> MakeOnlineTabletOptions(int32_t makeTabletOptionsFlags = 0);
    static std::shared_ptr<config::TabletOptions> MakeOfflineTabletOptions(int32_t makeTabletOptionsFlags = 0);
    static framework::ReopenOptions GetReopenOptions(framework::OpenOptions::BuildMode buildMode);

protected:
    virtual void AfterOpen() {}
    virtual void
    AddAlterTabletResourceIfNeed(std::vector<std::shared_ptr<framework::IndexTaskResource>>& extendResources,
                                 const std::shared_ptr<config::ITabletSchema>& schema)
    {
    }
    virtual Status DoBuild(std::string docs, bool oneBatch = false) = 0;
    virtual Status DoBuild(const std::string& docStr, const framework::Locator& locator) = 0;
    virtual bool DoQuery(std::string indexType, std::string indexName, std::string queryStr,
                         std::string expectValue) = 0;
    virtual std::shared_ptr<TableTestHelper> CreateMergeHelper() = 0;

private:
    Status DoMerge(const MergeOption& option, StepInfo* step);
    Status ExecuteMerge(const MergeOption& mergeOption);

protected:
    framework::IndexRoot _indexRoot;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::TabletOptions> _options;
    bool _autoCleanIndex = true;
    bool _needDeploy = true;

private:
    Status InitTabletResource();
    virtual Status InitTypedTabletResource();
    bool WriteDpDoneFile(const framework::IndexRoot& indexRoot, versionid_t versionId,
                         const std::string& content = "{}");

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace table
} // namespace indexlibv2
