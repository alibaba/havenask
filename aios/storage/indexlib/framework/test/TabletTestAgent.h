#pragma once

#include "autil/NoCopyable.h"
#include "indexlib/framework/Tablet.h"

namespace indexlib::file_system {
class FileSystemMetrics;
class Directory;
} // namespace indexlib::file_system

namespace indexlibv2::framework {

class TabletTestAgent : private autil::NoCopyable
{
public:
    TabletTestAgent(Tablet* tablet);
    TabletTestAgent(const std::shared_ptr<Tablet>& tablet);
    TabletTestAgent(const std::unique_ptr<Tablet>& tablet);
    ~TabletTestAgent();

public:
    TabletWriter* TEST_GetTabletWriter() const;
    ITabletFactory* TEST_GetTabletFactory() const;
    std::shared_ptr<TabletData> TEST_GetTabletData() const;
    TabletInfos* TEST_GetTabletInfos() const;
    std::shared_ptr<indexlib::file_system::IFileSystem> TEST_GetFileSystem() const;
    const Fence& TEST_GetFence() const;
    TabletReaderContainer* TEST_GetTabletReaderContainer() const;
    future_lite::NamedTaskScheduler* TEST_GetTaskScheduler() const;
    TabletMetrics* TEST_GetTabletMetrics() const;
    TabletDumper* TEST_GetTabletDumper() const;
    void TEST_ReportMetrics();
    MemoryQuotaController* TEST_GetTabletMemoryQuotaController() const;
    MemoryQuotaSynchronizer* TEST_GetBuildMemoryQuotaSynchronizer() const;
    MetricsManager* TEST_GetMetricsManager() const;
    Status TEST_TriggerDump();
    Version TEST_GetVersion(const VersionCoord& versionCoord) const;
    Version TEST_GetVersion(const VersionMeta& meta) const;
    void TEST_DeleteTask(const std::string& taskName);
    std::shared_ptr<ITabletMergeController> TEST_GetMergeController() const;
    ReadResource TEST_GetReadResource() const;
    bool TEST_SealSegment() const;
    indexlib::file_system::FileSystemMetrics TEST_GetFileSystemMetrics() const;
    std::shared_ptr<indexlib::file_system::Directory> TEST_GetRootDirectory() const;

private:
    Tablet* _tablet;
};

} // namespace indexlibv2::framework
