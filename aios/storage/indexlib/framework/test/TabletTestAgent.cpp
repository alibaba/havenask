#include "indexlib/framework/test/TabletTestAgent.h"

#include "indexlib/config/BuildConfig.h"
#include "indexlib/file_system/FileSystemMetrics.h"
#include "indexlib/file_system/IFileSystem.h"

namespace indexlibv2::framework {

TabletTestAgent::TabletTestAgent(const std::shared_ptr<Tablet>& tablet) : TabletTestAgent(tablet.get()) {}
TabletTestAgent::TabletTestAgent(const std::unique_ptr<Tablet>& tablet) : TabletTestAgent(tablet.get()) {}

TabletTestAgent::TabletTestAgent(Tablet* tablet) : _tablet(tablet) { assert(tablet); }

TabletTestAgent::~TabletTestAgent() {}

TabletWriter* TabletTestAgent::TEST_GetTabletWriter() const { return _tablet->_tabletWriter.get(); }
ITabletFactory* TabletTestAgent::TEST_GetTabletFactory() const { return _tablet->_tabletFactory.get(); }
std::shared_ptr<TabletData> TabletTestAgent::TEST_GetTabletData() const { return _tablet->_tabletData; }
TabletInfos* TabletTestAgent::TEST_GetTabletInfos() const { return _tablet->_tabletInfos.get(); }
const Fence& TabletTestAgent::TEST_GetFence() const { return _tablet->_fence; }
std::shared_ptr<indexlib::file_system::IFileSystem> TabletTestAgent::TEST_GetFileSystem() const
{
    return _tablet->_fence.GetFileSystem();
}
TabletReaderContainer* TabletTestAgent::TEST_GetTabletReaderContainer() const
{
    return _tablet->_tabletReaderContainer.get();
}
future_lite::NamedTaskScheduler* TabletTestAgent::TEST_GetTaskScheduler() const
{
    return _tablet->_taskScheduler.get();
}
TabletMetrics* TabletTestAgent::TEST_GetTabletMetrics() const { return _tablet->_tabletMetrics.get(); }
TabletDumper* TabletTestAgent::TEST_GetTabletDumper() const { return _tablet->_tabletDumper.get(); }
void TabletTestAgent::TEST_ReportMetrics() { _tablet->ReportMetrics(_tablet->GetTabletWriter()); }
MemoryQuotaController* TabletTestAgent::TEST_GetTabletMemoryQuotaController() const
{
    return _tablet->_tabletMemoryQuotaController.get();
}
MemoryQuotaSynchronizer* TabletTestAgent::TEST_GetBuildMemoryQuotaSynchronizer() const
{
    return _tablet->_buildMemoryQuotaSynchronizer.get();
}
MetricsManager* TabletTestAgent::TEST_GetMetricsManager() const { return _tablet->_metricsManager.get(); }
Status TabletTestAgent::TEST_TriggerDump()
{
    return _tablet->_tabletDumper->Dump(_tablet->_tabletOptions->GetBuildConfig().GetDumpThreadCount());
}
Version TabletTestAgent::TEST_GetVersion(const VersionCoord& versionCoord) const
{
    Version version;
    std::string versionRoot;
    [[maybe_unused]] auto status = _tablet->LoadVersion(versionCoord, &version, &versionRoot);
    assert(status.IsOK());
    return version;
}
Version TabletTestAgent::TEST_GetVersion(const VersionMeta& meta) const
{
    return TEST_GetVersion(VersionCoord(meta.GetVersionId(), meta.GetFenceName()));
}
void TabletTestAgent::TEST_DeleteTask(const std::string& taskName)
{
    _tablet->_taskScheduler->DeleteTask(/*taskName*/ taskName);
}

std::shared_ptr<ITabletMergeController> TabletTestAgent::TEST_GetMergeController() const
{
    return _tablet->_mergeController;
}
ReadResource TabletTestAgent::TEST_GetReadResource() const { return _tablet->GenerateReadResource(); }

bool TabletTestAgent::TEST_SealSegment() const { return _tablet->SealSegmentUnsafe(); }
indexlib::file_system::FileSystemMetrics TabletTestAgent::TEST_GetFileSystemMetrics() const
{
    return _tablet->_fence.GetFileSystem()->GetFileSystemMetrics();
}

std::shared_ptr<indexlib::file_system::Directory> TabletTestAgent::TEST_GetRootDirectory() const
{
    return _tablet->GetRootDirectory();
}

} // namespace indexlibv2::framework
