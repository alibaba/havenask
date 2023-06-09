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
#include "indexlib/framework/TabletMetrics.h"

#include "autil/TimeUtility.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/framework/VersionMerger.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, TabletMetrics);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

TabletMetrics::TabletMetrics(const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter,
                             const MemoryQuotaController* tabletMemoryQuotaController, const std::string& tabletName,
                             const TabletDumper* tabletDumper, VersionMerger* versionMerger)
    : _metricsReporter(metricsReporter)
    , _buildDocumentMetrics(new BuildDocumentMetrics(tabletName, metricsReporter))
    , _faultManager(new TabletFaultManager())
    , _tabletMemoryQuotaController(tabletMemoryQuotaController)
    , _tabletDumper(tabletDumper)
    , _versionMerger(versionMerger)
    , _tabletName(tabletName)

{
    _lastPrintMetricsTs = autil::TimeUtility::currentTimeInSeconds();
}

void TabletMetrics::RegisterOnlineMetrics()
{
#define REGISTER_TABLET_ONLINE_METRIC(metricName, metricType)                                                          \
    _##metricName = 0L;                                                                                                \
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, metricName, "online/" #metricName, metricType)

    REGISTER_TABLET_ONLINE_METRIC(rtIndexMemoryUseRatio, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(partitionMemoryQuotaUseRatio, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(freeMemoryQuotaRatio, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(partitionIndexSize, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(partitionMemoryUse, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(incIndexMemoryUse, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(rtIndexMemoryUse, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(builtRtIndexMemoryUse, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(buildingSegmentMemoryUse, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(dumpingSegmentMemoryUse, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(partitionMemoryQuotaUse, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(totalMemoryQuotaLimit, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(freeMemoryQuota, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(memoryStatus, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(dumpingSegmentCount, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(tabletPhase, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(partitionReaderVersionCount, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(latestReaderVersionId, kmonitor::GAUGE);
    REGISTER_TABLET_ONLINE_METRIC(oldestReaderVersionId, kmonitor::GAUGE);

#undef REGISTER_TABLET_ONLINE_METRIC
}

void TabletMetrics::RegisterReOpenMetrics()
{
#define REGISTER_TABLET_REOPEN_METRIC(metricName)                                                                      \
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, metricName, "reopen/" #metricName, kmonitor::GAUGE)

    REGISTER_TABLET_REOPEN_METRIC(reopenIncLatency);
    REGISTER_TABLET_REOPEN_METRIC(reopenRealtimeLatency);
    REGISTER_TABLET_REOPEN_METRIC(preloadLatency);
    REGISTER_TABLET_REOPEN_METRIC(finalLoadLatency);
    REGISTER_TABLET_REOPEN_METRIC(incVersionFreshness);
#undef REGISTER_TABLET_REOPEN_METRIC
}

void TabletMetrics::RegisterPartitionMetrics()
{
#define REGISTER_TABLET_PARTITION_METRIC(metricName)                                                                   \
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, metricName, "partition/" #metricName, kmonitor::GAUGE)

    REGISTER_TABLET_PARTITION_METRIC(partitionDocCount);
    REGISTER_TABLET_PARTITION_METRIC(segmentCount);
    REGISTER_TABLET_PARTITION_METRIC(incSegmentCount);
    REGISTER_TABLET_PARTITION_METRIC(tabletFaultCount);

#undef REGISTER_TABLET_PARTITION_METRIC
}

void TabletMetrics::RegisterMetrics()
{
    RegisterOnlineMetrics();
    RegisterReOpenMetrics();
    RegisterPartitionMetrics();
    _buildDocumentMetrics->RegisterMetrics();
}

void TabletMetrics::ReportPartitionMetrics()
{
    INDEXLIB_FM_REPORT_METRIC(partitionDocCount);
    INDEXLIB_FM_REPORT_METRIC(segmentCount);
    INDEXLIB_FM_REPORT_METRIC(incSegmentCount);
    INDEXLIB_FM_REPORT_METRIC(tabletFaultCount);
}

void TabletMetrics::ReportReOpenMetrics() { INDEXLIB_FM_REPORT_METRIC(incVersionFreshness); }

void TabletMetrics::ReportOnlineMetrics()
{
    INDEXLIB_FM_REPORT_METRIC(memoryStatus);
    INDEXLIB_FM_REPORT_METRIC(tabletPhase);
    INDEXLIB_FM_REPORT_METRIC(dumpingSegmentCount);
    INDEXLIB_FM_REPORT_METRIC(partitionIndexSize);
    INDEXLIB_FM_REPORT_METRIC(partitionMemoryUse);
    INDEXLIB_FM_REPORT_METRIC(incIndexMemoryUse);
    INDEXLIB_FM_REPORT_METRIC(rtIndexMemoryUse);
    INDEXLIB_FM_REPORT_METRIC(rtIndexMemoryUseRatio);
    INDEXLIB_FM_REPORT_METRIC(partitionMemoryQuotaUseRatio);
    INDEXLIB_FM_REPORT_METRIC(freeMemoryQuotaRatio);
    INDEXLIB_FM_REPORT_METRIC(builtRtIndexMemoryUse);
    INDEXLIB_FM_REPORT_METRIC(buildingSegmentMemoryUse);
    INDEXLIB_FM_REPORT_METRIC(dumpingSegmentMemoryUse);
    INDEXLIB_FM_REPORT_METRIC(partitionMemoryQuotaUse);
    INDEXLIB_FM_REPORT_METRIC(totalMemoryQuotaLimit);
    INDEXLIB_FM_REPORT_METRIC(freeMemoryQuota);
    INDEXLIB_FM_REPORT_METRIC(partitionReaderVersionCount);
    INDEXLIB_FM_REPORT_METRIC(latestReaderVersionId);
    INDEXLIB_FM_REPORT_METRIC(oldestReaderVersionId);
}

void TabletMetrics::ReportMetrics()
{
    ReportOnlineMetrics();
    ReportPartitionMetrics();
    ReportReOpenMetrics();
}

void TabletMetrics::PrintMetrics(const std::string& tabletName, int64_t printMetricsInterval)
{
    int64_t currentTsInSeconds = autil::TimeUtility::currentTimeInSeconds();
    if (currentTsInSeconds - _lastPrintMetricsTs > printMetricsInterval) {
        std::map<std::string, std::string> infoMap;
        FillMetricsInfo(infoMap);
        TABLET_LOG(INFO, "Print TabletMetrics for table [%s], %s", tabletName.c_str(),
                   autil::legacy::ToJsonString(infoMap, true).c_str());
        _lastPrintMetricsTs = currentTsInSeconds;
    }
}

void TabletMetrics::FillMetricsInfo(std::map<std::string, std::string>& infoMap)
{
    infoMap["partitionIndexSize"] = autil::StringUtil::toString(_partitionIndexSize);
    infoMap["partitionMemoryUse"] = autil::StringUtil::toString(_partitionMemoryUse);
    infoMap["incIndexMemoryUse"] = autil::StringUtil::toString(_incIndexMemoryUse);
    infoMap["rtIndexMemoryUse"] = autil::StringUtil::toString(_rtIndexMemoryUse);
    infoMap["builtRtIndexMemoryUse"] = autil::StringUtil::toString(_builtRtIndexMemoryUse);
    infoMap["buildingSegmentMemoryUse"] = autil::StringUtil::toString(_buildingSegmentMemoryUse);
    infoMap["partitionMemoryQuotaUse"] = autil::StringUtil::toString(_partitionMemoryQuotaUse);
    infoMap["partitionReaderVersionCount"] = autil::StringUtil::toString(_partitionReaderVersionCount);
    infoMap["latestReaderVersionId"] = autil::StringUtil::toString(_latestReaderVersionId);
    infoMap["oldestReaderVersionId"] = autil::StringUtil::toString(_oldestReaderVersionId);
    infoMap["incVersionFreshness"] = autil::StringUtil::toString(_incVersionFreshness);
    if (_faultManager) {
        infoMap["tabletFaults"] = autil::legacy::ToJsonString(*_faultManager);
    }
    if (_versionMerger) {
        _versionMerger->FillMetricsInfo(infoMap);
    }
}

autil::ScopeGuard TabletMetrics::CreateTabletPhaseGuard(const TabletPhase& tabletPhase)
{
    SetTabletPhase(tabletPhase);
    return autil::ScopeGuard(std::bind(&TabletMetrics::SetTabletPhase, this, TabletPhase::UNKNOWN));
}

void TabletMetrics::SetTabletPhase(const TabletPhase& tabletPhase)
{
    std::lock_guard<std::mutex> guard(_mutex);
    _currentTabletPhase = tabletPhase;
    SettabletPhaseValue(static_cast<int64_t>(_currentTabletPhase));
}

void TabletMetrics::UpdateMetrics(const std::shared_ptr<TabletReaderContainer>& tabletReaderContainer,
                                  const std::shared_ptr<TabletData>& tabletData,
                                  const std::shared_ptr<TabletWriter>& tabletWriter, int64_t maxRtMemsize,
                                  const std::shared_ptr<indexlib::file_system::IFileSystem>& fileSystem)
{
    if (_versionMerger) {
        _versionMerger->UpdateMetrics(tabletData.get());
    }

    std::lock_guard<std::mutex> guard(_mutex);
    _tabletMemoryCalculator.reset(new TabletMemoryCalculator(tabletWriter, tabletReaderContainer));
    size_t tabletDocCount = tabletData->GetTabletDocCount();
    size_t incSegmentCount = tabletData->GetIncSegmentCount();
    size_t segmentCount = tabletData->GetSegmentCount();
    int64_t tabletFaultCount = 0;
    if (_faultManager) {
        tabletFaultCount = _faultManager->GetFaultCount();
    }
    SetpartitionDocCountValue(tabletDocCount);
    SetsegmentCountValue(segmentCount);
    SetincSegmentCountValue(incSegmentCount);
    SettabletFaultCountValue(tabletFaultCount);

    // ===> from calculator view
    size_t dumppingSegmentMemsize = _tabletMemoryCalculator->GetDumpingSegmentMemsize();
    size_t rtBuiltSegmentMemsize = _tabletMemoryCalculator->GetRtBuiltSegmentsMemsize();
    size_t buildingSegmentMemsize = _tabletMemoryCalculator->GetBuildingSegmentMemsize();
    size_t rtIndexMemoryUse = rtBuiltSegmentMemsize + buildingSegmentMemsize + dumppingSegmentMemsize;
    SetdumpingSegmentCountValue((int64_t)_tabletDumper->GetDumpQueueSize());
    SetbuildingSegmentMemoryUseValue(buildingSegmentMemsize);
    SetdumpingSegmentMemoryUseValue(dumppingSegmentMemsize);
    SetbuiltRtIndexMemoryUseValue(rtBuiltSegmentMemsize);
    SetincIndexMemoryUseValue(_tabletMemoryCalculator->GetIncIndexMemsize());
    // TODO(xinfei.sxf) refactor "index size" to "memory size"
    // built segment: incBuilt + rtBuilt
    const size_t incIndexSize = GetIncIndexSize(tabletData, fileSystem);
    SetpartitionIndexSizeValue(incIndexSize + rtBuiltSegmentMemsize);
    // rt index: rtBuilt + dumping + building
    SetrtIndexMemoryUseValue(rtIndexMemoryUse);
    // incBuilt + rtBuilt + dumping + building
    SetpartitionMemoryUseValue(_tabletMemoryCalculator->GetIncIndexMemsize() + rtIndexMemoryUse);

    // ====> from controller view
    double rtIndexMemoryRatio = (double)rtIndexMemoryUse / maxRtMemsize;
    SetrtIndexMemoryUseRatioValue(rtIndexMemoryRatio);
    // maybe multi-tablets share the tabletMemoryQuotaController
    int64_t tabletAllocatedQuota = _tabletMemoryQuotaController->GetAllocatedQuota();
    SetpartitionMemoryQuotaUseValue(tabletAllocatedQuota);
    int64_t totalQuota = _tabletMemoryQuotaController->GetTotalQuota();
    SettotalMemoryQuotaLimitValue(totalQuota);
    int64_t freeQuota = _tabletMemoryQuotaController->GetFreeQuota();
    SetfreeMemoryQuotaValue(freeQuota);
    double tabletMemQuotaRatio = 1.0;
    if (tabletAllocatedQuota < totalQuota) {
        tabletMemQuotaRatio = (double)tabletAllocatedQuota / totalQuota;
    }
    SetpartitionMemoryQuotaUseRatioValue(tabletMemQuotaRatio);
    double freeQuotaRatio = 1.0;
    if (freeQuota < totalQuota) {
        freeQuotaRatio = (double)freeQuota / totalQuota;
    }
    SetfreeMemoryQuotaRatioValue(freeQuotaRatio);

    // update ReaderContainer related metrics
    SetpartitionReaderVersionCountValue(tabletReaderContainer->Size());
    SetoldestReaderVersionIdValue(tabletReaderContainer->GetOldestIncVersionId());
    SetlatestReaderVersionIdValue(tabletReaderContainer->GetLatestIncVersionId());

    int64_t latestIncVersionTs = tabletReaderContainer->GetLatestIncVersionTimestamp();
    int64_t currentTs = autil::TimeUtility::currentTime();
    if (latestIncVersionTs != INVALID_TIMESTAMP && currentTs > latestIncVersionTs) {
        SetincVersionFreshnessValue((currentTs - latestIncVersionTs) / 1000);
    }
}

size_t TabletMetrics::GetTabletMemoryUse() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (_tabletMemoryCalculator == nullptr) {
        return 0;
    }
    return _tabletMemoryCalculator->GetIncIndexMemsize();
}

size_t TabletMetrics::GetRtIndexMemsize() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (_tabletMemoryCalculator == nullptr) {
        return 0;
    }

    return _tabletMemoryCalculator->GetRtIndexMemsize();
}

size_t TabletMetrics::GetBuildingSegmentDumpExpandMemsize() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (_tabletMemoryCalculator == nullptr) {
        return 0;
    }

    return _tabletMemoryCalculator->GetBuildingSegmentDumpExpandMemsize();
}

void TabletMetrics::SetMemoryStatus(MemoryStatus memoryStatus)
{
    std::lock_guard<std::mutex> guard(_mutex);
    SetmemoryStatusValue((int64_t)memoryStatus);
}

void TabletMetrics::AddTabletFault(const std::string& fault)
{
    std::lock_guard<std::mutex> guard(_mutex);
    _faultManager->TriggerFailure(fault);
}

size_t TabletMetrics::GetIncIndexSize(const std::shared_ptr<TabletData>& tabletData,
                                      const std::shared_ptr<indexlib::file_system::IFileSystem>& fileSystem) const
{
    if (fileSystem == nullptr) {
        return 0;
    }
    auto lfs = std::dynamic_pointer_cast<indexlib::file_system::LogicalFileSystem>(fileSystem);
    assert(lfs != nullptr);
    return lfs->GetVersionFileSize(tabletData->GetOnDiskVersion().GetVersionId());
}

#undef TABLET_LOG
} // namespace indexlibv2::framework
