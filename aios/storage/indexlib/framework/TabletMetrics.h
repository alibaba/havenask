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
#include <mutex>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "autil/Scope.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/BuildDocumentMetrics.h"
#include "indexlib/framework/IMetrics.h"
#include "indexlib/framework/MetricsWrapper.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletDumper.h"
#include "indexlib/framework/TabletFault.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/framework/TabletMemoryCalculator.h"
#include "indexlib/framework/TabletReaderContainer.h"
#include "indexlib/framework/TabletWriter.h"
#include "indexlib/framework/VersionMerger.h"
#include "kmonitor/client/MetricsReporter.h"

namespace indexlibv2::framework {

enum class TabletPhase : int64_t { UNKNOWN, OPEN, NEW_SEGMENT, NORMAL_REOPEN, FORCE_REOPEN };

enum struct RealtimeIndexMemoryType : int32_t { BUILT = 0, DUMPING = 1, BUILDING = 2, UNKNOWN = -1 };

class TabletMetrics : public IMetrics
{
public:
    TabletMetrics(const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter,
                  const MemoryQuotaController* tabletMemoryQuotaController, const std::string& tabletName,
                  const TabletDumper* tabletDumper, VersionMerger* versionMerger);
    ~TabletMetrics() = default;

public:
    void RegisterMetrics() override;
    void ReportMetrics() override;

public:
    void UpdateMetrics(const std::shared_ptr<TabletReaderContainer>& tabletReaderContainer,
                       const std::shared_ptr<TabletData>& tabletData, const std::shared_ptr<TabletWriter>& tabletWriter,
                       int64_t maxRtMemsize, const std::shared_ptr<indexlib::file_system::IFileSystem>& fileSystem);
    void PrintMetrics(const std::string& partitionName, int64_t printMetricsInterval);
    autil::ScopeGuard CreateTabletPhaseGuard(const TabletPhase& tabletPhase);
    std::shared_ptr<kmonitor::MetricsReporter> GetMetricsReporter() const { return _metricsReporter; }
    BuildDocumentMetrics* GetBuildDocumentMetrics() const { return _buildDocumentMetrics.get(); }

    void AddTabletFault(const std::string& fault);

    // inc + rtBuilt + dumping + building
    size_t GetTabletMemoryUse() const;
    // rtBuilt + dumping + building
    size_t GetRtIndexMemSize() const;
    size_t GetRtIndexMemSize(RealtimeIndexMemoryType memType) const;

    size_t GetFreeQuota() const;
    size_t GetBuildingSegmentDumpExpandMemsize() const;
    size_t GetMaxDumpingSegmentExpandMemsize() const;
    void SetMemoryStatus(MemoryStatus memoryStatus);

    void FillMetricsInfo(std::map<std::string, std::string>& infoMap);

private:
    void SetTabletPhase(const TabletPhase& tabletPhase);
    void RegisterOnlineMetrics();
    void RegisterReOpenMetrics();
    void RegisterPartitionMetrics();
    void ReportOnlineMetrics();
    void RegisterMergeMetrics();
    void ReportPartitionMetrics();
    void ReportReOpenMetrics();
    size_t GetIncIndexSize(const std::shared_ptr<TabletData>& tabletData,
                           const std::shared_ptr<indexlib::file_system::IFileSystem>& fileSystem) const;

private:
    mutable std::mutex _mutex;
    TabletPhase _currentTabletPhase;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    std::unique_ptr<TabletMemoryCalculator> _tabletMemoryCalculator;
    std::unique_ptr<BuildDocumentMetrics> _buildDocumentMetrics;
    std::unique_ptr<TabletFaultManager> _faultManager;
    const MemoryQuotaController* _tabletMemoryQuotaController;
    const TabletDumper* _tabletDumper;
    VersionMerger* _versionMerger;
    std::string _tabletName;
    int64_t _lastPrintMetricsTs; // seconds

    // std::map<std::string, IndexMetrics> _indexMetricsInfo;

    // memory use
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, memoryStatus);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, buildingSegmentMemoryUse);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, dumpingSegmentMemoryUse);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, builtRtIndexMemoryUse);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, rtIndexMemoryUse); // building + dumping + built
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(double, rtIndexMemoryUseRatio);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, incIndexMemoryUse);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, partitionMemoryUse); // incIndex + rtIndex
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, partitionIndexSize); // incIndex + rtBuilt

    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, totalMemoryQuotaLimit);       // total from controller
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, partitionMemoryQuotaUse);     // allocate from controller
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(double, partitionMemoryQuotaUseRatio); // allocate / total
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, freeMemoryQuota);             // free from controller
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(double, freeMemoryQuotaRatio);         // free / total

    // reopen
    INDEXLIB_FM_DECLARE_METRIC(preloadLatency);
    INDEXLIB_FM_DECLARE_METRIC(finalLoadLatency);
    INDEXLIB_FM_DECLARE_METRIC(reopenIncLatency); // total, include preload and finalload
    INDEXLIB_FM_DECLARE_METRIC(reopenRealtimeLatency);

    // partition
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, tabletPhase);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, partitionDocCount);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, segmentCount);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, incSegmentCount);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, tabletFaultCount);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, dumpingSegmentCount);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, partitionReaderVersionCount);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, incVersionFreshness);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, incVersionLatestTaskFreshness);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int32_t, latestReaderVersionId);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int32_t, oldestReaderVersionId);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
