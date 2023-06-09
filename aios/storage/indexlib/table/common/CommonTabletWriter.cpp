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
#include "indexlib/table/common/CommonTabletWriter.h"

#include <unordered_set>

#include "autil/TimeUtility.h"
#include "autil/UnitUtil.h"
#include "future_lite/Future.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/framework/BuildDocumentMetrics.h"
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/SegmentDumpItem.h"
#include "indexlib/framework/SegmentDumper.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/table/common/TabletWriterResourceCalculator.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"

using namespace indexlibv2::framework;
using namespace indexlib::config;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, CommonTabletWriter);

#define TABLET_LOG(level, format, args...)                                                                             \
    assert(_tabletData);                                                                                               \
    AUTIL_LOG(level, "[%s] [%p]" format, _tabletData->GetTabletName().c_str(), this, ##args)

const float CommonTabletWriter::MIN_DUMP_MEM_LIMIT_RATIO = 0.2;

CommonTabletWriter::CommonTabletWriter(const std::shared_ptr<config::TabletSchema>& schema,
                                       const config::TabletOptions* options)
    : _schema(schema)
    , _options(options)
    , _startBuildTime(-1)
{
}

Status CommonTabletWriter::Open(const std::shared_ptr<framework::TabletData>& tabletData,
                                const BuildResource& buildResource, const framework::OpenOptions& openOptions)
{
    if (openOptions.GetUpdateControlFlowOnly()) {
        return Status::OK();
    }
    if (!tabletData) {
        return Status::InternalError("open failed, because tabletData is nullptr");
    }
    _tabletData = tabletData;
    _buildResource = buildResource;
    auto slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILDING);
    assert(slice.begin() != slice.end());
    _buildingSegment = std::dynamic_pointer_cast<MemSegment>(*slice.begin());
    RegisterMetrics();
    InitCounters(buildResource.counterMap);
    return DoOpen(tabletData, buildResource, openOptions);
}

void CommonTabletWriter::RegisterMetrics()
{
    REGISTER_BUILD_METRIC(buildMemoryUse);
    REGISTER_BUILD_METRIC(buildingSegmentMemoryUse);
    REGISTER_BUILD_METRIC(estimateMaxMemoryUseOfCurrentSegment);
    REGISTER_BUILD_METRIC(segmentBuildingTime);

    RegisterTableSepecificMetrics();
}

indexlib::util::AccumulativeCounterPtr
CommonTabletWriter::InitCounter(const std::string& nodePrefix, const std::string& counterName,
                                const std::shared_ptr<indexlib::util::CounterMap>& counterMap)
{
    std::string counterPath = nodePrefix + "." + counterName;
    auto accCounter = counterMap->GetAccCounter(counterPath);
    if (!accCounter) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "init counter[%s] failed", counterPath.c_str());
    }
    return accCounter;
}

void CommonTabletWriter::InitCounters(const std::shared_ptr<indexlib::util::CounterMap>& counterMap)
{
    if (!counterMap) {
        TABLET_LOG(ERROR, "init counters failed due to NULL counterMap");
        return;
    }

    std::string nodePrefix = _options->IsOnline() ? "online.build" : "offline.build";
    mAddDocCountCounter = InitCounter(nodePrefix, "addDocCount", counterMap);
}

void CommonTabletWriter::ReportMetrics()
{
    kmonitor::MetricsTags tags;
    tags.AddTag("schemaName", _schema->GetTableName());
    tags.AddTag("segmentId", autil::StringUtil::toString(_buildingSegment->GetSegmentId()));
    SetbuildMemoryUseValue(GetTotalMemSize());
    SetbuildingSegmentMemoryUseValue(_buildingSegment->EvaluateCurrentMemUsed());
    SetestimateMaxMemoryUseOfCurrentSegmentValue(EstimateMaxMemoryUseOfCurrentSegment());
    int64_t segmentBuildingTime = 0;
    if (_startBuildTime != -1) {
        segmentBuildingTime = autil::TimeUtility::currentTimeInSeconds() - _startBuildTime;
    }
    SetsegmentBuildingTimeValue(segmentBuildingTime);
    INDEXLIB_FM_REPORT_METRIC_WITH_TAGS(&tags, buildMemoryUse);
    INDEXLIB_FM_REPORT_METRIC_WITH_TAGS(&tags, buildingSegmentMemoryUse);
    INDEXLIB_FM_REPORT_METRIC_WITH_TAGS(&tags, estimateMaxMemoryUseOfCurrentSegment);
    INDEXLIB_FM_REPORT_METRIC_WITH_TAGS(&tags, segmentBuildingTime);

    ReportTableSepecificMetrics(tags);
}

Status CommonTabletWriter::DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                                  const framework::BuildResource& buildResource,
                                  const framework::OpenOptions& openOptions)
{
    return Status::OK();
}

Status CommonTabletWriter::DoBuild(const std::shared_ptr<document::IDocumentBatch>& batch)
{
    return _buildingSegment->Build(batch.get());
}

Status CommonTabletWriter::DoBuildAndReportMetrics(const std::shared_ptr<document::IDocumentBatch>& batch)
{
    auto status = DoBuild(batch);
    ReportBuildDocumentMetrics(batch.get());
    return status;
}

Status CommonTabletWriter::Build(const std::shared_ptr<document::IDocumentBatch>& batch)
{
    if (unlikely(_startBuildTime == -1)) {
        _startBuildTime = autil::TimeUtility::currentTimeInSeconds();
    }
    auto status = CheckMemStatus();
    if (!status.IsOK()) {
        TABLET_LOG(WARN, "segment[%d] check mem status: %s", _buildingSegment->GetSegmentId(),
                   status.ToString().c_str());
        return status;
    }
    // TODO(yonghao.fyh): add retry
    try {
        status = DoBuildAndReportMetrics(batch);
    } catch (const indexlib::util::FileIOException& e) {
        TABLET_LOG(ERROR, "segment [%d] build caught io exception [%s]", _buildingSegment->GetSegmentId(), e.what());
        return Status::IOError(_schema->GetTableName().c_str(), " ", _buildingSegment->GetSegmentId(),
                               " build caught io exception", e.what());
    } catch (...) {
        TABLET_LOG(ERROR, "segment [%d] build caught unknow exception", _buildingSegment->GetSegmentId());
        return Status::Unknown(_schema->GetTableName().c_str(), " ", _buildingSegment->GetSegmentId(),
                               " build caught unknown exception");
    }
    if (status.IsOK()) {
        UpdateDocCounter(batch.get());
    }
    return status;
}

void CommonTabletWriter::ReportBuildDocumentMetrics(document::IDocumentBatch* batch)
{
    if (_buildResource.buildDocumentMetrics != nullptr) {
        _buildResource.buildDocumentMetrics->ReportBuildDocumentMetrics(batch);
    }
}

void CommonTabletWriter::UpdateDocCounter(document::IDocumentBatch* batch)
{
    if (mAddDocCountCounter) {
        mAddDocCountCounter->Increase(batch->GetValidDocCount());
    }
}

size_t CommonTabletWriter::GetBuildingSegmentDumpExpandSize() const
{
    indexlib::table::TabletWriterResourceCalculator calculator(_options->IsOnline(),
                                                               _options->GetBuildConfig().GetDumpThreadCount());
    calculator.Init(_buildingSegment->GetBuildResourceMetrics());
    // TODO: consider dump file size need add in???
    return calculator.EstimateDumpMemoryUse() + calculator.EstimateDumpFileSize();
}

int64_t CommonTabletWriter::EstimateMaxMemoryUseOfCurrentSegment() const
{
    indexlib::table::TabletWriterResourceCalculator calculator(_options->IsOnline(),
                                                               _options->GetBuildConfig().GetDumpThreadCount());
    calculator.Init(_buildingSegment->GetBuildResourceMetrics());
    return calculator.EstimateMaxMemoryUseOfCurrentSegment();
}

// only used when dump max mem use exceeds free quota
int64_t CommonTabletWriter::GetMinDumpSegmentMemSize() const
{
    return _buildResource.buildingMemLimit * MIN_DUMP_MEM_LIMIT_RATIO;
}

size_t CommonTabletWriter::GetTotalMemSize() const { return _buildingSegment->EvaluateCurrentMemUsed(); }

Status CommonTabletWriter::CheckMemStatus() const
{
    int64_t freeQuota = _buildResource.memController->GetFreeQuota();
    if (freeQuota <= 0) {
        TABLET_LOG(ERROR, "segment[%d] build mem free quota [%s] less than 0", _buildingSegment->GetSegmentId(),
                   autil::UnitUtil::GiBDebugString(freeQuota).c_str());
        return Status::NoMem();
    }
    if (!IsDirty()) {
        return Status::OK();
    }
    int64_t curSegMaxMemUse = EstimateMaxMemoryUseOfCurrentSegment();
    if (curSegMaxMemUse >= freeQuota) {
        int64_t totalMemSize = GetTotalMemSize();
        if (totalMemSize >= GetMinDumpSegmentMemSize()) {
            TABLET_LOG(INFO, "segment[%d] current segment max mem [%s] use more than free quota[%s], trigger dump",
                       _buildingSegment->GetSegmentId(), autil::UnitUtil::GiBDebugString(curSegMaxMemUse).c_str(),
                       autil::UnitUtil::GiBDebugString(freeQuota).c_str());
            return Status::NeedDump();
        }
        TABLET_LOG(ERROR, "segment[%d] build no mem: current segment mem [%s], total mem [%s], free quota[%s]",
                   _buildingSegment->GetSegmentId(), autil::UnitUtil::GiBDebugString(curSegMaxMemUse).c_str(),
                   autil::UnitUtil::GiBDebugString(totalMemSize).c_str(),
                   autil::UnitUtil::GiBDebugString(freeQuota).c_str());
        return Status::NoMem();
    }
    assert(_buildResource.buildingMemLimit >= 0);
    if (curSegMaxMemUse >= _buildResource.buildingMemLimit) {
        TABLET_LOG(INFO,
                   "segment[%d] reach max mem use, trigger dump: current segment mem [%s], building mem limit [%s]",
                   _buildingSegment->GetSegmentId(), autil::UnitUtil::GiBDebugString(curSegMaxMemUse).c_str(),
                   autil::UnitUtil::GiBDebugString(_buildResource.buildingMemLimit).c_str());
        return Status::NeedDump();
    }
    if (_buildingSegment->NeedDump()) {
        TABLET_LOG(INFO, "segment[%d] building segment trigger dump", _buildingSegment->GetSegmentId());
        return Status::NeedDump();
    }
    return Status::OK();
}

bool CommonTabletWriter::IsDirty() const { return _buildingSegment->IsDirty(); }

std::unique_ptr<SegmentDumper> CommonTabletWriter::CreateSegmentDumper()
{
    if (!IsDirty()) {
        AUTIL_LOG(INFO, "building segment is not dirty, create nullptr segment dumper");
        return nullptr;
    }
    _buildingSegment->Seal();
    return std::make_unique<SegmentDumper>(_tabletData->GetTabletName(), _buildingSegment,
                                           GetBuildingSegmentDumpExpandSize(),
                                           _buildResource.metricsManager->GetMetricsReporter());
}

void CommonTabletWriter::RegisterTableSepecificMetrics() {}

void CommonTabletWriter::ReportTableSepecificMetrics(const kmonitor::MetricsTags& tags) {}

} // namespace indexlibv2::table
