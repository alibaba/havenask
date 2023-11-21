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
#include "indexlib/framework/TabletDumper.h"

#include <algorithm>
#include <assert.h>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "future_lite/Executor.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, TabletDumper);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

TabletDumper::TabletDumper(const std::string& tabletName, future_lite::Executor* dumpExecutor,
                           TabletCommitter* tabletCommitter)
    : _tabletName(tabletName)
    , _dumpExecutor(dumpExecutor)
    , _tabletCommitter(tabletCommitter)
{
    assert(_tabletCommitter != nullptr);
}

void TabletDumper::Init(int32_t maxRealtimeDumpIntervalSecond)
{
    _maxRealtimeDumpIntervalSecond = maxRealtimeDumpIntervalSecond;
    _lastDumpTimestampSecond = autil::TimeUtility::currentTimeInSeconds();
}

void TabletDumper::PushSegmentDumper(std::unique_ptr<SegmentDumper> segmentDumper)
{
    assert(segmentDumper);
    std::lock_guard<std::mutex> guard(_dataMutex);
    _segmentDumpQueue.emplace_back(std::move(segmentDumper));
    _lastDumpTimestampSecond = autil::TimeUtility::currentTimeInSeconds();
}

std::unique_ptr<SegmentDumpable> TabletDumper::PopOne()
{
    std::unique_ptr<SegmentDumpable> dumpable = nullptr;
    if (!_segmentDumpQueue.empty()) {
        dumpable = std::move(_segmentDumpQueue.front());
        _segmentDumpQueue.pop_front();
        // _condVar.notify_one();
    }
    return dumpable;
};

bool TabletDumper::NeedDump() const
{
    std::lock_guard<std::mutex> guard(_dataMutex);
    return !_segmentDumpQueue.empty();
}

void TabletDumper::TrimDumpingQueue(const TabletData& tabletData)
{
    auto slice = tabletData.CreateSlice(Segment::SegmentStatus::ST_DUMPING);
    auto version = tabletData.GetOnDiskVersion();
    std::lock_guard<std::mutex> guard(_dataMutex);
    decltype(_segmentDumpQueue) newDumpQueue;
    for (auto& dumpable : _segmentDumpQueue) {
        auto dumper = dynamic_cast<SegmentDumper*>(dumpable.get());
        if (!dumper) {
            auto alterTableLog = dynamic_cast<AlterTableLog*>(dumpable.get());
            assert(alterTableLog);
            if (alterTableLog->schemaId > tabletData.GetOnDiskVersion().GetSchemaId()) {
                newDumpQueue.push_back(std::move(dumpable));
            }
        } else {
            auto segId = dumper->GetSegmentId();
            auto result = std::find_if(slice.begin(), slice.end(),
                                       [segId](const auto& seg) { return seg->GetSegmentId() == segId; });
            if (result != slice.end()) {
                newDumpQueue.push_back(std::move(dumpable));
            }
        }
    }
    std::swap(_segmentDumpQueue, newDumpQueue);
}

Status TabletDumper::Dump(const uint32_t dumpThreadCount)
{
    autil::ScopedTime2 timer;
    if (unlikely(_dumpExecutor == nullptr)) {
        TABLET_LOG(ERROR, "future executor is NULL");
        return Status::IOError("future executor is NULL");
    }
    std::lock_guard<std::mutex> dumpGuard(_dumpMutex);
    size_t count = 0;
    {
        std::lock_guard<std::mutex> dataGuard(_dataMutex);
        count = _segmentDumpQueue.size();
    }
    for (size_t i = 0; i < count; ++i) {
        std::unique_ptr<SegmentDumpable> dumpable;
        {
            std::lock_guard<std::mutex> dataGuard(_dataMutex);
            dumpable = PopOne();
        }
        if (dumpable == nullptr) {
            count = i;
            break;
        }
        auto alterTableLog = dynamic_cast<AlterTableLog*>(dumpable.get());
        if (alterTableLog) {
            _tabletCommitter->AlterTable(alterTableLog->schemaId);
        } else {
            auto segDumper = dynamic_cast<SegmentDumper*>(dumpable.get());
            Status status = segDumper->Dump(_dumpExecutor, dumpThreadCount);
            if (!status.IsOK()) {
                TABLET_LOG(ERROR, "dump item[%lu/%lu] failed, error:%s", i, count, status.ToString().c_str());
                _tabletCommitter->SetDumpError(status);
                return status;
            }
            _tabletCommitter->Push(segDumper->GetSegmentId());
        }
    }
    if (count > 0) {
        TABLET_LOG(INFO, "dump segment finish, segment count[%lu] time used[%.3f]s", count, timer.done_sec());
    }
    return Status::OK();
}

void TabletDumper::AlterTable(schemaid_t schemaId)
{
    std::lock_guard<std::mutex> guard(_dataMutex);
    std::unique_ptr<SegmentDumpable> log = std::make_unique<AlterTableLog>(schemaId);
    _segmentDumpQueue.push_back(std::move(log));
}

Status TabletDumper::Seal(const uint32_t dumpThreadCount)
{
    autil::ScopedTime2 timer;
    if (unlikely(_dumpExecutor == nullptr)) {
        TABLET_LOG(ERROR, "dump executor is NULL");
        return Status::IOError("dump executor is NULL");
    }
    std::lock_guard<std::mutex> dumpGuard(_dumpMutex);
    std::lock_guard<std::mutex> dataGuard(_dataMutex);
    while (true) {
        auto dumpable = PopOne();
        if (!dumpable) {
            break;
        }
        auto alterTableLog = dynamic_cast<AlterTableLog*>(dumpable.get());
        if (alterTableLog) {
            _tabletCommitter->AlterTable(alterTableLog->schemaId);
        } else {
            auto dumper = dynamic_cast<SegmentDumper*>(dumpable.get());
            auto status = dumper->Dump(_dumpExecutor, dumpThreadCount);
            if (!status.IsOK()) {
                TABLET_LOG(ERROR, "dump segment[%d] failed: %s", dumper->GetSegmentId(), status.ToString().c_str());
                _tabletCommitter->SetDumpError(status);
                return status;
            }
            _tabletCommitter->Push(dumper->GetSegmentId());
        }
    }
    _tabletCommitter->Seal();
    TABLET_LOG(INFO, "tablet dumper sealed, time used[%.3f]s", timer.done_sec());
    return Status::OK();
}

size_t TabletDumper::GetDumpQueueSize() const
{
    std::lock_guard<std::mutex> guard(_dataMutex);
    return _segmentDumpQueue.size();
}

size_t TabletDumper::GetTotalDumpingSegmentsMemsize() const
{
    size_t totalSegmentMemsize = 0;
    std::lock_guard<std::mutex> guard(_dataMutex);
    for (const auto& dumpable : _segmentDumpQueue) {
        auto dumper = dynamic_cast<const SegmentDumper*>(dumpable.get());
        if (dumper) {
            totalSegmentMemsize += dumper->GetCurrentMemoryUse();
        }
    }
    return totalSegmentMemsize;
}

size_t TabletDumper::GetMaxDumpingSegmentExpandMemsize() const
{
    size_t maxDumpMemsize = 0;
    std::lock_guard<std::mutex> guard(_dataMutex);
    for (const auto& dumpable : _segmentDumpQueue) {
        const SegmentDumpable* raw = dumpable.get();
        auto dumper = dynamic_cast<const SegmentDumper*>(raw);
        if (dumper) {
            maxDumpMemsize = std::max(maxDumpMemsize, dumper->EstimateDumpExpandMemsize());
        }
    }
    return maxDumpMemsize;
}

bool TabletDumper::NeedDumpOverInterval() const
{
    if (_maxRealtimeDumpIntervalSecond < 0) {
        return false;
    }

    return autil::TimeUtility::currentTimeInSeconds() - _lastDumpTimestampSecond >= _maxRealtimeDumpIntervalSecond;
}

#undef TABLET_LOG

} // namespace indexlibv2::framework
