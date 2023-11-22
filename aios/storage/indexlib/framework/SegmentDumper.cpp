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
#include "indexlib/framework/SegmentDumper.h"

#include <memory>
#include <utility>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/SegmentDumpItem.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/util/metrics/Metric.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, SegmentDumper);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

std::tuple<uint32_t, uint32_t> DumpControl::StartTask()
{
    std::lock_guard<std::mutex> dumpGuard {_dumpMutex};
    return std::tuple(_nextSeq++, _totalCount);
}

std::tuple<uint32_t, uint32_t> DumpControl::Iterate(Status& taskStatus)
{
    std::lock_guard<std::mutex> dumpGuard {_dumpMutex};
    assert(_status != nullptr);
    uint32_t seq = _nextSeq;
    _finishCount++;
    if (_status->IsOK()) {
        if (taskStatus.IsOK()) {
            // Only if everything is fine, will we move forward.
            _nextSeq++;
        } else {
            *_status = taskStatus;
            // Reset the total count so that no one would be able to set off any new tasks.
            _totalCount = std::min(_totalCount, _nextSeq);
        }
    }
    return std::tuple(seq, _totalCount);
}

uint32_t DumpControl::ExitTask(const bool isCoordinator)
{
    std::unique_lock<std::mutex> dumpLock {_dumpMutex};
    if (isCoordinator) {
        while (_finishCount < _totalCount) {
            _dumpCv.wait(dumpLock);
        }
    } else if (_finishCount == _totalCount) {
        _dumpCv.notify_one();
        _finishCount++; // Prevent redundant notifications.
    }
    return --_refCount;
}

Status SegmentDumper::StoreSegmentInfo()
{
    auto segmentDir = _dumpingSegment->GetSegmentDirectory();
    auto segmentInfo = _dumpingSegment->GetSegmentInfo();
    if (!segmentDir) {
        return Status::InternalError("segment dir is empty");
    }
    TABLET_LOG(INFO, "begin store segment_info for segment[%d], segment info [%s]", GetSegmentId(),
               autil::legacy::ToJsonString(segmentInfo, true).c_str());
    auto status = segmentInfo->Store(segmentDir->GetIDirectory());
    if (!status.IsOK()) {
        auto s =
            Status::IOError("store segment_info failed, segId:%d, error:%s", GetSegmentId(), status.ToString().c_str());
        TABLET_LOG(ERROR, "%s", s.ToString().c_str());
        return s;
    }
    return status;
}

void SegmentDumper::DumpTask(const std::vector<std::shared_ptr<SegmentDumpItem>>& dumpItems, DumpControl* control,
                             const bool isCoordinator)
{
    assert(control);
    uint32_t total;
    uint32_t seq;
    std::tie(seq, total) = control->StartTask();
    while (seq < total) {
        // As it's just for log printing, it's okay to do concurrent read here.
        TABLET_LOG(INFO, "start dump segment task, segId[%d], seq[%u], isCoord[%d], total[%u], fin[%u]",
                   control->GetSegmentId(), seq, isCoordinator, control->GetTotalCount(), control->GetFinishCount());
        auto status = dumpItems[seq]->Dump();
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "dump segment task failed, segId[%d], seq[%u], error:%s", control->GetSegmentId(), seq,
                       status.ToString().c_str());
        }
        std::tie(seq, total) = control->Iterate(status);
    }
    if (control->ExitTask(isCoordinator) == 0) {
        delete control;
    }
}

// Note: The parameter `dumpThreadCount` only serves a directive purpose. In other words, a task
// will be sceduled `dumpThreadCount` times but it is not guaranteed to have exactly that many
// number of threads to do the dump. The de facto behavior is related to the internal mechanism of
// the `executor` passed in. It is possible that multiple dump tasks are assigned to one single
// thread.
Status SegmentDumper::Dump(future_lite::Executor* executor, const uint32_t dumpThreadCount)
{
    indexlib::util::ScopeLatencyReporter scopeTime(GetdumpSegmentLatencyMetric().get());
    auto segId = GetSegmentId();
    auto [st, dumpItems] = _dumpingSegment->CreateSegmentDumpItems();
    RETURN_IF_STATUS_ERROR(st, "create dump param failed, segId[%d]", segId);

    const uint32_t parallelism =
        executor == nullptr ? 1 : std::min(dumpThreadCount, static_cast<uint32_t>(dumpItems.size()));
    Status status;
    if (parallelism > 1 && dumpItems.size() >= PARALLEL_DUMP_THRESHOLD) {
        status = ParallelDump(executor, dumpItems, segId, parallelism);
    } else {
        status = SequentialDump(dumpItems, segId);
    }
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "dump segment failed, segId[%d]", segId);
        return status;
    }

    _dumpingSegment->EndDump();

    if (_dumpingSegment->GetSegmentDirectory()) {
        _dumpingSegment->GetSegmentDirectory()->FlushPackage();
    }
    TABLET_LOG(INFO, "dump segment[%d] success, docCount[%lu], parallelism[%u], time_used[%.3f]s", segId,
               _dumpingSegment->GetSegmentInfo()->GetDocCount(), parallelism, scopeTime.GetTimer().done_sec());
    return Status::OK();
}

Status SegmentDumper::SequentialDump(const std::vector<std::shared_ptr<SegmentDumpItem>>& dumpItems,
                                     const segmentid_t segId)
{
    for (size_t i = 0; i < dumpItems.size(); ++i) {
        auto& dumpItem = dumpItems[i];
        auto status = dumpItem->Dump();
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "dump segment failed, segId[%d], error:%s", segId, status.ToString().c_str());
            return status;
        }
    }
    return StoreSegmentInfo();
}

Status SegmentDumper::ParallelDump(future_lite::Executor* executor,
                                   const std::vector<std::shared_ptr<SegmentDumpItem>>& dumpItems,
                                   const segmentid_t segId, const uint32_t parallelism)
{
    Status status = Status::OK();
    // CAVEAT: The lifetime of the DumpControl object might transcend the Segmentdumper itself.
    DumpControl* control = new DumpControl(segId, dumpItems.size(), parallelism, &status);
    TABLET_LOG(INFO, "schedule dump segment task, segId[%d], parallelism[%u], total[%lu]", segId, parallelism,
               dumpItems.size());
    for (size_t i = 1; i < parallelism; i++) {
        executor->schedule(std::bind(&SegmentDumper::DumpTask, this, std::ref(dumpItems), control, false));
    }
    DumpTask(dumpItems, control, true); // The control object may get freed inside the function.
    if (!status.IsOK()) {
        return status;
    }
    return StoreSegmentInfo();
}

std::shared_ptr<MemSegment> SegmentDumper::TEST_GetDumpingSegment() const { return _dumpingSegment; }
} // namespace indexlibv2::framework
