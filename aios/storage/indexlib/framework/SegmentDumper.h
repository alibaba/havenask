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

#include <condition_variable>
#include <memory>
#include <mutex>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <tuple>

#include "autil/Log.h"
#include "future_lite/Executor.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/MetricsWrapper.h"
#include "indexlib/framework/Segment.h"
#include "kmonitor/client/MetricType.h"
#include "kmonitor/client/MetricsReporter.h"

namespace indexlibv2::framework {

class SegmentDumpable
{
public:
    virtual ~SegmentDumpable() {}
};

struct AlterTableLog : public SegmentDumpable {
    schemaid_t schemaId = DEFAULT_SCHEMAID;
    AlterTableLog(schemaid_t id) : schemaId(id) {}
    ~AlterTableLog() = default;
};

class DumpControl
{
public:
    DumpControl(segmentid_t segmentId, uint32_t total, uint32_t ref, Status* st)
        : _segId {segmentId}
        , _totalCount {total}
        , _refCount {ref}
        , _status {st}
    {
    }

    segmentid_t GetSegmentId() const { return _segId; }
    uint32_t GetTotalCount() const { return _totalCount; }
    uint32_t GetFinishCount() const { return _finishCount; }

    std::tuple<uint32_t, uint32_t> StartTask();
    std::tuple<uint32_t, uint32_t> Iterate(Status& taskStatus);
    uint32_t ExitTask(const bool isCoordinator);

private:
    const segmentid_t _segId;

    std::atomic<uint32_t> _finishCount = 0;
    uint32_t _nextSeq = 0;
    uint32_t _totalCount;
    uint32_t _refCount;
    Status* _status;

    std::mutex _dumpMutex;
    std::condition_variable _dumpCv;
};

class SegmentDumper : public SegmentDumpable
{
public:
    SegmentDumper(const std::string& tabletName, const std::shared_ptr<MemSegment>& segment, int64_t dumpExpandMemSize,
                  std::shared_ptr<kmonitor::MetricsReporter> metricsReporter)
        : _tabletName(tabletName)
        , _dumpingSegment(segment)
        , _dumpExpandMemSize(dumpExpandMemSize)
        , _metricsReporter(metricsReporter)
    {
        if (_metricsReporter) {
            REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, dumpSegmentLatency, "build/dumpSegmentLatency",
                                                 kmonitor::GAUGE);
        }
        _dumpingSegment->SetSegmentStatus(Segment::SegmentStatus::ST_DUMPING);
    }
    virtual ~SegmentDumper() = default;

    Status Dump(future_lite::Executor* executor, const uint32_t dumpThreadCount);
    segmentid_t GetSegmentId() const { return _dumpingSegment->GetSegmentId(); }
    size_t EstimateDumpExpandMemsize() const { return _dumpExpandMemSize; }
    size_t GetCurrentMemoryUse() const { return _dumpingSegment->EvaluateCurrentMemUsed(); }

    std::shared_ptr<MemSegment> TEST_GetDumpingSegment() const;

private:
    virtual Status StoreSegmentInfo();

    Status SequentialDump(const std::vector<std::shared_ptr<SegmentDumpItem>>& dumpItems, const segmentid_t segId);
    Status ParallelDump(future_lite::Executor* executor, const std::vector<std::shared_ptr<SegmentDumpItem>>& dumpItems,
                        const segmentid_t segId, const uint32_t parallelism);
    void DumpTask(const std::vector<std::shared_ptr<SegmentDumpItem>>& dumpItems, DumpControl* control,
                  const bool isCoordinator);

private:
    static constexpr size_t PARALLEL_DUMP_THRESHOLD {4};

    std::string _tabletName;
    std::shared_ptr<MemSegment> _dumpingSegment;
    int64_t _dumpExpandMemSize;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    INDEXLIB_FM_DECLARE_METRIC(dumpSegmentLatency);

    AUTIL_LOG_DECLARE();
};
} // namespace indexlibv2::framework
