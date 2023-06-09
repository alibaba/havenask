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

#include <assert.h>
#include <memory>
#include <vector>

#include "future_lite/Executor.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/MetricsWrapper.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentDumpItem.h"
#include "kmonitor/client/MetricsReporter.h"

namespace indexlibv2::framework {

class Version;

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

    Status Dump(future_lite::Executor* executor);
    segmentid_t GetSegmentId() const { return _dumpingSegment->GetSegmentId(); }
    size_t EstimateDumpExpandMemsize() const { return _dumpExpandMemSize; }
    size_t GetCurrentMemoryUse() const { return _dumpingSegment->EvaluateCurrentMemUsed(); }

    std::shared_ptr<MemSegment> TEST_GetDumpingSegment() const;

private:
    virtual Status StoreSegmentInfo();

private:
    std::string _tabletName;
    std::shared_ptr<MemSegment> _dumpingSegment;
    int64_t _dumpExpandMemSize;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    INDEXLIB_FM_DECLARE_METRIC(dumpSegmentLatency);

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
