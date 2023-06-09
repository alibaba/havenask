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

#include <deque>
#include <memory>
#include <mutex>

#include "autil/Log.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/SegmentDumper.h"
#include "indexlib/framework/TabletCommitter.h"

namespace future_lite {
class Executor;
}

namespace indexlibv2::framework {
class Version;
class TabletData;

class TabletDumper : public autil::NoMoveable
{
public:
    TabletDumper(const std::string& tabletName, future_lite::Executor* dumpExecutor, TabletCommitter* tabletCommitter);
    ~TabletDumper() = default;

    void Init(int32_t maxRealtimeDumpIntervalSecond);
    void PushSegmentDumper(std::unique_ptr<SegmentDumper> segmentDumper);
    bool NeedDump() const;
    Status Dump();
    Status Seal();
    void AlterTable(schemaid_t schemaId);
    void TrimDumpingQueue(const TabletData& tabletData);
    size_t GetDumpQueueSize() const;
    size_t GetTotalDumpingSegmentsMemsize() const;
    size_t GetMaxDumpingSegmentExpandMemsize() const;
    bool NeedDumpOverInterval() const;

private:
    std::unique_ptr<SegmentDumpable> PopOne();

    mutable std::mutex _dataMutex;
    mutable std::mutex _dumpMutex;
    const std::string _tabletName;
    std::deque<std::unique_ptr<SegmentDumpable>> _segmentDumpQueue;
    future_lite::Executor* _dumpExecutor;
    TabletCommitter* _tabletCommitter;
    int64_t _lastDumpTimestampSecond = -1;
    int64_t _maxRealtimeDumpIntervalSecond = -1;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
