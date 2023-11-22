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

#include "navi/engine/KernelMetric.h"
#include "lockless_allocator/LocklessApi.h"

namespace navi {

class ComputeScope {
public:
    ComputeScope(KernelMetric *metric,
                 GraphEventType type,
                 NaviPartId partId,
                 NaviWorkerBase *worker,
                 const ScheduleInfo &schedInfo,
                 bool collectMetric,
                 bool collectPerf)
        : _metric(metric)
        , _type(type)
        , _partId(partId)
        , _worker(worker)
        , _schedInfo(schedInfo)
        , _beginTime(CommonUtil::getTimelineTimeNs())
        , _endTime(0)
        , _ec(EC_NONE)
        , _collectMetric(collectMetric)
        , _collectPerf(collectPerf)
        , _prevPoolMallocCount(0)
        , _poolMallocCount(0)
        , _prevPoolMallocSize(0)
        , _poolMallocSize(0)
    {
        if (_collectMetric || _collectPerf) {
            _prevPoolMallocCount = mySwapPoolMallocCount(0);
            _prevPoolMallocSize = mySwapPoolMallocSize(0);
            getrusage(RUSAGE_THREAD, &_usageBegin);
        }
        if (_collectPerf) {
            _worker->beginSample();
        }
        _metric->incScheduleCount();
    }
    void setErrorCode(ErrorCode ec) {
        _ec = ec;
    }
    float finish() {
        stop();
        return (_endTime - _beginTime) / 1000.0f;
    }
    void stop() {
        if (_endTime == 0) {
            if (_collectMetric || _collectPerf) {
                getrusage(RUSAGE_THREAD, &_usageEnd);
                _poolMallocCount = mySwapPoolMallocCount(_prevPoolMallocCount);
                _poolMallocSize = mySwapPoolMallocSize(_prevPoolMallocSize);
            }
            if (_collectPerf) {
                _result = _worker->endSample();
            }
            _endTime = CommonUtil::getTimelineTimeNs();
        }
    }
private:
    size_t mySwapPoolMallocSize(size_t size) {
        return swapPoolMallocSize(size);
    }

    size_t mySwapPoolMallocCount(size_t count) {
        return swapPoolMallocCount(count);
    }
public:
    ~ComputeScope() {
        stop();
        if (_collectMetric || _collectPerf) {
            _metric->reportTime(_ec, (int32_t)_type, _partId, _beginTime,
                                _endTime, _usageBegin, _usageEnd, _schedInfo,
                                _result, _poolMallocCount, _poolMallocSize);
        } else {
            _metric->reportTime(_ec, _beginTime, _endTime, _schedInfo);
        }
    }
private:
    MallocPoolScope mallocScope;
    KernelMetric *_metric;
    GraphEventType _type;
    NaviPartId _partId;
    NaviWorkerBase *_worker;
    const ScheduleInfo &_schedInfo;
    int64_t _beginTime;
    int64_t _endTime;
    ErrorCode _ec;
    bool _collectMetric;
    bool _collectPerf;
    struct rusage _usageBegin;
    struct rusage _usageEnd;
    NaviPerfResultPtr _result;
    size_t _prevPoolMallocCount;
    size_t _poolMallocCount;
    size_t _prevPoolMallocSize;
    size_t _poolMallocSize;
};

}

