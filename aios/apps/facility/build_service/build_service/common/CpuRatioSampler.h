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

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "autil/metric/ProcessCpu.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class CpuRatioSampler
{
public:
    CpuRatioSampler(size_t sampleWindowSize);
    ~CpuRatioSampler();

public:
    bool start();
    int64_t getAvgCpuRatio() const;
    void TEST_setCpuQuota(int64_t cpuQuota);

private:
    void stop();
    void workLoop();

private:
    size_t _sampleWindowSize = 0;
    int64_t _cpuQuota = -1;
    std::atomic<int64_t> _avgRatio = -1;
    autil::metric::ProcessCpu _cpu;
    std::deque<int64_t> _samples;
    std::shared_ptr<autil::LoopThread> _loopThread;
    int64_t _totalCpuRatioInQueue = 0;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CpuRatioSampler);

}} // namespace build_service::common
