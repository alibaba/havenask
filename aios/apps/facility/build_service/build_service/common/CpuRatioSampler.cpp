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
#include "build_service/common/CpuRatioSampler.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <thread>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"

using namespace std;

namespace build_service { namespace common {
BS_LOG_SETUP(common, CpuRatioSampler);

CpuRatioSampler::CpuRatioSampler(size_t sampleWindowSize) : _sampleWindowSize(sampleWindowSize) {}

CpuRatioSampler::~CpuRatioSampler() { stop(); }

bool CpuRatioSampler::start()
{
    int64_t cpuQuota = autil::EnvUtil::getEnv("BS_ROLE_CPU_QUOTA", -1);
    if (cpuQuota <= 0) {
        // admin可能是低版本，忽略
        BS_LOG(INFO, "get env [BS_ROLE_CPU_QUOTA] failed, maybe version of admin is not compatible");
        return true;
    }
    _cpuQuota = cpuQuota;
    _loopThread = autil::LoopThread::createLoopThread(std::bind(&CpuRatioSampler::workLoop, this), 1 * 1000 * 1000);
    return _loopThread != nullptr;
}

void CpuRatioSampler::TEST_setCpuQuota(int64_t cpuQuota) { _cpuQuota = cpuQuota; }

void CpuRatioSampler::workLoop()
{
    if (_cpuQuota == -1) {
        return;
    }
    int64_t ratio = 100 * _cpu.getUsage() * std::thread::hardware_concurrency() / _cpuQuota;
    _samples.push_back(ratio);
    _totalCpuRatioInQueue += ratio;
    while (_samples.size() > _sampleWindowSize) {
        auto ratio = _samples.front();
        _samples.pop_front();
        _totalCpuRatioInQueue -= ratio;
    }
    assert(_totalCpuRatioInQueue >= 0);

    int64_t avgRatio = _samples.size() >= _sampleWindowSize ? _totalCpuRatioInQueue / _sampleWindowSize : -1;
    _avgRatio.store(avgRatio, std::memory_order_relaxed);
    BS_INTERVAL_LOG(100, INFO, "current cpu ratio is [%ld], avg ratio is [%ld], sample [%zu]", ratio, avgRatio,
                    _samples.size());
}

int64_t CpuRatioSampler::getAvgCpuRatio() const { return _avgRatio.load(std::memory_order_relaxed); }

void CpuRatioSampler::stop()
{
    if (_loopThread) {
        _loopThread->stop();
        _loopThread.reset();
    }
}

}} // namespace build_service::common
