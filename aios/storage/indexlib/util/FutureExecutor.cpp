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
#include "indexlib/util/FutureExecutor.h"

#include <unistd.h>

#include "autil/EnvUtil.h"
#include "future_lite/ExecutorCreator.h"
#include "indexlib/base/Constant.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"
using namespace std;

// CHECK_FUTURE_LITE_EXECUTOR(async_io);

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, FutureExecutor);

std::once_flag FutureExecutor::internalExecutorFlag;
future_lite::Executor* FutureExecutor::internalExecutor = nullptr;
std::once_flag FutureExecutor::internalBuildExecutorFlag;
future_lite::Executor* FutureExecutor::internalBuildExecutor = nullptr;
std::thread FutureExecutor::reportMetricsThread;

future_lite::Executor* FutureExecutor::CreateExecutor(int threadNum, int maxAio)
{
    static int32_t idx = 0;
    auto params = future_lite::ExecutorCreator::Parameters()
                      .SetExecutorName("async_io_thread_pool_" + std::to_string(idx++))
                      .SetThreadNum(threadNum)
                      .Set<uint32_t>("max_aio", maxAio);
    auto executor = future_lite::ExecutorCreator::Create(/*type*/ "async_io", params);
    AUTIL_LOG(INFO, "pool created[%p], threadNum[%d], max_aio [%d]", executor.get(), threadNum, maxAio);
    return executor.release();
}
void FutureExecutor::DestroyExecutor(future_lite::Executor* executor)
{
    if (executor) {
        delete executor;
    }
}
future_lite::Executor* FutureExecutor::GetInternalBuildExecutor()
{
    std::call_once(internalBuildExecutorFlag, []() {
        int threadNum = autil::EnvUtil::getEnv("INDEXLIB_INTERNAL_BUILD_THREADNUM", 1);
        int maxAio = autil::EnvUtil::getEnv("INDEXLIB_INTERNAL_BUILD_MAXAIO", 32);
        if (threadNum <= 0 || maxAio <= 0) {
            AUTIL_LOG(WARN, "threadNum[%d] or maxAio[%d] illegal, do not create internal build pool", threadNum,
                      maxAio);
            return;
        }

        internalBuildExecutor = CreateExecutor(threadNum, maxAio);
    });
    return internalBuildExecutor;
}

future_lite::Executor* FutureExecutor::GetInternalExecutor()
{
    std::call_once(internalExecutorFlag, []() {
        int threadNum = autil::EnvUtil::getEnv("INDEXLIB_INTERNAL_THREADNUM", -1);
        int queueSize = autil::EnvUtil::getEnv("INDEXLIB_INTERNAL_QUEUESIZE", 32);
        if (threadNum <= 0 || queueSize <= 0) {
            AUTIL_LOG(WARN, "threadNum[%d] or queueSize[%d] illegal, do not create internal pool", threadNum,
                      queueSize);
            return;
        }

        AUTIL_LOG(INFO, "internal pool created, threadNum[%d], queueSize/max_aio [%d]", threadNum, queueSize);
        internalExecutor = CreateExecutor(threadNum, queueSize);
    });
    return internalExecutor;
}

void FutureExecutor::SetInternalExecutor(future_lite::Executor* executor)
{
    std::call_once(internalExecutorFlag, [executor]() {
        if (executor) {
            AUTIL_LOG(INFO, "internal pool setted");
        } else {
            AUTIL_LOG(INFO, "internal pool set to null");
        }
        internalExecutor = executor;
    });
}

void FutureExecutor::SetInternalBuildExecutor(future_lite::Executor* executor)
{
    std::call_once(internalBuildExecutorFlag, [executor]() {
        if (executor) {
            AUTIL_LOG(INFO, "internal pool setted");
        } else {
            AUTIL_LOG(INFO, "internal pool set to null");
        }
        internalBuildExecutor = executor;
    });
}

bool FutureExecutor::RegisterMetricsReporter(util::MetricProviderPtr metricProvider)
{
    if (!metricProvider) {
        AUTIL_LOG(WARN, "%s", "task schedule or metrics provider is empty, do not report future executor metrics");
        return true;
    }
    int32_t sleepTime = REPORT_METRICS_INTERVAL;
    auto executor = GetInternalExecutor();
    if (!executor) {
        AUTIL_LOG(INFO, "no executor created, do not report metrics");
        return true;
    }
    reportMetricsThread = std::thread([sleepTime, metricProvider, executor]() {
        IE_DECLARE_METRIC(pendingTaskCount);
        IE_INIT_METRIC_GROUP(metricProvider, pendingTaskCount, "global/ExecutorPendingTaskCount", kmonitor::STATUS,
                             "count");
        for (;;) {
            auto stat = executor->stat();
            IE_REPORT_METRIC(pendingTaskCount, stat.pendingTaskCount);
            usleep(sleepTime);
        }
    });
    return true;
}
}} // namespace indexlib::util
