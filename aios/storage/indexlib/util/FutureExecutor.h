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

#include <mutex>
#include <thread>

#include "autil/Log.h"
#include "future_lite/Executor.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace util {

class FutureExecutor
{
private:
    FutureExecutor() {}
    ~FutureExecutor() {}

public:
    static future_lite::Executor* GetInternalBuildExecutor();
    static future_lite::Executor* GetInternalExecutor();
    static void SetInternalExecutor(future_lite::Executor* executor);
    static void SetInternalBuildExecutor(future_lite::Executor* executor);

    static bool RegisterMetricsReporter(util::MetricProviderPtr metricProvider);

    static future_lite::Executor* CreateExecutor(int threadNum, int maxAio);
    static void DestroyExecutor(future_lite::Executor* executor);

private:
    static std::once_flag internalExecutorFlag;
    static future_lite::Executor* internalExecutor;
    static std::once_flag internalBuildExecutorFlag;
    static future_lite::Executor* internalBuildExecutor;
    static std::thread reportMetricsThread;

private:
    AUTIL_LOG_DECLARE();
};
}} // namespace indexlib::util
