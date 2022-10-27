#ifndef __INDEXLIB_FUTURE_EXECUTOR_FACTORY_H
#define __INDEXLIB_FUTURE_EXECUTOR_FACTORY_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/env_util.h"
#include <future_lite/Executor.h>
#include "indexlib/misc/metric_provider.h"
#include <mutex>
#include <string>
#include <thread>
#include <tr1/memory>

IE_NAMESPACE_BEGIN(util);

class FutureExecutor {
private:
    FutureExecutor() {}
    ~FutureExecutor() {}

public:
    static future_lite::Executor* GetInternalExecutor();
    static void SetInternalExecutor(future_lite::Executor* executor);

    static bool RegisterMetricsReporter(misc::MetricProviderPtr metricProvider);

private:
    static std::once_flag internalExecutorFlag;
    static future_lite::Executor* internalExecutor;
    static std::thread reportMetricsThread;
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_FUTURE_EXECUTOR_FACTORY_H
