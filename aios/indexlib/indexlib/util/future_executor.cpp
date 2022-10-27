#include "indexlib/util/future_executor.h"
#include "indexlib/misc/monitor.h"
#include <future_lite/executors/ExecutorFactory.h>
#include "indexlib/misc/metric_provider.h"
#include <unistd.h>
using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, FutureExecutor);

std::once_flag FutureExecutor::internalExecutorFlag;
future_lite::Executor* FutureExecutor::internalExecutor = nullptr;
std::thread FutureExecutor::reportMetricsThread;

future_lite::Executor* FutureExecutor::GetInternalExecutor()
{
    std::call_once(internalExecutorFlag, []() {
        int threadNum = EnvUtil::GetEnv("INDEXLIB_INTERNAL_THREADNUM", -1);
        int queueSize = EnvUtil::GetEnv("INDEXLIB_INTERNAL_QUEUESIZE", 32);
        if (threadNum <= 0 || queueSize <= 0)
        {
            IE_LOG(WARN, "threadNum[%d] or queueSize[%d] illegal, do not create internal pool",
                threadNum, queueSize);
            return;
        }
        IE_LOG(INFO, "internal pool created, threadNum[%d], queueSize[%d]", threadNum, queueSize);
        internalExecutor = createFutureLiteExecutor("eigen", threadNum);
    });
    return internalExecutor;
}

void FutureExecutor::SetInternalExecutor(future_lite::Executor* executor)
{
    std::call_once(internalExecutorFlag, [executor]() {
        if (executor)
        {
            IE_LOG(INFO, "internal pool setted");
        }
        else
        {
            IE_LOG(INFO, "internal pool set to null");
        }
        internalExecutor = executor;
    });
}

bool FutureExecutor::RegisterMetricsReporter(misc::MetricProviderPtr metricProvider)
{
    if (!metricProvider)
    {
        IE_LOG(WARN, "%s",
            "task schedule or metrics provider is empty, do not report future executor metrics");
        return true;
    }
    int32_t sleepTime = INDEXLIB_REPORT_METRICS_INTERVAL;
    auto executor = GetInternalExecutor();
    if (!executor)
    {
        IE_LOG(INFO, "no executor created, do not report metrics");
        return true;
    }
    reportMetricsThread = std::thread([sleepTime, metricProvider, executor]() {
        IE_DECLARE_METRIC(pendingTaskCount);
        IE_INIT_METRIC_GROUP(metricProvider, pendingTaskCount, "global/ExecutorPendingTaskCount",
                             kmonitor::STATUS, "count");
        for (;;)
        {
            auto stat = executor->stat();
            IE_REPORT_METRIC(pendingTaskCount, stat.pendingTaskCount);
            usleep(sleepTime);
        }
    });
    return true;
}

IE_NAMESPACE_END(util);
