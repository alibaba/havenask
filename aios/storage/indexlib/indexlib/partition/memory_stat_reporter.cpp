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
#include "indexlib/partition/memory_stat_reporter.h"

#include <malloc.h>
#include <time.h>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/partition/memory_stat_collector.h"
#include "indexlib/util/TaskItem.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, MemoryStatReporter);

namespace {
class MemoryStatReporterTaskItem : public util::TaskItem
{
public:
    MemoryStatReporterTaskItem(const MemoryStatCollectorPtr& memStatCollector, uint32_t interval)
        : mMemStatCollector(memStatCollector)
        , mPrintInterval(interval)
        , mPrintTsInSeconds(0)
    {
        mPrintTsInSeconds = TimeUtility::currentTimeInSeconds();
    }

public:
    void Run() override
    {
        mMemStatCollector->ReportMetrics();
        int64_t currentTsInSeconds = TimeUtility::currentTimeInSeconds();
        if (currentTsInSeconds - mPrintTsInSeconds > mPrintInterval) {
            char buf[30];
            time_t clock;
            time(&clock);

            struct tm* tm = localtime(&clock);
            strftime(buf, sizeof(buf), " %x %X %Y ", tm);

            cout << "########" << buf << "########" << endl;
            cerr << "########" << buf << "########" << endl;
            malloc_stats();
            mMemStatCollector->PrintMetrics();
            mPrintTsInSeconds = currentTsInSeconds;
        }
    }

private:
    MemoryStatCollectorPtr mMemStatCollector;
    int64_t mPrintInterval;
    int64_t mPrintTsInSeconds;
};

}; // namespace

MemoryStatReporter::MemoryStatReporter() : mPrintMetricsTaskId(TaskScheduler::INVALID_TASK_ID) {}

MemoryStatReporter::~MemoryStatReporter()
{
    if (mTaskScheduler) {
        mTaskScheduler->DeleteTask(mPrintMetricsTaskId);
    }
}

bool MemoryStatReporter::Init(const string& param, const SearchCachePtr& searchCache,
                              const FileBlockCacheContainerPtr& blockCacheContainer,
                              const TaskSchedulerPtr& taskScheduler, util::MetricProviderPtr metricProvider)
{
    mTaskScheduler = taskScheduler;
    int64_t printInterval = 1200; // 20 min
    if (!param.empty() && !StringUtil::fromString(param, printInterval)) {
        IE_LOG(INFO, "invalid param [%s] to init MemoryStatReporter", param.c_str());
    }
    mMemStatCollector.reset(new MemoryStatCollector(searchCache, blockCacheContainer, metricProvider));

    int32_t sleepTime =
        autil::EnvUtil::getEnv("TEST_QUICK_EXIT", false) ? REPORT_METRICS_INTERVAL / 1000 : REPORT_METRICS_INTERVAL;
    TaskItemPtr taskItem(new MemoryStatReporterTaskItem(mMemStatCollector, printInterval));
    if (!mTaskScheduler->DeclareTaskGroup("report_metrics", sleepTime)) {
        IE_LOG(ERROR, "declare report metrics task failed!");
        return false;
    }

    mPrintMetricsTaskId = mTaskScheduler->AddTask("report_metrics", taskItem);
    if (mPrintMetricsTaskId == TaskScheduler::INVALID_TASK_ID) {
        IE_LOG(ERROR, "add report metrics task failed!");
        return false;
    }
    return true;
}
}} // namespace indexlib::partition
