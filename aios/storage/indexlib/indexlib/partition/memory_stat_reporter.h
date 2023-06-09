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
#ifndef __INDEXLIB_MEMORY_STAT_REPORTER_H
#define __INDEXLIB_MEMORY_STAT_REPORTER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/metrics/Monitor.h"

DECLARE_REFERENCE_CLASS(partition, MemoryStatCollector);
DECLARE_REFERENCE_CLASS(util, TaskScheduler);
DECLARE_REFERENCE_CLASS(util, SearchCache);
DECLARE_REFERENCE_CLASS(file_system, FileBlockCacheContainer);

namespace indexlib { namespace partition {

class MemoryStatReporter
{
public:
    MemoryStatReporter();
    ~MemoryStatReporter();

public:
    bool Init(const std::string& param, const util::SearchCachePtr& searchCache,
              const file_system::FileBlockCacheContainerPtr& blockCacheContainer,
              const util::TaskSchedulerPtr& taskScheduler,
              util::MetricProviderPtr metricProvider = util::MetricProviderPtr());

    const MemoryStatCollectorPtr& GetMemoryStatCollector() const { return mMemStatCollector; }

private:
    int32_t mPrintMetricsTaskId;
    MemoryStatCollectorPtr mMemStatCollector;
    util::TaskSchedulerPtr mTaskScheduler;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MemoryStatReporter);
}} // namespace indexlib::partition

#endif //__INDEXLIB_MEMORY_STAT_REPORTER_H
