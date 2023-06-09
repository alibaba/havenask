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

#include <memory>

#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib { namespace util {

class BuildResourceCalculator
{
public:
    BuildResourceCalculator() {}
    ~BuildResourceCalculator() {}

public:
    static int64_t GetCurrentTotalMemoryUse(const BuildResourceMetricsPtr& metrics)
    {
        return metrics ? metrics->GetValue(BMT_CURRENT_MEMORY_USE) : 0;
    }

    static int64_t EstimateDumpTempMemoryUse(const BuildResourceMetricsPtr& metrics, int dumpThreadCount)
    {
        return metrics ? dumpThreadCount * metrics->GetValue(BMT_DUMP_TEMP_MEMORY_SIZE) : 0;
    }

    static int64_t EstimateDumpExpandMemoryUse(const BuildResourceMetricsPtr& metrics)
    {
        return metrics ? metrics->GetValue(BMT_DUMP_EXPAND_MEMORY_SIZE) : 0;
    }

    static int64_t EstimateDumpFileSize(const BuildResourceMetricsPtr& metrics)
    {
        return metrics ? metrics->GetValue(BMT_DUMP_FILE_SIZE) : 0;
    }
};

typedef std::shared_ptr<BuildResourceCalculator> BuildResourceCalculatorPtr;
}} // namespace indexlib::util
