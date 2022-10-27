#ifndef __INDEXLIB_BUILD_RESOURCE_CALCULATOR_H
#define __INDEXLIB_BUILD_RESOURCE_CALCULATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"

IE_NAMESPACE_BEGIN(util);

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

    static int64_t EstimateDumpTempMemoryUse(const BuildResourceMetricsPtr& metrics,
                                             int dumpThreadCount)
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

DEFINE_SHARED_PTR(BuildResourceCalculator);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BUILD_RESOURCE_CALCULATOR_H
