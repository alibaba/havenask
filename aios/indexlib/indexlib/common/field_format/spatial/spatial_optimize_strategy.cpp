#include <unistd.h>
#include "indexlib/common/field_format/spatial/spatial_optimize_strategy.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, SpatialOptimizeStrategy);

SpatialOptimizeStrategy::SpatialOptimizeStrategy()
    : mEnableOptimize(true)
{
    char* envParam = getenv("DISABLE_SPATIAL_OPTIMIZE");
    if (envParam)
    {
        string envValue = string(envParam);
        if (envValue == "true")
        {
            IE_LOG(INFO, "disable spatial index optimize!");
            mEnableOptimize = false;
        }
    }
}

SpatialOptimizeStrategy::~SpatialOptimizeStrategy() 
{
}

IE_NAMESPACE_END(common);

