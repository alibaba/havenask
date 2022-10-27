#include "indexlib/util/unsafe_metrics.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);

UnsafeMetrics::UnsafeMetrics(const std::string& metricsName) 
    : TypedMetricsBase<UnsafeMetrics>(metricsName)
    , mValue(0) 
    , mCount(0)
{
}

UnsafeMetrics::UnsafeMetrics(const std::string& metricsName, const UnsafeMetricsPtr& parent) 
    : TypedMetricsBase<UnsafeMetrics>(metricsName, parent)
    , mValue(0)
    , mCount(0)
{
}

UnsafeMetrics::~UnsafeMetrics() 
{
}

void UnsafeMetrics::Reset()
{
    mValue = 0;
    mCount = 0;
    if (mParent)
    {
        mParent->Reset();
    }
}

IE_NAMESPACE_END(util);

