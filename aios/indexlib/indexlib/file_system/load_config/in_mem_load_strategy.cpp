#include "indexlib/file_system/load_config/in_mem_load_strategy.h"
#include "indexlib/misc/exception.h"
#include <unistd.h>

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemLoadStrategy);

InMemLoadStrategy::InMemLoadStrategy(uint32_t slice, uint32_t interval,
                                     const LoadSpeedLimitSwitchPtr& loadSpeedLimitSwitch)
    : mSlice(slice)
    , mInterval(interval)
    , mLoadSpeedLimitSwitch(loadSpeedLimitSwitch)
{
}

InMemLoadStrategy::~InMemLoadStrategy()
{
}

bool InMemLoadStrategy::EqualWith(const LoadStrategyPtr& loadStrategy) const
{
    assert(loadStrategy);
    if (GetLoadStrategyName() != loadStrategy->GetLoadStrategyName())
    {
        return false;
    }
    auto right = DYNAMIC_POINTER_CAST(
            InMemLoadStrategy, loadStrategy);
    assert(right);

    return  mSlice == right->mSlice
        && mInterval == right->mInterval;
}

void InMemLoadStrategy::Check()
{
    if (mSlice == 0)
    {
        INDEXLIB_THROW(misc::BadParameterException, "slice can not be 0");
    }
    if (mSlice % getpagesize() != 0)
    {
        INDEXLIB_THROW(misc::BadParameterException,
                       "slice must be the multiple of page size[%d]", getpagesize());
    }
}

uint32_t InMemLoadStrategy::GetInterval() const
{
    if (!mLoadSpeedLimitSwitch || mLoadSpeedLimitSwitch->IsSwitchOn())
    {
        return mInterval;
    }
    return 0;
}

IE_NAMESPACE_END(file_system);

