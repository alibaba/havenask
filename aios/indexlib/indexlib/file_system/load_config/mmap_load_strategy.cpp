#include "indexlib/file_system/load_config/mmap_load_strategy.h"
#include "indexlib/misc/exception.h"
#include <unistd.h>
using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, MmapLoadStrategy);

MmapLoadStrategy::MmapLoadStrategy()
    : mIsLock(false)
    , mIsPartialLock(false)
    , mAdviseRandom(false)
    , mSlice(4 * 1024 * 1024) // 4M
    , mInterval(0)
{
}

MmapLoadStrategy::MmapLoadStrategy(bool isLock, bool isPartialLock,
                                   bool adviseRandom, 
                                   uint32_t slice, uint32_t interval)
    : mIsLock(isLock)
    , mIsPartialLock(isPartialLock)
    , mAdviseRandom(adviseRandom)
    , mSlice(slice)
    , mInterval(interval)
{
}

MmapLoadStrategy::~MmapLoadStrategy() 
{
}

InMemLoadStrategy* MmapLoadStrategy::CreateInMemLoadStrategy() const
{
    return new InMemLoadStrategy(mSlice, mInterval, mLoadSpeedLimitSwitch);
}

uint32_t MmapLoadStrategy::GetInterval() const 
{
    if (!mLoadSpeedLimitSwitch || mLoadSpeedLimitSwitch->IsSwitchOn())
    {
        return mInterval;
    }
    return 0;
}

void MmapLoadStrategy::Check()
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

void MmapLoadStrategy::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("lock", mIsLock, false);
    json.Jsonize("partial_lock", mIsPartialLock, false);
    json.Jsonize("advise_random", mAdviseRandom, false);
    json.Jsonize("slice", mSlice, (uint32_t)(4 * 1024 * 1024));
    json.Jsonize("interval", mInterval, (uint32_t)0);
}

bool MmapLoadStrategy::EqualWith(const LoadStrategyPtr& loadStrategy) const
{
    assert(loadStrategy);
    if (GetLoadStrategyName() != loadStrategy->GetLoadStrategyName())
    {
        return false;
    }        
    MmapLoadStrategyPtr right = DYNAMIC_POINTER_CAST(
            MmapLoadStrategy, loadStrategy);
    assert(right);

    return *this == *right;
}

bool MmapLoadStrategy::operator==(const MmapLoadStrategy& loadStrategy) const
{
    return mIsLock == loadStrategy.mIsLock
        && mIsPartialLock == loadStrategy.mIsPartialLock
        && mAdviseRandom == loadStrategy.mAdviseRandom
        && mSlice == loadStrategy.mSlice
        && mInterval == loadStrategy.mInterval;
}

IE_NAMESPACE_END(file_system);

