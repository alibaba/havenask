#ifndef __INDEXLIB_MMAP_LOAD_STRATEGY_H
#define __INDEXLIB_MMAP_LOAD_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/load_config/load_strategy.h"
#include "indexlib/file_system/load_config/in_mem_load_strategy.h"

IE_NAMESPACE_BEGIN(file_system);

class MmapLoadStrategy : public LoadStrategy
{
public:
    MmapLoadStrategy();
    MmapLoadStrategy(bool isLock, bool isPartialLock, bool adviseRandom,
                     uint32_t slice, uint32_t interval);
    ~MmapLoadStrategy();

public:
    bool IsLock() const { return mIsLock; }
    bool IsPartialLock() const { return mIsPartialLock; }

    bool IsAdviseRandom() const { return mAdviseRandom; }

    uint32_t GetSlice() const { return mSlice; }

    uint32_t GetInterval() const;

    const std::string& GetLoadStrategyName() const { return READ_MODE_MMAP; }

    void Check();

    bool EqualWith(const LoadStrategyPtr& loadStrategy) const;

    void SetLoadSpeedLimitSwitch(const LoadSpeedLimitSwitchPtr& loadSpeedLimitSwitch)
    { mLoadSpeedLimitSwitch = loadSpeedLimitSwitch; }

    bool operator==(const MmapLoadStrategy& loadStrategy) const;

    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    InMemLoadStrategy* CreateInMemLoadStrategy() const;

private:
    bool mIsLock;
    bool mIsPartialLock;
    bool mAdviseRandom;
    uint32_t mSlice;
    uint32_t mInterval;
    LoadSpeedLimitSwitchPtr mLoadSpeedLimitSwitch;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MmapLoadStrategy);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_MMAP_LOAD_STRATEGY_H
