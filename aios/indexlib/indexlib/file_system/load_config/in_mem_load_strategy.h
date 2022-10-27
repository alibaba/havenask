#ifndef __INDEXLIB_IN_MEM_LOAD_STRATEGY_H
#define __INDEXLIB_IN_MEM_LOAD_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/load_config/load_strategy.h"

IE_NAMESPACE_BEGIN(file_system);

class InMemLoadStrategy : public LoadStrategy
{
public:
    InMemLoadStrategy(uint32_t slice, uint32_t interval,
                      const LoadSpeedLimitSwitchPtr& loadSpeedLimitSwitch = LoadSpeedLimitSwitchPtr());
    ~InMemLoadStrategy();

public:
    bool EqualWith(const LoadStrategyPtr& loadStrategy) const override;
    const std::string& GetLoadStrategyName() const override { return READ_MODE_IN_MEM; }

    uint32_t GetSlice() const { return mSlice; }
    uint32_t GetInterval() const;

    void Check() override;

private:
    uint32_t mSlice;
    uint32_t mInterval;
    LoadSpeedLimitSwitchPtr mLoadSpeedLimitSwitch;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemLoadStrategy);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_IN_MEM_LOAD_STRATEGY_H
