#ifndef __INDEXLIB_LOAD_STRATEGY_H
#define __INDEXLIB_LOAD_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

DECLARE_REFERENCE_CLASS(file_system, LoadStrategy);

IE_NAMESPACE_BEGIN(file_system);

class LoadSpeedLimitSwitch
{
public:
    LoadSpeedLimitSwitch() : mIsSwitchOn(true) {}
    ~LoadSpeedLimitSwitch() {}

    void Enable()
    { mIsSwitchOn = true; }

    void Disable()
    { mIsSwitchOn = false; }

    bool IsSwitchOn()
    { return mIsSwitchOn; }

private:
    bool mIsSwitchOn;
};
typedef std::tr1::shared_ptr<LoadSpeedLimitSwitch> LoadSpeedLimitSwitchPtr;

class LoadStrategy : public autil::legacy::Jsonizable
{
public:
    LoadStrategy();
    ~LoadStrategy();

public:
    virtual const std::string& GetLoadStrategyName() const = 0;

    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {}

    virtual bool EqualWith(const LoadStrategyPtr& loadStrategy) const = 0;

    virtual void Check() = 0;

    virtual void SetLoadSpeedLimitSwitch(const LoadSpeedLimitSwitchPtr& loadSpeedLimitSwitch) {}

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_LOAD_STRATEGY_H
