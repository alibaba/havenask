#ifndef __INDEXLIB_WARMUP_STRATEGY_H
#define __INDEXLIB_WARMUP_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(file_system);

class WarmupStrategy
{
public:
    enum WarmupType
    {
        WARMUP_NONE,
        WARMUP_SEQUENTIAL
    };

public:
    WarmupStrategy();
    virtual ~WarmupStrategy();

public:
    const WarmupType& GetWarmupType() const
    { return mWarmupType; }
    void SetWarmupType(const WarmupType& warmupType)
    { mWarmupType = warmupType; }

    bool operator==(const WarmupStrategy& warmupStrategy) const
    { return mWarmupType == warmupStrategy.mWarmupType; }

public:
    static WarmupType FromTypeString(const std::string& string);
    static std::string ToTypeString(const WarmupType& warmupType);

private:
    WarmupType mWarmupType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(WarmupStrategy);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_WARMUP_STRATEGY_H
