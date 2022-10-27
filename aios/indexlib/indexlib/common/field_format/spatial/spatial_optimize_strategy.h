#ifndef __INDEXLIB_SPATIAL_OPTIMIZE_STRATEGY_H
#define __INDEXLIB_SPATIAL_OPTIMIZE_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/singleton.h"

IE_NAMESPACE_BEGIN(common);

class SpatialOptimizeStrategy : public misc::Singleton<SpatialOptimizeStrategy>
{
public:
    SpatialOptimizeStrategy();
    ~SpatialOptimizeStrategy();

public:
    bool HasEnableOptimize() const { return mEnableOptimize; }
    void SetEnableOptimize(bool enable) { mEnableOptimize = enable; }
    
private:
    bool mEnableOptimize;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpatialOptimizeStrategy);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SPATIAL_OPTIMIZE_STRATEGY_H
