#ifndef __INDEXLIB_PRIMARY_KEY_LOAD_STRATEGY_CREATOR_H
#define __INDEXLIB_PRIMARY_KEY_LOAD_STRATEGY_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_load_strategy.h"

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyLoadStrategyCreator
{
public:
    PrimaryKeyLoadStrategyCreator();
    ~PrimaryKeyLoadStrategyCreator();

public:
    static PrimaryKeyLoadStrategyPtr CreatePrimaryKeyLoadStrategy(
            const config::PrimaryKeyIndexConfigPtr& pkIndexConfig);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrimaryKeyLoadStrategyCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_LOAD_STRATEGY_CREATOR_H
