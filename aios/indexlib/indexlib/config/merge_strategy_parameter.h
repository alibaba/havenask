#ifndef __INDEXLIB_MERGE_STRATEGY_PARAMETER_H
#define __INDEXLIB_MERGE_STRATEGY_PARAMETER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(config);

class MergeStrategyParameter : public autil::legacy::Jsonizable
{
public:
    MergeStrategyParameter();
    ~MergeStrategyParameter();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    // for legacy format compatible 
    void SetLegacyString(const std::string& legacyStr);
    std::string GetLegacyString() const;

public:
    std::string inputLimitParam;
    std::string strategyConditions;
    std::string outputLimitParam;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeStrategyParameter);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_MERGE_STRATEGY_PARAMETER_H
