#pragma once
#include <ha3/sql/ops/agg/AggFuncCreatorFactory.h>

BEGIN_HA3_NAMESPACE(sql);

class SampleAggFunctionCreatorFactory : public AggFuncCreatorFactory
{
public:
    SampleAggFunctionCreatorFactory();
    ~SampleAggFunctionCreatorFactory();
public:
    bool registerAggFunctions() override;
private:
    bool registerAggFunctionInfos() override;
private:
    AUTIL_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AggFuncCreatorFactory);

END_HA3_NAMESPACE(sql);
