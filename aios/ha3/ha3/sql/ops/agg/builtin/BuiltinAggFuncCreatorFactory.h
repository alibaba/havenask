#pragma once
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/agg/AggFuncCreator.h>
#include <ha3/sql/ops/agg/AggFuncCreatorFactory.h>

BEGIN_HA3_NAMESPACE(sql);

class BuiltinAggFuncCreatorFactory : public AggFuncCreatorFactory
{
public:
    BuiltinAggFuncCreatorFactory();
    ~BuiltinAggFuncCreatorFactory();
public:
    bool registerAggFunctions() override;
private:
    bool registerAggFunctionInfos() override;
    bool registerArbitraryFunctionInfos();
    bool registerMaxLabelFunctionInfos();
    bool registerGatherFunctionInfos();
    bool registerMultiGatherFunctionInfos();
    bool registerLogicAndFunctionInfos();
    bool registerLogicOrFunctionInfos();
private:
    AUTIL_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AggFuncCreatorFactory);

END_HA3_NAMESPACE(sql);
