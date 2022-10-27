#pragma once
#include <ha3/sql/ops/tvf/TvfFuncCreatorFactory.h>
#include "EchoTvfFunc.h"

BEGIN_HA3_NAMESPACE(sql);

class EchoTvfFunctionCreatorFactoryError : public TvfFuncCreatorFactory
{
public:
    EchoTvfFunctionCreatorFactoryError();
    ~EchoTvfFunctionCreatorFactoryError();
public:
    bool registerTvfFunctions() override;
private:
    AUTIL_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TvfFuncCreatorFactory);

END_HA3_NAMESPACE(sql);
