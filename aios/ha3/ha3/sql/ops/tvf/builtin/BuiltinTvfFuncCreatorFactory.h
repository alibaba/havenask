#pragma once
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/tvf/TvfFuncCreator.h>
#include <ha3/sql/ops/tvf/TvfFuncCreatorFactory.h>

BEGIN_HA3_NAMESPACE(sql);

class BuiltinTvfFuncCreatorFactory : public TvfFuncCreatorFactory
{
public:
    BuiltinTvfFuncCreatorFactory();
    ~BuiltinTvfFuncCreatorFactory();
public:
    bool registerTvfFunctions() override;
private:
    AUTIL_LOG_DECLARE();
};

END_HA3_NAMESPACE(sql);
