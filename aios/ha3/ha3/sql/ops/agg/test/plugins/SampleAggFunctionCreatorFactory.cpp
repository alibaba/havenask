#include <ha3/sql/ops/agg/test/plugins/SampleAggFunctionCreatorFactory.h>
#include <ha3/sql/ops/agg/test/plugins/SampleAggFunc.h>


using namespace std;
BEGIN_HA3_NAMESPACE(sql);

AUTIL_LOG_SETUP(sql, SampleAggFunctionCreatorFactory);

SampleAggFunctionCreatorFactory::SampleAggFunctionCreatorFactory() {
}

SampleAggFunctionCreatorFactory::~SampleAggFunctionCreatorFactory() {
}

bool SampleAggFunctionCreatorFactory::registerAggFunctions() {
    REGISTER_AGG_FUNC_CREATOR("sum2", SampleAggFuncCreator);
    return true;
}

bool SampleAggFunctionCreatorFactory::registerAggFunctionInfos() {
    REGISTER_AGG_FUNC_INFO("sum2", "[int64]", "[int32]", "[int64]");
    return true;
}

END_HA3_NAMESPACE(sql);

extern "C"
isearch::sql::AggFuncCreatorFactory *createFactory_Sql_Agg_Function() {
    return new isearch::sql::SampleAggFunctionCreatorFactory();
}

extern "C"
void destroyFactory_Sql_Agg_Function(build_service::plugin::ModuleFactory *factory) {
}
