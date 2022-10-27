#include "EchoTvfFunctionCreatorFactory.h"
#include "build_service/plugin/ModuleFactory.h"

using namespace std;
BEGIN_HA3_NAMESPACE(sql);

AUTIL_LOG_SETUP(sql, EchoTvfFuncCreatorFactory);

EchoTvfFuncCreatorFactory::EchoTvfFuncCreatorFactory() {
}

EchoTvfFuncCreatorFactory::~EchoTvfFuncCreatorFactory() {
}

bool EchoTvfFuncCreatorFactory::registerTvfFunctions() {
    REGISTER_TVF_FUNC_CREATOR("echoTvf", EchoTvfFuncCreator);
    return true;
}

END_HA3_NAMESPACE(sql);

extern "C"
isearch::sql::TvfFuncCreatorFactory *createFactory_Sql_Tvf_Function() {
    return new isearch::sql::EchoTvfFuncCreatorFactory();
}

extern "C"
void destroyFactory_Sql_Tvf_Function(build_service::plugin::ModuleFactory *factory) {
    factory->destroy();
}
