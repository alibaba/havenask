#include "EchoTvfFunctionCreatorFactoryErrorRegister.h"
#include "build_service/plugin/ModuleFactory.h"

using namespace std;
BEGIN_HA3_NAMESPACE(sql);

AUTIL_LOG_SETUP(sql, EchoTvfFunctionCreatorFactoryErrorRegister);

EchoTvfFunctionCreatorFactoryErrorRegister::EchoTvfFunctionCreatorFactoryErrorRegister() {
}

EchoTvfFunctionCreatorFactoryErrorRegister::~EchoTvfFunctionCreatorFactoryErrorRegister() {
}

bool EchoTvfFunctionCreatorFactoryErrorRegister::registerTvfFunctions() {
    return false;
}

END_HA3_NAMESPACE(sql);

extern "C"
isearch::sql::TvfFuncCreatorFactory *createFactory_Sql_Tvf_Function() {
    return new isearch::sql::EchoTvfFunctionCreatorFactoryErrorRegister();
}

extern "C"
void destroyFactory_Sql_Tvf_Function(build_service::plugin::ModuleFactory *factory) {
    factory->destroy();
}
