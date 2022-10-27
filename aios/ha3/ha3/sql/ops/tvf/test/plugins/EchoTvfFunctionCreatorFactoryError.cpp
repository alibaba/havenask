#include "EchoTvfFunctionCreatorFactoryError.h"
#include "build_service/plugin/ModuleFactory.h"

using namespace std;
BEGIN_HA3_NAMESPACE(sql);

AUTIL_LOG_SETUP(sql, EchoTvfFunctionCreatorFactoryError);

EchoTvfFunctionCreatorFactoryError::EchoTvfFunctionCreatorFactoryError() {
}

EchoTvfFunctionCreatorFactoryError::~EchoTvfFunctionCreatorFactoryError() {
}

bool EchoTvfFunctionCreatorFactoryError::registerTvfFunctions() {
    REGISTER_TVF_FUNC_CREATOR("echoTvf", EchoTvfFuncCreator);
    return true;
}

END_HA3_NAMESPACE(sql);
