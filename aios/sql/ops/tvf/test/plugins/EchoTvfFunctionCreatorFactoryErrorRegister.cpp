#include "EchoTvfFunctionCreatorFactoryErrorRegister.h"

#include <iosfwd>

using namespace std;
namespace sql {

EchoTvfFunctionCreatorFactoryErrorRegister::EchoTvfFunctionCreatorFactoryErrorRegister() {}

EchoTvfFunctionCreatorFactoryErrorRegister::~EchoTvfFunctionCreatorFactoryErrorRegister() {}

// bool EchoTvfFunctionCreatorFactoryErrorRegister::registerTvfFunctions() {
//     return false;
// }

} // namespace sql

// extern "C" TvfFuncCreatorFactory *createFactory_Sql_Tvf_Function() {
//     return new EchoTvfFunctionCreatorFactoryErrorRegister();
// }

// extern "C" void destroyFactory_Sql_Tvf_Function(build_service::plugin::ModuleFactory *factory) {
//     factory->destroy();
// }
