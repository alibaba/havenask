#include "SumFuncInterface.h"
#include <autil/StringTokenizer.h>
#include "FunctionPluginFactory.h"
 
using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);

namespace pluginplatform {
namespace udf_plugins {
HA3_LOG_SETUP(udf_plugins, FunctionPluginFactory);
 
DECLARE_UNKNOWN_TYPE_FUNCTION_CREATOR(InvalidFuncTypeInterface, "invalid_func_type", 0);
class InvalidFuncTypeInterfaceCreatorImpl : public FUNCTION_CREATOR_CLASS_NAME(InvalidFuncTypeInterface)
{
public:
    /* override */
    isearch::func_expression::FunctionInterface *createFunction(const isearch::func_expression::FunctionSubExprVec& funcSubExprVec) {
        return NULL;
    }
};
 
FunctionPluginFactory::FunctionPluginFactory() {
}
 
FunctionPluginFactory::~FunctionPluginFactory() {
}
 
bool FunctionPluginFactory::registeFunctions() {
    return true;
}
 
bool FunctionPluginFactory::init(const KeyValueMap &parameters) {
    REGISTE_FUNCTION_CREATOR(SumFuncInterfaceCreatorImpl);
    return true;
}
 
extern "C"
isearch::util::ModuleFactory* createFactory() {
    return new FunctionPluginFactory;
}
 
extern "C"
void destroyFactory(isearch::util::ModuleFactory *factory) {
    factory->destroy();
}
 
}}