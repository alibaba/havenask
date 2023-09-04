#include "SampleAggFunctionCreatorFactory.h"

#include <iosfwd>

using namespace std;
namespace sql {

SampleAggFunctionCreatorFactory::SampleAggFunctionCreatorFactory() {}

SampleAggFunctionCreatorFactory::~SampleAggFunctionCreatorFactory() {}

// bool SampleAggFunctionCreatorFactory::registerAggFunctions(
//         autil::InterfaceManager *manager)
// {
//     REGISTER_INTERFACE("sum2", SampleAggFuncCreator, manager);
//     return true;
// }

// bool SampleAggFunctionCreatorFactory::registerAggFunctionInfos() {
//     autil::legacy::json::JsonMap properties;
//     REGISTER_AGG_FUNC_INFO("sum2", "[int64]", "[int32]", "[int64]", properties);
//     return true;
// }

} // namespace sql

// extern "C" AggFuncCreatorFactory *createFactory_Sql_Agg_Function() {
//     return new SampleAggFunctionCreatorFactory();
// }

// extern "C" void destroyFactory_Sql_Agg_Function(build_service::plugin::ModuleFactory *factory) {
//     return factory->destroy();
// }
