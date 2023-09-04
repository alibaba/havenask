#include "indexlib/document/query/test/example_function_executor.h"

using namespace std;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, ExampleFunctionExecutor);

ExampleFunctionExecutor::ExampleFunctionExecutor() {}

ExampleFunctionExecutor::~ExampleFunctionExecutor() {}

FunctionExecutor* ExampleFunctionExecutorFactory::CreateFunctionExecutor(const QueryFunction& function,
                                                                         const util::KeyValueMap& kvMap)
{
    ExampleFunctionExecutor* ret = new ExampleFunctionExecutor();
    if (ret->Init(function, kvMap)) {
        return ret;
    }
    delete ret;
    return nullptr;
}

extern "C" plugin::ModuleFactory* createFunctionExecutorFactory() { return new ExampleFunctionExecutorFactory; }

extern "C" void destroyFunctionExecutorFactory(plugin::ModuleFactory* factory) { factory->destroy(); }
}} // namespace indexlib::document
