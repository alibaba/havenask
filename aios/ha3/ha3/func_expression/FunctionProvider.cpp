#include <ha3/func_expression/FunctionProvider.h>

BEGIN_HA3_NAMESPACE(func_expression);
HA3_LOG_SETUP(func_expression, FunctionProvider);

FunctionProvider::FunctionProvider(const FunctionResource &resource)
    : suez::turing::FunctionProvider(resource.matchDocAllocator.get(),
            resource.pool,
            resource.cavaAllocator,
            resource.requestTracer,
            resource.partitionReaderSnapshot,
            resource.kvpairs)
    , _resource(resource)
{
    setSearchPluginResource(&_resource);
    setupTraceRefer(convertRankTraceLevel(getRequest()));
}

FunctionProvider::~FunctionProvider() {
}

END_HA3_NAMESPACE(func_expression);
