#include <suez/turing/test/FunctionWrap.h>
#include <ha3/func_expression/FunctionProvider.h>

namespace pluginplatform {
namespace function_plugins {

class Ha3FunctionWrap : public suez::turing::FunctionWrap
{
public:
    Ha3FunctionWrap(const std::string& testDataPath, const std::string &caseName)
        : FunctionWrap(testDataPath, caseName)
    {
        _allocator.reset(new HA3_NS(common)::Ha3MatchDocAllocator(&_pool));
    }

    ~Ha3FunctionWrap() {
        _ha3FunctionProvider.reset();
    }

    void buildIndex(const std::string mainTable) {
        setMatchDocAllocator(_allocator.get());

        _indexWrap->buildIndex();
        _partitionReaderSnapshot = _indexWrap->CreateSnapshot();
        _dataProvider.reset(new HA3_NS(common)::DataProvider());
        HA3_NS(func_expression)::FunctionResource resource;
        resource.pool = &_pool;
        resource.matchDocAllocator = _allocator;
        resource.requestTracer = NULL;
        resource.partitionReaderSnapshot = _partitionReaderSnapshot.get();
        resource.kvpairs = &_param;
        resource.dataProvider = _dataProvider.get();
        _ha3FunctionProvider.reset(new HA3_NS(func_expression)::FunctionProvider(resource));
        _factory = new suez::turing::AttributeExpressionFactory(mainTable, _partitionReaderSnapshot.get(),
                NULL, &_pool, _allocator.get());
    }

    bool callBeginRequest(suez::turing::FunctionInterface *func) {
        if (!func || !_ha3FunctionProvider) {
            return false;
        }
        if (!func->beginRequest(_ha3FunctionProvider.get())) {
            return false;
        }
        return true;
    }

    bool callBeginRequest(suez::turing::FunctionInterface *func, HA3_NS(common)::Request *request) {
        if (!setRequest(request)) {
            return false;
        }
        return callBeginRequest(func);
    }

    bool setRequest(HA3_NS(common)::Request *request) {
        auto resource = _ha3FunctionProvider->getSearchPluginResource();
        resource->request = request;
        return true;
    }

    template<typename T>
    T* declareGlobalVariable(const std::string &variName,
                             bool needSerialize = false) {
        return _ha3FunctionProvider->getDataProvider()->declareGlobalVariable<T>(
                variName, needSerialize);
    }

public:
    HA3_NS(func_expression)::FunctionProviderPtr _ha3FunctionProvider;
    HA3_NS(common)::Ha3MatchDocAllocatorPtr _allocator;
    HA3_NS(common)::DataProviderPtr _dataProvider;
};

}}
