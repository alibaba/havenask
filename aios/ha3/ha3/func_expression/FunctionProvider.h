#ifndef ISEARCH_FUNCTIONPROVIDER_H
#define ISEARCH_FUNCTIONPROVIDER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/ProviderBase.h>
#include <ha3/common/Tracer.h>
#include <suez/turing/expression/provider/FunctionProvider.h>

BEGIN_HA3_NAMESPACE(func_expression);

typedef search::SearchPluginResource FunctionResource;

class FunctionProvider : public suez::turing::FunctionProvider,
                         public search::ProviderBase
{
public:
    FunctionProvider(const FunctionResource &resource);
    ~FunctionProvider();
    void setQueryTerminator(common::QueryTerminator *queryTerminator) {
        _resource.queryTerminator = queryTerminator;
    }
    void setRequestTracer(common::Tracer *tracer) {
        _resource.requestTracer = tracer;
        suez::turing::ProviderBase::setRequestTracer(tracer);
    }
private:
    FunctionProvider(const FunctionProvider &);
    FunctionProvider& operator=(const FunctionProvider &);
private:
    search::SearchPluginResource _resource;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FunctionProvider);

END_HA3_NAMESPACE(func_expression);

#endif //ISEARCH_FUNCTIONPROVIDER_H
