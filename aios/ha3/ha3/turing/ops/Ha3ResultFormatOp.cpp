#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <ha3/util/Log.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/common/XMLResultFormatter.h>
#include <ha3/qrs/MatchDocs2Hits.h>
#include <sstream>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(turing);

class Ha3ResultFormatOp : public tensorflow::OpKernel
{
public:
    explicit Ha3ResultFormatOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {     
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        SearcherSessionResource *searcherSessionResource = 
            dynamic_cast<SearcherSessionResource *>(sessionResource);
        SearcherQueryResource *searcherQueryResource = 
            dynamic_cast<SearcherQueryResource *>(queryResource);
        OP_REQUIRES(ctx, searcherSessionResource, 
                    errors::Unavailable("ha3 searcher session resource unavailable"));
        OP_REQUIRES(ctx, searcherQueryResource,
                    errors::Unavailable("ha3 searcher query resource unavailable"));
        HA3_NS(service)::SearcherResourcePtr searcherResource = searcherSessionResource->searcherResource;
        OP_REQUIRES(ctx, searcherResource, 
                    errors::Unavailable("ha3 searcher resource unavailable"));
        ConfigAdapterPtr configAdapter = searcherSessionResource->configAdapter;
        OP_REQUIRES(ctx, configAdapter, errors::Unavailable("ha3 config adapter unavailable"));


        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
 
        auto resultTensor = ctx->input(1).scalar<Variant>()();
        auto ha3ResultVariant = resultTensor.get<Ha3ResultVariant>();
        ResultPtr result = ha3ResultVariant->getResult();
        OP_REQUIRES(ctx, result, errors::Unavailable("ha3 result unavailable"));
        vector<string> clusterNames;
        if (!configAdapter->getClusterNames(clusterNames)) {
            OP_REQUIRES(ctx, false, errors::Unavailable("get cluster name failed."));
        }
        convertMatchDocsToHits(request, result, clusterNames);
        convertAggregateResults(request, result);
        XMLResultFormatter xmlResultFormatter;
        stringstream ss;
        xmlResultFormatter.format(result, ss);

        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {1}, &out));
        out->scalar<string>()() = ss.str();
    }

private:
    void convertMatchDocsToHits(const common::RequestPtr &requestPtr,
                                const ResultPtr &resultPtr,
                                const vector<string> &clusterNameVec)
    {
        if (!requestPtr || !resultPtr) {
            return;
        }

        MatchDocs2Hits converter;
        ErrorResult errResult;
        Hits *hits = converter.convert(resultPtr->getMatchDocs(),
                requestPtr, resultPtr->getSortExprMeta(),
                errResult, clusterNameVec);
        if (errResult.hasError()) {
            resultPtr->addErrorResult(errResult);
            resultPtr->clearMatchDocs();
            return ;
        }

        uint32_t actualMatchDocs = resultPtr->getActualMatchDocs();
        ConfigClause *configClause = requestPtr->getConfigClause();
        if (actualMatchDocs < configClause->getActualHitsLimit()) {
            HA3_LOG(DEBUG, "actual_hits_limit: %u, actual: %u, total: %u",
                    configClause->getActualHitsLimit(), actualMatchDocs,
                    resultPtr->getTotalMatchDocs());
            hits->setTotalHits(actualMatchDocs);
        } else {
            hits->setTotalHits(resultPtr->getTotalMatchDocs());
        }
        resultPtr->setHits(hits);
        resultPtr->clearMatchDocs();
    }

    void convertAggregateResults(const common::RequestPtr &requestPtr,
                                 const ResultPtr &resultPtr)
    {
        if (!requestPtr || !resultPtr) {
            return;
        }
        uint32_t aggResultCount = resultPtr->getAggregateResultCount();
        for (uint32_t i = 0; i < aggResultCount; ++i) {
            AggregateResultPtr aggResult = resultPtr->getAggregateResult(i);
            if (aggResult) {
                aggResult->constructGroupValueMap();
            }
        }
    }


private:
    HA3_LOG_DECLARE();

};

HA3_LOG_SETUP(turing, Ha3ResultFormatOp);

REGISTER_KERNEL_BUILDER(Name("Ha3ResultFormatOp")
                        .Device(DEVICE_CPU),
                        Ha3ResultFormatOp);

END_HA3_NAMESPACE(turing);
