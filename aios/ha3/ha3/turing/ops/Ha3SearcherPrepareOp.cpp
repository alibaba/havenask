#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/search/MatchDocSearcher.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/service/SearcherResource.h>
#include <indexlib/partition/index_partition.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/common/Tracer.h>
#include <ha3/common/TimeoutTerminator.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/turing/ops/Ha3ResourceUtil.h>
#include <ha3/turing/ops/RequestUtil.h>

using namespace std;
using namespace tensorflow;

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(func_expression);
BEGIN_HA3_NAMESPACE(turing);

class Ha3SearcherPrepareOp : public tensorflow::OpKernel
{
public:
    explicit Ha3SearcherPrepareOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();

        SearcherSessionResource *searcherSessionResource =
            dynamic_cast<SearcherSessionResource *>(sessionResource);
        OP_REQUIRES(ctx, searcherSessionResource,
                    errors::Unavailable("ha3 searcher session resource unavailable"));

        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        SearcherQueryResource *searcherQueryResource =
            dynamic_cast<SearcherQueryResource *>(queryResource);

        OP_REQUIRES(ctx, searcherQueryResource,
                    errors::Unavailable("ha3 searcher query resource unavailable"));
        HA3_NS(service)::SearcherResourcePtr searcherResource =
            searcherSessionResource->searcherResource;
        OP_REQUIRES(ctx, searcherResource,
                    errors::Unavailable("ha3 searcher resource unavailable"));

        auto pool = searcherQueryResource->getPool();
        auto metricsCollector = searcherQueryResource->sessionMetricsCollector;
        IndexPartitionReaderWrapperPtr idxPartReaderWrapperPtr
            = searcherQueryResource->indexPartitionReaderWrapper;
        if (idxPartReaderWrapperPtr == NULL) {
            OP_REQUIRES(ctx, idxPartReaderWrapperPtr,
                        errors::Unavailable("ha3 index partition reader wrapper unavailable"));
        }
        auto partitionReaderSnapshot = searcherQueryResource->getIndexSnapshotPtr();
        OP_REQUIRES(ctx, partitionReaderSnapshot,
                    errors::Unavailable("partitionReaderSnapshot unavailable"));
        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

        OP_REQUIRES(ctx, RequestUtil::defaultSorterBeginRequest(request),
                    errors::Unavailable("defaultSorter BeginReuest fail"));

        SearchCommonResourcePtr commonResource =
            Ha3ResourceUtil::createSearchCommonResource(
                    request.get(), pool, metricsCollector,
                    searcherQueryResource->getSeekTimeoutTerminator(),
                    searcherQueryResource->getTracer(), searcherResource,
                    searcherQueryResource->getCavaAllocator(),
                    searcherQueryResource->getCavaJitModules());
        OP_REQUIRES(ctx, commonResource, errors::Unavailable("create common resource failed"));

        SearchPartitionResourcePtr partitionResource =
            Ha3ResourceUtil::createSearchPartitionResource(request.get(),
                    idxPartReaderWrapperPtr, partitionReaderSnapshot,
                    commonResource);
        OP_REQUIRES(ctx, partitionResource, errors::Unavailable("create partition resource failed"));


        SearchRuntimeResourcePtr runtimeResource =
            Ha3ResourceUtil::createSearchRuntimeResource(request.get(),
                searcherResource, commonResource,
                partitionResource->attributeExpressionCreator.get());

        searcherQueryResource->commonResource = commonResource;
        searcherQueryResource->partitionResource = partitionResource;
        searcherQueryResource->runtimeResource = runtimeResource;
        OP_REQUIRES(ctx, runtimeResource, errors::Unavailable("create runtime resource failed"));
    }

private:
    HA3_LOG_DECLARE();

};

HA3_LOG_SETUP(turing, Ha3SearcherPrepareOp);

REGISTER_KERNEL_BUILDER(Name("Ha3SearcherPrepareOp")
                        .Device(DEVICE_CPU),
                        Ha3SearcherPrepareOp);

END_HA3_NAMESPACE(turing);
