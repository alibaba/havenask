#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/service/SearcherResource.h>
#include <indexlib/partition/index_partition.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(func_expression);

class Ha3FillGlobalIdInfoOp : public tensorflow::OpKernel
{
public:
    explicit Ha3FillGlobalIdInfoOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
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
        HA3_NS(service)::SearcherResourcePtr searcherResource =
            searcherSessionResource->searcherResource;
        OP_REQUIRES(ctx, searcherResource,
                    errors::Unavailable("ha3 searcher resource unavailable"));


        uint32_t ip = searcherSessionResource->getIp();
        IndexPartitionReaderWrapperPtr idxPartReaderWrapperPtr
            = searcherQueryResource->indexPartitionReaderWrapper;

        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

        auto resultTensor = ctx->input(1).scalar<Variant>()();
        auto ha3ResultVariant = resultTensor.get<Ha3ResultVariant>();
        ResultPtr result = ha3ResultVariant->getResult();
        OP_REQUIRES(ctx, result, errors::Unavailable("ha3 result unavailable"));


        MatchDocs *matchDocs = result->getMatchDocs();
        if (matchDocs) {
            auto phaseOneInfoMask = request->getConfigClause()->getPhaseOneBasicInfoMask();
            versionid_t versionId = idxPartReaderWrapperPtr->getCurrentVersion();
            matchDocs->setGlobalIdInfo(searcherResource->getHashIdRange().from(),
                    versionId, searcherResource->getFullIndexVersion(), ip, phaseOneInfoMask);
        }
        ctx->set_output(0, ctx->input(1));
    }
private:
    HA3_LOG_DECLARE();

};

HA3_LOG_SETUP(turing, Ha3FillGlobalIdInfoOp);

REGISTER_KERNEL_BUILDER(Name("Ha3FillGlobalIdInfoOp")
                        .Device(DEVICE_CPU),
                        Ha3FillGlobalIdInfoOp);

END_HA3_NAMESPACE(turing);
