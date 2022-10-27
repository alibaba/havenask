#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/search/DocCountLimits.h>
#include <ha3/common/ha3_op_common.h>
#include <autil/StringUtil.h>

using namespace tensorflow;
using namespace suez::turing;
using namespace autil;
using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(turing);

class Ha3HobbitPrepareOp : public tensorflow::OpKernel
{
public:
    explicit Ha3HobbitPrepareOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        OP_REQUIRES(ctx, queryResource,
                    errors::Unavailable("ha3 query resource unavailable"));

        // get request
        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        OP_REQUIRES(ctx, ha3RequestVariant, errors::Unavailable("get request variant failed"));

        auto matchdocsTensor = ctx->input(1).scalar<Variant>()();
        auto matchDocsVariant = matchdocsTensor.get<MatchDocsVariant>();
        OP_REQUIRES(ctx, matchDocsVariant, errors::Unavailable("get matchDocsVariant failed"));

        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("get request failed"));

        // prepare ConfigClause
        ConfigClause *configClause = request->getConfigClause();
        OP_REQUIRES(ctx, configClause, errors::Unavailable("get config clause failed"));

        // prepare KvPairVariant
        std::map<std::string, std::string> kvPair = configClause->getKVPairs();
        kvPair["s"] = StringUtil::toString(configClause->getStartOffset());
        kvPair["n"] = StringUtil::toString(configClause->getHitCount());
        if (DocCountLimits::determinScorerId(request.get()) == 1) {
            kvPair["HPS"] = suez::turing::BUILD_IN_REFERENCE_PREFIX + "score";
        }

        auto sortClause = request->getSortClause();
        if (sortClause) {
            auto ssDesc = sortClause->getSortDescription(1u);
            if (ssDesc) {
                string ssName = ssDesc->getOriginalString();
                kvPair["HSS"] = ssName;
                auto virtualAttributeClause = request->getVirtualAttributeClause();
                if (virtualAttributeClause) {
                    auto va = virtualAttributeClause->getVirtualAttribute(ssName);
                    if (va) {
                        string exprStr = va->getExprString();
                        matchDocsVariant->addAliasField(ssName, exprStr);
                    }
                }
            }
        }

        int32_t level = Tracer::convertLevel(configClause->getTrace());

        bool debug = level <= ISEARCH_TRACE_DEBUG ? true : false;
        int  debugLevel = 0;
        switch(level) {
            case ISEARCH_TRACE_ALL:
            case ISEARCH_TRACE_TRACE3: debugLevel = 3; break;
            case ISEARCH_TRACE_TRACE2: debugLevel = 2; break;
            case ISEARCH_TRACE_TRACE1: debugLevel = 1; break;
            default: debugLevel = 0;
        }
        kvPair["debug"] = debug ? "true" : "false";
        kvPair["debuglevel"] = StringUtil::toString(debugLevel);
        autil::mem_pool::Pool *pool = queryResource->getPool();
        KvPairVariant kvPairVariant(kvPair, pool);

        Tensor *kvPairTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, TensorShape({}), &kvPairTensor));
        kvPairTensor->scalar<Variant>()() = kvPairVariant;


        Tensor* matchDocs = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &matchDocs));
        matchDocs->scalar<Variant>()() = *matchDocsVariant;
    }
private:
    HA3_LOG_DECLARE();
};


HA3_LOG_SETUP(turing, Ha3HobbitPrepareOp);

REGISTER_KERNEL_BUILDER(Name("Ha3HobbitPrepareOp")
                        .Device(DEVICE_CPU),
                        Ha3HobbitPrepareOp);

END_HA3_NAMESPACE(truing);
