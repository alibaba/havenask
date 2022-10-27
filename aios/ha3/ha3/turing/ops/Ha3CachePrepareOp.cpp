#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/search/SearcherCacheInfo.h>
#include <ha3/service/SearcherResource.h>
#include <ha3/search/SearcherCacheManager.h>
#include <ha3/search/DocCountLimits.h>
#include <ha3/rank/RankProfile.h>
#include <indexlib/partition/index_partition.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/common/Tracer.h>
#include <autil/mem_pool/Pool.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(turing);

class Ha3CachePrepareOp : public tensorflow::OpKernel
{
public:
    explicit Ha3CachePrepareOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
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

        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

        SearcherCache *searcherCache = searcherResource->getSearcherCache().get();
        auto partitionResource = searcherQueryResource->partitionResource;
        OP_REQUIRES(ctx, partitionResource, errors::Unavailable("ha3 partition resource unavailable"));

        auto commonResource = searcherQueryResource->commonResource;
        OP_REQUIRES(ctx, commonResource, errors::Unavailable("ha3 common resource unavailable"));
        SearcherCacheManagerPtr searcherCacheManager(
                new SearcherCacheManager(searcherCache,
                        partitionResource->indexPartitionReaderWrapper,
                        commonResource->pool,
                        commonResource->tableInfo->hasSubSchema(),
                        *commonResource,
                        *partitionResource));
        OP_REQUIRES(ctx, searcherCacheManager, errors::Unavailable("searcher cache manager create failed."));

        DocCountLimits docCountLimits;
        auto rankProfileMgr = searcherResource->getRankProfileManager();
        auto clusterConfigInfo = searcherResource->getClusterConfig();
        const RankProfile *rankProfile = NULL;
        if (!rankProfileMgr->getRankProfile(request.get(), rankProfile, commonResource->errorResult)) {
            OP_REQUIRES(ctx, false, errors::Unavailable("get rank profile failed.."));
        }
        auto partCount = searcherResource->getPartCount();
        docCountLimits.init(request.get(), rankProfile, clusterConfigInfo, partCount, commonResource->tracer);

        if (!searcherCacheManager->initCacheBeforeOptimize(request.get(),
                        commonResource->sessionMetricsCollector,
                        commonResource->errorResult.get(),
                        docCountLimits.requiredTopK)){
            OP_REQUIRES(ctx, false, errors::Unavailable("cache manager init cache before optimize failed."));
        }

        bool useCache = searcherCacheManager->useCache(request.get());
        if(!useCache) {
            return;
        }

        SearcherCacheInfoPtr searcherCacheInfo(new SearcherCacheInfo());
        searcherCacheInfo->cacheManager = searcherCacheManager;
        searcherCacheInfo->docCountLimits = docCountLimits;
        searcherCacheInfo->isHit = searcherCacheManager->cacheHit();
        searcherQueryResource->searcherCacheInfo = searcherCacheInfo;
    }

private:
    HA3_LOG_DECLARE();

};

HA3_LOG_SETUP(turing, Ha3CachePrepareOp);

REGISTER_KERNEL_BUILDER(Name("Ha3CachePrepareOp")
                        .Device(DEVICE_CPU),
                        Ha3CachePrepareOp);

END_HA3_NAMESPACE(turing);
