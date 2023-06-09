/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <stddef.h>
#include <iostream>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "turing_ops_util/util/OpUtil.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/common.h"
#include "suez/turing/common/PoolConfig.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"

#include "ha3/common/AttributeItem.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/rank/RankProfileManager.h"
#include "ha3/search/DocCountLimits.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SearcherCacheInfo.h"
#include "ha3/search/SearcherCacheManager.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace rank {
class RankProfile;
}  // namespace rank
namespace search {
class SearcherCache;
}  // namespace search
}  // namespace isearch

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::rank;
using namespace isearch::service;
using namespace isearch::common;
using namespace indexlib::index;
using namespace isearch::search;
using namespace isearch::config;
namespace isearch {
namespace turing {

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
        isearch::service::SearcherResourcePtr searcherResource =
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
        auto partCount = searcherQueryResource->totalPartCount;
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
    AUTIL_LOG_DECLARE();

};

AUTIL_LOG_SETUP(ha3, Ha3CachePrepareOp);

REGISTER_KERNEL_BUILDER(Name("Ha3CachePrepareOp")
                        .Device(DEVICE_CPU),
                        Ha3CachePrepareOp);

} // namespace turing
} // namespace isearch
