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
#include "ha3/turing/ops/Ha3SeekAndJoinOp.h"

#include <iostream>
#include <memory>
#include <unordered_map>

#include "alog/Logger.h"
#include "turing_ops_util/util/OpUtil.h"
#include "turing_ops_util/variant/MatchDocsVariant.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/common.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorDocStorage.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "suez/turing/common/PoolConfig.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "table/Row.h"

#include "ha3/common/AttributeItem.h"
#include "ha3/search/HashJoinInfo.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/rank/RankProfileManager.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/MatchDocSearcher.h"
#include "ha3/search/OptimizerChainManager.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SearcherCacheInfo.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/turing/ops/Ha3SeekOp.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "autil/HashUtil.h"
#include "autil/Log.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace rank {
class RankProfile;
}  // namespace rank
}  // namespace isearch

using namespace std;
using namespace table;
using namespace tensorflow;
using namespace suez::turing;
using namespace matchdoc;
using namespace autil;
using namespace isearch::rank;
using namespace isearch::service;
using namespace isearch::common;
using namespace indexlib::index;
using namespace isearch::search;
using namespace isearch::config;
namespace isearch {
namespace turing {

Ha3SeekAndJoinOp::Ha3SeekAndJoinOp(tensorflow::OpKernelConstruction *ctx)
    : tensorflow::OpKernel(ctx)
{
    auto sessionResource = GET_SESSION_RESOURCE();
    SearcherSessionResource *searcherSessionResource =
        dynamic_cast<SearcherSessionResource *>(sessionResource);
    OP_REQUIRES(ctx,
        searcherSessionResource,
        errors::Unavailable("ha3 searcher session resource unavailable"));
    SearcherResourcePtr searcherResource =
        searcherSessionResource->searcherResource;
    const auto &joinConfig =
        searcherResource->getClusterConfig().getJoinConfig();
    _auxTableName = joinConfig.getScanJoinCluster();
    OP_REQUIRES(ctx,
        !_auxTableName.empty(),
        errors::Unavailable("empty scan join cluster name"));
    const auto &joinInfos = joinConfig.getJoinInfos();
    for (const auto &ji : joinInfos) {
        if (_auxTableName == ji.getJoinCluster()) {
	    _joinFieldName = ji.getJoinField();
            AUTIL_LOG(INFO,
                "get join cluster:  [%s] - [%s]",
                _auxTableName.c_str(),
                _joinFieldName.c_str());
            break;
        }
    }
    if (_joinFieldName.empty()) {
        OP_REQUIRES(ctx,
            false,
            errors::Unavailable(
                "scan join cluster name not in join table info"));
    }
}

void Ha3SeekAndJoinOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    SearcherSessionResource *searcherSessionResource = dynamic_cast<SearcherSessionResource *>(sessionResource);
    SearcherQueryResource *searcherQueryResource =  dynamic_cast<SearcherQueryResource *>(queryResource);
    OP_REQUIRES(ctx, searcherSessionResource,
                      errors::Unavailable("ha3 searcher session resource unavailable"));
    OP_REQUIRES(ctx, searcherQueryResource,
                      errors::Unavailable("ha3 searcher query resource unavailable"));
    isearch::service::SearcherResource *searcherResource =
        searcherSessionResource->searcherResource.get();
    OP_REQUIRES(ctx, searcherResource,
                      errors::Unavailable("ha3 searcher resource unavailable"));
    auto pool = searcherQueryResource->getPool();
    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
    Request *request = ha3RequestVariant->getRequest().get();
    OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

    REQUEST_TRACE_WITH_TRACER(queryResource->getTracer(), TRACE3, "begin ha3 seek and join op");

    auto rankProfileMgr = searcherResource->getRankProfileManager().get();
    const rank::RankProfile *rankProfile = nullptr;
    if (!rankProfileMgr->getRankProfile(request,
                    rankProfile, searcherQueryResource->commonResource->errorResult))
    {
        OP_REQUIRES(ctx, false, errors::Unavailable("get rank profile failed."));
    }

    HashJoinInfoPtr hashJoinInfo;
    auto auxOutputVariant = ctx->input(1).scalar<Variant>()();
    auto *auxMatchDocsVariant = auxOutputVariant.get<MatchDocsVariant>();
    MatchDocAllocatorPtr auxMatchDocAllocator = auxMatchDocsVariant->getAllocator();
    if (auxMatchDocAllocator) {
        REQUEST_TRACE_WITH_TRACER(
                queryResource->getTracer(), TRACE3, "begin build join hash info");

        hashJoinInfo = buildJoinHashInfo(auxMatchDocAllocator.get(),
                auxMatchDocsVariant->getMatchDocs());
        OP_REQUIRES(ctx, hashJoinInfo,
                errors::Unavailable("build hash join info failed."));
    }
    seek(ctx, request, rankProfile, pool, searcherSessionResource,  searcherQueryResource,
         searcherResource, hashJoinInfo.get());
}

HashJoinInfoPtr Ha3SeekAndJoinOp::buildJoinHashInfo(
        const MatchDocAllocator *matchDocAllocator,
        const vector<MatchDoc> &matchDocs)
{
    HashJoinInfoPtr hashJoinInfo(
            new HashJoinInfo(_auxTableName, _joinFieldName));
    ReferenceBase *reference =
            matchDocAllocator->findReferenceWithoutType(_joinFieldName);
    if (!reference) {
	return hashJoinInfo;
    }
    matchdoc::ValueType vt = reference->getValueType();
    if (vt.isMultiValue()) {
	return {};
    }
    auto &hashJoinMap = hashJoinInfo->getHashJoinMap();
    switch (vt.getBuiltinType()) {

#define CASE_MACRO(ft)                                                         \
    case ft: {                                                                 \
        typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;             \
        auto *ref = dynamic_cast<Reference<T> *>(reference);                   \
        if (!ref) {                                                            \
            AUTIL_LOG(WARN, "reference ValueType and dynamic type not matched"); \
            return {};                                                         \
        }                                                                      \
        for (auto doc : matchDocs) {                                           \
            const T &val = ref->get(doc);                                      \
            auto hashKey = HashUtil::calculateHashValue(val);                 \
            hashJoinMap[hashKey].emplace_back(doc.getDocId());             \
        }                                                                      \
        break;                                                                 \
    }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        AUTIL_LOG(ERROR, "impossible reach this branch");
        return {};
    }

    }
    return hashJoinInfo;
}

void Ha3SeekAndJoinOp::seek(tensorflow::OpKernelContext* ctx,
                            common::Request *request,
                            const rank::RankProfile *rankProfile,
                            autil::mem_pool::Pool *pool,
                            SearcherSessionResource *searcherSessionResource,
                            SearcherQueryResource *searcherQueryResource,
                            service::SearcherResource *searcherResource,
                            search::HashJoinInfo *hashJoinInfo)
{
    search::InnerSearchResult innerResult(pool);
    bool ret = processRequest(request, rankProfile, searcherQueryResource, searcherResource, hashJoinInfo, innerResult);
    Ha3SeekOp::outputResult(ctx, ret, innerResult, request, pool,
                            searcherSessionResource, searcherQueryResource, searcherResource);
}

bool Ha3SeekAndJoinOp::processRequest(common::Request *request,
                                      const rank::RankProfile *rankProfile,
                                      SearcherQueryResource *searcherQueryResource,
                                      service::SearcherResource *searcherResource,
                                      search::HashJoinInfo *hashJoinInfo,
                                      search::InnerSearchResult &innerResult)
{
    auto &commonResource = searcherQueryResource->commonResource;
    auto &partitionResource = searcherQueryResource->partitionResource;
    auto &runtimeResource = searcherQueryResource->runtimeResource;
    if (!commonResource || !partitionResource || !runtimeResource) {
        return false;
    }
    auto rankProfileMgr = searcherResource->getRankProfileManager().get();
    auto optimizerChainManager = searcherResource->getOptimizerChainManager().get();
    auto sorterManager = searcherResource->getSorterManager().get();
    auto searcherCache = searcherResource->getSearcherCache().get();
    search::MatchDocSearcher searcher(*commonResource, *partitionResource, *runtimeResource,
            rankProfileMgr, optimizerChainManager, sorterManager,
            searcherResource->getAggSamplerConfigInfo(), searcherResource->getClusterConfig(),
            searcherCache, rankProfile,
            search::LayerMetasPtr(), searcherQueryResource->ha3BizMeta);

    auto searcherCacheInfo = searcherQueryResource->searcherCacheInfo.get();
    if (hashJoinInfo) {
        REQUEST_TRACE_WITH_TRACER(searcherQueryResource->getTracer(),
                TRACE3,
                "with hash join info, begin seek and join");
        return searcher.seekAndJoin(
                request, searcherCacheInfo, hashJoinInfo, innerResult);
    } else {
        REQUEST_TRACE_WITH_TRACER(searcherQueryResource->getTracer(),
                TRACE3,
                "no hash join info, begin seek");
        return searcher.seek(request, searcherCacheInfo, innerResult);
    }
}

AUTIL_LOG_SETUP(ha3, Ha3SeekAndJoinOp);

REGISTER_KERNEL_BUILDER(Name("Ha3SeekAndJoinOp")
                        .Device(DEVICE_CPU),
                        Ha3SeekAndJoinOp);

} // namespace turing
} // namespace isearch
