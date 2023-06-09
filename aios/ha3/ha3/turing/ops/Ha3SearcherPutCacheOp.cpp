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
#include <assert.h>
#include <stdint.h>
#include <cstddef>
#include <memory>
#include <memory>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "turing_ops_util/util/OpUtil.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/index_partition_reader.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/SubDocAccessor.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"

#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/common/SearcherCacheClause.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/search/CacheMinScoreFilter.h"
#include "ha3/search/CacheResult.h"
#include "ha3/search/DefaultSearcherCacheStrategy.h"
#include "ha3/search/DocCountLimits.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearcherCache.h"
#include "ha3/search/SearcherCacheInfo.h"
#include "ha3/search/SearcherCacheManager.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

using namespace std;
using namespace tensorflow;
using namespace autil::mem_pool;

using namespace isearch::monitor;
using namespace isearch::search;
using namespace isearch::common;
namespace isearch {
namespace turing {

class Ha3SearcherPutCacheOp : public tensorflow::OpKernel
{
public:
    explicit Ha3SearcherPutCacheOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);

        SearcherQueryResource *searcherQueryResource = dynamic_cast<SearcherQueryResource *>(queryResource);
        OP_REQUIRES(ctx, searcherQueryResource, errors::Unavailable("ha3 searcher query resource unavailable"));

        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

        auto resultTensor = ctx->input(1).scalar<Variant>()();
        auto ha3ResultVariant = resultTensor.get<Ha3ResultVariant>();
        ResultPtr result = ha3ResultVariant->getResult();
        OP_REQUIRES(ctx, result, errors::Unavailable("ha3 result unavailable"));

        auto cacheInfo = searcherQueryResource->searcherCacheInfo;
        OP_REQUIRES(ctx, cacheInfo, errors::Unavailable("ha3 cache info unavailable"));

        auto cacheManager = cacheInfo->cacheManager;
        if (needPut(cacheManager, cacheInfo->beginTime)) {
            SessionMetricsCollector *collector = cacheManager->getMetricsCollector();
            result->setUseTruncateOptimizer(collector->useTruncateOptimizer());
            CacheResultPtr cacheResult(constructCacheResult(result.get(), cacheInfo->scores,
                        cacheManager->getIndexPartReaderWrapper()));
            OP_REQUIRES(ctx, cacheResult, errors::Unavailable("construct cache result failed."));
            cacheResult->setTruncated(cacheInfo->isTruncated);
            cacheResult->setUseTruncateOptimizer(collector->useTruncateOptimizer());
            SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
            uint64_t key = cacheClause->getKey();
            auto partRange = cacheClause->getPartRange();
            uint32_t curTime = cacheClause->getCurrentTime();
            auto searcherCache = cacheManager->getSearcherCache();
            const DefaultSearcherCacheStrategyPtr& cacheStrategy =
                cacheManager->getCacheStrategy();
            bool ret = searcherCache->put(key, partRange, curTime, cacheStrategy,
                    cacheResult.get(), request->getPool());
            if (ret) {
                collector->increaseCachePutNum();
                uint64_t memUse;
                uint32_t itemNum;
                searcherCache->getCacheStat(memUse, itemNum);
                collector->setCacheMemUse(memUse);
                collector->setCacheItemNum(itemNum);
            }
            cacheResult->stealResult();
            truncateResult(cacheInfo->docCountLimits.requiredTopK, result.get());
        }

        Ha3ResultVariant ha3ResultOut(result, searcherQueryResource->getPool());
        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &out));
        out->scalar<Variant>()() = ha3ResultOut;
    }
private:
    static void truncateResult(uint32_t requireTopK,
                               common::Result *result) {
        common::MatchDocs *matchDocs = result->getMatchDocs();
        std::vector<matchdoc::MatchDoc> &matchDocVect =
            matchDocs->getMatchDocsVect();
        const auto &matchDocAllocator = matchDocs->getMatchDocAllocator();
        releaseRedundantMatchDocs(matchDocVect,
                requireTopK, matchDocAllocator);
    }

    template <typename T>
    static void releaseRedundantMatchDocs(T &matchDocVect,
            uint32_t requiredTopK, const common::Ha3MatchDocAllocatorPtr &matchDocAllocator)
    {
        uint32_t size = (uint32_t)matchDocVect.size();
        while(size > requiredTopK)
        {
            auto matchDoc = matchDocVect[--size];
            matchDocAllocator->deallocate(matchDoc);
        }
        matchDocVect.erase(matchDocVect.begin() + size, matchDocVect.end());
    }

    CacheResult *constructCacheResult(common::Result *result,
                                      const vector<score_t> &minScores,
                                      const IndexPartitionReaderWrapperPtr &idxPartReaderWrapper) {
        CacheResult* cacheResult = new CacheResult;
        CacheHeader* header = cacheResult->getHeader();
        header->minScoreFilter = CacheMinScoreFilter(minScores);

        cacheResult->setResult(result);

        const shared_ptr<PartitionInfoWrapper> &partInfoPtr =
            idxPartReaderWrapper->getPartitionInfo();
        const indexlib::index::PartitionInfoHint &partInfoHint = partInfoPtr->GetPartitionInfoHint();
        cacheResult->setPartInfoHint(partInfoHint);

        vector<globalid_t> &gids = cacheResult->getGids();
        fillGids(result, partInfoPtr, gids);

        MatchDocs *matchDocs = result->getMatchDocs();
        if (matchDocs) {
            const auto &matchDocAllocator = matchDocs->getMatchDocAllocator();
            if (matchDocAllocator && matchDocAllocator->hasSubDocAllocator()) {
                const indexlib::index::PartitionInfoPtr &subPartitionInfoPtr =
                    idxPartReaderWrapper->getSubPartitionInfo();
                vector<globalid_t> &subDocGids = cacheResult->getSubDocGids();
                fillSubDocGids(result, subPartitionInfoPtr, subDocGids);
            }
        }
        return cacheResult;
    }

    void fillGids(const common::Result *result,
                  const shared_ptr<PartitionInfoWrapper> &partInfoPtr,
                  vector<globalid_t> &gids) {
        gids.clear();

        MatchDocs *matchDocs = result->getMatchDocs();
        if (!matchDocs || !partInfoPtr.get()) {
            return;
        }

        uint32_t size = matchDocs->size();
        gids.reserve(size);
        for (uint32_t i = 0; i < size; i++) {
            docid_t docId = matchDocs->getDocId(i);
            globalid_t gid = partInfoPtr->GetGlobalId(docId);
            gids.push_back(gid);
        }
    }

    void fillSubDocGids(const common::Result *result,
                        const indexlib::index::PartitionInfoPtr &partInfoPtr,
                        vector<globalid_t> &subDocGids) {
        subDocGids.clear();
        if (!partInfoPtr.get()) {
            return;
        }
        MatchDocs *matchDocs = result->getMatchDocs();
        assert(matchDocs);
        const auto &matchDocAllocator = matchDocs->getMatchDocAllocator();
        assert(matchDocAllocator);
        auto accessor = matchDocAllocator->getSubDocAccessor();
        assert(accessor != NULL);
        uint32_t size = matchDocs->size();
        for (uint32_t i = 0; i < size; i++) {
            auto matchDoc = matchDocs->getMatchDoc(i);
            vector<int32_t> subDocids;
            accessor->getSubDocIds(matchDoc, subDocids);
            for (auto id : subDocids) {
                subDocGids.push_back(partInfoPtr->GetGlobalId(id));
            }
        }
    }

    bool needPut(const SearcherCacheManagerPtr &cacheManager, uint64_t beginTime) {
        if (!cacheManager) {
            return false;
        }
        auto searcherCache = cacheManager->getSearcherCache();
        uint32_t latencyLimit = searcherCache->getCacheLatencyLimit();
        uint64_t latency = autil::TimeUtility::currentTime() - beginTime;
        return latency > (uint64_t)latencyLimit;
    }
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(ha3, Ha3SearcherPutCacheOp);

REGISTER_KERNEL_BUILDER(Name("Ha3SearcherPutCacheOp")
                        .Device(DEVICE_CPU),
                        Ha3SearcherPutCacheOp);

} // namespace turing
} // namespace isearch
