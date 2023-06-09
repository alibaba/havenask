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
#pragma once
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/AggregateResult.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/Result.h"
#include "ha3/proxy/AggFunResultMerger.h"
#include "matchdoc/MatchDocAllocator.h"
#include "tensorflow/core/framework/op_kernel.h"

namespace autil {
namespace mem_pool {
class Pool;
template <typename T> class PoolVector;
}  // namespace mem_pool
}  // namespace autil
namespace matchdoc {
class MatchDoc;
}  // namespace matchdoc

namespace isearch {
namespace common {
class AggregateClause;
class Request;
}  // namespace common
namespace proxy {
class AggFunResultMerger;
}  // namespace proxy

namespace turing {

class Ha3ResultMergeOp : public tensorflow::OpKernel
{
public:
    explicit Ha3ResultMergeOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
public:
    friend class Ha3ResultMergeOpTest;

private:
    struct MergeMatchDocResultMeta {
    public:
        MergeMatchDocResultMeta()
            : errorCode(ERROR_NONE)
            , actualMatchDocs(0)
            , totalMatchDocs(0)
        {
        }
    public:
        ErrorCode errorCode;
        uint32_t actualMatchDocs;
        uint32_t totalMatchDocs;
    };

private:
    void mergeResults(common::Result *outputResult,
                      const std::vector<common::ResultPtr> &inputResults,
                      const common::Request* request);

    void mergeErrors(common::Result *outputResult,
                     const std::vector<common::ResultPtr> &inputResults);

    void mergeTracer(const common::Request *request,
                     common::Result *outputResult,
                     const std::vector<common::ResultPtr> &inputResults);

    proxy::AggFunResultMerger *createAggFunResultMerger(
            const std::vector<std::string> &funNames,
            const matchdoc::MatchDocAllocatorPtr &allocatorPtr);

    common::AggregateResultPtr mergeOneAggregateResult(
            const std::vector<common::ResultPtr>& results, size_t aggOffset,
            const std::string& groupExprStr,
            autil::mem_pool::Pool* pool);

    bool checkAggResults(const common::AggregateClause *aggClause,
                         const std::vector<common::ResultPtr>& results);

    bool checkAggResult(const common::ResultPtr &resultPtr,
                        size_t expectSize);

    void mergeAggResults(common::Result *outputResult,
                         const std::vector<common::ResultPtr>& inputResults,
                         const common::Request *request);

    void mergeCoveredRanges(common::Result *outputResult,
                            const std::vector<common::ResultPtr> &inputResults);

    void mergeGlobalVariables(common::Result *outputResult,
                              const std::vector<common::ResultPtr> &inputResults);

    common::ResultPtr findFirstNoneEmptyResult(const std::vector<common::ResultPtr>& results,
            uint32_t &allMatchDocsSize);

    MergeMatchDocResultMeta doMergeMatchDocsMeta(
            const std::vector<common::ResultPtr> &inputResults,
            bool doDedup, const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
            autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect);

    void mergeMatchDocs(common::Result *outputResult,
                        const std::vector<common::ResultPtr> &inputResults,
                        const common::Request *request,
                        bool doDedup);

    void mergePhaseOneSearchInfos(common::Result *outputResult,
                                  const std::vector<common::ResultPtr> &inputResults);

    void mergeHits(common::Result *outputResult,
                   const std::vector<common::ResultPtr> &inputResults,
                   bool doDedup);

    void mergePhaseTwoSearchInfos(common::Result *outputResult,
                                  const std::vector<common::ResultPtr> &inputResults);

    bool judgeHasMatchDoc(const common::ResultPtr &outputResult);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch
