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

#include "matchdoc/MatchDocAllocator.h"

#include "ha3/common/AggregateResult.h"
#include "ha3/common/AttributeItem.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/Result.h"
#include "ha3/common/Tracer.h"
#include "ha3/sorter/SorterResource.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
namespace mem_pool {
class Pool;
template <typename T> class PoolVector;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace common {
class AggregateClause;
class DataProvider;
class Request;
}  // namespace common
namespace proxy {
class AggFunResultMerger;
}  // namespace proxy
namespace rank {
class ComparatorCreator;
}  // namespace rank
namespace search {
class SortExpressionCreator;
}  // namespace search
}  // namespace isearch
namespace matchdoc {
class MatchDoc;
}  // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpressionCreatorBase;
class SorterWrapper;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace proxy {

class Merger
{
public:
    Merger();
    ~Merger();
private:
    static void mergeResult(common::ResultPtr &outputResult,
                            const std::vector<common::ResultPtr> &inputResults,
                            const common::Request* request,
                            suez::turing::SorterWrapper *sorterWrapper,
                            sorter::SorterLocation sl);
public:
    // inputResult.size() must > 1
    static void mergeResults(common::Result *outputResult,
                             const std::vector<common::ResultPtr> &inputResults,
                             const common::Request* request,
                             suez::turing::SorterWrapper *sorterWrapper,
                             sorter::SorterLocation sl);
    static uint32_t mergeSummary(common::Result *outputResult,            
                                 const common::ResultPtrVector &inputResults,
                                 bool allowLackOfSummary);
    static void mergeErrors(common::Result *outputResult, 
                            const std::vector<common::ResultPtr> &inputResults);
    static void mergeTracer(common::Tracer* tracer,
                            const common::ResultPtr &resultPtr);
    static void mergeTracer(const common::Request *request,
                            common::Result *outputResult,
                            const std::vector<common::ResultPtr> &inputResults);
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
    static common::ResultPtr findFirstNoneEmptyResult(
            const std::vector<common::ResultPtr> &results,
            uint32_t &allMatchDocsSize);

    static void fillSorterResourceInfo(
            sorter::SorterResource& sorterResource, 
            const common::Request *request, 
            uint32_t &topK,
            suez::turing::AttributeExpressionCreatorBase *exprCreator,
            search::SortExpressionCreator *sortExprCreator,
            const common::Ha3MatchDocAllocatorPtr &allocator,
            common::DataProvider *dataProvider,
            rank::ComparatorCreator *compCreator,
            const std::vector<common::AttributeItemMapPtr> *globalVariables,
            autil::mem_pool::Pool *pool, uint32_t resultSourceNum, 
            sorter::SorterLocation sl, common::Tracer *requestTracer);

    // match doc related
    static void mergeMatchDocs(
            common::Result *outputResult,
            const std::vector<common::ResultPtr> &inputResults,
            const common::Request *request,
            suez::turing::SorterWrapper *sorterWrapper,
            bool doDedup, sorter::SorterLocation sl);

    static void mergePhaseOneSearchInfos(common::Result *outputResult, 
            const std::vector<common::ResultPtr> &inputResults);
    static void mergePhaseTwoSearchInfos(common::Result *outputResult,
            const std::vector<common::ResultPtr> &inputResults);

    static Merger::MergeMatchDocResultMeta doMergeMatchDocsMeta(
            const std::vector<common::ResultPtr> &inputResults,
            bool doDedup, const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
            autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect,
            sorter::SorterLocation sl);
public:
    // agg result related
    static void mergeAggResults(common::AggregateResults &aggregateResults,
                  const std::vector<common::AggregateResults> &inputResults,
                  const common::AggregateClause *aggClause,
                  autil::mem_pool::Pool *pool);

private:
    static common::AggregateResultPtr mergeOneAggregateResult(
            const std::vector<common::AggregateResults>& results,
            size_t aggOffset,int32_t maxGroupCount,
            const std::string &groupKeyExpr, autil::mem_pool::Pool* pool);
    static AggFunResultMerger* createAggFunResultMerger(
            const std::vector<std::string> &funNames, 
            const matchdoc::MatchDocAllocatorPtr &allocatorPtr);
    static bool checkAggResults(
            const common::AggregateClause *aggClause,
            const std::vector<common::AggregateResults> &results);
    static bool checkAggResult(
            const common::AggregateResults &aggregateResults,
            size_t expectSize);

    // hits related
    static void mergeHits(
            common::Result *outputResult,
            const std::vector<common::ResultPtr> &inputResults,
            bool doDedup);

    // range related
    static void mergeCoveredRanges(
            common::Result *outputResult,
            const std::vector<common::ResultPtr> &inputResults);

    // global variable related
    static void mergeGlobalVariables(
            common::Result *outputResult,
            const std::vector<common::ResultPtr> &inputResults);
private:
    friend class MergerTest;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace proxy
} // namespace isearch

