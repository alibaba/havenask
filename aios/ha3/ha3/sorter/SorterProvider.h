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
#include <utility>
#include <vector>

#include "autil/mem_pool/PoolVector.h"
#include "matchdoc/MatchDocAllocator.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/ExpressionEvaluator.h"
#include "suez/turing/expression/provider/ProviderBase.h"
#include "suez/turing/expression/provider/SorterProvider.h"

#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/SortExprMeta.h"
#include "ha3/isearch.h"
#include "ha3/search/ProviderBase.h"
#include "ha3/search/SortExpression.h"
#include "ha3/sorter/SorterResource.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace rank {
class ComboComparator;
}  // namespace rank
}  // namespace isearch
namespace matchdoc {
class MatchDoc;
class ReferenceBase;
template <typename T> class Reference;
}  // namespace matchdoc
namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace sorter {

typedef std::pair<const matchdoc::ReferenceBase *, bool> ComparatorItem;
typedef std::vector<ComparatorItem> ComparatorItemVec;

class SorterProvider : public suez::turing::SorterProvider,
                       public search::ProviderBase
{
public:
    SorterProvider(const SorterResource &sorterResource);
    ~SorterProvider();
private:
    SorterProvider(const SorterProvider & );
    SorterProvider& operator=(const SorterProvider &);
public:
    using suez::turing::ProviderBase::requireAttributeWithoutType;
    using suez::turing::SorterProvider::requireAttributeWithoutType;
    using suez::turing::SorterProvider::requireAttribute;
    using suez::turing::ProviderBase::requireAttribute;
    template<typename T>
    T *declareGlobalVariable(const std::string &variName, bool needSerialize = true);
    template<typename T>
    T *findGlobalVariableInSort(const std::string &variName) const;

    /* not support
      template<typename T>
      std::vector<T *> findGlobalVariableInMerge(const std::string &variName) const;
    */
    
    /*
       This method is only for buildin DefaultSorter,
       any other Sorters should not use it
    */
    suez::turing::AttributeExpression *createAttributeExpression(
            const suez::turing::SyntaxExpr *syntaxExpr,
            bool needSerialize = true);

    template<typename T>
    const matchdoc::Reference<T> *getBuildInReference(const std::string &refName);

    void addSortExprMeta(const std::string &name,
                         const matchdoc::ReferenceBase *sortRef, bool sortFlag);

    const std::vector<common::SortExprMeta>& getSortExprMeta();

public:
    // get resource from provider
    const KeyValueMap &getSortKVPairs() const;

    SorterLocation getLocation() const;

    uint32_t getResultSourceNum() const;

    uint32_t getTotalMatchDocs() const;

    const common::Ha3MatchDocAllocatorPtr &getMatchDocAllocator() const;

    autil::mem_pool::Pool *getSessionPool() const;

    suez::turing::AttributeExpression *getScoreExpression() const;

    const SorterResource& getSorterResource() const;

    void updateRequiredTopk(uint32_t requiredTopk);
    uint32_t getRequiredTopk() const;
public:
    // plugins helper
    rank::ComboComparator *createSortComparator(
            const search::SortExpressionVector &sortExprs);
    rank::ComboComparator *createComparator(
            const std::vector<ComparatorItem> &compItems);
    search::SortExpression *createSortExpression(
            suez::turing::AttributeExpression *attributeExpression, bool sortFlag);
    search::SortExpression *createSortExpression(const std::string &attributeName,
            bool sortFlag);
private:
    bool validateSortAttrExpr(suez::turing::AttributeExpression *expr);
        // for test
    SorterResource* getInnerSorterResource() {
        return &_sorterResource;
    }
    void prepareForSort(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs) {
        suez::turing::ExpressionEvaluator<suez::turing::ExpressionVector> evaluator(
                getNeedEvaluateAttrs(),
                getAllocator()->getSubDocAccessor());
        evaluator.batchEvaluateExpressions(matchDocs.data(), matchDocs.size());
        batchPrepareTracer(matchDocs.data(), matchDocs.size());
    }
private:
    SorterResource _sorterResource;
    std::string _errorMsg;
    std::vector<common::SortExprMeta> _sortExprMetaVec;
private:
    friend class SorterProviderTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SorterProvider> SorterProviderPtr;
////////////////////////////////////////////////////////////////////////////

template<typename T>
T *SorterProvider::declareGlobalVariable(const std::string &variName, bool needSerialize)
{
    if (getLocation() != SL_SEARCHER) {
        return NULL;
    } else {
        return doDeclareGlobalVariable<T>(variName, needSerialize);
    }
}

template<typename T>
T *SorterProvider::findGlobalVariableInSort(const std::string &variName) const
{
    if (getLocation() != SL_SEARCHER) {
        return NULL;
    }
    return findGlobalVariable<T>(variName);
}

} // namespace sorter
} // namespace isearch

