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
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/rank/DistinctHitCollector.h"
#include "ha3/search/SortExpression.h"
#include "suez/turing/expression/plugin/Sorter.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace common {
class DistinctDescription;
class Request;
}  // namespace common
namespace rank {
class ComboComparator;
}  // namespace rank
namespace sorter {
class SorterProvider;
}  // namespace sorter
}  // namespace isearch
namespace matchdoc {
class ReferenceBase;
}  // namespace matchdoc
namespace suez {
namespace turing {
class SorterProvider;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace sorter {

class DefaultSorter : public suez::turing::Sorter
{
public:
    enum DefaultSortType {
        DS_UNKNOWN,
        DS_NORMAL_SORT,
        DS_DISTINCT
    };
public:
    DefaultSorter();
    ~DefaultSorter();
public:
    bool beginSort(suez::turing::SorterProvider *provider) override;
    void sort(suez::turing::SortParam &sortParam) override;
    Sorter* clone() override {
        return new DefaultSorter(*this);
    }
    void endSort() override {
    }
    void destroy() override {
        delete this;
    }
private:
    bool initSortExpressions(isearch::sorter::SorterProvider *provider,
                             search::SortExpressionVector &sortExpressions);
    bool initSortExpressionsFromSortKey(const std::string &sortKeyDesc,
            search::SortExpressionVector &sortExpressions,
            isearch::sorter::SorterProvider *provider);
    void initComparator(isearch::sorter::SorterProvider *provider,
                        const search::SortExpressionVector &sortExpressions);

    bool prepareForDistinctHitCollector(
            common::DistinctDescription *distDesc,
            isearch::sorter::SorterProvider *provider, search::SortExpression *expr);

    void serializeSortExprRef(
            const search::SortExpressionVector &sortExpressions);
    void addSortExprMeta(const std::string &name,
                         matchdoc::ReferenceBase* sortRef,
                         bool sortFlag, isearch::sorter::SorterProvider *provider);
    bool needScoreInSort(const common::Request *request) const;
    bool validateSortInfo(const common::Request *request) const;
    bool needSort(isearch::sorter::SorterProvider *provider) const;

private:
    struct DistincCollectorParam {
        DistincCollectorParam()
            : pool(NULL), sortExpr(NULL) { }
        common::Ha3MatchDocAllocatorPtr allocator;
        autil::mem_pool::Pool *pool;
        rank::DistinctParameter dp;
        search::SortExpression *sortExpr;
    };
private:
    DefaultSortType _type;
    rank::ComboComparator *_cmp;
    DistincCollectorParam _distinctCollectorParam;
    bool _needSort;

private:
    friend class DefaultSorterTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DefaultSorter> DefaultSorterPtr;

} // namespace sorter
} // namespace isearch

