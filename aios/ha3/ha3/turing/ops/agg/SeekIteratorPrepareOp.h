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

#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/turing/variant/ExpressionResourceVariant.h"
#include "ha3/turing/variant/SeekIteratorVariant.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/partition_define.h"
#include "suez/turing/common/QueryResource.h"
#include "tensorflow/core/framework/op_kernel.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace matchdoc {
class MatchDocAllocator;
}  // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpressionCreator;
class TimeoutTerminator;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace common {
class FilterClause;
class PKFilterClause;
class Query;
}  // namespace common
namespace search {
class IndexPartitionReaderWrapper;
class QueryExecutor;
}  // namespace search

namespace turing {

class SeekIteratorPrepareOp : public tensorflow::OpKernel
{
public:
    explicit SeekIteratorPrepareOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {}
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
public:
    static bool createSeekIterator(const std::string &mainTableName,
                                   const ExpressionResourcePtr &resource,
                                   const search::LayerMetasPtr &layerMetas,
                                   suez::turing::QueryResource *queryResource,
                                   SeekIteratorPtr &seekIterator);
private:
    static bool createFilterWrapper(
            const common::FilterClause *filterClause,
            suez::turing::AttributeExpressionCreator *attrExprCreator,
            matchdoc::MatchDocAllocator *matchDocAllocator,
            autil::mem_pool::Pool *pool,
            search::FilterWrapperPtr &filterWrapper);
    static search::QueryExecutor* createQueryExecutor(
            const common::Query *query,
            search::IndexPartitionReaderWrapper *wrapper,
            const common::PKFilterClause *pkFilterClause,
            const search::LayerMeta *layerMeta,
            autil::mem_pool::Pool *pool,
            autil::TimeoutTerminator *timeoutTerminator);

    static search::QueryExecutor* createPKExecutor(
            const common::PKFilterClause *pkFilterClause,
            search::QueryExecutor *queryExecutor,
            search::IndexPartitionReaderWrapper* wrapper,
            autil::mem_pool::Pool *pool);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch
