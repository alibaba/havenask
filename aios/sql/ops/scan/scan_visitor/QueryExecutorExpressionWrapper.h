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

#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "indexlib/base/Types.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

namespace isearch {
namespace common {
class Query;
} // namespace common
namespace search {
class QueryExecutor;
} // namespace search
} // namespace isearch
namespace matchdoc {
class MatchDoc;
} // namespace matchdoc
namespace autil {
class TimeoutTerminator;
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace sql {

class QueryExecutorExpressionWrapper : public suez::turing::AttributeExpressionTyped<bool> {
public:
    QueryExecutorExpressionWrapper(isearch::common::Query *query);
    ~QueryExecutorExpressionWrapper();

private:
    QueryExecutorExpressionWrapper(const QueryExecutorExpressionWrapper &);
    QueryExecutorExpressionWrapper &operator=(const QueryExecutorExpressionWrapper &);

public:
    bool init(isearch::search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
              const std::string &mainTableName,
              autil::mem_pool::Pool *pool,
              autil::TimeoutTerminator *timeoutTerminator,
              const std::vector<isearch::search::LayerMetaPtr> &layerMetas);
    bool evaluateAndReturn(matchdoc::MatchDoc matchDoc) override;
    void reset();

private:
    static isearch::search::QueryExecutor *
    createQueryExecutor(const isearch::common::Query *query,
                        isearch::search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
                        const std::string &mainTableName,
                        autil::mem_pool::Pool *pool,
                        autil::TimeoutTerminator *timeoutTerminator,
                        isearch::search::LayerMeta *layerMeta);

private:
    isearch::common::Query *_query;
    std::vector<isearch::search::QueryExecutor *> _queryExecutors;
    indexlib::DocIdRangeVector _docIdRanges;
    bool _initialized;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QueryExecutorExpressionWrapper> QueryExecutorExpressionWrapperPtr;
} // namespace sql
