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

#include "suez/turing/expression/framework/AttributeExpression.h"
#include "ha3/sql/common/Log.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"

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
} // namespace autil

namespace isearch {
namespace sql {

class QueryExecutorExpressionWrapper : public suez::turing::AttributeExpressionTyped<bool> {
public:
    QueryExecutorExpressionWrapper(common::Query *query);
    ~QueryExecutorExpressionWrapper();

private:
    QueryExecutorExpressionWrapper(const QueryExecutorExpressionWrapper &);
    QueryExecutorExpressionWrapper &operator=(const QueryExecutorExpressionWrapper &);

public:
    bool init(search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
              const std::string &mainTableName,
              autil::mem_pool::Pool *pool,
              autil::TimeoutTerminator *timeoutTerminator,
              const std::vector<search::LayerMetaPtr> &layerMetas);
    bool evaluateAndReturn(matchdoc::MatchDoc matchDoc) override;
    void reset();
private:
    static search::QueryExecutor *createQueryExecutor(
            const common::Query *query,
            search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
            const std::string &mainTableName,
            autil::mem_pool::Pool *pool,
            autil::TimeoutTerminator *timeoutTerminator,
            search::LayerMeta *layerMeta);

private:
    common::Query *_query;
    std::vector<search::QueryExecutor *> _queryExecutors;
    indexlib::DocIdRangeVector _docIdRanges;
    bool _initialized;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QueryExecutorExpressionWrapper> QueryExecutorExpressionWrapperPtr;
} // namespace sql
} // namespace isearch
