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
#include "indexlib/document/query/query_evaluator_creator.h"

#include "indexlib/document/query/and_query.h"
#include "indexlib/document/query/and_query_evaluator.h"
#include "indexlib/document/query/not_query.h"
#include "indexlib/document/query/not_query_evaluator.h"
#include "indexlib/document/query/or_query.h"
#include "indexlib/document/query/or_query_evaluator.h"
#include "indexlib/document/query/pattern_query.h"
#include "indexlib/document/query/pattern_query_evaluator.h"
#include "indexlib/document/query/range_query.h"
#include "indexlib/document/query/range_query_evaluator.h"
#include "indexlib/document/query/sub_term_query.h"
#include "indexlib/document/query/sub_term_query_evaluator.h"
#include "indexlib/document/query/term_query.h"
#include "indexlib/document/query/term_query_evaluator.h"

using namespace std;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, QueryEvaluatorCreator);

QueryEvaluatorCreator::QueryEvaluatorCreator() : mEvaluatorId(0) {}

QueryEvaluatorCreator::~QueryEvaluatorCreator() {}

bool QueryEvaluatorCreator::Init(const string& pluginPath, const config::FunctionExecutorResource& resource)
{
    return mFunctionCreator.Init(pluginPath, resource);
}

QueryEvaluatorPtr QueryEvaluatorCreator::Create(const QueryBasePtr& query)
{
    mEvaluatorId = 0;
    return InnerCreate(query);
}

QueryEvaluatorPtr QueryEvaluatorCreator::InnerCreate(const QueryBasePtr& query)
{
    if (!query) {
        return QueryEvaluatorPtr();
    }

    QueryType type = query->GetType();
    QueryEvaluatorPtr ret;
    switch (type) {
    case QT_TERM: {
        ret = CreateTermQueryEvaluator(query);
        break;
    }
    case QT_SUB_TERM: {
        ret = CreateSubTermQueryEvaluator(query);
        break;
    }
    case QT_PATTERN: {
        ret = CreatePatternQueryEvaluator(query);
        break;
    }
    case QT_RANGE: {
        ret = CreateRangeQueryEvaluator(query);
        break;
    }
    case QT_AND: {
        ret = CreateAndQueryEvaluator(query);
        break;
    }
    case QT_OR: {
        ret = CreateOrQueryEvaluator(query);
        break;
    }
    case QT_NOT: {
        ret = CreateNotQueryEvaluator(query);
        break;
    }
    default:
        assert(false);
    }
    return ret;
}

QueryEvaluatorPtr QueryEvaluatorCreator::CreateTermQueryEvaluator(const QueryBasePtr& query)
{
    TermQueryPtr termQuery = static_pointer_cast<TermQuery>(query);
    if (!termQuery) {
        return QueryEvaluatorPtr();
    }

    TermQueryEvaluatorPtr evaluator(new TermQueryEvaluator(mEvaluatorId++));
    if (!termQuery->IsFunctionQuery()) {
        evaluator->Init(termQuery->GetFieldName(), termQuery->GetValue());
    } else {
        FunctionExecutorPtr executor(mFunctionCreator.CreateFunctionExecutor(termQuery->GetFunction()));
        if (!executor) {
            return QueryEvaluatorPtr();
        }
        evaluator->Init(executor, termQuery->GetValue());
    }
    return evaluator;
}

QueryEvaluatorPtr QueryEvaluatorCreator::CreateSubTermQueryEvaluator(const QueryBasePtr& query)
{
    SubTermQueryPtr subTermQuery = static_pointer_cast<SubTermQuery>(query);
    if (!subTermQuery) {
        return QueryEvaluatorPtr();
    }

    SubTermQueryEvaluatorPtr evaluator(new SubTermQueryEvaluator(mEvaluatorId++));
    if (!subTermQuery->IsFunctionQuery()) {
        evaluator->Init(subTermQuery->GetFieldName(), subTermQuery->GetSubValue(), subTermQuery->GetDelimeter());
    } else {
        FunctionExecutorPtr executor(mFunctionCreator.CreateFunctionExecutor(subTermQuery->GetFunction()));
        if (!executor) {
            return QueryEvaluatorPtr();
        }
        evaluator->Init(executor, subTermQuery->GetSubValue(), subTermQuery->GetDelimeter());
    }
    return evaluator;
}

QueryEvaluatorPtr QueryEvaluatorCreator::CreatePatternQueryEvaluator(const QueryBasePtr& query)
{
    PatternQueryPtr patternQuery = static_pointer_cast<PatternQuery>(query);
    if (!patternQuery) {
        return QueryEvaluatorPtr();
    }

    PatternQueryEvaluatorPtr evaluator(new PatternQueryEvaluator(mEvaluatorId++));
    if (!patternQuery->IsFunctionQuery()) {
        evaluator->Init(patternQuery->GetFieldName(), patternQuery->GetFieldPattern());
    } else {
        FunctionExecutorPtr executor(mFunctionCreator.CreateFunctionExecutor(patternQuery->GetFunction()));
        if (!executor) {
            return QueryEvaluatorPtr();
        }
        evaluator->Init(executor, patternQuery->GetFieldPattern());
    }
    if (evaluator->IsPatternIntialized()) {
        return evaluator;
    }
    return QueryEvaluatorPtr();
}

QueryEvaluatorPtr QueryEvaluatorCreator::CreateRangeQueryEvaluator(const QueryBasePtr& query)
{
    RangeQueryPtr rangeQuery = static_pointer_cast<RangeQuery>(query);
    if (!rangeQuery) {
        return QueryEvaluatorPtr();
    }

    RangeQueryEvaluatorPtr evaluator(new RangeQueryEvaluator(mEvaluatorId++));
    if (!rangeQuery->IsFunctionQuery()) {
        evaluator->Init(rangeQuery->GetFieldName(), rangeQuery->GetRangeInfo());
    } else {
        FunctionExecutorPtr executor(mFunctionCreator.CreateFunctionExecutor(rangeQuery->GetFunction()));
        if (!executor) {
            return QueryEvaluatorPtr();
        }
        evaluator->Init(executor, rangeQuery->GetRangeInfo());
    }
    return evaluator;
}

QueryEvaluatorPtr QueryEvaluatorCreator::CreateAndQueryEvaluator(const QueryBasePtr& query)
{
    AndQueryPtr andQuery = static_pointer_cast<AndQuery>(query);
    if (!andQuery) {
        return QueryEvaluatorPtr();
    }

    const vector<QueryBasePtr>& subQuerys = andQuery->GetSubQuery();
    vector<QueryEvaluatorPtr> subEvaluator;
    subEvaluator.reserve(subQuerys.size());

    for (auto& subQuery : subQuerys) {
        QueryEvaluatorPtr evaluator = InnerCreate(subQuery);
        if (!evaluator) {
            AUTIL_LOG(ERROR, "create and query evaluator for sub query [%s] fail", ToJsonString(subQuery).c_str());
            return QueryEvaluatorPtr();
        }
        subEvaluator.push_back(evaluator);
    }

    AndQueryEvaluatorPtr evaluator(new AndQueryEvaluator(mEvaluatorId++));
    evaluator->Init(subEvaluator);
    return evaluator;
}

QueryEvaluatorPtr QueryEvaluatorCreator::CreateOrQueryEvaluator(const QueryBasePtr& query)
{
    OrQueryPtr orQuery = static_pointer_cast<OrQuery>(query);
    if (!orQuery) {
        return QueryEvaluatorPtr();
    }

    const vector<QueryBasePtr>& subQuerys = orQuery->GetSubQuery();
    vector<QueryEvaluatorPtr> subEvaluator;
    subEvaluator.reserve(subQuerys.size());

    for (auto& subQuery : subQuerys) {
        QueryEvaluatorPtr evaluator = InnerCreate(subQuery);
        if (!evaluator) {
            AUTIL_LOG(ERROR, "create or query evaluator for sub query [%s] fail", ToJsonString(subQuery).c_str());
            return QueryEvaluatorPtr();
        }
        subEvaluator.push_back(evaluator);
    }
    OrQueryEvaluatorPtr evaluator(new OrQueryEvaluator(mEvaluatorId++));
    evaluator->Init(subEvaluator);
    return evaluator;
}

QueryEvaluatorPtr QueryEvaluatorCreator::CreateNotQueryEvaluator(const QueryBasePtr& query)
{
    NotQueryPtr notQuery = static_pointer_cast<NotQuery>(query);
    if (!notQuery) {
        return QueryEvaluatorPtr();
    }

    const vector<QueryBasePtr>& subQuerys = notQuery->GetSubQuery();
    if (subQuerys.size() != 1) {
        AUTIL_LOG(ERROR, "create not query evaluator for sub query [%s], sub query size is not 1",
                  ToJsonString(subQuerys).c_str());
        return QueryEvaluatorPtr();
    }
    vector<QueryEvaluatorPtr> subEvaluator;
    subEvaluator.reserve(subQuerys.size());

    QueryEvaluatorPtr innerEvaluator = InnerCreate(subQuerys[0]);
    if (!innerEvaluator) {
        AUTIL_LOG(ERROR, "create not query evaluator for sub query [%s] fail", ToJsonString(subQuerys[0]).c_str());
        return QueryEvaluatorPtr();
    }
    subEvaluator.push_back(innerEvaluator);

    NotQueryEvaluatorPtr evaluator(new NotQueryEvaluator(mEvaluatorId++));
    evaluator->Init(subEvaluator);
    return evaluator;
}
}} // namespace indexlib::document
