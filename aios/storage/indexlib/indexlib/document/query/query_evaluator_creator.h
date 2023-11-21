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

#include "indexlib/common_define.h"
#include "indexlib/document/query/function_executor_creator.h"
#include "indexlib/document/query/query_base.h"
#include "indexlib/document/query/query_evaluator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class QueryEvaluatorCreator
{
public:
    QueryEvaluatorCreator();
    ~QueryEvaluatorCreator();

public:
    bool Init(const std::string& pluginPath, const config::FunctionExecutorResource& resource);
    QueryEvaluatorPtr Create(const QueryBasePtr& query);

private:
    QueryEvaluatorPtr InnerCreate(const QueryBasePtr& query);
    QueryEvaluatorPtr CreateTermQueryEvaluator(const QueryBasePtr& query);
    QueryEvaluatorPtr CreateSubTermQueryEvaluator(const QueryBasePtr& query);
    QueryEvaluatorPtr CreatePatternQueryEvaluator(const QueryBasePtr& query);
    QueryEvaluatorPtr CreateRangeQueryEvaluator(const QueryBasePtr& query);
    QueryEvaluatorPtr CreateAndQueryEvaluator(const QueryBasePtr& query);
    QueryEvaluatorPtr CreateOrQueryEvaluator(const QueryBasePtr& query);
    QueryEvaluatorPtr CreateNotQueryEvaluator(const QueryBasePtr& query);

private:
    evaluatorid_t mEvaluatorId;
    FunctionExecutorCreator mFunctionCreator;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(QueryEvaluatorCreator);
}} // namespace indexlib::document
