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
#ifndef __INDEXLIB_TERM_QUERY_EVALUATOR_H
#define __INDEXLIB_TERM_QUERY_EVALUATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/query/function_executor.h"
#include "indexlib/document/query/query_evaluator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class TermQueryEvaluator : public QueryEvaluator
{
public:
    TermQueryEvaluator(evaluatorid_t id = INVALID_EVALUATOR_ID) : QueryEvaluator(QET_TERM, id) {}

    ~TermQueryEvaluator() {}

public:
    void Init(const std::string& fieldName, const std::string& fieldValue)
    {
        mFieldName = fieldName;
        mFieldValue = fieldValue;
    }

    void Init(const FunctionExecutorPtr& function, const std::string& value)
    {
        assert(function);
        mFunc = function;
        mFieldName = mFunc->GetFunctionKey();
        mFieldValue = value;
    }

    EvaluatorState DoEvaluate(EvaluatorStateHandler& handler, const RawDocumentPtr& rawDoc,
                              EvaluateParam param) override
    {
        if (!rawDoc) {
            return ES_PENDING;
        }

        if (mFunc) {
            mFunc->Execute(rawDoc);
        }
        autil::StringView fn(mFieldName);
        if (!rawDoc->exist(fn)) {
            return param.pendingForFieldNotExist ? ES_PENDING : ES_FALSE;
        }
        autil::StringView fv = rawDoc->getField(fn);
        return (fv == mFieldValue) ? ES_TRUE : ES_FALSE;
    }

private:
    FunctionExecutorPtr mFunc;
    std::string mFieldName;
    std::string mFieldValue;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TermQueryEvaluator);
}} // namespace indexlib::document

#endif //__INDEXLIB_TERM_QUERY_EVALUATOR_H
