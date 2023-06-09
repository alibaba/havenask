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
#ifndef __INDEXLIB_OR_QUERY_EVALUATOR_H
#define __INDEXLIB_OR_QUERY_EVALUATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/query/query_evaluator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class OrQueryEvaluator : public QueryEvaluator
{
public:
    OrQueryEvaluator(evaluatorid_t id = INVALID_EVALUATOR_ID) : QueryEvaluator(QET_OR, id) {}

    ~OrQueryEvaluator() {}

public:
    void Init(const std::vector<QueryEvaluatorPtr>& subQueryEvaluators) { mSubQueryEvaluator = subQueryEvaluators; }

    EvaluatorState DoEvaluate(EvaluatorStateHandler& handler, const RawDocumentPtr& rawDoc,
                              EvaluateParam param) override
    {
        bool hasPending = false;
        for (auto& evaluator : mSubQueryEvaluator) {
            auto state = evaluator->Evaluate(handler, rawDoc, param);
            if (state == ES_TRUE) {
                return ES_TRUE;
            }
            if (state == ES_PENDING) {
                hasPending = true;
            }
        }
        return hasPending ? ES_PENDING : ES_FALSE;
    }

private:
    std::vector<QueryEvaluatorPtr> mSubQueryEvaluator;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OrQueryEvaluator);
}} // namespace indexlib::document

#endif //__INDEXLIB_OR_QUERY_EVALUATOR_H
