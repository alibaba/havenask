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
#ifndef __INDEXLIB_QUERY_EVALUATOR_H
#define __INDEXLIB_QUERY_EVALUATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/query/evaluator_state_handler.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

enum QueryEvaluatorType {
    QET_TERM = 0,
    QET_SUB_TERM = 1,
    QET_PATTERN = 2,
    QET_RANGE = 3,
    QET_AND = 4,
    QET_OR = 5,
    QET_NOT = 6,
    QET_UNKNOWN = 7,
};

class QueryEvaluator
{
public:
    struct EvaluateParam {
        EvaluateParam() : pendingForFieldNotExist(false) {}
        bool pendingForFieldNotExist;
    };

public:
    QueryEvaluator(QueryEvaluatorType type = QET_UNKNOWN, evaluatorid_t id = INVALID_EVALUATOR_ID)
        : mType(type)
        , mId(id)
    {
    }

    virtual ~QueryEvaluator() {}

public:
    QueryEvaluatorType GetType() const { return mType; }

    EvaluatorState Evaluate(const RawDocumentPtr& rawDoc, EvaluateParam param = QueryEvaluator::EvaluateParam())
    {
        EvaluatorStateHandler handler;
        handler.Init(mId, rawDoc);
        EvaluatorState state = Evaluate(handler, rawDoc, param);
        handler.SyncToDocument(rawDoc);
        return state;
    }

    EvaluatorState Evaluate(EvaluatorStateHandler& handler, const RawDocumentPtr& rawDoc,
                            EvaluateParam param = QueryEvaluator::EvaluateParam())
    {
        EvaluatorState state = handler.GetState(mId);
        if (state != ES_PENDING) {
            return state;
        }
        state = DoEvaluate(handler, rawDoc, param);
        if (state != ES_PENDING) {
            handler.UpdateState(mId, state);
        }
        return state;
    }

    virtual EvaluatorState DoEvaluate(EvaluatorStateHandler& handler, const RawDocumentPtr& rawDoc,
                                      EvaluateParam param) = 0;

protected:
    QueryEvaluatorType mType;
    evaluatorid_t mId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(QueryEvaluator);
}} // namespace indexlib::document

#endif //__INDEXLIB_QUERY_EVALUATOR_H
