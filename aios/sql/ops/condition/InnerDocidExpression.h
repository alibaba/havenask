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

#include <stdint.h>

#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

namespace sql {

class InnerDocIdExpression : public suez::turing::AttributeExpressionTyped<int32_t> {
public:
    InnerDocIdExpression();
    virtual ~InnerDocIdExpression() {}

public:
    bool evaluate(matchdoc::MatchDoc matchDoc) override {
        this->storeValue(matchDoc, matchDoc.getDocId());
        return true;
    }
    int32_t evaluateAndReturn(matchdoc::MatchDoc matchDoc) override {
        int32_t docId = matchDoc.getDocId();
        this->storeValue(matchDoc, docId);
        return docId;
    }
    bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t count) override {
        for (uint32_t i = 0; i < count; i++) {
            this->storeValue(matchDocs[i], matchDocs[i].getDocId());
        }
        return true;
    }
    suez::turing::ExpressionType getExpressionType() const override {
        return suez::turing::ET_ATOMIC;
    }
};

} // namespace sql
