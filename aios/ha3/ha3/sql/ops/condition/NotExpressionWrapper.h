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

namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

namespace isearch {
namespace sql {

class NotExpressionWrapper : public suez::turing::AttributeExpressionTyped<bool> {
public:
    NotExpressionWrapper(suez::turing::AttributeExpressionTyped<bool> *expression);
    ~NotExpressionWrapper();

private:
    NotExpressionWrapper(const NotExpressionWrapper &);
    NotExpressionWrapper &operator=(const NotExpressionWrapper &);

public:
    bool evaluateAndReturn(matchdoc::MatchDoc matchDoc) override;

private:
    suez::turing::AttributeExpressionTyped<bool> *_expression;
};

typedef std::shared_ptr<NotExpressionWrapper> NotExpressionWrapperPtr;
} // namespace sql
} // namespace isearch
