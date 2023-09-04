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

#include <vector>

class CavaCtx;
namespace suez {
namespace turing {
class AttributeExpression;
} // namespace turing
} // namespace suez

namespace unsafe {
class AttributeExpression;

class ExpressionVector {
public:
    ExpressionVector(const std::vector<suez::turing::AttributeExpression *> *expressionVec)
        : _expressionVec(expressionVec) {}
    ~ExpressionVector() {}

public:
    AttributeExpression *get(CavaCtx *ctx, int index);

private:
    const std::vector<suez::turing::AttributeExpression *> *_expressionVec;
};

} // namespace unsafe
