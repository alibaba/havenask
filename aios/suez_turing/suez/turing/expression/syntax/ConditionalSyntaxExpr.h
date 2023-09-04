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

#include <string>

#include "autil/Log.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

namespace autil {
class DataBuffer;
} // namespace autil

namespace suez {
namespace turing {
class SyntaxExprVisitor;

class ConditionalSyntaxExpr : public SyntaxExpr {
public:
    ConditionalSyntaxExpr(SyntaxExpr *conditionalExpr, SyntaxExpr *trueExpr, SyntaxExpr *falseExpr);
    virtual ~ConditionalSyntaxExpr();

public:
    const SyntaxExpr *getConditionalExpr() const { return _conditionalExpr; }
    const SyntaxExpr *getTrueExpr() const { return _trueExpr; }
    const SyntaxExpr *getFalseExpr() const { return _falseExpr; }

public:
    bool operator==(const SyntaxExpr *expr) const override;
    void accept(SyntaxExprVisitor *visitor) const override;
    virtual std::string getExprString() const override;
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;

public:
private:
    SyntaxExpr *_conditionalExpr;
    SyntaxExpr *_trueExpr;
    SyntaxExpr *_falseExpr;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
