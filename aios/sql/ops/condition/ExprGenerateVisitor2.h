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

#include "autil/result/Result.h"
#include "sql/ops/condition/ExprVisitor.h"

namespace turbojet::expr {
class SyntaxNode;
using SyntaxNodePtr = std::shared_ptr<const SyntaxNode>;
} // namespace turbojet::expr

namespace sql {

class ExprGenerateVisitor2 : public ExprVisitor {
public:
    autil::Result<turbojet::expr::SyntaxNodePtr> apply(const autil::SimpleValue &value);

#define EXPR_GEN2_DECLARE_VISIT_(name)                                                             \
    void name(const autil::SimpleValue &value) override {                                          \
        _r = name##_(value);                                                                       \
    }                                                                                              \
    autil::Result<turbojet::expr::SyntaxNodePtr> name##_(const autil::SimpleValue &value)

    EXPR_GEN2_DECLARE_VISIT_(visitCastUdf);
    EXPR_GEN2_DECLARE_VISIT_(visitMultiCastUdf);
    EXPR_GEN2_DECLARE_VISIT_(visitNormalUdf);
    EXPR_GEN2_DECLARE_VISIT_(visitInOp);
    EXPR_GEN2_DECLARE_VISIT_(visitNotInOp);
    EXPR_GEN2_DECLARE_VISIT_(visitCaseOp);
    EXPR_GEN2_DECLARE_VISIT_(visitOtherOp);
    EXPR_GEN2_DECLARE_VISIT_(visitInt64);
    EXPR_GEN2_DECLARE_VISIT_(visitDouble);
    EXPR_GEN2_DECLARE_VISIT_(visitBool);
    EXPR_GEN2_DECLARE_VISIT_(visitString);
    EXPR_GEN2_DECLARE_VISIT_(visitColumn);

#undef EXPR_GEN2_DECLARE_VISIT_

    virtual autil::Result<turbojet::expr::SyntaxNodePtr> getId(const std::string &id);

private:
    autil::Result<turbojet::expr::SyntaxNodePtr> castOpImpl(const autil::SimpleValue &value,
                                                            bool multi);
    autil::Result<turbojet::expr::SyntaxNodePtr> inOpImpl(const autil::SimpleValue &value);

private:
    autil::Result<turbojet::expr::SyntaxNodePtr> _r;
    std::map<std::string, turbojet::expr::SyntaxNodePtr> _idMap;
};

} // namespace sql
