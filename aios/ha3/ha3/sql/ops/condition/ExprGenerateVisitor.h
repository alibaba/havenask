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
#include <string>
#include <unordered_map>

#include "autil/Log.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/ops/condition/ExprVisitor.h"

namespace isearch {
namespace sql {

class ExprGenerateVisitor : public ExprVisitor {
public:
    typedef std::unordered_map<std::string, std::string> VisitorMap;
    ExprGenerateVisitor(VisitorMap *renameMap);
    ~ExprGenerateVisitor();

public:
    const std::string &getExpr() const {
        return _expr;
    }

protected:
    void visitCastUdf(const autil::SimpleValue &value) override;
    void visitMultiCastUdf(const autil::SimpleValue &value) override;
    void visitNormalUdf(const autil::SimpleValue &value) override;
    void visitInOp(const autil::SimpleValue &value) override;
    void visitNotInOp(const autil::SimpleValue &value) override;
    void visitCaseOp(const autil::SimpleValue &value) override;
    void visitOtherOp(const autil::SimpleValue &value) override;
    void visitInt64(const autil::SimpleValue &value) override;
    void visitDouble(const autil::SimpleValue &value) override;
    void visitBool(const autil::SimpleValue &value) override;
    void visitItemOp(const autil::SimpleValue &value) override;
    void visitColumn(const autil::SimpleValue &value) override;
    void visitString(const autil::SimpleValue &value) override;

private:
    void parseInOrNotIn(const autil::SimpleValue &value, const std::string &op);

protected:
    std::string _expr;
    VisitorMap *_renameMapPtr = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ExprGenerateVisitor> ExprGenerateVisitorPtr;
} // namespace sql
} // namespace isearch
