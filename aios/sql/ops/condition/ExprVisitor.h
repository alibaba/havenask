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

#include "autil/legacy/RapidJsonCommon.h"
#include "sql/common/Log.h" // IWYU pragma: keep

namespace sql {

class ExprVisitor {
public:
    ExprVisitor();
    virtual ~ExprVisitor();
    ExprVisitor(const ExprVisitor &) = delete;
    ExprVisitor &operator=(const ExprVisitor &) = delete;

public:
    void visit(const autil::SimpleValue &value);
    bool isError() const {
        return _hasError;
    }
    const std::string &errorInfo() const {
        return _errorInfo;
    }
    void setErrorInfo(const char *fmt, ...);

protected:
    virtual void visitCastUdf(const autil::SimpleValue &value);
    virtual void visitMultiCastUdf(const autil::SimpleValue &value);
    virtual void visitNormalUdf(const autil::SimpleValue &value);
    virtual void visitInOp(const autil::SimpleValue &value);
    virtual void visitNotInOp(const autil::SimpleValue &value);
    virtual void visitCaseOp(const autil::SimpleValue &value);
    virtual void visitOtherOp(const autil::SimpleValue &value);
    virtual void visitInt64(const autil::SimpleValue &value);
    virtual void visitDouble(const autil::SimpleValue &value);
    virtual void visitBool(const autil::SimpleValue &value);
    virtual void visitItemOp(const autil::SimpleValue &value);
    virtual void visitColumn(const autil::SimpleValue &value);
    virtual void visitString(const autil::SimpleValue &value);

private:
    bool _hasError;
    std::string _errorInfo;
};

typedef std::shared_ptr<ExprVisitor> ExprVisitorPtr;
} // namespace sql
