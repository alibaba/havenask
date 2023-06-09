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
#ifndef ISEARCH_EXPRESSION_ATOMICATTRIBUTEEXPRESSIONCREATORBASE_H
#define ISEARCH_EXPRESSION_ATOMICATTRIBUTEEXPRESSIONCREATORBASE_H

#include "autil/mem_pool/Pool.h"
#include "expression/common.h"
#include "expression/common/ErrorDefine.h"
#include "expression/common/SessionResource.h"
#include "expression/framework/AttributeExpression.h"

namespace expression {

class AtomicAttributeExpressionCreatorBase {
public:
    AtomicAttributeExpressionCreatorBase() : _errorCode(ERROR_NONE) {}
    virtual ~AtomicAttributeExpressionCreatorBase() {}

private:
    AtomicAttributeExpressionCreatorBase(const AtomicAttributeExpressionCreatorBase &);
    AtomicAttributeExpressionCreatorBase &operator=(const AtomicAttributeExpressionCreatorBase &);

public:
    virtual AttributeExpression *createAtomicExpr(const std::string &name, const std::string &tableName) = 0;
    virtual void endRequest() = 0;
    virtual bool beginRequest(std::vector<AttributeExpression *> &allExprInSession, SessionResource *resource) = 0;

    ErrorCode getErrorCode() const { return _errorCode; }
    const std::string &getErrorMsg() const { return _errorMsg; }

protected:
    ErrorCode _errorCode;
    std::string _errorMsg;
};

} // namespace expression

#endif // ISEARCH_EXPRESSION_ATOMICATTRIBUTEEXPRESSIONCREATORBASE_H
