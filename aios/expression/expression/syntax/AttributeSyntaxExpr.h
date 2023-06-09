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
#ifndef ISEARCH_EXPRESSION_ATTRIBUTESYNTAXEXPR_H
#define ISEARCH_EXPRESSION_ATTRIBUTESYNTAXEXPR_H

#include "expression/common.h"
#include "expression/syntax/SyntaxExpr.h"

namespace expression {


class AttributeSyntaxExpr : public SyntaxExpr
{
public:
    AttributeSyntaxExpr(const std::string &attrName = "",
                        const std::string &tableName = "",
                        autil::mem_pool::Pool* pool = NULL)
        : SyntaxExpr(SYNTAX_EXPR_TYPE_ATOMIC_ATTR, pool)
        , _attrName(attrName)
        , _tableName(tableName)
    {
        if (!attrName.empty()) {
            initExprString();
        }
    }
    
    ~AttributeSyntaxExpr() {}
    
public:
    /* override */ bool operator == (const SyntaxExpr *expr) const;
    /* override */ void accept(SyntaxExprVisitor *visitor) const;

    const std::string &getAttributeName() const { return _attrName; }
    const std::string &getTableName() const { return _tableName; }
public:
    /* override */ void serialize(autil::DataBuffer &dataBuffer) const;
    /* override */ void deserialize(autil::DataBuffer &dataBuffer);

private:
    void initExprString() {
        if (_tableName.empty()) {
            setExprString(_attrName);
        } else {
            setExprString(_tableName + ":" + _attrName);        
        }
    }

private:
    std::string _attrName;
    std::string _tableName;
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_ATTRIBUTESYNTAXEXPR_H
