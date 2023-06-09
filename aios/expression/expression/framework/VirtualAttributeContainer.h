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
#ifndef ISEARCH_EXPRESSION_VIRTUALATTRIBUTECONTAINER_H
#define ISEARCH_EXPRESSION_VIRTUALATTRIBUTECONTAINER_H

#include "expression/common.h"
#include "expression/framework/AttributeExpression.h"

namespace expression {

class VirtualAttributeContainer {
public:
    VirtualAttributeContainer();
    ~VirtualAttributeContainer();
public:
    bool addVirtualAttribute(const std::string &name,
                             AttributeExpression *expr);
    AttributeExpression* findVirtualAttribute(const std::string &name) const;
    
    void reset()
    {
        _virtualAttrExpressions.clear();
    }

    // bool addVirtualAttribute(const std::string &name, SyntaxExpr *syntaxExpr);
    // VirtualAttribute* getVirtualAttribute(const std::string &name) const;
    // const SyntaxExpr *getSyntax(const std::string &name) const;
private:
    // std::map<std::string, VirtualAttribute*> _virtualAttributes;
    typedef std::map<std::string, AttributeExpression*> VirtualAttrExprMap;
    VirtualAttrExprMap _virtualAttrExpressions;
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_VIRTUALATTRIBUTECONTAINER_H
