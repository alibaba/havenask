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
#include "expression/framework/VirtualAttributeContainer.h"

using namespace std;

namespace expression {
AUTIL_LOG_SETUP(expression, VirtualAttributeContainer);

VirtualAttributeContainer::VirtualAttributeContainer() {
}

VirtualAttributeContainer::~VirtualAttributeContainer() {
}

bool VirtualAttributeContainer::addVirtualAttribute(
        const string &name, AttributeExpression *expr)
{
    VirtualAttrExprMap::const_iterator it = _virtualAttrExpressions.find(name);
    if (it != _virtualAttrExpressions.end()) {
        AUTIL_LOG(WARN, "virtual attribute[%s] already exist", name.c_str());
        return false;
    }
    _virtualAttrExpressions[name] = expr;
    return true;
}
   
AttributeExpression* VirtualAttributeContainer::findVirtualAttribute(
        const string &name) const
{
    VirtualAttrExprMap::const_iterator it = _virtualAttrExpressions.find(name);
    return it == _virtualAttrExpressions.end() ? NULL : it->second;
}

// bool VirtualAttributeContainer::addVirtualAttribute(const string &name, SyntaxExpr *syntaxExpr) {
//     if (getVirtualAttribute(name) != NULL) {
//         AUTIL_LOG(WARN, "virtual attribute[%s] already exist", name.c_str());
//         return false;
//     }
//     _virtualAttributes[name] = new VirtualAttribute(name, syntaxExpr);
//     return true;
// }

// VirtualAttribute* VirtualAttributeContainer::getVirtualAttribute(const string &name) const {
//     map<string, VirtualAttribute*>::const_iterator it = _virtualAttributes.find(name);
//     return it == _virtualAttributes.end() ? NULL : it->second;
// }

// const SyntaxExpr *VirtualAttributeContainer::getSyntax(const string &name) const {
//     set<const SyntaxExpr*> visitedSyntaxSet;
//     const SyntaxExpr *syntax = NULL;
//     string exprName = name;

//     while (true) {
//         map<string, VirtualAttribute*>::const_iterator it = _virtualAttributes.find(exprName);
//         if (it == _virtualAttributes.end()) {
//             return syntax;
//         }
//         syntax = it->second->getSyntaxExpr();
//         if (syntax->getSyntaxType() != SYNTAX_EXPR_TYPE_ATOMIC_ATTR) {
//             return syntax;
//         }
//         const string &childExprName = syntax->getExprString();
//         if (_virtualAttributes.find(childExprName) == _virtualAttributes.end()) {
//             return syntax;
//         }
//         // check loop
//         if (visitedSyntaxSet.find(syntax) != visitedSyntaxSet.end()) {
//             AUTIL_LOG(WARN, "virtual attribute get into loop");
//             return NULL;
//         }
//         visitedSyntaxSet.insert(syntax);
//         exprName = childExprName;
//     }
//     return NULL;
// }
}
