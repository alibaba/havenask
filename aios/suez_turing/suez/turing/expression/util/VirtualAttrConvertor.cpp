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
#include "suez/turing/expression/util/VirtualAttrConvertor.h"

#include <cstddef>
#include <set>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, VirtualAttrConvertor);

VirtualAttrConvertor::VirtualAttrConvertor() {}

VirtualAttrConvertor::~VirtualAttrConvertor() {}

void VirtualAttrConvertor::initVirtualAttrs(const VirtualAttributes &virtualAttributes) {
    for (auto it = virtualAttributes.begin(); it != virtualAttributes.end(); ++it) {
        VirtualAttribute *virtualAttr = *it;
        _virtualAttrs[virtualAttr->getVirtualAttributeName()] = virtualAttr->getVirtualAttributeSyntaxExpr();
    }
}

std::string VirtualAttrConvertor::nameToName(const std::string &attrName) const {
    const SyntaxExpr *convertSyntax = nameToExpr(attrName);
    return convertSyntax ? convertSyntax->getExprString() : attrName;
}

const SyntaxExpr *VirtualAttrConvertor::exprToExpr(const SyntaxExpr *syntax) const {
    if (syntax->getSyntaxExprType() != SYNTAX_EXPR_TYPE_VIRTUAL_ATTR) {
        return syntax;
    }
    const SyntaxExpr *convertSyntax = nameToExpr(syntax->getExprString());
    return convertSyntax ? convertSyntax : syntax;
}

const SyntaxExpr *VirtualAttrConvertor::nameToExpr(const std::string &exprName) const {
    std::string name = exprName;
    set<const SyntaxExpr *> avoidLoop;
    const SyntaxExpr *syntax = NULL;
    while (true) {
        VirtualAttrMap::const_iterator it = _virtualAttrs.find(name);
        if (it == _virtualAttrs.end()) {
            return syntax;
        }
        syntax = it->second;
        if (syntax->getSyntaxExprType() != SYNTAX_EXPR_TYPE_VIRTUAL_ATTR) {
            return syntax;
        }
        if (avoidLoop.find(syntax) != avoidLoop.end()) {
            AUTIL_LOG(WARN, "virtual attribute get into loop.!!");
            return syntax;
        }
        avoidLoop.insert(syntax);
        name = syntax->getExprString();
    }
    return NULL;
}

} // namespace turing
} // namespace suez
