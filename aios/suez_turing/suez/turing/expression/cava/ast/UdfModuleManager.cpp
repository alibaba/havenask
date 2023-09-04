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
#include "suez/turing/expression/cava/ast/UdfModuleManager.h"

#include <cstddef>
#include <vector>

#include "alog/Logger.h"
#include "cava/ast/ASTContext.h"
#include "cava/ast/ASTNode.h"
#include "cava/ast/AmbExpr.h"
#include "cava/ast/AmbTypeNode.h"
#include "cava/ast/CallExpr.h"
#include "cava/ast/ClassDecl.h"
#include "cava/ast/Expr.h"
#include "cava/ast/ExprVisitor.h"
#include "cava/ast/FieldDecl.h"
#include "cava/ast/Formal.h"
#include "cava/ast/LiteralExpr.h"
#include "cava/ast/LocalVarDeclStmt.h"
#include "cava/ast/MethodDecl.h"
#include "cava/ast/Name.h"
#include "cava/ast/NodeFactory.h"
#include "cava/ast/TypeNode.h"
#include "cava/common/Common.h"
#include "cava/parse/position.hh"

using namespace std;
using namespace cava;

namespace suez {
namespace turing {
AUTIL_LOG_SETUP(ast, UdfModuleManager);
bool UdfModuleManager::addUdfModule(const std::string &methodName,
                                    ::cava::ClassDecl *classDecl,
                                    ::cava::MethodDecl *methodDecl,
                                    GenRule *genRule) {
    auto iter = _udfModsMap.find(methodName);
    if (_udfModsMap.end() == iter) {
        _udfModsMap[methodName] = make_tuple(classDecl, methodDecl, genRule);
    }
    return true;
}

} // namespace turing
} // namespace suez
