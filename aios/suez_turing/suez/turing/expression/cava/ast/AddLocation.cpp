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
#include "suez/turing/expression/cava/ast/AddLocation.h"

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
AUTIL_LOG_SETUP(ast, AddLocation);
EXPORT_PLUGIN_CREATOR(ASTRewriter, AddLocation);

bool AddLocation::process(ASTContext &astCtx) {
    _astCtx = &astCtx;
    for (auto classDecl : astCtx.getClassDecls()) {
        _classDecl = classDecl;
        genClassMemberVar2Type();
        for (auto methodDecl : classDecl->getMethods()) {
            _methodDecl = methodDecl;

            _methodLocalVar2Type.clear();
            for (auto formal : _methodDecl->getFormals()) {
                if (formal->getTypeNode()->getKind() == TypeNode::TNK_Amb &&
                    ((AmbTypeNode *)(formal->getTypeNode()))->getClassName() == "TraceApi") {
                    _methodLocalVar2Type[formal->getFormalName()] = "TraceApi";
                }
            }

            handleCompoundStmt(methodDecl->getBody());
        }
        _classMemberVar2Type.clear();
    }
    return true;
}

void AddLocation::genClassMemberVar2Type() {
    for (auto &fieldDecl : _classDecl->getFields()) {
        AmbTypeNode *typeNode = dynamic_cast<AmbTypeNode *>(fieldDecl->getTypeNode());
        if (typeNode != NULL && "TraceApi" == typeNode->getClassName()) {
            _classMemberVar2Type[fieldDecl->getFieldName()] = "TraceApi";
        }
    }
}

void AddLocation::visitCallExpr(CallExpr *expr) {
    StmtExprVisitor::visitCallExpr(expr);

    if (expr->getFuncName() != "docTrace" && expr->getFuncName() != "queryTrace") {
        return;
    }
    AmbExpr *tracer = dynamic_cast<AmbExpr *>(expr->getExpr());
    if (tracer == NULL) {
        return;
    }
    auto it1 = _methodLocalVar2Type.find(tracer->getName()->getPostName());
    auto it2 = _classMemberVar2Type.find(tracer->getName()->getPostName());
    if (it1 == _methodLocalVar2Type.end() && it2 == _classMemberVar2Type.end()) {
        return;
    }

    AUTIL_LOG(DEBUG, "begin to rewrite call expr :%s", expr->toString().c_str());

    // rewrite
    auto &args = expr->getArgs();
    Location loc = expr->getLocation();
    auto lineExpr = NodeFactory::createTypedLiteralExpr<int>(*_astCtx, loc, LiteralExpr::LT_INT, (int)loc.begin.line);
    auto fileNameExpr =
        NodeFactory::createTypedLiteralExpr<std::string *>(*_astCtx, loc, LiteralExpr::LT_STRING, loc.begin.filename);
    // args is ref
    args.insert(args.begin(), lineExpr);
    args.insert(args.begin(), fileNameExpr);

    AUTIL_LOG(DEBUG, "rewrited call expr :%s", expr->toString().c_str());
}

bool AddLocation::handleLocalVarDeclStmt(LocalVarDeclStmt *localVarDeclStmt) {
    if (localVarDeclStmt->getTypeNode()->getKind() == TypeNode::TNK_Amb) {
        if ("TraceApi" == (dynamic_cast<AmbTypeNode *>(localVarDeclStmt->getTypeNode()))->getClassName()) {
            _methodLocalVar2Type[localVarDeclStmt->getVarName()] = "TraceApi";
        }
    }
    return true;
}

} // namespace turing
} // namespace suez
