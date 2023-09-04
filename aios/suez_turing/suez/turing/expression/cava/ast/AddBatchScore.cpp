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
#include "suez/turing/expression/cava/ast/AddBatchScore.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <vector>

#include "alog/Logger.h"
#include "cava/ast/ASTContext.h"
#include "cava/ast/ASTUtil.h"
#include "cava/ast/AmbExpr.h"
#include "cava/ast/AmbTypeNode.h"
#include "cava/ast/CallExpr.h"
#include "cava/ast/CanonicalTypeNode.h"
#include "cava/ast/ClassBodyDecl.h"
#include "cava/ast/ClassDecl.h"
#include "cava/ast/ExprStmt.h"
#include "cava/ast/ForStmt.h"
#include "cava/ast/MethodDecl.h"
#include "cava/ast/NodeFactory.h"
#include "cava/ast/ReturnStmt.h"
#include "cava/common/Common.h"
#include "cava/parse/location.hh"
#include "suez/turing/expression/cava/common/CavaASTUtil.h"

namespace cava {
class AssignExpr;
class BinaryOpExpr;
class CompoundStmt;
class Expr;
class Formal;
class LiteralExpr;
class Modifier;
class Stmt;
class TypeNode;
class UnaryOpExpr;
class VarDecl;
} // namespace cava

using namespace std;
// using namespace ::cava::MethodDecl;
using ::cava::AmbTypeNode;
using ::cava::AssignExpr;
using ::cava::BinaryOpExpr;
using ::cava::CanonicalTypeNode;
using ::cava::CompoundStmt;
using ::cava::Expr;
using ::cava::Formal;
using ::cava::LiteralExpr;
using ::cava::location;
using ::cava::MethodDecl;
using ::cava::NodeFactory;
using ::cava::Stmt;
using ::cava::UnaryOpExpr;
using ::cava::VarDecl;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(plugins, AddBatchScore);
EXPORT_PLUGIN_CREATOR(ASTRewriter, AddBatchScore);

AddBatchScore::AddBatchScore() {}

AddBatchScore::~AddBatchScore() {}

::cava::MethodDecl *AddBatchScore::genBatchProcessDecl(::cava::ASTContext &astCtx) {
    location loc;
    ::cava::Modifier *modifier = ::cava::NodeFactory::allocModifier(astCtx);
    std::string *methodName = astCtx.allocString("batchProcess");
    std::vector<Formal *> *formals = astCtx.allocFormalVec();
    AmbTypeNode *paraTypeNode = NodeFactory::createAmbTypeNode(
        astCtx, loc, NodeFactory::createName(astCtx, loc, NULL, astCtx.allocString("Ha3CavaScorerParam")));

    VarDecl *varDecl = NodeFactory::createVarDecl(astCtx, loc, astCtx.allocString("scorerParam"));
    formals->push_back(NodeFactory::createFormal(astCtx, loc, paraTypeNode, varDecl));
    ::cava::TypeNode *retType =
        ::cava::NodeFactory::createCanonicalTypeNode(astCtx, loc, ::cava::CanonicalTypeNode::CT_VOID);
    return ::cava::NodeFactory::createMethodDecl(astCtx, loc, modifier, methodName, formals, retType);
}

/*
  for (int i, i = 0; i < scorerParam.getScoreDocCount(); ++i)
  {
       scorerParam.getScoreFieldRef().set(scorerParam.getMatchDoc(i), process(scorerParam.getMatchDoc(i)));
  }
  return ;
*/
::cava::CompoundStmt *AddBatchScore::genBatchProcessBody(::cava::ASTContext &astCtx) {
    location loc;
    auto scorerParamExpr = NodeFactory::createAmbExpr(
        astCtx, loc, NodeFactory::createName(astCtx, loc, NULL, astCtx.allocString("scorerParam")));
    auto docCountCallExpr = NodeFactory::createCallExpr(
        astCtx, loc, scorerParamExpr, astCtx.allocString("getScoreDocCount"), astCtx.allocExprVec());

    std::string indexName = "i";
    auto indexExpr = NodeFactory::createAmbExpr(
        astCtx, loc, NodeFactory::createName(astCtx, loc, NULL, astCtx.allocString(indexName)));
    auto forStmt = ::cava::ASTUtil::createForStmt(astCtx, docCountCallExpr, indexName);

    auto refCallExpr = NodeFactory::createCallExpr(
        astCtx, loc, scorerParamExpr, astCtx.allocString("getScoreFieldRef"), astCtx.allocExprVec());
    std::string *methodName = astCtx.allocString("set");
    std::vector<Expr *> *setArgs = astCtx.allocExprVec();
    // arg1
    std::vector<Expr *> *getMatchDocArgs = astCtx.allocExprVec();
    getMatchDocArgs->push_back(indexExpr);
    auto matchDocCallExpr =
        NodeFactory::createCallExpr(astCtx, loc, scorerParamExpr, astCtx.allocString("getMatchDoc"), getMatchDocArgs);
    setArgs->push_back(matchDocCallExpr);

    // arg2
    std::string *processMethodName = astCtx.allocString("process");
    std::vector<Expr *> *processArgs = astCtx.allocExprVec();
    processArgs->push_back(matchDocCallExpr);
    auto processCallExpr = NodeFactory::createCallExpr(astCtx, loc, NULL, processMethodName, processArgs);
    setArgs->push_back(processCallExpr);
    auto callExpr = NodeFactory::createCallExpr(astCtx, loc, refCallExpr, methodName, setArgs);

    forStmt->setBody(NodeFactory::createExprStmt(astCtx, loc, callExpr));
    vector<Stmt *> *methodBody = astCtx.allocStmtVec();
    methodBody->push_back(forStmt);
    // create ret
    auto returnStmt = NodeFactory::createReturnStmt(astCtx, loc, NULL);
    methodBody->push_back(returnStmt);
    return NodeFactory::createCompoundStmt(astCtx, loc, methodBody);
}

bool AddBatchScore::process(::cava::ASTContext &astCtx) {
    ::cava::ASTUtil::ClassMatcher scorerMatch =
        std::bind(suez::turing::CavaASTUtil::scorerMatcher, std::placeholders::_1);
    for (auto classDecl : astCtx.getClassDecls()) {
        if (!scorerMatch(classDecl)) {
            continue;
        }
        std::vector<MethodDecl *> batchProcessVec = ::cava::ASTUtil::getMethodDecls(classDecl, "batchProcess");
        if (batchProcessVec.size() > 0) {
            continue;
        }

        AUTIL_LOG(DEBUG, "start to process class:%s", classDecl->getClassName().c_str());
        ::cava::MethodDecl *method = genBatchProcessDecl(astCtx);
        ::cava::CompoundStmt *body = genBatchProcessBody(astCtx);
        method->setBody(body);
        AUTIL_LOG(DEBUG, "\n%s", method->toString().c_str());
        ::cava::ClassBodyDecl *classBodyDecl = classDecl->getClassBodyDecl();
        classBodyDecl->addMethod(method);
    }
    return true;
}

} // namespace turing
} // namespace suez
