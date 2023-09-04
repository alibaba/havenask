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
#include "suez/turing/expression/cava/ast/FunctionGenerator.h"

#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/variadic/to_seq.hpp>
#include <cstddef>
#include <iostream>
#include <tuple>
#include <vector>

#include "alog/Logger.h"
#include "cava/ast/ASTUtil.h"
#include "cava/ast/AmbExpr.h"
#include "cava/ast/AmbTypeNode.h"
#include "cava/ast/AssignExpr.h"
#include "cava/ast/CallExpr.h"
#include "cava/ast/CanonicalTypeNode.h"
#include "cava/ast/ClassBodyDecl.h"
#include "cava/ast/ClassDecl.h"
#include "cava/ast/ExprStmt.h"
#include "cava/ast/Formal.h"
#include "cava/ast/LiteralExpr.h"
#include "cava/ast/LocalVarDeclStmt.h"
#include "cava/ast/MethodDecl.h"
#include "cava/ast/Modifier.h"
#include "cava/ast/ReturnStmt.h"
#include "cava/ast/TypeNode.h"
#include "cava/common/Common.h"
#include "fslib/util/FileUtil.h"
#include "suez/turing/expression/cava/common/CavaASTUtil.h"

namespace cava {
class CompoundStmt;
class FieldDecl;
class Name;
class VarDecl;
} // namespace cava

using namespace std;
using namespace cava;

namespace suez {
namespace turing {
AUTIL_LOG_SETUP(ast, FunctionGenerator);
EXPORT_PLUGIN_CREATOR(ASTRewriter, FunctionGenerator);

#define CAVA_NUMERIC_TYPE_SEQ()                                                                                        \
    BOOST_PP_VARIADIC_TO_SEQ("byte", "ubyte", "short", "ushort", "int", "uint", "long", "ulong", "float", "double")

#undef DECLARE_TUPLE1_BY_SEQ
#define DECLARE_TUPLE1_BY_DEQ(_1, _2, SEQ_ELEMENT) {SEQ_ELEMENT},

const vector<tuple<string>> OPERAND_NUMERIC_TYPES_STR{
    BOOST_PP_SEQ_FOR_EACH(DECLARE_TUPLE1_BY_DEQ, _, CAVA_NUMERIC_TYPE_SEQ())};

#undef DECLARE_TUPLE1_BY_SEQ

#undef DECLARE_TUPLE2_BY_SEQ
#define DECLARE_TUPLE2_BY_DEQ(_1, _2, SEQ_ELEMENT) {SEQ_ELEMENT, SEQ_ELEMENT},
const vector<tuple<string, string>> OPERAND_NUMERIC_NUMERIC_TYPES_STR{
    BOOST_PP_SEQ_FOR_EACH(DECLARE_TUPLE2_BY_DEQ, _, CAVA_NUMERIC_TYPE_SEQ())};
#undef DECLARE_TUPLE2_BY_SEQ

#undef DECLARE_NUMERIC_INT_BY_DEQ
#define DECLARE_NUMERIC_INT_BY_DEQ(_1, _2, SEQ_ELEMENT) {SEQ_ELEMENT, "long"},

#undef DECLARE_NUMERIC_VOID_BY_DEQ
#define DECLARE_NUMERIC_VOID_BY_DEQ(_1, _2, SEQ_ELEMENT) {SEQ_ELEMENT, "void"},

const vector<tuple<string, string>> OPERAND_NUMERIC_OPTIONAL_INTEGER_TYPES_STR{
    BOOST_PP_SEQ_FOR_EACH(DECLARE_NUMERIC_INT_BY_DEQ, _, CAVA_NUMERIC_TYPE_SEQ())
        BOOST_PP_SEQ_FOR_EACH(DECLARE_NUMERIC_VOID_BY_DEQ, _, CAVA_NUMERIC_TYPE_SEQ())};

#undef DECLARE_NUMERIC_INT_BY_DEQ
#undef DECLARE_NUMERIC_VOID_BY_DEQ

const vector<tuple<string>> OPERAND_CHARACTER_TYPES_STR{{"MChar"}};

#define METHOD_BODY_FUNC_KV_DECLARE(CAVA_MULTI_VALUE_TYPE)                                                             \
    {#CAVA_MULTI_VALUE_TYPE, createMethodBodyForMultiValueFormal<ha3::CAVA_MULTI_VALUE_TYPE>},

map<string, createMethodBodyFunc> CREATE_METHOD_BODY_FUNC_MAP = {
    METHOD_BODY_FUNC_KV_DECLARE(MChar)

    // TODO: need to support
    METHOD_BODY_FUNC_KV_DECLARE(MInt8) METHOD_BODY_FUNC_KV_DECLARE(MInt16) METHOD_BODY_FUNC_KV_DECLARE(MInt32)
        METHOD_BODY_FUNC_KV_DECLARE(MInt64) METHOD_BODY_FUNC_KV_DECLARE(MUInt8) METHOD_BODY_FUNC_KV_DECLARE(MUInt16)
            METHOD_BODY_FUNC_KV_DECLARE(MUInt32) METHOD_BODY_FUNC_KV_DECLARE(MUInt64)
                METHOD_BODY_FUNC_KV_DECLARE(MFloat) METHOD_BODY_FUNC_KV_DECLARE(MDouble)
                    METHOD_BODY_FUNC_KV_DECLARE(MString)};

TypeNode *FunctionGenerator::createArgNode(const string &nodeTypeName) {
    Location loc;
    TypeNode *typeNode = nullptr;
    CanonicalTypeNode::CanonicalType canonicalType;
    if (CanonicalTypeNode::getCanonicalType(nodeTypeName, canonicalType)) {
        typeNode = NodeFactory::createCanonicalTypeNode(*_astCtx, loc, canonicalType);
    } else {
        Name *fullClassName = NodeFactory::createName(*_astCtx, loc, NULL, _astCtx->allocString(nodeTypeName));
        typeNode = NodeFactory::createAmbTypeNode(*_astCtx, loc, fullClassName);
    }

    return typeNode;
}

bool FunctionGenerator::init(const map<string, string> &parameters, const string &configPath) {
    auto iter = parameters.find("srcPackageName");
    if (iter != parameters.end()) {
        _srcPackageName = iter->second;
    }

    iter = parameters.find("dstPackageName");
    if (iter != parameters.end()) {
        _dstPackageName = iter->second;
    }

    iter = parameters.find("genRulesFilePath");
    if (iter != parameters.end()) {
        string genRulesFilePath = iter->second;
        string rulesStr(fslib::util::FileUtil::readFile(genRulesFilePath));
        try {
            GenRules genRules;
            FastFromJsonString(genRules, rulesStr);
            for (auto &genRule : genRules) {
                _genRulesMap.insert(make_pair(genRule._name, genRule));
            }
        } catch (const exception &e) {
            AUTIL_LOG(ERROR,
                      "Json Parse Error, config path: %s, Json:[%s], exception:[%s]",
                      genRulesFilePath.c_str(),
                      rulesStr.c_str(),
                      e.what());
            return false;
        }
    } else {
        AUTIL_LOG(ERROR, "lack parameter \"genRulesFilePath\"");
        return false;
    }

    if (!initRewriters()) {
        return false;
    }

    return true;
}

bool FunctionGenerator::initImports() {
    // import ha3.*;
    Location loc;
    _imports.push_back(
        NodeFactory::createImport(*_astCtx,
                                  loc,
                                  NodeFactory::createName(*_astCtx, loc, NULL, _astCtx->allocString("ha3")),
                                  Import::IT_ON_DEMAND));
    return true;
}

bool FunctionGenerator::initRewriters() {
    if (!_addDefaultCtor.init({}, "")) {
        AUTIL_LOG(ERROR, "AddDefaultCtor init failed.");
        return false;
    }

    if (!_addDefaultCreate.init({}, "")) {
        AUTIL_LOG(ERROR, "AddDefaultCreate init failed.");
        return false;
    }

    if (!_functionRewrite.init({}, "")) {
        AUTIL_LOG(ERROR, "FunctionRewrite init failed.");
        return false;
    }

    return true;
}

bool FunctionGenerator::postProcess(ASTContext &astCtx) {
    if (!_addDefaultCtor.process(astCtx)) {
        AUTIL_LOG(ERROR, "AddDefaultCtor process failed.");
        return false;
    }
    if (!_addDefaultCreate.process(astCtx)) {
        AUTIL_LOG(ERROR, "AddDefaultCreate process failed.");
        return false;
    }
    if (!_functionRewrite.process(astCtx)) {
        AUTIL_LOG(ERROR, "FunctionRewrite process failed.");
        return false;
    }
    return true;
}

bool FunctionGenerator::initUdfModuleManager(ASTContext &astCtx) {
    auto &classDecls = astCtx.getClassDecls();
    for (auto *classDecl : classDecls) {
        auto *pkg = classDecl->getPackage();
        if (pkg == NULL) {
            continue;
        }

        auto *name = pkg->getName();
        if (name == NULL || name->getPostName() != _srcPackageName) {
            continue;
        }

        for (auto *methodDecl : classDecl->getMethods()) {
            if (!methodDecl->getModifier()->isStatic()) {
                continue;
            }

            auto &methodName = methodDecl->getMethodName();
            auto iter = _genRulesMap.find(methodName);
            if (iter == _genRulesMap.end()) {
                continue;
            }

            _udfModuleMgr.addUdfModule(methodName, classDecl, methodDecl, &(iter->second));
        }
    }
    return true;
}

bool FunctionGenerator::doProcess() {
    auto &udfModMap = _udfModuleMgr.getUdfModuleMap();
    for (auto iter : udfModMap) {
        auto [classDecl, methodDecl, genRule] = iter.second;
        if (!addNewClass(classDecl, methodDecl, *genRule)) {
            AUTIL_LOG(ERROR, "Add new class[%s] failed.", classDecl->getClassName().c_str());
            return false;
        }
    }
    return true;
}

bool FunctionGenerator::process(ASTContext &astCtx) {
    _astCtx = &astCtx;

    initImports();
    initUdfModuleManager(astCtx);
    if (!doProcess()) {
        AUTIL_LOG(ERROR, "Do process failed.");
        return false;
    }

    if (!postProcess(astCtx)) {
        AUTIL_LOG(ERROR, "Post process failed.");
        return false;
    }
    return true;
}

MethodDecl *FunctionGenerator::genInitMethod() {
    // gen "boolean init(FunctionProvider provider) {return true;}"
    Location loc;
    auto *modifier = NodeFactory::createModifier(*_astCtx, loc);

    // gen "boolean init(FunctionProvider provider)"
    auto *initFormals = _astCtx->allocFormalVec();
    auto *package = NodeFactory::createName(*_astCtx, loc, NULL, _astCtx->allocString("ha3"));
    auto *fullClassName = NodeFactory::createName(*_astCtx, loc, package, _astCtx->allocString("FunctionProvider"));
    auto *typeNode = NodeFactory::createAmbTypeNode(*_astCtx, loc, fullClassName);
    auto *varDecl = NodeFactory::createVarDecl(*_astCtx, loc, _astCtx->allocString("provider"));
    auto *formal = NodeFactory::createFormal(*_astCtx, loc, typeNode, varDecl);
    initFormals->push_back(formal);

    auto *initRetType = NodeFactory::createCanonicalTypeNode(*_astCtx, loc, CanonicalTypeNode::CT_BOOLEAN);
    auto *initMethodDecl =
        NodeFactory::createMethodDecl(*_astCtx, loc, modifier, _astCtx->allocString("init"), initFormals, initRetType);

    auto methodBodyStmtVec = _astCtx->allocStmtVec();

    // "return true"
    auto *returnExpr = NodeFactory::createTypedLiteralExpr<bool>(*_astCtx, loc, LiteralExpr::LT_BOOLEAN, true);
    auto *returnStmt = NodeFactory::createReturnStmt(*_astCtx, loc, returnExpr);
    methodBodyStmtVec->push_back(returnStmt);

    auto *body = NodeFactory::createCompoundStmt(*_astCtx, loc, methodBodyStmtVec);
    initMethodDecl->setBody(body);
    return initMethodDecl;
}

bool FunctionGenerator::addNewClass(ClassDecl *rawClassDecl, MethodDecl *rawMethodDecl, GenRule &genRule) {
    Location loc;
    auto *modifier = NodeFactory::createModifier(*_astCtx, loc);

    auto *classBodyDecl = NodeFactory::createClassBodyDecl(*_astCtx, loc);
    auto *initMethodDecl = genInitMethod();
    classBodyDecl->addMethod(initMethodDecl);

    auto *rawClassName = NodeFactory::createName(
        *_astCtx, loc, rawClassDecl->getPackage()->getName(), _astCtx->allocString(rawClassDecl->getClassName()));

    auto *callNameExpr = NodeFactory::createAmbExpr(*_astCtx, loc, rawClassName);

    vector<MethodDecl *> processMethods;

    if (!genProcessMethods(genRule, callNameExpr, rawMethodDecl->getMethodName(), processMethods)) {
        AUTIL_LOG(ERROR, "class[%s] genProcessMethods failed.", rawMethodDecl->getMethodName().c_str());
        return false;
    }

    for (auto *processMethodDecl : processMethods) {
        classBodyDecl->addMethod(processMethodDecl);
    }

    auto &newClassName = rawMethodDecl->getMethodName();
    auto *classDecl =
        NodeFactory::createClassDecl(*_astCtx, loc, modifier, _astCtx->allocString(newClassName), classBodyDecl);
    auto *packageName = NodeFactory::createName(*_astCtx, loc, NULL, _astCtx->allocString(_dstPackageName));
    auto *package = NodeFactory::createPackage(*_astCtx, loc, packageName);
    classDecl->setPackage(package);
    classDecl->setImports(&_imports);
    return true;
}

bool FunctionGenerator::genProcessMethods(const GenRule &genRule,
                                          Expr *callNameExpr,
                                          const string &methodName,
                                          vector<MethodDecl *> &newMethodDecls) {
    bool ret = false;
    newMethodDecls.clear();
    switch (genRule._operandType) {
    case OPERAND_NUMERIC:
        ret = genProcessMethods(genRule._retType, callNameExpr, methodName, OPERAND_NUMERIC_TYPES_STR, newMethodDecls);
        break;
    case OPERAND_NUMERIC_NUMERIC:
        ret = genProcessMethods(
            genRule._retType, callNameExpr, methodName, OPERAND_NUMERIC_NUMERIC_TYPES_STR, newMethodDecls);
        break;
    case OPERAND_NUMERIC_OPTIONAL_INTEGER:
        ret = genProcessMethods(
            genRule._retType, callNameExpr, methodName, OPERAND_NUMERIC_OPTIONAL_INTEGER_TYPES_STR, newMethodDecls);
        break;
    case OPERAND_CHARACTER:
        ret =
            genProcessMethods(genRule._retType, callNameExpr, methodName, OPERAND_CHARACTER_TYPES_STR, newMethodDecls);
        break;
    default:
        AUTIL_LOG(ERROR, "unknown operand args type:[%d]", (int32_t)genRule._operandType);
        break;
    }
    return ret;
}

Formal *FunctionGenerator::genMatchDocFormal() {
    // gen "ha3.MatchDoc doc"
    Location loc;
    auto *package = NodeFactory::createName(*_astCtx, loc, NULL, _astCtx->allocString("ha3"));
    auto *fullClassName = NodeFactory::createName(*_astCtx, loc, package, _astCtx->allocString("MatchDoc"));
    auto *typeNode = NodeFactory::createAmbTypeNode(*_astCtx, loc, fullClassName);
    auto *varDecl = NodeFactory::createVarDecl(*_astCtx, loc, _astCtx->allocString("doc"));
    auto *formal = NodeFactory::createFormal(*_astCtx, loc, typeNode, varDecl);
    return formal;
}

TypeNode *FunctionGenerator::genMethodReturnType(GenRuleReturnType typeRule, const vector<Formal *> &methodFormals) {
    Location loc;
    TypeNode *retType = nullptr;
    switch (typeRule) {
    case RET_INTEGER:
        retType = NodeFactory::createCanonicalTypeNode(*_astCtx, loc, CanonicalTypeNode::CT_INT);
        break;
    case RET_DOUBLE:
        retType = NodeFactory::createCanonicalTypeNode(*_astCtx, loc, CanonicalTypeNode::CT_DOUBLE);
        break;
    case RET_ARG0:
        if (methodFormals.size() >= 2u) {
            Formal *argFormal = methodFormals[1];
            retType = argFormal->getTypeNode();
        } else {
            AUTIL_LOG(ERROR, "RET_ARG0 infered failed, methodFormal's size:[%d]", (int32_t)methodFormals.size());
        }
        break;
    default:
        AUTIL_LOG(ERROR, "unknown ret type:[%d]", (int32_t)typeRule);
        break;
    }
    return retType;
}

CompoundStmt *FunctionGenerator::genProcessMethodBody(Expr *callNameExpr,
                                                      const string &methodName,
                                                      const vector<Formal *> &methodFormals) {
    Location loc;
    CompoundStmt *body = nullptr;
    auto *methodBodyStmtVec = _astCtx->allocStmtVec();
    auto *argsExprVec = _astCtx->allocExprVec();
    for (size_t i = 1; i < methodFormals.size(); i++) {
        auto *formal = methodFormals[i];
        auto *type = formal->getTypeNode();
        auto formalName = formal->getFormalName();
        if (type->getKind() == TypeNode::TNK_Canonical) {
            createMethodBodyForCanonicalFormal(_astCtx, formal->getFormalName(), methodBodyStmtVec, argsExprVec);
        } else if (type->getKind() == TypeNode::TNK_Amb) {
            auto &className = ((AmbTypeNode *)type)->getClassName();
            auto iter = CREATE_METHOD_BODY_FUNC_MAP.find(className);
            if (iter == CREATE_METHOD_BODY_FUNC_MAP.end()) {
                AUTIL_LOG(ERROR,
                          "class[%s] is illegal, process method's args only support canonical or multi value type.",
                          iter->first.c_str());
                return nullptr;
            }

            auto createFunc = iter->second;
            if (!createFunc(_astCtx, formal->getFormalName(), methodBodyStmtVec, argsExprVec)) {
                AUTIL_LOG(ERROR, "class[%s]'s createFunc unimplemented.", iter->first.c_str());
                return nullptr;
            }
        }
    }

    auto *returnExpr =
        NodeFactory::createCallExpr(*_astCtx, loc, callNameExpr, _astCtx->allocString(methodName), argsExprVec);
    auto *returnStmt = NodeFactory::createReturnStmt(*_astCtx, loc, returnExpr);
    methodBodyStmtVec->push_back(returnStmt);
    body = NodeFactory::createCompoundStmt(*_astCtx, loc, methodBodyStmtVec);
    return body;
}

} // namespace turing
} // namespace suez
