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

#include <map>
#include <stddef.h>
#include <string>
#include <unordered_map>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "cava/ast/ASTContext.h"
#include "cava/ast/ASTNode.h"
#include "cava/ast/NodeFactory.h"
#include "cava/plugin/ASTRewriter.h"
#include "cava/plugin/AddDefaultCreate.h"
#include "cava/plugin/AddDefaultCtor.h"
#include "suez/turing/expression/cava/ast/FunctionRewrite.h"
#include "suez/turing/expression/cava/ast/UdfModuleManager.h"
#include "suez/turing/expression/cava/impl/CavaMultiValueTyped.h"
#include "suez/turing/expression/common.h"

namespace cava {
class ASTContext;
class ClassDecl;
class MethodDecl;
class CompoundStmt;
class TypeNode;
class Formal;
class Expr;
class Import;
} // namespace cava

namespace suez {
namespace turing {

enum GenRuleReturnType {
    RET_UNKNOWN,
    RET_INTEGER,
    RET_DOUBLE,
    RET_ARG0
};

enum GenRuleOperandType {
    OPERAND_UNKNOWN,
    OPERAND_NUMERIC,
    OPERAND_NUMERIC_NUMERIC,
    OPERAND_NUMERIC_OPTIONAL_INTEGER,
    OPERAND_CHARACTER
};

template <typename tuple_t>
constexpr auto get_array_from_tuple(tuple_t &&tuple) {
    constexpr auto get_array = [](auto &&...x) { return std::array{std::forward<decltype(x)>(x)...}; };
    return std::apply(get_array, std::forward<tuple_t>(tuple));
}

using createMethodBodyFunc = std::function<bool(
    ::cava::ASTContext *, const std::string &, std::vector<::cava::Stmt *> *, std::vector<::cava::Expr *> *)>;

createMethodBodyFunc createMethodBodyForCanonicalFormal = [](::cava::ASTContext *astCtx,
                                                             const std::string &formalName,
                                                             std::vector<::cava::Stmt *> *,
                                                             std::vector<::cava::Expr *> *argsExprVec) {
    ::cava::Location loc;
    auto *arg = ::cava::NodeFactory::createAmbExpr(
        *astCtx, loc, ::cava::NodeFactory::createName(*astCtx, loc, NULL, astCtx->allocString(formalName)));
    argsExprVec->push_back(arg);
    return true;
};

template <typename T>
bool createMethodBodyForMultiValueFormal(::cava::ASTContext *astCtx,
                                         const std::string &formalName,
                                         std::vector<::cava::Stmt *> *methodBodyStmtVec,
                                         std::vector<::cava::Expr *> *argsExprVec) {
    return false;
}

template <>
bool createMethodBodyForMultiValueFormal<ha3::MChar>(::cava::ASTContext *astCtx,
                                                     const std::string &formalName,
                                                     std::vector<::cava::Stmt *> *methodBodyStmtVec,
                                                     std::vector<::cava::Expr *> *argsExprVec) {
    // assume formalName is "varX";
    ::cava::Location loc;
    ::cava::AmbExpr *assignLeftExpr = NULL;
    auto formalNameInner = formalName;

    //"CString varX_CString;"
    formalNameInner += CAVA_INNER_METHOD_SEP + std::string("CString");
    auto *type = ::cava::NodeFactory::createAmbTypeNode(
        *astCtx, loc, ::cava::NodeFactory::createName(*astCtx, loc, NULL, astCtx->allocString("CString")));
    auto *varDecl = ::cava::NodeFactory::createVarDecl(*astCtx, loc, astCtx->allocString(formalNameInner));
    auto *localVar = ::cava::NodeFactory::createLocalVarDeclStmt(*astCtx, loc, type, varDecl);
    methodBodyStmtVec->push_back(localVar);

    //"varX_CString = varX.toCString();"
    assignLeftExpr = ::cava::NodeFactory::createAmbExpr(
        *astCtx, loc, ::cava::NodeFactory::createName(*astCtx, loc, NULL, astCtx->allocString(formalNameInner)));
    auto *formalExpr = ::cava::NodeFactory::createAmbExpr(
        *astCtx, loc, ::cava::NodeFactory::createName(*astCtx, loc, NULL, astCtx->allocString(formalName)));
    auto *rightExpr = ::cava::NodeFactory::createCallExpr(
        *astCtx, loc, formalExpr, astCtx->allocString("toCString"), astCtx->allocExprVec());

    auto *assignExpr =
        ::cava::NodeFactory::createAssignExpr(*astCtx, loc, assignLeftExpr, ::cava::AssignExpr::AT_EQ, rightExpr);
    methodBodyStmtVec->push_back(::cava::NodeFactory::createExprStmt(*astCtx, loc, assignExpr));

    auto *arg = ::cava::NodeFactory::createAmbExpr(
        *astCtx, loc, ::cava::NodeFactory::createName(*astCtx, loc, NULL, astCtx->allocString(formalNameInner)));

    // make "varX_CString" as an argument
    argsExprVec->push_back(arg);
    return true;
}

class GenRule : public autil::legacy::Jsonizable {
public:
    GenRule() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        std::string retType;
        std::string operandType;
        json.Jsonize("name", _name, _name);

        json.Jsonize("return_type", retType, retType);
        _retType = convertToReturnType(retType);

        json.Jsonize("operand_type", operandType, operandType);
        _operandType = convertToOperandType(operandType);
    }

public:
    static GenRuleReturnType convertToReturnType(const std::string &str) {
        auto retType = RET_UNKNOWN;
        auto typeStr = str;
        autil::StringUtil::toLowerCase(typeStr);
        if ("integer" == typeStr) {
            retType = RET_INTEGER;
        } else if ("double" == typeStr) {
            retType = RET_DOUBLE;
        } else if ("arg0" == typeStr) {
            retType = RET_ARG0;
        }

        return retType;
    }
    static GenRuleOperandType convertToOperandType(const std::string &str) {
        auto typeStr = str;
        autil::StringUtil::toLowerCase(typeStr);
        auto operandType = OPERAND_UNKNOWN;
        if ("numeric" == typeStr) {
            operandType = OPERAND_NUMERIC;
        } else if ("numeric_numeric" == typeStr) {
            operandType = OPERAND_NUMERIC_NUMERIC;
        } else if ("numeric_optional_integer" == typeStr) {
            operandType = OPERAND_NUMERIC_OPTIONAL_INTEGER;
        } else if ("character" == typeStr) {
            operandType = OPERAND_CHARACTER;
        }

        return operandType;
    }

public:
    std::string _name;
    GenRuleReturnType _retType;
    GenRuleOperandType _operandType;
};

typedef std::vector<GenRule> GenRules;

class FunctionGenerator : public ::cava::ASTRewriter {
public:
    FunctionGenerator()
        : _astCtx(NULL)
        , _methodDecl(NULL)
        , _classDecl(NULL)
        , _srcPackageName("__builtin_udf__")
        , _dstPackageName("__self_register__") {}
    ~FunctionGenerator() {}

private:
    FunctionGenerator(const FunctionGenerator &);
    FunctionGenerator &operator=(const FunctionGenerator &);

public:
    bool init(const std::map<std::string, std::string> &parameters, const std::string &configPath) override;
    bool process(::cava::ASTContext &astCtx) override;

private:
    bool onlyForPreload() override { return true; }
    bool initImports();
    bool initRewriters();
    bool initUdfModuleManager(::cava::ASTContext &astCtx);
    bool doProcess();
    bool postProcess(::cava::ASTContext &astCtx);
    bool addNewClass(::cava::ClassDecl *rawClassDecl, ::cava::MethodDecl *rawMethodDecl, GenRule &genRule);
    bool genProcessMethods(const GenRule &genRule,
                           ::cava::Expr *callNameExpr,
                           const std::string &methodName,
                           std::vector<::cava::MethodDecl *> &newMethods);

    ::cava::CompoundStmt *genProcessMethodBody(::cava::Expr *callNameExpr,
                                               const std::string &methodName,
                                               const std::vector<::cava::Formal *> &methodFormals);
    ::cava::TypeNode *genMethodReturnType(GenRuleReturnType typeRule,
                                          const std::vector<::cava::Formal *> &methodFormals);
    ::cava::Formal *genMatchDocFormal();

    ::cava::TypeNode *createArgNode(const std::string &nodeTypeName);

    template <typename... ArgsDef>
    bool genProcessMethods(GenRuleReturnType typeRule,
                           ::cava::Expr *callNameExpr,
                           const std::string &methodName,
                           const std::vector<std::tuple<ArgsDef...>> &argsDefVec,
                           std::vector<::cava::MethodDecl *> &newMethodDecls) {

        ::cava::Location loc;
        auto *modifier = ::cava::NodeFactory::createModifier(*_astCtx, loc);
        std::vector<std::vector<::cava::Formal *> *> multiMethodFormals;
        if (!genProcessMethodFormals(argsDefVec, multiMethodFormals)) {
            AUTIL_LOG(ERROR, "genProcessMethodFormals failed.");
            return false;
        } else {
            for (auto const &oneMethodFormals : multiMethodFormals) {
                auto *retType = genMethodReturnType(typeRule, *oneMethodFormals);
                if (!retType) {
                    AUTIL_LOG(ERROR, "genMethodReturnType failed.");
                    return false;
                }
                auto *methodBody = genProcessMethodBody(callNameExpr, methodName, *oneMethodFormals);
                if (!methodBody) {
                    AUTIL_LOG(ERROR, "genProcessMethodBody failed.");
                    return false;
                }
                auto *methodDecl = ::cava::NodeFactory::createMethodDecl(
                    *_astCtx, loc, modifier, _astCtx->allocString("process"), oneMethodFormals, retType);
                methodDecl->setBody(methodBody);
                newMethodDecls.push_back(methodDecl);
            }
        }
        return true;
    }

    template <typename... ArgsDef>
    bool genProcessMethodFormals(const std::vector<std::tuple<ArgsDef...>> &argsDefVec,
                                 std::vector<std::vector<::cava::Formal *> *> &multiMethodFormals) {
        ::cava::Location loc;
        auto *matchDocFormal = genMatchDocFormal();

        for (auto argsDef : argsDefVec) {
            auto *oneMethodFormals = _astCtx->allocFormalVec();
            oneMethodFormals->push_back(matchDocFormal);
            auto argsDefArr = get_array_from_tuple(argsDef);
            int counter = 0;
            for (auto argType : argsDefArr) {
                if ("void" == argType) {
                    break;
                }
                auto counterStr = autil::StringUtil::toString(counter++);
                auto *var = ::cava::NodeFactory::createVarDecl(*_astCtx, loc, _astCtx->allocString("var" + counterStr));
                auto *type = createArgNode(argType);
                ::cava::Formal *varFormal = ::cava::NodeFactory::createFormal(*_astCtx, loc, type, var);
                oneMethodFormals->push_back(varFormal);
            }

            multiMethodFormals.push_back(oneMethodFormals);
        }

        return true;
    }

    ::cava::MethodDecl *genInitMethod();

private:
    ::cava::ASTContext *_astCtx;
    ::cava::MethodDecl *_methodDecl;
    ::cava::ClassDecl *_classDecl;
    std::string _srcPackageName;
    std::string _dstPackageName;

    std::map<std::string, GenRule> _genRulesMap;
    UdfModuleManager _udfModuleMgr;
    static std::unordered_map<std::string, std::string> Type2MethodName;
    ::cava::AddDefaultCtor _addDefaultCtor;
    ::cava::AddDefaultCreate _addDefaultCreate;
    suez::turing::FunctionRewrite _functionRewrite;
    std::vector<::cava::Import *> _imports;
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(FunctionGenerator);

} // namespace turing
} // namespace suez
