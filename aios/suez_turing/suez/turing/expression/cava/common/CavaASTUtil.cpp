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
#include "suez/turing/expression/cava/common/CavaASTUtil.h"

#include <cstddef>
#include <vector>

#include "alog/Logger.h"
#include "cava/ast/ASTUtil.h"
#include "cava/ast/AmbTypeNode.h"
#include "cava/ast/CanonicalTypeNode.h"
#include "cava/ast/ClassDecl.h"
#include "cava/ast/Formal.h"
#include "cava/ast/MethodDecl.h"
#include "cava/ast/Modifier.h"
#include "cava/ast/Name.h"
#include "cava/ast/TypeNode.h"

using namespace std;
using ::cava::AmbTypeNode;
using ::cava::ClassDecl;
using ::cava::Formal;
using ::cava::MethodDecl;
using ::cava::TypeNode;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, CavaASTUtil);

CavaASTUtil::CavaASTUtil() {}

CavaASTUtil::~CavaASTUtil() {}

bool CavaASTUtil::matcher(::cava::ClassDecl *classDecl,
                          ::cava::CanonicalTypeNode &initResultType,
                          const std::string &initFirstParam,
                          const std::string &processFirstParam) {
    int flag = 0;
    for (auto method : classDecl->getMethods()) {
        if (matchCreateMethod(method, classDecl->getClassName())) {
            flag |= 1;
            continue;
        }
        auto &formals = method->getFormals();
        if (::cava::ASTUtil::matchMethod(method, "init", initResultType, false)) {
            if (formals.size() > 0 && checkFormal(initFirstParam, formals[0])) {
                flag |= 2;
                continue;
            }
        }
        if (method->getMethodName() == "process" && method->getModifier()->isStatic() == false) {
            if (formals.size() > 0 && checkFormal(processFirstParam, formals[0])) {
                flag |= 4;
                continue;
            }
        }
    }
    if (flag != 7) {
        if (!(flag & 1)) {
            AUTIL_LOG(DEBUG,
                      "class %s not contain create func for %s",
                      classDecl->getClassName().c_str(),
                      initFirstParam.c_str());
        }
        if (!(flag & 2)) {
            AUTIL_LOG(DEBUG,
                      "class %s not contain init func for %s",
                      classDecl->getClassName().c_str(),
                      initFirstParam.c_str());
        }
        if (!(flag & 4)) {
            AUTIL_LOG(DEBUG,
                      "class %s not contain process func for %s",
                      classDecl->getClassName().c_str(),
                      initFirstParam.c_str());
        }
        return false;
    }
    return true;
}

bool CavaASTUtil::checkFormal(const std::string &shortClassName, ::cava::Formal *formal) {
    auto typeNode = formal->getTypeNode();
    if (typeNode->getKind() != ::cava::AmbTypeNode::TNK_Amb) {
        return false;
    }
    ::cava::AmbTypeNode *ambType = static_cast<::cava::AmbTypeNode *>(typeNode);
    if (shortClassName == ambType->getClassName()) {
        return true;
    }
    return false;
}

bool CavaASTUtil::functionMatcher(::cava::ClassDecl *classDecl) {
    ::cava::CanonicalTypeNode booleanType(::cava::CanonicalTypeNode::CT_BOOLEAN);
    return matcher(classDecl, booleanType, "FunctionProvider", "MatchDoc");
}

bool CavaASTUtil::summaryMatcher(::cava::ClassDecl *classDecl) {
    ::cava::CanonicalTypeNode booleanType(::cava::CanonicalTypeNode::CT_BOOLEAN);
    return matcher(classDecl, booleanType, "SummaryProvider", "SummaryDoc");
}

bool CavaASTUtil::scorerMatcher(::cava::ClassDecl *classDecl) {
    ::cava::CanonicalTypeNode booleanType(::cava::CanonicalTypeNode::CT_BOOLEAN);
    return matcher(classDecl, booleanType, "ScorerProvider", "MatchDoc");
}

bool CavaASTUtil::featureLibMatcher(::cava::ClassDecl *classDecl) {
    if (classDecl->getFullClassName() != "ha3.FeatureLib") {
        return false;
    }
    int flag = 0;
    ::cava::CanonicalTypeNode booleanType(::cava::CanonicalTypeNode::CT_BOOLEAN);
    for (auto method : classDecl->getMethods()) {
        if (matchCreateMethod(method, classDecl->getClassName())) {
            flag |= 1;
        }
        if (::cava::ASTUtil::matchMethod(method, "init", booleanType, false)) {
            flag |= 2;
        }
    }
    return flag == 3;
}

bool CavaASTUtil::matchCreateMethod(::cava::MethodDecl *methodDecl, const string &className) {
    if (methodDecl->getMethodName() != "create") {
        return false;
    }
    auto retType = dynamic_cast<::cava::AmbTypeNode *>(methodDecl->getRetType());
    if (!retType) {
        return false;
    }
    const string &retTypeName = retType->getClassName();
    if (retTypeName != className) {
        return false;
    }
    if (!methodDecl->getModifier()->isStatic()) {
        return false;
    }
    auto name = retType->getName()->getPrefix();
    if (name != NULL) {
        return false;
    }
    if (!methodDecl->getFormals().empty()) {
        return false;
    }
    return true;
}

} // namespace turing
} // namespace suez
