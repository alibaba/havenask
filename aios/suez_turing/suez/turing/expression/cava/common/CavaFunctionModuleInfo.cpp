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
#include "suez/turing/expression/cava/common/CavaFunctionModuleInfo.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "cava/ast/ASTUtil.h"
#include "cava/ast/ClassDecl.h"
#include "cava/ast/Formal.h"
#include "cava/ast/MethodDecl.h"
#include "cava/ast/Modifier.h"
#include "cava/codegen/CavaJitModule.h"
#include "suez/turing/expression/cava/common/CavaFieldTypeHelper.h"
#include "suez/turing/expression/function/FunctionInfo.h"

namespace cava {
class TypeNode;
} // namespace cava

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, CavaFunctionModuleInfo);

CavaFunctionModuleInfo::CavaFunctionModuleInfo(const std::string &className,
                                               ::cava::CavaJitModulePtr &cavaJitModule,
                                               CreateProtoType createFunc)
    : CavaModuleInfo(className, cavaJitModule, createFunc) {}

CavaFunctionModuleInfo::~CavaFunctionModuleInfo() {}

bool CavaFunctionModuleInfo::create(::cava::CavaJitModulePtr &cavaJitModule,
                                    ::cava::ClassDecl *classDecl,
                                    std::unordered_map<std::string, CavaModuleInfoPtr> &moduleInfoMap,
                                    const std::vector<FunctionInfo> &funcInfos) {
    const std::string &className = classDecl->getFullClassName();
    const vector<string> &strs = autil::StringUtil::split(className, ".");
    auto createFunc = findCreateFunc(strs, cavaJitModule);
    if (!createFunc) {
        AUTIL_LOG(ERROR, "find create func failed,class: %s", className.c_str());
        return false;
    }
    auto initFunc = findInitFunc(strs, cavaJitModule);
    if (!initFunc) {
        AUTIL_LOG(ERROR, "find init func failed,class: %s", className.c_str());
        return false;
    }
    std::vector<std::string> processNames;
    for (auto methodDecl : classDecl->getMethods()) {
        if (methodDecl->getMethodName() == "process" && methodDecl->getModifier()->isStatic() == false) {
            std::string processName = CAVA_INNER_METHOD_NAME;
            bool isNeededProcessFunc = true;
            uint32_t argsCnt = 0;
            std::vector<::cava::Formal *> &formals = methodDecl->getFormals();
            if (formals.size() < 1 || ::cava::ASTUtil::getTypeShortName(formals[0]->getTypeNode()) != "MatchDoc") {
                continue;
            }

            for (size_t i = 1; i < formals.size(); ++i) {
                std::string argTypeName;
                if (!getTypeNameFromAstType(formals[i]->getTypeNode(), argTypeName)) {
                    isNeededProcessFunc = false;
                    break;
                }
                processName += CAVA_INNER_METHOD_SEP + argTypeName;
                argsCnt++;
            }
            if (!isNeededProcessFunc) {
                continue;
            }

            std::string retTypeName;
            if (!getTypeNameFromAstType(methodDecl->getRetType(), retTypeName)) {
                continue;
            }
            auto retType = ha3::CavaFieldTypeHelper::getTypePair(retTypeName);

            auto processAddress = findProcessFunc(strs, processName, cavaJitModule);
            if (!processAddress) {
                AUTIL_LOG(ERROR, "find process func failed: %s", processName.c_str());
                return false;
            }

            for (auto &info : funcInfos) {
                std::string protoName = info.funcName + CAVA_INNER_METHOD_SEP + processName;
                if (moduleInfoMap.find(protoName) != moduleInfoMap.end()) {
                    continue;
                }
                CavaFunctionModuleInfo *cavaFuncModule =
                    new CavaFunctionModuleInfo(className, cavaJitModule, createFunc);
                cavaFuncModule->initFunc = initFunc;
                if (info.matchInfoType == FunctionInfo::SUB_MATCH_INFO_TYPE) {
                    cavaFuncModule->functionProto.reset(new FunctionProtoInfo(
                        FUNCTION_UNLIMITED_ARGUMENT_COUNT,
                        retType.first,
                        retType.second,
                        FuncActionScopeType::FUNC_ACTION_SCOPE_SUB_DOC,
                        info.isDeterministic)); // FUNCTION_UNLIMITED_ARGUMENT_COUNT是因为常量参数的存在
                } else {
                    cavaFuncModule->functionProto.reset(new FunctionProtoInfo(
                        FUNCTION_UNLIMITED_ARGUMENT_COUNT,
                        retType.first,
                        retType.second,
                        FuncActionScopeType::FUNC_ACTION_SCOPE_ADAPTER,
                        info.isDeterministic)); // FUNCTION_UNLIMITED_ARGUMENT_COUNT是因为常量参数的存在
                }
                cavaFuncModule->processAddress = processAddress;
                cavaFuncModule->params = info.params;
                cavaFuncModule->protoName = protoName;
                cavaFuncModule->methodDecl = methodDecl;
                moduleInfoMap[protoName] = CavaModuleInfoPtr(cavaFuncModule);
                AUTIL_LOG(INFO, "add cava module[%s] in moduleInfoMap", protoName.c_str());
            }
        }
    }
    return true;
}

// should use CGClassInfo to fetech method info
bool CavaFunctionModuleInfo::getTypeNameFromAstType(::cava::TypeNode *typeNode, std::string &typeName) {
    typeName = ::cava::ASTUtil::getTypeShortName(typeNode);
    auto type = ha3::CavaFieldTypeHelper::getType(typeName);
    if (type == ha3::CAVA_FIELD_TYPE::ct_UnkownType) {
        AUTIL_LOG(ERROR, "only support basic type or multi type: %s", typeName.c_str());
        return false;
    }

    return true;
}

CavaFunctionModuleInfo::InitProtoType CavaFunctionModuleInfo::findInitFunc(const vector<string> &strs,
                                                                           ::cava::CavaJitModulePtr &cavaJitModule) {
    vector<string> params;
    params.push_back("PN3ha316FunctionProviderE");
    const string &initFuncName = genInitFunc(strs, params);
    auto jitSymbol = cavaJitModule->findSymbol(initFuncName);
    InitProtoType ret = (InitProtoType)jitSymbol.getAddress();
    if (!ret) {
        AUTIL_LOG(ERROR, "find init func failed: %s", initFuncName.c_str());
    }
    return ret;
}

llvm::JITTargetAddress CavaFunctionModuleInfo::findProcessFunc(std::vector<std::string> strs,
                                                               const std::string &methodName,
                                                               ::cava::CavaJitModulePtr &cavaJitModule) {
    vector<string> params;
    params.push_back("PN3ha38MatchDocE");
    params.push_back("PN6unsafe16ExpressionVectorE");
    const string &processFuncName = genProcessFunc(strs, params, methodName);
    auto jitSymbol = cavaJitModule->findSymbol(processFuncName);
    if (!jitSymbol) {
        return 0;
    }
    return jitSymbol.getAddress();
}

} // namespace turing
} // namespace suez
