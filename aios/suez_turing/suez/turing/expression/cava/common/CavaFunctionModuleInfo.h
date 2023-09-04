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

#include "autil/StringUtil.h"
#include "cava/ast/ClassDecl.h"
#include "cava/ast/MethodDecl.h"
#include "cava/codegen/CavaJitModule.h"
#include "cava/runtime/CavaCtx.h"
#include "suez/turing/expression/cava/common/CavaFieldTypeHelper.h"
#include "suez/turing/expression/cava/common/CavaModuleInfo.h"
#include "suez/turing/expression/cava/impl/ExpressionVector.h"
#include "suez/turing/expression/cava/impl/MatchDoc.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/function/FunctionInfo.h"
#include "suez/turing/expression/function/FunctionMap.h"

namespace ha3 {
class FunctionProvider;
}

namespace suez {
namespace turing {
class CavaFunctionModuleInfo : public CavaModuleInfo {
public:
    // boolean init(FunctionProvider provider)
    typedef bool (*InitProtoType)(void *, CavaCtx *, ::ha3::FunctionProvider *);

    CavaFunctionModuleInfo(const std::string &className,
                           ::cava::CavaJitModulePtr &cavaJitModule,
                           CreateProtoType createFunc);
    ~CavaFunctionModuleInfo();

private:
    CavaFunctionModuleInfo(const CavaFunctionModuleInfo &);
    CavaFunctionModuleInfo &operator=(const CavaFunctionModuleInfo &);

public:
    static bool create(::cava::CavaJitModulePtr &cavaJitModule,
                       ::cava::ClassDecl *classDecl,
                       std::unordered_map<std::string, CavaModuleInfoPtr> &moduleInfoMap,
                       const std::vector<FunctionInfo> &funcInfos);

private:
    static bool getTypeNameFromAstType(::cava::TypeNode *typeNode, std::string &typeName);
    static llvm::JITTargetAddress findProcessFunc(std::vector<std::string> strs,
                                                  const std::string &methodName,
                                                  ::cava::CavaJitModulePtr &cavaJitModule);
    static InitProtoType findInitFunc(const std::vector<std::string> &strs, ::cava::CavaJitModulePtr &cavaJitModule);

public:
    InitProtoType initFunc;
    llvm::JITTargetAddress processAddress;
    FunctionProtoInfoPtr functionProto;
    KeyValueMap params;
    std::string protoName;
    ::cava::MethodDecl *methodDecl;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(CavaFunctionModuleInfo);

} // namespace turing
} // namespace suez
