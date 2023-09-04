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
#include "suez/turing/expression/cava/common/CavaModuleInfo.h"

#include <iosfwd>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/Support/Error.h>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "cava/codegen/CavaJitModule.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, CavaModuleInfo);

CavaModuleInfo::CavaModuleInfo(const std::string &className_,
                               ::cava::CavaJitModulePtr &cavaJitModule_,
                               CreateProtoType createFunc_)
    : className(className_), createFunc(createFunc_), cavaJitModule(cavaJitModule_) {}

CavaModuleInfo::~CavaModuleInfo() {}

std::string CavaModuleInfo::genCreateFunc(const std::vector<std::string> &strs) {
    std::string ret = "_ZN";
    for (const auto &str : strs) {
        ret += autil::StringUtil::toString(str.size()) + str;
    }
    ret += "6createEP7CavaCtx";
    return ret;
}
CreateProtoType CavaModuleInfo::findCreateFunc(const std::vector<std::string> &strs,
                                               ::cava::CavaJitModulePtr &cavaJitModule) {
    autil::ScopedTime2 timer;
    const std::string &createFuncName = genCreateFunc(strs);
    llvm::JITSymbol jitSymbol = cavaJitModule->findSymbol(createFuncName);
    AUTIL_LOG(DEBUG, "findSymbol[%s] use [%lf] ms", createFuncName.c_str(), timer.done_ms());
    CreateProtoType ret = (CreateProtoType)*jitSymbol.getAddress();
    if (!ret) {
        AUTIL_LOG(WARN, "find create func failed: %s", createFuncName.c_str());
    }
    return ret;
}

std::string CavaModuleInfo::genInitFunc(const std::vector<std::string> &strs, const std::vector<std::string> &params) {
    std::string ret = "_ZN";
    for (const auto &str : strs) {
        ret += autil::StringUtil::toString(str.size()) + str;
    }
    ret += "4initEP7CavaCtx";
    for (const auto &param : params) {
        ret += param;
    }
    return ret;
}

std::string CavaModuleInfo::genProcessFunc(const std::vector<std::string> &strs,
                                           const std::vector<std::string> &params,
                                           const std::string &methodName) {
    std::string ret = "_ZN";
    for (const auto &str : strs) {
        ret += autil::StringUtil::toString(str.size()) + str;
    }
    ret += autil::StringUtil::toString(methodName.size()) + methodName + "E";
    ret += "P7CavaCtx";
    for (const auto &param : params) {
        ret += param;
    }
    return ret;
}

} // namespace turing
} // namespace suez
