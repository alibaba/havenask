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
#include "suez/turing/expression/cava/common/CavaSummaryModuleInfo.h"

#include <algorithm>
#include <iosfwd>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <memory>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "cava/codegen/CavaJitModule.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, CavaSummaryModuleInfo);

CavaSummaryModuleInfo::CavaSummaryModuleInfo(const std::string &className,
                                             ::cava::CavaJitModulePtr &cavaJitModule,
                                             CreateProtoType createFunc,
                                             InitProtoType initFunc,
                                             ProcessProtoType processFunc)
    : CavaModuleInfo(className, cavaJitModule, createFunc), initFunc(initFunc), processFunc(processFunc) {}

CavaSummaryModuleInfo::~CavaSummaryModuleInfo() {}

CavaModuleInfoPtr CavaSummaryModuleInfo::create(const string &className, ::cava::CavaJitModulePtr &cavaJitModule) {
    const vector<string> &strs = autil::StringUtil::split(className, ".");
    auto createFunc = findCreateFunc(strs, cavaJitModule);
    if (!createFunc) {
        AUTIL_LOG(ERROR, "find create func failed,class: %s", className.c_str());
        return CavaModuleInfoPtr();
    }
    auto initFunc = findInitFunc(strs, cavaJitModule);
    if (!initFunc) {
        AUTIL_LOG(ERROR, "find init func failed,class: %s", className.c_str());
        return CavaModuleInfoPtr();
    }
    auto processFunc = findProcessFunc(strs, cavaJitModule);
    if (!processFunc) {
        AUTIL_LOG(ERROR, "find process func failed,class: %s", className.c_str());
        return CavaModuleInfoPtr();
    }
    return CavaModuleInfoPtr(new CavaSummaryModuleInfo(className, cavaJitModule, createFunc, initFunc, processFunc));
}

CavaSummaryModuleInfo::InitProtoType CavaSummaryModuleInfo::findInitFunc(const vector<string> &strs,
                                                                         ::cava::CavaJitModulePtr &cavaJitModule) {
    vector<string> params;
    params.push_back("PN3ha315SummaryProviderE");
    const string &initFuncName = genInitFunc(strs, params);
    auto jitSymbol = cavaJitModule->findSymbol(initFuncName);
    InitProtoType ret = (InitProtoType)jitSymbol.getAddress();
    if (!ret) {
        AUTIL_LOG(ERROR, "find init func failed: %s", initFuncName.c_str());
    }
    return ret;
}

CavaSummaryModuleInfo::ProcessProtoType
CavaSummaryModuleInfo::findProcessFunc(const vector<string> &strs, ::cava::CavaJitModulePtr &cavaJitModule) {
    vector<string> params;
    params.push_back("PN3ha310SummaryDocE");
    const string &processFuncName = genProcessFunc(strs, params);
    auto jitSymbol = cavaJitModule->findSymbol(processFuncName);
    ProcessProtoType ret = (ProcessProtoType)jitSymbol.getAddress();
    if (!ret) {
        AUTIL_LOG(ERROR, "find process func failed: %s", processFuncName.c_str());
    }
    return ret;
}

} // namespace turing
} // namespace suez
