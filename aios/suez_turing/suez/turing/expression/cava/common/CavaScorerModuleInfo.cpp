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
#include "suez/turing/expression/cava/common/CavaScorerModuleInfo.h"

#include <algorithm>
#include <iosfwd>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <memory>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "cava/codegen/CavaJitModule.h"
#include "suez/turing/expression/cava/common/CavaModuleInfo.h"

using namespace std;
using namespace autil;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, CavaScorerModuleInfo);

CavaScorerModuleInfo::CavaScorerModuleInfo(const string &className_,
                                           ::cava::CavaJitModulePtr &cavaJitModule_,
                                           CreateProtoType createFunc_,
                                           BeginRequestTuringProtoType beginRequestTuringFunc_,
                                           BeginRequestHa3ProtoType beginRequestHa3Func_,
                                           ScoreProtoType scoreFunc_,
                                           BatchScoreProtoType batchScoreFunc_)
    : CavaModuleInfo(className_, cavaJitModule_, createFunc_)
    , beginRequestTuringFunc(beginRequestTuringFunc_)
    , beginRequestHa3Func(beginRequestHa3Func_)
    , scoreFunc(scoreFunc_)
    , batchScoreFunc(batchScoreFunc_) {}

CavaScorerModuleInfo::~CavaScorerModuleInfo() {}

CavaModuleInfoPtr CavaScorerModuleInfo::create(const string &className, ::cava::CavaJitModulePtr &cavaJitModule) {
    const vector<string> &strs = StringUtil::split(className, ".");
    auto createFunc = findCreateFunc(strs, cavaJitModule);
    if (!createFunc) {
        AUTIL_LOG(ERROR, "find create func failed,class: %s", className.c_str());
        return CavaModuleInfoPtr();
    }
    BeginRequestTuringProtoType beginRequestTuringFunc = findInitTuringFunc(strs, cavaJitModule);
    BeginRequestHa3ProtoType beginRequestHa3Func = findInitHa3Func(strs, cavaJitModule);
    if (!beginRequestTuringFunc && !beginRequestHa3Func) {
        AUTIL_LOG(ERROR, "find init func failed,class: %s", className.c_str());
        return CavaModuleInfoPtr();
    }
    auto scoreFunc = findProcessFunc(strs, cavaJitModule);
    if (!scoreFunc) {
        AUTIL_LOG(ERROR, "find process func failed,class: %s", className.c_str());
        return CavaModuleInfoPtr();
    }
    auto batchScoreFunc = findBatchProcessFunc(strs, cavaJitModule);
    if (!batchScoreFunc) {
        AUTIL_LOG(ERROR, "find batch process func failed,class: %s", className.c_str());
        return CavaModuleInfoPtr();
    }
    return CavaModuleInfoPtr(new CavaScorerModuleInfo(
        className, cavaJitModule, createFunc, beginRequestTuringFunc, beginRequestHa3Func, scoreFunc, batchScoreFunc));
}

CavaScorerModuleInfo::BeginRequestTuringProtoType
CavaScorerModuleInfo::findInitTuringFunc(const vector<string> &strs, ::cava::CavaJitModulePtr &cavaJitModule) {
    vector<string> params;
    params.push_back("PN4suez6turing14ScorerProviderE");
    const string &beginRequestTuringFuncName = genInitFunc(strs, params);
    auto jitSymbol = cavaJitModule->findSymbol(beginRequestTuringFuncName);
    auto address = (BeginRequestTuringProtoType)jitSymbol.getAddress();
    if (!address) {
        AUTIL_LOG(DEBUG, "skip turing init func as not found, mangle name: %s", beginRequestTuringFuncName.c_str());
    }
    return address;
}

CavaScorerModuleInfo::BeginRequestHa3ProtoType
CavaScorerModuleInfo::findInitHa3Func(const vector<string> &strs, ::cava::CavaJitModulePtr &cavaJitModule) {
    vector<string> params;
    params.push_back("PN3ha314ScorerProviderE");
    const string &beginRequestHa3FuncName = genInitFunc(strs, params);
    auto jitSymbol = cavaJitModule->findSymbol(beginRequestHa3FuncName);
    auto address = (BeginRequestHa3ProtoType)jitSymbol.getAddress();
    if (!address) {
        AUTIL_LOG(DEBUG, "skip ha3 init func as not found, mangle name: %s", beginRequestHa3FuncName.c_str());
    }
    return address;
}

CavaScorerModuleInfo::ScoreProtoType CavaScorerModuleInfo::findProcessFunc(const vector<string> &strs,
                                                                           ::cava::CavaJitModulePtr &cavaJitModule) {
    vector<string> params;
    params.push_back("PN3ha38MatchDocE");
    const string &processFuncName = genProcessFunc(strs, params);
    auto jitSymbol = cavaJitModule->findSymbol(processFuncName);
    auto address = (ScoreProtoType)jitSymbol.getAddress();
    if (!address) {
        AUTIL_LOG(ERROR, "find process func failed, mangle name: %s", processFuncName.c_str());
    }
    return address;
}

CavaScorerModuleInfo::BatchScoreProtoType
CavaScorerModuleInfo::findBatchProcessFunc(const vector<string> &strs, ::cava::CavaJitModulePtr &cavaJitModule) {
    const string &scoreFuncName = genBatchProcessFunc(strs);
    auto jitSymbol = cavaJitModule->findSymbol(scoreFuncName);
    auto address = (BatchScoreProtoType)jitSymbol.getAddress();
    if (!address) {
        AUTIL_LOG(ERROR, "find batch process func failed, mangle name: %s", scoreFuncName.c_str());
    }
    return address;
}

string CavaScorerModuleInfo::genBatchProcessFunc(const vector<string> &strs) {
    string ret = "_ZN";
    for (const auto &str : strs) {
        ret += StringUtil::toString(str.size()) + str;
    }
    ret += "12batchProcessE";
    ret += "P7CavaCtxPN3ha318Ha3CavaScorerParamE"; // batchProcess
    return ret;
}

} // namespace turing
} // namespace suez
