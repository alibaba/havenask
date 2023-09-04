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
#include "suez/turing/expression/cava/common/CavaAggModuleInfo.h"

#include <cstddef>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <memory>

#include "autil/Log.h"
#include "cava/codegen/CavaJitModule.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, CavaAggModuleInfo);

namespace suez {
namespace turing {

CavaAggModuleInfo::CavaAggModuleInfo(const std::string &className,
                                     ::cava::CavaJitModulePtr &cavaJitModule,
                                     AggCreateProtoType createProtoType_,
                                     AggCountProtoType countProtoType_,
                                     AggBatchProtoType batchProtoType_,
                                     AggResultProtoType resultProtoType_)
    : CavaModuleInfo(className, cavaJitModule, NULL)
    , createProtoType(createProtoType_)
    , countProtoType(countProtoType_)
    , batchProtoType(batchProtoType_)
    , resultProtoType(resultProtoType_) {}

CavaAggModuleInfo::~CavaAggModuleInfo() {}

CavaModuleInfoPtr CavaAggModuleInfo::create(::cava::CavaJitModulePtr &cavaJitModule) {
    // create
    auto jitSymbol = cavaJitModule->findSymbol("_ZN3ha313JitAggregator6createEP7CavaCtxPNS_10AggregatorE");
    if (!jitSymbol) {
        return CavaModuleInfoPtr();
    }
    auto createProtoType = (AggCreateProtoType)jitSymbol.getAddress();
    // count
    jitSymbol = cavaJitModule->findSymbol("_ZN3ha313JitAggregator5countEP7CavaCtx");
    if (!jitSymbol.getAddress()) {
        return CavaModuleInfoPtr();
    }
    auto countProtoType = (AggCountProtoType)jitSymbol.getAddress();
    // batch
    jitSymbol = cavaJitModule->findSymbol("_ZN3ha313JitAggregator5batchEP7CavaCtxPN6unsafe9MatchDocsEj");
    if (!jitSymbol.getAddress()) {
        return CavaModuleInfoPtr();
    }
    auto batchProtoType = (AggBatchProtoType)jitSymbol.getAddress();
    // result
    jitSymbol = cavaJitModule->findSymbol("_ZN3ha313JitAggregator6resultEP7CavaCtxPN6unsafe9MatchDocsEj");
    if (!jitSymbol.getAddress()) {
        return CavaModuleInfoPtr();
    }
    auto resultProtoType = (AggResultProtoType)jitSymbol.getAddress();

    return CavaModuleInfoPtr(new CavaAggModuleInfo(
        "ha3.JitAggregator", cavaJitModule, createProtoType, countProtoType, batchProtoType, resultProtoType));
}

} // namespace turing
} // namespace suez
