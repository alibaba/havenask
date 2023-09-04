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
#include "suez/turing/expression/cava/impl/FunctionProvider.h"

#include <cstddef>

#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"

class CavaCtx;
namespace cava {
namespace lang {
class CString;
} // namespace lang
} // namespace cava
namespace ha3 {
class IRefManager;
class TraceApi;
} // namespace ha3

using namespace std;

namespace ha3 {

KVMapApi *FunctionProvider::getKVMapApi(CavaCtx *ctx) { return IApiProvider::getKVMapApi(ctx); }
TraceApi *FunctionProvider::getTraceApi(CavaCtx *ctx) { return IApiProvider::getTraceApi(ctx); }
KVMapApi *FunctionProvider::getConfigMap(CavaCtx *ctx) { return &_configMap; }
IRefManager *FunctionProvider::getRefManager(CavaCtx *ctx) { return &_refManager; }

cava::lang::CString *FunctionProvider::getConstArgsByIndex(CavaCtx *ctx, uint32_t index) {
    if (index >= _argsStrVec.size()) {
        return NULL;
    }
    return suez::turing::SuezCavaAllocUtil::allocCString(ctx, &(_argsStrVec[index]));
}

int32_t FunctionProvider::getConstArgsCount(CavaCtx *ctx) { return _argsStrVec.size(); }

MatchInfoReader *FunctionProvider::getMatchInfoReader(CavaCtx *ctx) {
    if (_matchInfoReader.getMetaInfo() == nullptr) {
        return nullptr;
    }
    return &_matchInfoReader;
}

} // namespace ha3
