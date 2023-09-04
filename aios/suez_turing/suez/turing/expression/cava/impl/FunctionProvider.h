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

#include <stdint.h>
#include <string>
#include <vector>

#include "suez/turing/expression/cava/impl/CommonRefManager.h"
#include "suez/turing/expression/cava/impl/IApiProvider.h"
#include "suez/turing/expression/cava/impl/KVMapApi.h"
#include "suez/turing/expression/cava/impl/MatchInfoReader.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/provider/FunctionProvider.h"

class CavaCtx;
namespace cava {
namespace lang {
class CString;
} // namespace lang
} // namespace cava

namespace ha3 {
class IRefManager;
class TraceApi;

class FunctionProvider : public IApiProvider {
public:
    FunctionProvider(suez::turing::FunctionProvider *provider,
                     const suez::turing::KeyValueMap &configMap,
                     std::vector<std::string> &argsStrVec)
        : IApiProvider(provider->getKVMap(), provider->getTracerRefer(), provider->getRequestTracer())
        , _configMap(configMap)
        , _refManager(provider, false, true)
        , _provider(provider)
        , _argsStrVec(argsStrVec) {
        suez::turing::MatchInfoReader *matchInfoReader = provider->getMatchInfoReader();
        if (matchInfoReader) {
            _matchInfoReader.init(matchInfoReader->getMetaInfo(), matchInfoReader->getSimpleMatchDataRef());
            _matchInfoReader.setMatchValuesRef(matchInfoReader->getMatchValuesRef());
        }
    }
    ~FunctionProvider() {}

private:
    FunctionProvider(const FunctionProvider &);
    FunctionProvider &operator=(const FunctionProvider &);

public:
    suez::turing::FunctionProvider *getFunctionProvider() { return _provider; }

public:
    KVMapApi *getKVMapApi(CavaCtx *ctx);
    TraceApi *getTraceApi(CavaCtx *ctx);
    KVMapApi *getConfigMap(CavaCtx *ctx);
    IRefManager *getRefManager(CavaCtx *ctx);
    cava::lang::CString *getConstArgsByIndex(CavaCtx *ctx, uint32_t index);
    int32_t getConstArgsCount(CavaCtx *ctx);
    MatchInfoReader *getMatchInfoReader(CavaCtx *ctx);

private:
    ha3::KVMapApi _configMap;
    ha3::CommonRefManager _refManager;
    suez::turing::FunctionProvider *_provider;
    std::vector<std::string> &_argsStrVec;
    MatchInfoReader _matchInfoReader;
};

} // namespace ha3
