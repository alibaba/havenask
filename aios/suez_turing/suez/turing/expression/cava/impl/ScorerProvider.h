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

#include <assert.h>
#include <map>
#include <stddef.h>
#include <utility>

#include "cava/codegen/CavaJitModule.h"
#include "suez/turing/expression/cava/impl/CommonRefManager.h"
#include "suez/turing/expression/cava/impl/IApiProvider.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/provider/ScoringProvider.h"

class CavaCtx;
namespace ha3 {
class IRefManager;
class KVMapApi;
class TraceApi;
} // namespace ha3

namespace suez {
namespace turing {

class ScorerProvider : public ha3::IApiProvider {
public:
    ScorerProvider(ScoringProvider *provider, cava::CavaJitModulesWrapper *cavaJitModulesWrapper)
        : IApiProvider(provider->getKVMap(), provider->getTracerRefer(), provider->getRequestTracer())
        , _scoringProvider(provider)
        , _refManager(provider)
        , _cavaJitModulesWrapper(cavaJitModulesWrapper) {
        provider->setCavaScorerProvider(this);
    }
    ~ScorerProvider() {}

private:
    ScorerProvider(const ScorerProvider &);
    ScorerProvider &operator=(const ScorerProvider &);

public:
    ScoringProvider *getScoringProvider() { return _scoringProvider; }
    cava::CavaJitModulePtr getCavaJitModule(size_t hashKey) {
        return _cavaJitModulesWrapper->getCavaJitModule(hashKey);
    }
    cava::CavaJitModulePtr getCavaJitModule() { return _cavaJitModulesWrapper->getCavaJitModule(); }

    virtual ha3::IRefManager *getRefManagerImpl(CavaCtx *ctx);

    ha3::KVMapApi *getKVMapApi(CavaCtx *ctx);
    ha3::TraceApi *getTraceApi(CavaCtx *ctx);
    ha3::IRefManager *getRefManager(CavaCtx *ctx);

protected:
    ScoringProvider *_scoringProvider;

private:
    ha3::CommonRefManager _refManager;
    cava::CavaJitModulesWrapper *_cavaJitModulesWrapper = nullptr;
};

SUEZ_TYPEDEF_PTR(ScorerProvider);

} // namespace turing
} // namespace suez
