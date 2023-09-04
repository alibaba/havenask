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

#include "suez/turing/expression/cava/impl/KVMapApi.h"
#include "suez/turing/expression/cava/impl/TraceApi.h"

class CavaCtx;
namespace matchdoc {
template <typename T>
class Reference;
} // namespace matchdoc
namespace suez {
namespace turing {
class Tracer;
} // namespace turing
} // namespace suez

namespace ha3 {

class IApiProvider {
public:
    IApiProvider(const std::map<std::string, std::string> &kvMap,
                 matchdoc::Reference<suez::turing::Tracer> *traceRefer,
                 suez::turing::Tracer *tracer)
        : _kvMap(kvMap), _tracer(traceRefer, tracer) {}
    virtual ~IApiProvider() = 0;

private:
    IApiProvider(const IApiProvider &);
    IApiProvider &operator=(const IApiProvider &);

public:
    virtual KVMapApi *getKVMapApiImpl(CavaCtx *ctx);
    virtual TraceApi *getTraceApiImpl(CavaCtx *ctx);

    KVMapApi *getKVMapApi(CavaCtx *ctx);
    TraceApi *getTraceApi(CavaCtx *ctx);

private:
    ha3::KVMapApi _kvMap;
    ha3::TraceApi _tracer;
};

} // namespace ha3
