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

#include <stddef.h>

#include "suez/turing/expression/common.h"
#include "suez/turing/expression/cava/impl/IApiProvider.h"

#include "autil/Log.h" // IWYU pragma: keep

class CavaCtx;
namespace suez {
namespace turing {
class Tracer;
}  // namespace turing
}  // namespace suez

namespace ha3 {
class KVMapApi;
class TraceApi;

class SummaryProvider : public IApiProvider
{
public:
    SummaryProvider(const suez::turing::KeyValueMap& kvMap,
                    suez::turing::Tracer *tracer)
        : IApiProvider(kvMap, NULL, tracer)
    {
    }
    ~SummaryProvider() {
    }
private:
    SummaryProvider(const SummaryProvider &);
    SummaryProvider& operator=(const SummaryProvider &);
public:
    virtual KVMapApi *getKVMapApi(CavaCtx *ctx);
    virtual TraceApi *getTraceApi(CavaCtx *ctx);
};

}
