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
#include "ha3/cava/SummaryProvider.h"

#include <iosfwd>

#include "suez/turing/expression/cava/impl/IApiProvider.h"

class CavaCtx;
namespace ha3 {
class KVMapApi;
class TraceApi;
}  // namespace ha3

using namespace std;

namespace ha3 {
    
    KVMapApi *SummaryProvider::getKVMapApi(CavaCtx *ctx) {
        return IApiProvider::getKVMapApi(ctx);
    }
    TraceApi *SummaryProvider::getTraceApi(CavaCtx *ctx) {
        return IApiProvider::getTraceApi(ctx);
    }

}
