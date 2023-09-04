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
#include "suez/turing/expression/cava/impl/ScorerProvider.h"

#include <iosfwd>

class CavaCtx;
namespace ha3 {
class IRefManager;
class KVMapApi;
class TraceApi;
} // namespace ha3

using namespace std;

namespace suez {
namespace turing {

ha3::IRefManager *ScorerProvider::getRefManagerImpl(CavaCtx *ctx) { return &_refManager; }

ha3::KVMapApi *ScorerProvider::getKVMapApi(CavaCtx *ctx) { return getKVMapApiImpl(ctx); }

ha3::TraceApi *ScorerProvider::getTraceApi(CavaCtx *ctx) { return getTraceApiImpl(ctx); }

ha3::IRefManager *ScorerProvider::getRefManager(CavaCtx *ctx) { return getRefManagerImpl(ctx); }

} // namespace turing
} // namespace suez
