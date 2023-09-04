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
#include "suez/turing/expression/cava/impl/IApiProvider.h"

#include <iosfwd>

class CavaCtx;

using namespace std;
namespace ha3 {

IApiProvider::~IApiProvider() {}

KVMapApi *IApiProvider::getKVMapApiImpl(CavaCtx *ctx) { return &_kvMap; }

TraceApi *IApiProvider::getTraceApiImpl(CavaCtx *ctx) { return &_tracer; }

KVMapApi *IApiProvider::getKVMapApi(CavaCtx *ctx) { return getKVMapApiImpl(ctx); }

TraceApi *IApiProvider::getTraceApi(CavaCtx *ctx) { return getTraceApiImpl(ctx); }

} // namespace ha3
