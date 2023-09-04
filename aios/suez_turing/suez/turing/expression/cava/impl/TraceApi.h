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

#include "cava/common/Common.h"

class CavaCtx;
namespace cava {
namespace lang {
class CString;
} // namespace lang
} // namespace cava
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
class MDouble;
class MFloat;
class MInt16;
class MInt32;
class MInt64;
class MInt8;
class MUInt16;
class MUInt32;
class MUInt64;
class MUInt8;
class MatchDoc;

class TraceApi {
public:
    TraceApi(matchdoc::Reference<suez::turing::Tracer> *traceRefer, suez::turing::Tracer *tracer)
        : _traceRefer(traceRefer), _tracer(tracer) {}
    ~TraceApi() {}

private:
    TraceApi(const TraceApi &);
    TraceApi &operator=(const TraceApi &);

public:
#define TRACE_DEC(type)                                                                                                \
    void docTrace(CavaCtx *ctx, MatchDoc *doc, type msg);                                                              \
    void docTrace(CavaCtx *ctx, cava::lang::CString *file, int line, MatchDoc *doc, type msg);                         \
    void queryTrace(CavaCtx *ctx, type msg);                                                                           \
    void queryTrace(CavaCtx *ctx, cava::lang::CString *file, int line, type msg);

    TRACE_DEC(cava::byte);
    TRACE_DEC(short);
    TRACE_DEC(int);
    TRACE_DEC(long);
    TRACE_DEC(ubyte);
    TRACE_DEC(ushort);
    TRACE_DEC(uint);
    TRACE_DEC(ulong);
    TRACE_DEC(float);
    TRACE_DEC(double);

    TRACE_DEC(MInt8 *);
    TRACE_DEC(MInt16 *);
    TRACE_DEC(MInt32 *);
    TRACE_DEC(MInt64 *);
    TRACE_DEC(MUInt8 *);
    TRACE_DEC(MUInt16 *);
    TRACE_DEC(MUInt32 *);
    TRACE_DEC(MUInt64 *);
    TRACE_DEC(MFloat *);
    TRACE_DEC(MDouble *);

    void docTrace(CavaCtx *ctx, MatchDoc *doc, cava::lang::CString *msg);
    void queryTrace(CavaCtx *ctx, cava::lang::CString *msg);
    void docTrace(CavaCtx *ctx, cava::lang::CString *file, int line, MatchDoc *doc, cava::lang::CString *msg);
    void queryTrace(CavaCtx *ctx, cava::lang::CString *file, int line, cava::lang::CString *msg);

    suez::turing::Tracer *getQueryTracer() { return _tracer; }

private:
    // doc trace
    matchdoc::Reference<suez::turing::Tracer> *_traceRefer;
    // query trace
    suez::turing::Tracer *_tracer;
};

} // namespace ha3
