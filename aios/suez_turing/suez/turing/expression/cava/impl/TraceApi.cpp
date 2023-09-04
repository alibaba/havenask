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
#include "suez/turing/expression/cava/impl/TraceApi.h"

#include <cstddef>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "cava/runtime/CString.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/cava/impl/CavaMultiValueTyped.h"
#include "suez/turing/expression/cava/impl/MatchDoc.h"
#include "turing_ops_util/variant/Tracer.h"

class CavaCtx;
namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

using namespace std;

namespace ha3 {

#define TRACE_IMP(type)                                                                                                \
    void TraceApi::docTrace(CavaCtx *ctx, MatchDoc *doc, type msg) {                                                   \
        SUEZ_CAVA_RANK_TRACE(DEBUG, (*(matchdoc::MatchDoc *)doc), "[%s]", autil::StringUtil::toString(msg).c_str());   \
    }                                                                                                                  \
    void TraceApi::docTrace(CavaCtx *ctx, cava::lang::CString *file, int line, MatchDoc *doc, type msg) {              \
        SUEZ_CAVA_RANK_TRACE(DEBUG,                                                                                    \
                             (*(matchdoc::MatchDoc *)doc),                                                             \
                             "[%s:%d][%s]",                                                                            \
                             file ? file->getStr().c_str() : "",                                                       \
                             line,                                                                                     \
                             autil::StringUtil::toString(msg).c_str());                                                \
    }                                                                                                                  \
    void TraceApi::queryTrace(CavaCtx *ctx, type msg) {                                                                \
        SUEZ_REQUEST_TRACE(DEBUG, "[%s]", autil::StringUtil::toString(msg).c_str());                                   \
    }                                                                                                                  \
    void TraceApi::queryTrace(CavaCtx *ctx, cava::lang::CString *file, int line, type msg) {                           \
        SUEZ_REQUEST_TRACE(                                                                                            \
            DEBUG, "[%s:%d][%s]", file ? file->getStr().c_str() : "", line, autil::StringUtil::toString(msg).c_str()); \
    }

TRACE_IMP(cava::byte);
TRACE_IMP(short);
TRACE_IMP(int);
TRACE_IMP(long);
TRACE_IMP(ubyte);
TRACE_IMP(ushort);
TRACE_IMP(uint);
TRACE_IMP(ulong);
TRACE_IMP(float);
TRACE_IMP(double);

#define MULTI_VALUE_STR(tracer)                                                                                        \
    if (likely(tracer == NULL || !tracer->isLevelEnabled(ISEARCH_TRACE_DEBUG))) {                                      \
        return;                                                                                                        \
    }                                                                                                                  \
    std::string msgStr;                                                                                                \
    if (unlikely(msg == NULL)) {                                                                                       \
        msgStr = "";                                                                                                   \
    } else {                                                                                                           \
        for (uint i = 0; i < msg->size(ctx); i++) {                                                                    \
            msgStr += autil::StringUtil::toString(msg->getWithoutCheck(ctx, i));                                       \
            if (i != (msg->size(ctx) - 1)) {                                                                           \
                msgStr += ",";                                                                                         \
            }                                                                                                          \
        }                                                                                                              \
    }

#define MULTI_TRACE_IMP(type)                                                                                          \
    void TraceApi::docTrace(CavaCtx *ctx, MatchDoc *doc, type *msg) {                                                  \
        if (likely(_traceRefer == NULL)) {                                                                             \
            return;                                                                                                    \
        }                                                                                                              \
        auto *matchDoc = (matchdoc::MatchDoc *)doc;                                                                    \
        MULTI_VALUE_STR(_traceRefer->getPointer(*matchDoc));                                                           \
        SUEZ_CAVA_RANK_TRACE(DEBUG, (*matchDoc), "[%s]", msgStr.c_str());                                              \
    }                                                                                                                  \
                                                                                                                       \
    void TraceApi::docTrace(CavaCtx *ctx, cava::lang::CString *file, int line, MatchDoc *doc, type *msg) {             \
        if (likely(_traceRefer == NULL)) {                                                                             \
            return;                                                                                                    \
        }                                                                                                              \
        auto *matchDoc = (matchdoc::MatchDoc *)doc;                                                                    \
        MULTI_VALUE_STR(_traceRefer->getPointer(*matchDoc));                                                           \
        SUEZ_CAVA_RANK_TRACE(                                                                                          \
            DEBUG, (*matchDoc), "[%s:%d][%s]", file ? file->getStr().c_str() : "", line, msgStr.c_str());              \
    }                                                                                                                  \
                                                                                                                       \
    void TraceApi::queryTrace(CavaCtx *ctx, type *msg) {                                                               \
        MULTI_VALUE_STR(_tracer);                                                                                      \
        SUEZ_REQUEST_TRACE(DEBUG, "[%s]", msgStr.c_str());                                                             \
    }                                                                                                                  \
                                                                                                                       \
    void TraceApi::queryTrace(CavaCtx *ctx, cava::lang::CString *file, int line, type *msg) {                          \
        MULTI_VALUE_STR(_tracer);                                                                                      \
        SUEZ_REQUEST_TRACE(DEBUG, "[%s:%d][%s]", file ? file->getStr().c_str() : "", line, msgStr.c_str());            \
    }

MULTI_TRACE_IMP(MInt8);
MULTI_TRACE_IMP(MInt16);
MULTI_TRACE_IMP(MInt32);
MULTI_TRACE_IMP(MInt64);
MULTI_TRACE_IMP(MUInt8);
MULTI_TRACE_IMP(MUInt16);
MULTI_TRACE_IMP(MUInt32);
MULTI_TRACE_IMP(MUInt64);
MULTI_TRACE_IMP(MFloat);
MULTI_TRACE_IMP(MDouble);

void TraceApi::docTrace(CavaCtx *ctx, MatchDoc *doc, cava::lang::CString *msg) {
    SUEZ_CAVA_RANK_TRACE(DEBUG, (*(matchdoc::MatchDoc *)doc), "[%s]", msg ? msg->getStr().c_str() : "");
}

void TraceApi::queryTrace(CavaCtx *ctx, cava::lang::CString *msg) {
    SUEZ_REQUEST_TRACE(DEBUG, "[%s]", msg ? msg->getStr().c_str() : "");
}

void TraceApi::docTrace(CavaCtx *ctx, cava::lang::CString *file, int line, MatchDoc *doc, cava::lang::CString *msg) {
    SUEZ_CAVA_RANK_TRACE(DEBUG,
                         (*(matchdoc::MatchDoc *)doc),
                         "[%s:%d][%s]",
                         file ? file->getStr().c_str() : "",
                         line,
                         msg ? msg->getStr().c_str() : "");
}

void TraceApi::queryTrace(CavaCtx *ctx, cava::lang::CString *file, int line, cava::lang::CString *msg) {
    SUEZ_REQUEST_TRACE(
        DEBUG, "[%s:%d][%s]", file ? file->getStr().c_str() : "", line, msg ? msg->getStr().c_str() : "");
}

} // namespace ha3
