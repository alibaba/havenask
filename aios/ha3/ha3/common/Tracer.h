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

#include "autil/CommonMacros.h"
#include "turing_ops_util/variant/Tracer.h"


namespace isearch {
namespace common {


typedef suez::turing::Tracer Tracer;
typedef suez::turing::TracerPtr TracerPtr;

#define RANK_TRACE_LEVEL_ENABLED(level, matchdoc)                       \
    (unlikely( _traceRefer == NULL ? false : (_traceRefer->getPointer(matchdoc)->isLevelEnabled(ISEARCH_TRACE_##level) ) ))

#define RANK_TRACE(level, matchdoc, format, args...)                    \
    {                                                                   \
        if (unlikely(_traceRefer!=NULL)) {                              \
            isearch::common::Tracer *tracer                              \
                = _traceRefer->getPointer(matchdoc); \
            if (unlikely(tracer->isLevelEnabled(ISEARCH_TRACE_##level))) { \
                tracer->rankTrace(#level, __FILE__, __LINE__, format, ##args); \
            }                                                           \
        }                                                               \
    }

#define INNER_REQUEST_TRACE_WITH_TRACER(tracer, level, levelStr, format, args...) \
    {                                                                   \
        if (unlikely(tracer != NULL &&                                  \
                     tracer->isLevelEnabled(level)))                    \
        {                                                               \
            tracer->requestTrace(levelStr, format, ##args);             \
        }                                                               \
    }

#define REQUEST_TRACE_WITH_TRACER(tracer, level, format, args...)       \
    INNER_REQUEST_TRACE_WITH_TRACER(tracer, ISEARCH_TRACE_##level, #level, format, ##args)

#define REQUEST_TRACE(level, format, args...)                           \
    INNER_REQUEST_TRACE_WITH_TRACER(_tracer, ISEARCH_TRACE_##level, #level, format, ##args)

#define SUMMARY_TRACE(tracer, level, format, args...)       \
    INNER_REQUEST_TRACE_WITH_TRACER(tracer, ISEARCH_TRACE_##level, #level, format, ##args)

#define HA3_COMBINE_STRING(x, y) x##y
#define HA3_COMBINE_STRING1(x, y) HA3_COMBINE_STRING(x,y)

#define TRACE_DECLARE()                                                 \
    matchdoc::Reference<isearch::common::Tracer> *_traceRefer;

#define TRACE_SETUP(provider) do {                      \
        _traceRefer = provider->getTracerRefer();       \
    } while (0)

} // namespace common
} // namespace isearch
