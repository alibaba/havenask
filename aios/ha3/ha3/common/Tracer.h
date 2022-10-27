#ifndef ISEARCH_TRACER_H
#define ISEARCH_TRACER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/common/Tracer.h>

BEGIN_HA3_NAMESPACE(common);


typedef suez::turing::Tracer Tracer;
typedef suez::turing::TracerPtr TracerPtr;

#define RANK_TRACE_LEVEL_ENABLED(level, matchdoc)                       \
    (unlikely( _traceRefer == NULL ? false : (_traceRefer->getPointer(matchdoc)->isLevelEnabled(ISEARCH_TRACE_##level) ) ))

#define RANK_TRACE(level, matchdoc, format, args...)                    \
    {                                                                   \
        if (unlikely(_traceRefer!=NULL)) {                              \
            HA3_NS(common)::Tracer *tracer                              \
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
    matchdoc::Reference<HA3_NS(common)::Tracer> *_traceRefer;

#define TRACE_SETUP(provider) do {                      \
        _traceRefer = provider->getTracerRefer();       \
    } while (0)

END_HA3_NAMESPACE(common);

#endif //ISEARCH_TRACER_H
