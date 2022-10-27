#ifndef __INDEXLIB_DOC_TRACER_H
#define __INDEXLIB_DOC_TRACER_H
#include "indexlib/document/document.h"
#include "indexlib/document/raw_document.h"
#include <beeper/beeper.h>

namespace beeper {
class EventTags;
}

#define IE_RAW_DOC_TRACE(rawDoc, msg) do {                              \
        if (rawDoc) {                                                   \
            string traceId = rawDoc->GetTraceId();                      \
            if (!traceId.empty()) {                                     \
                beeper::EventTags traceTags;                            \
                traceTags.AddTag("pk", traceId);                        \
                BEEPER_REPORT(IE_DOC_TRACER_COLLECTOR_NAME, msg, traceTags); \
            }                                                           \
        }                                                               \
    } while (0)

#define IE_RAW_DOC_FORMAT_TRACE(rawDoc, format, args...) do {           \
        if (rawDoc) {                                                   \
            string traceId = rawDoc->GetTraceId();                      \
            if (!traceId.empty()) {                                     \
                beeper::EventTags traceTags;                            \
                traceTags.AddTag("pk", traceId);                        \
                char msg[1024];                                         \
                sprintf(msg,  format, args);                            \
                BEEPER_REPORT(IE_DOC_TRACER_COLLECTOR_NAME, msg, traceTags); \
            }                                                           \
        }                                                               \
    } while (0)

#define IE_INDEX_DOC_TRACE(indexDoc, msg) do {                          \
        if (indexDoc && indexDoc->NeedTrace()) {                        \
            beeper::EventTags traceTags;                                \
            traceTags.AddTag("pk", indexDoc->GetPrimaryKey());          \
            BEEPER_REPORT(IE_DOC_TRACER_COLLECTOR_NAME, msg, traceTags);   \
        }                                                               \
    } while (0)

#define IE_INDEX_DOC_FORMAT_TRACE(indexDoc, format, args...) do {       \
        if (indexDoc && indexDoc->NeedTrace()) {                        \
            beeper::EventTags traceTags;                                \
            traceTags.AddTag("pk", indexDoc->GetPrimaryKey());          \
            char msg[1024];                                             \
            sprintf(msg, format, args);                                 \
            BEEPER_REPORT(IE_DOC_TRACER_COLLECTOR_NAME, msg, traceTags); \
        }                                                               \
    } while (0)

#define IE_DOC_TRACER_COLLECTOR_NAME "doc_tracer"

#endif //__INDEXLIB_DOC_TRACER_H
