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

#include "beeper/beeper.h"

namespace beeper {
class EventTags;
}

#define IE_RAW_DOC_TRACE(rawDoc, msg)                                                                                  \
    do {                                                                                                               \
        if (rawDoc) {                                                                                                  \
            std::string traceId = rawDoc->GetTraceId();                                                                \
            if (!traceId.empty()) {                                                                                    \
                beeper::EventTags traceTags;                                                                           \
                traceTags.AddTag("pk", traceId);                                                                       \
                BEEPER_REPORT(IE_DOC_TRACER_COLLECTOR_NAME, msg, traceTags);                                           \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)

#define IE_RAW_DOC_FORMAT_TRACE(rawDoc, format, args...)                                                               \
    do {                                                                                                               \
        if (rawDoc) {                                                                                                  \
            std::string traceId = rawDoc->GetTraceId();                                                                \
            if (!traceId.empty()) {                                                                                    \
                beeper::EventTags traceTags;                                                                           \
                traceTags.AddTag("pk", traceId);                                                                       \
                char msg[1024];                                                                                        \
                sprintf(msg, format, args);                                                                            \
                BEEPER_REPORT(IE_DOC_TRACER_COLLECTOR_NAME, msg, traceTags);                                           \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)

#define IE_INDEX_DOC_TRACE(indexDoc, msg)                                                                              \
    do {                                                                                                               \
        if (indexDoc && indexDoc->NeedTrace()) {                                                                       \
            beeper::EventTags traceTags;                                                                               \
            traceTags.AddTag("pk", indexDoc->GetPrimaryKey());                                                         \
            BEEPER_REPORT(IE_DOC_TRACER_COLLECTOR_NAME, msg, traceTags);                                               \
        }                                                                                                              \
    } while (0)

#define IE_DOC_TRACE(doc, msg)                                                                                         \
    do {                                                                                                               \
        if (doc) {                                                                                                     \
            autil::StringView traceId = doc->GetTraceId();                                                             \
            if (!traceId.empty()) {                                                                                    \
                beeper::EventTags traceTags;                                                                           \
                traceTags.AddTag("pk", traceId.to_string());                                                           \
                BEEPER_REPORT(IE_DOC_TRACER_COLLECTOR_NAME, msg, traceTags);                                           \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)

#define IE_DOC_TRACER_COLLECTOR_NAME "doc_tracer"
