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

#include "aios/network/opentelemetry/core/SpanMeta.h"
#include "autil/StringUtil.h"

namespace opentelemetry {

class SpanContext;
OTEL_TYPEDEF_PTR(SpanContext);
/* SpanContext contains the state that must propagate to child Spans and across
 * process boundaries. It contains TraceId and SpanId, TraceFlags, TraceState
 * and whether it was propagated from a remote parent.
 */
class SpanContext {

public:
    SpanContext() = default;
    virtual ~SpanContext() = default;

    SpanContext(const SpanContext &ctx) = default;

    // @returns the trace_id associated with this span_context
    const std::string &getTraceId() const { return EMPTY_STRING; }

    // @returns the span_id associated with this span_context
    const std::string &getSpanId() const { return EMPTY_STRING; }

    // @returns whether this context is valid
    virtual bool isValid() const { return false; }

    /*
     * @param that SpanContext for comparing.
     * @return true if `that` equals the current SpanContext.
     * N.B. trace_state is ignored for the comparison.
     */
    bool operator==(const SpanContext &that) const { return false; }

    SpanContext &operator=(const SpanContext &ctx) = default;

    bool isRemote() const { return false; }
    bool isSampled() const { return false; }
    bool isDebug() const { return false; }
    void setDebug() {}
};
} // namespace opentelemetry
