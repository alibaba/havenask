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

#include "aios/network/opentelemetry/core/Span.h"
#include <memory>

namespace opentelemetry {
class TracerContext;
using TracerContextPtr = std::shared_ptr<TracerContext>;

class Tracer {
protected:
    Tracer() {}

public:
    Tracer(TracerContextPtr context)
    {}
    virtual ~Tracer() {}

public:
    virtual SpanPtr startSpan(SpanKind spanKind,
                              const SpanContextPtr& parent = SpanContextPtr());

    SpanPtr getCurrentSpan() {
        return nullptr;
    }

    void withActiveSpan(const SpanPtr &span) {
    }

protected:
    SpanPtr startChildSpan(SpanKind spanKind, const SpanContextPtr& parent);
    SpanPtr startNewTrace(SpanKind spanKind);

};
OTEL_TYPEDEF_PTR(Tracer);
}
