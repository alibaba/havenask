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
#include "aios/network/opentelemetry/core/SpanContext.h"
#include <memory>

namespace opentelemetry {

class TracerContext;
class Recordable;

using TracerContextPtr = std::shared_ptr<TracerContext>;

class Span {

protected:
    Span() = default;

public:
    Span(const TracerContextPtr& tracer,
        SpanKind spanKind,
        const SpanContextPtr& parent,
        SpanContextPtr& spanContext);

    virtual ~Span();

public:
    // trace_api::Span
    virtual void setAttribute(const std::string& key,
                      const std::string& value);

    void addEvent(const std::string& name);

    void addEvent(const std::string& name,
                  int64_t timestamp);

    void addEvent(const std::string& name,
                  const AttributeMap& attributes);

    void addEvent(const std::string& name,
                  int64_t timestamp,
                  const AttributeMap& attributes);

    void addLink(const SpanContextPtr& context);

    void addLink(const SpanContextPtr& context,
                 const AttributeMap& attributes);

    virtual void setStatus(StatusCode code, const std::string& description = EMPTY_STRING);

    virtual void updateName(const std::string& name);

    virtual void end();

    bool isRecording() const;
    bool isDebug() const;

    virtual SpanContextPtr getContext() const;
    virtual SpanContextPtr getParentContext() const;

    void addLog(uint32_t level, const std::string& body);
    void addLog(uint32_t level, const char* format, ...);

    void setAutoCommit(bool b) { }
    void setDebug();
};

OTEL_TYPEDEF_PTR(Span);

}
