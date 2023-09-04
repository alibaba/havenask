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
#include "aios/network/opentelemetry/core/Span.h"

namespace opentelemetry {

Span::Span(const TracerContextPtr &tracer,
           SpanKind spanKind,
           const SpanContextPtr &parentSpanContext,
           SpanContextPtr &spanContext) {}

Span::~Span() {}

void Span::setAttribute(const std::string &key, const std::string &value) {}

void Span::addEvent(const std::string &name) {}

void Span::addEvent(const std::string &name, int64_t timestamp) {}

void Span::addEvent(const std::string &name, const AttributeMap &attributes) {}

void Span::addEvent(const std::string &name, int64_t timestamp, const AttributeMap &attributes) {}

void Span::addLink(const SpanContextPtr &context) {}

void Span::addLink(const SpanContextPtr &context, const AttributeMap &attributes) {}

void Span::setStatus(StatusCode code, const std::string &description) {}

void Span::updateName(const std::string &name) {}

void Span::end() {}

bool Span::isRecording() const { return false; }

bool Span::isDebug() const { return false; }

void Span::setDebug() {}

SpanContextPtr Span::getContext() const { return nullptr; }

SpanContextPtr Span::getParentContext() const { return nullptr; }

void Span::addLog(uint32_t level, const std::string &body) {}

void Span::addLog(uint32_t level, const char *format, ...) {}

} // namespace opentelemetry
