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
#include "aios/network/opentelemetry/core/TracerProvider.h"

namespace opentelemetry {

bool TracerProvider::init(TraceConfig config) { return true; }

TracerPtr TracerProvider::getTracer() const { return nullptr; }

void TracerProvider::setSamplerRate(uint32_t r) {}

void TracerProvider::setSamplerZoomOutRate(uint32_t r) {}

bool TracerProvider::initDefault() { return false; }

std::shared_ptr<TracerProvider> TracerProvider::get() { return nullptr; }

void TracerProvider::destroyDefault() {}

} // namespace opentelemetry
