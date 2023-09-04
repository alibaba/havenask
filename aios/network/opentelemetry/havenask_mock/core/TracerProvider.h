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

#include "aios/network/opentelemetry/core/TraceConfig.h"
#include "aios/network/opentelemetry/core/Tracer.h"

namespace opentelemetry {

class TracerProvider {

public:
    TracerProvider() {}

public:
    bool init(TraceConfig config);
    TracerPtr getTracer() const;
    void setSamplerRate(uint32_t r);
    void setSamplerZoomOutRate(uint32_t r);

    static bool initDefault();
    static std::shared_ptr<TracerProvider> get();
    static void destroyDefault();
};
OTEL_TYPEDEF_PTR(TracerProvider);
} // namespace opentelemetry
