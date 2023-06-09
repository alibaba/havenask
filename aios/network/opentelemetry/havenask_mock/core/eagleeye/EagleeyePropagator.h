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

#include <memory>
#include <map>

namespace opentelemetry {
class SpanContext;
using SpanContextPtr = std::shared_ptr<SpanContext>;

class EagleeyePropagator {

public:
    static SpanContextPtr extract(const std::string& eagleeyeTraceId,
                                  const std::string& eagleeyeRpcId,
                                  const std::string& eagleeyeUserData,
                                  std::string traceparent,
                                  std::string tracestate);

    static void inject(const SpanContextPtr& context,
                       std::string& eagleeyeTraceId,
                       std::string& eagleeyeRpcId,
                       std::map<std::string, std::string>& eagleeyeUserData,
                       std::string& traceparent,
                       std::string& tracestate);
};
}
