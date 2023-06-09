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

namespace opentelemetry {

class EagleeyeUtil {

public:
    static bool validSpan(const SpanPtr& span);
    static const std::string& getETraceId(const SpanPtr& span);
    static const std::string& getERpcId(const SpanPtr& span);
    static const std::string& getEAppId(const SpanPtr& span);
    static void setEAppId(const SpanPtr& span, const std::string& appid);
    static void putEUserData(const SpanPtr& span, const std::string& k, const std::string& v);
    static void putEUserData(const SpanPtr& span, const std::map<std::string, std::string>& dataMap);
    static const std::string& getEUserData(const SpanPtr& span, const std::string& k);
    static const std::map<std::string, std::string>& getEUserDatas(const SpanPtr& span);
    static void splitUserData(const std::string &userData, std::map<std::string, std::string>& datas);
    static std::string joinUserData(const std::map<std::string, std::string> &m);
    static bool isSampled(const SpanContextPtr& context);
};

}
