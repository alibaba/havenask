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
#ifndef ISEARCH_BS_PKTRACER_H
#define ISEARCH_BS_PKTRACER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class PkTracer
{
private:
    PkTracer();
    ~PkTracer();

private:
    PkTracer(const PkTracer&);
    PkTracer& operator=(const PkTracer&);

public:
    static void fromReadTrace(const std::string& str, int64_t locatorOffset);
    static void toSwiftTrace(const std::string& str, const std::string& locator);
    static void fromSwiftTrace(const std::string& str, const std::string& locator, int64_t brokerMsgId);
    static void toBuildTrace(const std::string& str, int originDocOperator, int docOperator);
    static void buildFailTrace(const std::string& str, int originDocOperator, int docOperator);

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::common

#endif // ISEARCH_BS_PKTRACER_H
