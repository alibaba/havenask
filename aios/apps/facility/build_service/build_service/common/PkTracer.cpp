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
#include "build_service/common/PkTracer.h"

using namespace std;
using namespace autil;

namespace build_service { namespace common {
BS_LOG_SETUP(common, PkTracer);

void PkTracer::fromReadTrace(const std::string& str, int64_t locatorOffset)
{
    BS_LOG(TRACE1, "from_read %s %ld", str.c_str(), locatorOffset);
}

void PkTracer::toSwiftTrace(const std::string& str, const string& locator)
{
    BS_LOG(TRACE1, "to_swift %s %s", str.c_str(), locator.c_str());
}

void PkTracer::fromSwiftTrace(const std::string& str, const string& locator, int64_t brokerMsgId)
{
    BS_LOG(TRACE1, "from_swift %s %s %ld", str.c_str(), locator.c_str(), brokerMsgId);
}

void PkTracer::toBuildTrace(const std::string& str, int originDocOperator, int docOperator)
{
    BS_LOG(TRACE1, "to_build %s %d %d", str.c_str(), originDocOperator, docOperator);
}

void PkTracer::buildFailTrace(const std::string& str, int originDocOperator, int docOperator)
{
    BS_LOG(TRACE1, "build_fail %s %d %d", str.c_str(), originDocOperator, docOperator);
}

}} // namespace build_service::common
