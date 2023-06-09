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
#include "ha3/service/TraceSpanUtil.h"

#ifndef AIOS_OPEN_SOURCE
#include <Span.h>
#else
#include <aios/network/opentelemetry/core/Span.h>
#endif
#include <iomanip>
#include <ostream>

#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"

using namespace std;
using namespace autil;

using namespace isearch::proto;

namespace isearch {
namespace service {
AUTIL_LOG_SETUP(ha3, TraceSpanUtil);

const char *TraceSpanUtil::roleStr(proto::RoleType roleType) {
    switch (roleType) {
    case proto::ROLE_QRS:
        return "qrs";
    case proto::ROLE_PROXY:
        return "pxy";
    case proto::ROLE_SEARCHER:
        return "se";
    default:
        return "";
    }
}

std::string TraceSpanUtil::getRpcRole(const proto::PartitionID &pid) {
    std::stringstream ss;
    ss << roleStr(pid.role()) << '_' << pid.clustername() << '_';
    ss << setw(5) << setfill('0') << pid.range().from() << "_";
    ss << setw(5) << setfill('0') << pid.range().to();
    return ss.str();
}

void TraceSpanUtil::qrsReceiveRpc(
        const SpanPtr &span,
        const string &groupid,
        const string &appid)
{
    if (!span) {
        return;
    }
    span->setAttribute(opentelemetry::kEagleeyeAppGroup, groupid);
    span->setAttribute(opentelemetry::kEagleeyeAppId, appid);
    span->setAttribute(opentelemetry::kEagleeyeServiceName, "qrs");
    span->setAttribute(opentelemetry::kEagleeyeMethodName, "search");
    opentelemetry::EagleeyeUtil::setEAppId(span, appid);
}

std::string TraceSpanUtil::normAddr(const std::string &addr) {
    if (autil::StringUtil::startsWith(addr, "tcp:")) {
        return addr.substr(4);
    } else if (autil::StringUtil::startsWith(addr, "udp:")) {
        return addr.substr(4);
    } else {
        return addr;
    }
}

} // namespace service
} // namespace isearch
