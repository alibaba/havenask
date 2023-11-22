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
#include "navi/engine/NaviError.h"
#include "navi/util/CommonUtil.h"

namespace navi {

void NaviError::toProto(NaviErrorDef &errorDef) const {
    fillSessionId(errorDef.mutable_session(), id);
    errorDef.set_ec(ec);
    if (EC_NONE != ec) {
        errorDef.set_ec_str(CommonUtil::getErrorString(ec));
        if (errorEvent) {
            errorEvent->toProto(errorDef.mutable_error_event(), nullptr);
        }
        if (location.locations_size() > 0) {
            errorDef.mutable_location()->CopyFrom(location);
        }
    }
}

void NaviError::fromProto(const NaviErrorDef &errorDef) {
    const auto &sessionId = errorDef.session();
    id.instance = sessionId.instance();
    id.currentInstance = sessionId.current_instance();
    id.queryId = sessionId.query_id();
    ec = (ErrorCode)errorDef.ec();
    if (EC_NONE != ec) {
        if (errorDef.has_error_event()) {
            errorEvent = std::make_shared<LoggingEvent>();
            errorEvent->fromProto(errorDef.error_event());
        }
        if (errorDef.has_location()) {
            location.CopyFrom(errorDef.location());
        }
    }
}

void NaviError::fillSessionId(SessionIdDef *sessionDef, const SessionId &id) {
    sessionDef->set_instance(id.instance);
    sessionDef->set_current_instance(id.currentInstance);
    sessionDef->set_query_id(id.queryId);
}

}

