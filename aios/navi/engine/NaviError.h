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

#include "navi/proto/GraphVis.pb.h"
#include "navi/common.h"
#include "navi/log/LoggingEvent.h"

namespace navi {

class NaviError
{
public:
    NaviError() {
    }
    ~NaviError() {
    }
    NaviError(const NaviError &) = delete;
    NaviError &operator=(const NaviError &) = delete;
public:
    void toProto(NaviErrorDef &errorDef) const;
    void fromProto(const NaviErrorDef &errorDef);
private:
    static void fillSessionId(SessionIdDef *session, const SessionId &id);
private:
    friend class GraphResult;
public:
    SessionId id;
    ErrorCode ec = EC_NONE;
    LoggingEventPtr errorEvent;
    ResultLocationDef location;
};

NAVI_TYPEDEF_PTR(NaviError);

}

