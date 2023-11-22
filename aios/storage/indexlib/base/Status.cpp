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
#include "indexlib/base/Status.h"

namespace indexlib { namespace detail {

static std::string CodeToStatusStr(StatusCode code)
{
    switch (code) {
    case StatusCode::EC_OK:
        return "OK";
    case StatusCode::EC_CORRUPTION:
        return "CORRUPTION";
    case StatusCode::EC_IOERROR:
        return "IO_ERROR";
    case StatusCode::EC_NOMEM:
        return "NO_MEMORY";
    case StatusCode::EC_UNKNOWN:
        return "UNKNOWN";
    case StatusCode::EC_CONFIGERROR:
        return "CONFIG_ERROR";
    case StatusCode::EC_INVALID_ARGS:
        return "INVALID_ARGS";
    case StatusCode::EC_INTERNAL:
        return "INTERNAL_ERROR";
    case StatusCode::EC_NEED_DUMP:
        return "NEED_DUMP";
    case StatusCode::EC_UNIMPLEMENT:
        return "UNIMPLEMENT";
    case StatusCode::EC_OUT_OF_RANGE:
        return "OUT_OF_RANGE";
    // case StatusCode::EC_NOENT:
    //     return "NO_ENTRY";
    case StatusCode::EC_UNINITIALIZE:
        return "UNINITIALIZE";
    case StatusCode::EC_NOT_FOUND:
        return "NOT_FOUND";
    case StatusCode::EC_EXPIRED:
        return "EXPIRED";
    case StatusCode::EC_EXIST:
        return "EXIST";
    case StatusCode::EC_SEALED:
        return "SEALED";
    default:
        return "";
    }
}

std::string StatusError::message() const
{
    std::string msg = CodeToStatusStr(code);
    if (!_msg.empty()) {
        msg.append(": ");
        msg.append(_msg.data(), _msg.length());
    }
    return msg;
}

}} // namespace indexlib::detail
