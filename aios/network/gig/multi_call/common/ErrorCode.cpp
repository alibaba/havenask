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
#include "aios/network/gig/multi_call/common/ErrorCode.h"

using namespace std;

namespace multi_call {

const char *translateErrorCode(MultiCallErrorCode ec) {
    switch (ec) {
    case MULTI_CALL_ERROR_NONE:
        return "MULTI_CALL_ERROR_NONE";
    case MULTI_CALL_ERROR_DEC_WEIGHT:
        return "MULTI_CALL_ERROR_DEC_WEIGHT";
    case MULTI_CALL_ERROR_NO_RESPONSE:
        return "MULTI_CALL_ERROR_NO_RESPONSE";
    case MULTI_CALL_ERROR_RPC_FAILED:
        return "MULTI_CALL_ERROR_RPC_FAILED";
    case MULTI_CALL_REPLY_ERROR_VERSION_NOT_EXIST:
        return "MULTI_CALL_REPLY_ERROR_VERSION_NOT_EXIST";
    case MULTI_CALL_REPLY_ERROR_REQUEST_NOT_EXIST:
        return "MULTI_CALL_REPLY_ERROR_REQUEST_NOT_EXIST";
    case MULTI_CALL_REPLY_ERROR_NOT_CREATED:
        return "MULTI_CALL_REPLY_ERROR_NOT_CREATED";
    case MULTI_CALL_REPLY_ERROR_NO_PROVIDER_FOUND:
        return "MULTI_CALL_REPLY_ERROR_NO_PROVIDER_FOUND";
    case MULTI_CALL_REPLY_ERROR_LACK_OF_PROVIDER:
        return "MULTI_CALL_REPLY_ERROR_LACK_OF_PROVIDER";
    case MULTI_CALL_REPLY_ERROR_TIMEOUT:
        return "MULTI_CALL_REPLY_ERROR_TIMEOUT";
    case MULTI_CALL_REPLY_ERROR_DESERIALIZE_APP:
        return "MULTI_CALL_REPLY_ERROR_DESERIALIZE_APP";
    case MULTI_CALL_REPLY_ERROR_CREATE_RESPONSE:
        return "MULTI_CALL_REPLY_ERROR_CREATE_RESPONSE";
    case MULTI_CALL_REPLY_ERROR_REQUEST:
        return "MULTI_CALL_REPLY_ERROR_REQUEST";
    case MULTI_CALL_REPLY_ERROR_CONNECTION:
        return "MULTI_CALL_REPLY_ERROR_CONNECTION";
    case MULTI_CALL_REPLY_ERROR_PROTOCOL_MESSAGE:
        return "MULTI_CALL_REPLY_ERROR_PROTOCOL_MESSAGE";
    case MULTI_CALL_REPLY_ERROR_RESPONSE:
        return "MULTI_CALL_REPLY_ERROR_RESPONSE";
    case MULTI_CALL_REPLY_ERROR_POST:
        return "MULTI_CALL_REPLY_ERROR_POST";
    case MULTI_CALL_ERROR_NO_METHOD:
        return "MULTI_CALL_ERROR_NO_METHOD";
    case MULTI_CALL_ERROR_STREAM_CREATOR:
        return "MULTI_CALL_ERROR_STREAM_CREATOR";
    case MULTI_CALL_ERROR_STREAM_RECEIVE:
        return "MULTI_CALL_ERROR_STREAM_RECEIVE";
    case MULTI_CALL_ERROR_STREAM_CANCELLED:
        return "MULTI_CALL_ERROR_STREAM_CANCELLED";
    case MULTI_CALL_ERROR_STREAM_INIT:
        return "MULTI_CALL_ERROR_STREAM_INIT";
    case MULTI_CALL_ERROR_STREAM_STOPPED:
        return "MULTI_CALL_ERROR_STREAM_STOPPED";
    case MULTI_CALL_ERROR_STREAM_OVERRUN:
        return "MULTI_CALL_ERROR_STREAM_OVERRUN";
    case MULTI_CALL_ERROR_QUEUE_FULL:
        return "MULTI_CALL_ERROR_QUEUE_FULL";
    case MULTI_CALL_ERROR_GENERATE:
        return "MULTI_CALL_ERROR_GENERATE";
    case MULTI_CALL_NULL_GENERATOR:
        return "MULTI_CALL_NULL_GENERATOR";
    case MULTI_CALL_NULL_SNAPSHOT:
        return "MULTI_CALL_NULL_SNAPSHOT";
    default:
        return "MULTI_CALL_ERROR_UNKNOWN";
    }
}

} // namespace multi_call
