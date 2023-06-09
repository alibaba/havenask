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
#ifndef ISEARCH_MULTI_CALL_ERRORCODE_H
#define ISEARCH_MULTI_CALL_ERRORCODE_H

#include <string>

namespace multi_call {

enum MultiCallErrorCode {
    MULTI_CALL_ERROR_NONE = 0,
    MULTI_CALL_ERROR_DEC_WEIGHT = 1,
    // below is failed ec
    MULTI_CALL_ERROR_NO_RESPONSE = 32,
    MULTI_CALL_ERROR_RPC_FAILED = 33,
    MULTI_CALL_REPLY_ERROR_VERSION_NOT_EXIST = 34,
    MULTI_CALL_REPLY_ERROR_REQUEST_NOT_EXIST = 35,
    MULTI_CALL_REPLY_ERROR_NOT_CREATED = 36,
    MULTI_CALL_REPLY_ERROR_NO_PROVIDER_FOUND = 37,
    MULTI_CALL_REPLY_ERROR_LACK_OF_PROVIDER = 38,
    MULTI_CALL_REPLY_ERROR_TIMEOUT = 39,
    MULTI_CALL_REPLY_ERROR_DESERIALIZE_APP = 40,
    MULTI_CALL_REPLY_ERROR_CREATE_RESPONSE = 41,
    MULTI_CALL_REPLY_ERROR_REQUEST = 42,
    MULTI_CALL_REPLY_ERROR_CONNECTION = 43,
    MULTI_CALL_REPLY_ERROR_PROTOCOL_MESSAGE = 44,
    MULTI_CALL_REPLY_ERROR_RESPONSE = 45,
    MULTI_CALL_REPLY_ERROR_POST = 46,
    MULTI_CALL_ERROR_NO_METHOD = 47,
    MULTI_CALL_ERROR_STREAM_CREATOR = 48,
    MULTI_CALL_ERROR_STREAM_RECEIVE = 49,
    MULTI_CALL_ERROR_STREAM_CANCELLED = 50,
    MULTI_CALL_ERROR_STREAM_INIT = 51,
    MULTI_CALL_ERROR_STREAM_STOPPED = 52,
    MULTI_CALL_ERROR_STREAM_OVERRUN = 53,
    MULTI_CALL_ERROR_QUEUE_FULL = 60,
    MULTI_CALL_ERROR_GENERATE = 61,
    // global error
    MULTI_CALL_NULL_GENERATOR = 129,
    MULTI_CALL_NULL_SNAPSHOT = 130,

    MULTI_CALL_ERROR_UNKNOWN = 255,
};

const char *translateErrorCode(MultiCallErrorCode ec);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_ERRORCODE_H
