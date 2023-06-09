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
#include "aios/network/anet/httpstreamingcontext.h"
#include "aios/network/anet/aneterror.h"

#include "aios/network/anet/streamingcontext.h"

namespace anet {
HTTPStreamingContext::HTTPStreamingContext() {
    _step = HSS_START_LINE;
    _dataLength = 0;
    _drainedLength = 0;
    _headersCount = 0;
    _chunkState = CHUNK_SIZE;
}

HTTPStreamingContext:: ~HTTPStreamingContext() {
}

void HTTPStreamingContext::reset() {
    _step = HSS_START_LINE;
    _dataLength = 0;
    _drainedLength = 0;
    _headersCount = 0;
    _chunkState = CHUNK_SIZE;
    StreamingContext::reset();
}

void HTTPStreamingContext::setErrorNo(int errorNo) {
    setBroken(true);
    _errorNo = errorNo;
    if (errorNo == AnetError::INVALID_DATA) {
        _errorString = AnetError::INVALID_DATA_S;
    } else if (errorNo == AnetError::PKG_TOO_LARGE) {
        _errorString = AnetError::PKG_TOO_LARGE_S;
    } else if (errorNo == AnetError::LENGTH_REQUIRED) {
        _errorString = AnetError::LENGTH_REQUIRED_S;
    } else if (errorNo == AnetError::URI_TOO_LARGE) {
        _errorString = AnetError::URI_TOO_LARGE_S;
    } else if (errorNo == AnetError::VERSION_NOT_SUPPORT) {
        _errorString = AnetError::VERSION_NOT_SUPPORT_S;
    } else if (errorNo == AnetError::TOO_MANY_HEADERS) {
        _errorString = AnetError::TOO_MANY_HEADERS_S;
    } else if (errorNo == AnetError::CONNECTION_CLOSED) {
        _errorString = AnetError::CONNECTION_CLOSED_S;
    } else {
        _errorString = NULL;
    }
}

}/*end namespace anet*/
