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
#ifndef ANET_HTTPSTREAMINGCONTEXT_H_
#define ANET_HTTPSTREAMINGCONTEXT_H_
#include <cstring>

#include "aios/network/anet/streamingcontext.h"

namespace anet {
class HTTPStreamingContext : public StreamingContext {
public:
    HTTPStreamingContext();
    virtual ~HTTPStreamingContext();

    enum HTTPStreamingStep {
        HSS_START_LINE = 0,
        HSS_MESSAGE_HEADER,
        HSS_MESSAGE_BODY
    };
    enum HTTPEncodingType {
        HET_NO_BODY = 0,
        HET_LENGTH,
        HET_CHUNKED,
        HET_EOF
    };
    enum HTTPChunkedState {
        CHUNK_SIZE = 0,
        CHUNK_DATA,
        CHUNK_DATA_CRLF,
        TRAILER
    };
    virtual void reset();
    void setErrorNo(int errorNo);

public:
    HTTPStreamingStep _step;
    HTTPChunkedState _chunkState;
    HTTPEncodingType _encodingType;
    size_t _dataLength;
    size_t _drainedLength;
    size_t _headersCount;
};

} /*end namespace anet*/
#endif /*ANET_HTTPSTREAMINGCONTEXT_H_*/
