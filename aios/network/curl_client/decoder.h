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
#ifndef CURL_CLIENT_DECODER_H
#define CURL_CLIENT_DECODER_H

#include <cstdio>
#include <cstdlib>
#include <curl_client/base_type.h>
#include <memory>
#include <rapidjson/error/en.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <stdio.h>

#include "autil/Log.h"
#include "rapidjson/document.h"

namespace network {

using HttpResponseCallback = std::function<void(const HttpResponse &)>;

class Decoder {
public:
    Decoder(const HttpResponseCallback &callback) : callback(callback) { result.curlCode = 0; }
    virtual ~Decoder() {}

    const HttpResponse &GetResult() const;
    size_t DecodeHeader(void *data, size_t size, size_t nmemb);
    size_t Decode(void *data, size_t size, size_t nmemb);
    void DecodeDone();

private:
    Decoder(const Decoder &);
    Decoder &operator=(const Decoder &);

private:
    std::string DecodeEventStream(const std::string &body) const;

protected:
    HttpResponseCallback callback;
    HttpResponse result;
    bool isEventStream = false;
    AUTIL_LOG_DECLARE();
};

CURL_CLIENT_TYPEDEF_PTR(Decoder)

} // namespace network

#endif // CURL_CLIENT_DECODER_H
