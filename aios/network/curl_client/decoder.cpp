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
#include <curl_client/decoder.h>
#include <sstream>

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace network {
AUTIL_LOG_SETUP(network, Decoder);

size_t Decoder::DecodeHeader(void *data, size_t size, size_t nmemb) {
    string header(reinterpret_cast<char *>(data), size * nmemb);
    StringUtil::trim(header);
    StringUtil::replaceAll(header, "\r", "");
    StringUtil::replaceAll(header, "\n", "");
    size_t seperator = header.find_first_of(':');
    if (string::npos != seperator) {
        string key = header.substr(0, seperator);
        StringUtil::trim(key);
        string value = header.substr(seperator + 1);
        StringUtil::trim(value);
        result.headers.insert(make_pair(key, value));
        AUTIL_LOG(DEBUG, "adding header [%s]=[%s]", key.c_str(), value.c_str());
    } else if (StringUtil::startsWith(header, "HTTP/")) {
        istringstream iss(header);
        string httpVersion;
        iss >> httpVersion;
        string statusCode;
        iss >> statusCode;
        string status;
        getline(iss, status);
        StringUtil::trim(status);
        result.httpCode = atoi(statusCode.c_str());
        AUTIL_LOG(
            DEBUG, "parsed http version %s code %d status %s", httpVersion.c_str(), result.httpCode, status.c_str());
    } else {
        if (header.length() > 0) {
            AUTIL_LOG(DEBUG, "unknown header %s", header.c_str());
            this->result.headers[header] = "unknown-header";
        }
    }
    isEventStream = result.headers.find("Content-Type") != result.headers.end() &&
                    result.headers.at("Content-Type") == "text/event-stream";
    return (size * nmemb);
}

const HttpResponse &Decoder::GetResult() const { return result; }

void Decoder::DecodeDone() {
    result.done = true;
    callback(result);
}

size_t Decoder::Decode(void *data, size_t size, size_t nmemb) {
    size_t len = size * nmemb;
    const auto body = string(reinterpret_cast<char *>(data), len);
    AUTIL_LOG(DEBUG, "decode [%s], sse=%d", body.c_str(), isEventStream);
    if (isEventStream) {
        const auto sseResponse = DecodeEventStream(body);
        if (!sseResponse.empty()) {
            result.body = sseResponse;
        }
    } else {
        result.body.append(body);
    }
    return len;
}

string Decoder::DecodeEventStream(const string &body) const {
    const auto segments = StringUtil::split(body, "\r\n", false);
    if (segments.empty()) {
        AUTIL_LOG(DEBUG, "invalid sse body [%s]", body.c_str());
        return "";
    }

    for (auto segment_iter = segments.rbegin(); segment_iter != segments.rend(); segment_iter++) {
        auto segment = *segment_iter;
        StringUtil::trim(segment);
        StringUtil::replaceAll(segment, "\r\n", "");

        if (segment.empty()) {
            continue;
        }
        const auto colon = segment.find_first_of(':');
        if (colon == string::npos) {
            AUTIL_LOG(DEBUG, "invalid sse segment [%s]", segment.c_str());
            continue;
        }
        const auto key = segment.substr(0, colon);
        const auto value = segment.substr(colon + 1);
        if (key == "data" && value != "[done]") {
            AUTIL_LOG(DEBUG, "sse decoder decode segment [%s]", value.c_str());
            return value;
        }
    }
    AUTIL_LOG(DEBUG, "invalid sse body [%s]", body.c_str());
    return "";
}

} // namespace network
