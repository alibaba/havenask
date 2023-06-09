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
#include "aios/network/gig/multi_call/java/GigHttpClosure.h"
#include "aios/network/anet/httppacket.h"
#include "aios/network/gig/multi_call/interface/HttpResponse.h"
#include "aios/network/gig/multi_call/java/GigJavaUtil.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigHttpClosure);

GigHttpClosure::GigHttpClosure(JavaCallback callback, long callbackId,
                               const GigRequestGeneratorPtr &generator)
    : GigJavaClosure(callback, callbackId, generator) {}

GigHttpClosure::~GigHttpClosure() {}

bool GigHttpClosure::extractResponse(ResponsePtr response,
                                     GigResponseHeader *responseHeader,
                                     const char *&body, size_t &bodySize) {
    auto httpResponse = dynamic_pointer_cast<HttpResponse>(response);
    if (!httpResponse) {
        string errStr = "gig http response is null";
        AUTIL_LOG(ERROR, "%s", errStr.c_str());
        responseHeader->set_error_msg(errStr);
        responseHeader->set_error_code(GigErrorCode::GIG_REPLY_ERROR_RESPONSE);
        return false;
    }
    responseHeader->set_status_code(httpResponse->getStatusCode()); // http code

    HTTPPacketPtr httpPacket = httpResponse->getPacket();
    if (!httpPacket) {
        AUTIL_LOG(ERROR,
                  "gig call failed, packet is null, biz [%s], remote [%s]",
                  httpResponse->getBizName().c_str(),
                  httpResponse->getSpecStr().c_str());
        return false;
    }
    body = httpPacket->getBody(bodySize);
    for (auto it = httpPacket->headerBegin(); it != httpPacket->headerEnd();
         it++) {
        auto header = responseHeader->add_headers();
        header->set_key(string(it->first));
        header->set_value(string(it->second));
    }

    if (httpResponse->isFailed()) {
        AUTIL_LOG(ERROR,
                  "gig call failed, ec [%d], http code [%d], err [%s], biz "
                  "[%s], remote [%s]",
                  httpResponse->getErrorCode(), httpResponse->getStatusCode(),
                  httpResponse->errorString().c_str(),
                  httpResponse->getBizName().c_str(),
                  httpResponse->getSpecStr().c_str());
        return false;
    }

    return true;
}

} // namespace multi_call
