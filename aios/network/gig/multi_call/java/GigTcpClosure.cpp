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
#include "aios/network/gig/multi_call/java/GigTcpClosure.h"

#include "aios/network/gig/multi_call/interface/TcpResponse.h"
#include "aios/network/gig/multi_call/java/GigJavaUtil.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigTcpClosure);

GigTcpClosure::GigTcpClosure(JavaCallback callback, long callbackId,
                             const GigRequestGeneratorPtr &generator)
    : GigJavaClosure(callback, callbackId, generator) {
}

GigTcpClosure::~GigTcpClosure() {
}

bool GigTcpClosure::extractResponse(ResponsePtr response, GigResponseHeader *responseHeader,
                                    const char *&body, size_t &bodySize) {
    TcpResponsePtr gigTcpResponse = dynamic_pointer_cast<TcpResponse>(response);
    if (!gigTcpResponse) {
        string errStr = "gig tcp response is null";
        AUTIL_LOG(ERROR, "%s", errStr.c_str());
        responseHeader->set_error_msg(errStr);
        responseHeader->set_error_code(GigErrorCode::GIG_REPLY_ERROR_RESPONSE);
        return false;
    }
    body = gigTcpResponse->getBody(bodySize);
    if (!body) {
        AUTIL_LOG(ERROR,
                  "tcp response data is empty, ec [%d], err [%s], biz [%s], "
                  "remote [%s]",
                  gigTcpResponse->getErrorCode(), gigTcpResponse->errorString().c_str(),
                  gigTcpResponse->getBizName().c_str(), gigTcpResponse->getSpecStr().c_str());
        return false;
    }
    return true;
}

} // namespace multi_call
