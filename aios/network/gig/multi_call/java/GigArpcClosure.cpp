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
#include "aios/network/gig/multi_call/java/GigArpcClosure.h"

#include "aios/network/gig/multi_call/java/GigArpcResponse.h"
#include "aios/network/gig/multi_call/java/GigJavaUtil.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigArpcClosure);

GigArpcClosure::GigArpcClosure(JavaCallback callback, long callbackId,
                               const GigRequestGeneratorPtr &generator)
    : GigJavaClosure(callback, callbackId, generator) {
}

GigArpcClosure::~GigArpcClosure() {
}

bool GigArpcClosure::extractResponse(ResponsePtr response, GigResponseHeader *responseHeader,
                                     const char *&body, size_t &bodySize) {
    GigArpcResponsePtr gigArpcResponse = dynamic_pointer_cast<GigArpcResponse>(response);
    if (!gigArpcResponse) {
        string errStr = "gig arpc response is null";
        AUTIL_LOG(ERROR, "%s", errStr.c_str());
        responseHeader->set_error_msg(errStr);
        responseHeader->set_error_code(GigErrorCode::GIG_REPLY_ERROR_RESPONSE);
        return false;
    }
    std::string &responseData = gigArpcResponse->getData();
    if (!responseData.empty()) {
        body = responseData.c_str();
        bodySize = responseData.size();
        return true;
    } else {
        AUTIL_LOG(ERROR,
                  "arpc response data is empty, ec [%d], err [%s], biz [%s], "
                  "remote [%s]",
                  gigArpcResponse->getErrorCode(), gigArpcResponse->errorString().c_str(),
                  gigArpcResponse->getBizName().c_str(), gigArpcResponse->getSpecStr().c_str());
        return false;
    }
}

} // namespace multi_call
