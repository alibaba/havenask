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
#include "aios/network/gig/multi_call/java/GigJavaClosure.h"

#include "aios/network/gig/multi_call/java/GigJavaUtil.h"
#include "aios/network/gig/multi_call/java/GigJniThreadAttacher.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigJavaClosure);

void GigJavaClosure::Run() {
    if (!_callback) {
        AUTIL_LOG(ERROR, "callback is null, callbackId [%ld]", _callbackId);
        delete this;
        return;
    }
    GigJniThreadAttacher::attachCurrentThread();

    assert(_arena);
    GigResponseHeader *responseHeader =
        google::protobuf::Arena::CreateMessage<GigResponseHeader>(_arena.get());
    string header;

    size_t lackCount = 0;
    vector<ResponsePtr> responses = _reply->getResponses(lackCount);

    if (responses.empty()) {
        string errStr = "response is empty, maybe degrade triggered or no provider for biz";
        AUTIL_LOG(ERROR, "%s", errStr.c_str());
        responseHeader->set_error_msg(errStr);
        responseHeader->set_error_code(GigErrorCode::GIG_ERROR_NO_RESPONSE);
        responseHeader->SerializeToString(&header);
        _callback(_callbackId, (char *)header.c_str(), header.size(), NULL, 0);
        delete this;
        return;
    }

    const char *body = NULL;
    size_t bodySize = 0;
    string output;
    bool result = false;
    if (!_isMulti) {
        assert(1u == responses.size());
        result = extract(responses[0], responseHeader, body, bodySize);
    } else {
        GigResponseHeader *multiHeader = NULL;
        GigMultiResponse *multiResponse =
            google::protobuf::Arena::CreateMessage<GigMultiResponse>(_arena.get());
        for (int i = 0; i < responses.size(); i++) {
            body = NULL;
            bodySize = 0;
            multiHeader = multiResponse->add_headers();
            extract(responses[i], multiHeader, body, bodySize);
            multiResponse->add_responses(body, bodySize);
        }
        if (!_requestId.empty()) {
            responseHeader->set_request_id(_requestId);
        }
        if (multiResponse->SerializeToString(&output)) {
            body = output.c_str();
            bodySize = output.size();
            responseHeader->set_error_code(GigErrorCode::GIG_ERROR_NONE);
            result = true;
        } else {
            AUTIL_LOG(ERROR, "[%s] serialize multi response failed, callbackId [%p]",
                      typeid(*this).name(), (void *)_callbackId);
            responseHeader->set_error_code(GigErrorCode::GIG_REPLY_ERROR_CREATE_RESPONSE);
            result = false;
        }
    }

    if (_span) {
        _span->setAttribute("gig.status", GigErrorCode_Name(responseHeader->error_code()));
        _span->setStatus(result ? opentelemetry::StatusCode::kOk
                                : opentelemetry::StatusCode::kError,
                         responseHeader->error_msg());
    }
    if (!result) {
        std::string traceId, spanId;
        if (_span) {
            auto spanContext = _span->getContext();
            if (spanContext) {
                traceId = spanContext->getTraceId();
                spanId = spanContext->getSpanId();
            }
        }
        AUTIL_LOG(ERROR, "[%s %s][%s] run failed, callbackId [%p]", traceId.c_str(), spanId.c_str(),
                  typeid(*this).name(), (void *)_callbackId);
    }
    responseHeader->SerializeToString(&header);
    _callback(_callbackId, (char *)header.c_str(), header.size(), (char *)body, bodySize);
    if (_span) {
        _span->end();
    }
    delete this;
}

bool GigJavaClosure::extract(ResponsePtr response, GigResponseHeader *responseHeader,
                             const char *&body, size_t &bodySize) {
    responseHeader->set_error_code(GigErrorCode(response->getErrorCode()));
    responseHeader->set_error_msg(response->errorString());
    responseHeader->set_remote_address(response->getSpecStr());
    responseHeader->set_request_id(response->getRequestId());
    responseHeader->set_node_id(response->getNodeId());
    responseHeader->set_part_count(response->getPartCount());
    responseHeader->set_part_id(response->getPartId());
    return extractResponse(response, responseHeader, body, bodySize);
}

} // namespace multi_call
