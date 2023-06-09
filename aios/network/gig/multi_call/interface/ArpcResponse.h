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
#ifndef ISEARCH_MULTI_CALL_ARPCRESPONSE_H
#define ISEARCH_MULTI_CALL_ARPCRESPONSE_H

#include "aios/network/gig/multi_call/interface/Response.h"
#include "google/protobuf/message.h"

namespace multi_call {

typedef std::shared_ptr<google::protobuf::Message> ProtoMessagePtr;

class ArpcResponse : public Response {
public:
    ArpcResponse(const std::shared_ptr<google::protobuf::Arena> &arena);
    ~ArpcResponse();

public:
    void init(void *data) override;
    size_t size() const override {
        if (_message) {
            return _message->ByteSize();
        } else {
            return 0;
        }
    }
    virtual google::protobuf::Message *getMessage() const;
    void fillSpan() override;

private:
    google::protobuf::Message *_message;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ArpcResponse);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_ARPCRESPONSE_H
