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
#include "ha3/service/QrsRequest.h"

#include <cstddef>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "google/protobuf/message.h"
#include "ha3/service/QrsResponse.h"
#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "aios/network/gig/multi_call/interface/ArpcRequest.h"
#include "suez/turing/proto/Search.pb.h"

namespace google {
namespace protobuf {
class Arena;
}  // namespace protobuf
}  // namespace google

using namespace std;
using namespace multi_call;
using namespace autil;

namespace isearch {
namespace service {
AUTIL_LOG_SETUP(ha3, QrsRequest);

QrsRequest::QrsRequest(const std::string &methodName,
                       google::protobuf::Message *message,
                       uint32_t timeout,
                       const std::shared_ptr<google::protobuf::Arena> &arena)
    : ArpcRequest<suez::turing::GraphService_Stub>(methodName, arena)
{
    setMessage(message);
    setTimeout(timeout);
}

QrsRequest::~QrsRequest() {
}

ResponsePtr QrsRequest::newResponse() {
    return ResponsePtr(new QrsResponse(getProtobufArena()));
}

google::protobuf::Message *QrsRequest::serializeMessage() {
    auto pbMessage = static_cast<suez::turing::GraphRequest *>(_message);
    if (!pbMessage) {
        return NULL;
    }
    return _message;
}

} // namespace service
} // namespace isearch
