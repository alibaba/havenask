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
#pragma once

#include <stdint.h>
#include <memory>
#include <string>

#include "google/protobuf/message.h"
#include "aios/network/gig/multi_call/interface/ArpcRequest.h"
#include "aios/network/gig/multi_call/interface/Response.h"

#include "autil/Log.h" // IWYU pragma: keep

namespace google {
namespace protobuf {
class Arena;
}  // namespace protobuf
}  // namespace google
namespace suez {
namespace turing {
class GraphService_Stub;
}  // namespace turing
}  // namespace suez


namespace isearch {
namespace service {

class QrsRequest : public multi_call::ArpcRequest<suez::turing::GraphService_Stub>
{
public:
    QrsRequest(const std::string &bizName,
               google::protobuf::Message *message,
               uint32_t timeout,
               const std::shared_ptr<google::protobuf::Arena> &arena);
    ~QrsRequest();
private:
    QrsRequest(const QrsRequest &);
    QrsRequest& operator=(const QrsRequest &);
public:
    multi_call::ResponsePtr newResponse() override;
    google::protobuf::Message *serializeMessage() override;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsRequest> QrsRequestPtr;

} // namespace service
} // namespace isearch

