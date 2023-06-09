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
#include "ha3/sql/ops/externalTable/ha3sql/Ha3SqlRequestGenerator.h"

#include "google/protobuf/arena.h"
#include "ha3/proto/QrsService.pb.h"
#include "multi_call/common/common.h"
#include "multi_call/interface/Request.h"

using namespace std;

namespace isearch {
namespace sql {

const std::string GigHa3SqlRequest::HA3_SQL_METHOD_NAME = "searchSql";

GigHa3SqlRequest::GigHa3SqlRequest(google::protobuf::Message *message,
                                   const std::shared_ptr<google::protobuf::Arena> &arena,
                                   uint64_t timeoutInMs)
    : ArpcRequest<isearch::proto::QrsService_Stub>(HA3_SQL_METHOD_NAME, arena) {
    setMessage(message);
    setTimeout(timeoutInMs);
}

GigHa3SqlRequest::~GigHa3SqlRequest() {}

multi_call::ResponsePtr GigHa3SqlRequest::newResponse() {
    return std::make_shared<GigHa3SqlResponse>(getProtobufArena());
}

google::protobuf::Message *GigHa3SqlRequest::serializeMessage() {
    auto pbMessage = dynamic_cast<isearch::proto::QrsRequest *>(_message);
    if (!pbMessage) {
        return nullptr;
    }
    return _message;
}

GigHa3SqlResponse::GigHa3SqlResponse(const std::shared_ptr<google::protobuf::Arena> &arena)
    : multi_call::ArpcResponse(arena) {}

GigHa3SqlResponse::~GigHa3SqlResponse() {}

bool GigHa3SqlResponse::deserializeApp() {
    auto message = dynamic_cast<isearch::proto::QrsResponse *>(getMessage());
    if (!message) {
        return false;
    }
    return true;
}

AUTIL_LOG_SETUP(externalTable, Ha3SqlRequestGenerator);

Ha3SqlRequestGenerator::Ha3SqlRequestGenerator(const Ha3SqlParam &param)
    : RequestGenerator(param.bizName)
    , _param(param) {}

Ha3SqlRequestGenerator::~Ha3SqlRequestGenerator() {}

void Ha3SqlRequestGenerator::generate(multi_call::PartIdTy partCnt,
                                      multi_call::PartRequestMap &requestMap) {
    auto arena = getProtobufArena();
    auto *qrsRequest = google::protobuf::Arena::CreateMessage<isearch::proto::QrsRequest>(arena.get());
    qrsRequest->set_assemblyquery(_param.assemblyQuery);
    auto gigRequest = std::make_shared<GigHa3SqlRequest>(
        qrsRequest, arena, std::max<uint64_t>(_param.timeoutInMs, 0));
    requestMap.emplace(0, std::move(gigRequest));
}

} // namespace sql
} // namespace isearch
