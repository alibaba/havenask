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
#include "suez/service/GetSchemaVersionGenerator.h"

#include <autil/Log.h>

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, GetSchemaVersionGenerator);

namespace suez {

static const std::string GET_SCHEMA_VERSION_METHOD_NAME = "getSchemaVersion";

GigGetSchemaVersionRequest::GigGetSchemaVersionRequest(google::protobuf::Message *message,
                                                       uint32_t timeoutMs,
                                                       const std::shared_ptr<google::protobuf::Arena> &arena)
    : ArpcRequest<suez::TableService_Stub>(GET_SCHEMA_VERSION_METHOD_NAME, arena) {
    setMessage(message);
    setTimeout(timeoutMs);
}

GigGetSchemaVersionRequest::~GigGetSchemaVersionRequest() {}

multi_call::ResponsePtr GigGetSchemaVersionRequest::newResponse() {
    return std::make_shared<GigGetSchemaVersionResponse>(getProtobufArena());
}

google::protobuf::Message *GigGetSchemaVersionRequest::serializeMessage() {
    auto pbMessage = dynamic_cast<suez::GetSchemaVersionRequest *>(_message);
    if (!pbMessage) {
        return nullptr;
    }
    return _message;
}

GigGetSchemaVersionResponse::GigGetSchemaVersionResponse(const std::shared_ptr<google::protobuf::Arena> &arena)
    : multi_call::ArpcResponse(arena) {}

GigGetSchemaVersionResponse::~GigGetSchemaVersionResponse() {}

bool GigGetSchemaVersionResponse::deserializeApp() {
    auto message = dynamic_cast<suez::GetSchemaVersionResponse *>(getMessage());
    if (!message) {
        return false;
    }
    return true;
}

GetSchemaVersionGenerator::GetSchemaVersionGenerator(const std::string &bizName, const GetSchemaVersionRequest *request)
    : RequestGenerator(bizName), _request(request) {
    setDisableRetry(true);
    setDisableProbe(true);
    auto tags = std::make_shared<multi_call::MatchTagMap>();
    multi_call::TagMatchInfo info;
    info.type = multi_call::TMT_REQUIRE;
    tags->emplace(GeneratorDef::LEADER_TAG, info);
    setMatchTags(tags);
}

GetSchemaVersionGenerator::~GetSchemaVersionGenerator() {}

void GetSchemaVersionGenerator::generate(multi_call::PartIdTy partCnt, multi_call::PartRequestMap &requestMap) {
    auto arena = getProtobufArena();
    const auto &zoneName = _request->zonename();
    const auto &tableName = _request->tablename();
    auto timeout = _request->timeoutinms();
    const auto &partIds = _request->partids();
    for (auto partId : partIds) {
        auto requestPb = google::protobuf::Arena::CreateMessage<GetSchemaVersionRequest>(arena.get());
        requestPb->set_zonename(zoneName);
        requestPb->set_tablename(tableName);
        requestPb->mutable_partids()->Add(partId);
        auto request = std::make_shared<GigGetSchemaVersionRequest>(requestPb, timeout, arena);
        requestMap[partId] = request;
    }
}
} // namespace suez
