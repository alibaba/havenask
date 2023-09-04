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
#include "suez/service/UpdateSchemaGenerator.h"

#include <autil/Log.h>

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, UpdateSchemaGenerator);

namespace suez {

static const std::string UPDATE_SCHEMA_METHOD_NAME = "updateSchema";

GigUpdateSchemaRequest::GigUpdateSchemaRequest(google::protobuf::Message *message,
                                               uint32_t timeoutMs,
                                               const std::shared_ptr<google::protobuf::Arena> &arena)
    : ArpcRequest<suez::TableService_Stub>(UPDATE_SCHEMA_METHOD_NAME, arena) {
    setMessage(message);
    setTimeout(timeoutMs);
}

GigUpdateSchemaRequest::~GigUpdateSchemaRequest() {}

multi_call::ResponsePtr GigUpdateSchemaRequest::newResponse() {
    return std::make_shared<GigUpdateSchemaResponse>(getProtobufArena());
}

google::protobuf::Message *GigUpdateSchemaRequest::serializeMessage() {
    auto pbMessage = dynamic_cast<suez::UpdateSchemaRequest *>(_message);
    if (!pbMessage) {
        return nullptr;
    }
    return _message;
}

GigUpdateSchemaResponse::GigUpdateSchemaResponse(const std::shared_ptr<google::protobuf::Arena> &arena)
    : multi_call::ArpcResponse(arena) {}

GigUpdateSchemaResponse::~GigUpdateSchemaResponse() {}

bool GigUpdateSchemaResponse::deserializeApp() {
    auto message = dynamic_cast<suez::UpdateSchemaResponse *>(getMessage());
    if (!message) {
        return false;
    }
    return true;
}

UpdateSchemaGenerator::UpdateSchemaGenerator(const std::string &bizName, const UpdateSchemaRequest *request)
    : RequestGenerator(bizName), _request(request) {
    setDisableRetry(true);
    setDisableProbe(true);
    auto tags = std::make_shared<multi_call::MatchTagMap>();
    multi_call::TagMatchInfo info;
    info.type = multi_call::TMT_REQUIRE;
    tags->emplace(GeneratorDef::LEADER_TAG, info);
    setMatchTags(tags);
}

UpdateSchemaGenerator::~UpdateSchemaGenerator() {}

void UpdateSchemaGenerator::generate(multi_call::PartIdTy partCnt, multi_call::PartRequestMap &requestMap) {
    auto arena = getProtobufArena();
    const auto &zoneName = _request->zonename();
    const auto &tableName = _request->tablename();
    const auto &configPath = _request->configpath();
    auto timeout = _request->timeoutinms();
    const auto &partIds = _request->partids();
    for (auto partId : partIds) {
        auto updateRequestPb = google::protobuf::Arena::CreateMessage<UpdateSchemaRequest>(arena.get());
        updateRequestPb->set_zonename(zoneName);
        updateRequestPb->set_tablename(tableName);
        updateRequestPb->mutable_partids()->Add(partId);
        updateRequestPb->set_configpath(configPath);
        updateRequestPb->set_schemaversion(_request->schemaversion());
        auto request = std::make_shared<GigUpdateSchemaRequest>(updateRequestPb, timeout, arena);
        requestMap[partId] = request;
    }
}

} // namespace suez
