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

#include "aios/network/gig/multi_call/interface/ArpcRequest.h"
#include "aios/network/gig/multi_call/interface/ArpcResponse.h"
#include "multi_call/interface/RequestGenerator.h"
#include "suez/common/GeneratorDef.h"
#include "suez/service/Service.pb.h"

namespace suez {

class TableService_Stub;

class GigUpdateSchemaRequest : public multi_call::ArpcRequest<suez::TableService_Stub> {
public:
    GigUpdateSchemaRequest(google::protobuf::Message *message,
                           uint32_t timeoutMs,
                           const std::shared_ptr<google::protobuf::Arena> &arena);
    ~GigUpdateSchemaRequest();
    GigUpdateSchemaRequest(const GigUpdateSchemaRequest &) = delete;
    GigUpdateSchemaRequest &operator=(const GigUpdateSchemaRequest &) = delete;

public:
    multi_call::ResponsePtr newResponse() override;
    google::protobuf::Message *serializeMessage() override;
};

class GigUpdateSchemaResponse : public multi_call::ArpcResponse {
public:
    GigUpdateSchemaResponse(const std::shared_ptr<google::protobuf::Arena> &arena);
    ~GigUpdateSchemaResponse();

private:
    GigUpdateSchemaResponse(const GigUpdateSchemaResponse &) = delete;
    GigUpdateSchemaResponse &operator=(const GigUpdateSchemaResponse &) = delete;

public:
    bool deserializeApp() override;
};

class UpdateSchemaGenerator : public multi_call::RequestGenerator {
public:
    UpdateSchemaGenerator(const std::string &bizName, const UpdateSchemaRequest *request);
    ~UpdateSchemaGenerator();

private:
    UpdateSchemaGenerator(const UpdateSchemaGenerator &);
    UpdateSchemaGenerator &operator=(const UpdateSchemaGenerator &);

public:
    void generate(multi_call::PartIdTy partCnt, multi_call::PartRequestMap &requestMap) override;

private:
    const UpdateSchemaRequest *_request;
};

} // namespace suez
