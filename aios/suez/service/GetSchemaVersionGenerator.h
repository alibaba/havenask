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

class GigGetSchemaVersionRequest : public multi_call::ArpcRequest<suez::TableService_Stub> {
public:
    GigGetSchemaVersionRequest(google::protobuf::Message *message,
                               uint32_t timeoutMs,
                               const std::shared_ptr<google::protobuf::Arena> &arena);
    ~GigGetSchemaVersionRequest();
    GigGetSchemaVersionRequest(const GigGetSchemaVersionRequest &) = delete;
    GigGetSchemaVersionRequest &operator=(const GigGetSchemaVersionRequest &) = delete;

public:
    multi_call::ResponsePtr newResponse() override;
    google::protobuf::Message *serializeMessage() override;
};

class GigGetSchemaVersionResponse : public multi_call::ArpcResponse {
public:
    GigGetSchemaVersionResponse(const std::shared_ptr<google::protobuf::Arena> &arena);
    ~GigGetSchemaVersionResponse();

private:
    GigGetSchemaVersionResponse(const GigGetSchemaVersionResponse &) = delete;
    GigGetSchemaVersionResponse &operator=(const GigGetSchemaVersionResponse &) = delete;

public:
    bool deserializeApp() override;
};

class GetSchemaVersionGenerator : public multi_call::RequestGenerator {
public:
    GetSchemaVersionGenerator(const std::string &bizName, const GetSchemaVersionRequest *request);
    ~GetSchemaVersionGenerator();

private:
    GetSchemaVersionGenerator(const GetSchemaVersionGenerator &);
    GetSchemaVersionGenerator &operator=(const GetSchemaVersionGenerator &);

public:
    void generate(multi_call::PartIdTy partCnt, multi_call::PartRequestMap &requestMap) override;

private:
    const GetSchemaVersionRequest *_request;
};

} // namespace suez
