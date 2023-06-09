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
#include "autil/Log.h"
#include "multi_call/interface/RequestGenerator.h"

namespace isearch {

namespace proto {
class QrsService_Stub;
} // namespace proto

namespace sql {

class GigHa3SqlRequest : public multi_call::ArpcRequest<isearch::proto::QrsService_Stub> {
public:
    GigHa3SqlRequest(google::protobuf::Message *message,
                     const std::shared_ptr<google::protobuf::Arena> &arena,
                     uint64_t timeoutInMs);
    ~GigHa3SqlRequest();
    GigHa3SqlRequest(const GigHa3SqlRequest &) = delete;

public:
    multi_call::ResponsePtr newResponse() override;
    google::protobuf::Message *serializeMessage() override;

private:
    static const std::string HA3_SQL_METHOD_NAME;
};

class GigHa3SqlResponse : public multi_call::ArpcResponse {
public:
    GigHa3SqlResponse(const std::shared_ptr<google::protobuf::Arena> &arena);
    ~GigHa3SqlResponse();
    GigHa3SqlResponse(const GigHa3SqlResponse &) = delete;
    GigHa3SqlResponse &operator=(const GigHa3SqlResponse &) = delete;

public:
    bool deserializeApp() override;
};

class Ha3SqlRequestGenerator : public multi_call::RequestGenerator {
public:
    struct Ha3SqlParam {
        std::string assemblyQuery;
        std::string bizName;
        int64_t timeoutInMs;
    };

public:
    Ha3SqlRequestGenerator(const Ha3SqlParam &param);
    ~Ha3SqlRequestGenerator();
    Ha3SqlRequestGenerator(const Ha3SqlRequestGenerator &) = delete;
    Ha3SqlRequestGenerator& operator=(const Ha3SqlRequestGenerator &) = delete;

public:
    void generate(multi_call::PartIdTy partCnt, multi_call::PartRequestMap &requestMap) override;

private:
    Ha3SqlParam _param;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Ha3SqlRequestGenerator> Ha3SqlRequestGeneratorPtr;

} // namespace sql
} // namespace isearch
