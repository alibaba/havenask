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

#include <map>
#include <vector>

#include "aios/network/gig/multi_call/interface/ArpcRequest.h"
#include "aios/network/gig/multi_call/interface/ArpcResponse.h"
#include "autil/Log.h"
#include "multi_call/common/common.h"
#include "multi_call/interface/Request.h"
#include "multi_call/interface/RequestGenerator.h"
#include "suez/common/GeneratorDef.h"
#include "suez/sdk/RemoteTableWriterParam.h"

namespace isearch {
namespace sql {
class SqlSearcherRequest;
} // namespace sql
} // namespace isearch

namespace suez {

class TableService_Stub;

class RemoteTableWriterRequest : public multi_call::ArpcRequest<suez::TableService_Stub> {
public:
    RemoteTableWriterRequest(const std::string &methodName,
                             google::protobuf::Message *message,
                             uint32_t timeoutMs,
                             const std::shared_ptr<google::protobuf::Arena> &arena);
    ~RemoteTableWriterRequest();

    RemoteTableWriterRequest(const RemoteTableWriterRequest &) = delete;
    RemoteTableWriterRequest &operator=(const RemoteTableWriterRequest &) = delete;

public:
    multi_call::ResponsePtr newResponse() override;
    google::protobuf::Message *serializeMessage() override;
};

class RemoteTableWriterResponse : public multi_call::ArpcResponse {
public:
    RemoteTableWriterResponse(const std::shared_ptr<google::protobuf::Arena> &arena);
    ~RemoteTableWriterResponse();

private:
    RemoteTableWriterResponse(const RemoteTableWriterResponse &) = delete;
    RemoteTableWriterResponse &operator=(const RemoteTableWriterResponse &) = delete;

public:
    bool deserializeApp() override;
    std::string toString() const override;

private:
    AUTIL_LOG_DECLARE();
};

class RemoteTableWriterRequestGenerator : public multi_call::RequestGenerator {
public:
    RemoteTableWriterRequestGenerator(RemoteTableWriterParam param);
    ~RemoteTableWriterRequestGenerator() {}

private:
    RemoteTableWriterRequestGenerator(const RemoteTableWriterRequestGenerator &);
    RemoteTableWriterRequestGenerator &operator=(const RemoteTableWriterRequestGenerator &);

private:
    void generate(multi_call::PartIdTy partCnt, multi_call::PartRequestMap &requestMap) override;

private:
    void createPartMap(multi_call::PartIdTy partCnt, PartDocMap &partMap);
    multi_call::RequestPtr createWriteRequest(const std::vector<WriteDoc> &docs);

private:
    RemoteTableWriterParam _param;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RemoteTableWriterRequestGenerator> RemoteTableWriterRequestGeneratorPtr;

} // namespace suez
