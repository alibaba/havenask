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
#include "suez/sdk/RemoteTableWriterRequestGenerator.h"

#include <algorithm>

#include "alog/Logger.h"
#include "autil/RangeUtil.h"
#include "autil/TimeUtility.h"
#include "google/protobuf/arena.h"
#include "multi_call/common/ErrorCode.h"
#include "multi_call/interface/RequestGenerator.h"
#include "suez/service/Service.pb.h"

using namespace std;
using namespace autil;
using namespace multi_call;

namespace suez {

static const std::string WRITE_METHOD_NAME = "writeTable";

AUTIL_LOG_SETUP(sql, RemoteTableWriterRequestGenerator);

RemoteTableWriterRequest::RemoteTableWriterRequest(const std::string &methodName,
                                                   google::protobuf::Message *message,
                                                   uint32_t timeoutMs,
                                                   const std::shared_ptr<google::protobuf::Arena> &arena)
    : ArpcRequest<suez::TableService_Stub>(methodName, arena) {
    setMessage(message);
    setTimeout(timeoutMs);
}

RemoteTableWriterRequest::~RemoteTableWriterRequest() {}

ResponsePtr RemoteTableWriterRequest::newResponse() {
    return std::make_shared<RemoteTableWriterResponse>(getProtobufArena());
}

google::protobuf::Message *RemoteTableWriterRequest::serializeMessage() {
    auto pbMessage = dynamic_cast<suez::WriteRequest *>(_message);
    if (!pbMessage) {
        return nullptr;
    }
    return _message;
}

RemoteTableWriterResponse::RemoteTableWriterResponse(const std::shared_ptr<google::protobuf::Arena> &arena)
    : ArpcResponse(arena) {}

RemoteTableWriterResponse::~RemoteTableWriterResponse() {}

bool RemoteTableWriterResponse::deserializeApp() {
    auto message = dynamic_cast<suez::WriteResponse *>(getMessage());
    if (!message) {
        return false;
    }
    return true;
}

std::string RemoteTableWriterResponse::toString() const {
    auto writeResponse = dynamic_cast<WriteResponse *>(getMessage());
    if (writeResponse == nullptr) {
        return Response::toString();
    } else {
        return Response::toString() + ", errorMsg: [" + writeResponse->errorinfo().ShortDebugString() + "]";
    }
}

RemoteTableWriterRequestGenerator::RemoteTableWriterRequestGenerator(RemoteTableWriterParam param)
    : RequestGenerator(param.bizName, param.sourceId), _param(std::move(param)) {
    setDisableRetry(true);
    setDisableProbe(true);
    auto tags = make_shared<MatchTagMap>();
    tags->emplace(GeneratorDef::LEADER_TAG, param.tagInfo);
    setMatchTags(tags);
}

void RemoteTableWriterRequestGenerator::generate(multi_call::PartIdTy partCnt, multi_call::PartRequestMap &requestMap) {
    PartDocMap partMap;
    createPartMap(partCnt, partMap);
    for (const auto &pair : partMap) {
        requestMap.emplace(pair.first, createWriteRequest(pair.second));
    }
}

void RemoteTableWriterRequestGenerator::createPartMap(multi_call::PartIdTy partCnt, PartDocMap &partMap) {
    auto docs = std::move(_param.docs);
    for (auto &doc : docs) {
        auto idx = RangeUtil::getRangeIdxByHashId(0, RangeUtil::MAX_PARTITION_RANGE, partCnt, doc.first);
        partMap[idx].emplace_back(std::move(doc));
    }
}

multi_call::RequestPtr RemoteTableWriterRequestGenerator::createWriteRequest(const std::vector<WriteDoc> &docs) {
    auto arena = getProtobufArena().get();
    auto writeRequest = google::protobuf::Arena::CreateMessage<WriteRequest>(arena);
    writeRequest->set_tablename(_param.tableName);
    writeRequest->set_format(_param.format);
    for (const auto &doc : docs) {
        auto newWrite = writeRequest->mutable_writes()->Add();
        newWrite->set_hashid(doc.first);
        newWrite->set_str(doc.second);
    }
    auto request = std::make_shared<RemoteTableWriterRequest>(
        WRITE_METHOD_NAME, writeRequest, TimeUtility::us2ms(_param.timeoutUs), getProtobufArena());
    return request;
}

} // namespace suez
