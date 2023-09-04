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
#include "suez/service/KVBatchGetGenerator.h"

#include <autil/Log.h>

namespace suez {
const std::string GigKVBatchGetRequest::BATCH_GET_METHOD_NAME = "kvBatchGet";

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, KVBatchGetGenerator);

GigKVBatchGetRequest::GigKVBatchGetRequest(google::protobuf::Message *message,
                                           const std::shared_ptr<google::protobuf::Arena> &arena,
                                           uint64_t timeout)
    : ArpcRequest<suez::TableService_Stub>(BATCH_GET_METHOD_NAME, arena) {
    setMessage(message);
    setTimeout(timeout);
}

GigKVBatchGetRequest::~GigKVBatchGetRequest() {}

multi_call::ResponsePtr GigKVBatchGetRequest::newResponse() {
    return std::make_shared<GigKVBatchGetResponse>(getProtobufArena());
}

google::protobuf::Message *GigKVBatchGetRequest::serializeMessage() {
    auto pbMessage = dynamic_cast<suez::KVBatchGetRequest *>(_message);
    if (!pbMessage) {
        return nullptr;
    }
    return _message;
}

GigKVBatchGetResponse::GigKVBatchGetResponse(const std::shared_ptr<google::protobuf::Arena> &arena)
    : multi_call::ArpcResponse(arena) {}

GigKVBatchGetResponse::~GigKVBatchGetResponse() {}

bool GigKVBatchGetResponse::deserializeApp() {
    auto message = dynamic_cast<suez::KVBatchGetResponse *>(getMessage());
    if (!message) {
        appendErrorString("multi call error: get null message.");
        return false;
    }
    if (message->has_errorinfo()) {
        appendErrorString(message->errorinfo().errormsg());
        appendErrorString(message->DebugString());
    }
    if (message->has_binvalues()) {
        _isBinValues = true;
        _binKVList = std::make_pair(std::vector<uint64_t>(message->foundkeys().begin(), message->foundkeys().end()),
                                    autil::StringView(message->binvalues().data(), message->binvalues().size()));
        return true;
    }
    if (message->foundkeys_size() != message->values_size()) {
        appendErrorString("multi call error: foundkeys.size() != values.size().");
    } else {
        _kvList.reserve(message->foundkeys_size());
        for (auto i = 0; i < message->foundkeys_size(); i++) {
            _kvList.emplace_back(std::make_pair(
                message->foundkeys(i), autil::StringView(message->values(i).data(), message->values(i).size())));
        }
    }
    return true;
}

KVBatchGetGenerator::KVBatchGetGenerator(const KVBatchGetOptions &options)
    : RequestGenerator(options.serviceName), _options(options) {}

#define GEN_REQUEST_MAP(KEYTYPE)                                                                                       \
    auto arena = getProtobufArena();                                                                                   \
    auto kvBatchGetRequestPb = google::protobuf::Arena::CreateMessage<suez::KVBatchGetRequest>(arena.get());           \
    kvBatchGetRequestPb->set_tablename(_options.tableName);                                                            \
    kvBatchGetRequestPb->set_indexname(_options.indexName);                                                            \
    for (const auto &attr : _options.attrs) {                                                                          \
        kvBatchGetRequestPb->add_attrs(attr);                                                                          \
    }                                                                                                                  \
    kvBatchGetRequestPb->set_resulttype(_options.resultType);                                                          \
    kvBatchGetRequestPb->set_usehashkey(_options.useHashKey);                                                          \
    kvBatchGetRequestPb->set_timeoutinus(_options.timeout);                                                            \
    uint64_t partId = 0;                                                                                               \
    for (auto keySet : it.second) {                                                                                    \
        if (keySet.empty()) {                                                                                          \
            partId++;                                                                                                  \
            continue;                                                                                                  \
        }                                                                                                              \
        auto inputKey = kvBatchGetRequestPb->add_inputkeys();                                                          \
        inputKey->set_partid(partId++);                                                                                \
        for (auto key : keySet) {                                                                                      \
            inputKey->add_##KEYTYPE(key);                                                                              \
        }                                                                                                              \
    }                                                                                                                  \
    auto request = std::make_shared<GigKVBatchGetRequest>(kvBatchGetRequestPb, arena, _options.timeout);               \
    requestMap[it.first] = request

void KVBatchGetGenerator::generate(multi_call::PartIdTy partCnt, multi_call::PartRequestMap &requestMap) {
    if (_options.resultType == KVBatchGetResultType::BYTES || _options.resultType == KVBatchGetResultType::FLATBYTES) {
        for (auto it : _partId2HashKeysMap) {
            GEN_REQUEST_MAP(hashkeys);
        }
    } else {
        for (auto it : _partId2KeysMap) {
            GEN_REQUEST_MAP(keys);
        }
    }
}

} // namespace suez
