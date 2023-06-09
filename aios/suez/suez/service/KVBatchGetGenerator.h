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
#include <sstream>

#include "absl/container/flat_hash_set.h"
#include "aios/network/gig/multi_call/interface/ArpcRequest.h"
#include "aios/network/gig/multi_call/interface/ArpcResponse.h"
#include "multi_call/interface/RequestGenerator.h"
#include "suez/service/Service.pb.h"

namespace suez {
class TableService_Stub;

class GigKVBatchGetRequest : public multi_call::ArpcRequest<suez::TableService_Stub> {
public:
    GigKVBatchGetRequest(google::protobuf::Message *message,
                         const std::shared_ptr<google::protobuf::Arena> &arena,
                         uint64_t timeout);
    ~GigKVBatchGetRequest();
    GigKVBatchGetRequest(const GigKVBatchGetRequest &) = delete;

public:
    multi_call::ResponsePtr newResponse() override;
    google::protobuf::Message *serializeMessage() override;

private:
    static const std::string BATCH_GET_METHOD_NAME;
};

class GigKVBatchGetResponse : public multi_call::ArpcResponse {
public:
    GigKVBatchGetResponse(const std::shared_ptr<google::protobuf::Arena> &arena);
    ~GigKVBatchGetResponse();
    GigKVBatchGetResponse(const GigKVBatchGetResponse &) = delete;
    GigKVBatchGetResponse &operator=(const GigKVBatchGetResponse &) = delete;

public:
    bool deserializeApp() override;
    bool isBinValues() const { return _isBinValues; }
    std::vector<std::pair<uint64_t, autil::StringView>> &getKVList() { return _kvList; };
    std::pair<std::vector<uint64_t>, autil::StringView> &getBinKVList() { return _binKVList; };

    bool hasErrorString() const { return _hasErrorString; }

    void appendErrorString(std::string error) {
        _hasErrorString = true;
        _errorString += "\n";
        _errorString += error;
    }

    const std::string getErrorString() const { return _errorString; }

private:
    std::vector<std::pair<uint64_t, autil::StringView>> _kvList;
    std::pair<std::vector<uint64_t>, autil::StringView> _binKVList;
    std::string _errorString;
    bool _hasErrorString = false;
    bool _isBinValues = false;
};

class KVBatchGetGenerator : public multi_call::RequestGenerator {
public:
    using HashKeyVec = std::vector<absl::flat_hash_set<uint64_t>>;
    using KeyVec = std::vector<std::set<std::string>>;
    struct KVBatchGetOptions {
        std::string serviceName;
        std::string tableName;
        std::string indexName;
        std::vector<std::string> attrs;
        KVBatchGetResultType resultType = KVBatchGetResultType::BYTES;
        bool useHashKey = true;
        uint64_t timeout = 60000; // 60000 us
        std::string toString() {
            std::stringstream ss;
            ss << "serviceName: " << serviceName << "\t"
               << "tableName: " << tableName << "\t"
               << "indexName: " << indexName << "\t"
               << "attrs: ";
            for (auto i = 0; i < attrs.size(); i++) {
                ss << attrs[i] << ";";
            }
            ss << "resultType: " << resultType << "\t";
            ss << "useHashKey: " << useHashKey << "\t";
            ss << "timeout: " << timeout << "\t";
            return ss.str();
        }
    };

    KVBatchGetGenerator(const KVBatchGetOptions &options);
    ~KVBatchGetGenerator() = default;

    KVBatchGetGenerator(const KVBatchGetGenerator &) = delete;
    KVBatchGetGenerator &operator=(const KVBatchGetGenerator &) = delete;

public:
    void generate(multi_call::PartIdTy partCnt, multi_call::PartRequestMap &requestMap) override;
    HashKeyVec &getPartIdToHashKeys(multi_call::PartIdTy id) { return _partId2HashKeysMap[id]; };
    void setPartIdToHashKeyMap(std::unordered_map<multi_call::PartIdTy, HashKeyVec> &partId2HashKeysMap) {
        _partId2HashKeysMap = partId2HashKeysMap;
    };
    void setPartIdToKeyMap(std::unordered_map<multi_call::PartIdTy, KeyVec> &partId2KeysMap) {
        _partId2KeysMap = partId2KeysMap;
    };

private:
    std::unordered_map<multi_call::PartIdTy, HashKeyVec> _partId2HashKeysMap;
    std::unordered_map<multi_call::PartIdTy, KeyVec> _partId2KeysMap;
    KVBatchGetOptions _options;
};

} // namespace suez
