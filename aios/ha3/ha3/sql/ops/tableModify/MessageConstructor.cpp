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
#include "ha3/sql/ops/tableModify/MessageConstructor.h"

#include "autil/HashFuncFactory.h"
#include "autil/legacy/RapidJsonHelper.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "swift/common/FieldGroupWriter.h"

using namespace std;
using namespace autil;
using namespace swift::common;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, MessageConstructor);

MessageConstructor::MessageConstructor() {}

bool MessageConstructor::constructMessage(const HashMode &hashMode,
                                          const string &pkField,
                                          const vector<map<string, string>> &hashVals,
                                          const string &msgType,
                                          const string &traceId,
                                          autil::mem_pool::Pool *pool,
                                          const string &inputJsonStr,
                                          vector<pair<uint16_t, string>> &outMessages) {
    AutilPoolAllocator allocator(pool);
    SimpleDocument modifyExprsDoc(&allocator);
    if (!inputJsonStr.empty()) {
        modifyExprsDoc.Parse(inputJsonStr.c_str());
        if (modifyExprsDoc.HasParseError()) {
            SQL_LOG(ERROR, "parse modify exprs error, jsonStr [%s]", inputJsonStr.c_str());
            return false;
        }
        if (!modifyExprsDoc.IsObject()) {
            SQL_LOG(ERROR, "parsed result is not object, jsonStr [%s]", inputJsonStr.c_str());
            return false;
        }
    }
    bool ret = false;
    if (msgType == TABLE_OPERATION_UPDATE) {
        ret = constructUpdateMessage(
            modifyExprsDoc, hashMode, pkField, hashVals, traceId, outMessages);
    } else if (msgType == TABLE_OPERATION_INSERT) {
        ret = constructAddMessage(modifyExprsDoc, hashMode, pkField, traceId, outMessages);
    } else if (msgType == TABLE_OPERATION_DELETE) {
        ret = constructDeleteMessage(hashMode, pkField, hashVals, traceId, outMessages);
    } else {
        SQL_LOG(ERROR, "not supported msg type [%s]", msgType.c_str());
        return false;
    }
    if (!ret) {
        SQL_LOG(ERROR, "construct swift message failed, input jsonStr [%s]", inputJsonStr.c_str());
    } else {
        for (const auto &outMessage : outMessages) {
            SQL_LOG(TRACE3,
                    "parse message info success, payload[%u], data[%s]",
                    outMessage.first,
                    outMessage.second.c_str());
        }
    }
    return ret;
}

bool MessageConstructor::constructAddMessage(SimpleDocument &modifyExprsDoc,
                                             const HashMode &hashMode,
                                             const string &pkField,
                                             const string &traceId,
                                             vector<pair<uint16_t, string>> &outMessages) {
    vector<string> hashStrVec;
    if (!getHashStrVec(modifyExprsDoc, hashMode._hashFields, hashStrVec)) {
        SQL_LOG(ERROR,
                "get hash string failed, json doc is [%s]",
                RapidJsonHelper::SimpleValue2Str(modifyExprsDoc).c_str());
        return false;
    }
    autil::HashFunctionBasePtr hashFunc = createHashFunc(hashMode);
    if (!hashFunc) {
        return false;
    }
    uint16_t payload = (uint16_t)hashFunc->getHashId(hashStrVec);
    string msgData;
    if (!genAddMessageData(modifyExprsDoc, pkField, traceId, msgData)) {
        SQL_LOG(ERROR,
                "gen add message data failed, json doc is [%s]",
                RapidJsonHelper::SimpleValue2Str(modifyExprsDoc).c_str());
        return false;
    }
    outMessages.emplace_back(make_pair(payload, msgData));
    return true;
}

bool MessageConstructor::constructUpdateMessage(SimpleDocument &modifyExprsDoc,
                                                const HashMode &hashMode,
                                                const string &pkField,
                                                const vector<map<string, string>> &hashVals,
                                                const string &traceId,
                                                vector<pair<uint16_t, string>> &outMessages) {
    map<string, uint16_t> payloadMap;
    if (!genPayloadMap(hashMode, pkField, hashVals, payloadMap)) {
        return false;
    }
    if (!genUpdateMessages(modifyExprsDoc, payloadMap, pkField, traceId, outMessages)) {
        SQL_LOG(ERROR,
                "gen update message failed, json doc is [%s]",
                RapidJsonHelper::SimpleValue2Str(modifyExprsDoc).c_str());
        return false;
    }
    return true;
}

bool MessageConstructor::constructDeleteMessage(const HashMode &hashMode,
                                                const string &pkField,
                                                const vector<map<string, string>> &hashVals,
                                                const string &traceId,
                                                vector<pair<uint16_t, string>> &outMessages) {
    map<string, uint16_t> payloadMap;
    if (!genPayloadMap(hashMode, pkField, hashVals, payloadMap)) {
        return false;
    }
    if (!genDeleteMessages(hashVals, payloadMap, pkField, traceId, outMessages)) {
        SQL_LOG(ERROR, "gen delete message failed.");
        return false;
    }
    return true;
}

bool MessageConstructor::genPayloadMap(const HashMode &hashMode,
                                       const string &pkField,
                                       const vector<map<string, string>> &hashVals,
                                       map<string, uint16_t> &payloadMap) {
    if (hashVals.empty()) {
        SQL_LOG(WARN, "hash values map is empty.");
        return false;
    }
    autil::HashFunctionBasePtr hashFunc = createHashFunc(hashMode);
    if (!hashFunc) {
        return false;
    }
    vector<string> hashStrVec;
    for (const auto &hashValMap : hashVals) {
        hashStrVec.clear();
        for (auto hashField : hashMode._hashFields) {
            auto iter = hashValMap.find(hashField);
            if (iter != hashValMap.end() && !iter->second.empty()) {
                hashStrVec.push_back(iter->second);
            } else {
                SQL_LOG(WARN, "hash field [%s] not found or is empty.", hashField.c_str());
                return false;
            }
        }
        auto iter = hashValMap.find(pkField);
        if (iter != hashValMap.end() && !iter->second.empty()) {
            payloadMap[iter->second] = (uint16_t)hashFunc->getHashId(hashStrVec);
        } else {
            SQL_LOG(WARN, "pk field [%s] not found or is empty.", pkField.c_str());
            return false;
        }
    }
    return true;
}

autil::HashFunctionBasePtr MessageConstructor::createHashFunc(const HashMode &hashMode) {
    if (!hashMode.validate()) {
        SQL_LOG(WARN, "validate hash mode failed.");
        return HashFunctionBasePtr();
    }
    string hashFunc = hashMode._hashFunction;
    StringUtil::toUpperCase(hashFunc);
    autil::HashFunctionBasePtr hashFunctionBasePtr
        = HashFuncFactory::createHashFunc(hashFunc, hashMode._hashParams);
    if (!hashFunctionBasePtr) {
        SQL_LOG(WARN, "invalid hash function [%s].", hashFunc.c_str());
        return HashFunctionBasePtr();
    }
    return hashFunctionBasePtr;
}

bool MessageConstructor::getHashStrVec(SimpleDocument &modifyExprsDoc,
                                       const vector<string> &hashFields,
                                       vector<string> &hashStrVec) {
    for (const auto &field : hashFields) {
        std::string hashField = SQL_COLUMN_PREFIX + field;
        if (!modifyExprsDoc.HasMember(hashField)) {
            SQL_LOG(ERROR, "hash field [%s] not exist.", hashField.c_str());
            return false;
        }
        if (modifyExprsDoc[hashField].IsString()) {
            hashStrVec.push_back(modifyExprsDoc[hashField].GetString());
        } else if (modifyExprsDoc[hashField].IsNumber()) {
            hashStrVec.push_back(RapidJsonHelper::SimpleValue2Str(modifyExprsDoc[hashField]));
        } else {
            SQL_LOG(ERROR,
                    "parse hash value failed. field [%s], value [%s]",
                    hashField.c_str(),
                    RapidJsonHelper::SimpleValue2Str(modifyExprsDoc[hashField]).c_str());
            return false;
        }
    }
    return true;
}

bool MessageConstructor::genAddMessageData(SimpleDocument &modifyExprsDoc,
                                           const string &pkField,
                                           const string &traceId,
                                           string &outMsgData) {
    FieldGroupWriter msgWriter;
    msgWriter.addProductionField(MESSAGE_KEY_CMD, MESSAGE_CMD_ADD, true);
    bool hasPk = false;
    bool hasDmlRequestIdField = false;
    for (auto it = modifyExprsDoc.MemberBegin(); it != modifyExprsDoc.MemberEnd(); ++it) {
        if (!it->name.IsString()) {
            SQL_LOG(ERROR,
                    "key type is not string, key[%s] value[%s]",
                    RapidJsonHelper::SimpleValue2Str(it->name).c_str(),
                    RapidJsonHelper::SimpleValue2Str(it->value).c_str());
            return false;
        }
        string fieldName = it->name.GetString();
        KernelUtil::stripName(fieldName);
        string fieldValue;
        if (it->value.IsString()) {
            fieldValue = it->value.GetString();
        } else if (it->value.IsNumber()) {
            fieldValue = RapidJsonHelper::SimpleValue2Str(it->value);
        } else {
            SQL_LOG(ERROR,
                    "value type is not string or int , key[%s] value[%s]",
                    RapidJsonHelper::SimpleValue2Str(it->name).c_str(),
                    RapidJsonHelper::SimpleValue2Str(it->value).c_str());
            return false;
        }
        if (fieldName == pkField) {
            hasPk = true;
            if (fieldValue.empty()) {
                SQL_LOG(WARN, "pk field [%s] is empty.", fieldName.c_str());
                return false;
            }
        }
        if (fieldName == DML_REQUEST_ID_FIELD_NAME) {
            if (fieldValue.empty()) {
                fieldValue = traceId;
            }
            hasDmlRequestIdField = true;
        }
        msgWriter.addProductionField(fieldName, fieldValue, true);
    }
    if (!hasPk) {
        SQL_LOG(WARN, "not pk field [%s] found.", pkField.c_str());
        return false;
    }
    if (!hasDmlRequestIdField && !traceId.empty()) {
        msgWriter.addProductionField(DML_REQUEST_ID_FIELD_NAME, traceId, true);
    }
    outMsgData = msgWriter.toString();
    return true;
}

bool MessageConstructor::genUpdateMessages(SimpleDocument &modifyExprsDoc,
                                           const map<string, uint16_t> &pkMaps,
                                           const string &pkField,
                                           const string &traceId,
                                           vector<pair<uint16_t, string>> &outMessages) {
    for (const auto &iter : pkMaps) {
        FieldGroupWriter msgWriter;
        msgWriter.addProductionField(MESSAGE_KEY_CMD, MESSAGE_CMD_UPDATE, true);
        msgWriter.addProductionField(pkField, iter.first, true);
        bool hasDmlRequestIdField = false;
        for (auto it = modifyExprsDoc.MemberBegin(); it != modifyExprsDoc.MemberEnd(); ++it) {
            if (!it->name.IsString()) {
                SQL_LOG(ERROR,
                        "key type is not string, key[%s] value[%s]",
                        RapidJsonHelper::SimpleValue2Str(it->name).c_str(),
                        RapidJsonHelper::SimpleValue2Str(it->value).c_str());
                return false;
            }
            string fieldName = it->name.GetString();
            KernelUtil::stripName(fieldName);
            string fieldValue;
            if (it->value.IsString()) {
                fieldValue = it->value.GetString();
            } else if (it->value.IsNumber()) {
                fieldValue = RapidJsonHelper::SimpleValue2Str(it->value);
            } else {
                SQL_LOG(ERROR,
                        "value type is not string or int , key[%s] value[%s]",
                        RapidJsonHelper::SimpleValue2Str(it->name).c_str(),
                        RapidJsonHelper::SimpleValue2Str(it->value).c_str());
                return false;
            }
            if (fieldName == DML_REQUEST_ID_FIELD_NAME) {
                if (fieldValue.empty()) {
                    fieldValue = traceId;
                }
                hasDmlRequestIdField = true;
            }
            msgWriter.addProductionField(fieldName, fieldValue, true);
        }
        if (!hasDmlRequestIdField &&!traceId.empty()) {
            msgWriter.addProductionField(DML_REQUEST_ID_FIELD_NAME, traceId, true);
        }
        outMessages.emplace_back(make_pair(iter.second, msgWriter.toString()));
    }
    return true;
}

bool MessageConstructor::genDeleteMessages(const vector<map<string, string>> &hashVals,
                                           const map<string, uint16_t> &pkMaps,
                                           const string &pkField,
                                           const string &traceId,
                                           vector<pair<uint16_t, string>> &outMessages) {
    for (const auto &hashValMap : hashVals) {
        FieldGroupWriter msgWriter;
        msgWriter.addProductionField(MESSAGE_KEY_CMD, MESSAGE_CMD_DELETE, true);
        if (!traceId.empty()) {
            msgWriter.addProductionField(DML_REQUEST_ID_FIELD_NAME, traceId, true);
        }
        for (const auto &pair : hashValMap) {
            msgWriter.addProductionField(pair.first, pair.second, true);
        }
        auto iter = hashValMap.find(pkField);
        if (iter != hashValMap.end() && !iter->second.empty()) {
            auto hashIdIter = pkMaps.find(iter->second);
            if (hashIdIter != pkMaps.end()) {
                outMessages.emplace_back(make_pair(hashIdIter->second, msgWriter.toString()));
            } else {
                SQL_LOG(ERROR,
                        "not find pk [%s] in pkMaps [%s]",
                        iter->second.c_str(),
                        StringUtil::toString(pkMaps).c_str());
                return false;
            }
        } else {
            SQL_LOG(ERROR, "invalid pk field [%s]", StringUtil::toString(hashValMap).c_str());
            return false;
        }
    }
    return true;
}

} // end namespace sql
} // end namespace isearch
