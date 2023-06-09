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

#include "autil/HashFunctionBase.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/common/TableDistribution.h"

namespace isearch {
namespace sql {

class MessageConstructor {
public:
    MessageConstructor();
    ~MessageConstructor() = default;
    MessageConstructor(const MessageConstructor &) = delete;
    MessageConstructor &operator=(const MessageConstructor &) = delete;

public:
    static bool constructMessage(const HashMode &hashModel,
                                 const std::string &pkField,
                                 const std::vector<std::map<std::string, std::string>> &hashVals,
                                 const std::string &msgType,
                                 const std::string &traceId,
                                 autil::mem_pool::Pool *pool,
                                 const std::string &inputJsonStr,
                                 std::vector<std::pair<uint16_t, std::string>> &outMessages);

private:
    static bool constructAddMessage(autil::SimpleDocument &modifyExprsDoc,
                                    const HashMode &hashModel,
                                    const std::string &pkField,
                                    const std::string &traceId,
                                    std::vector<std::pair<uint16_t, std::string>> &outMessages);
    static bool
    constructUpdateMessage(autil::SimpleDocument &modifyExprsDoc,
                           const HashMode &hashModel,
                           const std::string &pkField,
                           const std::vector<std::map<std::string, std::string>> &hashVals,
                           const std::string &traceId,
                           std::vector<std::pair<uint16_t, std::string>> &outMessages);
    static bool
    constructDeleteMessage(const HashMode &hashModel,
                           const std::string &pkField,
                           const std::vector<std::map<std::string, std::string>> &hashVals,
                           const std::string &traceId,
                           std::vector<std::pair<uint16_t, std::string>> &outMessages);

    static bool getHashStrVec(autil::SimpleDocument &modifyExprsDoc,
                              const std::vector<std::string> &hashFields,
                              std::vector<std::string> &hashStrVec);
    static bool genAddMessageData(autil::SimpleDocument &modifyExprsDoc,
                                  const std::string &pkField,
                                  const std::string &traceId,
                                  std::string &outMsgData);
    static bool genUpdateMessages(autil::SimpleDocument &modifyExprsDoc,
                                  const std::map<std::string, uint16_t> &pkMaps,
                                  const std::string &pkField,
                                  const std::string &traceId,
                                  std::vector<std::pair<uint16_t, std::string>> &outMessages);
    static bool genDeleteMessages(const std::vector<std::map<std::string, std::string>> &hashVals,
                                  const std::map<std::string, uint16_t> &pkMaps,
                                  const std::string &pkField,
                                  const std::string &traceId,
                                  std::vector<std::pair<uint16_t, std::string>> &outMessages);
    static bool genPayloadMap(const HashMode &hashMode,
                              const std::string &pkField,
                              const std::vector<std::map<std::string, std::string>> &hashVals,
                              std::map<std::string, uint16_t> &payloadMap);
    static autil::HashFunctionBasePtr createHashFunc(const HashMode &hashMode);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MessageConstructor> MessageConstructorPtr;

} // end namespace sql
} // end namespace isearch
