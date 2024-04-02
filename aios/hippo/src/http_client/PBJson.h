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
#ifndef HIPPO_PBJSON_H
#define HIPPO_PBJSON_H

#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "rapidjson/document.h"
#include <string>
#include "util/Log.h"
#include "common/common.h"

BEGIN_HIPPO_NAMESPACE(http_client);

#define ERR_INVALID_ARG -1
#define ERR_INVALID_PB -2
#define ERR_UNKNOWN_FIELD -3
#define ERR_INVALID_JSON -4

class PBJson
{
public:
    PBJson();
    ~PBJson();
private:
    PBJson(const PBJson &);
    PBJson& operator=(const PBJson &);
public:
    static int json2pb(const std::string& json,
                       google::protobuf::Message* msg,
                       std::string& err);

    static void pb2json(const google::protobuf::Message* msg,
                        std::string& str);

private:
    static int jsonobject2pb(const rapidjson::Value* json,
                             google::protobuf::Message* msg,
                             std::string& err);

    static int parse_json(const rapidjson::Value* json,
                          google::protobuf::Message* msg,
                          std::string& err);

    static rapidjson::Value *parse_msg(
            const google::protobuf::Message *msg,
            rapidjson::Value::AllocatorType& allocator);

    static int json2field(const rapidjson::Value* json,
                          google::protobuf::Message* msg,
                          const google::protobuf::FieldDescriptor *field,
                          std::string& err);

    static rapidjson::Value* field2json(
            const google::protobuf::Message *msg,
            const google::protobuf::FieldDescriptor *field,
            rapidjson::Value::AllocatorType& allocator);

    static void json2string(const rapidjson::Value* json, std::string& str);

    inline static std::string hex2bin(const std::string &s);

    inline static std::string bin2hex(const std::string &s);

    inline static std::string b64_encode(const std::string &s);

    inline static std::string b64_decode(const std::string &s);

private:
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(PBJson);

END_HIPPO_NAMESPACE(http_client);

#endif //HIPPO_PBJSON_H
