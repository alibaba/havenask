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
#include "sql/rpc/SqlProtoJsonizer.h"

#include <google/protobuf/message.h>
#include <typeinfo>

#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/proto/QrsService.pb.h"

namespace sql {

SqlProtoJsonizer::SqlProtoJsonizer() {}

SqlProtoJsonizer::~SqlProtoJsonizer() {}

bool SqlProtoJsonizer::fromJson(const std::string &jsonStr, google::protobuf::Message *message) {
    try {
        autil::legacy::json::JsonMap jsonMap;
        autil::legacy::FastFromJsonString(jsonMap, jsonStr);
        fromJsonMap(jsonMap, message);
    } catch (const autil::legacy::ExceptionBase &e) {
        auto request = dynamic_cast<isearch::proto::QrsRequest *>(message);
        if (!request) {
            return false;
        }
        request->set_assemblyquery(jsonStr);
    }
    return true;
}

std::string SqlProtoJsonizer::toJson(const google::protobuf::Message &message) {
    auto qrsResponse = dynamic_cast<const isearch::proto::QrsResponse *>(&message);
    if (!qrsResponse) {
        auto sqlClientResponse = dynamic_cast<const isearch::proto::SqlClientResponse *>(&message);
        if (sqlClientResponse) {
            return sqlClientResponse->assemblyresult();
        } else {
            return std::string("error response type: ") + std::string(typeid(message).name())
                   + ", message: " + http_arpc::ProtoJsonizer::toJson(message);
        }
    } else {
        return qrsResponse->assemblyresult();
    }
}

} // namespace sql
