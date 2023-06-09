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
#include "ha3/worker/HaProtoJsonizer.h"

#include <iosfwd>
#include <typeinfo>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "google/protobuf/message.h"
#include "google/protobuf/stubs/port.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/proto/QrsService.pb.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "suez/turing/proto/Search.pb.h"

using namespace std;
using namespace ::google::protobuf;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace isearch::proto;

namespace isearch {
namespace worker {
AUTIL_LOG_SETUP(ha3, HaProtoJsonizer);

HaProtoJsonizer::HaProtoJsonizer() {
}

HaProtoJsonizer::~HaProtoJsonizer() {
}

bool HaProtoJsonizer::fromJson(const string &jsonStr, Message *message) {
    JsonMap jsonMap;
    try {
        FromJsonString(jsonMap, jsonStr);
        fromJsonMap(jsonMap, message);
    } catch (const ExceptionBase &e) {
        string msgName = string(typeid(*message).name());
        if (msgName == string(typeid(QrsRequest).name())) {
            auto request = dynamic_cast<QrsRequest*>(message);
            if(!request) {
                return false;
            }
            request->set_assemblyquery(jsonStr);
            return true;
        } else if (msgName == string(typeid(suez::turing::HealthCheckRequest).name())) {
            auto request = dynamic_cast<suez::turing::HealthCheckRequest*>(message);
            if(!request) {
                return false;
            }
            request->set_request(jsonStr);
        } else {
            AUTIL_LOG(WARN, "%s", e.what());
            return false;
        }
    }
    return true;
}

string HaProtoJsonizer::toJson(const Message &message) {
    const QrsResponse *qrsResponse = dynamic_cast<const QrsResponse *>(&message);
    const SqlClientResponse *sqlClientResponse =
        dynamic_cast<const SqlClientResponse *>(&message);
    if (qrsResponse) {
        return qrsResponse->assemblyresult();
    } else if (sqlClientResponse) {
        return sqlClientResponse->assemblyresult();
    }

    return http_arpc::ProtoJsonizer::toJson(message);
}

} // namespace worker
} // namespace isearch
