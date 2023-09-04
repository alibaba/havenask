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
#include "suez/admin/SuezProtoJsonizer.h"

#include <cstddef>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/port.h>
#include <json/config.h>
#include <json/reader.h>
#include <json/writer.h>
#include <memory>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "suez/admin/Admin.pb.h"

using namespace std;
using namespace http_arpc;
using namespace ::google::protobuf;
using namespace autil::legacy;
using namespace autil::legacy::json;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SuezProtoJsonizer);

namespace suez {

Json::Value SuezProtoJsonizer::formatJsonArray(const Json::Value &jsonArray) {
    Json::Value ret(Json::arrayValue);
    for (auto it = jsonArray.begin(); it != jsonArray.end(); ++it) {
        ret.append(formatJson(*it));
    }
    return ret;
}

Json::Value SuezProtoJsonizer::formatJsonMap(const Json::Value &jsonMap) {
    Json::Value ret(Json::objectValue);
    for (auto it = jsonMap.begin(); it != jsonMap.end(); ++it) {
        const auto &key = it.name();
        ret[key] = formatJson(*it);
    }
    return ret;
}

Json::Value SuezProtoJsonizer::formatJson(const Json::Value &json) {
    if (json.isString()) {
        Json::Value newValue;
        std::unique_ptr<Json::CharReader> reader(Json::CharReaderBuilder().newCharReader());
        string jsonStr = json.asString();
        JSONCPP_STRING errs;
        if (!reader->parse(jsonStr.data(), jsonStr.data() + jsonStr.size(), &newValue, &errs)) {
            return json;
        }
        if (newValue.isObject() || newValue.isArray()) {
            return formatJson(newValue);
        } else {
            return json;
        }
    } else if (json.isObject()) {
        return formatJsonMap(json);
    } else if (json.isArray()) {
        return formatJsonArray(json);
    } else {
        return json;
    }
}

string SuezProtoJsonizer::toJson(const Message &message) {
    const ReadResponse *readResponse = dynamic_cast<const ReadResponse *>(&message);
    if (readResponse != NULL && readResponse->format()) {
        try {
            string before = toJsonString(message);
            Json::Value status;
            std::unique_ptr<Json::CharReader> reader(Json::CharReaderBuilder().newCharReader());
            JSONCPP_STRING errs;
            if (!reader->parse(before.data(), before.data() + before.size(), &status, &errs)) {
                return before;
            }
            Json::Value formatedJson = formatJson(status);
            formatedJson.removeMember("format");
            return Json::writeString(Json::StreamWriterBuilder(), formatedJson);
        } catch (...) { AUTIL_LOG(ERROR, "jsoncpp parse failed, maybe stack overflow"); }
    }
    return ProtoJsonizer::toJson(message);
}

} // namespace suez
