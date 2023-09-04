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

#include <json/value.h>
#include <string>

#include "aios/network/http_arpc/ProtoJsonizer.h"

namespace google {
namespace protobuf {
class Message;
}
} // namespace google

namespace suez {

class SuezProtoJsonizer : public http_arpc::ProtoJsonizer {
public:
    SuezProtoJsonizer() {}
    ~SuezProtoJsonizer() {}

private:
    SuezProtoJsonizer(const SuezProtoJsonizer &);
    SuezProtoJsonizer &operator=(const SuezProtoJsonizer &);

public:
    std::string toJson(const ::google::protobuf::Message &message) override;

private:
    static Json::Value formatJsonArray(const Json::Value &jsonArray);
    static Json::Value formatJsonMap(const Json::Value &jsonMap);
    static Json::Value formatJson(const Json::Value &json);
};

using SuezProtoJsonizerPtr = std::shared_ptr<SuezProtoJsonizer>;

} // namespace suez
