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

#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "google/protobuf/message.h"

namespace http_arpc {

class SimpleProtoJsonizer : public http_arpc::ProtoJsonizer {
public:
    SimpleProtoJsonizer() = default;
    ~SimpleProtoJsonizer() = default;
    SimpleProtoJsonizer(const SimpleProtoJsonizer &) = delete;
    SimpleProtoJsonizer &operator=(const SimpleProtoJsonizer &) = delete;

public:
    bool fromJson(const std::string &jsonStr, ::google::protobuf::Message *message) override;
    std::string toJson(const ::google::protobuf::Message &message) override;
};

} // namespace http_arpc
