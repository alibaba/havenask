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
#ifndef ISEARCH_BS_PROTOJSONIZER_H
#define ISEARCH_BS_PROTOJSONIZER_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "google/protobuf/message.h"

namespace build_service { namespace proto {

class ProtoJsonizer
{
private:
    ProtoJsonizer();
    ~ProtoJsonizer();
    ProtoJsonizer(const ProtoJsonizer&);
    ProtoJsonizer& operator=(const ProtoJsonizer&);

public:
    static bool fromJsonString(const std::string& jsonStr, ::google::protobuf::Message* message);
    static std::string toJsonString(const ::google::protobuf::Message& message);

private:
    static void doFromJsonString(const autil::legacy::json::JsonMap& jsonMap, ::google::protobuf::Message* message);
    static void doToJsonString(const ::google::protobuf::Message& message, autil::legacy::json::JsonMap& jsonMap);
    static void setOneValue(const ::google::protobuf::FieldDescriptor* fieldDesc, const autil::legacy::Any& any,
                            ::google::protobuf::Message* message);
    template <typename T>
    static T parseType(const autil::legacy::Any& any);

private:
    BS_LOG_DECLARE();
};

}}     // namespace build_service::proto
#endif // ISEARCH_BS_PROTOJSONIZER_H
