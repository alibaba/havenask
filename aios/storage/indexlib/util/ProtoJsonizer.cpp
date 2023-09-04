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
#include "indexlib/util/ProtoJsonizer.h"

#include "google/protobuf/util/json_util.h"

using google::protobuf::util::JsonParseOptions;
using google::protobuf::util::JsonStringToMessage;
using google::protobuf::util::MessageToJsonString;

namespace indexlib::util {
AUTIL_LOG_SETUP(indexlib, ProtoJsonizer);

Status ProtoJsonizer::FromJson(const std::string& jsonStr, ::google::protobuf::Message* message)
{
    JsonParseOptions options;
    options.ignore_unknown_fields = true;
    const auto& status = JsonStringToMessage(jsonStr, message, options);
    if (!status.ok()) {
        return Status::InternalError("failed to parse json[%s] to message[%s] with error[%s]", jsonStr.c_str(),
                                     message->GetTypeName().c_str(), status.ToString().c_str());
    }
    return Status::OK();
}

Status ProtoJsonizer::ToJson(const ::google::protobuf::Message& message, std::string* jsonStr)
{
    const auto& status = MessageToJsonString(message, jsonStr);
    if (!status.ok()) {
        return Status::InternalError("failed to print message[%s] [%s] as json with error[%s]",
                                     message.GetTypeName().c_str(), message.ShortDebugString().c_str(),
                                     status.ToString().c_str());
    }
    return Status::OK();
}

} // namespace indexlib::util
