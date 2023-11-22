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

#include <string>

#include "autil/Log.h"
#include "google/protobuf/message.h"
#include "indexlib/base/Status.h"

namespace indexlib::util {
class ProtoJsonizer
{
public:
    static indexlib::Status FromJson(const std::string& jsonStr, ::google::protobuf::Message* message);
    static indexlib::Status ToJson(const ::google::protobuf::Message& message, std::string* jsonStr);

private:
    AUTIL_LOG_DECLARE();
};
} // namespace indexlib::util
