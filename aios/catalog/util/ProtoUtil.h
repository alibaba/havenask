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
#include "google/protobuf/util/message_differencer.h"

namespace catalog {

class ProtoUtil {
public:
    ProtoUtil() = delete;
    ~ProtoUtil() = delete;

public:
    static inline bool compareProto(const google::protobuf::Message &expected,
                                    const google::protobuf::Message &actual,
                                    std::string *message = nullptr) {
        return compareProto(expected, actual, false, message);
    }
    static bool compareProto(const google::protobuf::Message &expected,
                             const google::protobuf::Message &actual,
                             bool strict,
                             std::string *message = nullptr);

    using DiffCustomizer = std::function<void(google::protobuf::util::MessageDifferencer &)>;
    static bool compareProto(const google::protobuf::Message &expected,
                             const google::protobuf::Message &actual,
                             DiffCustomizer customizer);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace catalog
