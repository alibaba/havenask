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

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"

namespace hippo {
struct ProcessInfo;
}; // namespace hippo

namespace swift {
namespace util {

class MultiProcessUtil {
public:
    MultiProcessUtil();
    ~MultiProcessUtil();

public:
    static void generateLogSenderProcess(std::vector<hippo::ProcessInfo> &processInfos, protocol::RoleType roleType);

private:
    static const std::string SWIFT_BROKER_DIR;
    static const std::string SWIFT_ADMIN_DIR;
    static const std::string SWIFT_BROKER_ROLE_NAME_SUFFIX;
    static const std::string SWIFT_ADMIN_ROLE_NAME_SUFFIX;

    static const std::string LOG_SENDER_CMD;
    static const std::string DEFAULT_SEPARATOR;

    static const std::string ENV_SEPARATOR;
    static const std::string ENV_SWIFT_TOPIC_PREFIX;
    static const std::string ENV_SWIFT_LOG_SUFFIX;
    static const std::string ENV_SWIFT_LOG_DIR_SUFFIX;
    // log sender env
    static const std::string ENV_TOPIC_NAME;
    static const std::string ENV_STREAM_ACCESS_LOG_PATH;
    static const std::string ENV_ACCESS_LOG_DIR;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace util
} // namespace swift
