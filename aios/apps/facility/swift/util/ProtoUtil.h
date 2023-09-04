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

namespace google {
namespace protobuf {
class Message;
} // namespace protobuf
} // namespace google

namespace swift {
namespace protocol {
class AllTopicInfoResponse;
class DispatchInfo;
class HeartbeatInfo;
class TaskInfo;
} // namespace protocol
} // namespace swift

namespace swift {
namespace util {

class ProtoUtil {
public:
    ProtoUtil();
    ~ProtoUtil();

private:
    ProtoUtil(const ProtoUtil &) = delete;
    ProtoUtil &operator=(const ProtoUtil &) = delete;

public:
    static std::string plainDiffStr(const google::protobuf::Message *msg1, const google::protobuf::Message *msg2);
    static std::string getHeartbeatStr(const protocol::HeartbeatInfo &msg);
    static std::string getDispatchStr(const protocol::DispatchInfo &msg);
    static std::string getTaskInfoStr(const protocol::TaskInfo &ti);
    static std::string getAllTopicInfoStr(const protocol::AllTopicInfoResponse &allTopicInfo);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace util
} // namespace swift
