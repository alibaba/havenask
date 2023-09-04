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

#include <stdint.h>
#include <string>
#include <unordered_map>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/ZkDataAccessor.h"

namespace swift {
namespace protocol {
class TopicWriterVersionInfo;
} // namespace protocol
namespace common {

class SingleTopicWriterController {
private:
    SingleTopicWriterController(const SingleTopicWriterController &);
    SingleTopicWriterController &operator=(const SingleTopicWriterController &);

public:
    SingleTopicWriterController(util::ZkDataAccessorPtr zkDataAccessor,
                                const std::string &zkRoot,
                                const std::string &topicName);
    ~SingleTopicWriterController();

    bool init();
    protocol::ErrorInfo updateWriterVersion(const protocol::TopicWriterVersionInfo &topicWriterVersionInfo);
    bool validateThenUpdate(const std::string &name, uint32_t majorVersion, uint32_t minorVersion);
    std::string getDebugVersionInfo(const std::string &writerName);

private:
    protocol::ErrorCode updateVersionFile(uint32_t majorVersion,
                                          const std::unordered_map<std::string, uint32_t> &writerVersion);
    std::string getWriterVersionStr(uint32_t majorVersion,
                                    const std::unordered_map<std::string, uint32_t> &writerVersion);

private:
    autil::ThreadMutex _lock;
    util::ZkDataAccessorPtr _zkDataAccessor;
    std::string _path;
    uint32_t _majorVersion = 0;
    std::unordered_map<std::string, uint32_t> _writerVersion;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SingleTopicWriterController);

} // namespace common
} // namespace swift
