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

#include <map>
#include <stdint.h>
#include <string>

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/proto/JsonizableProtobuf.h"
#include "build_service/util/Log.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"

namespace build_service { namespace config {

class SwiftTopicConfig : public proto::JsonizableProtobuf<swift::protocol::TopicCreationRequest>
{
public:
    SwiftTopicConfig();
    ~SwiftTopicConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate(bool allowMemoryPrefer = false) const;
    std::string getTopicMode() const;
    uint32_t getPartitionCount() const;
    bool hasPartitionRestrict() const;
    static constexpr uint64_t FULL_TOPIC_SRC_SIGNATURE = (uint64_t)1 << 63;
    static constexpr uint64_t INC_TOPIC_SRC_SIGNATURE = 1 + ((uint64_t)1 << 63);

private:
    static std::string topicModeToStr(swift::protocol::TopicMode topicMode);
    static swift::protocol::TopicMode strTotopicMode(const std::string& str);

private:
    static const int64_t DEFAULT_NO_MORE_MSG_PERIOD;
    static const int64_t DEFAULT_MAX_COMMIT_INTERVAL;
    static const double DEFAULT_BULDER_MEM_TO_PROCESSOR_BUFFER_FACTOR;

public:
    static const std::string TOPIC_MODE_SECURITY_STR;
    static const std::string TOPIC_MODE_MEMORY_ONLY_STR;
    static const std::string TOPIC_MODE_MEMORY_PREFER_STR;
    static const std::string TOPIC_MODE_NORMAL_STR;
    static const std::string TOPIC_MODE_PERSIST_DATA_STR;

public:
    // only for tuning memory prefer topic config,
    // do not intend to open for common users.
    int64_t noMoreMsgPeriod;
    int64_t maxCommitInterval;
    std::map<std::string, uint64_t> writerMaxBufferSize;
    double builderMemToProcessorBufferFactor;
    bool enablePartitionRestrict;
    std::string readerConfigStr;
    std::string writerConfigStr;
    std::string swiftClientConfigStr;
    std::string swiftClientShareMode;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftTopicConfig);
}} // namespace build_service::config
