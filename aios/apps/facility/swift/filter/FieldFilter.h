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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/common/FieldGroupReader.h"
#include "swift/common/FieldGroupWriter.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace filter {
class DescFieldFilter;
class EliminateFieldFilter;
} // namespace filter
namespace protocol {
class ConsumptionRequest;
class MessageResponse;
} // namespace protocol
} // namespace swift

namespace swift {
namespace filter {

class FieldFilter {
public:
    FieldFilter();
    ~FieldFilter();

private:
    FieldFilter(const FieldFilter &);
    FieldFilter &operator=(const FieldFilter &);

public:
    protocol::ErrorCode init(const protocol::ConsumptionRequest *request);
    protocol::ErrorCode init(const std::vector<std::string> &requiredFileds, const std::string &filterDesc);

    void filter(protocol::MessageResponse *response,
                int64_t &totalSize,
                int64_t &rawTotalSize,
                int64_t &readMsgCount,
                int64_t &updateMsgCount);
    bool filter(const std::string &inputMsg, std::string &outputMsg);
    bool filter(const char *inputMsg, size_t len, std::string &outputMsg);
    void setTopicName(const std::string &topicName);
    void setPartId(uint32_t id);

private:
    void filterPBMessage(protocol::MessageResponse *response,
                         int64_t &totalSize,
                         int64_t &rawTotalSize,
                         int64_t &readMsgCount,
                         int64_t &updateMsgCount);
    void filterFBMessage(protocol::MessageResponse *response,
                         int64_t &totalSize,
                         int64_t &rawTotalSize,
                         int64_t &readMsgCount,
                         int64_t &updateMsgCount);

private:
    EliminateFieldFilter *_elimFilter;
    DescFieldFilter *_descFilter;
    common::FieldGroupReader _fieldGroupReader;
    common::FieldGroupWriter _fieldGroupWriter;
    std::string _topicName;
    uint32_t _partId;

private:
    friend class FieldFilterTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FieldFilter);

} // namespace filter
} // namespace swift
