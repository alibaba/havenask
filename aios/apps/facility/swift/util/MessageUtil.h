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
#include <vector>

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace autil {
class ZlibCompressor;
} // namespace autil
namespace swift {
namespace common {
class MessageInfo;
class RangeUtil;
struct MergedMessageMeta;
} // namespace common
namespace filter {
class FieldFilter;
class MessageFilter;
} // namespace filter
namespace protocol {
class Filter;
class Message;
class PartitionId;
} // namespace protocol
} // namespace swift

namespace swift {
namespace util {

class MessageUtil {
public:
    MessageUtil();
    ~MessageUtil();

private:
    MessageUtil(const MessageUtil &);
    MessageUtil &operator=(const MessageUtil &);

public:
    static bool unpackMessage(autil::ZlibCompressor *compressor,
                              const char *allData,
                              size_t allSize,
                              int64_t msgid,
                              int64_t timestamp,
                              const swift::filter::MessageFilter *metaFilter,
                              swift::filter::FieldFilter *fieldFilter,
                              std::vector<swift::protocol::Message> &msgVec,
                              int32_t &totalCount);
    static bool unpackMessage(const common::MessageInfo &inMsg, std::vector<common::MessageInfo> &outMsgs);

private:
    static bool unpackMessage(int64_t msgid,
                              int64_t timestamp,
                              const common::MergedMessageMeta *metaVec,
                              uint16_t msgCount,
                              const char *dataPart,
                              size_t dataLen,
                              const filter::MessageFilter *metaFilter,
                              filter::FieldFilter *fieldFilter,
                              std::vector<swift::protocol::Message> &msgVec);

public:
    static void mergeMessage(const std::vector<swift::protocol::Message> &mmv,
                             common::RangeUtil *rangeUtil,
                             uint32_t mergeCount,
                             std::vector<swift::protocol::Message> &mergedVec); // for test
private:
    static void doMergeMessage(uint16_t hashId,
                               const std::vector<swift::protocol::Message> &msgVec,
                               swift::protocol::Message &msgInfo);

public:
    static void writeDataHead(common::MessageInfo &msgInfo);
    static bool readDataHead(protocol::Message &msg);

public:
    static bool needFilter(const protocol::PartitionId &partId, const protocol::Filter &filter);

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MessageUtil);

} // namespace util
} // namespace swift
