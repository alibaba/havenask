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

#include <limits>
#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace protocol {
class Filter;
} // namespace protocol

namespace filter {
class MessageFilter;
}

namespace storage {

struct MetaFileInfo {
    MetaFileInfo(const std::string &fileName) : name(fileName), timestamp(0), beginMsgId(-1), endMsgId(-1) {}
    MetaFileInfo(const std::string &fileName, int64_t ts, int64_t bMsgId, int64_t eMsgId)
        : name(fileName), timestamp(ts), beginMsgId(bMsgId), endMsgId(eMsgId) {}
    std::string name;
    int64_t timestamp;
    int64_t beginMsgId;
    int64_t endMsgId;
};

struct MessageMeta {
    MessageMeta(int64_t mId, int64_t ts, uint16_t pl, uint8_t m, bool merge)
        : msgId(mId), timestamp(ts), payload(pl), mask(m), isMerged(merge) {}

    int64_t msgId;
    int64_t timestamp;
    uint16_t payload;
    uint8_t mask;
    bool isMerged;
};

inline bool operator<(const MessageMeta &lhs, const MessageMeta &rhs) {
    if (lhs.payload < rhs.payload) {
        return true;
    } else if (lhs.payload == rhs.payload) {
        if (lhs.timestamp < rhs.timestamp) {
            return true;
        } else if (lhs.timestamp == rhs.timestamp) {
            return lhs.mask < rhs.mask;
        } else {
            return false;
        }
    } else {
        return false;
    }
}

inline bool operator==(const MessageMeta &lhs, const MessageMeta &rhs) {
    return lhs.msgId == rhs.msgId && lhs.timestamp == rhs.timestamp && lhs.payload == rhs.payload &&
           lhs.mask == rhs.mask && lhs.isMerged == rhs.isMerged;
}

inline bool operator!=(const MessageMeta &lhs, const MessageMeta &rhs) { return !operator==(lhs, rhs); }

class FsMessageMetaReader {
public:
    FsMessageMetaReader(const std::string &dfsRoot, const std::string &topic, const protocol::Filter *filter);
    ~FsMessageMetaReader();

    bool readMeta(std::vector<MessageMeta> &outMetaVec,
                  int64_t beginTimestamp = 0,
                  int64_t endTimestamp = std::numeric_limits<int64_t>::max());

private:
    bool initMetaFileLst(const std::string &dfsRoot, const std::string &topic, const protocol::Filter *filter);
    bool getPartMetaFileList(const std::string &partPath, std::vector<MetaFileInfo> &metaLst);
    bool findMetaFileIndex(const std::vector<MetaFileInfo> &metaFileInfo,
                           uint64_t beginTimestamp,
                           uint64_t endTimestamp,
                           size_t &beginPos,
                           size_t &endPos);
    bool fillMeta(const std::string &readStr,
                  const MetaFileInfo &info,
                  const std::string &metaPath,
                  int64_t beginTimestamp,
                  int64_t endTimestamp,
                  std::vector<MessageMeta> &outMetaVec);
    bool extractMessageIdAndTimestamp(const std::string &prefix, int64_t &msgId, int64_t &timestamp);

private:
    FsMessageMetaReader(const FsMessageMetaReader &);
    FsMessageMetaReader &operator=(const FsMessageMetaReader &);
    AUTIL_LOG_DECLARE();

private:
    std::string _dataPath;
    std::map<uint32_t, std::vector<MetaFileInfo>> _metaFileMap;
    filter::MessageFilter *_filter = nullptr;
};

SWIFT_TYPEDEF_PTR(FsMessageMetaReader);

} // namespace storage
} // namespace swift
