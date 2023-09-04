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
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/protocol/ErrCode.pb.h"

namespace fslib {
namespace fs {
class File;
}
} // namespace fslib

namespace swift {
namespace common {
class FileMessageMeta;
} // namespace common

namespace storage {

class FilePair;

SWIFT_TYPEDEF_PTR(FilePair);
class FilePair {
public:
    FilePair(const std::string &dataFileName,
             const std::string &metaFileName,
             const int64_t beginMessageId,
             const int64_t beginTimestamp,
             bool isAppending = false);

    int64_t getMessageCount();
    FilePairPtr getNext() const;
    void setNext(const FilePairPtr &pair);
    int64_t getEndMessageId();
    protocol::ErrorCode getEndMessageId(int64_t &msgId);
    void setEndMessageId(int64_t msgId);
    bool isAppending() { return _isAppending; }
    void endAppending() { _isAppending = false; }
    int64_t getMetaLength();
    int64_t getDataLength();

private:
    protocol::ErrorCode updateEndMessageId();
    protocol::ErrorCode getMessageCountByMetaLength(int64_t &msgCnt);
    protocol::ErrorCode getMessageCountByMetaContent(int64_t &msgCnt);
    protocol::ErrorCode getMessageMetaByMessageId(int64_t messageId, common::FileMessageMeta &meta);

    static const int64_t DEFAULT_READ_BUF_SIZE = 1024 * 1024; // 1M
    static protocol::ErrorCode
    seekFslibFile(const std::string &fileName, fslib::fs::File *file, int64_t seekPos, int64_t &actualSeekPos);

public:
    const std::string dataFileName;
    const std::string metaFileName;
    const int64_t beginMessageId;
    const int64_t beginTimestamp;

private:
    mutable autil::ThreadMutex _mutex;
    FilePairPtr _next;
    int64_t _endMessageId; // [beginId, endId)
    bool _isAppending;

private:
    AUTIL_LOG_DECLARE();
    friend class FilePairTest;
};

typedef std::vector<FilePairPtr> FilePairVec;

SWIFT_TYPEDEF_PTR(FilePair);

} // namespace storage
} // namespace swift
