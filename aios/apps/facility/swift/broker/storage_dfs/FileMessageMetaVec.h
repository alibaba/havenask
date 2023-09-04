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

#include "autil/Log.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/Block.h"
#include "swift/util/ReaderRec.h"

namespace swift {
namespace common {
class FileMessageMeta;
} // namespace common
namespace monitor {
struct ReadMetricsCollector;
} // namespace monitor
namespace storage {
class FileCache;
} // namespace storage
namespace util {
class TimeoutChecker;
} // namespace util
} // namespace swift

namespace swift {
namespace storage {
// only contain one block meta data
class FileMessageMetaVec {
public:
    FileMessageMetaVec();
    ~FileMessageMetaVec();

private:
    FileMessageMetaVec(const FileMessageMetaVec &);
    FileMessageMetaVec &operator=(const FileMessageMetaVec &);

public:
    protocol::ErrorCode init(FileCache *fileCache,
                             const std::string &fileName,
                             bool isAppending,
                             int64_t begPos,
                             int64_t endPos,
                             util::TimeoutChecker *timeoutChecker,
                             util::ReaderInfo *readerInfo = NULL,
                             const util::ReaderInfoMap *readerInfoMap = NULL,
                             monitor::ReadMetricsCollector *collector = NULL,
                             bool useLen = true);
    bool fillMeta(int64_t index, common::FileMessageMeta &meta) const;
    int64_t getMsgLen(int64_t index) const;
    int64_t getMsgBegin(int64_t index) const;
    int64_t start() const { return _start; }
    int64_t end() const { return _end; }
    void getReadInfo(int64_t &dfsReadSize, int64_t &fromCacheSize) const;

private:
    util::BlockPtr _metaBlock;
    common::FileMessageMeta *_fileMetaVec;
    int64_t _start;
    int64_t _end;
    int64_t _firstMsgDataStart;
    int64_t _dfsReadSize;
    int64_t _fromCacheSize;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace storage
} // namespace swift
