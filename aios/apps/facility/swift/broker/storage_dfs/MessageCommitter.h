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

#include <atomic>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "swift/common/Common.h"
#include "swift/common/FilePair.h"
#include "swift/common/MemoryMessage.h"
#include "swift/protocol/Common.pb.h" // IWYU pragma: keep
#include "swift/protocol/ErrCode.pb.h"

namespace fslib {
namespace fs {
class File;
} // namespace fs
} // namespace fslib
namespace swift {
namespace config {
class PartitionConfig;
} // namespace config
namespace monitor {
class BrokerMetricsReporter;
} // namespace monitor
namespace storage {
class FileManager;
} // namespace storage
} // namespace swift

namespace swift {
namespace protocol {
class Message;

namespace flat {
struct Messages;
} // namespace flat

} // namespace protocol

namespace storage {

class MessageCommitter {
public:
    MessageCommitter(const protocol::PartitionId &partitionId,
                     const config::PartitionConfig &config,
                     FileManager *fileManager,
                     monitor::BrokerMetricsReporter *metricsReporter,
                     bool enableFastRecover = false);
    ~MessageCommitter();

public:
    int64_t getWritedId() const { return _writedId; }
    int64_t getCommittedId() const { return _committedId; }
    void updateCommittedId(int64_t newCommittedId);
    void setCommitFileIsReaded() { _commitFileIsReaded = true; }

private:
    MessageCommitter(const MessageCommitter &);
    MessageCommitter &operator=(const MessageCommitter &);

public:
    protocol::ErrorCode write(const common::MemoryMessageVector &vec);
    protocol::ErrorCode write(const ::google::protobuf::RepeatedPtrField<::swift::protocol::Message> &msgs);
    protocol::ErrorCode write(const ::swift::protocol::flat::Messages &msgs);
    protocol::ErrorCode commitFile();
    protocol::ErrorCode closeFile();
    int32_t getWriteFailMetaCount() { return _writeFailMetaCount; }
    int32_t getWriteFailDataCount() { return _writeFailDataCount; }
    int64_t getWriteBufferSize() { return _writeBufferSize; }
    bool hasSealError() { return _hasSealError; }

protected:
    template <typename T>
    protocol::ErrorCode write(const T &msg);
    protocol::ErrorCode openFile(int64_t msgId, int64_t timestamp);
    protocol::ErrorCode maybeCommit(int64_t curTime);

    bool needCommitFile(int64_t curTime);
    bool needCloseFile(int64_t curTime);

public:
    // for test
    void setMaxFileSize(int64_t bytes) { _maxFileSize = bytes; }
    const fslib::fs::File *getDataFile() const { return _dataOutputFile; }
    const fslib::fs::File *getMetaFile() const { return _metaOutputFile; }

private:
    template <typename ContainerType>
    protocol::ErrorCode writeMessages(const ContainerType &msgs);
    template <typename T>
    protocol::ErrorCode writeSingleMessage(const T &msg);
    protocol::ErrorCode writeDataBuffer(protocol::ErrorCode emptyError = protocol::ERROR_NONE);
    void resetBuffer();
    fslib::fs::File *openFileForWrite(const std::string &fileName, fslib::ErrorCode &ec);
    void setSealError(fslib::ErrorCode ec, bool tryRefreshVersion = false);

protected:
    protocol::PartitionId _partitionId;
    std::string _dataRoot;
    bool _commitFileIsReaded;
    int64_t _maxCommitSize;                // byte
    int64_t _maxCommitInterval;            // us
    int64_t _maxFileSize;                  // byte
    int64_t _minFileSize;                  // byte
    int64_t _maxFileSplitTime;             // us
    int64_t _closeNoWriteFileInterval;     // us
    int64_t _minChangeFileForDfsErrorTime; // us
    int64_t _maxCommitTimeAsError;         // us
    int64_t _lastCommitTimeAsError;        // us
    FileManager *_fileManager;
    FilePairPtr _nowFilePair;
    int64_t _committedId;
    int64_t _writedId;
    int64_t _writedSize;
    int64_t _committedSize;
    int64_t _fileCreateTime;
    int64_t _lastCommitTime;
    int64_t _fileMessageCount;
    int64_t _writedSizeForReport;
    int64_t _maxMessageCountInOneFile;

    int32_t _writeFailDataCount;
    int32_t _writeFailMetaCount;
    fslib::fs::File *_dataOutputFile;
    fslib::fs::File *_metaOutputFile;

    monitor::BrokerMetricsReporter *_metricsReporter;
    std::vector<char> _meta; // for batch write
    std::string _dataBuffer;
    int64_t _writeBufferSize;
    kmonitor::MetricsTags _tags;
    bool _enableFastRecover;
    bool _canRefeshVersion;
    std::atomic<bool> _hasSealError;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MessageCommitter);

} // namespace storage
} // namespace swift
