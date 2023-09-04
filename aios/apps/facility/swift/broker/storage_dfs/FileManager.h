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
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "swift/common/Common.h"
#include "swift/common/FilePair.h"
#include "swift/config/ConfigDefine.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/FileManagerUtil.h"
#include "swift/util/PanguInlineFileUtil.h"

namespace swift {
namespace storage {

struct ObsoleteFileCriterion {
    ObsoleteFileCriterion(int64_t timeInterval = std::numeric_limits<int64_t>::max(),
                          int32_t fileCount = std::numeric_limits<int32_t>::max(),
                          int64_t delInterval = config::DEFAULT_DEL_OBSOLETE_FILE_INTERVAL,
                          int64_t candidateFileInterval = config::DEFAULT_CANDIDATE_OBSOLETE_FILE_INTERVAL)
        : obsoleteFileTimeInterval(timeInterval)
        , reservedFileCount(fileCount)
        , delObsoleteFileInterval(delInterval)
        , candidateObsoleteFileInterval(candidateFileInterval) {}
    int64_t obsoleteFileTimeInterval;
    int32_t reservedFileCount;
    int64_t delObsoleteFileInterval;
    int64_t candidateObsoleteFileInterval;
};

// It can provide input/output files (FilePair) needed by
// FsMessageReader and MessageCommitter, including data and meta files.
// If in Only Memory mode, this will not be created.
// NOTE: thread-safe.

class FileManager {
public:
    static const int64_t COMMITTED_TIMESTAMP_INVALID = -2;

public:
    FileManager();
    virtual ~FileManager();

private:
    FileManager(const FileManager &);
    FileManager &operator=(const FileManager &);

public:
    protocol::ErrorCode init(const std::string &dataPath, const ObsoleteFileCriterion &obsoleteFileCriterion);
    protocol::ErrorCode init(const std::string &dataPath,
                             const std::vector<std::string> &extendDataPathVec,
                             const ObsoleteFileCriterion &obsoleteFileCriterion,
                             const util::InlineVersion &inlineVersion,
                             bool enableFastRecover = false);
    protocol::ErrorCode initForReadOnly(const std::string &dataPath, const std::vector<std::string> &extendDataPathVec);
    FilePairPtr createNewFilePair(int64_t messageId, int64_t timestamp);
    FilePairPtr getLastFilePair() const;
    FilePairPtr getFilePairById(int64_t messageId) const;
    FilePairPtr getFilePairByTime(int64_t timestamp) const;
    int64_t getLastMessageId() const;
    int64_t getMinMessageId() const;
    void delExpiredFile(int64_t commitedTimestamp = COMMITTED_TIMESTAMP_INVALID);
    virtual protocol::ErrorCode refreshVersion(); // virtual for test
    util::InlineVersion getInlineVersion() { return _inlineVersion; }
    std::string getInlineFilePath() const;

private:
    protocol::ErrorCode removeInlineFile();
    protocol::ErrorCode adjustFilePairVec(FilePairVec &filePairVec);
    void syncFilePair();
    void deleteObsoleteFile(int64_t commitedTimestamp = COMMITTED_TIMESTAMP_INVALID);
    void doDeleteObsoleteFile();
    int32_t getObsoleteFilePos(uint32_t reservedFileCount, int64_t commitedTs = COMMITTED_TIMESTAMP_INVALID) const;
    // for unit test
    virtual fslib::ErrorCode doListFiles(const fslib::FileList &filePaths, util::FileManagerUtil::FileLists &fileList);
    fslib::ErrorCode listFiles(util::FileManagerUtil::FileLists &fileList);
    bool isFileObsolete(int64_t fileTime, int64_t curTime, int64_t commitedTs) const;
    bool isTimestamp(int64_t obsoleteTime, int64_t &realTime) const;
    protocol::ErrorCode updateInlineVersion(const util::InlineVersion &inlineVersion);
    protocol::ErrorCode sealLastFilePair(const FilePairVec &filePairVec);
    protocol::ErrorCode sealOtherWriter(const util::InlineVersion &inlineVersion);
    void updateDfsTimeout(int64_t timeout = -1);

private:
    mutable autil::ReadWriteLock _rwLock;
    std::string _dataPath;
    std::vector<std::string> _extendDataPathVec;
    FilePairVec _filePairVec;
    FilePairVec _obsoleteFilePairVec;
    ObsoleteFileCriterion _obsoleteFileCriterion;
    int64_t _lastDelFileTime;
    util::InlineVersion _inlineVersion;
    bool _enableFastRecover;

private:
    AUTIL_LOG_DECLARE();
};
SWIFT_TYPEDEF_PTR(FileManager);

} // namespace storage
} // namespace swift
