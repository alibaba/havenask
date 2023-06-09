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
#include "indexlib/file_system/archive/LogFile.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"

namespace indexlib { namespace file_system {

class IDirectory;

class ArchiveFolder
{
public:
    ArchiveFolder(bool legacyMode) noexcept;
    ~ArchiveFolder() noexcept;

public:
    FSResult<void> Open(const std::shared_ptr<IDirectory>& directory, const std::string& logFileSuffix = "") noexcept;
    FSResult<void> ReOpen() noexcept;
    FSResult<bool> IsExist(const std::string& relativePath) noexcept;
    FSResult<std::shared_ptr<FileReader>> CreateFileReader(const std::string& relativePath) noexcept;
    FSResult<std::shared_ptr<FileWriter>> CreateFileWriter(const std::string& relativePath) noexcept;
    FSResult<void> List(std::vector<std::string>& fileList) noexcept;

    FSResult<void> Close() noexcept;
    std::shared_ptr<IDirectory> GetRootDirectory() const noexcept { return _directory; }

public:
    static const std::string LOG_FILE_NAME;

private:
    FSResult<LogFilePtr> GetLogFileForWrite(const std::string& logFileSuffix) noexcept;
    FSResult<void> GetLogFiles(const fslib::FileList& fileList, const std::string& logFileSuffix,
                               fslib::FileList& logFiles, std::vector<int64_t>& logFileVersions) noexcept;
    FSResult<void> InitLogFiles() noexcept;
    FSResult<std::shared_ptr<FileWriter>> CreateInnerFileWriter(const std::string fileName) noexcept;
    FSResult<int64_t> GetMaxWriteVersion(const fslib::FileList& fileList) noexcept;

private:
    struct ReadLogFile {
        int64_t version;
        LogFilePtr logFile;
    };
    struct ReadLogCompare {
        bool operator()(const ReadLogFile& left, const ReadLogFile& right) noexcept
        {
            return left.version > right.version;
        }
    };

private:
    // std::string _dirPath;
    std::shared_ptr<IDirectory> _directory;

    std::set<std::string> _inMemFiles;
    std::vector<ReadLogFile> _readLogFiles;
    LogFilePtr _writeLogFile;
    autil::ThreadMutex _lock;
    std::string _logFileSuffix;
    int64_t _logFileVersion;
    int64_t _fileCreateCount;
    bool _legacyMode;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ArchiveFolder> ArchiveFolderPtr;
}} // namespace indexlib::file_system
