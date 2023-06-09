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
#include "indexlib/file_system/archive/ArchiveFolder.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/archive/ArchiveFile.h"
#include "indexlib/file_system/archive/ArchiveFileReader.h"
#include "indexlib/file_system/archive/ArchiveFileWriter.h"
#include "indexlib/util/Algorithm.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, ArchiveFolder);

const std::string ArchiveFolder::LOG_FILE_NAME = "INDEXLIB_ARCHIVE_FILES";
// const std::string ArchiveFolder::LOG_FILE_HINT_NAME = "INDEXLIB_ARCHIVE_HINT_FILE";

ArchiveFolder::ArchiveFolder(bool legacyMode) noexcept
    : _logFileVersion(-1)
    , _fileCreateCount(0)
    , _legacyMode(legacyMode)
{
}

ArchiveFolder::~ArchiveFolder() noexcept {}

FSResult<void> ArchiveFolder::Open(const std::shared_ptr<IDirectory>& directory,
                                   const std::string& logFileSuffix) noexcept
{
    assert(directory);
    ScopedLock lock(_lock);
    _directory = directory;
    _logFileSuffix = logFileSuffix;
    _inMemFiles.clear();
    _readLogFiles.clear();
    RETURN_IF_FS_ERROR(InitLogFiles(), "InitLogFiles failed");
    return FSEC_OK;
}

FSResult<void> ArchiveFolder::ReOpen() noexcept { return Open(_directory, _logFileSuffix); }

FSResult<void> ArchiveFolder::InitLogFiles() noexcept
{
    fslib::FileList fileList;
    RETURN_IF_FS_ERROR(_directory->ListDir("", ListOption(), fileList), "ListDir failed");
    fslib::FileList logFiles;
    vector<int64_t> logFileVersions;
    string emptySuffix;
    RETURN_IF_FS_ERROR(GetLogFiles(fileList, emptySuffix, logFiles, logFileVersions), "GetLogFiles failed");
    for (unsigned int i = 0; i < logFiles.size(); i++) {
        ReadLogFile readLogFile;
        readLogFile.logFile.reset(new LogFile());
        RETURN_IF_FS_ERROR(readLogFile.logFile->Open(_directory, logFiles[i], fslib::READ), "Open in [%s] failed",
                           _directory->DebugString().c_str());
        readLogFile.version = logFileVersions[i];
        _readLogFiles.push_back(readLogFile);
    }
    sort(_readLogFiles.begin(), _readLogFiles.end(), ReadLogCompare());
    auto [ec, version] = GetMaxWriteVersion(fileList);
    RETURN_IF_FS_ERROR(ec, "GetMaxWriteVersion failed");
    _logFileVersion = version + 1;
    return FSEC_OK;
}

FSResult<int64_t> ArchiveFolder::GetMaxWriteVersion(const fslib::FileList& fileList) noexcept
{
    fslib::FileList logFiles;
    vector<int64_t> logFileVersions;
    RETURN2_IF_FS_ERROR(GetLogFiles(fileList, _logFileSuffix, logFiles, logFileVersions), -1, "GetLogFiles failed");
    if (logFileVersions.size() == 0) {
        return {FSEC_OK, -1};
    }
    sort(logFileVersions.begin(), logFileVersions.end());
    return {FSEC_OK, logFileVersions.back()};
}

FSResult<void> ArchiveFolder::GetLogFiles(const fslib::FileList& fileList, const string& logFileSuffix,
                                          fslib::FileList& logFiles, vector<int64_t>& logFileVersions) noexcept
{
    fslib::FileList tmpFiles;
    string suffix = _logFileSuffix.empty() ? "" : string("_") + logFileSuffix;
    string logFileName = LOG_FILE_NAME + suffix;
    for (unsigned int i = 0; i < fileList.size(); ++i) {
        if (fileList[i].find(logFileName) != string::npos) {
            tmpFiles.push_back(fileList[i]);
        }
    }
    LogFile::GetLogFiles(tmpFiles, logFiles);
    for (auto& logFile : logFiles) {
        string version = logFile.substr(logFile.rfind("_") + 1, logFile.size());
        int64_t tmpVersion;
        if (!StringUtil::fromString(version, tmpVersion)) {
            assert(false);
            AUTIL_LOG(ERROR, "invalid archive folder log file name[%s]", logFile.c_str());
            return FSEC_NOTSUP;
        }
        logFileVersions.push_back(tmpVersion);
    }
    return FSEC_OK;
}

FSResult<LogFilePtr> ArchiveFolder::GetLogFileForWrite(const string& logFileSuffix) noexcept
{
    if (_writeLogFile) {
        return {FSEC_OK, _writeLogFile};
    }
    string suffix = logFileSuffix.empty() ? "" : string("_") + logFileSuffix;
    string logFileName = LOG_FILE_NAME + suffix + "_" + StringUtil::toString(_logFileVersion);
    LogFilePtr logFile(new LogFile());
    RETURN2_IF_FS_ERROR(logFile->Open(_directory, logFileName, fslib::APPEND), LogFilePtr(), "Open in dir[%s] failed",
                        _directory->DebugString().c_str());
    _writeLogFile = logFile;
    return {FSEC_OK, logFile};
}

FSResult<std::shared_ptr<FileWriter>> ArchiveFolder::CreateInnerFileWriter(const std::string fileName) noexcept
{
    string signature = StringUtil::toString(_logFileVersion) + "_" + StringUtil::toString(_fileCreateCount);
    ArchiveFilePtr archiveFile(new ArchiveFile(false, signature));
    auto [ec, logFile] = GetLogFileForWrite(_logFileSuffix);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "GetLogFileForWrite failed");
    RETURN2_IF_FS_ERROR(archiveFile->Open(fileName, logFile), std::shared_ptr<FileWriter>(), "Open [%s] failed",
                        fileName.c_str());
    _fileCreateCount++;
    return {FSEC_OK, ArchiveFileWriterPtr(new ArchiveFileWriter(archiveFile))};
}

FSResult<std::shared_ptr<FileWriter>> ArchiveFolder::CreateFileWriter(const std::string& fileName) noexcept
{
    ScopedLock lock(_lock);
    _inMemFiles.insert(fileName);
    if (_legacyMode) {
        RETURN2_IF_FS_ERROR(_directory->RemoveFile(fileName, RemoveOption::MayNonExist()),
                            std::shared_ptr<FileWriter>(), "RemoveFile failed");
        return _directory->CreateFileWriter(fileName, WriterOption());
    }
    return CreateInnerFileWriter(fileName);
}

FSResult<bool> ArchiveFolder::IsExist(const std::string& fileName) noexcept
{
    ScopedLock lock(_lock);
    if (_inMemFiles.count(fileName) > 0) {
        return {FSEC_OK, true};
    }
    for (auto readLogFile : _readLogFiles) {
        if (ArchiveFile::IsExist(fileName, readLogFile.logFile)) {
            return {FSEC_OK, true};
        }
    }
    return _directory->IsExist(fileName);
}

FSResult<std::shared_ptr<FileReader>> ArchiveFolder::CreateFileReader(const std::string& fileName) noexcept
{
    ScopedLock lock(_lock);
    ArchiveFilePtr archiveFile;
    for (size_t i = 0; i < _readLogFiles.size(); i++) {
        if (ArchiveFile::IsExist(fileName, _readLogFiles[i].logFile)) {
            archiveFile.reset(new ArchiveFile(true, ""));
            RETURN2_IF_FS_ERROR(archiveFile->Open(fileName, _readLogFiles[i].logFile), std::shared_ptr<FileReader>(),
                                "Open [%s] failed", fileName.c_str());
            return {FSEC_OK, ArchiveFileReaderPtr(new ArchiveFileReader(archiveFile))};
        }
    }

    auto [ec, fileReader] = _directory->CreateFileReader(fileName, FSOT_BUFFERED);
    if (ec == FSEC_OK) {
        return {ec, fileReader};
    } else if (ec == FSEC_NOENT) {
        return {FSEC_OK, std::shared_ptr<FileReader>()};
    }
    return {ec, std::shared_ptr<FileReader>()};
}

FSResult<void> ArchiveFolder::List(std::vector<std::string>& fileList) noexcept
{
    ScopedLock lock(_lock);
    for (size_t i = 0; i < _readLogFiles.size(); i++) {
        ArchiveFile::List(_readLogFiles[i].logFile, fileList);
    }
    fileList.insert(fileList.end(), _inMemFiles.begin(), _inMemFiles.end());

    std::vector<std::string> tmpFileList;
    auto fsec = _directory->ListDir("", ListOption::Recursive(), tmpFileList);
    RETURN_IF_FS_ERROR(fsec, "list dir [%s] failed", _directory->DebugString().c_str());
    for (const std::string& fileName : tmpFileList) {
        if (!autil::StringUtil::startsWith(fileName, ArchiveFolder::LOG_FILE_NAME)) {
            fileList.push_back(fileName);
        }
    }
    util::Algorithm::SortAndUnique(fileList.begin(), fileList.end());
    return FSEC_OK;
}

FSResult<void> ArchiveFolder::Close() noexcept
{
    ScopedLock lock(_lock);
    for (size_t i = 0; i < _readLogFiles.size(); i++) {
        RETURN_IF_FS_ERROR(_readLogFiles[i].logFile->Close(), "close reader[%s] failed",
                           _directory->DebugString().c_str());
    }
    _readLogFiles.clear();
    if (_writeLogFile) {
        RETURN_IF_FS_ERROR(_writeLogFile->Close(), "close writer[%s] failed", _directory->DebugString().c_str());
        _writeLogFile.reset();
    }
    return FSEC_OK;
}

}} // namespace indexlib::file_system
