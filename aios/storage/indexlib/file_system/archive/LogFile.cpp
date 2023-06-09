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
#include "indexlib/file_system/archive/LogFile.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <memory>
#include <sys/types.h>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "fslib/fs/File.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/archive/LineReader.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, LogFile);

const std::string LogFile::META_FILE_SUFFIX = ".lfm";
const std::string LogFile::DATA_FILE_SUFFIX = ".lfd";

LogFile::~LogFile() {}

FSResult<void> LogFile::Open(const std::shared_ptr<IDirectory>& directory, const std::string& filePath,
                             fslib::Flag openMode) noexcept
{
    ScopedLock lock(_fileLock);
    _filePath = filePath;
    _openMode = openMode;
    if (openMode != fslib::READ && openMode != fslib::APPEND) {
        AUTIL_LOG(ERROR, "log file [%s] open only support read or append", filePath.c_str());
        return FSEC_NOTSUP;
    }
    string metaFile = filePath + META_FILE_SUFFIX;
    string dataFile = filePath + DATA_FILE_SUFFIX;
    RETURN_IF_FS_ERROR(LoadLogFile(directory, metaFile, dataFile, openMode).Code(), "LoadLogFile failed");
    return FSEC_OK;
}

bool LogFile::HasKey(const std::string& key) noexcept
{
    ScopedLock lock(_fileLock);
    return _metaInfos.find(key) != _metaInfos.end();
}

void LogFile::ListKeys(std::vector<std::string>& fileList) noexcept
{
    ScopedLock lock(_fileLock);
    for (const auto& metaInfo : _metaInfos) {
        fileList.push_back(metaInfo.first);
    }
}

bool LogFile::Parse(std::string& lineStr, std::string& key, RecordInfo& record) noexcept
{
    vector<string> recordInfos = StringUtil::split(lineStr, "\t");
    if (recordInfos.size() != 4) {
        return false;
    }
    int64_t keyLength;
    if (!StringUtil::numberFromString(recordInfos[0], keyLength)) {
        return false;
    }
    if (keyLength != (int64_t)recordInfos[3].length()) {
        return false;
    }
    key = recordInfos[3];
    if (!StringUtil::numberFromString(recordInfos[1], record.valueOffset) ||
        !StringUtil::numberFromString(recordInfos[2], record.valueLength)) {
        return false;
    }
    return true;
}

FSResult<void> LogFile::LoadLogFile(const std::shared_ptr<IDirectory>& directory, const std::string& metaFile,
                                    const std::string& dataFile, fslib::Flag openMode) noexcept
{
    if (openMode == fslib::READ) {
        LineReader lineReader;
        auto [ec, fileReader] = directory->CreateFileReader(metaFile, ReaderOption::NoCache(FSOT_BUFFERED));
        RETURN_IF_FS_ERROR(ec, "CreateFileReader failed");
        lineReader.Open(fileReader);
        string line;
        while (lineReader.NextLine(line, ec)) {
            string key;
            RecordInfo recordInfo;
            if (Parse(line, key, recordInfo)) {
                _metaInfos[key] = recordInfo;
            } else {
                AUTIL_LOG(DEBUG, "log file [%s] record is illegal format", _filePath.c_str());
            }
        }
        RETURN_IF_FS_ERROR(ec, "NextLine failed");
        std::tie(ec, _dataFileReader) =
            directory->CreateFileReader(dataFile, ReaderOption::NoCache(FSOT_BUFFERED)).CodeWith();
        RETURN_IF_FS_ERROR(ec, "CreateFileReader failed");
    } else if (openMode == fslib::APPEND) {
        WriterOption writerOption = WriterOption::Buffer();
        writerOption.isAppendMode = true;
        auto [ec, tmpWriter] = directory->CreateFileWriter(dataFile, writerOption);
        RETURN_IF_FS_ERROR(ec, "CreateFileWriter failed");
        _dataFileWriter = std::dynamic_pointer_cast<BufferedFileWriter>(tmpWriter);
        assert(_dataFileWriter);
        if (unlikely(!_dataFileWriter)) {
            AUTIL_LOG(ERROR, "Open LogFile [%s] data writer failed, not BufferedFileWriter",
                      tmpWriter->GetPhysicalPath().c_str());
            return FSEC_ERROR;
        }

        std::tie(ec, tmpWriter) = directory->CreateFileWriter(metaFile, writerOption).CodeWith();
        _metaFileWriter = std::dynamic_pointer_cast<BufferedFileWriter>(tmpWriter);
        assert(_metaFileWriter);
        if (unlikely(!_metaFileWriter)) {
            AUTIL_LOG(ERROR, "Open LogFile [%s] meta writer failed, not BufferedFileWriter",
                      tmpWriter->GetPhysicalPath().c_str());
            return FSEC_ERROR;
        }
        char endLine[1] = {'\n'};
        RETURN_IF_FS_ERROR(_metaFileWriter->Write(endLine, 1).Code(), "write meta failed");
    }
    return FSEC_OK;
}

FSResult<void> LogFile::Write(const std::string& key, const char* value, int64_t valueSize) noexcept
{
    ScopedLock lock(_fileLock);
    RecordInfo recordInfo;
    recordInfo.valueOffset = _dataFileWriter->GetLength();
    recordInfo.valueLength = valueSize;
    RETURN_IF_FS_ERROR(_dataFileWriter->Write(value, valueSize).Code(), "write data failed");
    auto [ec, flushRet] = _dataFileWriter->Flush();
    RETURN_IF_FS_ERROR(ec, "flush data failed");
    if (!flushRet) {
        AUTIL_LOG(ERROR, "flush data file [%s.%s] failed", _filePath.c_str(), DATA_FILE_SUFFIX.c_str());
        return FSEC_ERROR;
    }
    RETURN_IF_FS_ERROR(WriteMeta(key, recordInfo), "WriteMeta failed");
    _metaInfos[key] = recordInfo;
    return FSEC_OK;
}

FSResult<void> LogFile::WriteMeta(const std::string& key, const RecordInfo& recordInfo) noexcept
{
    // totalLegth, valueOffset, valueLength is int64_t and "\n"
    string keyLengthStr = StringUtil::toString(key.size()) + "\t";
    RETURN_IF_FS_ERROR(_metaFileWriter->Write((void*)(keyLengthStr.c_str()), keyLengthStr.length()).Code(),
                       "write meta failed");
    string valueOffsetStr = StringUtil::toString(recordInfo.valueOffset) + "\t";
    RETURN_IF_FS_ERROR(_metaFileWriter->Write((void*)valueOffsetStr.c_str(), valueOffsetStr.length()).Code(),
                       "write meta failed");
    string valueLengthStr = StringUtil::toString(recordInfo.valueLength) + "\t";
    RETURN_IF_FS_ERROR(_metaFileWriter->Write((char*)(valueLengthStr.c_str()), valueLengthStr.length()).Code(),
                       "write meta failed");
    RETURN_IF_FS_ERROR(_metaFileWriter->Write(key.c_str(), key.size()).Code(), "write meta failed");
    char endLine[1] = {'\n'};
    RETURN_IF_FS_ERROR(_metaFileWriter->Write(endLine, 1).Code(), "write meta failed");
    auto [ec, flushRet] = _metaFileWriter->Flush();
    if (!flushRet) {
        AUTIL_LOG(ERROR, "flush meta file [%s.%s] failed", _filePath.c_str(), META_FILE_SUFFIX.c_str());
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

FSResult<void> LogFile::Flush() noexcept
{
    ScopedLock lock(_fileLock);
    auto [ec1, ret1] = _dataFileWriter->Flush();
    RETURN_IF_FS_ERROR(ec1, "flush data failed");
    if (!ret1) {
        AUTIL_LOG(ERROR, "flush data file [%s.%s] failed", _filePath.c_str(), DATA_FILE_SUFFIX.c_str());
        return FSEC_ERROR;
    }
    auto [ec2, ret2] = _metaFileWriter->Flush();
    if (!ret2) {
        AUTIL_LOG(ERROR, "flush meta file [%s.%s] failed", _filePath.c_str(), META_FILE_SUFFIX.c_str());
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

FSResult<size_t> LogFile::Read(const std::string& key, char* readBuffer, int64_t bufferSize) noexcept
{
    ScopedLock lock(_fileLock);
    auto iter = _metaInfos.find(key);
    if (iter == _metaInfos.end()) {
        return {FSEC_OK, 0};
    }
    if (bufferSize < iter->second.valueLength) {
        assert(false);
        AUTIL_LOG(ERROR, "read log file failed,buffer size [%ld] is smaller than value size [%ld], key[%s]", bufferSize,
                  iter->second.valueLength, key.c_str());
        return {FSEC_BADARGS, 0};
    }
    auto [ec, readLength] = _dataFileReader->Read(readBuffer, iter->second.valueLength, iter->second.valueOffset);
    RETURN2_IF_FS_ERROR(ec, readLength, "Read failed");

    if ((int64_t)readLength != iter->second.valueLength) {
        assert(false);
        AUTIL_LOG(ERROR, "read log file failed, read size [%ld] is not equal with value size [%lu], key[%s]",
                  readLength, iter->second.valueLength, key.c_str());
        return {FSEC_ERROR, readLength};
    }
    return {FSEC_OK, readLength};
}

FSResult<void> LogFile::Close() noexcept
{
    ScopedLock lock(_fileLock);
    if (_dataFileReader) {
        RETURN_IF_FS_ERROR(_dataFileReader->Close(), "close data file reader failed, file[%s]", _filePath.c_str());
        _dataFileReader.reset();
    }
    if (_dataFileWriter) {
        RETURN_IF_FS_ERROR(_dataFileWriter->Close(), "close data file writer failed, file[%s]", _filePath.c_str());
        _dataFileWriter.reset();
    }
    if (_metaFileWriter) {
        RETURN_IF_FS_ERROR(_metaFileWriter->Close(), "close meta file writer failed, file[%s]", _filePath.c_str());
        _metaFileWriter.reset();
    }
    return FSEC_OK;
}

int64_t LogFile::GetValueLength(const std::string& key) noexcept
{
    ScopedLock lock(_fileLock);
    auto iter = _metaInfos.find(key);
    if (iter == _metaInfos.end()) {
        return 0;
    }
    return iter->second.valueLength;
}

void LogFile::GetLogFiles(const fslib::FileList& fileList, fslib::FileList& logFiles) noexcept
{
    for (size_t i = 0; i < fileList.size(); ++i) {
        if (fileList[i].find(META_FILE_SUFFIX) == string::npos) {
            continue;
        }
        string logFileName = fileList[i].substr(0, fileList[i].find(META_FILE_SUFFIX));
        logFiles.push_back(logFileName);
    }
}
}} // namespace indexlib::file_system
