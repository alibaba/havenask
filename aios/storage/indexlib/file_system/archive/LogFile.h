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

#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/FSResult.h"

namespace indexlib { namespace file_system {

class IDirectory;
class FileReader;
class BufferedFileWriter;

class LogFile
{
public:
    LogFile() : _openMode(fslib::APPEND) {}
    ~LogFile();

public:
    // void Open(const std::string& filePath, fslib::Flag openMode); //flag only support write, read
    FSResult<void> Open(const std::shared_ptr<IDirectory>& directory, const std::string& filePath,
                        fslib::Flag openMode) noexcept; // flag only support write, read
    FSResult<void> Write(const std::string& key, const char* value, int64_t valueSize) noexcept;
    FSResult<void> Flush() noexcept;
    FSResult<size_t> Read(const std::string& key, char* readBuffer, int64_t bufferSize) noexcept;
    FSResult<void> Close() noexcept;
    bool HasKey(const std::string& key) noexcept;
    void ListKeys(std::vector<std::string>& fileList) noexcept;
    int64_t GetValueLength(const std::string& key) noexcept;

    static void GetLogFiles(const fslib::FileList& fileList, fslib::FileList& logFileInfos) noexcept;

private:
    struct RecordInfo : public autil::legacy::Jsonizable {
        int64_t valueOffset;
        int64_t valueLength;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("value_offset", valueOffset, valueOffset);
            json.Jsonize("value_length", valueLength, valueLength);
        }
    };
    FSResult<void> WriteMeta(const std::string& key, const RecordInfo& recordInfo) noexcept;
    // void LoadLogFile(const std::string& dataFile);
    FSResult<void> LoadLogFile(const std::shared_ptr<IDirectory>& directory, const std::string& metaFile,
                               const std::string& dataFile, fslib::Flag openMode) noexcept;
    bool Parse(std::string& lineStr, std::string& key, RecordInfo& record) noexcept;

private:
    std::string _filePath;
    std::shared_ptr<FileReader> _dataFileReader;
    std::shared_ptr<BufferedFileWriter> _metaFileWriter;
    std::shared_ptr<BufferedFileWriter> _dataFileWriter;
    autil::ThreadMutex _fileLock;
    fslib::Flag _openMode;
    std::map<std::string, RecordInfo> _metaInfos;
    static const std::string META_FILE_SUFFIX;
    static const std::string DATA_FILE_SUFFIX;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<LogFile> LogFilePtr;
}} // namespace indexlib::file_system
