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

#include <algorithm>
#include <assert.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <sys/types.h>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/archive/LogFile.h"
#include "indexlib/file_system/file/FileBuffer.h"

namespace indexlib { namespace file_system {

class ArchiveFile
{
public:
    ArchiveFile(bool isReadOnly, const std::string& signature) noexcept
        : _offset(0)
        , _currentBlock(-1)
        , _isReadOnly(isReadOnly)
    {
        _fileMeta.maxBlockSize = 4 * 1024 * 1024; // default 4M
        _fileMeta.signature = signature;
        _buffer = new FileBuffer(_fileMeta.maxBlockSize);
    }
    ~ArchiveFile() noexcept;

private:
    static std::string GetArchiveFileMeta(const std::string& fileName) noexcept;
    static bool ParseArchiveFileMeta(const std::string& archiveFileMetaName, std::string& fileName) noexcept;

public:
    FSResult<void> Open(const std::string& fileName, const LogFilePtr& logFile) noexcept;
    FSResult<size_t> Read(void* buffer, size_t length) noexcept;
    FSResult<size_t> PRead(void* buffer, size_t length, off_t offset) noexcept;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept;
    FSResult<void> Close() noexcept;
    static bool IsExist(const std::string& innerFileName, const LogFilePtr& logFile) noexcept
    {
        std::string fileMetaName = GetArchiveFileMeta(innerFileName);
        return logFile->HasKey(fileMetaName);
    }
    static void List(const LogFilePtr& logFile, std::vector<std::string>& fileList) noexcept
    {
        std::vector<std::string> itemList;
        logFile->ListKeys(itemList);
        std::string fileName;
        for (size_t i = 0; i < itemList.size(); ++i) {
            if (ArchiveFile::ParseArchiveFileMeta(itemList[i], fileName)) {
                fileList.push_back(fileName);
            }
        }
    }

    int64_t GetFileLength() const noexcept;

private:
    ErrorCode Flush() noexcept;
    ArchiveFile(int64_t bufferSize) : _buffer(new FileBuffer(bufferSize)), _offset(0), _currentBlock(-1) {}
    void Seek(int64_t offset, fslib::SeekFlag flag) noexcept;
    int64_t Tell() noexcept;
    ErrorCode DumpBuffer() noexcept;

    struct Block : public autil::legacy::Jsonizable {
        std::string key;
        int64_t length;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("block_key", key, key);
            json.Jsonize("block_length", length, length);
        }
    };

    struct FileMeta : public autil::legacy::Jsonizable {
        int64_t maxBlockSize;
        std::string signature;
        std::vector<Block> blocks;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("max_block_size", maxBlockSize, maxBlockSize);
            json.Jsonize("blocks", blocks, blocks);
            json.Jsonize("signature", signature, signature);
        }
    };

    struct ReadBlockInfo {
        int64_t beginPos;
        int64_t readLength;
        int64_t blockIdx;
    };

private:
    std::string GetBlockKey(int64_t blockIdx) noexcept;
    void CalculateBlockInfo(int64_t offset, size_t length, std::vector<ReadBlockInfo>& readInfos) noexcept;
    FSResult<void> FillReadBuffer(int64_t blockIdx) noexcept;
    FSResult<void> FillBufferFromBlock(const ReadBlockInfo& readInfo, char* buffer) noexcept;

    // ForTest
    void ResetBufferSize(int64_t bufferSize)
    {
        if (_buffer) {
            delete _buffer;
        }
        _fileMeta.maxBlockSize = bufferSize;
        _buffer = new FileBuffer(bufferSize);
    }
    int64_t GetBufferSize() { return _buffer->GetBufferSize(); }

private:
    friend class ArchiveFolderTest;

private:
    std::string _fileName;
    LogFilePtr _logFile;
    FileBuffer* _buffer;
    FileMeta _fileMeta;
    int64_t _offset;
    int64_t _currentBlock;
    bool _isReadOnly;
    std::string _signature;

private:
    static const std::string ARCHIVE_FILE_META_SUFFIX;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ArchiveFile> ArchiveFilePtr;
}} // namespace indexlib::file_system
