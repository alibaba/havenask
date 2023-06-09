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
#include "indexlib/file_system/archive/ArchiveFile.h"

#include <algorithm>
#include <assert.h>
#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string.h>
#include <string>
#include <sys/types.h>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/archive/LogFile.h"
#include "indexlib/file_system/file/FileBuffer.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, ArchiveFile);

const std::string ArchiveFile::ARCHIVE_FILE_META_SUFFIX("_archive_file_meta");

std::string ArchiveFile::GetArchiveFileMeta(const std::string& fileName) noexcept
{
    return fileName + ARCHIVE_FILE_META_SUFFIX;
}
bool ArchiveFile::ParseArchiveFileMeta(const std::string& archiveFileMetaName, std::string& fileName) noexcept
{
    if (!autil::StringUtil::endsWith(archiveFileMetaName, ARCHIVE_FILE_META_SUFFIX)) {
        return false;
    }
    fileName = archiveFileMetaName.substr(0, archiveFileMetaName.size() - ARCHIVE_FILE_META_SUFFIX.size());
    return true;
}

ArchiveFile::~ArchiveFile() noexcept { delete _buffer; }

FSResult<size_t> ArchiveFile::Read(void* buffer, size_t length) noexcept
{
    size_t readLength = 0;
    vector<ReadBlockInfo> readBlockInfos;
    CalculateBlockInfo(_offset, length, readBlockInfos);
    char* cursor = (char*)buffer;
    for (size_t i = 0; i < readBlockInfos.size(); i++) {
        RETURN2_IF_FS_ERROR(FillBufferFromBlock(readBlockInfos[i], cursor), readLength, "FillBufferFromBlock failed");
        readLength += readBlockInfos[i].readLength;
        cursor += readBlockInfos[i].readLength;
        _offset += readBlockInfos[i].readLength;
    }
    return {FSEC_OK, readLength};
}

FSResult<void> ArchiveFile::FillBufferFromBlock(const ReadBlockInfo& readInfo, char* buffer) noexcept
{
    if (_currentBlock != readInfo.blockIdx) {
        RETURN_IF_FS_ERROR(FillReadBuffer(readInfo.blockIdx).Code(), "FillReadBuffer failed");
        _currentBlock = readInfo.blockIdx;
    }
    memcpy(buffer, _buffer->GetBaseAddr() + readInfo.beginPos, readInfo.readLength);
    return FSEC_OK;
}

FSResult<void> ArchiveFile::FillReadBuffer(int64_t blockIdx) noexcept
{
    string blockKey = _fileMeta.blocks[blockIdx].key;
    int64_t length = _fileMeta.blocks[blockIdx].length;
    _buffer->SetCursor(0);
    auto [ec, readLen] = _logFile->Read(blockKey, _buffer->GetBaseAddr(), length);
    assert(readLen == length);
    return ec;
}

void ArchiveFile::CalculateBlockInfo(int64_t offset, size_t length, vector<ReadBlockInfo>& readInfos) noexcept
{
    int64_t blockBeginOffset = 0;
    int64_t blockEndOffset = 0;
    for (int64_t i = 0; i < (int64_t)_fileMeta.blocks.size(); i++) {
        blockEndOffset = blockBeginOffset + _fileMeta.blocks[i].length;
        if (blockBeginOffset >= offset + (int64_t)length) {
            break;
        }

        if (blockEndOffset <= offset) {
            blockBeginOffset += _fileMeta.blocks[i].length;
            continue;
        }

        ReadBlockInfo readBlock;
        readBlock.blockIdx = i;
        readBlock.beginPos = offset >= blockBeginOffset ? offset - blockBeginOffset : 0;
        readBlock.readLength =
            offset + (int64_t)length >= blockEndOffset
                ? _fileMeta.blocks[i].length - readBlock.beginPos
                : _fileMeta.blocks[i].length - readBlock.beginPos - (blockEndOffset - offset - length);
        readInfos.push_back(readBlock);
        blockBeginOffset += _fileMeta.blocks[i].length;
    }
}

FSResult<size_t> ArchiveFile::PRead(void* buffer, size_t length, off_t offset) noexcept
{
    Seek(offset, fslib::FILE_SEEK_SET);
    return Read(buffer, length);
}

FSResult<void> ArchiveFile::Open(const std::string& fileName, const LogFilePtr& logFile) noexcept
{
    _fileName = fileName;
    _logFile = logFile;
    if (!_isReadOnly) {
        return FSEC_OK;
    }
    string metaKey = GetArchiveFileMeta(fileName);
    if (!logFile->HasKey(metaKey)) {
        return FSEC_OK;
    }
    int64_t fileMetaSize = _logFile->GetValueLength(metaKey);
    if ((int64_t)_buffer->GetBufferSize() < fileMetaSize) {
        delete _buffer;
        _buffer = new FileBuffer(fileMetaSize);
    }
    auto [ec, valueLength] = _logFile->Read(metaKey, _buffer->GetBaseAddr(), _buffer->GetBufferSize());
    RETURN_IF_FS_ERROR(ec, "Read failed");
    string metaStr(_buffer->GetBaseAddr(), valueLength);
    RETURN_IF_FS_ERROR(JsonUtil::FromString(metaStr, &_fileMeta), "FromString failed");
    if (_fileMeta.maxBlockSize != _buffer->GetBufferSize()) {
        delete _buffer;
        _buffer = new FileBuffer(_fileMeta.maxBlockSize);
    }
    return FSEC_OK;
}

FSResult<size_t> ArchiveFile::Write(const void* buffer, size_t length) noexcept
{
    const char* cursor = (const char*)buffer;
    size_t leftLen = length;
    while (true) {
        uint32_t leftLenInBuffer = _buffer->GetBufferSize() - _buffer->GetCursor();
        uint32_t lengthToWrite = leftLenInBuffer < leftLen ? leftLenInBuffer : leftLen;
        _buffer->CopyToBuffer(cursor, lengthToWrite);
        cursor += lengthToWrite;
        leftLen -= lengthToWrite;
        if (leftLen <= 0) {
            assert(leftLen == 0);
            break;
        }
        if (_buffer->GetCursor() == _buffer->GetBufferSize()) {
            RETURN2_IF_FS_ERROR(DumpBuffer(), (length - leftLen), "DumpBuffer failed");
        }
    }
    return {FSEC_OK, length};
}

std::string ArchiveFile::GetBlockKey(int64_t blockIdx) noexcept
{
    return _fileName + "_" + _fileMeta.signature + "_" + "archive_file_block_" + autil::StringUtil::toString(blockIdx);
}

ErrorCode ArchiveFile::DumpBuffer() noexcept
{
    string blockKey = GetBlockKey(_fileMeta.blocks.size());
    Block block;
    block.key = blockKey;
    block.length = _buffer->GetCursor();
    _fileMeta.blocks.push_back(block);
    RETURN_IF_FS_ERROR(_logFile->Write(blockKey, _buffer->GetBaseAddr(), _buffer->GetCursor()).Code(),
                       "Write log file failed");
    _buffer->SetCursor(0);
    return FSEC_OK;
}

void ArchiveFile::Seek(int64_t offset, fslib::SeekFlag flag) noexcept { _offset = offset; }

int64_t ArchiveFile::Tell() noexcept
{
    assert(false);
    return 0;
}

ErrorCode ArchiveFile::Flush() noexcept
{
    if (_buffer->GetCursor() > 0) {
        RETURN_IF_FS_ERROR(DumpBuffer(), "DumpBuffer failed");
        RETURN_IF_FS_ERROR(_logFile->Flush(), "flush log file failed");
    }
    return FSEC_OK;
}

FSResult<void> ArchiveFile::Close() noexcept
{
    if (_isReadOnly) {
        return FSEC_OK;
    }
    if (_buffer->GetCursor() > 0) {
        RETURN_IF_FS_ERROR(DumpBuffer(), "DumpBuffer failed");
    }
    string fileMeta;
    RETURN_IF_FS_ERROR(JsonUtil::ToString(_fileMeta, &fileMeta).Code(), "ToString failed");
    string fileMetaName = GetArchiveFileMeta(_fileName);
    RETURN_IF_FS_ERROR(_logFile->Write(fileMetaName, fileMeta.c_str(), fileMeta.size()).Code(), "Write failed");
    RETURN_IF_FS_ERROR(_logFile->Flush().Code(), "Flush failed");
    return FSEC_OK;
}

int64_t ArchiveFile::GetFileLength() const noexcept
{
    int64_t totalLength = 0;
    for (int64_t i = 0; i < (int64_t)_fileMeta.blocks.size(); i++) {
        totalLength += _fileMeta.blocks[i].length;
    }
    return totalLength;
}

}} // namespace indexlib::file_system
