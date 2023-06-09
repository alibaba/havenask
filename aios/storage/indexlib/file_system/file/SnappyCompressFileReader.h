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

#include <assert.h>
#include <memory>
#include <stddef.h>
#include <string>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/buffer_compressor/SnappyCompressor.h"

namespace indexlib { namespace util {
class ByteSliceList;
}} // namespace indexlib::util

namespace indexlib { namespace file_system {
class CompressFileMeta;

class SnappyCompressFileReader : public FileReader
{
public:
    SnappyCompressFileReader() noexcept
        : _curInBlockOffset(0)
        , _curBlockBeginOffset(0)
        , _offset(0)
        , _oriFileLength(0)
        , _curBlockIdx(0)
        , _blockSize(0)
    {
    }

    ~SnappyCompressFileReader() noexcept {}

public:
    bool Init(const std::shared_ptr<FileReader>& fileReader) noexcept(false);

public:
    FSResult<void> Open() noexcept override { return FSEC_OK; }
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;
    FSResult<size_t> Read(void* buffer, size_t length, ReadOption option) noexcept override;
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override
    {
        AUTIL_LOG(ERROR, "CompressReader does not support Read ByteSliceList");
        assert(false);
        return NULL;
    }
    void* GetBaseAddress() const noexcept override { return NULL; }
    size_t GetLength() const noexcept override
    {
        assert(_dataFileReader);
        return _dataFileReader->GetLength();
    }
    const std::string& GetLogicalPath() const noexcept override
    {
        assert(_dataFileReader);
        return _dataFileReader->GetLogicalPath();
    }
    const std::string& GetPhysicalPath() const noexcept override
    {
        assert(_dataFileReader);
        return _dataFileReader->GetPhysicalPath();
    }
    FSResult<void> Close() noexcept override
    {
        if (_dataFileReader) {
            auto ret = _dataFileReader->Close();
            _dataFileReader.reset();
            return ret;
        }
        return FSEC_OK;
    };
    FSOpenType GetOpenType() const noexcept override
    {
        if (_dataFileReader) {
            _dataFileReader->GetOpenType();
        }
        return FSOT_UNKNOWN;
    }
    FSFileType GetType() const noexcept override
    {
        if (_dataFileReader) {
            _dataFileReader->GetType();
        }
        return FSFT_UNKNOWN;
    }
    std::shared_ptr<FileNode> GetFileNode() const noexcept override
    {
        assert(false);
        return std::shared_ptr<FileNode>();
    }

public:
    size_t GetUncompressedFileLength() const noexcept(false);
    std::shared_ptr<FileReader> GetDataFileReader() const noexcept { return _dataFileReader; }

private:
    bool Init(const std::shared_ptr<FileReader>& fileReader, const std::shared_ptr<CompressFileMeta>& compressMeta,
              size_t blockSize) noexcept(false);

    std::shared_ptr<CompressFileMeta> LoadCompressMeta(const std::shared_ptr<FileReader>& fileReader) noexcept(false);
    bool InCurrentBlock(size_t offset) const noexcept;
    bool LocateBlockOffsetInCompressFile(size_t offset, size_t& blockIdx, size_t& uncompBlockOffset,
                                         size_t& compBlockOffset, size_t& compBlockSize) const noexcept(false);

    void LoadBuffer(size_t offset, ReadOption option) noexcept(false);

private:
    std::shared_ptr<FileReader> _dataFileReader;
    std::shared_ptr<CompressFileMeta> _compressMeta;
    util::SnappyCompressor _compressor;
    size_t _curInBlockOffset;
    size_t _curBlockBeginOffset;
    size_t _offset;
    size_t _oriFileLength;
    size_t _curBlockIdx;
    size_t _blockSize;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SnappyCompressFileReader> SnappyCompressFileReaderPtr;
}} // namespace indexlib::file_system
