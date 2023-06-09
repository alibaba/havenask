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
#include "indexlib/file_system/file/SnappyCompressFileReader.h"

#include <iosfwd>
#include <stdint.h>
#include <string.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/DynamicBuf.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "indexlib/file_system/file/CompressFileMeta.h"
#include "indexlib/util/cache/BlockAccessCounter.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::util;
namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, SnappyCompressFileReader);

bool SnappyCompressFileReader::Init(const std::shared_ptr<FileReader>& fileReader) noexcept(false)
{
    std::shared_ptr<CompressFileMeta> compressMeta = LoadCompressMeta(fileReader);
    if (!compressMeta) {
        return false;
    }

    assert(fileReader);
    _dataFileReader = fileReader;
    _compressMeta = compressMeta;
    _compressor.SetBufferInLen(_compressMeta->GetMaxCompressBlockSize());
    _compressor.SetBufferOutLen(_blockSize);

    _curInBlockOffset = 0;
    _oriFileLength = _compressMeta->GetUnCompressFileLength();
    _curBlockBeginOffset = _oriFileLength;
    _curBlockIdx = _compressMeta->GetBlockCount();
    _offset = 0;
    return true;
}

size_t SnappyCompressFileReader::GetUncompressedFileLength() const noexcept(false)
{
    assert(_compressMeta);
    return _compressMeta->GetUnCompressFileLength();
}

std::shared_ptr<CompressFileMeta>
SnappyCompressFileReader::LoadCompressMeta(const std::shared_ptr<FileReader>& fileReader) noexcept(false)
{
    if (!fileReader) {
        return std::shared_ptr<CompressFileMeta>();
    }

    size_t fileLen = fileReader->GetLength();
    if (fileLen == 0) {
        return std::shared_ptr<CompressFileMeta>(new CompressFileMeta());
    }

    size_t blockCount;
    if (fileLen < (sizeof(blockCount) + sizeof(_blockSize))) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Invalid compress file[%s]", fileReader->DebugString().c_str());
        return std::shared_ptr<CompressFileMeta>();
    }
    int64_t readOffset = fileLen - sizeof(blockCount);
    fileReader->Read(&blockCount, sizeof(blockCount), readOffset).GetOrThrow();
    readOffset -= sizeof(_blockSize);
    fileReader->Read(&_blockSize, sizeof(_blockSize), readOffset).GetOrThrow();
    size_t metaLen = CompressFileMeta::GetMetaLength(blockCount);
    readOffset -= metaLen;
    if (readOffset < 0) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Invalid compress file[%s]", fileReader->DebugString().c_str());
        return std::shared_ptr<CompressFileMeta>();
    }

    vector<char> tmpBuffer(metaLen);
    if (metaLen != fileReader->Read(tmpBuffer.data(), metaLen, readOffset).GetOrThrow()) {
        return std::shared_ptr<CompressFileMeta>();
    }

    std::shared_ptr<CompressFileMeta> meta(new CompressFileMeta());
    if (!meta->Init(blockCount, tmpBuffer.data())) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Invalid compress file[%s]", fileReader->DebugString().c_str());
        return std::shared_ptr<CompressFileMeta>();
    }
    return meta;
}

bool SnappyCompressFileReader::InCurrentBlock(size_t offset) const noexcept
{
    return offset >= _curBlockBeginOffset && offset < _curBlockBeginOffset + _compressor.GetBufferOutLen();
}

bool SnappyCompressFileReader::LocateBlockOffsetInCompressFile(size_t offset, size_t& blockIdx,
                                                               size_t& uncompBlockOffset, size_t& compBlockOffset,
                                                               size_t& compBlockSize) const noexcept(false)
{
    assert(_compressMeta);
    const vector<CompressFileMeta::CompressBlockMeta>& blockMetas = _compressMeta->GetCompressBlockMetas();

    size_t beginIdx = 0;
    size_t endIdx = 0;
    if (offset >= _curBlockBeginOffset) {
        beginIdx = _curBlockIdx;
        endIdx = blockMetas.size();
    } else {
        beginIdx = 0;
        endIdx = _curBlockIdx;
    }

    for (size_t i = beginIdx; i < endIdx; ++i) {
        if (offset < blockMetas[i].uncompressEndPos) {
            blockIdx = i;
            uncompBlockOffset = (i == 0) ? 0 : blockMetas[i - 1].uncompressEndPos;
            compBlockOffset = (i == 0) ? 0 : blockMetas[i - 1].compressEndPos;
            compBlockSize = blockMetas[i].compressEndPos - compBlockOffset;
            return true;
        }
    }
    return false;
}

void SnappyCompressFileReader::LoadBuffer(size_t offset, ReadOption option) noexcept(false)
{
    assert(_dataFileReader);
    size_t uncompressBlockOffset = 0;
    size_t compressBlockOffset = 0;
    size_t compressBlockSize = 0;
    size_t curBlockIdx = 0;
    if (!LocateBlockOffsetInCompressFile(offset, curBlockIdx, uncompressBlockOffset, compressBlockOffset,
                                         compressBlockSize)) {
        INDEXLIB_FATAL_ERROR(FileIO, "locate block offset [%lu] fail!", offset);
        return;
    }

    _compressor.Reset();
    DynamicBuf& inBuffer = _compressor.GetInBuffer();
    if (compressBlockSize !=
        _dataFileReader->Read(inBuffer.getBuffer(), compressBlockSize, compressBlockOffset, option).GetOrThrow()) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "decompress file[%s] failed", _dataFileReader->DebugString().c_str());
        return;
    }

    inBuffer.movePtr(compressBlockSize);
    if (!_compressor.Decompress(nullptr, _blockSize)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "decompress file [%s] failed", _dataFileReader->DebugString().c_str());
        return;
    }
    _curBlockIdx = curBlockIdx;
    _curBlockBeginOffset = uncompressBlockOffset;
    _curInBlockOffset = offset - _curBlockBeginOffset;
}

FSResult<size_t> SnappyCompressFileReader::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    assert(_compressMeta);
    if (offset >= _oriFileLength) {
        return {FSEC_OK, 0};
    }
    _offset = offset;
    return Read(buffer, length, option);
}

FSResult<size_t> SnappyCompressFileReader::Read(void* buffer, size_t length, ReadOption option) noexcept
{
    int64_t leftLen = length;
    char* cursor = (char*)buffer;
    while (true) {
        if (!InCurrentBlock(_offset)) {
            RETURN2_IF_FS_EXCEPTION(LoadBuffer(_offset, option), (size_t)(cursor - (char*)buffer), "LoadBuffer failed");
        } else {
            _curInBlockOffset = _offset - _curBlockBeginOffset;
        }

        int64_t dataLeftInBlock = _compressor.GetBufferOutLen() - _curInBlockOffset;
        int64_t lengthToCopy = leftLen < dataLeftInBlock ? leftLen : dataLeftInBlock;

        memcpy(cursor, _compressor.GetBufferOut() + _curInBlockOffset, lengthToCopy);
        cursor += lengthToCopy;
        leftLen -= lengthToCopy;
        _offset += lengthToCopy;
        _curInBlockOffset += lengthToCopy;

        if (leftLen <= 0) {
            assert(leftLen == 0);
            return {FSEC_OK, length};
        }

        if (_offset >= _oriFileLength) {
            return {FSEC_OK, (size_t)(cursor - (char*)buffer)};
        }
    }
    return {FSEC_OK, 0};
}
}} // namespace indexlib::file_system
