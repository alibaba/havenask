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
#include "indexlib/file_system/file/SnappyCompressFileWriter.h"

#include <cstddef>
#include <stdint.h>

#include "alog/Logger.h"
#include "indexlib/file_system/file/CompressFileMeta.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, SnappyCompressFileWriter);

SnappyCompressFileWriter::SnappyCompressFileWriter() noexcept : _bufferSize(0), _writeLength(0) {}

SnappyCompressFileWriter::~SnappyCompressFileWriter() noexcept {}

void SnappyCompressFileWriter::Init(const std::shared_ptr<FileWriter>& fileWriter, size_t bufferSize) noexcept
{
    assert(fileWriter);
    _bufferSize = bufferSize;
    _writeLength = 0;
    assert(fileWriter);
    _dataWriter = fileWriter;

    _compressor.SetBufferInLen(_bufferSize);
    _compressor.SetBufferOutLen(_bufferSize);
    _compressMeta.reset(new CompressFileMeta);
}

FSResult<size_t> SnappyCompressFileWriter::Write(const void* buffer, size_t length) noexcept
{
    const char* cursor = (const char*)buffer;
    size_t leftLen = length;
    while (true) {
        uint32_t leftLenInBuffer = _bufferSize - _compressor.GetBufferInLen();
        uint32_t lengthToWrite = (leftLenInBuffer < leftLen) ? leftLenInBuffer : leftLen;
        _compressor.AddDataToBufferIn(cursor, lengthToWrite);
        cursor += lengthToWrite;
        _writeLength += lengthToWrite;
        leftLen -= lengthToWrite;
        if (leftLen <= 0) {
            assert(leftLen == 0);
            break;
        }

        if (_compressor.GetBufferInLen() == _bufferSize) {
            RETURN2_IF_FS_EXCEPTION(DumpCompressData(), length, "DumpCompressData failed");
        }
    }
    return {FSEC_OK, length};
}

ErrorCode SnappyCompressFileWriter::DoClose() noexcept
{
    if (!_compressMeta) {
        // already close
        return FSEC_OK;
    }

    if (_dataWriter) {
        RETURN_IF_FS_EXCEPTION(DumpCompressData(), "DumpCompressData failed");
        RETURN_IF_FS_EXCEPTION(DumpCompressMeta(), "DumpCompressMeta failed");
        RETURN_IF_FS_ERROR(_dataWriter->Close(), "data writer close failed");
        _compressMeta.reset();
    }
    return FSEC_OK;
}

void SnappyCompressFileWriter::DumpCompressData() noexcept(false)
{
    if (_compressor.GetBufferInLen() == 0) {
        // empty buffer
        return;
    }

    if (!_compressor.Compress()) {
        INDEXLIB_FATAL_ERROR(FileIO, "compress fail!");
        return;
    }

    assert(_dataWriter);
    size_t writeLen = _compressor.GetBufferOutLen();
    size_t ret = _dataWriter->Write(_compressor.GetBufferOut(), writeLen).GetOrThrow();
    if (ret != writeLen) {
        _compressor.Reset();
        INDEXLIB_FATAL_ERROR(FileIO, "write compress data fail!");
        return;
    }
    assert(_compressMeta);
    _compressMeta->AddOneBlock(_writeLength, _dataWriter->GetLength());
    _compressor.Reset();
}

void SnappyCompressFileWriter::DumpCompressMeta() noexcept(false)
{
    assert(_compressMeta);
    assert(_dataWriter);
    size_t compressBlockCount = _compressMeta->GetBlockCount();
    if (compressBlockCount > 0) {
        _dataWriter->Write(_compressMeta->Data(), _compressMeta->Size()).GetOrThrow();
        _dataWriter->Write(&_bufferSize, sizeof(_bufferSize)).GetOrThrow();
        _dataWriter->Write(&compressBlockCount, sizeof(compressBlockCount)).GetOrThrow();
    }
}
}} // namespace indexlib::file_system
