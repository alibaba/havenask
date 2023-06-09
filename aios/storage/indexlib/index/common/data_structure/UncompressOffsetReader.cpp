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
#include "indexlib/index/common/data_structure/UncompressOffsetReader.h"

#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/index/common/Constant.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, UncompressOffsetReader);

UncompressOffsetReader::UncompressOffsetReader()
    : _u64Buffer(NULL)
    , _u32Buffer(NULL)
    , _docCount(0)
    , _isU32Offset(false)
    , _disableGuardOffset(false)
    , _offsetThreshold(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD)
{
}

UncompressOffsetReader::UncompressOffsetReader(uint64_t offsetExtendThreshold, bool disableGuardOffset)
    : _u64Buffer(NULL)
    , _u32Buffer(NULL)
    , _docCount(0)
    , _isU32Offset(false)
    , _disableGuardOffset(disableGuardOffset)
    , _offsetThreshold(offsetExtendThreshold)
{
}

UncompressOffsetReader::~UncompressOffsetReader() {}

Status UncompressOffsetReader::Init(uint32_t docCount,
                                    const std::shared_ptr<indexlib::file_system::FileReader>& fileReader,
                                    const std::shared_ptr<indexlib::file_system::SliceFileReader>& u64SliceFileReader)
{
    size_t fileLen = (u64SliceFileReader != nullptr) ? u64SliceFileReader->GetLength() : 0;
    if (fileLen > 0) {
        size_t u64OffsetFileLen = GetOffsetCount(docCount) * sizeof(uint64_t);
        if (fileLen != u64OffsetFileLen) {
            RETURN_STATUS_ERROR(InternalError,
                                "Attribute offset slice file length [%lu]"
                                " is not right, only can be 0 or %lu",
                                fileLen, u64OffsetFileLen);
        }
        // already extend to u64
        _fileReader = u64SliceFileReader;
        _sliceFileReader.reset();
    } else {
        assert(fileReader);
        _fileReader = fileReader;
        _sliceFileReader = u64SliceFileReader;
    }
    if (_fileReader->GetBaseAddress()) {
        return InitFromBuffer((uint8_t*)_fileReader->GetBaseAddress(), _fileReader->GetLength(), docCount);
    }
    return InitFromFile(_fileReader, docCount);
}

Status UncompressOffsetReader::InitFromBuffer(uint8_t* buffer, uint64_t bufferLen, uint32_t docCount)
{
    uint64_t expectOffsetCount = GetOffsetCount(docCount);
    if (bufferLen == expectOffsetCount * sizeof(uint32_t)) {
        _u32Buffer = (uint32_t*)buffer;
        _isU32Offset = true;
    } else if (bufferLen == expectOffsetCount * sizeof(uint64_t)) {
        _u64Buffer = (uint64_t*)buffer;
        _isU32Offset = false;
    } else {
        RETURN_STATUS_ERROR(InternalError,
                            "Attribute offset file length is not consistent with segment info, "
                            "offset length is %ld != %ld or %ld, doc number in segment info is %d",
                            bufferLen, expectOffsetCount * sizeof(uint32_t), expectOffsetCount * sizeof(uint64_t),
                            (int32_t)docCount);
    }
    _docCount = docCount;
    return Status::OK();
}

Status UncompressOffsetReader::InitFromFile(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader,
                                            uint32_t docCount)
{
    uint64_t expectOffsetCount = GetOffsetCount(docCount);
    _fileStream = indexlib::file_system::FileStreamCreator::CreateConcurrencyFileStream(_fileReader, nullptr);
    auto bufferLen = _fileStream->GetStreamLength();
    if (bufferLen == expectOffsetCount * sizeof(uint32_t)) {
        _isU32Offset = true;
    } else if (bufferLen == expectOffsetCount * sizeof(uint64_t)) {
        _isU32Offset = false;
    } else {
        RETURN_STATUS_ERROR(InternalError,
                            "Attribute offset file length is not consistent "
                            "with segment info, offset length is %ld, "
                            "doc number in segment info is %d",
                            bufferLen, (int32_t)docCount);
    }
    _docCount = docCount;
    return Status::OK();
}

Status UncompressOffsetReader::ExtendU32OffsetToU64Offset()
{
    if (_u64Buffer) {
        return Status::OK();
    }

    if (!_sliceFileReader) {
        RETURN_STATUS_ERROR(InternalError, "no slice file reader for extend offset usage.");
    }

    auto sliceArray = _sliceFileReader->GetBytesAlignedSliceArray();
    uint32_t bufferItemCount = 64 * 1024;
    // add extend buffer to decrease call Insert interface
    std::vector<uint64_t> buffer;
    buffer.resize(bufferItemCount);
    uint64_t* extendOffsetBuffer = (uint64_t*)buffer.data();

    uint32_t remainCount = GetOffsetCount(_docCount);
    uint32_t* u32Begin = _u32Buffer;
    while (remainCount > 0) {
        uint32_t convertNum = bufferItemCount <= remainCount ? bufferItemCount : remainCount;
        ExtendU32OffsetToU64Offset(u32Begin, extendOffsetBuffer, convertNum);
        sliceArray->Insert(extendOffsetBuffer, convertNum * sizeof(uint64_t));
        remainCount -= convertNum;
        u32Begin += convertNum;
    }
    _u64Buffer = (uint64_t*)_sliceFileReader->GetBaseAddress();
    _isU32Offset = false;
    return Status::OK();
}

void UncompressOffsetReader::ExtendU32OffsetToU64Offset(uint32_t* u32Offset, uint64_t* u64Offset, uint32_t count)
{
    const uint32_t pipeLineNum = 8;
    uint32_t processMod = count % pipeLineNum;
    uint32_t processSize = count - processMod;
    uint32_t pos = 0;
    for (; pos < processSize; pos += pipeLineNum) {
        u64Offset[pos] = u32Offset[pos];
        u64Offset[pos + 1] = u32Offset[pos + 1];
        u64Offset[pos + 2] = u32Offset[pos + 2];
        u64Offset[pos + 3] = u32Offset[pos + 3];
        u64Offset[pos + 4] = u32Offset[pos + 4];
        u64Offset[pos + 5] = u32Offset[pos + 5];
        u64Offset[pos + 6] = u32Offset[pos + 6];
        u64Offset[pos + 7] = u32Offset[pos + 7];
    }
    for (; pos < count; ++pos) {
        u64Offset[pos] = u32Offset[pos];
    }
}

} // namespace indexlibv2::index
