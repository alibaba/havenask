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
#include "indexlib/index/attribute/MultiValueAttributeUnCompressOffsetReader.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/index/common/data_structure/VarLenDataParam.h"
#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"

using namespace indexlib::util;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, MultiValueAttributeUnCompressOffsetReader);
Status
MultiValueAttributeUnCompressOffsetReader::Init(const std::shared_ptr<AttributeConfig>& attrConfig,
                                                const std::shared_ptr<indexlib::file_system::IDirectory>& attrDir,
                                                bool disableUpdate, uint32_t docCount)
{
    auto varLenDataParam = VarLenDataParamHelper::MakeParamForAttribute(attrConfig);
    _disableGuardOffset = varLenDataParam.disableGuardOffset;
    _offsetThreshold = varLenDataParam.offsetThreshold;
    _docCount = docCount;

    Status status;
    if (disableUpdate || !attrConfig->IsAttributeUpdatable()) {
        status = InitFileReader(attrDir);
    } else {
        status = InitSliceFileReader(attrConfig, attrDir);
    }
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "init attribute file reader failed.");
        return status;
    }

    if (_fileReader->GetBaseAddress()) {
        status = InitFromBuffer((uint8_t*)_fileReader->GetBaseAddress(), _fileReader->GetLength(), _docCount);
    } else {
        status = InitFromFile(_fileReader, _docCount);
    }
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "init unCompress offset reader failed.");
        return status;
    }
    return Status::OK();
}

Status MultiValueAttributeUnCompressOffsetReader::InitFromBuffer(uint8_t* buffer, uint64_t bufferLen, uint32_t docCount)
{
    uint64_t expectOffsetCount = GetOffsetCount(docCount);
    if (bufferLen == expectOffsetCount * sizeof(uint32_t)) {
        _u32Buffer = (uint32_t*)buffer;
        _isU32Offset = true;
    } else if (bufferLen == expectOffsetCount * sizeof(uint64_t)) {
        _u64Buffer = (uint64_t*)buffer;
        _isU32Offset = false;
    } else {
        AUTIL_LOG(ERROR,
                  "Attribute offset file length is not consistent with segment info, "
                  "offset length is %ld != %ld or %ld, doc number in segment info is %d",
                  bufferLen, expectOffsetCount * sizeof(uint32_t), expectOffsetCount * sizeof(uint64_t),
                  (int32_t)docCount);
        return Status::Corruption("mismatch offset count");
    }
    _docCount = docCount;
    return Status::OK();
}

Status MultiValueAttributeUnCompressOffsetReader::InitFromFile(
    const std::shared_ptr<indexlib::file_system::FileReader>& fileReader, uint32_t docCount)
{
    uint64_t expectOffsetCount = GetOffsetCount(docCount);
    _fileStream = indexlib::file_system::FileStreamCreator::CreateConcurrencyFileStream(_fileReader, nullptr);
    auto bufferLen = _fileStream->GetStreamLength();
    if (bufferLen == expectOffsetCount * sizeof(uint32_t)) {
        _isU32Offset = true;
    } else if (bufferLen == expectOffsetCount * sizeof(uint64_t)) {
        _isU32Offset = false;
    } else {
        AUTIL_LOG(ERROR,
                  "Attribute offset file length is not consistent "
                  "with segment info, offset length is %ld, "
                  "doc number in segment info is %d",
                  bufferLen, (int32_t)docCount);
        return Status::Corruption("mismatch offset count");
    }
    _docCount = docCount;
    return Status::OK();
}

bool MultiValueAttributeUnCompressOffsetReader::ExtendU32OffsetToU64Offset()
{
    assert(_u64Buffer == nullptr);
    if (_sliceFileReader == nullptr) {
        AUTIL_LOG(ERROR, "no slice file reader for extend offset usage.");
        return false;
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
    return true;
}

void MultiValueAttributeUnCompressOffsetReader::ExtendU32OffsetToU64Offset(uint32_t* u32Offset, uint64_t* u64Offset,
                                                                           uint32_t count)
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

Status MultiValueAttributeUnCompressOffsetReader::InitFileReader(
    const std::shared_ptr<indexlib::file_system::IDirectory>& attrDir)
{
    auto option = indexlib::file_system::ReaderOption::Writable(indexlib::file_system::FSOT_LOAD_CONFIG);
    option.supportCompress = true;
    auto [status, fileReader] = attrDir->CreateFileReader(ATTRIBUTE_OFFSET_FILE_NAME, option).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create offset file reader failed");
        return status;
    }
    _fileReader = fileReader;
    return Status::OK();
}

Status MultiValueAttributeUnCompressOffsetReader::InitSliceFileReader(
    const std::shared_ptr<AttributeConfig>& attrConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& attrDir)
{
    size_t u64OffsetFileLen = sizeof(uint64_t) * (_docCount + 1);
    std::string extendFileName = std::string(ATTRIBUTE_OFFSET_FILE_NAME) + ATTRIBUTE_OFFSET_EXTEND_SUFFIX;

    auto [status, extendFileReader] =
        attrDir->CreateFileReader(extendFileName, indexlib::file_system::FSOT_SLICE).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create extend file reader failed, file name[%s]", extendFileName.c_str());
        return status;
    }
    if (extendFileReader == nullptr) {
        auto [stWriter, fileWriter] =
            attrDir->CreateFileWriter(extendFileName, indexlib::file_system::WriterOption::Slice(u64OffsetFileLen, 1))
                .StatusWith();
        if (!stWriter.IsOK()) {
            AUTIL_LOG(ERROR, "create extend file writer failed, file name[%s]", extendFileName.c_str());
            return stWriter;
        }

        auto [stReader, reader] =
            attrDir->CreateFileReader(extendFileName, indexlib::file_system::FSOT_SLICE).StatusWith();
        if (!stReader.IsOK()) {
            AUTIL_LOG(ERROR, "create extend file reader failed, file name[%s]", extendFileName.c_str());
            return stReader;
        }

        auto status = fileWriter->Close().Status();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "close extend file failed, file name[%s]", extendFileName.c_str());
            return status;
        }
        extendFileReader = reader;
    }
    size_t fileLen = extendFileReader->GetLength();
    if (fileLen > 0) {
        size_t u64OffsetFileLen = GetOffsetCount(_docCount) * sizeof(uint64_t);
        if (fileLen != u64OffsetFileLen) {
            AUTIL_LOG(ERROR,
                      "Attribute offset slice file length [%lu]"
                      " is not right, only can be 0 or %lu",
                      fileLen, u64OffsetFileLen);
            return Status::Corruption("mismatch attribute offset slice file len");
        }

        // already extend to u64
        _fileReader = extendFileReader;
    } else {
        auto status = InitFileReader(attrDir);
        if (!status.IsOK()) {
            return status;
        }
        if (_fileReader->GetBaseAddress()) {
            _sliceFileReader = std::dynamic_pointer_cast<indexlib::file_system::SliceFileReader>(extendFileReader);
        }
    }
    return Status::OK();
}

} // namespace indexlibv2::index
