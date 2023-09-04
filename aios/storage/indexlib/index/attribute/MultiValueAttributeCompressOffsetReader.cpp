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
#include "indexlib/index/attribute/MultiValueAttributeCompressOffsetReader.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/file_system/stream/FileStreamCreator.h"
#include "indexlib/index/common/data_structure/VarLenDataParam.h"
#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"
#include "indexlib/util/slice_array/BytesAlignedSliceArray.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, MultiValueAttributeCompressOffsetReader);

Status MultiValueAttributeCompressOffsetReader::Init(const std::shared_ptr<AttributeConfig>& attrConfig,
                                                     const std::shared_ptr<indexlib::file_system::IDirectory>& attrDir,
                                                     bool disableUpdate, uint32_t docCount,
                                                     std::shared_ptr<AttributeMetrics> attributeMetrics)
{
    auto varLenDataParam = VarLenDataParamHelper::MakeParamForAttribute(attrConfig);
    _docCount = docCount;
    _disableGuardOffset = varLenDataParam.disableGuardOffset;
    _attributeMetrics = attributeMetrics;

    Status status = InitFileReader(attrDir);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "init attribute file reader fail");
        return status;
    }

    uint8_t* buffer = (uint8_t*)_fileReader->GetBaseAddress();
    if (!disableUpdate && buffer != nullptr && attrConfig->IsAttributeUpdatable()) {
        // full memory load, not block cache
        status = InitSliceFileReader(attrConfig, attrDir);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "init attribute slice file reader fail. ");
            return status;
        }
    }

    size_t compressLen = 0;
    uint32_t magicTail = 0;
    status = GetCompressLenAndMagicTail(&compressLen, &magicTail);
    if (!status.IsOK()) {
        return status;
    }
    uint32_t itemCount = 0;

    if (magicTail == UINT32_OFFSET_TAIL_MAGIC) {
        if (_sliceFileReader != nullptr) {
            AUTIL_LOG(ERROR, "updatable compress offset should be u64 offset");
            return Status::Corruption("updatable compress offset should be u64 offset");
        }

        _u32CompressReader.reset(new indexlib::index::EquivalentCompressReader<uint32_t>());
        if (buffer) {
            _u32CompressReader->Init(buffer, compressLen, /*expandSliceArray*/ nullptr);
        } else {
            _u32CompressReader->Init(_fileReader);
        }
        _u32CompressSessionReader =
            _u32CompressReader->CreateSessionReader(nullptr, indexlib::index::EquivalentCompressSessionOption());
        itemCount = _u32CompressReader->Size();
    } else if (magicTail == UINT64_OFFSET_TAIL_MAGIC) {
        std::shared_ptr<indexlib::util::BytesAlignedSliceArray> byteSliceArray;
        if (_sliceFileReader) {
            byteSliceArray = _sliceFileReader->GetBytesAlignedSliceArray();
        }

        _u64CompressReader.reset(new indexlib::index::EquivalentCompressReader<uint64_t>());
        if (buffer) {
            _u64CompressReader->Init(buffer, compressLen, byteSliceArray);
        } else {
            _u64CompressReader->Init(_fileReader);
        }
        _u64CompressSessionReader =
            _u64CompressReader->CreateSessionReader(nullptr, indexlib::index::EquivalentCompressSessionOption());
        itemCount = _u64CompressReader->Size();
    } else {
        AUTIL_LOG(ERROR, "attribute compressed offset file magic tail not match");
        return Status::Corruption("attribute compressed offset file magic tail not match");
    }

    auto offsetCnt = GetOffsetCount(_docCount);
    if (offsetCnt != itemCount) {
        AUTIL_LOG(ERROR, "mismatch offset cnt, expect[%lu], real[%u]", offsetCnt, itemCount);
        return Status::Corruption("mismatch offset cnt");
    }
    InitAttributeMetrics();
    return Status::OK();
}

Status MultiValueAttributeCompressOffsetReader::InitFileReader(
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

Status MultiValueAttributeCompressOffsetReader::InitSliceFileReader(
    const std::shared_ptr<AttributeConfig>& attrConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& attrDir)
{
    if (!attrConfig->IsAttributeUpdatable() || !_fileReader->GetBaseAddress()) {
        // not allowed to update if offset is open in block cache
        return Status::OK();
    }

    std::string extendFileName =
        std::string(ATTRIBUTE_OFFSET_FILE_NAME) + ATTRIBUTE_EQUAL_COMPRESS_UPDATE_EXTEND_SUFFIX;
    auto [status, fileReader] =
        attrDir->CreateFileReader(extendFileName, indexlib::file_system::FSOT_SLICE).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create extend file reader failed, file name[%s]", extendFileName.c_str());
        return status;
    }
    if (!fileReader) {
        auto [stWriter, fileWriter] =
            attrDir
                ->CreateFileWriter(extendFileName, indexlib::file_system::WriterOption::Slice(
                                                       indexlib::index::EQUAL_COMPRESS_UPDATE_SLICE_LEN,
                                                       indexlib::index::EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT))
                .StatusWith();
        if (!stWriter.IsOK()) {
            AUTIL_LOG(ERROR, "create extend file writer failed, file name[%s]", extendFileName.c_str());
            return stWriter;
        }

        auto [stReader, extendFileReader] =
            attrDir->CreateFileReader(extendFileName, indexlib::file_system::FSOT_SLICE).StatusWith();
        if (!stReader.IsOK()) {
            AUTIL_LOG(ERROR, "create extend file reader failed, file name[%s]", extendFileName.c_str());
            return stReader;
        }

        auto stClose = fileWriter->Close().Status();
        if (!stClose.IsOK()) {
            AUTIL_LOG(ERROR, "close extend file failed, file name[%s]", extendFileName.c_str());
            return stClose;
        }
        _sliceFileReader = std::dynamic_pointer_cast<indexlib::file_system::SliceFileReader>(extendFileReader);
    } else {
        _sliceFileReader = std::dynamic_pointer_cast<indexlib::file_system::SliceFileReader>(fileReader);
    }

    return Status::OK();
}
Status MultiValueAttributeCompressOffsetReader::GetCompressLenAndMagicTail(size_t* compressLen,
                                                                           uint32_t* magicTail) const
{
    auto fileStream = indexlib::file_system::FileStreamCreator::CreateFileStream(_fileReader, nullptr);
    assert(fileStream != nullptr);
    uint64_t bufferLen = fileStream->GetStreamLength();
    assert(bufferLen >= 4);
    *compressLen = bufferLen - sizeof(uint32_t); // minus tail len
    auto [status, readed] =
        fileStream->Read(magicTail, sizeof(*magicTail), bufferLen - 4, indexlib::file_system::ReadOption())
            .StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "read magicTail from file-stream failed");
        return status;
    }
    if (readed != sizeof(*magicTail)) {
        AUTIL_LOG(ERROR, "read magic tail failed, expected length [%lu], actual [%lu]", sizeof(*magicTail), readed);
        return Status::Corruption("mismatch magic tail");
    }
    return Status::OK();
}

void MultiValueAttributeCompressOffsetReader::InitAttributeMetrics()
{
    if (_sliceFileReader != nullptr) {
        _compressMetrics = (_u64CompressReader != nullptr) ? _u64CompressReader->GetUpdateMetrics()
                                                           : _u32CompressReader->GetUpdateMetrics();
        _expandSliceLen = _sliceFileReader->GetLength();
        if (_attributeMetrics != nullptr) {
            _attributeMetrics->IncreaseequalCompressExpandFileLenValue(_expandSliceLen);
            _attributeMetrics->IncreaseequalCompressWastedBytesValue(_compressMetrics.noUsedBytesSize);
        }
    }
}
} // namespace indexlibv2::index
