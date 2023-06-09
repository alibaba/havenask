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
#include "indexlib/index/common/numeric_compress/EquivalentCompressReader.h"

#include "indexlib/file_system/file/FileReader.h"

using namespace indexlib::util;

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, EquivalentCompressReaderBase);

EquivalentCompressReaderBase::EquivalentCompressReaderBase(const uint8_t* buffer, int64_t compLen)
    : _itemCount(0)
    , _slotBitNum(0)
    , _slotMask(0)
    , _slotNum(0)
    , _slotBaseAddr(NULL)
    , _valueBaseAddr(NULL)
    , _slotBaseOffset(0u)
    , _valueBaseOffset(0u)
    , _compressLen(INVALID_COMPRESS_LEN)
    , _expandDataBuffer(NULL)
    , _updateMetrics(NULL)
{
    Init(buffer, compLen, /*expandSliceArray*/ nullptr);
}

EquivalentCompressReaderBase::EquivalentCompressReaderBase()
    : _itemCount(0)
    , _slotBitNum(0)
    , _slotMask(0)
    , _slotNum(0)
    , _slotBaseAddr(NULL)
    , _valueBaseAddr(NULL)
    , _slotBaseOffset(0u)
    , _valueBaseOffset(0u)
    , _compressLen(INVALID_COMPRESS_LEN)
    , _expandDataBuffer(NULL)
    , _updateMetrics(NULL)
{
}

EquivalentCompressReaderBase::~EquivalentCompressReaderBase() { ARRAY_DELETE_AND_SET_NULL(_expandDataBuffer); }

void EquivalentCompressReaderBase::Init(const uint8_t* buffer, int64_t compLen,
                                        const std::shared_ptr<BytesAlignedSliceArray>& expandSliceArray)
{
    const uint8_t* cursor = buffer;
    _itemCount = *(uint32_t*)cursor;
    cursor += sizeof(uint32_t);
    _slotBitNum = *(uint32_t*)cursor;
    _slotMask = (1 << _slotBitNum) - 1;
    cursor += sizeof(uint32_t);
    _slotNum = (_itemCount + (1 << _slotBitNum) - 1) >> _slotBitNum;
    if (_slotNum > 0) {
        _slotBaseAddr = (uint64_t*)cursor;
        cursor += (_slotNum * sizeof(uint64_t));
        _valueBaseAddr = (uint8_t*)cursor;
    }
    InitInplaceUpdateFlags();
    _compressLen = compLen;
    _expandSliceArray = expandSliceArray;
    InitUpdateMetrics();
}

bool EquivalentCompressReaderBase::Init(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader)
{
    if (!fileReader) {
        AUTIL_LOG(ERROR, "fileReader is NULL");
        return false;
    }
    _fileReader = fileReader;
    _fileStream = indexlib::file_system::FileStreamCreator::CreateConcurrencyFileStream(fileReader, nullptr);
    size_t cursor = 0;
    auto [status, len] =
        _fileStream->Read(&_itemCount, sizeof(uint32_t), cursor, indexlib::file_system::ReadOption()).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "read item count fail from FileStream");
        return false;
    }
    if (sizeof(_itemCount) != len) {
        return false;
    }
    cursor += sizeof(_itemCount);
    std::tie(status, len) =
        _fileStream->Read(&_slotBitNum, sizeof(uint32_t), cursor, indexlib::file_system::ReadOption()).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "read slob bit num fail from FileStream");
        return false;
    }
    if (sizeof(_slotBitNum) != len) {
        return false;
    }
    cursor += sizeof(_slotBitNum);
    _slotMask = (1 << _slotBitNum) - 1;
    _slotNum = (_itemCount + (1 << _slotBitNum) - 1) >> _slotBitNum;
    if (_slotNum > 0) {
        _slotBaseOffset = cursor;
        cursor += (_slotNum * sizeof(uint64_t));
        _valueBaseOffset = cursor;
    }

    _fileContent = std::make_unique<EquivalentCompressFileContent>(
        _itemCount, _slotBitNum, _slotMask, _slotNum, _slotBaseOffset, _valueBaseOffset, _fileReader->GetLogicLength());

    return true;
}

EquivalentCompressUpdateMetrics EquivalentCompressReaderBase::GetUpdateMetrics()
{
    if (unlikely(!_updateMetrics)) {
        InitUpdateMetrics();
    }

    if (_updateMetrics) {
        return *_updateMetrics;
    }

    static EquivalentCompressUpdateMetrics emptyUpdateMetrics;
    return emptyUpdateMetrics;
}

void EquivalentCompressReaderBase::InitInplaceUpdateFlags()
{
    for (size_t i = 0; i < 8; i++) {
        for (size_t j = 0; j < 8; j++) {
            INPLACE_UPDATE_FLAG_ARRAY[i][j] = false;
        }
    }

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_BIT1][SIT_DELTA_BIT1] = true;

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_BIT2][SIT_DELTA_BIT2] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_BIT2][SIT_DELTA_BIT1] = true;

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_BIT4][SIT_DELTA_BIT4] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_BIT4][SIT_DELTA_BIT2] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_BIT4][SIT_DELTA_BIT1] = true;

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT8][SIT_DELTA_UINT8] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT8][SIT_DELTA_BIT4] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT8][SIT_DELTA_BIT2] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT8][SIT_DELTA_BIT1] = true;

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT16][SIT_DELTA_UINT16] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT16][SIT_DELTA_UINT8] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT16][SIT_DELTA_BIT4] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT16][SIT_DELTA_BIT2] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT16][SIT_DELTA_BIT1] = true;

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT32][SIT_DELTA_UINT32] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT32][SIT_DELTA_UINT16] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT32][SIT_DELTA_UINT8] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT32][SIT_DELTA_BIT4] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT32][SIT_DELTA_BIT2] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT32][SIT_DELTA_BIT1] = true;

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_UINT64] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_UINT32] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_UINT16] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_UINT8] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_BIT4] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_BIT2] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_BIT1] = true;
}

void EquivalentCompressReaderBase::InitUpdateMetrics()
{
    if (!_expandSliceArray) {
        return;
    }

    size_t sliceUsedBytes = _expandSliceArray->GetTotalUsedBytes();
    if (sliceUsedBytes == 0) {
        return;
    }

    assert(sliceUsedBytes >= sizeof(EquivalentCompressUpdateMetrics));
    _updateMetrics = (EquivalentCompressUpdateMetrics*)_expandSliceArray->GetValueAddress(0);
}

Status EquivalentCompressReaderBase::IncreaseNoUsedMemorySize(size_t noUsedBytes)
{
    if (unlikely(!_updateMetrics && _expandSliceArray)) {
        auto st = WriteUpdateMetricsToEmptySlice();
        RETURN_IF_STATUS_ERROR(st, "write update metrics failed.");
        InitUpdateMetrics();
    }

    if (_updateMetrics) {
        _updateMetrics->noUsedBytesSize = _updateMetrics->noUsedBytesSize + noUsedBytes;
    }
    return Status::OK();
}

Status EquivalentCompressReaderBase::IncreaseInplaceUpdateCount()
{
    if (unlikely(!_updateMetrics && _expandSliceArray)) {
        auto st = WriteUpdateMetricsToEmptySlice();
        RETURN_IF_STATUS_ERROR(st, "write update metrics failed.");
        InitUpdateMetrics();
    }

    if (_updateMetrics) {
        _updateMetrics->inplaceUpdateCount++;
    }
    return Status::OK();
}

Status EquivalentCompressReaderBase::IncreaseExpandUpdateCount()
{
    if (unlikely(!_updateMetrics && _expandSliceArray)) {
        auto st = WriteUpdateMetricsToEmptySlice();
        RETURN_IF_STATUS_ERROR(st, "write update metrics failed.");
        InitUpdateMetrics();
    }

    if (_updateMetrics) {
        _updateMetrics->expandUpdateCount++;
    }
    return Status::OK();
}

Status EquivalentCompressReaderBase::WriteUpdateMetricsToEmptySlice()
{
    if (_expandSliceArray->GetTotalUsedBytes() != 0) {
        return Status::OK();
    }

    EquivalentCompressUpdateMetrics updateMetrics;
    int64_t offset = _expandSliceArray->Insert(&updateMetrics, sizeof(EquivalentCompressUpdateMetrics));
    if (offset != 0) {
        AUTIL_LOG(ERROR, "noUsedMemoryBytes should store in slice array offset 0!");
        return Status::Corruption("noUsedMemoryBytes should store in slice array offset 0!");
    }
    return Status::OK();
}
} // namespace indexlib::index
