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
#include <deque>
#include <memory>

#include "indexlib/base/Define.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlibv2::index {
class AttributeConfig;

class MultiValueAttributeDataFormatter
{
public:
    MultiValueAttributeDataFormatter();
    ~MultiValueAttributeDataFormatter() = default;

public:
    void Init(const std::shared_ptr<AttributeConfig>& attrConfig);

    uint32_t GetDataLength(const uint8_t* data, bool& isNull) const __ALWAYS_INLINE;

    Status GetDataLengthFromStream(const std::shared_ptr<indexlib::file_system::FileStream>& stream, size_t offset,
                                   bool& isNull, uint32_t& length) const;

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    BatchGetDataLenghFromStream(const std::shared_ptr<indexlib::file_system::FileStream>& stream,
                                const std::vector<size_t>& offsets, autil::mem_pool::Pool* sessionPool,
                                indexlib::file_system::ReadOption readOption,
                                std::vector<size_t>* dataLength) const noexcept;

private:
    uint32_t GetNormalAttrDataLength(const uint8_t* data, bool& isNull) const __ALWAYS_INLINE;
    uint32_t GetMultiStringAttrDataLength(const uint8_t* data, bool& isNull) const __ALWAYS_INLINE;
    Status GetMultiStringAttrDataLengthFromStream(const std::shared_ptr<indexlib::file_system::FileStream>& stream,
                                                  size_t offset, bool& isNull, uint32_t& length) const;
    Status GetNormalAttrDataLengthFromStream(const std::shared_ptr<indexlib::file_system::FileStream>& stream,
                                             size_t offset, bool& isNull, uint32_t& length) const;

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    BatchGetNormalLengthFromStream(const std::shared_ptr<indexlib::file_system::FileStream>& stream,
                                   const std::vector<size_t>& offsets, autil::mem_pool::Pool* sessionPool,
                                   indexlib::file_system::ReadOption readOption,
                                   std::vector<size_t>* dataLength) const noexcept;
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    BatchGetStringLengthFromStream(const std::shared_ptr<indexlib::file_system::FileStream>& stream,
                                   const std::vector<size_t>& offsets, autil::mem_pool::Pool* sessionPool,
                                   indexlib::file_system::ReadOption readOption,
                                   std::vector<size_t>* dataLength) const noexcept;

private:
    bool _isMultiString;
    uint32_t _fieldSizeShift;
    int32_t _fixedValueCount;
    uint32_t _fixedValueLength;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////
inline uint32_t MultiValueAttributeDataFormatter::GetDataLength(const uint8_t* data, bool& isNull) const
{
    if (unlikely(_isMultiString)) {
        return GetMultiStringAttrDataLength(data, isNull);
    }
    return GetNormalAttrDataLength(data, isNull);
}

inline Status MultiValueAttributeDataFormatter::GetDataLengthFromStream(
    const std::shared_ptr<indexlib::file_system::FileStream>& stream, size_t offset, bool& isNull,
    uint32_t& length) const
{
    if (unlikely(_isMultiString)) {
        return GetMultiStringAttrDataLengthFromStream(stream, offset, isNull, length);
    }
    return GetNormalAttrDataLengthFromStream(stream, offset, isNull, length);
}

inline uint32_t MultiValueAttributeDataFormatter::GetNormalAttrDataLength(const uint8_t* data, bool& isNull) const
{
    if (_fixedValueCount != -1) {
        isNull = false;
        return _fixedValueLength;
    }

    size_t encodeCountLen = 0;
    uint32_t count = MultiValueAttributeFormatter::DecodeCount((const char*)data, encodeCountLen, isNull);
    if (isNull) {
        return encodeCountLen;
    }
    return encodeCountLen + (count << _fieldSizeShift);
}

inline Status MultiValueAttributeDataFormatter::GetNormalAttrDataLengthFromStream(
    const std::shared_ptr<indexlib::file_system::FileStream>& stream, size_t offset, bool& isNull,
    uint32_t& length) const
{
    if (_fixedValueCount != -1) {
        isNull = false;
        length = _fixedValueLength;
        return Status::OK();
    }
    char buffer[4];
    size_t toRead = std::min((size_t)4, stream->GetStreamLength() - offset);
    indexlib::file_system::ReadOption option;
    option.advice = indexlib::file_system::IO_ADVICE_LOW_LATENCY;
    auto [status, len] = stream->Read(buffer, toRead, offset, option).StatusWith();
    if (!status.IsOK()) {
        return status;
    }
    size_t encodeCountLen = 0;
    uint32_t count = MultiValueAttributeFormatter::DecodeCount((const char*)buffer, encodeCountLen, isNull);
    if (isNull) {
        length = encodeCountLen;
        return Status::OK();
    }
    length = encodeCountLen + (count << _fieldSizeShift);
    return Status::OK();
}
// structure: | encodeLen | offsetByteCnt | multi-offset | multi-data |
// data stored as: | varNumLen | dataBody |
inline uint32_t MultiValueAttributeDataFormatter::GetMultiStringAttrDataLength(const uint8_t* data, bool& isNull) const
{
    const char* docBeginAddr = (const char*)data;
    // TODO: check fixed value count ?
    size_t encodeCountLen = 0;
    uint32_t valueCount = MultiValueAttributeFormatter::DecodeCount(docBeginAddr, encodeCountLen, isNull);
    if (valueCount == 0 || isNull) {
        return encodeCountLen;
    }
    const uint64_t offsetByteCnt = *(uint8_t*)(docBeginAddr + encodeCountLen);
    const size_t offsetBeginCursor = encodeCountLen + sizeof(uint8_t);
    const size_t dataBeginCursor = offsetBeginCursor + valueCount * offsetByteCnt;
    const uint32_t lastItemOffsetCursor =
        MultiValueAttributeFormatter::GetOffset(docBeginAddr + offsetBeginCursor, offsetByteCnt, valueCount - 1);
    const size_t lastItemDataCursor = dataBeginCursor + lastItemOffsetCursor;
    size_t lastItemDataLen = 0;
    bool tmpIsNull = false;
    uint32_t lastItemDataSize =
        MultiValueAttributeFormatter::DecodeCount(docBeginAddr + lastItemDataCursor, lastItemDataLen, tmpIsNull);
    assert(!tmpIsNull);
    return lastItemDataCursor + lastItemDataLen + lastItemDataSize;
}

inline Status MultiValueAttributeDataFormatter::GetMultiStringAttrDataLengthFromStream(
    const std::shared_ptr<indexlib::file_system::FileStream>& stream, size_t offset, bool& isNull,
    uint32_t& length) const
{
    indexlib::file_system::ReadOption option;
    option.advice = indexlib::file_system::IO_ADVICE_LOW_LATENCY;
    char buffer[8];
    const char* docBeginAddr = buffer;
    // TODO: check fixed value count ?
    size_t expectedReaded = std::min(8 * sizeof(char), stream->GetStreamLength() - offset);
    auto [status0, len0] = stream->Read(buffer, expectedReaded, offset, option).StatusWith();
    if (!status0.IsOK()) {
        return status0;
    }
    // check failed
    size_t encodeCountLen = 0;
    uint32_t valueCount = MultiValueAttributeFormatter::DecodeCount(docBeginAddr, encodeCountLen, isNull);
    if (valueCount == 0 || isNull) {
        length = encodeCountLen;
        return Status::OK();
    }
    assert(encodeCountLen <= 4);
    uint32_t offsetByteCnt = *(uint8_t*)(docBeginAddr + encodeCountLen);
    const size_t offsetBeginCursor = encodeCountLen + sizeof(uint8_t);
    const size_t dataBeginCursor = offsetBeginCursor + valueCount * offsetByteCnt;
    expectedReaded = std::min(
        stream->GetStreamLength() - (offset + offsetBeginCursor + offsetByteCnt * (valueCount - 1)), 8 * sizeof(char));
    auto [status1, len1] =
        stream->Read(buffer, expectedReaded, offset + offsetBeginCursor + offsetByteCnt * (valueCount - 1), option)
            .StatusWith();
    if (!status1.IsOK()) {
        return status1;
    }
    uint32_t lastOffsetFromDataBegin = 0;
    switch (offsetByteCnt) {
    case 1:
        lastOffsetFromDataBegin = *((const uint8_t*)buffer);
        break;
    case 2:
        lastOffsetFromDataBegin = *((const uint16_t*)buffer);
        break;
    case 4:
        lastOffsetFromDataBegin = *((const uint32_t*)buffer);
        break;
    default:
        assert(false);
    }

    const size_t lastItemDataCursor = dataBeginCursor + lastOffsetFromDataBegin;
    size_t lastItemDataLen = 0;
    bool tmpIsNull = false;
    expectedReaded = std::min(8 * sizeof(char), stream->GetStreamLength() - (offset + lastItemDataCursor));
    auto [status2, len2] = stream->Read(buffer, expectedReaded, offset + lastItemDataCursor, option).StatusWith();
    if (!status2.IsOK()) {
        return status2;
    }
    uint32_t lastItemDataSize = MultiValueAttributeFormatter::DecodeCount(buffer, lastItemDataLen, tmpIsNull);
    assert(!tmpIsNull);
    length = lastItemDataCursor + lastItemDataLen + lastItemDataSize;
    return Status::OK();
}

inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
MultiValueAttributeDataFormatter::BatchGetStringLengthFromStream(
    const std::shared_ptr<indexlib::file_system::FileStream>& stream, const std::vector<size_t>& offsets,
    autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption readOption,
    std::vector<size_t>* dataLengthsPtr) const noexcept
{
    assert(dataLengthsPtr);
    std::vector<size_t>& dataLengths = *dataLengthsPtr;
    dataLengths.assign(offsets.size(), 0);
    indexlib::index::ErrorCodeVec ec(offsets.size(), indexlib::index::ErrorCode::Runtime);
    std::deque<bool> filled(offsets.size(), false);
    char* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(sessionPool, char, 8 * offsets.size());
    // read count
    indexlib::file_system::BatchIO batchIO, valueBatchIO;
    valueBatchIO.reserve(offsets.size());
    batchIO.reserve(offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        size_t toRead = std::min((size_t)8, stream->GetStreamLength() - offsets[i]);
        batchIO.emplace_back(buffer + i * 8, toRead, offsets[i]);
    }
    auto countResult = co_await stream->BatchRead(batchIO, readOption);

    for (size_t i = 0; i < countResult.size(); ++i) {
        if (countResult[i].OK()) {
            size_t encodeCountLen = 0;
            bool isNull;
            uint32_t valueCount =
                MultiValueAttributeFormatter::DecodeCount((const char*)batchIO[i].buffer, encodeCountLen, isNull);
            if (valueCount == 0 || isNull) {
                dataLengths[i] += encodeCountLen;
                ec[i] = indexlib::index::ErrorCode::OK;
                filled[i] = true;
            } else {
                assert(encodeCountLen <= 4);
                dataLengths[i] += encodeCountLen;
                uint32_t offsetItemLen = *((uint8_t*)batchIO[i].buffer + encodeCountLen);
                dataLengths[i] += sizeof(uint8_t);
                dataLengths[i] += valueCount * offsetItemLen;
                size_t offset = offsets[i] + encodeCountLen + sizeof(uint8_t) + (valueCount - 1) * offsetItemLen;
                valueBatchIO.emplace_back(buffer + i * 8, offsetItemLen, offset);
            }
        } else {
            ec[i] = indexlib::index::ConvertFSErrorCode(countResult[i].ec);
            filled[i] = true;
        }
    }
    if (!valueBatchIO.empty()) {
        auto valueResult = co_await stream->BatchRead(valueBatchIO, readOption);
        size_t valueIdx = 0;
        for (size_t i = 0; i < valueResult.size(); ++i) {
            while (filled[valueIdx]) {
                valueIdx++;
                assert(valueIdx < filled.size());
            }
            if (!valueResult[i].OK()) {
                while (valueIdx < filled.size()) {
                    ec[valueIdx++] = indexlib::index::ConvertFSErrorCode(valueResult[i].ec);
                }
                IE_POOL_COMPATIBLE_DELETE_VECTOR(sessionPool, buffer, 8 * offsets.size());
                co_return ec;
            }
            uint32_t lastOffsetFromDataBegin = 0;
            switch (valueBatchIO[i].len) {
            case 1:
                lastOffsetFromDataBegin = *((const uint8_t*)valueBatchIO[i].buffer);
                break;
            case 2:
                lastOffsetFromDataBegin = *((const uint16_t*)valueBatchIO[i].buffer);
                break;
            case 4:
                lastOffsetFromDataBegin = *((const uint32_t*)valueBatchIO[i].buffer);
                break;
            default:
                assert(false);
            }
            dataLengths[valueIdx++] += lastOffsetFromDataBegin;
            valueBatchIO[i].offset += (valueBatchIO[i].len + lastOffsetFromDataBegin);
            valueBatchIO[i].len = std::min(size_t(8), stream->GetStreamLength() - valueBatchIO[i].offset);
        }
        auto finalIoResult = co_await stream->BatchRead(valueBatchIO, readOption);
        valueIdx = 0;
        for (size_t i = 0; i < valueResult.size(); ++i) {
            while (filled[valueIdx]) {
                valueIdx++;
                assert(valueIdx < filled.size());
            }
            if (finalIoResult[i].OK()) {
                bool tmpIsNull;
                size_t lastItemCountLen;
                uint32_t lastItemLen = MultiValueAttributeFormatter::DecodeCount((const char*)valueBatchIO[i].buffer,
                                                                                 lastItemCountLen, tmpIsNull);
                dataLengths[valueIdx] += (lastItemCountLen + lastItemLen);
                ec[valueIdx] = indexlib::index::ErrorCode::OK;
            } else {
                ec[valueIdx] = indexlib::index::ConvertFSErrorCode(finalIoResult[i].ec);
            }
            filled[valueIdx] = true;
        }
    }
    IE_POOL_COMPATIBLE_DELETE_VECTOR(sessionPool, buffer, 8 * offsets.size());
    co_return ec;
}
inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
MultiValueAttributeDataFormatter::BatchGetNormalLengthFromStream(
    const std::shared_ptr<indexlib::file_system::FileStream>& stream, const std::vector<size_t>& offsets,
    autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption readOption,
    std::vector<size_t>* dataLengthsPtr) const noexcept
{
    assert(dataLengthsPtr);
    std::vector<size_t>& dataLengths = *dataLengthsPtr;
    indexlib::index::ErrorCodeVec ec(offsets.size(), indexlib::index::ErrorCode::OK);
    if (_fixedValueCount != -1) {
        dataLengths.assign(offsets.size(), _fixedValueLength);
    } else {
        dataLengths.resize(offsets.size());
        char* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(sessionPool, char, 4 * offsets.size());
        indexlib::file_system::BatchIO batchIO;
        batchIO.reserve(offsets.size());
        for (size_t i = 0; i < offsets.size(); ++i) {
            size_t toRead = std::min(size_t(4), stream->GetStreamLength() - offsets[i]);
            batchIO.emplace_back(buffer + i * 4, toRead, offsets[i]);
        }
        auto batchResult = co_await stream->BatchRead(batchIO, readOption);
        assert(batchResult.size() == batchIO.size());
        for (size_t i = 0; i < batchResult.size(); ++i) {
            if (batchResult[i].OK()) {
                size_t encodeCountLen = 0;
                bool isNull;
                uint32_t count =
                    MultiValueAttributeFormatter::DecodeCount((const char*)batchIO[i].buffer, encodeCountLen, isNull);
                if (isNull) {
                    dataLengths[i] = encodeCountLen;
                } else {
                    dataLengths[i] = encodeCountLen + (count << _fieldSizeShift);
                }
            } else {
                ec[i] = indexlib::index::ConvertFSErrorCode(batchResult[i].ec);
            }
        }
        IE_POOL_COMPATIBLE_DELETE_VECTOR(sessionPool, buffer, 4 * offsets.size());
    }
    co_return ec;
}

inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
MultiValueAttributeDataFormatter::BatchGetDataLenghFromStream(
    const std::shared_ptr<indexlib::file_system::FileStream>& stream, const std::vector<size_t>& offsets,
    autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption readOption,
    std::vector<size_t>* dataLengths) const noexcept
{
    assert(dataLengths);
    if (unlikely(_isMultiString)) {
        co_return co_await BatchGetStringLengthFromStream(stream, offsets, sessionPool, readOption, dataLengths);
    }
    co_return co_await BatchGetNormalLengthFromStream(stream, offsets, sessionPool, readOption, dataLengths);
}

} // namespace indexlibv2::index
