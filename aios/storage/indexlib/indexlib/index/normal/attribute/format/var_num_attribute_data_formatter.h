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

#include <memory>

#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class VarNumAttributeDataFormatter
{
public:
    VarNumAttributeDataFormatter();
    ~VarNumAttributeDataFormatter();

public:
    void Init(const config::AttributeConfigPtr& attrConfig);

    uint32_t GetDataLength(const uint8_t* data, bool& isNull) const __ALWAYS_INLINE;

    uint32_t GetDataLengthFromStream(const std::shared_ptr<file_system::FileStream>& stream, size_t offset,
                                     bool& isNull) const;

    future_lite::coro::Lazy<index::ErrorCodeVec>
    BatchGetDataLenghFromStream(const std::shared_ptr<file_system::FileStream>& stream,
                                const std::vector<size_t>& offsets, autil::mem_pool::Pool* sessionPool,
                                file_system::ReadOption readOption, std::vector<size_t>* dataLength) const noexcept;

private:
    uint32_t GetNormalAttrDataLength(const uint8_t* data, bool& isNull) const __ALWAYS_INLINE;
    uint32_t GetMultiStringAttrDataLength(const uint8_t* data, bool& isNull) const __ALWAYS_INLINE;
    uint32_t GetMultiStringAttrDataLengthFromStream(const std::shared_ptr<file_system::FileStream>& stream,
                                                    size_t offset, bool& isNull) const;
    uint32_t GetNormalAttrDataLengthFromStream(const std::shared_ptr<file_system::FileStream>& stream, size_t offset,
                                               bool& isNull) const;

    future_lite::coro::Lazy<index::ErrorCodeVec>
    BatchGetNormalLengthFromStream(const std::shared_ptr<file_system::FileStream>& stream,
                                   const std::vector<size_t>& offsets, autil::mem_pool::Pool* sessionPool,
                                   file_system::ReadOption readOption, std::vector<size_t>* dataLength) const noexcept;
    future_lite::coro::Lazy<index::ErrorCodeVec>
    BatchGetStringLengthFromStream(const std::shared_ptr<file_system::FileStream>& stream,
                                   const std::vector<size_t>& offsets, autil::mem_pool::Pool* sessionPool,
                                   file_system::ReadOption readOption, std::vector<size_t>* dataLength) const noexcept;

private:
    bool mIsMultiString;
    uint32_t mFieldSizeShift;
    int32_t mFixedValueCount;
    uint32_t mFixedValueLength;

private:
    friend class VarNumAttributeDataFormatterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarNumAttributeDataFormatter);

///////////////////////////////////////////////////////////////////////
inline uint32_t VarNumAttributeDataFormatter::GetDataLength(const uint8_t* data, bool& isNull) const
{
    if (unlikely(mIsMultiString)) {
        return GetMultiStringAttrDataLength(data, isNull);
    }
    return GetNormalAttrDataLength(data, isNull);
}

inline uint32_t
VarNumAttributeDataFormatter::GetDataLengthFromStream(const std::shared_ptr<file_system::FileStream>& stream,
                                                      size_t offset, bool& isNull) const
{
    if (unlikely(mIsMultiString)) {
        return GetMultiStringAttrDataLengthFromStream(stream, offset, isNull);
    }
    return GetNormalAttrDataLengthFromStream(stream, offset, isNull);
}

inline uint32_t VarNumAttributeDataFormatter::GetNormalAttrDataLength(const uint8_t* data, bool& isNull) const
{
    if (mFixedValueCount != -1) {
        isNull = false;
        return mFixedValueLength;
    }

    size_t encodeCountLen = 0;
    uint32_t count = common::VarNumAttributeFormatter::DecodeCount((const char*)data, encodeCountLen, isNull);
    if (isNull) {
        return encodeCountLen;
    }
    return encodeCountLen + (count << mFieldSizeShift);
}

inline uint32_t
VarNumAttributeDataFormatter::GetNormalAttrDataLengthFromStream(const std::shared_ptr<file_system::FileStream>& stream,
                                                                size_t offset, bool& isNull) const
{
    if (mFixedValueCount != -1) {
        isNull = false;
        return mFixedValueLength;
    }

    char buffer[4];
    size_t toRead = std::min((size_t)4, stream->GetStreamLength() - offset);
    file_system::ReadOption option;
    option.advice = file_system::IO_ADVICE_LOW_LATENCY;
    stream->Read(buffer, toRead, offset, option).GetOrThrow();
    size_t encodeCountLen = 0;
    uint32_t count = common::VarNumAttributeFormatter::DecodeCount((const char*)buffer, encodeCountLen, isNull);
    if (isNull) {
        return encodeCountLen;
    }
    return encodeCountLen + (count << mFieldSizeShift);
}

inline uint32_t VarNumAttributeDataFormatter::GetMultiStringAttrDataLength(const uint8_t* data, bool& isNull) const
{
    const char* docBeginAddr = (const char*)data;
    // TODO: check fixed value count ?
    size_t encodeCountLen = 0;
    uint32_t valueCount = common::VarNumAttributeFormatter::DecodeCount(docBeginAddr, encodeCountLen, isNull);
    if (valueCount == 0 || isNull) {
        return encodeCountLen;
    }
    uint32_t offsetItemLen = *(uint8_t*)(docBeginAddr + encodeCountLen);
    size_t offsetDataOffset = encodeCountLen + sizeof(uint8_t);
    size_t dataOffset = offsetDataOffset + valueCount * offsetItemLen;
    uint32_t lastOffsetFromDataBegin =
        common::VarNumAttributeFormatter::GetOffset(docBeginAddr + offsetDataOffset, offsetItemLen, valueCount - 1);
    size_t lastItemOffset = dataOffset + lastOffsetFromDataBegin;

    size_t lastItemCountLen;

    bool tmpIsNull = false;
    uint32_t lastItemLen =
        common::VarNumAttributeFormatter::DecodeCount(docBeginAddr + lastItemOffset, lastItemCountLen, tmpIsNull);
    assert(!tmpIsNull);
    return lastItemOffset + lastItemCountLen + lastItemLen;
}

inline uint32_t VarNumAttributeDataFormatter::GetMultiStringAttrDataLengthFromStream(
    const std::shared_ptr<file_system::FileStream>& stream, size_t offset, bool& isNull) const
{
    file_system::ReadOption option;
    option.advice = file_system::IO_ADVICE_LOW_LATENCY;
    char buffer[8];
    const char* docBeginAddr = buffer;
    // TODO: check fixed value count ?
    size_t expectedReaded = std::min(8 * sizeof(char), stream->GetStreamLength() - offset);
    size_t readed = stream->Read(buffer, expectedReaded, offset, option).GetOrThrow();
    (void)readed;
    // check failed
    size_t encodeCountLen = 0;
    uint32_t valueCount = common::VarNumAttributeFormatter::DecodeCount(docBeginAddr, encodeCountLen, isNull);
    if (valueCount == 0 || isNull) {
        return encodeCountLen;
    }
    assert(encodeCountLen <= 4);
    uint32_t offsetItemLen = *(uint8_t*)(docBeginAddr + encodeCountLen);
    size_t offsetDataOffset = encodeCountLen + sizeof(uint8_t);
    size_t dataOffset = offsetDataOffset + valueCount * offsetItemLen;
    expectedReaded = std::min(
        stream->GetStreamLength() - (offset + offsetDataOffset + offsetItemLen * (valueCount - 1)), 8 * sizeof(char));
    stream->Read(buffer, expectedReaded, offset + offsetDataOffset + offsetItemLen * (valueCount - 1), option)
        .GetOrThrow();
    uint32_t lastOffsetFromDataBegin = 0;
    switch (offsetItemLen) {
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

    size_t lastItemOffset = dataOffset + lastOffsetFromDataBegin;
    size_t lastItemCountLen;
    bool tmpIsNull = false;
    expectedReaded = std::min(8 * sizeof(char), stream->GetStreamLength() - (offset + lastItemOffset));
    stream->Read(buffer, expectedReaded, offset + lastItemOffset, option).GetOrThrow();
    uint32_t lastItemLen = common::VarNumAttributeFormatter::DecodeCount(buffer, lastItemCountLen, tmpIsNull);
    assert(!tmpIsNull);
    return lastItemOffset + lastItemCountLen + lastItemLen;
}

inline future_lite::coro::Lazy<index::ErrorCodeVec> VarNumAttributeDataFormatter::BatchGetStringLengthFromStream(
    const std::shared_ptr<file_system::FileStream>& stream, const std::vector<size_t>& offsets,
    autil::mem_pool::Pool* sessionPool, file_system::ReadOption readOption,
    std::vector<size_t>* dataLengthsPtr) const noexcept
{
    assert(dataLengthsPtr);
    std::vector<size_t>& dataLengths = *dataLengthsPtr;
    dataLengths.assign(offsets.size(), 0);
    index::ErrorCodeVec ec(offsets.size(), index::ErrorCode::Runtime);
    std::deque<bool> filled(offsets.size(), false);
    char* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(sessionPool, char, 8 * offsets.size());
    // read count
    file_system::BatchIO batchIO, valueBatchIO;
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
                common::VarNumAttributeFormatter::DecodeCount((const char*)batchIO[i].buffer, encodeCountLen, isNull);
            if (valueCount == 0 || isNull) {
                dataLengths[i] += encodeCountLen;
                ec[i] = index::ErrorCode::OK;
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
            ec[i] = index::ConvertFSErrorCode(countResult[i].ec);
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
                    ec[valueIdx++] = index::ConvertFSErrorCode(valueResult[i].ec);
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
                uint32_t lastItemLen = common::VarNumAttributeFormatter::DecodeCount(
                    (const char*)valueBatchIO[i].buffer, lastItemCountLen, tmpIsNull);
                dataLengths[valueIdx] += (lastItemCountLen + lastItemLen);
                ec[valueIdx] = index::ErrorCode::OK;
            } else {
                ec[valueIdx] = index::ConvertFSErrorCode(finalIoResult[i].ec);
            }
            filled[valueIdx] = true;
        }
    }
    IE_POOL_COMPATIBLE_DELETE_VECTOR(sessionPool, buffer, 8 * offsets.size());
    co_return ec;
}
inline future_lite::coro::Lazy<index::ErrorCodeVec> VarNumAttributeDataFormatter::BatchGetNormalLengthFromStream(
    const std::shared_ptr<file_system::FileStream>& stream, const std::vector<size_t>& offsets,
    autil::mem_pool::Pool* sessionPool, file_system::ReadOption readOption,
    std::vector<size_t>* dataLengthsPtr) const noexcept
{
    assert(dataLengthsPtr);
    std::vector<size_t>& dataLengths = *dataLengthsPtr;
    index::ErrorCodeVec ec(offsets.size(), index::ErrorCode::OK);
    if (mFixedValueCount != -1) {
        dataLengths.assign(offsets.size(), mFixedValueLength);
    } else {
        dataLengths.resize(offsets.size());
        char* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(sessionPool, char, 4 * offsets.size());
        file_system::BatchIO batchIO;
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
                uint32_t count = common::VarNumAttributeFormatter::DecodeCount((const char*)batchIO[i].buffer,
                                                                               encodeCountLen, isNull);
                if (isNull) {
                    dataLengths[i] = encodeCountLen;
                } else {
                    dataLengths[i] = encodeCountLen + (count << mFieldSizeShift);
                }
            } else {
                ec[i] = index::ConvertFSErrorCode(batchResult[i].ec);
            }
        }
        IE_POOL_COMPATIBLE_DELETE_VECTOR(sessionPool, buffer, 4 * offsets.size());
    }
    co_return ec;
}
inline future_lite::coro::Lazy<index::ErrorCodeVec> VarNumAttributeDataFormatter::BatchGetDataLenghFromStream(
    const std::shared_ptr<file_system::FileStream>& stream, const std::vector<size_t>& offsets,
    autil::mem_pool::Pool* sessionPool, file_system::ReadOption readOption,
    std::vector<size_t>* dataLengths) const noexcept
{
    assert(dataLengths);
    if (unlikely(mIsMultiString)) {
        co_return co_await BatchGetStringLengthFromStream(stream, offsets, sessionPool, readOption, dataLengths);
    }
    co_return co_await BatchGetNormalLengthFromStream(stream, offsets, sessionPool, readOption, dataLengths);
}

}} // namespace indexlib::index
