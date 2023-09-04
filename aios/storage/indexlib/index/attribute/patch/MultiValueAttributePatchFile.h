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
#include <queue>

#include "autil/MultiValueType.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"

namespace indexlibv2::index {

class MultiValueAttributePatchFile
{
public:
    MultiValueAttributePatchFile(segmentid_t segmentId, const std::shared_ptr<AttributeConfig>& attrConfig);
    ~MultiValueAttributePatchFile() = default;

public:
    Status Open(const std::shared_ptr<indexlib::file_system::IDirectory>& dir, const std::string& fileName);
    bool HasNext() const;
    Status Next();
    docid_t GetCurDocId() const { return _docId; }
    segmentid_t GetSegmentId() const { return _segId; }

    template <typename T>
    std::pair<Status, size_t> GetPatchValue(uint8_t* value, size_t maxLen, bool& isNull);

    template <typename T>
    Status SkipCurDocValue();

    uint32_t GetPatchItemCount() const { return _patchItemCount; }
    uint32_t GetMaxPatchItemLen() const { return _maxPatchLen; }
    int64_t GetFileLength() const { return _fileLength; }

private:
    Status ReadAndMove(size_t length, uint8_t*& buffPtr, size_t& leftBuffsize);
    std::pair<Status, uint32_t> ReadCount(uint8_t* buffPtr, size_t maxLen, size_t& encodeCountLen, bool& isNull);
    Status InitPatchFileReader(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                               const std::string& fileName);

    size_t GetPatchDataLen(uint32_t unitSize, uint32_t itemCount)
    {
        return _fixedValueCount == -1 ? (unitSize * itemCount) : _patchDataLen;
    }

protected:
    std::shared_ptr<indexlib::file_system::FileReader> _fileReader;
    int64_t _fileLength;
    int64_t _cursor;
    docid_t _docId;
    segmentid_t _segId;
    uint32_t _patchItemCount;
    uint32_t _maxPatchLen;
    size_t _patchDataLen;
    int32_t _fixedValueCount;
    bool _patchCompressed;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////
inline bool MultiValueAttributePatchFile::HasNext() const
{
    // uint32_t * 2 (tail): patchCount & maxPatchLen
    return _cursor < _fileLength - 8;
}

inline Status MultiValueAttributePatchFile::Next()
{
    auto fsResult = _fileReader->Read((void*)(&_docId), sizeof(docid_t), _cursor);
    if (!fsResult.OK()) {
        AUTIL_LOG(ERROR, "read patch file failed, docid[%d] error[%d]", _docId, fsResult.Code());
        return fsResult.Status();
    }
    if (fsResult.result < (ssize_t)sizeof(docid_t)) {
        AUTIL_LOG(ERROR, "read patch file failed, file path is: %s", _fileReader->DebugString().c_str());
        return Status::InternalError("read patch file failed");
    }

    _cursor += sizeof(docid_t);

    if (unlikely(!HasNext())) {
        AUTIL_LOG(ERROR, "reach end of patch file!");
        return Status::InternalError("reach end of patch file!");
    }
    return Status::OK();
}

template <typename T>
inline std::pair<Status, size_t> MultiValueAttributePatchFile::GetPatchValue(uint8_t* value, size_t maxLen,
                                                                             bool& isNull)
{
    if (unlikely(!HasNext())) {
        AUTIL_LOG(ERROR, "reach end of patch file!");
        return std::make_pair(Status::InternalError("reach end of patch file!"), 0);
    }

    uint8_t* valuePtr = value;
    size_t encodeCountLen = 0;
    auto [st, recordNum] = ReadCount(valuePtr, maxLen, encodeCountLen, isNull);
    RETURN2_IF_STATUS_ERROR(st, 0, "read patch count failed.");
    valuePtr += encodeCountLen;
    maxLen -= encodeCountLen;

    size_t length = GetPatchDataLen(sizeof(T), recordNum);
    st = ReadAndMove(length, valuePtr, maxLen);
    RETURN2_IF_STATUS_ERROR(st, 0, "read patch value failed.");

    assert((int32_t)(valuePtr - value) == (int32_t)(encodeCountLen + length));
    return std::make_pair(Status::OK(), valuePtr - value);
}

template <>
inline std::pair<Status, size_t>
MultiValueAttributePatchFile::GetPatchValue<autil::MultiChar>(uint8_t* value, size_t maxLen, bool& isNull)
{
    if (unlikely(!HasNext())) {
        AUTIL_LOG(ERROR, "reach end of patch file!");
        return std::make_pair(Status::InternalError("reach end of patch file!"), 0);
    }
    uint8_t* valuePtr = value;
    size_t encodeCountLen = 0;
    auto [st, recordNum] = ReadCount(valuePtr, maxLen, encodeCountLen, isNull);
    RETURN2_IF_STATUS_ERROR(st, 0, "read patch count failed.");
    valuePtr += encodeCountLen;
    maxLen -= encodeCountLen;

    if (recordNum > 0) {
        // read offsetLen
        uint8_t* offsetLenPtr = valuePtr;
        st = ReadAndMove(sizeof(uint8_t), valuePtr, maxLen);
        RETURN2_IF_STATUS_ERROR(st, 0, "read patch offsetLen failed.");

        uint8_t offsetLen = *offsetLenPtr;
        // read offsets
        uint8_t* offsetAddr = valuePtr;
        st = ReadAndMove(offsetLen * recordNum, valuePtr, maxLen);
        RETURN2_IF_STATUS_ERROR(st, 0, "read patch offsets failed.");

        // read items except last one
        uint32_t lastOffset =
            MultiValueAttributeFormatter::GetOffset((const char*)offsetAddr, offsetLen, recordNum - 1);
        st = ReadAndMove(lastOffset, valuePtr, maxLen);
        RETURN2_IF_STATUS_ERROR(st, 0, "read patch value failed.");

        // read last item
        bool isNull;
        auto [status, lastItemLen] = ReadCount(valuePtr, maxLen, encodeCountLen, isNull);
        RETURN2_IF_STATUS_ERROR(status, 0, "read patch count failed.");

        assert(!isNull);
        valuePtr += encodeCountLen;
        maxLen -= encodeCountLen;
        st = ReadAndMove(lastItemLen, valuePtr, maxLen);
        RETURN2_IF_STATUS_ERROR(st, 0, "read patch last value failed.");
    }
    return std::make_pair(Status::OK(), valuePtr - value);
}

template <typename T>
inline Status MultiValueAttributePatchFile::SkipCurDocValue()
{
    if (!HasNext()) {
        AUTIL_LOG(ERROR, "reach end of patch file!");
        return Status::InternalError("reach end of patch file!");
    }

    uint8_t encodeCountBuf[4];
    size_t encodeCountLen = 0;
    bool isNull = false;
    auto [status, recordNum] = ReadCount(encodeCountBuf, 4, encodeCountLen, isNull);
    RETURN_IF_STATUS_ERROR(status, "read patch count failed.");

    size_t length = GetPatchDataLen(sizeof(T), recordNum);
    _cursor += length;
    return Status::OK();
}

template <>
inline Status MultiValueAttributePatchFile::SkipCurDocValue<autil::MultiChar>()
{
    if (!HasNext()) {
        AUTIL_LOG(ERROR, "reach end of patch file!");
        return Status::InternalError("reach end of patch file!");
    }

    uint8_t buffer[4];
    size_t encodeCountLen = 0;
    bool isNull = false;
    auto [st, recordNum] = ReadCount(buffer, 4, encodeCountLen, isNull);
    RETURN_IF_STATUS_ERROR(st, "read patch count failed.");

    if (recordNum > 0) {
        uint8_t* bufferPtr = buffer;
        size_t readLen = 1;
        st = ReadAndMove(readLen, bufferPtr, readLen);
        RETURN_IF_STATUS_ERROR(st, "read patch offset failed.");

        uint8_t offsetLen = buffer[0];
        _cursor += (recordNum - 1) * offsetLen;

        bufferPtr = buffer;
        readLen = offsetLen;
        st = ReadAndMove(readLen, bufferPtr, readLen);
        RETURN_IF_STATUS_ERROR(st, "read patch value failed.");

        uint32_t lastOffset = MultiValueAttributeFormatter::GetOffset((const char*)buffer, offsetLen, 0);
        _cursor += lastOffset;

        auto [status, lastItemLen] = ReadCount(buffer, 4, encodeCountLen, isNull);
        RETURN_IF_STATUS_ERROR(status, "read patch count failed.");

        assert(!isNull);
        _cursor += lastItemLen;
    }
    return Status::OK();
}

inline Status MultiValueAttributePatchFile::ReadAndMove(size_t length, uint8_t*& buffPtr, size_t& leftBuffSize)
{
    if (length > leftBuffSize) {
        AUTIL_LOG(ERROR, "exceed buffer max length, length[%lu] < leftBufferSize[%lu], file [%s]", length, leftBuffSize,
                  _fileReader->DebugString().c_str());
        return Status::InternalError("exceed buffer max length");
    }
    auto fsResult = _fileReader->Read((void*)buffPtr, length, _cursor);
    if (!fsResult.OK()) {
        AUTIL_LOG(ERROR, "read patch file failed, error[%d]", fsResult.Code());
        return fsResult.Status();
    }
    if (fsResult.result < (ssize_t)length) {
        AUTIL_LOG(ERROR, "read patch file failed, file path is: %s", _fileReader->DebugString().c_str());
        return Status::InternalError("read patch file failed");
    }
    _cursor += length;
    buffPtr += length;
    leftBuffSize -= length;
    return Status::OK();
}

inline std::pair<Status, uint32_t> MultiValueAttributePatchFile::ReadCount(uint8_t* buffPtr, size_t maxLen,
                                                                           size_t& encodeCountLen, bool& isNull)
{
    if (_fixedValueCount != -1) {
        encodeCountLen = 0;
        return std::make_pair(Status::OK(), _fixedValueCount);
    }

    uint8_t* valuePtr = buffPtr;
    size_t readLen = sizeof(uint8_t);
    auto st = ReadAndMove(readLen, valuePtr, maxLen);
    RETURN2_IF_STATUS_ERROR(st, 0, "read patch value len failed.");

    encodeCountLen = MultiValueAttributeFormatter::GetEncodedCountFromFirstByte(*buffPtr);
    if (encodeCountLen == 0 || encodeCountLen > 4) {
        AUTIL_LOG(ERROR, "read value count from patch file failed, file path is: %s",
                  _fileReader->DebugString().c_str());
        return std::make_pair(Status::InternalError("read value count from patch file failed"), 0);
    }

    readLen = encodeCountLen - 1;
    st = ReadAndMove(readLen, valuePtr, maxLen);
    RETURN2_IF_STATUS_ERROR(st, 0, "read patch value failed.");

    auto cnt = MultiValueAttributeFormatter::DecodeCount((char*)buffPtr, encodeCountLen, isNull);
    return std::make_pair(Status::OK(), cnt);
}

} // namespace indexlibv2::index
