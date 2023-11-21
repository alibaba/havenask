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
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace index {

class VarNumAttributePatchFile
{
public:
    VarNumAttributePatchFile(segmentid_t segmentId, const config::AttributeConfigPtr& attrConfig);

    ~VarNumAttributePatchFile();

public:
    void Open(const file_system::DirectoryPtr& directory, const std::string& fileName);
    bool HasNext();
    void Next();
    docid_t GetCurDocId() const { return mDocId; }
    segmentid_t GetSegmentId() const { return mSegmentId; }

    template <typename T>
    size_t GetPatchValue(uint8_t* value, size_t maxLen, bool& isNull);

    template <typename T>
    void SkipCurDocValue();

    uint32_t GetPatchItemCount() const { return mPatchItemCount; }
    uint32_t GetMaxPatchItemLen() const { return mMaxPatchLen; }
    int64_t GetFileLength() const { return mFileLength; }

private:
    void ReadAndMove(size_t length, uint8_t*& buffPtr, size_t& leftBuffsize);
    uint32_t ReadCount(uint8_t* buffPtr, size_t maxLen, size_t& encodeCountLen, bool& isNull);
    void InitPatchFileReader(const file_system::DirectoryPtr& directory, const std::string& fileName);

    size_t GetPatchDataLen(uint32_t unitSize, uint32_t itemCount)
    {
        return mFixedValueCount == -1 ? (unitSize * itemCount) : mPatchDataLen;
    }
    bool ReachEnd() const;

protected:
    file_system::FileReaderPtr mFile;
    int64_t mFileLength;
    int64_t mCursor;
    docid_t mDocId;
    segmentid_t mSegmentId;
    uint32_t mPatchItemCount;
    uint32_t mMaxPatchLen;
    size_t mPatchDataLen;
    int32_t mFixedValueCount;
    bool mPatchCompressed;

private:
    friend class VarNumAttributePatchFileTest;
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////
inline bool VarNumAttributePatchFile::HasNext()
{
    // uint32_t * 2 (tail): patchCount & maxPatchLen
    return mCursor < mFileLength - 8;
}

inline bool VarNumAttributePatchFile::ReachEnd() const { return mCursor >= mFileLength - 8; }

inline void VarNumAttributePatchFile::Next()
{
    if (mFile->Read((void*)(&mDocId), sizeof(docid_t), mCursor).GetOrThrow() < (ssize_t)sizeof(docid_t)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Read patch file failed, file path is: %s", mFile->DebugString().c_str());
    }

    mCursor += sizeof(docid_t);

    if (unlikely(ReachEnd())) {
        INDEXLIB_FATAL_ERROR(OutOfRange, "reach end of patch file!");
    }
}

template <typename T>
inline size_t VarNumAttributePatchFile::GetPatchValue(uint8_t* value, size_t maxLen, bool& isNull)
{
    if (unlikely(ReachEnd())) {
        INDEXLIB_FATAL_ERROR(OutOfRange, "reach end of patch file!");
    }
    uint8_t* valuePtr = value;
    size_t encodeCountLen = 0;
    uint32_t recordNum = ReadCount(valuePtr, maxLen, encodeCountLen, isNull);
    valuePtr += encodeCountLen;
    maxLen -= encodeCountLen;

    size_t length = GetPatchDataLen(sizeof(T), recordNum);
    ReadAndMove(length, valuePtr, maxLen);

    assert((int32_t)(valuePtr - value) == (int32_t)(encodeCountLen + length));
    return valuePtr - value;
}

template <>
inline size_t VarNumAttributePatchFile::GetPatchValue<autil::MultiChar>(uint8_t* value, size_t maxLen, bool& isNull)
{
    if (unlikely(ReachEnd())) {
        INDEXLIB_FATAL_ERROR(OutOfRange, "reach end of patch file!");
    }
    uint8_t* valuePtr = value;
    size_t encodeCountLen = 0;
    uint32_t recordNum = ReadCount(valuePtr, maxLen, encodeCountLen, isNull);
    valuePtr += encodeCountLen;
    maxLen -= encodeCountLen;

    if (recordNum > 0) {
        // read offsetLen
        uint8_t* offsetLenPtr = valuePtr;
        ReadAndMove(sizeof(uint8_t), valuePtr, maxLen);
        uint8_t offsetLen = *offsetLenPtr;

        // read offsets
        uint8_t* offsetAddr = valuePtr;
        ReadAndMove(offsetLen * recordNum, valuePtr, maxLen);

        // read items except last one
        uint32_t lastOffset =
            common::VarNumAttributeFormatter::GetOffset((const char*)offsetAddr, offsetLen, recordNum - 1);
        ReadAndMove(lastOffset, valuePtr, maxLen);

        // read last item
        bool isNull;
        uint32_t lastItemLen = ReadCount(valuePtr, maxLen, encodeCountLen, isNull);
        assert(!isNull);
        valuePtr += encodeCountLen;
        maxLen -= encodeCountLen;
        ReadAndMove(lastItemLen, valuePtr, maxLen);
    }
    return valuePtr - value;
}

template <typename T>
inline void VarNumAttributePatchFile::SkipCurDocValue()
{
    if (!HasNext()) {
        INDEXLIB_FATAL_ERROR(OutOfRange, "reach end of patch file!");
    }

    uint8_t encodeCountBuf[4];
    size_t encodeCountLen = 0;
    bool isNull = false;
    uint32_t recordNum = ReadCount(encodeCountBuf, 4, encodeCountLen, isNull);
    size_t length = GetPatchDataLen(sizeof(T), recordNum);
    mCursor += length;
}

template <>
inline void VarNumAttributePatchFile::SkipCurDocValue<autil::MultiChar>()
{
    if (!HasNext()) {
        INDEXLIB_FATAL_ERROR(OutOfRange, "reach end of patch file!");
    }

    uint8_t buffer[4];
    size_t encodeCountLen = 0;
    bool isNull = false;
    uint32_t recordNum = ReadCount(buffer, 4, encodeCountLen, isNull);
    if (recordNum > 0) {
        uint8_t* bufferPtr = buffer;
        size_t readLen = 1;
        ReadAndMove(readLen, bufferPtr, readLen);

        uint8_t offsetLen = buffer[0];
        mCursor += (recordNum - 1) * offsetLen;

        bufferPtr = buffer;
        readLen = offsetLen;
        ReadAndMove(readLen, bufferPtr, readLen);

        uint32_t lastOffset = common::VarNumAttributeFormatter::GetOffset((const char*)buffer, offsetLen, 0);
        mCursor += lastOffset;

        uint32_t lastItemLen = ReadCount(buffer, 4, encodeCountLen, isNull);
        assert(!isNull);
        mCursor += lastItemLen;
    }
}

inline void VarNumAttributePatchFile::ReadAndMove(size_t length, uint8_t*& buffPtr, size_t& leftBuffSize)
{
    if (length > leftBuffSize) {
        INDEXLIB_FATAL_ERROR(BufferOverflow, "Exceed buffer max length, file path is: %s",
                             mFile->DebugString().c_str());
    }
    if (mFile->Read((void*)buffPtr, length, mCursor).GetOrThrow() < (size_t)length) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Read patch file failed, file path is: %s", mFile->DebugString().c_str());
    }
    mCursor += length;
    buffPtr += length;
    leftBuffSize -= length;
}

inline uint32_t VarNumAttributePatchFile::ReadCount(uint8_t* buffPtr, size_t maxLen, size_t& encodeCountLen,
                                                    bool& isNull)
{
    if (mFixedValueCount != -1) {
        encodeCountLen = 0;
        return mFixedValueCount;
    }

    uint8_t* valuePtr = buffPtr;
    size_t readLen = sizeof(uint8_t);
    ReadAndMove(readLen, valuePtr, maxLen);
    encodeCountLen = common::VarNumAttributeFormatter::GetEncodedCountFromFirstByte(*buffPtr);
    if (encodeCountLen == 0 || encodeCountLen > 4) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Read value count from patch file failed, file path is: %s",
                             mFile->DebugString().c_str());
    }

    readLen = encodeCountLen - 1;
    ReadAndMove(readLen, valuePtr, maxLen);
    return common::VarNumAttributeFormatter::DecodeCount((char*)buffPtr, encodeCountLen, isNull);
}
}} // namespace indexlib::index
