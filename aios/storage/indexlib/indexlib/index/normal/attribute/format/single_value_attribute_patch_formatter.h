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
#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_PATCH_FORMATTER_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_PATCH_FORMATTER_H

#include "indexlib/common_define.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace index {

class SingleValueAttributePatchFormatter
{
public:
    SingleValueAttributePatchFormatter() : mPatchItemCount(0), mFileLength(0) {}
    ~SingleValueAttributePatchFormatter() {}

public:
    void InitForWrite(bool supportNull, file_system::FileWriterPtr output);
    void InitForRead(bool supportNull, file_system::FileReaderPtr input, int64_t recordSize, int64_t fileLength);
    uint32_t GetPatchItemCount() const { return mPatchItemCount; }

public:
    void Close()
    {
        if (mOutput) {
            if (mIsSupportNull) {
                mOutput->Write(&mPatchItemCount, sizeof(uint32_t)).GetOrThrow();
            }
            mOutput->Close().GetOrThrow();
        }
        if (mInput) {
            mInput->Close().GetOrThrow();
        }
    }

public:
    inline static void EncodedDocId(docid_t& docId) { docId = ~docId; }

    inline static docid_t CompareDocId(const docid_t& left, const docid_t& right)
    {
        if (left < 0 && right < 0) {
            return ~left < ~right;
        }
        if (right < 0) {
            return left < ~right;
        }
        if (left < 0) {
            return ~left < right;
        }
        return left < right;
    }

    inline static int32_t TailLength() { return sizeof(uint32_t); }

    inline void Write(docid_t docId, uint8_t* value, uint8_t length)
    {
        mOutput->Write(&docId, sizeof(docid_t)).GetOrThrow();
        if (docId >= 0) // only write not null value
        {
            mOutput->Write(value, length).GetOrThrow();
        }
        mPatchItemCount++;
    }

    template <typename T>
    inline bool Read(docid_t& docId, T& value, bool& isNull)
    {
        if (!HasNext()) {
            return false;
        }
        if (mInput->Read((void*)(&docId), sizeof(docid_t), mCursor).GetOrThrow() < (ssize_t)sizeof(docid_t)) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "Read patch file failed, file path is: %s",
                                 mInput->DebugString().c_str());
        }
        mCursor += sizeof(docid_t);
        if (mIsSupportNull && docId < 0) {
            docId = ~docId;
            isNull = true;
            return true;
        }

        isNull = false;
        if (mInput->Read((void*)(&value), sizeof(T), mCursor).GetOrThrow() < (ssize_t)sizeof(T)) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "Read patch file failed, file path is: %s",
                                 mInput->DebugString().c_str());
        }
        mCursor += sizeof(T);
        return true;
    }

    bool HasNext() const
    {
        if (mInput) {
            if (mIsSupportNull) {
                return mCursor < mFileLength - sizeof(uint32_t);
            }
            return mCursor < mFileLength;
        }
        return false;
    }

private:
    bool mIsSupportNull;
    file_system::FileWriterPtr mOutput;
    file_system::FileReaderPtr mInput;
    uint64_t mCursor;
    uint64_t mPatchItemCount;
    uint64_t mFileLength;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleValueAttributePatchFormatter);
}} // namespace indexlib::index

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_PATCH_FORMATTER_H
