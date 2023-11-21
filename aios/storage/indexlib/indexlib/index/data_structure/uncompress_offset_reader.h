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

#include "indexlib/common_define.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/file_system/stream/FileStreamCreator.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class UncompressOffsetReader
{
public:
    UncompressOffsetReader(uint64_t offsetExtendThreshold = VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD,
                           bool disableGuardOffset = false);
    UncompressOffsetReader(UncompressOffsetReader&& other) = default;
    UncompressOffsetReader& operator=(UncompressOffsetReader&& other) = default;
    ~UncompressOffsetReader();

public:
    void Init(uint32_t docCount, const file_system::FileReaderPtr& fileReader,
              const file_system::SliceFileReaderPtr& u64SliceFileReader = file_system::SliceFileReaderPtr());

    void InitFromBuffer(uint8_t* buffer, uint64_t bufferLen, uint32_t docCount);

    inline uint64_t GetOffset(docid_t docId) const __ALWAYS_INLINE;

    inline future_lite::coro::Lazy<index::ErrorCodeVec> GetOffset(const std::vector<docid_t>& docIds,
                                                                  file_system::ReadOption option,
                                                                  std::vector<uint64_t>* offsets) const noexcept;

    bool SetOffset(docid_t docId, uint64_t offset);

    inline bool IsU32Offset() const __ALWAYS_INLINE;

    uint32_t GetDocCount() const { return mDocCount; }

    const file_system::FileReaderPtr& GetFileReader() const { return mFileReader; }

    inline UncompressOffsetReader CreateSessionReader(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE;

private:
    void InitFromFile(const file_system::FileReaderPtr& fileReader, uint32_t docCount);
    void ExtendU32OffsetToU64Offset();
    static void ExtendU32OffsetToU64Offset(uint32_t* u32Offset, uint64_t* u64Offset, uint32_t count);

    uint64_t GetOffsetCount(uint32_t docCount) { return mDisableGuardOffset ? docCount : (docCount + 1); }

private:
    // u32->u64, ensure attribute reader used by modifier & reader is same one
    volatile uint64_t* mU64Buffer;
    uint32_t* mU32Buffer;
    uint32_t mDocCount;
    file_system::FileReaderPtr mFileReader;
    bool mIsU32Offset;
    bool mDisableGuardOffset;
    uint64_t mOffsetThreshold;
    std::shared_ptr<file_system::FileStream> mFileStream;
    file_system::SliceFileReaderPtr mSliceFileReader;

private:
    friend class UncompressOffsetReaderTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(UncompressOffsetReader);
///////////////////////////////////////////////////////////
inline uint64_t UncompressOffsetReader::GetOffset(docid_t docId) const
{
    if (IsU32Offset()) {
        if (mU32Buffer) {
            return mU32Buffer[docId];
        } else {
            uint32_t offset = 0;
            mFileStream->Read(&offset, sizeof(offset), sizeof(offset) * docId, file_system::ReadOption::LowLatency())
                .GetOrThrow();
            return offset;
        }
    } else {
        if (mU64Buffer) {
            return mU64Buffer[docId];
        } else {
            uint64_t offset = 0;
            mFileStream->Read(&offset, sizeof(offset), sizeof(offset) * docId, file_system::ReadOption::LowLatency())
                .GetOrThrow();
            return offset;
        }
    }
}
inline future_lite::coro::Lazy<index::ErrorCodeVec>
UncompressOffsetReader::GetOffset(const std::vector<docid_t>& docIds, file_system::ReadOption option,
                                  std::vector<uint64_t>* offsets) const noexcept
{
    offsets->resize(docIds.size(), 0);
    if (IsU32Offset()) {
        if (mU32Buffer) {
            for (size_t i = 0; i < docIds.size(); ++i) {
                (*offsets)[i] = mU32Buffer[docIds[i]];
            }
            co_return index::ErrorCodeVec(docIds.size(), index::ErrorCode::OK);
        } else {
            file_system::BatchIO batchIO;
            std::vector<uint32_t> shortOffsets(docIds.size());
            for (size_t i = 0; i < docIds.size(); ++i) {
                const docid_t& docId = docIds[i];
                uint32_t& offset = shortOffsets[i];
                batchIO.emplace_back(&offset, sizeof(offset), sizeof(offset) * docId);
            }
            auto readRet = co_await mFileStream->BatchRead(batchIO, option);
            index::ErrorCodeVec ret(docIds.size());
            for (size_t i = 0; i < docIds.size(); ++i) {
                (*offsets)[i] = shortOffsets[i];
                ret[i] = index::ConvertFSErrorCode(readRet[i].ec);
            }
            co_return ret;
        }
    } else {
        if (mU64Buffer) {
            for (size_t i = 0; i < docIds.size(); ++i) {
                (*offsets)[i] = mU64Buffer[docIds[i]];
            }
            co_return index::ErrorCodeVec(docIds.size(), index::ErrorCode::OK);
        } else {
            file_system::BatchIO batchIO;
            for (size_t i = 0; i < docIds.size(); ++i) {
                const docid_t& docId = docIds[i];
                uint64_t& offset = (*offsets)[i];
                batchIO.emplace_back(&offset, sizeof(offset), sizeof(offset) * docId);
            }
            auto readRet = co_await mFileStream->BatchRead(batchIO, option);
            index::ErrorCodeVec ret(docIds.size());
            for (size_t i = 0; i < docIds.size(); ++i) {
                ret[i] = index::ConvertFSErrorCode(readRet[i].ec);
            }
            co_return ret;
        }
    }
}

inline bool UncompressOffsetReader::SetOffset(docid_t docId, uint64_t offset)
{
    assert(mU64Buffer || mU32Buffer);
    if (!IsU32Offset()) {
        mU64Buffer[docId] = offset;
        return true;
    }

    if (offset > mOffsetThreshold) {
        ExtendU32OffsetToU64Offset();
        mU64Buffer[docId] = offset;
        return true;
    }
    mU32Buffer[docId] = (uint32_t)offset;
    return true;
}

inline bool UncompressOffsetReader::IsU32Offset() const { return mIsU32Offset; }

inline UncompressOffsetReader UncompressOffsetReader::CreateSessionReader(autil::mem_pool::Pool* sessionPool) const
{
    UncompressOffsetReader sessionReader(mOffsetThreshold, mDisableGuardOffset);

    sessionReader.mU64Buffer = mU64Buffer;
    sessionReader.mU32Buffer = mU32Buffer;
    sessionReader.mDocCount = mDocCount;
    sessionReader.mFileReader = mFileReader;
    sessionReader.mIsU32Offset = mIsU32Offset;
    sessionReader.mDisableGuardOffset = mDisableGuardOffset;
    sessionReader.mOffsetThreshold = mOffsetThreshold;
    if (mFileStream) {
        sessionReader.mFileStream = file_system::FileStreamCreator::CreateFileStream(mFileReader, nullptr);
    }
    sessionReader.mSliceFileReader = mSliceFileReader;
    return sessionReader;
}
}} // namespace indexlib::index
