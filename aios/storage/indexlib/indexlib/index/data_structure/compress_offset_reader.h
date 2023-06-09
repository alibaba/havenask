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
#ifndef __INDEXLIB_COMPRESS_OFFSET_READER_H
#define __INDEXLIB_COMPRESS_OFFSET_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressReader.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressSessionReader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib { namespace index {

class CompressOffsetReader
{
private:
    typedef EquivalentCompressReader<uint32_t> U32CompressReader;
    typedef EquivalentCompressReader<uint64_t> U64CompressReader;
    DEFINE_SHARED_PTR(U32CompressReader);
    DEFINE_SHARED_PTR(U64CompressReader);
    typedef EquivalentCompressSessionReader<uint32_t> U32CompressSessionReader;
    typedef EquivalentCompressSessionReader<uint64_t> U64CompressSessionReader;

public:
    CompressOffsetReader(bool disableGuardOffset = false);
    CompressOffsetReader(CompressOffsetReader&& other) = default;
    CompressOffsetReader& operator=(CompressOffsetReader&& other) = default;
    CompressOffsetReader(CompressOffsetReader& other) = delete;
    CompressOffsetReader& operator=(CompressOffsetReader& other) = delete;
    ~CompressOffsetReader();

public:
    void Init(uint32_t docCount, const file_system::FileReaderPtr& fileReader,
              const file_system::SliceFileReaderPtr& expandSliceFile = file_system::SliceFileReaderPtr());

    inline uint64_t GetOffset(docid_t docId) const __ALWAYS_INLINE;
    inline future_lite::coro::Lazy<index::ErrorCodeVec> GetOffset(const std::vector<docid_t>& docIds,
                                                                  indexlib::file_system::ReadOption readOption,
                                                                  std::vector<uint64_t>* offsets) const noexcept;
    bool SetOffset(docid_t docId, uint64_t offset);

    inline bool IsU32Offset() const __ALWAYS_INLINE;

    EquivalentCompressUpdateMetrics GetUpdateMetrics()
    {
        if (mU64CompressReader) {
            return mU64CompressReader->GetUpdateMetrics();
        }
        assert(mU32CompressReader);
        return mU32CompressReader->GetUpdateMetrics();
    }

    bool GetSliceFileLen(size_t& len) const
    {
        if (mSliceFileReader) {
            len = mSliceFileReader->GetLength();
            return true;
        }
        return false;
    }
    CompressOffsetReader CreateSessionReader(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE
    {
        CompressOffsetReader cloneReader(mDisableGuardOffset);

        cloneReader.mDocCount = mDocCount;
        cloneReader.mIsSessionReader = true;
        cloneReader.mFileReader = mFileReader;
        cloneReader.mSliceFileReader = mSliceFileReader;
        cloneReader.mU32CompressReader = mU32CompressReader;
        cloneReader.mU64CompressReader = mU64CompressReader;
        if (mU32CompressReader) {
            cloneReader.mU32CompressSessionReader =
                mU32CompressReader->CreateSessionReader(sessionPool, EquivalentCompressSessionOption());
        }
        if (mU64CompressReader) {
            cloneReader.mU64CompressSessionReader =
                mU64CompressReader->CreateSessionReader(sessionPool, EquivalentCompressSessionOption());
        }
        return cloneReader;
    }

    uint32_t GetDocCount() const { return mDocCount; }

    const file_system::FileReaderPtr& GetFileReader() const { return mFileReader; }

private:
    std::pair<Status, uint64_t> GetOffsetWithStatus(docid_t docId) const;
    uint64_t GetOffsetCount(uint32_t docCount) { return mDisableGuardOffset ? docCount : (docCount + 1); }

private:
    mutable U32CompressSessionReader mU32CompressSessionReader;
    mutable U64CompressSessionReader mU64CompressSessionReader;
    U32CompressReaderPtr mU32CompressReader;
    U64CompressReaderPtr mU64CompressReader;
    uint32_t mDocCount;
    bool mDisableGuardOffset;
    bool mIsSessionReader;
    file_system::FileReaderPtr mFileReader;
    file_system::SliceFileReaderPtr mSliceFileReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressOffsetReader);
////////////////////////////////////////////////////////////////

inline std::pair<Status, uint64_t> CompressOffsetReader::GetOffsetWithStatus(docid_t docId) const
{
    if (mU64CompressReader) {
        return mIsSessionReader ? mU64CompressSessionReader[docId] : (*mU64CompressReader)[docId];
    }
    assert(mU32CompressReader);
    return mIsSessionReader ? mU32CompressSessionReader[docId] : (*mU32CompressReader)[docId];
}

inline uint64_t CompressOffsetReader::GetOffset(docid_t docId) const
{
    auto [status, offset] = GetOffsetWithStatus(docId);
    indexlib::util::ThrowIfStatusError(status);
    return offset;
}
inline future_lite::coro::Lazy<index::ErrorCodeVec>
CompressOffsetReader::GetOffset(const std::vector<docid_t>& docIds, indexlib::file_system::ReadOption readOption,
                                std::vector<uint64_t>* offsets) const noexcept
{
    assert(mIsSessionReader);
    if (!mIsSessionReader) {
        co_return index::ErrorCodeVec(docIds.size(), index::ErrorCode::Runtime);
    }
    if (mU64CompressReader) {
        co_return co_await mU64CompressSessionReader.BatchGet(docIds, readOption, offsets);
    }
    std::vector<uint32_t> u32Offsets;
    assert(mU32CompressReader);
    auto ret = co_await mU32CompressSessionReader.BatchGet(docIds, readOption, &u32Offsets);
    offsets->resize(u32Offsets.size());
    for (size_t i = 0; i < u32Offsets.size(); ++i) {
        (*offsets)[i] = u32Offsets[i];
    }
    co_return ret;
}

inline bool CompressOffsetReader::SetOffset(docid_t docId, uint64_t offset)
{
    if (mU64CompressReader) {
        return mU64CompressReader->Update(docId, offset);
    }
    assert(mU32CompressReader);
    return mU32CompressReader->Update(docId, (uint32_t)offset);
}

inline bool CompressOffsetReader::IsU32Offset() const { return mU32CompressReader != NULL; }
}} // namespace indexlib::index

#endif //__INDEXLIB_COMPRESS_OFFSET_READER_H
