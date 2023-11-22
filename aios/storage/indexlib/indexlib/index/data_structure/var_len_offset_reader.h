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
#include "indexlib/index/data_structure/compress_offset_reader.h"
#include "indexlib/index/data_structure/uncompress_offset_reader.h"
#include "indexlib/index/data_structure/var_len_data_param.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class VarLenOffsetReader
{
public:
    VarLenOffsetReader(const VarLenDataParam& param);
    VarLenOffsetReader(VarLenOffsetReader&& other) = default;
    VarLenOffsetReader& operator=(VarLenOffsetReader&& other) = default;
    ~VarLenOffsetReader();

public:
    void Init(uint32_t docCount, const file_system::FileReaderPtr& fileReader,
              const file_system::SliceFileReaderPtr& extFileReader = file_system::SliceFileReaderPtr());

    inline uint64_t GetOffset(docid_t docId) const __ALWAYS_INLINE;
    inline future_lite::coro::Lazy<index::ErrorCodeVec> GetOffset(const std::vector<docid_t>& docIds,
                                                                  file_system::ReadOption option,
                                                                  std::vector<uint64_t>* offsets) const noexcept;
    inline bool IsU32Offset() const __ALWAYS_INLINE;
    inline uint32_t GetDocCount() const __ALWAYS_INLINE;
    bool SetOffset(docid_t docId, uint64_t offset);

    CompressOffsetReader* GetCompressOffsetReader() const
    {
        if (mParam.equalCompressOffset) {
            return const_cast<CompressOffsetReader*>(&mCompressOffsetReader);
        }
        return nullptr;
    }
    const file_system::FileReaderPtr& GetFileReader() const;
    VarLenOffsetReader CreateSessionReader(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE
    {
        VarLenOffsetReader cloneReader(mParam);
        if (mParam.equalCompressOffset) {
            cloneReader.mCompressOffsetReader = mCompressOffsetReader.CreateSessionReader(sessionPool);
        } else {
            cloneReader.mUncompressOffsetReader = mUncompressOffsetReader.CreateSessionReader(sessionPool);
        }
        return cloneReader;
    }

private:
    VarLenDataParam mParam;
    CompressOffsetReader mCompressOffsetReader;
    UncompressOffsetReader mUncompressOffsetReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarLenOffsetReader);

////////////////////////////////////////////////////////////////////////////////////
inline uint64_t VarLenOffsetReader::GetOffset(docid_t docId) const
{
    if (mParam.equalCompressOffset) {
        return mCompressOffsetReader.GetOffset(docId);
    }
    return mUncompressOffsetReader.GetOffset(docId);
}
inline future_lite::coro::Lazy<index::ErrorCodeVec>
VarLenOffsetReader::GetOffset(const std::vector<docid_t>& docIds, file_system::ReadOption option,
                              std::vector<uint64_t>* offsets) const noexcept
{
    if (mParam.equalCompressOffset) {
        co_return co_await mCompressOffsetReader.GetOffset(docIds, option, offsets);
    }
    co_return co_await mUncompressOffsetReader.GetOffset(docIds, option, offsets);
}

inline bool VarLenOffsetReader::IsU32Offset() const
{
    if (mParam.equalCompressOffset) {
        return mCompressOffsetReader.IsU32Offset();
    }
    return mUncompressOffsetReader.IsU32Offset();
}

inline uint32_t VarLenOffsetReader::GetDocCount() const
{
    if (mParam.equalCompressOffset) {
        return mCompressOffsetReader.GetDocCount();
    }
    return mUncompressOffsetReader.GetDocCount();
}
}} // namespace indexlib::index
