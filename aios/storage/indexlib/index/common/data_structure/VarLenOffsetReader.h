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

#include "autil/Log.h"
#include "indexlib/index/common/data_structure/CompressOffsetReader.h"
#include "indexlib/index/common/data_structure/UncompressOffsetReader.h"
#include "indexlib/index/common/data_structure/VarLenDataParam.h"

namespace indexlibv2::index {

class VarLenOffsetReader
{
public:
    VarLenOffsetReader(const VarLenDataParam& param);
    VarLenOffsetReader(VarLenOffsetReader&& other) = default;
    VarLenOffsetReader& operator=(VarLenOffsetReader&& other) = default;
    ~VarLenOffsetReader();

public:
    Status Init(uint32_t docCount, const std::shared_ptr<indexlib::file_system::FileReader>& fileReader,
                const std::shared_ptr<indexlib::file_system::SliceFileReader>& extFileReader);

    inline std::pair<Status, uint64_t> GetOffset(docid_t docId) const __ALWAYS_INLINE;
    inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetOffset(const std::vector<docid_t>& docIds, indexlib::file_system::ReadOption option,
              std::vector<uint64_t>* offsets) const noexcept;
    inline bool IsU32Offset() const __ALWAYS_INLINE;
    inline uint32_t GetDocCount() const __ALWAYS_INLINE;
    std::pair<Status, bool> SetOffset(docid_t docId, uint64_t offset);

    CompressOffsetReader* GetCompressOffsetReader() const
    {
        if (_param.equalCompressOffset) {
            return const_cast<CompressOffsetReader*>(&_compressOffsetReader);
        }
        return nullptr;
    }
    const std::shared_ptr<indexlib::file_system::FileReader>& GetFileReader() const;
    VarLenOffsetReader CreateSessionReader(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE
    {
        VarLenOffsetReader cloneReader(_param);
        if (_param.equalCompressOffset) {
            cloneReader._compressOffsetReader = _compressOffsetReader.CreateSessionReader(sessionPool);
        } else {
            cloneReader._uncompressOffsetReader = _uncompressOffsetReader.CreateSessionReader(sessionPool);
        }
        return cloneReader;
    }

private:
    VarLenDataParam _param;
    CompressOffsetReader _compressOffsetReader;
    UncompressOffsetReader _uncompressOffsetReader;

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////////////////
inline std::pair<Status, uint64_t> VarLenOffsetReader::GetOffset(docid_t docId) const
{
    if (_param.equalCompressOffset) {
        return _compressOffsetReader.GetOffset(docId);
    }
    return _uncompressOffsetReader.GetOffset(docId);
}
inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
VarLenOffsetReader::GetOffset(const std::vector<docid_t>& docIds, indexlib::file_system::ReadOption option,
                              std::vector<uint64_t>* offsets) const noexcept
{
    if (_param.equalCompressOffset) {
        co_return co_await _compressOffsetReader.GetOffset(docIds, option, offsets);
    }
    co_return co_await _uncompressOffsetReader.GetOffset(docIds, option, offsets);
}

inline bool VarLenOffsetReader::IsU32Offset() const
{
    if (_param.equalCompressOffset) {
        return _compressOffsetReader.IsU32Offset();
    }
    return _uncompressOffsetReader.IsU32Offset();
}

inline uint32_t VarLenOffsetReader::GetDocCount() const
{
    if (_param.equalCompressOffset) {
        return _compressOffsetReader.GetDocCount();
    }
    return _uncompressOffsetReader.GetDocCount();
}

} // namespace indexlibv2::index
