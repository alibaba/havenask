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
#include "indexlib/base/Status.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressReader.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressSessionReader.h"

namespace indexlibv2::index {

class CompressOffsetReader
{
private:
    using U32CompressReader = indexlib::index::EquivalentCompressReader<uint32_t>;
    using U64CompressReader = indexlib::index::EquivalentCompressReader<uint64_t>;
    using U32CompressSessionReader = indexlib::index::EquivalentCompressSessionReader<uint32_t>;
    using U64CompressSessionReader = indexlib::index::EquivalentCompressSessionReader<uint64_t>;

public:
    CompressOffsetReader(bool disableGuardOffset = false);
    CompressOffsetReader(CompressOffsetReader&& other) = default;
    CompressOffsetReader& operator=(CompressOffsetReader&& other) = default;
    CompressOffsetReader(CompressOffsetReader& other) = delete;
    CompressOffsetReader& operator=(CompressOffsetReader& other) = delete;
    ~CompressOffsetReader();

public:
    Status Init(uint32_t docCount, const std::shared_ptr<indexlib::file_system::FileReader>& fileReader,
                const std::shared_ptr<indexlib::file_system::SliceFileReader>& expandSliceFile);

    inline std::pair<Status, uint64_t> GetOffset(docid_t docId) const __ALWAYS_INLINE;
    inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetOffset(const std::vector<docid_t>& docIds, indexlib::file_system::ReadOption readOption,
              std::vector<uint64_t>* offsets) const noexcept;
    bool SetOffset(docid_t docId, uint64_t offset);

    inline bool IsU32Offset() const __ALWAYS_INLINE;

    indexlib::index::EquivalentCompressUpdateMetrics GetUpdateMetrics()
    {
        if (_u64CompressReader) {
            return _u64CompressReader->GetUpdateMetrics();
        }
        assert(_u32CompressReader);
        return _u32CompressReader->GetUpdateMetrics();
    }

    bool GetSliceFileLen(size_t& len) const
    {
        if (_sliceFileReader) {
            len = _sliceFileReader->GetLength();
            return true;
        }
        return false;
    }
    CompressOffsetReader CreateSessionReader(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE
    {
        CompressOffsetReader cloneReader(_disableGuardOffset);

        cloneReader._docCount = _docCount;
        cloneReader._isSessionReader = true;
        cloneReader._fileReader = _fileReader;
        cloneReader._sliceFileReader = _sliceFileReader;
        cloneReader._u32CompressReader = _u32CompressReader;
        cloneReader._u64CompressReader = _u64CompressReader;
        if (_u32CompressReader) {
            cloneReader._u32CompressSessionReader = _u32CompressReader->CreateSessionReader(
                sessionPool, indexlib::index::EquivalentCompressSessionOption());
        }
        if (_u64CompressReader) {
            cloneReader._u64CompressSessionReader = _u64CompressReader->CreateSessionReader(
                sessionPool, indexlib::index::EquivalentCompressSessionOption());
        }
        return cloneReader;
    }

    uint32_t GetDocCount() const { return _docCount; }

    const std::shared_ptr<indexlib::file_system::FileReader>& GetFileReader() const { return _fileReader; }

private:
    uint64_t GetOffsetCount(uint32_t docCount) { return _disableGuardOffset ? docCount : (docCount + 1); }

private:
    mutable U32CompressSessionReader _u32CompressSessionReader;
    mutable U64CompressSessionReader _u64CompressSessionReader;
    std::shared_ptr<U32CompressReader> _u32CompressReader;
    std::shared_ptr<U64CompressReader> _u64CompressReader;
    uint32_t _docCount;
    bool _disableGuardOffset;
    bool _isSessionReader;
    std::shared_ptr<indexlib::file_system::FileReader> _fileReader;
    std::shared_ptr<indexlib::file_system::SliceFileReader> _sliceFileReader;

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////

inline std::pair<Status, uint64_t> CompressOffsetReader::GetOffset(docid_t docId) const
{
    if (_u64CompressReader) {
        return _isSessionReader ? _u64CompressSessionReader[docId] : (*_u64CompressReader)[docId];
    }
    assert(_u32CompressReader);
    return _isSessionReader ? _u32CompressSessionReader[docId] : (*_u32CompressReader)[docId];
}

inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
CompressOffsetReader::GetOffset(const std::vector<docid_t>& docIds, indexlib::file_system::ReadOption readOption,
                                std::vector<uint64_t>* offsets) const noexcept
{
    assert(_isSessionReader);
    if (!_isSessionReader) {
        co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::Runtime);
    }
    if (_u64CompressReader) {
        co_return co_await _u64CompressSessionReader.BatchGet(docIds, readOption, offsets);
    }
    std::vector<uint32_t> u32Offsets;
    assert(_u32CompressReader);
    auto ret = co_await _u32CompressSessionReader.BatchGet(docIds, readOption, &u32Offsets);
    offsets->resize(u32Offsets.size());
    for (size_t i = 0; i < u32Offsets.size(); ++i) {
        (*offsets)[i] = u32Offsets[i];
    }
    co_return ret;
}

inline bool CompressOffsetReader::SetOffset(docid_t docId, uint64_t offset)
{
    if (_u64CompressReader) {
        return _u64CompressReader->Update(docId, offset);
    }
    assert(_u32CompressReader);
    return _u32CompressReader->Update(docId, (uint32_t)offset);
}

inline bool CompressOffsetReader::IsU32Offset() const { return _u32CompressReader != NULL; }

} // namespace indexlibv2::index
