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

#include "autil/Log.h"
#include "indexlib/base/Define.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/file_system/stream/FileStreamCreator.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/ErrorCode.h"

namespace indexlibv2::index {

class MultiValueAttributeUnCompressOffsetReader
{
public:
    MultiValueAttributeUnCompressOffsetReader()
        : _u64Buffer(nullptr)
        , _u32Buffer(nullptr)
        , _docCount(0)
        , _isU32Offset(false)
        , _disableGuardOffset(false)
        , _offsetThreshold(0)
    {
    }

    MultiValueAttributeUnCompressOffsetReader(MultiValueAttributeUnCompressOffsetReader&& other) = default;
    MultiValueAttributeUnCompressOffsetReader& operator=(MultiValueAttributeUnCompressOffsetReader&& other) = default;
    MultiValueAttributeUnCompressOffsetReader(MultiValueAttributeUnCompressOffsetReader& other) = delete;
    MultiValueAttributeUnCompressOffsetReader& operator=(MultiValueAttributeUnCompressOffsetReader& other) = delete;
    ~MultiValueAttributeUnCompressOffsetReader() = default;

public:
    Status Init(const std::shared_ptr<config::AttributeConfig>& attrConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& attrDir, bool disableUpdate,
                uint32_t docCount);

    inline std::pair<Status, uint64_t> GetOffset(docid_t docId) const __ALWAYS_INLINE;

    inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetOffset(const std::vector<docid_t>& docIds, indexlib::file_system::ReadOption option,
              std::vector<uint64_t>* offsets) const noexcept;

    bool SetOffset(docid_t docId, uint64_t offset);

    inline bool IsU32Offset() const __ALWAYS_INLINE;

    uint32_t GetDocCount() const { return _docCount; }

    const std::shared_ptr<indexlib::file_system::FileReader>& GetFileReader() const { return _fileReader; }

    inline MultiValueAttributeUnCompressOffsetReader
    CreateSessionReader(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE;

private:
    Status InitFromBuffer(uint8_t* buffer, uint64_t bufferLen, uint32_t docCount);
    Status InitFromFile(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader, uint32_t docCount);
    bool ExtendU32OffsetToU64Offset();
    static void ExtendU32OffsetToU64Offset(uint32_t* u32Offset, uint64_t* u64Offset, uint32_t count);

    uint64_t GetOffsetCount(uint32_t docCount) { return _disableGuardOffset ? docCount : (docCount + 1); }
    Status InitFileReader(const std::shared_ptr<indexlib::file_system::IDirectory>& attrDir);
    Status InitSliceFileReader(const std::shared_ptr<config::AttributeConfig>& attrConfig,
                               const std::shared_ptr<indexlib::file_system::IDirectory>& attrDir);

private:
    // u32->u64, ensure attribute reader used by modifier & reader is same one
    volatile uint64_t* _u64Buffer;
    uint32_t* _u32Buffer;
    uint32_t _docCount;
    std::shared_ptr<indexlib::file_system::FileReader> _fileReader;
    bool _isU32Offset;
    bool _disableGuardOffset;
    uint64_t _offsetThreshold;
    std::shared_ptr<indexlib::file_system::FileStream> _fileStream;
    std::shared_ptr<indexlib::file_system::SliceFileReader> _sliceFileReader;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////
inline std::pair<Status, uint64_t> MultiValueAttributeUnCompressOffsetReader::GetOffset(docid_t docId) const
{
    if (IsU32Offset()) {
        if (_u32Buffer) {
            return std::make_pair(Status::OK(), _u32Buffer[docId]);
        } else {
            uint32_t offset = 0;
            auto [status, _] = _fileStream
                                   ->Read(&offset, sizeof(offset), sizeof(offset) * docId,
                                          indexlib::file_system::ReadOption::LowLatency())
                                   .StatusWith();
            RETURN2_IF_STATUS_ERROR(status, 0, "exception occurs when read offset value from FileStream.");
            return std::make_pair(Status::OK(), offset);
        }
    } else {
        if (_u64Buffer) {
            return std::make_pair(Status::OK(), _u64Buffer[docId]);
        } else {
            uint64_t offset = 0;
            auto [status, _] = _fileStream
                                   ->Read(&offset, sizeof(offset), sizeof(offset) * docId,
                                          indexlib::file_system::ReadOption::LowLatency())
                                   .StatusWith();
            RETURN2_IF_STATUS_ERROR(status, 0, "exception occurs when read offset value from FileStream.");
            return std::make_pair(Status::OK(), offset);
        }
    }
}

inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
MultiValueAttributeUnCompressOffsetReader::GetOffset(const std::vector<docid_t>& docIds,
                                                     indexlib::file_system::ReadOption option,
                                                     std::vector<uint64_t>* offsets) const noexcept
{
    offsets->resize(docIds.size(), 0);
    if (IsU32Offset()) {
        if (_u32Buffer) {
            for (size_t i = 0; i < docIds.size(); ++i) {
                (*offsets)[i] = _u32Buffer[docIds[i]];
            }
            co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::OK);
        } else {
            indexlib::file_system::BatchIO batchIO;
            std::vector<uint32_t> shortOffsets(docIds.size());
            for (size_t i = 0; i < docIds.size(); ++i) {
                const docid_t& docId = docIds[i];
                uint32_t& offset = shortOffsets[i];
                batchIO.emplace_back(&offset, sizeof(offset), sizeof(offset) * docId);
            }
            auto readRet = co_await _fileStream->BatchRead(batchIO, option);
            indexlib::index::ErrorCodeVec ret(docIds.size());
            for (size_t i = 0; i < docIds.size(); ++i) {
                (*offsets)[i] = shortOffsets[i];
                ret[i] = indexlib::index::ConvertFSErrorCode(readRet[i].ec);
            }
            co_return ret;
        }
    } else {
        if (_u64Buffer) {
            for (size_t i = 0; i < docIds.size(); ++i) {
                (*offsets)[i] = _u64Buffer[docIds[i]];
            }
            co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::OK);
        } else {
            indexlib::file_system::BatchIO batchIO;
            for (size_t i = 0; i < docIds.size(); ++i) {
                const docid_t& docId = docIds[i];
                uint64_t& offset = (*offsets)[i];
                batchIO.emplace_back(&offset, sizeof(offset), sizeof(offset) * docId);
            }
            auto readRet = co_await _fileStream->BatchRead(batchIO, option);
            indexlib::index::ErrorCodeVec ret(docIds.size());
            for (size_t i = 0; i < docIds.size(); ++i) {
                ret[i] = indexlib::index::ConvertFSErrorCode(readRet[i].ec);
            }
            co_return ret;
        }
    }
}

inline bool MultiValueAttributeUnCompressOffsetReader::SetOffset(docid_t docId, uint64_t offset)
{
    assert(_u64Buffer || _u32Buffer);
    if (!IsU32Offset()) {
        _u64Buffer[docId] = offset;
        return true;
    }

    if (offset > _offsetThreshold) {
        if (!ExtendU32OffsetToU64Offset()) {
            return false;
        }
        _u64Buffer[docId] = offset;
        return true;
    }
    _u32Buffer[docId] = (uint32_t)offset;
    return true;
}

inline bool MultiValueAttributeUnCompressOffsetReader::IsU32Offset() const { return _isU32Offset; }

inline MultiValueAttributeUnCompressOffsetReader
MultiValueAttributeUnCompressOffsetReader::CreateSessionReader(autil::mem_pool::Pool* sessionPool) const
{
    MultiValueAttributeUnCompressOffsetReader sessionReader;
    sessionReader._offsetThreshold = _offsetThreshold;
    sessionReader._disableGuardOffset = _disableGuardOffset;
    sessionReader._u64Buffer = _u64Buffer;
    sessionReader._u32Buffer = _u32Buffer;
    sessionReader._docCount = _docCount;
    sessionReader._fileReader = _fileReader;
    sessionReader._isU32Offset = _isU32Offset;
    if (_fileStream) {
        sessionReader._fileStream = indexlib::file_system::FileStreamCreator::CreateFileStream(_fileReader, nullptr);
    }
    sessionReader._sliceFileReader = _sliceFileReader;
    return sessionReader;
}

} // namespace indexlibv2::index
