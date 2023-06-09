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
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Define.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/index/attribute/AttributeMetrics.h"
#include "indexlib/index/attribute/MultiValueAttributeCompressOffsetReader.h"
#include "indexlib/index/attribute/MultiValueAttributeUnCompressOffsetReader.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/data_structure/VarLenDataParam.h"

namespace indexlibv2::index {

class MultiValueAttributeOffsetReader
{
public:
    MultiValueAttributeOffsetReader(std::shared_ptr<AttributeMetrics> attributeMetric)
        : _attributeMetrics(attributeMetric)
    {
    }
    MultiValueAttributeOffsetReader(MultiValueAttributeOffsetReader&& other) = default;
    MultiValueAttributeOffsetReader& operator=(MultiValueAttributeOffsetReader&& other) = default;
    MultiValueAttributeOffsetReader(MultiValueAttributeOffsetReader& other) = delete;
    MultiValueAttributeOffsetReader& operator=(MultiValueAttributeOffsetReader& other) = delete;
    ~MultiValueAttributeOffsetReader() = default;

    Status Init(const std::shared_ptr<config::AttributeConfig>& attrConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& attrDirectory, uint32_t docCount,
                bool disableUpdate);

    inline std::pair<Status, uint64_t> GetOffset(docid_t docId) const __ALWAYS_INLINE;
    inline bool SetOffset(docid_t docId, uint64_t offset) __ALWAYS_INLINE;
    inline bool IsU32Offset() const __ALWAYS_INLINE;

    uint32_t GetDocCount() const;
    bool IsSupportUpdate() const;
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    BatchGetOffset(const std::vector<docid_t>& docIds, indexlib::file_system::ReadOption readOption,
                   std::vector<uint64_t>* offsets) const noexcept;

    inline MultiValueAttributeOffsetReader
    CreateSessionReader(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE;
    const std::shared_ptr<indexlib::file_system::FileReader>& GetFileReader() const;

private:
    VarLenDataParam _varLenDataParam;
    MultiValueAttributeCompressOffsetReader _compressOffsetReader;
    MultiValueAttributeUnCompressOffsetReader _unCompressOffsetReader;
    std::shared_ptr<config::AttributeConfig> _attrConfig;
    std::shared_ptr<AttributeMetrics> _attributeMetrics;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////

inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
MultiValueAttributeOffsetReader::BatchGetOffset(const std::vector<docid_t>& docIds,
                                                indexlib::file_system::ReadOption option,
                                                std::vector<uint64_t>* offsets) const noexcept
{
    if (_varLenDataParam.equalCompressOffset) {
        co_return co_await _compressOffsetReader.GetOffset(docIds, option, offsets);
    }
    co_return co_await _unCompressOffsetReader.GetOffset(docIds, option, offsets);
}

inline std::pair<Status, uint64_t> MultiValueAttributeOffsetReader::GetOffset(docid_t docId) const
{
    if (_varLenDataParam.equalCompressOffset) {
        return _compressOffsetReader.GetOffset(docId);
    }
    return _unCompressOffsetReader.GetOffset(docId);
}

inline bool MultiValueAttributeOffsetReader::SetOffset(docid_t docId, uint64_t offset)
{
    if (_varLenDataParam.equalCompressOffset) {
        return _compressOffsetReader.SetOffset(docId, offset);
    }
    return _unCompressOffsetReader.SetOffset(docId, offset);
}

inline bool MultiValueAttributeOffsetReader::IsU32Offset() const
{
    if (_varLenDataParam.equalCompressOffset) {
        return _compressOffsetReader.IsU32Offset();
    } else {
        return _unCompressOffsetReader.IsU32Offset();
    }
}

inline MultiValueAttributeOffsetReader
MultiValueAttributeOffsetReader::CreateSessionReader(autil::mem_pool::Pool* sessionPool) const
{
    MultiValueAttributeOffsetReader cloneReader(_attributeMetrics);
    cloneReader._attrConfig = _attrConfig;
    cloneReader._varLenDataParam = _varLenDataParam;
    if (_varLenDataParam.equalCompressOffset) {
        cloneReader._compressOffsetReader = _compressOffsetReader.CreateSessionReader(sessionPool);
    } else {
        cloneReader._unCompressOffsetReader = _unCompressOffsetReader.CreateSessionReader(sessionPool);
    }
    return cloneReader;
}

inline uint32_t MultiValueAttributeOffsetReader::GetDocCount() const
{
    if (_varLenDataParam.equalCompressOffset) {
        return _compressOffsetReader.GetDocCount();
    } else {
        return _unCompressOffsetReader.GetDocCount();
    }
}

inline bool MultiValueAttributeOffsetReader::IsSupportUpdate() const
{
    if (_varLenDataParam.equalCompressOffset) {
        return _compressOffsetReader.GetFileReader()->GetBaseAddress() != nullptr;
    } else {
        return _unCompressOffsetReader.GetFileReader()->GetBaseAddress() != nullptr;
    }
}
} // namespace indexlibv2::index
