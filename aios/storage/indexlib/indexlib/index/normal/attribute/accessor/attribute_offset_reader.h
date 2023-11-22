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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/data_structure/var_len_offset_reader.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class AttributeOffsetReader
{
public:
    AttributeOffsetReader(const config::AttributeConfigPtr& attrConfig, AttributeMetrics* attributeMetric = NULL);
    AttributeOffsetReader(AttributeOffsetReader&& other) = default;
    AttributeOffsetReader& operator=(AttributeOffsetReader&& other) = default;
    AttributeOffsetReader(AttributeOffsetReader& other) = delete;
    AttributeOffsetReader& operator=(AttributeOffsetReader& other) = delete;
    ~AttributeOffsetReader();

public:
    void Init(const file_system::DirectoryPtr& attrDirectory, uint32_t docCount, bool supportFileCompress,
              bool disableUpdate);

    // public for test
    void Init(uint32_t docCount, const file_system::FileReaderPtr& fileReader,
              const file_system::SliceFileReaderPtr& sliceFile = file_system::SliceFileReaderPtr());

    inline uint64_t GetOffset(docid_t docId) const __ALWAYS_INLINE;
    bool SetOffset(docid_t docId, uint64_t offset);
    inline bool IsU32Offset() const __ALWAYS_INLINE;

    uint32_t GetDocCount() const { return mOffsetReader.GetDocCount(); }
    bool IsSupportUpdate() const { return mOffsetReader.GetFileReader()->GetBaseAddress() != nullptr; }
    AttributeOffsetReader CreateSessionReader(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE
    {
        AttributeOffsetReader sessionReader(mAttrConfig, nullptr);
        sessionReader.mOffsetReader = mOffsetReader.CreateSessionReader(sessionPool);
        return sessionReader;
    }

    future_lite::coro::Lazy<index::ErrorCodeVec> BatchGetOffset(const std::vector<docid_t>& docIds,
                                                                file_system::ReadOption readOption,
                                                                std::vector<uint64_t>* offsets) const noexcept;

private:
    void InitCompressOffsetFile(const file_system::DirectoryPtr& attrDirectory,
                                file_system::FileReaderPtr& offsetFileReader,
                                file_system::SliceFileReaderPtr& sliceFileReader, bool supportFileCompress);

    void InitUncompressOffsetFile(const file_system::DirectoryPtr& attrDirectory, uint32_t docCount,
                                  file_system::FileReaderPtr& offsetFileReader,
                                  file_system::SliceFileReaderPtr& sliceFileReader, bool supportFileCompress);

    file_system::FileReaderPtr InitOffsetFile(const file_system::DirectoryPtr& attrDirectory, bool supportFileCompress);

    void InitAttributeMetrics();

    void UpdateMetrics();

private:
    VarLenOffsetReader mOffsetReader;
    config::AttributeConfigPtr mAttrConfig;
    EquivalentCompressUpdateMetrics mCompressMetrics;
    size_t mExpandSliceLen;
    AttributeMetrics* mAttributeMetrics;

private:
    friend class AttributeOffsetReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeOffsetReader);
///////////////////////////////////////////////////

inline future_lite::coro::Lazy<index::ErrorCodeVec>
AttributeOffsetReader::BatchGetOffset(const std::vector<docid_t>& docIds, file_system::ReadOption readOption,
                                      std::vector<uint64_t>* offsets) const noexcept
{
    co_return co_await mOffsetReader.GetOffset(docIds, readOption, offsets);
}

inline uint64_t AttributeOffsetReader::GetOffset(docid_t docId) const { return mOffsetReader.GetOffset(docId); }

inline bool AttributeOffsetReader::SetOffset(docid_t docId, uint64_t offset)
{
    bool ret = mOffsetReader.SetOffset(docId, offset);
    UpdateMetrics();
    return ret;
}

inline void AttributeOffsetReader::UpdateMetrics()
{
    size_t newSliceFileLen = 0;
    CompressOffsetReader* reader = mOffsetReader.GetCompressOffsetReader();
    if (!reader || !reader->GetSliceFileLen(newSliceFileLen)) {
        return;
    }

    EquivalentCompressUpdateMetrics newMetrics = reader->GetUpdateMetrics();
    if (mAttributeMetrics) {
        mAttributeMetrics->IncreaseEqualCompressExpandFileLenValue(newSliceFileLen - mExpandSliceLen);
        mAttributeMetrics->IncreaseEqualCompressWastedBytesValue(newMetrics.noUsedBytesSize -
                                                                 mCompressMetrics.noUsedBytesSize);
        mAttributeMetrics->IncreaseEqualCompressInplaceUpdateCountValue(newMetrics.inplaceUpdateCount -
                                                                        mCompressMetrics.inplaceUpdateCount);
        mAttributeMetrics->IncreaseEqualCompressExpandUpdateCountValue(newMetrics.expandUpdateCount -
                                                                       mCompressMetrics.expandUpdateCount);
    }
    mExpandSliceLen = newSliceFileLen;
    mCompressMetrics = newMetrics;
}

inline bool AttributeOffsetReader::IsU32Offset() const { return mOffsetReader.IsU32Offset(); }
}} // namespace indexlib::index
